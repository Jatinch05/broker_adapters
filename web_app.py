"""
Dhan Super Order - Web Application

A Flask-based web interface for placing Dhan Super Orders.
Users can manage credentials and place orders through a browser.
"""
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify, make_response
from functools import wraps
import os
from datetime import datetime, timedelta
import pandas as pd
from werkzeug.utils import secure_filename
import logging
import time
import threading
import uuid
from orchestrator.super_order import DhanSuperOrderOrchestrator, DhanSuperOrderError
from orchestrator.forever import DhanForeverOrderOrchestrator, DhanForeverOrderError
from validator.instruments.dhan_store import DhanStore
from validator.instruments.dhan_refresher import refresh_dhan_instruments
from apis.dhan.auth import authenticate, DhanAuthError

app = Flask(__name__)

# IMPORTANT: Do not use a random secret in production.
# A random secret causes all sessions to invalidate on every restart/scale event.
app.secret_key = os.environ.get('FLASK_SECRET_KEY') or os.environ.get('SECRET_KEY') or 'CHANGE_ME_DEV_ONLY'

app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=12)
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_SECURE'] = True if os.environ.get('RENDER') else False
ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'csv'}

# Setup logging - logs to console (visible in Render)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Console output only for Render
    ]
)
logger = logging.getLogger(__name__)

# Also log to file if running locally
if not os.environ.get('RENDER'):
    file_handler = logging.FileHandler('dhan_app.log')
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)

# Store order history in memory (in production, use a database)
# Limit to last 1000 orders to prevent memory bloat
order_history = []
MAX_ORDER_HISTORY = 1000

# Rate limiting for Dhan API: 25 orders/second
DHAN_RATE_LIMIT = 25  # orders per second
RATE_LIMIT_WINDOW = 1.0  # 1 second window
rate_limit_timestamps = []

# Bulk processing jobs (background threads)
bulk_jobs = {}
bulk_jobs_lock = threading.Lock()

"""Bulk jobs are intentionally in-memory only.

Render (and similar platforms) have limited ephemeral disk; persisting per-job JSON
snapshots can lead to storage growth. We keep only the current process' memory state.
"""

def rate_limit_wait():
    """Ensure we don't exceed Dhan's 25 orders/second rate limit"""
    global rate_limit_timestamps
    current_time = time.time()
    
    # Remove timestamps older than 1 second
    rate_limit_timestamps = [t for t in rate_limit_timestamps if current_time - t < RATE_LIMIT_WINDOW]
    
    # If we've hit the limit, wait until we can proceed
    if len(rate_limit_timestamps) >= DHAN_RATE_LIMIT:
        sleep_time = RATE_LIMIT_WINDOW - (current_time - rate_limit_timestamps[0])
        if sleep_time > 0:
            logger.info(f"Rate limit reached, waiting {sleep_time:.2f}s")
            time.sleep(sleep_time)
            # Clean up old timestamps after waiting
            current_time = time.time()
            rate_limit_timestamps = [t for t in rate_limit_timestamps if current_time - t < RATE_LIMIT_WINDOW]
    
    # Record this request
    rate_limit_timestamps.append(current_time)


def allowed_file(filename):
    """Check if the uploaded file has an allowed extension"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def create_bulk_job(df: pd.DataFrame, client_id: str, access_token: str) -> str:
    """Create and start a background bulk job"""
    job_id = str(uuid.uuid4())
    cancel_event = threading.Event()
    job = {
        'id': job_id,
        'client_id': client_id,
        'status': 'pending',
        'message': 'Queued',
        'results': [],
        'success_count': 0,
        'failed_count': 0,
        'total': len(df),
        'started_at': None,
        'finished_at': None,
        'error': None,
        'perf': {
            'prefetch_s': None,
            'orders_seen': 0,
            'build_s_total': 0.0,
            'rate_wait_s_total': 0.0,
            'api_s_total': 0.0,
            'row_s_total': 0.0,
            'avg_build_s': None,
            'avg_rate_wait_s': None,
            'avg_api_s': None,
            'avg_row_s': None,
        },
        'cancel_event': cancel_event,
    }
    with bulk_jobs_lock:
        bulk_jobs[job_id] = job
    thread = threading.Thread(
        target=_run_bulk_job,
        args=(job_id, df.copy(), client_id, access_token, cancel_event),
        daemon=True,
    )
    thread.start()
    return job_id


def _run_bulk_job(job_id: str, df: pd.DataFrame, client_id: str, access_token: str, cancel_event: threading.Event):
    """Background worker to process bulk orders without blocking the request thread"""
    with bulk_jobs_lock:
        job = bulk_jobs.get(job_id)
        if job is None:
            return
    try:
        with bulk_jobs_lock:
            job = bulk_jobs.get(job_id)
            if job is None:
                return
            job['status'] = 'running'
            job['message'] = 'Processing'
            job['started_at'] = datetime.now().isoformat()

        last_persist = time.monotonic()

        # Warm instruments once to avoid repeated loads during the run
        try:
            DhanStore.load()
            # Big speed-up in streaming mode: prefetch all symbols/contracts in one scan
            prefetch_start = time.perf_counter()
            try:
                DhanStore.prefetch_bulk(df)
            except Exception as e:  # pragma: no cover
                logger.warning(f"Bulk job {job_id}: prefetch warning: {e}")
            prefetch_s = time.perf_counter() - prefetch_start
            with bulk_jobs_lock:
                job = bulk_jobs.get(job_id)
                if job is not None:
                    job['perf']['prefetch_s'] = round(prefetch_s, 4)
            logger.info(f"Bulk job {job_id}: prefetch completed in {prefetch_s:.3f}s")
        except Exception as e:  # pragma: no cover
            logger.warning(f"Bulk job {job_id}: instrument load warning: {e}")

        orch_super = DhanSuperOrderOrchestrator(client_id=client_id, access_token=access_token)
        orch_forever = DhanForeverOrderOrchestrator(client_id=client_id, access_token=access_token)

        # Precompute column presence (avoid repeated 'in df.columns' checks)
        cols = set(df.columns)
        has_flow_col = 'DhanOrderType' in cols
        has_order_category = 'OrderCategory' in cols
        has_trigger = 'TriggerPrice' in cols
        has_price = 'Price' in cols
        has_order_flag = 'OrderFlag' in cols
        has_validity = 'Validity' in cols
        has_dq = 'DisclosedQuantity' in cols
        has_tag = 'Tag' in cols
        has_price1 = 'Price1' in cols
        has_trigger1 = 'TriggerPrice1' in cols
        has_qty1 = 'Quantity1' in cols
        has_target = 'TargetPrice' in cols
        has_sl = 'StopLoss' in cols
        has_trail = 'TrailingStopLoss' in cols
        has_strike = 'StrikePrice' in cols
        has_expiry = 'ExpiryDate' in cols
        has_opt_type = 'OptionType' in cols

        # Iterate efficiently (faster than iterrows)
        for index, row in enumerate(df.itertuples(index=False), start=0):
            row_start = time.perf_counter()
            if cancel_event.is_set():
                with bulk_jobs_lock:
                    job = bulk_jobs.get(job_id)
                    if job is None:
                        return
                    job['status'] = 'cancelled'
                    job['message'] = 'Cancelled by user'
                break

            row_num = index + 2  # Excel row number (1-indexed + header)
            result = {
                'row': row_num,
                'symbol': getattr(row, 'Symbol', 'N/A'),
                'status': 'Processing',
                'message': '',
                'order_id': None
            }
            try:
                build_start = time.perf_counter()
                build_s = 0.0
                rate_s = 0.0
                api_s = 0.0
                # Validate required fields
                row_symbol = getattr(row, 'Symbol', None)
                if row_symbol is None or pd.isna(row_symbol) or not str(row_symbol).strip():
                    result['status'] = 'Failed'
                    result['message'] = 'Symbol is required'
                    with bulk_jobs_lock:
                        job = bulk_jobs.get(job_id)
                        if job is None:
                            return
                        job['results'].append(result)
                        job['failed_count'] += 1
                    continue

                # Decide flow: SUPER or FOREVER
                order_flow = 'SUPER'
                if has_flow_col:
                    v = getattr(row, 'DhanOrderType', None)
                    if v is not None and not pd.isna(v):
                        order_flow = str(v).strip().upper()
                elif has_order_category:
                    v = getattr(row, 'OrderCategory', None)
                    if v is not None and not pd.isna(v):
                        order_flow = str(v).strip().upper()
                is_forever = order_flow == 'FOREVER' or (has_trigger and not pd.isna(getattr(row, 'TriggerPrice', None)))

                base_common = {
                    'symbol': str(getattr(row, 'Symbol')).strip().upper(),
                    'exchange': str(getattr(row, 'Exchange')).strip().upper(),
                    'txn_type': str(getattr(row, 'TransactionType')).strip().upper(),
                    'qty': int(getattr(row, 'Quantity')),
                    'order_type': str(getattr(row, 'OrderType')).strip().upper(),
                    'product': str(getattr(row, 'ProductType')).strip().upper(),
                    'price': None,
                }
                if has_price:
                    v = getattr(row, 'Price', None)
                    if v is not None and not pd.isna(v) and v != '':
                        base_common['price'] = float(v)

                # Optional derivative lookup fields
                if has_strike:
                    v = getattr(row, 'StrikePrice', None)
                    if v is not None and not pd.isna(v) and v != '':
                        base_common['strike_price'] = float(v)
                if has_expiry:
                    v = getattr(row, 'ExpiryDate', None)
                    if v is not None and not pd.isna(v) and v != '':
                        base_common['expiry_date'] = str(v).strip()
                if has_opt_type:
                    v = getattr(row, 'OptionType', None)
                    if v is not None and not pd.isna(v) and v != '':
                        base_common['option_type'] = str(v).strip().upper()

                if is_forever:
                    order_data = dict(base_common)
                    order_data['order_category'] = 'FOREVER'
                    if has_trigger:
                        v = getattr(row, 'TriggerPrice', None)
                        if v is not None and not pd.isna(v) and v != '':
                            order_data['trigger_price'] = float(v)
                        else:
                            v = None
                    else:
                        v = None

                    if v is None:
                        result['status'] = 'Failed'
                        result['message'] = 'TriggerPrice is required for Forever Orders'
                        with bulk_jobs_lock:
                            job = bulk_jobs.get(job_id)
                            if job is None:
                                return
                            job['results'].append(result)
                            job['failed_count'] += 1
                        continue

                    if has_order_flag:
                        of = getattr(row, 'OrderFlag', None)
                        if of is not None and not pd.isna(of):
                            order_data['order_flag'] = str(of).strip().upper()
                        else:
                            order_data['order_flag'] = 'SINGLE'
                    else:
                        order_data['order_flag'] = 'SINGLE'
                    if order_data['order_flag'] == 'OCO':
                        if has_price1:
                            vv = getattr(row, 'Price1', None)
                            if vv is not None and not pd.isna(vv):
                                order_data['price1'] = float(vv)
                        if has_trigger1:
                            vv = getattr(row, 'TriggerPrice1', None)
                            if vv is not None and not pd.isna(vv):
                                order_data['trigger_price1'] = float(vv)
                        if has_qty1:
                            vv = getattr(row, 'Quantity1', None)
                            if vv is not None and not pd.isna(vv):
                                order_data['quantity1'] = int(vv)

                    if has_validity:
                        vv = getattr(row, 'Validity', None)
                        if vv is not None and not pd.isna(vv):
                            order_data['validity'] = str(vv).strip().upper()
                        else:
                            order_data['validity'] = 'DAY'
                    else:
                        order_data['validity'] = 'DAY'
                    if has_dq:
                        vv = getattr(row, 'DisclosedQuantity', None)
                        if vv is not None and not pd.isna(vv):
                            order_data['disclosed_quantity'] = int(vv)
                    if has_tag:
                        vv = getattr(row, 'Tag', None)
                        if vv is not None and not pd.isna(vv) and vv != '':
                            order_data['tag'] = str(vv).strip()
                else:
                    order_data = dict(base_common)
                    order_data['order_category'] = 'SUPER'
                    if has_target:
                        v = getattr(row, 'TargetPrice', None)
                        if v is not None and not pd.isna(v) and v != '':
                            order_data['target_price'] = float(v)
                        else:
                            v = None
                    else:
                        v = None

                    if v is None:
                        result['status'] = 'Failed'
                        result['message'] = 'TargetPrice is required for Super Orders'
                        with bulk_jobs_lock:
                            job = bulk_jobs.get(job_id)
                            if job is None:
                                return
                            job['results'].append(result)
                            job['failed_count'] += 1
                        continue

                    if has_sl:
                        vv = getattr(row, 'StopLoss', None)
                        if vv is not None and not pd.isna(vv) and vv != '':
                            order_data['stop_loss_price'] = float(vv)
                        else:
                            vv = None
                    else:
                        vv = None

                    if vv is None:
                        result['status'] = 'Failed'
                        result['message'] = 'StopLoss is required for Super Orders'
                        with bulk_jobs_lock:
                            job = bulk_jobs.get(job_id)
                            if job is None:
                                return
                            job['results'].append(result)
                            job['failed_count'] += 1
                        continue

                    if has_trail:
                        vv = getattr(row, 'TrailingStopLoss', None)
                        if vv is not None and not pd.isna(vv) and vv != '':
                            order_data['trailing_jump'] = float(vv)
                    if has_tag:
                        vv = getattr(row, 'Tag', None)
                        if vv is not None and not pd.isna(vv) and vv != '':
                            order_data['tag'] = str(vv).strip()

                # Keep logging light (bulk speed + avoids noisy logs)
                if (row_num % 25) == 0:
                    logger.info(
                        f"Bulk job {job_id}: progress {row_num - 1}/{len(df)} (last={order_data['order_category']} {order_data['symbol']})"
                    )
                build_s = time.perf_counter() - build_start

                # Allow cancellation right before we call broker APIs
                if cancel_event.is_set():
                    raise RuntimeError("Cancelled")

                rate_start = time.perf_counter()
                rate_limit_wait()
                rate_s = time.perf_counter() - rate_start

                api_start = time.perf_counter()
                try:
                    if is_forever:
                        response = orch_forever.place_forever_order(order_data)
                    else:
                        response = orch_super.place_super_order(order_data)
                except Exception:
                    api_s = time.perf_counter() - api_start
                    raise
                api_s = time.perf_counter() - api_start

                order_record = {
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'symbol': order_data['symbol'],
                    'exchange': order_data['exchange'],
                    'type': order_data['txn_type'],
                    'quantity': order_data['qty'],
                    'order_type': order_data['order_type'],
                    'price': order_data.get('price', 'MARKET'),
                    'product': order_data['product'],
                    'target': order_data.get('target_price', 'N/A'),
                    'stop_loss': order_data.get('stop_loss_price', 'N/A'),
                    'trail_sl': order_data.get('trailing_jump', 'N/A'),
                    'tag': order_data.get('tag', ''),
                    'order_id': response.get('orderId', 'N/A'),
                    'status': 'Success'
                }
                order_history.append(order_record)
                if len(order_history) > MAX_ORDER_HISTORY:
                    order_history.pop(0)

                result['status'] = 'Success'
                result['message'] = 'Order placed successfully'
                result['order_id'] = response.get('orderId', 'N/A')
                with bulk_jobs_lock:
                    job = bulk_jobs.get(job_id)
                    if job is None:
                        return
                    job['success_count'] += 1
                    job['perf']['orders_seen'] += 1
                    job['perf']['build_s_total'] += build_s
                    job['perf']['rate_wait_s_total'] += rate_s
                    job['perf']['api_s_total'] += api_s
            except ValueError as e:
                result['status'] = 'Failed'
                result['message'] = f'Validation error: {str(e)}'
                with bulk_jobs_lock:
                    job = bulk_jobs.get(job_id)
                    if job is None:
                        return
                    job['failed_count'] += 1
                    # Still track time even when validation fails (build cost)
                    job['perf']['orders_seen'] += 1
                    job['perf']['build_s_total'] += (time.perf_counter() - build_start)
            except (DhanSuperOrderError, DhanForeverOrderError) as e:
                result['status'] = 'Failed'
                result['message'] = f'Order error: {str(e)}'
                with bulk_jobs_lock:
                    job = bulk_jobs.get(job_id)
                    if job is None:
                        return
                    job['failed_count'] += 1
                    job['perf']['orders_seen'] += 1
                    job['perf']['build_s_total'] += build_s
                    job['perf']['rate_wait_s_total'] += rate_s
                    job['perf']['api_s_total'] += api_s
            except Exception as e:
                if str(e) == 'Cancelled':
                    # Respect cancellation without counting as a failure
                    with bulk_jobs_lock:
                        job = bulk_jobs.get(job_id)
                        if job is None:
                            return
                        job['status'] = 'cancelled'
                        job['message'] = 'Cancelled by user'
                    break
                result['status'] = 'Failed'
                result['message'] = f'Error: {str(e)}'
                with bulk_jobs_lock:
                    job = bulk_jobs.get(job_id)
                    if job is None:
                        return
                    job['failed_count'] += 1
                    job['perf']['orders_seen'] += 1
                    job['perf']['build_s_total'] += build_s
                    job['perf']['rate_wait_s_total'] += rate_s
                    job['perf']['api_s_total'] += api_s

            with bulk_jobs_lock:
                job = bulk_jobs.get(job_id)
                if job is None:
                    return
                job['results'].append(result)

                # Track overall row time
                row_s = time.perf_counter() - row_start
                job['perf']['row_s_total'] += row_s

                # Update averages for UI/diagnostics
                seen = int(job['perf'].get('orders_seen') or 0)
                if seen > 0:
                    job['perf']['avg_build_s'] = round(job['perf']['build_s_total'] / seen, 4)
                    job['perf']['avg_rate_wait_s'] = round(job['perf']['rate_wait_s_total'] / seen, 4)
                    job['perf']['avg_api_s'] = round(job['perf']['api_s_total'] / seen, 4)
                    job['perf']['avg_row_s'] = round(job['perf']['row_s_total'] / seen, 4)

                # Keep last_persist variable (reserved if we later re-add throttled updates)
                last_persist = last_persist

        with bulk_jobs_lock:
            job = bulk_jobs.get(job_id)
            if job is None:
                return
            if job.get('status') != 'cancelled':
                job['status'] = 'completed'
                job['message'] = 'Completed'
            job['finished_at'] = datetime.now().isoformat()

            # Final averages
            seen = int(job['perf'].get('orders_seen') or 0)
            if seen > 0:
                job['perf']['avg_build_s'] = round(job['perf']['build_s_total'] / seen, 4)
                job['perf']['avg_rate_wait_s'] = round(job['perf']['rate_wait_s_total'] / seen, 4)
                job['perf']['avg_api_s'] = round(job['perf']['api_s_total'] / seen, 4)
                job['perf']['avg_row_s'] = round(job['perf']['row_s_total'] / seen, 4)

            logger.info(
                f"Bulk job {job_id}: perf avg_row={job['perf'].get('avg_row_s')}s avg_api={job['perf'].get('avg_api_s')}s avg_rate_wait={job['perf'].get('avg_rate_wait_s')}s avg_build={job['perf'].get('avg_build_s')}s"
            )
            logger.info(
                f"Bulk job {job_id}: done status={job.get('status')} total={job.get('total')} success={job.get('success_count')} failed={job.get('failed_count')} results_len={len(job.get('results') or [])}"
            )

            snap = {k: v for k, v in job.items() if k != 'cancel_event'}
            if isinstance(snap.get('results'), list) and len(snap['results']) > 200:
                snap['results'] = snap['results'][-200:]

    except Exception as e:  # pragma: no cover
        with bulk_jobs_lock:
            job = bulk_jobs.get(job_id)
            if job is None:
                return
            job['status'] = 'failed'
            job['message'] = 'Failed'
            job['error'] = str(e)
            job['finished_at'] = datetime.now().isoformat()

            snap = {k: v for k, v in job.items() if k != 'cancel_event'}



def login_required(f):
    """Decorator to require login for certain routes"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'client_id' not in session or 'access_token' not in session:
            flash('Please login with your Dhan credentials first.', 'warning')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function


@app.route('/')
def index():
    """Home page - redirect to dashboard if logged in, else login"""
    if 'client_id' in session:
        return redirect(url_for('dashboard'))
    return redirect(url_for('login'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    """Login page for entering Dhan credentials"""
    if request.method == 'POST':
        client_id = request.form.get('client_id', '').strip()
        access_token = request.form.get('access_token', '').strip()
        
        if not client_id or not access_token:
            flash('Please provide both Client ID and Access Token.', 'error')
            return render_template('login.html')
        
        try:
            # Test authentication
            authenticate(client_id, access_token)
            
            # Store in session
            session['client_id'] = client_id
            session['access_token'] = access_token
            session['login_time'] = datetime.now().isoformat()
            session.permanent = True
            
            flash('Login successful! Welcome to Dhan Super Orders.', 'success')
            return redirect(url_for('dashboard'))
            
        except DhanAuthError as e:
            flash(f'Authentication failed: {str(e)}', 'error')
            return render_template('login.html')
    
    return render_template('login.html')


@app.route('/logout')
def logout():
    """Logout and clear session"""
    session.clear()
    flash('You have been logged out successfully.', 'info')
    return redirect(url_for('login'))


@app.route('/dashboard')
@login_required
def dashboard():
    """Main dashboard for placing orders"""
    return render_template('dashboard.html', 
                         client_id=session.get('client_id'),
                         order_count=len(order_history))


@app.route('/place-order', methods=['GET', 'POST'])
@login_required
def place_order():
    """Order placement form and handler"""
    if request.method == 'POST':
        try:
            order_flow = request.form.get('order_flow', 'SUPER').strip().upper()
            # Get form data
            order_data = {
                'symbol': request.form.get('symbol', '').strip().upper(),
                'exchange': request.form.get('exchange', '').strip().upper(),
                'txn_type': request.form.get('txn_type', '').strip().upper(),
                'qty': int(request.form.get('qty', 0)),
                'order_type': request.form.get('order_type', '').strip().upper(),
                'price': float(request.form.get('price', 0)) if request.form.get('price') else None,
                'product': request.form.get('product', '').strip().upper(),
                'order_category': order_flow,
            }
            
            # Add optional advanced lookup fields (for derivatives)
            strike_price = request.form.get('strike_price', '').strip()
            if strike_price:
                order_data['strike_price'] = float(strike_price)
            
            expiry_date = request.form.get('expiry_date', '').strip()
            if expiry_date:
                order_data['expiry_date'] = expiry_date
            
            option_type = request.form.get('option_type', '').strip()
            if option_type:
                order_data['option_type'] = option_type.upper()
            
            # Add tag if provided
            tag = request.form.get('tag', '').strip()
            if tag:
                order_data['tag'] = tag
            
            if order_flow == 'FOREVER':
                # Forever specific fields
                order_data['trigger_price'] = float(request.form.get('trigger_price', 0) or 0)
                order_data['order_flag'] = request.form.get('order_flag', 'SINGLE').strip().upper() or 'SINGLE'
                order_data['validity'] = request.form.get('validity', 'DAY').strip().upper() or 'DAY'
                dq = request.form.get('disclosed_quantity', '')
                order_data['disclosed_quantity'] = int(dq) if dq != '' else 0
                # OCO legs
                if order_data['order_flag'] == 'OCO':
                    if request.form.get('price1'): order_data['price1'] = float(request.form.get('price1'))
                    if request.form.get('trigger_price1'): order_data['trigger_price1'] = float(request.form.get('trigger_price1'))
                    if request.form.get('quantity1'): order_data['quantity1'] = int(request.form.get('quantity1'))

                rate_limit_wait()
                orch_f = DhanForeverOrderOrchestrator(client_id=session['client_id'], access_token=session['access_token'])
                result = orch_f.place_forever_order(order_data)
            else:
                # Super-specific fields
                order_data['target_price'] = float(request.form.get('target_price', 0))
                order_data['stop_loss_price'] = float(request.form.get('stop_loss_price', 0))
                order_data['trailing_jump'] = float(request.form.get('trailing_jump', 0))
                order_data['order_category'] = 'SUPER'

                orchestrator = DhanSuperOrderOrchestrator(
                    client_id=session['client_id'],
                    access_token=session['access_token']
                )
                rate_limit_wait()
                result = orchestrator.place_super_order(order_data)
            
            # Store in history
            order_record = {
                'timestamp': datetime.now().isoformat(),
                'order_id': result['orderId'],
                'status': result['orderStatus'],
                'order_category': order_flow,
                'symbol': order_data['symbol'],
                'exchange': order_data.get('exchange'),
                'txn_type': order_data['txn_type'],
                'qty': order_data['qty'],
                'order_type': order_data['order_type'],
                'price': order_data['price'],
                'product': order_data.get('product'),
                # SUPER fields (may not exist for FOREVER)
                'target_price': order_data.get('target_price'),
                'stop_loss_price': order_data.get('stop_loss_price'),
                # FOREVER fields (may not exist for SUPER)
                'trigger_price': order_data.get('trigger_price'),
                'order_flag': order_data.get('order_flag'),
            }
            order_history.insert(0, order_record)  # Add to beginning
            # Keep only last MAX_ORDER_HISTORY orders
            if len(order_history) > MAX_ORDER_HISTORY:
                order_history.pop()
            
            flash(f'✅ Order placed successfully! Order ID: {result["orderId"]}', 'success')
            return redirect(url_for('order_history_page'))
            
        except ValueError as e:
            logger.error(f'Validation error in place_order: {str(e)}')
            flash(f'Invalid input: {str(e)}', 'error')
        except (DhanSuperOrderError, DhanForeverOrderError) as e:
            logger.error(f'Order placement error: {str(e)}')
            flash(f'Order failed: {str(e)}', 'error')
        except Exception as e:
            logger.error(f'Unexpected error in place_order: {str(e)}', exc_info=True)
            flash(f'Unexpected error: {str(e)}', 'error')
    
    return render_template('place_order.html')




@app.route('/order-history')
@login_required
def order_history_page():
    """Display order history"""
    return render_template('order_history.html', orders=order_history)


@app.route('/refresh-instruments', methods=['POST'])
@login_required
def refresh_instruments():
    """Refresh instrument master data"""
    try:
        csv_path = refresh_dhan_instruments()
        flash(f'✅ Instruments refreshed successfully!', 'success')
    except Exception as e:
        flash(f'Failed to refresh instruments: {str(e)}', 'error')
    
    return redirect(url_for('dashboard'))


@app.route('/settings')
@login_required
def settings():
    """Settings page"""
    return render_template('settings.html', 
                         client_id=session.get('client_id'))


@app.route('/api/validate-symbol/<symbol>')
@login_required
def validate_symbol(symbol):
    """API endpoint to validate symbol"""
    try:
        logger.info(f'Validating symbol: {symbol}')
        # Ensure instruments are loaded
        store = DhanStore.load()
        instrument = store.lookup_symbol(symbol.upper())
        
        if instrument:
            return jsonify({
                'valid': True,
                'symbol': instrument.symbol,
                'security_id': instrument.security_id,
                'exchange': instrument.exchange_segment,
                'lot_size': instrument.lot_size,
                'instrument_type': instrument.instrument_type
            })
        else:
            return jsonify({'valid': False, 'message': 'Symbol not found'})
    except Exception as e:
        logger.error(f'Error validating symbol {symbol}: {str(e)}')
        return jsonify({'valid': False, 'message': str(e)})


@app.route('/bulk-upload', methods=['GET', 'POST'])
@login_required
def bulk_upload():
    """Bulk order upload from Excel file"""
    if request.method == 'POST':
        # Block new uploads while an existing job is still processing
        existing_id = session.get('last_bulk_job_id')
        if existing_id:
            existing = _job_snapshot(existing_id)
            if existing is not None and existing.get('client_id') == session.get('client_id'):
                if existing.get('status') in ['pending', 'running', 'cancelling']:
                    flash('A bulk job is already running. Please wait for it to finish (or cancel it) before uploading another file.', 'warning')
                    return redirect(url_for('bulk_status', job_id=existing_id))

        # Check if file was uploaded
        if 'file' not in request.files:
            flash('No file uploaded.', 'error')
            # Clear stale job pointer so UI doesn't look "stuck"
            session.pop('last_bulk_job_id', None)
            return redirect(url_for('bulk_upload'), code=303)
        
        file = request.files['file']
        
        # Check if file is selected
        if file.filename == '':
            flash('No file selected.', 'error')
            session.pop('last_bulk_job_id', None)
            return redirect(url_for('bulk_upload'), code=303)
        
        # Check if file type is allowed
        if not allowed_file(file.filename):
            flash('Invalid file type. Please upload Excel (.xlsx, .xls) or CSV file.', 'error')
            session.pop('last_bulk_job_id', None)
            return redirect(url_for('bulk_upload'), code=303)
        
        try:
            # Read the Excel/CSV file
            filename = secure_filename(file.filename)
            if filename.endswith('.csv'):
                df = pd.read_csv(file)
            else:
                df = pd.read_excel(file)
            
            # Validate required columns
            required_columns = ['Symbol', 'Exchange', 'TransactionType', 'Quantity', 
                              'OrderType', 'ProductType']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                flash(f'Missing required columns: {", ".join(missing_columns)}', 'error')
                # Render a clean page; do not keep any stale job state.
                session.pop('last_bulk_job_id', None)
                resp = make_response(render_template(
                    'bulk_upload.html',
                    bulk_in_progress=False,
                    job=None,
                    results=[],
                    success_count=0,
                    failed_count=0,
                ))
                resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
                resp.headers['Pragma'] = 'no-cache'
                resp.headers['Expires'] = '0'
                return resp

            # If there was an older finished job, drop it from memory so the new run overrides results
            old_id = session.get('last_bulk_job_id')
            if old_id:
                with bulk_jobs_lock:
                    old_job = bulk_jobs.get(old_id)
                    if old_job is not None and old_job.get('client_id') == session.get('client_id'):
                        if old_job.get('status') not in ['pending', 'running', 'cancelling']:
                            bulk_jobs.pop(old_id, None)

            # Start background job
            job_id = create_bulk_job(df, session['client_id'], session['access_token'])
            session['last_bulk_job_id'] = job_id
            flash(f'Started bulk upload. Job ID: {job_id}', 'info')
            return redirect(url_for('bulk_status', job_id=job_id), code=303)
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            logger.error(f'ERROR in bulk upload: {error_details}')
            flash(f'Error processing file: {str(e)}', 'error')
            session.pop('last_bulk_job_id', None)
            return redirect(url_for('bulk_upload'), code=303)
    
    # GET request - show the upload form.
    # Only surface a job here if it is still running; finished jobs should not
    # keep disabling the UI or showing stale results.
    job = None
    last_id = session.get('last_bulk_job_id')
    if last_id:
        snap = _job_snapshot(last_id)
        if snap is not None and snap.get('client_id') == session.get('client_id'):
            if snap.get('status') in ['pending', 'running', 'cancelling']:
                job = snap
    resp = make_response(render_template(
        'bulk_upload.html',
        bulk_in_progress=bool(job and job.get('status') in ['pending', 'running', 'cancelling']),
        job=job,
        results=(job.get('results', [])[-200:] if job else []),
        success_count=(job.get('success_count', 0) if job else 0),
        failed_count=(job.get('failed_count', 0) if job else 0),
    ))
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    return resp


def _job_snapshot(job_id: str):
    with bulk_jobs_lock:
        job = bulk_jobs.get(job_id)
        if job is not None:
            # Copy excluding non-serializable items; clone results list to avoid race with worker thread
            snap = {k: v for k, v in job.items() if k != 'cancel_event'}
            if 'results' in snap and isinstance(snap['results'], list):
                snap['results'] = list(snap['results'])
            return snap

    return None


@app.route('/bulk-status/<job_id>', methods=['GET'])
@login_required
def bulk_status(job_id):
    job = _job_snapshot(job_id)
    if job is None:
        flash('Bulk job not found or expired.', 'error')
        return redirect(url_for('bulk_upload'))
    if job.get('client_id') != session.get('client_id'):
        flash('Bulk job not found or expired.', 'error')
        return redirect(url_for('bulk_upload'))
    session['last_bulk_job_id'] = job_id
    resp = make_response(render_template(
        'bulk_upload.html',
        bulk_in_progress=job['status'] in ['pending', 'running', 'cancelling'],
        job=job,
        results=job.get('results', [])[-200:],
        success_count=job.get('success_count', 0),
        failed_count=job.get('failed_count', 0)
    ))
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    return resp


@app.route('/bulk-status/<job_id>/json', methods=['GET'])
@login_required
def bulk_status_json(job_id):
    job = _job_snapshot(job_id)
    if job is None:
        return jsonify({'error': 'not found'}), 404
    if job.get('client_id') != session.get('client_id'):
        return jsonify({'error': 'not found'}), 404
    # Trim results in JSON to avoid huge payload; do NOT mutate the stored job
    results = job.get('results', [])
    payload = dict(job)
    payload['results'] = results[-20:]
    resp = make_response(jsonify(payload))
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    return resp


@app.route('/bulk-cancel/<job_id>', methods=['POST'])
@login_required
def bulk_cancel(job_id):
    with bulk_jobs_lock:
        job = bulk_jobs.get(job_id)
        if job is None:
            flash('Bulk job not found or already finished.', 'warning')
            return redirect(url_for('bulk_upload'))
        cancel_event = job.get('cancel_event')
        if cancel_event:
            cancel_event.set()
            job['message'] = 'Cancellation requested'
            job['status'] = 'cancelling'
            snap = {k: v for k, v in job.items() if k != 'cancel_event'}
        else:
            snap = None
    flash('Bulk upload cancellation requested. Pending orders will stop shortly.', 'info')
    return redirect(url_for('bulk_status', job_id=job_id), code=303)




if __name__ == '__main__':
    # Create necessary directories
    os.makedirs('templates', exist_ok=True)
    os.makedirs('static', exist_ok=True)
    os.makedirs('uploads', exist_ok=True)
    
    logger.info("=" * 60)
    logger.info("Dhan Super Order - Web Application")
    logger.info("=" * 60)
    logger.info("\nStarting server...")
    logger.info("Open your browser and go to: http://localhost:5000")
    logger.info("\nPress Ctrl+C to stop the server\n")
    logger.info("Logs being written to: dhan_app.log")
    
    app.run(debug=False, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))