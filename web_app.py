"""
Dhan Super Order - Web Application

A Flask-based web interface for placing Dhan Super Orders.
Users can manage credentials and place orders through a browser.
"""
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
from functools import wraps
import os
from datetime import datetime
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
app.secret_key = os.urandom(24)  # Random secret key for sessions
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
app.config['UPLOAD_FOLDER'] = 'uploads'
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

app = Flask(__name__)
app.secret_key = os.urandom(24)  # Random secret key for sessions
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
app.config['UPLOAD_FOLDER'] = 'uploads'
ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'csv'}

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
        'status': 'pending',
        'message': 'Queued',
        'results': [],
        'success_count': 0,
        'failed_count': 0,
        'total': len(df),
        'started_at': None,
        'finished_at': None,
        'error': None,
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
        job['status'] = 'running'
        job['message'] = 'Processing'
        job['started_at'] = datetime.now().isoformat()

        # Warm instruments once to avoid repeated loads during the run
        try:
            DhanStore.load()
        except Exception as e:  # pragma: no cover
            logger.warning(f"Bulk job {job_id}: instrument load warning: {e}")

        orch_super = DhanSuperOrderOrchestrator(client_id=client_id, access_token=access_token)
        orch_forever = DhanForeverOrderOrchestrator(client_id=client_id, access_token=access_token)

        for index, row in df.iterrows():
            if cancel_event.is_set():
                job['status'] = 'cancelled'
                job['message'] = 'Cancelled by user'
                break

            row_num = index + 2  # Excel row number (1-indexed + header)
            result = {
                'row': row_num,
                'symbol': row.get('Symbol', 'N/A'),
                'status': 'Processing',
                'message': '',
                'order_id': None
            }
            try:
                # Validate required fields
                if pd.isna(row.get('Symbol')) or not str(row.get('Symbol')).strip():
                    result['status'] = 'Failed'
                    result['message'] = 'Symbol is required'
                    job['results'].append(result)
                    job['failed_count'] += 1
                    continue

                # Decide flow: SUPER or FOREVER
                order_flow = 'SUPER'
                if 'DhanOrderType' in df.columns and not pd.isna(row.get('DhanOrderType')):
                    order_flow = str(row.get('DhanOrderType')).strip().upper()
                elif 'OrderCategory' in df.columns and not pd.isna(row.get('OrderCategory')):
                    order_flow = str(row.get('OrderCategory')).strip().upper()
                is_forever = order_flow == 'FOREVER' or ('TriggerPrice' in df.columns and not pd.isna(row.get('TriggerPrice')))

                base_common = {
                    'symbol': str(row['Symbol']).strip().upper(),
                    'exchange': str(row['Exchange']).strip().upper(),
                    'txn_type': str(row['TransactionType']).strip().upper(),
                    'qty': int(row['Quantity']),
                    'order_type': str(row['OrderType']).strip().upper(),
                    'product': str(row['ProductType']).strip().upper(),
                    'price': None,
                }
                if 'Price' in df.columns and not pd.isna(row.get('Price')) and row.get('Price') != '':
                    base_common['price'] = float(row['Price'])

                # Optional derivative lookup fields
                if 'StrikePrice' in row and not pd.isna(row['StrikePrice']) and row['StrikePrice'] != '':
                    base_common['strike_price'] = float(row['StrikePrice'])
                if 'ExpiryDate' in row and not pd.isna(row['ExpiryDate']) and row['ExpiryDate'] != '':
                    base_common['expiry_date'] = str(row['ExpiryDate']).strip()
                if 'OptionType' in row and not pd.isna(row['OptionType']) and row['OptionType'] != '':
                    base_common['option_type'] = str(row['OptionType']).strip().upper()

                if is_forever:
                    order_data = dict(base_common)
                    order_data['order_category'] = 'FOREVER'
                    if 'TriggerPrice' in df.columns and not pd.isna(row.get('TriggerPrice')) and row.get('TriggerPrice') != '':
                        order_data['trigger_price'] = float(row['TriggerPrice'])
                    else:
                        result['status'] = 'Failed'
                        result['message'] = 'TriggerPrice is required for Forever Orders'
                        job['results'].append(result)
                        job['failed_count'] += 1
                        continue
                    if 'OrderFlag' in df.columns and not pd.isna(row.get('OrderFlag')):
                        order_data['order_flag'] = str(row['OrderFlag']).strip().upper()
                    else:
                        order_data['order_flag'] = 'SINGLE'
                    if order_data['order_flag'] == 'OCO':
                        if 'Price1' in df.columns and not pd.isna(row.get('Price1')): order_data['price1'] = float(row['Price1'])
                        if 'TriggerPrice1' in df.columns and not pd.isna(row.get('TriggerPrice1')): order_data['trigger_price1'] = float(row['TriggerPrice1'])
                        if 'Quantity1' in df.columns and not pd.isna(row.get('Quantity1')): order_data['quantity1'] = int(row['Quantity1'])
                    if 'Validity' in df.columns and not pd.isna(row.get('Validity')):
                        order_data['validity'] = str(row['Validity']).strip().upper()
                    else:
                        order_data['validity'] = 'DAY'
                    if 'DisclosedQuantity' in df.columns and not pd.isna(row.get('DisclosedQuantity')):
                        order_data['disclosed_quantity'] = int(row['DisclosedQuantity'])
                    if 'Tag' in df.columns and not pd.isna(row.get('Tag')) and row.get('Tag') != '':
                        order_data['tag'] = str(row['Tag']).strip()
                else:
                    order_data = dict(base_common)
                    order_data['order_category'] = 'SUPER'
                    if 'TargetPrice' in df.columns and not pd.isna(row.get('TargetPrice')) and row.get('TargetPrice') != '':
                        order_data['target_price'] = float(row['TargetPrice'])
                    else:
                        result['status'] = 'Failed'
                        result['message'] = 'TargetPrice is required for Super Orders'
                        job['results'].append(result)
                        job['failed_count'] += 1
                        continue
                    if 'StopLoss' in df.columns and not pd.isna(row.get('StopLoss')) and row.get('StopLoss') != '':
                        order_data['stop_loss_price'] = float(row['StopLoss'])
                    else:
                        result['status'] = 'Failed'
                        result['message'] = 'StopLoss is required for Super Orders'
                        job['results'].append(result)
                        job['failed_count'] += 1
                        continue
                    if 'TrailingStopLoss' in df.columns and not pd.isna(row.get('TrailingStopLoss')) and row.get('TrailingStopLoss') != '':
                        order_data['trailing_jump'] = float(row['TrailingStopLoss'])
                    if 'Tag' in df.columns and not pd.isna(row.get('Tag')) and row.get('Tag') != '':
                        order_data['tag'] = str(row['Tag']).strip()

                logger.info(f"Bulk job {job_id}: Placing {order_data['order_category']} row={row_num} symbol={order_data['symbol']} ex={order_data['exchange']} qty={order_data['qty']}")
                rate_limit_wait()
                if is_forever:
                    response = orch_forever.place_forever_order(order_data)
                else:
                    response = orch_super.place_super_order(order_data)

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
                job['success_count'] += 1
            except ValueError as e:
                result['status'] = 'Failed'
                result['message'] = f'Validation error: {str(e)}'
                job['failed_count'] += 1
            except (DhanSuperOrderError, DhanForeverOrderError) as e:
                result['status'] = 'Failed'
                result['message'] = f'Order error: {str(e)}'
                job['failed_count'] += 1
            except Exception as e:
                result['status'] = 'Failed'
                result['message'] = f'Error: {str(e)}'
                job['failed_count'] += 1

            job['results'].append(result)

        if job['status'] != 'cancelled':
            job['status'] = 'completed'
            job['message'] = 'Completed'
        job['finished_at'] = datetime.now().isoformat()

    except Exception as e:  # pragma: no cover
        job['status'] = 'failed'
        job['message'] = 'Failed'
        job['error'] = str(e)
        job['finished_at'] = datetime.now().isoformat()



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
                'symbol': order_data['symbol'],
                'txn_type': order_data['txn_type'],
                'qty': order_data['qty'],
                'order_type': order_data['order_type'],
                'price': order_data['price'],
                'target_price': order_data['target_price'],
                'stop_loss_price': order_data['stop_loss_price'],
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
        # Check if file was uploaded
        if 'file' not in request.files:
            flash('No file uploaded.', 'error')
            return redirect(request.url)
        
        file = request.files['file']
        
        # Check if file is selected
        if file.filename == '':
            flash('No file selected.', 'error')
            return redirect(request.url)
        
        # Check if file type is allowed
        if not allowed_file(file.filename):
            flash('Invalid file type. Please upload Excel (.xlsx, .xls) or CSV file.', 'error')
            return redirect(request.url)
        
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
                with bulk_lock:
                    bulk_in_progress = False
                flash(f'Missing required columns: {", ".join(missing_columns)}', 'error')
                return render_template('bulk_upload.html', bulk_in_progress=False)

            # Start background job
            job_id = create_bulk_job(df, session['client_id'], session['access_token'])
            flash(f'Started bulk upload. Job ID: {job_id}', 'info')
            return redirect(url_for('bulk_status', job_id=job_id))
            error_details = traceback.format_exc()
            logger.error(f'ERROR in bulk upload: {error_details}')
            flash(f'Error processing file: {str(e)}', 'error')
            return redirect(request.url)
        finally:
            with bulk_lock:
                bulk_in_progress = False
    
    # GET request - show the upload form
    return render_template('bulk_upload.html', bulk_in_progress=False)


def _job_snapshot(job_id: str):
    with bulk_jobs_lock:
        job = bulk_jobs.get(job_id)
        if job is None:
            return None
        # Shallow copy excluding non-serializable items
        snap = {k: v for k, v in job.items() if k != 'cancel_event'}
        return snap


@app.route('/bulk-status/<job_id>', methods=['GET'])
@login_required
def bulk_status(job_id):
    job = _job_snapshot(job_id)
    if job is None:
        flash('Bulk job not found or expired.', 'error')
        return redirect(url_for('bulk_upload'))
    return render_template(
        'bulk_upload.html',
        bulk_in_progress=job['status'] in ['pending', 'running'],
        job=job,
        results=job.get('results', []),
        success_count=job.get('success_count', 0),
        failed_count=job.get('failed_count', 0)
    )


@app.route('/bulk-status/<job_id>/json', methods=['GET'])
@login_required
def bulk_status_json(job_id):
    job = _job_snapshot(job_id)
    if job is None:
        return jsonify({'error': 'not found'}), 404
    # Trim results in JSON to avoid huge payload; send last 20
    results = job.get('results', [])
    job['results'] = results[-20:]
    return jsonify(job)


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
    flash('Bulk upload cancellation requested. Pending orders will stop shortly.', 'info')
    return redirect(url_for('bulk_status', job_id=job_id))




if __name__ == '__main__':
    # Create necessary directories
    os.makedirs('templates', exist_ok=True)
    os.makedirs('static', exist_ok=True)
    os.makedirs('uploads', exist_ok=True)
    
    logger.info("=" * 60)
    logger.info("Dhan Super Order - Web Application")
    logger.info("=" * 60)
    logger.info("\n🚀 Starting server...")
    logger.info("📱 Open your browser and go to: http://localhost:5000")
    logger.info("\n⚠️  Press Ctrl+C to stop the server\n")
    logger.info("Logs being written to: dhan_app.log")
    
    app.run(debug=False, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))