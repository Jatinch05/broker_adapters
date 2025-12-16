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
from orchestrator.super_order import DhanSuperOrderOrchestrator, DhanSuperOrderError
from validator.instruments.dhan_store import DhanStore
from validator.instruments.dhan_refresher import refresh_dhan_instruments
from apis.dhan.auth import authenticate, DhanAuthError

app = Flask(__name__)
app.secret_key = os.urandom(24)  # Random secret key for sessions
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
app.config['UPLOAD_FOLDER'] = 'uploads'
ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'csv'}

# Store order history in memory (in production, use a database)
order_history = []


def allowed_file(filename):
    """Check if the uploaded file has an allowed extension"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


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
            # Get form data
            order_data = {
                'symbol': request.form.get('symbol', '').strip().upper(),
                'exchange': request.form.get('exchange', '').strip().upper(),
                'txn_type': request.form.get('txn_type', '').strip().upper(),
                'qty': int(request.form.get('qty', 0)),
                'order_type': request.form.get('order_type', '').strip().upper(),
                'price': float(request.form.get('price', 0)) if request.form.get('price') else None,
                'product': request.form.get('product', '').strip().upper(),
                'target_price': float(request.form.get('target_price', 0)),
                'stop_loss_price': float(request.form.get('stop_loss_price', 0)),
                'trailing_jump': float(request.form.get('trailing_jump', 0)),
                'order_category': 'SUPER',
            }
            
            # Add tag if provided
            tag = request.form.get('tag', '').strip()
            if tag:
                order_data['tag'] = tag
            
            # Create orchestrator
            orchestrator = DhanSuperOrderOrchestrator(
                client_id=session['client_id'],
                access_token=session['access_token']
            )
            
            # Place order
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
            
            flash(f'✅ Order placed successfully! Order ID: {result["orderId"]}', 'success')
            return redirect(url_for('order_history'))
            
        except ValueError as e:
            flash(f'Invalid input: {str(e)}', 'error')
        except DhanSuperOrderError as e:
            flash(f'Order failed: {str(e)}', 'error')
        except Exception as e:
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
                return render_template('bulk_upload.html')
            
            # Process each row
            results = []
            orchestrator = DhanSuperOrderOrchestrator(
                client_id=session['client_id'],
                access_token=session['access_token']
            )
            
            for index, row in df.iterrows():
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
                        results.append(result)
                        continue
                    
                    # Build order data
                    order_data = {
                        'symbol': str(row['Symbol']).strip().upper(),
                        'exchange': str(row['Exchange']).strip().upper(),
                        'transaction_type': str(row['TransactionType']).strip().upper(),
                        'quantity': int(row['Quantity']),
                        'order_type': str(row['OrderType']).strip().upper(),
                        'product_type': str(row['ProductType']).strip().upper()
                    }
                    
                    # Add optional fields if present and not NaN
                    if 'Price' in row and not pd.isna(row['Price']) and row['Price'] != '':
                        order_data['price'] = float(row['Price'])
                    
                    if 'TargetPrice' in row and not pd.isna(row['TargetPrice']) and row['TargetPrice'] != '':
                        order_data['target_price'] = float(row['TargetPrice'])
                    
                    if 'StopLoss' in row and not pd.isna(row['StopLoss']) and row['StopLoss'] != '':
                        order_data['stop_loss'] = float(row['StopLoss'])
                    
                    if 'TrailingStopLoss' in row and not pd.isna(row['TrailingStopLoss']) and row['TrailingStopLoss'] != '':
                        order_data['trailing_stop_loss'] = float(row['TrailingStopLoss'])
                    
                    if 'Tag' in row and not pd.isna(row['Tag']) and row['Tag'] != '':
                        order_data['tag'] = str(row['Tag']).strip()
                    
                    # Place the order
                    response = orchestrator.place_super_order(**order_data)
                    
                    # Store in history
                    order_record = {
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'symbol': order_data['symbol'],
                        'exchange': order_data['exchange'],
                        'type': order_data['transaction_type'],
                        'quantity': order_data['quantity'],
                        'order_type': order_data['order_type'],
                        'price': order_data.get('price', 'MARKET'),
                        'product': order_data['product_type'],
                        'target': order_data.get('target_price', 'N/A'),
                        'stop_loss': order_data.get('stop_loss', 'N/A'),
                        'trail_sl': order_data.get('trailing_stop_loss', 'N/A'),
                        'tag': order_data.get('tag', ''),
                        'order_id': response.get('orderId', 'N/A'),
                        'status': 'Success'
                    }
                    order_history.append(order_record)
                    
                    result['status'] = 'Success'
                    result['message'] = 'Order placed successfully'
                    result['order_id'] = response.get('orderId', 'N/A')
                    
                except ValueError as e:
                    result['status'] = 'Failed'
                    result['message'] = f'Validation error: {str(e)}'
                except DhanSuperOrderError as e:
                    result['status'] = 'Failed'
                    result['message'] = f'Order error: {str(e)}'
                except Exception as e:
                    result['status'] = 'Failed'
                    result['message'] = f'Error: {str(e)}'
                
                results.append(result)
            
            # Calculate statistics
            success_count = sum(1 for r in results if r['status'] == 'Success')
            failed_count = sum(1 for r in results if r['status'] == 'Failed')
            
            flash(f'Processed {len(results)} orders: {success_count} successful, {failed_count} failed.', 
                  'success' if failed_count == 0 else 'warning')
            
            return render_template('bulk_upload.html', results=results, 
                                 success_count=success_count, failed_count=failed_count)
            
        except Exception as e:
            flash(f'Error processing file: {str(e)}', 'error')
            return redirect(request.url)
    
    # GET request - show the upload form
    return render_template('bulk_upload.html')


if __name__ == '__main__':
    # Create necessary directories
    os.makedirs('templates', exist_ok=True)
    os.makedirs('static', exist_ok=True)
    os.makedirs('uploads', exist_ok=True)
    
    print("=" * 60)
    print("Dhan Super Order - Web Application")
    print("=" * 60)
    print("\n🚀 Starting server...")
    print("📱 Open your browser and go to: http://localhost:5000")
    print("\n⚠️  Press Ctrl+C to stop the server\n")
    
    app.run(debug=False, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))