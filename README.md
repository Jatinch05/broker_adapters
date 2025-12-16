# Dhan Super Order - Broker Adapter

A Python framework for placing **Dhan Super Orders** with comprehensive validation and error handling.

## Features

✅ **Complete Super Order Support**
- Entry, Target, and Stop Loss legs in a single order
- Trailing stop loss support
- MARKET and LIMIT order types
- Multi-segment support (NSE, BSE, NFO, BFO, MCX)

✅ **Robust Validation**
- Three-layer validation (base → broker → order-type)
- Price relationship validation (BUY/SELL logic)
- Lot size validation for derivatives
- Instrument verification against Dhan's master data

✅ **Production Ready**
- Comprehensive error handling
- Type hints throughout
- 24 passing unit tests
- API v2 compliant

## Quick Start

### 1. Installation

```bash
# Clone or download this repository
cd broker_adapters

# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows:
.\venv\Scripts\Activate.ps1
# Linux/Mac:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configuration

Create a `.env` file with your Dhan credentials:

```bash
cp .env.example .env
# Edit .env and add your credentials
```

Get your credentials from [Dhan HQ API Portal](https://dhanhq.co/).

### 3. Run

```python
python main.py
```

## Usage Examples

### Example 1: BUY LIMIT Super Order

```python
from orchestrator.super_order import DhanSuperOrderOrchestrator

orchestrator = DhanSuperOrderOrchestrator(
    client_id="your_client_id",
    access_token="your_access_token"
)

order = {
    "symbol": "HDFCBANK",
    "exchange": "NSE",
    "txn_type": "BUY",
    "qty": 1,
    "order_type": "LIMIT",
    "price": 1500.0,
    "product": "CNC",
    "target_price": 1600.0,      # Take profit at ₹1600
    "stop_loss_price": 1400.0,   # Stop loss at ₹1400
    "trailing_jump": 10.0,       # Trail SL by ₹10
    "order_category": "SUPER",
}

result = orchestrator.place_super_order(order)
print(f"Order ID: {result['orderId']}")
```

### Example 2: SELL MARKET Super Order

```python
order = {
    "symbol": "RELIANCE",
    "exchange": "NSE",
    "txn_type": "SELL",
    "qty": 1,
    "order_type": "MARKET",
    "price": None,  # No price for MARKET
    "product": "INTRADAY",
    "target_price": 2400.0,
    "stop_loss_price": 2600.0,
    "trailing_jump": 0.0,  # No trailing
    "order_category": "SUPER",
}

result = orchestrator.place_super_order(order)
```

### Example 3: Futures Super Order

```python
order = {
    "symbol": "NIFTY 27 FEB 2025 FUT",
    "exchange": "NFO",
    "txn_type": "BUY",
    "qty": 50,  # Must be multiple of lot size
    "order_type": "LIMIT",
    "price": 23500.0,
    "product": "MARGIN",
    "target_price": 23600.0,
    "stop_loss_price": 23400.0,
    "trailing_jump": 20.0,
    "order_category": "SUPER",
}

result = orchestrator.place_super_order(order)
```

## Project Structure

```
broker_adapters/
├── main.py                      # Entry point with examples
├── orchestrator/
│   └── super_order.py          # Complete order placement flow
├── adapters/
│   ├── base.py                 # Abstract base adapter
│   └── dhan/
│       ├── client.py           # Dhan adapter (SDK-backed)
│       ├── super_order.py      # Super order API integration
│       └── errors.py           # Custom exceptions
├── apis/
│   └── dhan/
│       └── auth.py             # Authentication
├── validator/
│   ├── base_validation.py      # Broker-agnostic validation
│   ├── dhan_validator.py       # Dhan-specific validation
│   ├── dhan_super_validator.py # Super order validation
│   └── instruments/
│       ├── dhan_store.py       # Instrument data cache
│       ├── dhan_instrument.py  # Instrument wrapper
│       └── dhan_refresher.py   # Download instrument master
└── tests/                      # Comprehensive test suite
```

## Validation Rules

### Price Relationships

**For BUY orders:**
```
stop_loss_price < price < target_price
```

**For SELL orders:**
```
target_price < price < stop_loss_price
```

### Order Types

- **MARKET**: price must be None
- **LIMIT**: price is required

### Product Types

Supported: `CNC`, `INTRADAY`, `MARGIN`, `MTF`

### Exchange Mapping

| User Input | Dhan Segment |
|------------|--------------|
| NSE        | NSE_EQ       |
| BSE        | BSE_EQ       |
| NFO        | NSE_FNO      |
| BFO        | BSE_FNO      |
| MCX        | MCX          |

## Testing

Run the test suite:

```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test file
python -m pytest tests/test_super_order.py -v

# Run with coverage
python -m pytest tests/ --cov=. --cov-report=html
```

**Test Results**: 24/24 passing ✅

## API Documentation

This implementation follows [Dhan's official API v2 documentation](https://dhanhq.co/docs/v2/super-order/).

### Super Order Endpoint

```
POST https://api.dhan.co/v2/super/orders
```

**Headers:**
```json
{
    "Content-Type": "application/json",
    "access-token": "YOUR_ACCESS_TOKEN"
}
```

**Request Body:**
```json
{
    "dhanClientId": "1000000003",
    "securityId": "1333",
    "exchangeSegment": "NSE_EQ",
    "transactionType": "BUY",
    "quantity": 10,
    "orderType": "LIMIT",
    "price": 1500,
    "productType": "CNC",
    "targetPrice": 1600,
    "stopLossPrice": 1400,
    "trailingJump": 10,
    "correlationId": "optional_tag"
}
```

**Response:**
```json
{
    "orderId": "112111182198",
    "orderStatus": "PENDING"
}
```

## Error Handling

The framework handles multiple error scenarios:

- ❌ Invalid credentials
- ❌ Validation errors (price relationships, lot sizes, etc.)
- ❌ Symbol not found
- ❌ Exchange mismatches
- ❌ HTTP errors
- ❌ Broker rejections
- ❌ Network timeouts

All errors are wrapped in `DhanSuperOrderError` with descriptive messages.

## Requirements

```
Python 3.12+
pydantic==2.12.5
pandas==2.3.3
requests==2.32.5
python-dotenv==1.0.1
dhanhq (official SDK)
pytest (for testing)
```

## Roadmap

- [x] Super Order placement
- [x] Super Order validation
- [x] Instrument management
- [x] Authentication
- [x] Comprehensive testing
- [ ] Normal order placement
- [ ] Order modification
- [ ] Order cancellation
- [ ] WebSocket integration
- [ ] Portfolio management
- [ ] Historical data

## Contributing

Contributions are welcome! Please ensure:

1. All tests pass
2. New features include tests
3. Code follows existing patterns
4. Documentation is updated

## License

MIT License

## Support

For Dhan API issues: [Dhan Support](https://dhanhq.co/)

For this adapter: Create an issue in the repository

## Disclaimer

This is an unofficial adapter. Use at your own risk. Always test with small quantities first.

Trading and investing involve risks. Past performance is not indicative of future results.
