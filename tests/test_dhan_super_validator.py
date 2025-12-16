"""
Tests for Dhan Super Order Validator
"""
import pytest
import pandas as pd
from validator.dhan_super_validator import DhanSuperOrderIntent, validate_super_orders_df
from pydantic import ValidationError


class TestDhanSuperOrderIntent:
    """Test Dhan Super Order Intent validation"""

    def test_valid_buy_limit_super_order(self):
        """Test valid BUY LIMIT super order"""
        data = {
            "symbol": "HDFCBANK",
            "exchange": "NSE",
            "txn_type": "BUY",
            "qty": 10,
            "order_type": "LIMIT",
            "price": 1500,
            "product": "CNC",
            "target_price": 1600,
            "stop_loss_price": 1400,
            "trailing_jump": 10,
            "order_category": "SUPER"
        }
        intent = DhanSuperOrderIntent(**data)
        assert intent.symbol == "HDFCBANK"
        assert intent.txn_type == "BUY"
        assert intent.price == 1500
        assert intent.target_price == 1600
        assert intent.stop_loss_price == 1400

    def test_valid_sell_limit_super_order(self):
        """Test valid SELL LIMIT super order"""
        data = {
            "symbol": "RELIANCE",
            "exchange": "NSE",
            "txn_type": "SELL",
            "qty": 5,
            "order_type": "LIMIT",
            "price": 2500,
            "product": "INTRADAY",
            "target_price": 2400,  # For SELL: target < price
            "stop_loss_price": 2600,  # For SELL: sl > price
            "trailing_jump": 5,
            "order_category": "SUPER"
        }
        intent = DhanSuperOrderIntent(**data)
        assert intent.txn_type == "SELL"
        assert intent.target_price == 2400
        assert intent.stop_loss_price == 2600

    def test_valid_market_super_order(self):
        """Test valid MARKET super order"""
        data = {
            "symbol": "TCS",
            "exchange": "NSE",
            "txn_type": "BUY",
            "qty": 1,
            "order_type": "MARKET",
            "price": None,  # No price for MARKET
            "product": "MARGIN",
            "target_price": 3500,
            "stop_loss_price": 3300,
            "trailing_jump": 0,
            "order_category": "SUPER"
        }
        intent = DhanSuperOrderIntent(**data)
        assert intent.order_type == "MARKET"
        assert intent.price is None

    def test_invalid_txn_type(self):
        """Test invalid transaction type"""
        data = {
            "symbol": "HDFCBANK",
            "exchange": "NSE",
            "txn_type": "HOLD",  # Invalid
            "qty": 10,
            "order_type": "LIMIT",
            "price": 1500,
            "product": "CNC",
            "target_price": 1600,
            "stop_loss_price": 1400,
            "trailing_jump": 10,
            "order_category": "SUPER"
        }
        with pytest.raises(ValidationError) as exc:
            DhanSuperOrderIntent(**data)
        assert "must be BUY or SELL" in str(exc.value)

    def test_invalid_order_type(self):
        """Test invalid order type"""
        data = {
            "symbol": "HDFCBANK",
            "exchange": "NSE",
            "txn_type": "BUY",
            "qty": 10,
            "order_type": "STOP",  # Not supported for Super Orders
            "price": 1500,
            "product": "CNC",
            "target_price": 1600,
            "stop_loss_price": 1400,
            "trailing_jump": 10,
            "order_category": "SUPER"
        }
        with pytest.raises(ValidationError) as exc:
            DhanSuperOrderIntent(**data)
        assert "MARKET or LIMIT" in str(exc.value)

    def test_invalid_product(self):
        """Test invalid product type"""
        data = {
            "symbol": "HDFCBANK",
            "exchange": "NSE",
            "txn_type": "BUY",
            "qty": 10,
            "order_type": "LIMIT",
            "price": 1500,
            "product": "BO",  # Invalid product
            "target_price": 1600,
            "stop_loss_price": 1400,
            "trailing_jump": 10,
            "order_category": "SUPER"
        }
        with pytest.raises(ValidationError) as exc:
            DhanSuperOrderIntent(**data)
        assert "INTRADAY, CNC, MARGIN, MTF" in str(exc.value)

    def test_negative_qty(self):
        """Test negative quantity"""
        data = {
            "symbol": "HDFCBANK",
            "exchange": "NSE",
            "txn_type": "BUY",
            "qty": -10,  # Invalid
            "order_type": "LIMIT",
            "price": 1500,
            "product": "CNC",
            "target_price": 1600,
            "stop_loss_price": 1400,
            "trailing_jump": 10,
            "order_category": "SUPER"
        }
        with pytest.raises(ValidationError) as exc:
            DhanSuperOrderIntent(**data)
        assert "qty must be > 0" in str(exc.value)

    def test_limit_order_without_price(self):
        """Test LIMIT order without price"""
        data = {
            "symbol": "HDFCBANK",
            "exchange": "NSE",
            "txn_type": "BUY",
            "qty": 10,
            "order_type": "LIMIT",
            "price": None,  # Required for LIMIT
            "product": "CNC",
            "target_price": 1600,
            "stop_loss_price": 1400,
            "trailing_jump": 10,
            "order_category": "SUPER"
        }
        with pytest.raises(ValidationError) as exc:
            DhanSuperOrderIntent(**data)
        assert "price is required" in str(exc.value)

    def test_market_order_with_price(self):
        """Test MARKET order with price (should fail)"""
        data = {
            "symbol": "HDFCBANK",
            "exchange": "NSE",
            "txn_type": "BUY",
            "qty": 10,
            "order_type": "MARKET",
            "price": 1500,  # Should be None for MARKET
            "product": "CNC",
            "target_price": 1600,
            "stop_loss_price": 1400,
            "trailing_jump": 10,
            "order_category": "SUPER"
        }
        with pytest.raises(ValidationError) as exc:
            DhanSuperOrderIntent(**data)
        assert "must be empty when order_type is MARKET" in str(exc.value)

    def test_invalid_price_relationship_buy(self):
        """Test invalid price relationship for BUY order"""
        data = {
            "symbol": "HDFCBANK",
            "exchange": "NSE",
            "txn_type": "BUY",
            "qty": 10,
            "order_type": "LIMIT",
            "price": 1500,
            "product": "CNC",
            "target_price": 1450,  # Should be > price for BUY
            "stop_loss_price": 1400,
            "trailing_jump": 10,
            "order_category": "SUPER"
        }
        with pytest.raises(ValidationError) as exc:
            DhanSuperOrderIntent(**data)
        assert "stop_loss_price < price < target_price" in str(exc.value)

    def test_invalid_price_relationship_sell(self):
        """Test invalid price relationship for SELL order"""
        data = {
            "symbol": "HDFCBANK",
            "exchange": "NSE",
            "txn_type": "SELL",
            "qty": 10,
            "order_type": "LIMIT",
            "price": 1500,
            "product": "CNC",
            "target_price": 1600,  # Should be < price for SELL
            "stop_loss_price": 1400,
            "trailing_jump": 10,
            "order_category": "SUPER"
        }
        with pytest.raises(ValidationError) as exc:
            DhanSuperOrderIntent(**data)
        assert "target_price < price < stop_loss_price" in str(exc.value)

    def test_negative_target_price(self):
        """Test negative target price"""
        data = {
            "symbol": "HDFCBANK",
            "exchange": "NSE",
            "txn_type": "BUY",
            "qty": 10,
            "order_type": "MARKET",
            "price": None,
            "product": "CNC",
            "target_price": -1600,  # Invalid
            "stop_loss_price": 1400,
            "trailing_jump": 10,
            "order_category": "SUPER"
        }
        with pytest.raises(ValidationError) as exc:
            DhanSuperOrderIntent(**data)
        assert "target_price must be > 0" in str(exc.value)

    def test_negative_trailing_jump(self):
        """Test negative trailing jump"""
        data = {
            "symbol": "HDFCBANK",
            "exchange": "NSE",
            "txn_type": "BUY",
            "qty": 10,
            "order_type": "MARKET",
            "price": None,
            "product": "CNC",
            "target_price": 1600,
            "stop_loss_price": 1400,
            "trailing_jump": -10,  # Invalid
            "order_category": "SUPER"
        }
        with pytest.raises(ValidationError) as exc:
            DhanSuperOrderIntent(**data)
        assert "trailing_jump must be >= 0" in str(exc.value)

    def test_wrong_order_category(self):
        """Test wrong order category"""
        data = {
            "symbol": "HDFCBANK",
            "exchange": "NSE",
            "txn_type": "BUY",
            "qty": 10,
            "order_type": "LIMIT",
            "price": 1500,
            "product": "CNC",
            "target_price": 1600,
            "stop_loss_price": 1400,
            "trailing_jump": 10,
            "order_category": "NORMAL"  # Should be SUPER
        }
        with pytest.raises(ValidationError) as exc:
            DhanSuperOrderIntent(**data)
        assert "must be SUPER" in str(exc.value)


class TestValidateSuperOrdersDF:
    """Test batch validation of super orders"""

    def test_valid_dataframe(self):
        """Test validation of valid dataframe"""
        df = pd.DataFrame([
            {
                "symbol": "HDFCBANK",
                "exchange": "NSE",
                "txn_type": "BUY",
                "qty": 10,
                "order_type": "LIMIT",
                "price": 1500,
                "product": "CNC",
                "target_price": 1600,
                "stop_loss_price": 1400,
                "trailing_jump": 10,
                "order_category": "SUPER"
            }
        ])

        intents, validated_df, errors = validate_super_orders_df(df)

        assert len(intents) == 1
        assert len(errors) == 0
        assert len(validated_df) == 1
        assert intents[0].symbol == "HDFCBANK"

    def test_dataframe_with_errors(self):
        """Test validation with some invalid rows"""
        df = pd.DataFrame([
            {
                "symbol": "HDFCBANK",
                "exchange": "NSE",
                "txn_type": "BUY",
                "qty": 10,
                "order_type": "LIMIT",
                "price": 1500,
                "product": "CNC",
                "target_price": 1600,
                "stop_loss_price": 1400,
                "trailing_jump": 10,
                "order_category": "SUPER"
            },
            {
                "symbol": "RELIANCE",
                "exchange": "NSE",
                "txn_type": "INVALID",  # Invalid txn_type
                "qty": 5,
                "order_type": "LIMIT",
                "price": 2500,
                "product": "CNC",
                "target_price": 2600,
                "stop_loss_price": 2400,
                "trailing_jump": 5,
                "order_category": "SUPER"
            }
        ])

        intents, validated_df, errors = validate_super_orders_df(df)

        assert len(intents) == 1
        assert len(errors) == 1
        assert len(validated_df) == 1
        assert errors[0][0] == 1  # Second row (index 1)
        assert "BUY or SELL" in errors[0][1]
