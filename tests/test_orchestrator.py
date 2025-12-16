"""
Tests for Super Order Orchestrator
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from orchestrator.super_order import DhanSuperOrderOrchestrator, DhanSuperOrderError
from validator.instruments.dhan_instrument import DhanInstrument


class TestDhanSuperOrderOrchestrator:
    """Test the super order orchestrator"""

    @pytest.fixture
    def orchestrator(self):
        """Create orchestrator instance"""
        return DhanSuperOrderOrchestrator(
            client_id="1000000003",
            access_token="test_token"
        )

    @pytest.fixture
    def mock_instrument(self):
        """Create mock instrument"""
        mock_row = {
            "SEM_SM_SYMBOL": "HDFCBANK",
            "SEM_SM_SECURITY_ID": "1333",
            "SEM_EXM_EXCHANGE_CODE": "NSE_EQ",
            "SEM_SM_LOT_SIZE": 1,
            "SEM_SM_EXPIRY_DATE": None,
            "SEM_SM_INSTRUMENT_TYPE": "EQUITY"
        }
        return DhanInstrument(mock_row)

    def test_successful_buy_limit_super_order(self, orchestrator, mock_instrument):
        """Test successful BUY LIMIT super order placement"""
        order_data = {
            "symbol": "HDFCBANK",
            "exchange": "NSE",
            "txn_type": "BUY",
            "qty": 1,
            "order_type": "LIMIT",
            "price": 1500.0,
            "product": "CNC",
            "target_price": 1600.0,
            "stop_loss_price": 1400.0,
            "trailing_jump": 10.0,
            "order_category": "SUPER",
        }

        with patch('orchestrator.super_order.DhanStore') as mock_store, \
             patch('orchestrator.super_order.place_dhan_super_order') as mock_place:
            
            # Setup mocks
            mock_store.load.return_value = mock_store
            mock_store.lookup_symbol.return_value = mock_instrument
            mock_place.return_value = {
                "orderId": "112111182198",
                "orderStatus": "PENDING"
            }

            # Place order
            result = orchestrator.place_super_order(order_data)

            # Verify
            assert result["orderId"] == "112111182198"
            assert result["orderStatus"] == "PENDING"
            
            # Verify place_dhan_super_order was called correctly
            mock_place.assert_called_once()
            call_kwargs = mock_place.call_args[1]
            assert call_kwargs["security_id"] == "1333"
            assert call_kwargs["exchange_segment"] == "NSE_EQ"
            assert call_kwargs["client_id"] == "1000000003"
            assert call_kwargs["access_token"] == "test_token"

    def test_symbol_not_found(self, orchestrator):
        """Test error when symbol is not found"""
        order_data = {
            "symbol": "INVALID",
            "exchange": "NSE",
            "txn_type": "BUY",
            "qty": 1,
            "order_type": "LIMIT",
            "price": 1500.0,
            "product": "CNC",
            "target_price": 1600.0,
            "stop_loss_price": 1400.0,
            "trailing_jump": 10.0,
            "order_category": "SUPER",
        }

        with patch('orchestrator.super_order.DhanStore') as mock_store:
            mock_store.load.return_value = mock_store
            mock_store.lookup_symbol.return_value = None

            with pytest.raises(DhanSuperOrderError) as exc:
                orchestrator.place_super_order(order_data)
            
            assert "not found" in str(exc.value)

    def test_exchange_mismatch(self, orchestrator):
        """Test error when exchange doesn't match instrument"""
        order_data = {
            "symbol": "HDFCBANK",
            "exchange": "BSE",  # Wrong exchange
            "txn_type": "BUY",
            "qty": 1,
            "order_type": "LIMIT",
            "price": 1500.0,
            "product": "CNC",
            "target_price": 1600.0,
            "stop_loss_price": 1400.0,
            "trailing_jump": 10.0,
            "order_category": "SUPER",
        }

        mock_row = {
            "SEM_SM_SYMBOL": "HDFCBANK",
            "SEM_SM_SECURITY_ID": "1333",
            "SEM_EXM_EXCHANGE_CODE": "NSE_EQ",  # NSE, not BSE
            "SEM_SM_LOT_SIZE": 1,
            "SEM_SM_EXPIRY_DATE": None,
            "SEM_SM_INSTRUMENT_TYPE": "EQUITY"
        }
        mock_instrument = DhanInstrument(mock_row)

        with patch('orchestrator.super_order.DhanStore') as mock_store:
            mock_store.load.return_value = mock_store
            mock_store.lookup_symbol.return_value = mock_instrument

            with pytest.raises(DhanSuperOrderError) as exc:
                orchestrator.place_super_order(order_data)
            
            assert "mismatch" in str(exc.value).lower()

    def test_lot_size_validation_derivative(self, orchestrator):
        """Test lot size validation for derivatives"""
        order_data = {
            "symbol": "NIFTY27FEB2025FUT",
            "exchange": "NFO",
            "txn_type": "BUY",
            "qty": 25,  # Not a multiple of 50
            "order_type": "LIMIT",
            "price": 23500.0,
            "product": "MARGIN",
            "target_price": 23600.0,
            "stop_loss_price": 23400.0,
            "trailing_jump": 10.0,
            "order_category": "SUPER",
        }

        mock_row = {
            "SEM_SM_SYMBOL": "NIFTY27FEB2025FUT",
            "SEM_SM_SECURITY_ID": "12345",
            "SEM_EXM_EXCHANGE_CODE": "NSE_FNO",
            "SEM_SM_LOT_SIZE": 50,
            "SEM_SM_EXPIRY_DATE": "2025-02-27",
            "SEM_SM_INSTRUMENT_TYPE": "FUTIDX"
        }
        mock_instrument = DhanInstrument(mock_row)

        with patch('orchestrator.super_order.DhanStore') as mock_store:
            mock_store.load.return_value = mock_store
            mock_store.lookup_symbol.return_value = mock_instrument

            with pytest.raises(DhanSuperOrderError) as exc:
                orchestrator.place_super_order(order_data)
            
            assert "lot size" in str(exc.value).lower()

    def test_sell_market_order(self, orchestrator, mock_instrument):
        """Test SELL MARKET super order"""
        order_data = {
            "symbol": "HDFCBANK",
            "exchange": "NSE",
            "txn_type": "SELL",
            "qty": 1,
            "order_type": "MARKET",
            "price": None,
            "product": "INTRADAY",
            "target_price": 1400.0,
            "stop_loss_price": 1600.0,
            "trailing_jump": 0.0,
            "order_category": "SUPER",
        }

        with patch('orchestrator.super_order.DhanStore') as mock_store, \
             patch('orchestrator.super_order.place_dhan_super_order') as mock_place:
            
            mock_store.load.return_value = mock_store
            mock_store.lookup_symbol.return_value = mock_instrument
            mock_place.return_value = {
                "orderId": "123456",
                "orderStatus": "PENDING"
            }

            result = orchestrator.place_super_order(order_data)

            assert result["orderId"] == "123456"
            mock_place.assert_called_once()

    def test_invalid_exchange(self, orchestrator):
        """Test unsupported exchange"""
        order_data = {
            "symbol": "TEST",
            "exchange": "INVALID",  # Unsupported exchange
            "txn_type": "BUY",
            "qty": 1,
            "order_type": "LIMIT",
            "price": 1500.0,
            "product": "CNC",
            "target_price": 1600.0,
            "stop_loss_price": 1400.0,
            "trailing_jump": 10.0,
            "order_category": "SUPER",
        }

        with patch('orchestrator.super_order.DhanStore') as mock_store:
            mock_store.load.return_value = mock_store
            mock_store.lookup_symbol.return_value = Mock()

            with pytest.raises(DhanSuperOrderError) as exc:
                orchestrator.place_super_order(order_data)
            
            assert "not supported" in str(exc.value)

    def test_instruments_loaded_once(self, orchestrator, mock_instrument):
        """Test that instruments are loaded only once"""
        order_data = {
            "symbol": "HDFCBANK",
            "exchange": "NSE",
            "txn_type": "BUY",
            "qty": 1,
            "order_type": "LIMIT",
            "price": 1500.0,
            "product": "CNC",
            "target_price": 1600.0,
            "stop_loss_price": 1400.0,
            "trailing_jump": 10.0,
            "order_category": "SUPER",
        }

        with patch('orchestrator.super_order.DhanStore') as mock_store, \
             patch('orchestrator.super_order.place_dhan_super_order') as mock_place:
            
            mock_store.load.return_value = mock_store
            mock_store.lookup_symbol.return_value = mock_instrument
            mock_place.return_value = {"orderId": "123", "orderStatus": "PENDING"}

            # Place two orders
            orchestrator.place_super_order(order_data)
            orchestrator.place_super_order(order_data)

            # Load should be called only once
            assert mock_store.load.call_count == 1
