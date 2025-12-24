"""
Tests for Dhan Super Order implementation
"""
import pytest
from unittest.mock import Mock, patch
from adapters.dhan.super_order import place_dhan_super_order
from adapters.dhan.errors import DhanSuperOrderError


class TestPlaceDhanSuperOrder:
    """Test super order API calls"""

    def test_successful_super_order_placement(self):
        """Test successful super order placement"""
        # Mock intent
        mock_intent = Mock()
        mock_intent.txn_type = "BUY"
        mock_intent.qty = 10
        mock_intent.order_type = "LIMIT"
        mock_intent.product = "CNC"
        mock_intent.target_price = 1600
        mock_intent.stop_loss_price = 1400
        mock_intent.trailing_jump = 10
        mock_intent.price = 1500
        mock_intent.tag = "test123"

        # Mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "orderId": "112111182198",
            "orderStatus": "PENDING"
        }

        mock_session = Mock()
        mock_session.post.return_value = mock_response

        with patch('adapters.dhan.super_order.get_session', return_value=mock_session) as _:
            result = place_dhan_super_order(
                intent=mock_intent,
                security_id="1333",
                exchange_segment="NSE_EQ",
                client_id="1000000003",
                access_token="test_token"
            )

            # Verify request was made correctly
            mock_session.post.assert_called_once()
            
            # Get positional and keyword arguments
            args, kwargs = mock_session.post.call_args

            # Check URL (first positional argument)
            assert args[0] == "https://api.dhan.co/v2/super/orders"

            # Check headers - should only have access-token
            assert kwargs['headers']['Content-Type'] == 'application/json'
            assert kwargs['headers']['access-token'] == 'test_token'
            assert 'client-id' not in kwargs['headers']

            # Check payload
            payload = kwargs['json']
            assert payload['dhanClientId'] == "1000000003"
            assert payload['securityId'] == "1333"
            assert payload['exchangeSegment'] == "NSE_EQ"
            assert payload['transactionType'] == "BUY"
            assert payload['quantity'] == 10
            assert payload['orderType'] == "LIMIT"
            assert payload['productType'] == "CNC"
            assert payload['targetPrice'] == 1600
            assert payload['stopLossPrice'] == 1400
            assert payload['trailingJump'] == 10
            assert payload['price'] == 1500
            assert payload['correlationId'] == "test123"

            # Verify response
            assert result['orderId'] == "112111182198"
            assert result['orderStatus'] == "PENDING"

    def test_super_order_market_order(self):
        """Test super order with MARKET order type (no price)"""
        mock_intent = Mock()
        mock_intent.txn_type = "SELL"
        mock_intent.qty = 5
        mock_intent.order_type = "MARKET"
        mock_intent.product = "INTRADAY"
        mock_intent.target_price = 1400
        mock_intent.stop_loss_price = 1600
        mock_intent.trailing_jump = 5
        mock_intent.price = None
        mock_intent.tag = None

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"orderId": "123", "orderStatus": "PENDING"}

        mock_session = Mock()
        mock_session.post.return_value = mock_response

        with patch('adapters.dhan.super_order.get_session', return_value=mock_session) as _:
            place_dhan_super_order(
                intent=mock_intent,
                security_id="11536",
                exchange_segment="NSE_EQ",
                client_id="1000000003",
                access_token="test_token"
            )

            args, kwargs = mock_session.post.call_args
            payload = kwargs['json']
            # Price should not be in payload for MARKET orders
            assert 'price' not in payload
            # correlationId should not be in payload if tag is None
            assert 'correlationId' not in payload

    def test_super_order_http_error(self):
        """Test handling of HTTP error responses"""
        mock_intent = Mock()
        mock_intent.txn_type = "BUY"
        mock_intent.qty = 10
        mock_intent.order_type = "LIMIT"
        mock_intent.product = "CNC"
        mock_intent.target_price = 1600
        mock_intent.stop_loss_price = 1400
        mock_intent.trailing_jump = 10
        mock_intent.price = 1500
        mock_intent.tag = None

        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.text = "Bad Request"

        mock_session = Mock()
        mock_session.post.return_value = mock_response

        with patch('adapters.dhan.super_order.get_session', return_value=mock_session):
            with pytest.raises(DhanSuperOrderError) as exc:
                place_dhan_super_order(
                    intent=mock_intent,
                    security_id="1333",
                    exchange_segment="NSE_EQ",
                    client_id="1000000003",
                    access_token="test_token"
                )
            assert "HTTP 400" in str(exc.value)

    def test_super_order_broker_rejection(self):
        """Test handling of broker rejection (error response)"""
        mock_intent = Mock()
        mock_intent.txn_type = "BUY"
        mock_intent.qty = 10
        mock_intent.order_type = "LIMIT"
        mock_intent.product = "CNC"
        mock_intent.target_price = 1600
        mock_intent.stop_loss_price = 1400
        mock_intent.trailing_jump = 10
        mock_intent.price = 1500
        mock_intent.tag = None

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "errorType": "VALIDATION_ERROR",
            "errorCode": "ERR001",
            "errorMessage": "Insufficient funds"
        }

        mock_session = Mock()
        mock_session.post.return_value = mock_response

        with patch('adapters.dhan.super_order.get_session', return_value=mock_session):
            with pytest.raises(DhanSuperOrderError) as exc:
                place_dhan_super_order(
                    intent=mock_intent,
                    security_id="1333",
                    exchange_segment="NSE_EQ",
                    client_id="1000000003",
                    access_token="test_token"
                )
            assert "Insufficient funds" in str(exc.value)

    def test_super_order_rejected_status(self):
        """Test handling of REJECTED order status"""
        mock_intent = Mock()
        mock_intent.txn_type = "BUY"
        mock_intent.qty = 10
        mock_intent.order_type = "LIMIT"
        mock_intent.product = "CNC"
        mock_intent.target_price = 1600
        mock_intent.stop_loss_price = 1400
        mock_intent.trailing_jump = 10
        mock_intent.price = 1500
        mock_intent.tag = None

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "orderId": "112111182198",
            "orderStatus": "REJECTED"
        }

        mock_session = Mock()
        mock_session.post.return_value = mock_response

        with patch('adapters.dhan.super_order.get_session', return_value=mock_session):
            with pytest.raises(DhanSuperOrderError) as exc:
                place_dhan_super_order(
                    intent=mock_intent,
                    security_id="1333",
                    exchange_segment="NSE_EQ",
                    client_id="1000000003",
                    access_token="test_token"
                )
            assert "rejected" in str(exc.value).lower()
