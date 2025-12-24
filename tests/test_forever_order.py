"""Tests for Dhan Forever Order implementation and validation."""

import pytest
from unittest.mock import Mock, patch

from adapters.dhan.forever import place_dhan_forever_order, DhanForeverOrderError
from validator.dhan_forever_validator import DhanForeverOrderIntent


class TestDhanForeverValidator:
    def test_product_must_be_cnc_or_mtf(self):
        with pytest.raises(Exception):
            DhanForeverOrderIntent(
                symbol="HDFCBANK",
                exchange="NSE",
                txn_type="BUY",
                qty=10,
                order_type="LIMIT",
                product="INTRADAY",
                price=1500.0,
                trigger_price=1499.0,
            )

    def test_order_flag_must_be_single_or_oco(self):
        with pytest.raises(Exception):
            DhanForeverOrderIntent(
                symbol="HDFCBANK",
                exchange="NSE",
                txn_type="BUY",
                qty=10,
                order_type="LIMIT",
                product="CNC",
                price=1500.0,
                trigger_price=1499.0,
                order_flag="NORMAL",
            )

    def test_disclosed_quantity_must_be_at_least_30_percent_if_set(self):
        # qty=10 => min disclosedQuantity is ceil(3.0)=3
        with pytest.raises(Exception):
            DhanForeverOrderIntent(
                symbol="HDFCBANK",
                exchange="NSE",
                txn_type="BUY",
                qty=10,
                order_type="LIMIT",
                product="CNC",
                price=1500.0,
                trigger_price=1499.0,
                disclosed_quantity=1,
            )

    def test_oco_requires_secondary_leg_fields(self):
        with pytest.raises(Exception):
            DhanForeverOrderIntent(
                symbol="HDFCBANK",
                exchange="NSE",
                txn_type="BUY",
                qty=10,
                order_type="LIMIT",
                product="CNC",
                price=1500.0,
                trigger_price=1499.0,
                order_flag="OCO",
            )


class TestPlaceDhanForeverOrder:
    def test_success_single_payload(self):
        mock_intent = Mock()
        mock_intent.order_flag = "SINGLE"
        mock_intent.txn_type = "BUY"
        mock_intent.qty = 5
        mock_intent.order_type = "LIMIT"
        mock_intent.product = "CNC"
        mock_intent.validity = "DAY"
        mock_intent.disclosed_quantity = 0
        mock_intent.price = 1428
        mock_intent.trigger_price = 1427
        mock_intent.tag = "abc"

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"orderId": "1", "orderStatus": "PENDING"}

        mock_session = Mock()
        mock_session.post.return_value = mock_response

        with patch('adapters.dhan.forever.get_session', return_value=mock_session):
            result = place_dhan_forever_order(
                intent=mock_intent,
                security_id="1333",
                exchange_segment="NSE_EQ",
                client_id="1000000132",
                access_token="token",
            )

        assert result["orderId"] == "1"

        args, kwargs = mock_session.post.call_args
        assert args[0] == "https://api.dhan.co/v2/forever/orders"
        payload = kwargs["json"]
        assert payload["dhanClientId"] == "1000000132"
        assert payload["correlationId"] == "abc"
        assert payload["orderFlag"] == "SINGLE"
        assert payload["exchangeSegment"] == "NSE_EQ"
        assert payload["securityId"] == "1333"
        assert payload["price"] == float(1428)
        assert payload["triggerPrice"] == float(1427)
        assert "price1" not in payload
        assert "triggerPrice1" not in payload
        assert "quantity1" not in payload

    def test_oco_includes_secondary_leg_fields(self):
        mock_intent = Mock()
        mock_intent.order_flag = "OCO"
        mock_intent.txn_type = "BUY"
        mock_intent.qty = 5
        mock_intent.order_type = "LIMIT"
        mock_intent.product = "CNC"
        mock_intent.validity = "DAY"
        mock_intent.disclosed_quantity = 0
        mock_intent.price = 1428
        mock_intent.trigger_price = 1427
        mock_intent.price1 = 1420
        mock_intent.trigger_price1 = 1419
        mock_intent.quantity1 = 10
        mock_intent.tag = None

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"orderId": "2", "orderStatus": "PENDING"}

        mock_session = Mock()
        mock_session.post.return_value = mock_response

        with patch('adapters.dhan.forever.get_session', return_value=mock_session):
            result = place_dhan_forever_order(
                intent=mock_intent,
                security_id="1333",
                exchange_segment="NSE_EQ",
                client_id="1000000132",
                access_token="token",
            )

        assert result["orderId"] == "2"
        payload = mock_session.post.call_args[1]["json"]
        assert payload["orderFlag"] == "OCO"
        assert payload["price1"] == float(1420)
        assert payload["triggerPrice1"] == float(1419)
        assert payload["quantity1"] == 10

    def test_http_error_raises(self):
        mock_intent = Mock()
        mock_intent.order_flag = "SINGLE"
        mock_intent.txn_type = "BUY"
        mock_intent.qty = 5
        mock_intent.order_type = "LIMIT"
        mock_intent.product = "CNC"
        mock_intent.validity = "DAY"
        mock_intent.disclosed_quantity = 0
        mock_intent.price = 1428
        mock_intent.trigger_price = 1427
        mock_intent.tag = None

        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.text = "Bad Request"

        mock_session = Mock()
        mock_session.post.return_value = mock_response

        with patch('adapters.dhan.forever.get_session', return_value=mock_session):
            with pytest.raises(DhanForeverOrderError):
                place_dhan_forever_order(
                    intent=mock_intent,
                    security_id="1333",
                    exchange_segment="NSE_EQ",
                    client_id="1000000132",
                    access_token="token",
                )
