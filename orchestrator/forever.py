"""
Forever Order Orchestrator for Dhan

Flow mirrors Super Order orchestrator but targets the Forever endpoint.
1. Validate forever order intent
2. Resolve instrument details
3. Exchange and lot validations
4. Place forever order (fire-and-forget)
"""
from typing import Dict, Any
from validator.dhan_forever_validator import DhanForeverOrderIntent
from validator.instruments.dhan_store import DhanStore
from adapters.dhan.forever import place_dhan_forever_order


class DhanForeverOrderError(Exception):
    pass


class DhanForeverOrderOrchestrator:
    def __init__(self, client_id: str, access_token: str):
        self.client_id = client_id
        self.access_token = access_token
        self._instruments_loaded = False

    def ensure_instruments_loaded(self):
        if not self._instruments_loaded:
            DhanStore.load()
            self._instruments_loaded = True

    def place_forever_order(self, order_data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            self.ensure_instruments_loaded()
            intent = DhanForeverOrderIntent(**order_data)

            # Resolve instrument (support advanced fields for derivatives)
            strike_price = order_data.get('strike_price')
            expiry_date = order_data.get('expiry_date')
            option_type = order_data.get('option_type')

            instrument = None
            if strike_price is not None or expiry_date is not None or option_type is not None:
                instrument = DhanStore.lookup_by_details(
                    symbol=intent.symbol,
                    strike_price=strike_price,
                    expiry_date=expiry_date,
                    option_type=option_type,
                )
            else:
                instrument = DhanStore.lookup_symbol(intent.symbol)

            if instrument is None:
                raise DhanForeverOrderError(
                    f"Symbol '{intent.symbol}' not found in Dhan instruments"
                )

            # Exchange mapping for validation (instrument EXCH_ID), and API exchangeSegment mapping
            base_validation_mapping = {
                "NSE": "NSE",
                "BSE": "BSE",
                "NFO": "NSE",
                "NSE_FNO": "NSE",
                "BFO": "BSE",
                "BSE_FNO": "BSE",
                "MCX": "MCX",
            }
            api_segment_mapping = {
                "NSE": "NSE_EQ",
                "BSE": "BSE_EQ",
                "NFO": "NSE_FNO",
                "NSE_FNO": "NSE_FNO",
                "BFO": "BSE_FNO",
                "BSE_FNO": "BSE_FNO",
            }

            expected_segment = base_validation_mapping.get(intent.exchange)
            if expected_segment is None:
                raise DhanForeverOrderError(
                    f"Exchange '{intent.exchange}' not supported"
                )

            if instrument.exchange_segment != expected_segment:
                raise DhanForeverOrderError(
                    f"Exchange mismatch for {intent.symbol}: Expected {expected_segment}, got {instrument.exchange_segment}"
                )

            # Lot-size validation for derivatives
            if instrument.is_derivative and (intent.qty % instrument.lot_size != 0):
                raise DhanForeverOrderError(
                    f"Quantity {intent.qty} must be a multiple of lot size {instrument.lot_size}"
                )

            # Place forever order to Dhan endpoint
            api_exchange_segment = api_segment_mapping.get(intent.exchange, intent.exchange)

            result = place_dhan_forever_order(
                intent=intent,
                security_id=instrument.security_id,
                exchange_segment=api_exchange_segment,
                client_id=self.client_id,
                access_token=self.access_token,
            )
            return result
        except Exception as e:
            if isinstance(e, DhanForeverOrderError):
                raise
            raise DhanForeverOrderError(f"Forever order placement failed: {e}") from e
