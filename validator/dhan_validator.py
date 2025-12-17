from typing import Dict, Any, Tuple
from .base_validation import BaseValidator
from .instruments.dhan_store import DhanStore


class DhanValidationError(Exception):
    pass


class DhanValidator:

    @staticmethod
    def validate(row: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Returns:
            (normalized_base_dict, dhan_meta_dict)
        """

        base = BaseValidator.validate_row(row)
        base_dict = base.model_dump()

        symbol = base.symbol
        exchange = base.exchange
        qty = base.qty
        order_type = base.order_type

        store = DhanStore.load()

        # Advanced lookup for derivatives when extra fields are present (e.g., BSXOPT)
        strike = row.get("StrikePrice") or row.get("strike_price")
        expiry = row.get("ExpiryDate") or row.get("expiry_date")
        opt_type = row.get("OptionType") or row.get("option_type")

        inst = None
        if strike or expiry or opt_type:
            inst = store.lookup_by_details(symbol, strike_price=strike, expiry_date=expiry, option_type=opt_type)
        else:
            inst = store.lookup_symbol(symbol)

        if inst is None:
            # Provide clearer error for derivative inputs
            if strike or expiry or opt_type:
                raise DhanValidationError(
                    f"Instrument not found for {symbol} with strike={strike}, expiry={expiry}, option={opt_type}"
                )
            raise DhanValidationError(f"Invalid symbol for Dhan: {symbol}")

        allowed = {"MARKET", "LIMIT", "STOP", "STOP_LIMIT", "AMO"}
        if order_type not in allowed:
            raise DhanValidationError(
                f"order_type '{order_type}' not supported by Dhan"
            )

        # Map Excel exchange to instrument EXCH_ID. F&O map to base exchange and must be derivatives.
        mapping = {
            "NSE": "NSE",
            "BSE": "BSE",
            "NFO": "NSE",
            "NSE_FNO": "NSE",
            "BFO": "BSE",
            "MCX": "MCX",
        }

        expected = mapping.get(exchange)
        if expected is None:
            raise DhanValidationError(f"Exchange '{exchange}' unsupported by Dhan")

        if inst.exchange_segment.upper() != expected:
            raise DhanValidationError(
                f"Exchange mismatch for {symbol}: Excel={exchange}, Dhan={inst.exchange_segment}"
            )

        # If user selected an F&O exchange (NFO/NSE_FNO/BFO), instrument must be a derivative
        if exchange in {"NFO", "NSE_FNO", "BFO"} and not inst.is_derivative:
            raise DhanValidationError(
                f"Selected exchange {exchange} requires a derivative instrument"
            )

        lot = inst.lot_size
        expiry = inst.expiry

        is_derivative = expiry not in (None, "", "0", 0)

        if is_derivative and qty % lot != 0:
            raise DhanValidationError(
                f"Qty {qty} must be multiple of lot size {lot}"
            )

        dhan_meta = {
            "symbol": inst.symbol,
            "exchange": expected,
            "security_id": inst.security_id,
            "lot_size": lot,
            "expiry": expiry,
            "instrument_type": inst.instrument_type,
        }

        return base_dict, dhan_meta
