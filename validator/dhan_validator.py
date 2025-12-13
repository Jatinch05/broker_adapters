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
        inst = store.lookup_symbol(symbol)

        if inst is None:
            raise DhanValidationError(f"Invalid symbol for Dhan: {symbol}")

        allowed = {"MARKET", "LIMIT", "STOP", "STOP_LIMIT", "AMO"}
        if order_type not in allowed:
            raise DhanValidationError(
                f"order_type '{order_type}' not supported by Dhan"
            )

        mapping = {
            "NSE": "NSE",
            "BSE": "BSE",
            "NFO": "NSE_FNO",
            "BFO": "BSE_FNO",
            "MCX": "MCX",
        }

        expected = mapping.get(exchange)
        if expected is None:
            raise DhanValidationError(f"Exchange '{exchange}' unsupported by Dhan")

        if inst.raw["SEM_EXM_EXCHANGE_CODE"].upper() != expected:
            raise DhanValidationError(
                f"Exchange mismatch for {symbol}: Excel={exchange}, Dhan={inst.raw['SEM_EXM_EXCHANGE_CODE']}"
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
            "security_id": inst.raw.get("SEM_SM_SECURITY_ID"),
            "lot_size": lot,
            "expiry": expiry,
            "instrument_type": inst.instrument_type,
        }

        return base_dict, dhan_meta
