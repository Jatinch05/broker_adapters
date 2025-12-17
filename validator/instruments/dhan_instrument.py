"""
Helper class to wrap instrument data from DhanStore
"""


class DhanInstrument:
    """
    Wrapper around a Dhan instrument row for easier access.
    """

    def __init__(self, raw_row):
        if raw_row is None:
            raise ValueError("Instrument row cannot be None")
        self.raw = raw_row

    @property
    def symbol(self) -> str:
        return str(self.raw.get("SYMBOL_NAME", "")).strip().upper()

    @property
    def security_id(self) -> str:
        return str(self.raw.get("SECURITY_ID", "")).strip()

    @property
    def exchange_segment(self) -> str:
        return str(self.raw.get("EXCH_ID", "")).strip().upper()

    @property
    def lot_size(self) -> int:
        try:
            return int(float(self.raw.get("LOT_SIZE", 1)))
        except Exception:
            return 1

    @property
    def expiry(self):
        expiry_val = self.raw.get("SM_EXPIRY_DATE")
        if expiry_val in (None, "", "0", 0):
            return None
        return expiry_val

    @property
    def instrument_type(self) -> str:
        return str(self.raw.get("INSTRUMENT_TYPE", "")).strip()

    @property
    def is_derivative(self) -> bool:
        return self.expiry not in (None, "", "0", 0)

    def __repr__(self):
        return f"DhanInstrument(symbol={self.symbol}, security_id={self.security_id}, exchange={self.exchange_segment})"
