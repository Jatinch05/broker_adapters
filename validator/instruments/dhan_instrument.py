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
        return str(self.raw.get("SEM_SM_SYMBOL", "")).strip().upper()

    @property
    def security_id(self) -> str:
        return str(self.raw.get("SEM_SM_SECURITY_ID", "")).strip()

    @property
    def exchange_segment(self) -> str:
        return str(self.raw.get("SEM_EXM_EXCHANGE_CODE", "")).strip()

    @property
    def lot_size(self) -> int:
        return int(self.raw.get("SEM_SM_LOT_SIZE", 1))

    @property
    def expiry(self):
        expiry_val = self.raw.get("SEM_SM_EXPIRY_DATE")
        if expiry_val in (None, "", "0", 0):
            return None
        return expiry_val

    @property
    def instrument_type(self) -> str:
        return str(self.raw.get("SEM_SM_INSTRUMENT_TYPE", "")).strip()

    @property
    def is_derivative(self) -> bool:
        return self.expiry not in (None, "", "0", 0)

    def __repr__(self):
        return f"DhanInstrument(symbol={self.symbol}, security_id={self.security_id}, exchange={self.exchange_segment})"
