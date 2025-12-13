import os
import pandas as pd


class DhanStore:
    """
    Loads dhan_instruments.csv and provides fast lookup utilities.
    """

    _df = None
    _by_symbol = None
    _by_security_id = None

    @classmethod
    def load(cls):
        """
        Loads the CSV from disk, builds indexes.
        Called once per session.
        """
        csv_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "dhan_instruments.csv"
        )

        if not os.path.exists(csv_path):
            raise FileNotFoundError(
                f"Dhan instruments not found at {csv_path}. Run dhan_refresher first."
            )

        cls._df = pd.read_csv(csv_path)

        # Core indexes
        cls._by_symbol = {
            str(row["SEM_SM_SYMBOL"]).strip().upper(): row
            for _, row in cls._df.iterrows()
        }

        cls._by_security_id = {
            str(row["SEM_SM_SECURITY_ID"]).strip(): row
            for _, row in cls._df.iterrows()
        }

        return cls

    # -----------------------------
    # Lookup Methods
    # -----------------------------

    @classmethod
    def lookup_symbol(cls, symbol: str):
        """
        Returns instrument row by symbol (case-insensitive).
        """
        if cls._df is None:
            raise RuntimeError("Call DhanStore.load() first")

        key = symbol.strip().upper()
        return cls._by_symbol.get(key)

    @classmethod
    def lookup_security_id(cls, security_id: str):
        """
        Returns row by Dhan security ID.
        """
        if cls._df is None:
            raise RuntimeError("Call DhanStore.load() first")

        key = str(security_id).strip()
        return cls._by_security_id.get(key)

    @classmethod
    def exists(cls, symbol: str) -> bool:
        return cls.lookup_symbol(symbol) is not None

    @classmethod
    def lot_size(cls, symbol: str) -> int:
        """
        Returns lot size for a symbol.
        """
        row = cls.lookup_symbol(symbol)
        if row is None:
            return None
        return int(row.get("SEM_SM_LOT_SIZE", 1))

    @classmethod
    def segment(cls, symbol: str):
        """
        Returns the segment (NSE, NFO, BSE, BFO)
        """
        row = cls.lookup_symbol(symbol)
        if row is None:
            return None
        return row.get("SEM_EXM_EXCHANGE_CODE")

    @classmethod
    def expiry(cls, symbol: str):
        """
        Returns expiry date (if F&O)
        """
        row = cls.lookup_symbol(symbol)
        if row is None:
            return None
        return row.get("SEM_SM_EXPIRY_DATE")
