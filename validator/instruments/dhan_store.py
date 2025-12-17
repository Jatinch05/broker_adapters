import os
import pandas as pd
from validator.instruments.dhan_instrument import DhanInstrument


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
        Optimized for memory efficiency.
        """
        csv_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "dhan_instruments.csv"
        )

        if not os.path.exists(csv_path):
            raise FileNotFoundError(
                f"Dhan instruments not found at {csv_path}. Run dhan_refresher first."
            )

        # Load only necessary columns to save memory
        required_cols = [
            'SEM_SM_SYMBOL', 'SEM_SM_SECURITY_ID', 'SEM_EXCH_ID',
            'SEM_SEGMENT_ID', 'SEM_SM_LOT_SIZE', 'SEM_SM_ISIN'
        ]
        
        # Optimize data types for memory
        dtype_dict = {
            'SEM_SM_SYMBOL': 'string',
            'SEM_SM_SECURITY_ID': 'string',
            'SEM_EXCH_ID': 'string',
            'SEM_SEGMENT_ID': 'string',
            'SEM_SM_LOT_SIZE': 'int32',
            'SEM_SM_ISIN': 'string'
        }
        
        try:
            cls._df = pd.read_csv(
                csv_path,
                usecols=required_cols,
                dtype=dtype_dict,
                low_memory=True,
                engine='c'  # Use C engine for faster parsing
            )
        except ValueError:
            # If usecols fails, load all columns but with optimization
            cls._df = pd.read_csv(
                csv_path,
                low_memory=True,
                engine='c'
            )

        # Core indexes - use minimal memory
        cls._by_symbol = {}
        cls._by_security_id = {}
        
        for _, row in cls._df.iterrows():
            symbol = str(row.get("SEM_SM_SYMBOL", "")).strip().upper()
            sec_id = str(row.get("SEM_SM_SECURITY_ID", "")).strip()
            
            if symbol:
                cls._by_symbol[symbol] = row
            if sec_id:
                cls._by_security_id[sec_id] = row

        return cls

    # -----------------------------
    # Lookup Methods
    # -----------------------------

    @classmethod
    def lookup_symbol(cls, symbol: str):
        """
        Returns DhanInstrument by symbol (case-insensitive).
        Returns None if not found.
        """
        if cls._df is None:
            raise RuntimeError("Call DhanStore.load() first")

        key = symbol.strip().upper()
        row = cls._by_symbol.get(key)
        if row is None:
            return None
        return DhanInstrument(row)

    @classmethod
    def lookup_security_id(cls, security_id: str):
        """
        Returns DhanInstrument by Dhan security ID.
        Returns None if not found.
        """
        if cls._df is None:
            raise RuntimeError("Call DhanStore.load() first")

        key = str(security_id).strip()
        row = cls._by_security_id.get(key)
        if row is None:
            return None
        return DhanInstrument(row)

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
