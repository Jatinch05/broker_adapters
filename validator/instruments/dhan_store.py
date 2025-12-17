import os
import pandas as pd
import json
from datetime import datetime, timedelta
from validator.instruments.dhan_instrument import DhanInstrument


class DhanStore:
    """
    Loads dhan_instruments.csv and provides fast lookup utilities.
    Auto-refreshes if data is stale (>1 day old).
    """

    _df = None
    _by_symbol = None
    _by_security_id = None
    _derivative_index = None
    _csv_path = None

    @classmethod
    def _is_stale(cls) -> bool:
        """Check if instruments data is older than 1 day"""
        csv_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "dhan_instruments.csv"
        )
        meta_path = csv_path.replace(".csv", "_meta.json")
        
        if not os.path.exists(meta_path):
            return True  # No metadata, consider stale
        
        try:
            with open(meta_path, 'r') as f:
                meta = json.load(f)
            
            last_updated = datetime.fromisoformat(meta['last_updated'])
            age = datetime.now() - last_updated
            
            # Stale if older than 1 day
            return age > timedelta(days=1)
        except Exception:
            return True  # Error reading metadata, consider stale

    @classmethod
    def load(cls):
        """
        Loads the CSV from disk, builds indexes.
        Auto-refreshes if data is >1 day old (disabled on Render to save memory).
        Called once per session.
        Optimized for memory efficiency.
        """
        # If already loaded, skip reloading to speed up bulk operations
        if cls._df is not None and cls._by_symbol is not None and cls._by_security_id is not None:
            return cls
        csv_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "dhan_instruments.csv"
        )
        cls._csv_path = csv_path
        
        # Auto-refresh if stale (only if not on Render or file missing)
        # On Render, use manual refresh to avoid memory issues
        is_render = os.environ.get('RENDER') == 'true'
        file_missing = not os.path.exists(csv_path)
        
        if cls._is_stale() and (not is_render or file_missing):
            try:
                from validator.instruments.dhan_refresher import refresh_dhan_instruments
                import logging
                logging.info("Auto-refreshing stale instruments data...")
                refresh_dhan_instruments()
            except Exception as e:
                # Log warning but continue with existing data if available
                import logging
                logging.warning(f"Failed to auto-refresh instruments: {e}")
        elif is_render and cls._is_stale() and not file_missing:
            import logging
            logging.warning("Instruments data is stale but auto-refresh disabled on Render. Use manual refresh from dashboard.")
        
        csv_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "dhan_instruments.csv"
        )

        if not os.path.exists(csv_path):
            raise FileNotFoundError(
                f"Dhan instruments not found at {csv_path}. Run dhan_refresher first."
            )

        # Load only necessary columns to save memory (aligned to actual CSV headers)
        required_cols = [
            'EXCH_ID', 'SEGMENT', 'SECURITY_ID', 'ISIN', 'INSTRUMENT',
            'UNDERLYING_SECURITY_ID', 'UNDERLYING_SYMBOL', 'SYMBOL_NAME',
            'DISPLAY_NAME', 'INSTRUMENT_TYPE', 'SERIES', 'LOT_SIZE',
            'SM_EXPIRY_DATE', 'STRIKE_PRICE', 'OPTION_TYPE'
        ]
        
        # Optimize data types for memory
        dtype_dict = {
            'EXCH_ID': 'string',
            'SEGMENT': 'string',
            'SECURITY_ID': 'string',
            'ISIN': 'string',
            'INSTRUMENT': 'string',
            'UNDERLYING_SECURITY_ID': 'string',
            'UNDERLYING_SYMBOL': 'string',
            'SYMBOL_NAME': 'string',
            'DISPLAY_NAME': 'string',
            'INSTRUMENT_TYPE': 'string',
            'SERIES': 'string',
            'LOT_SIZE': 'float32',
            'SM_EXPIRY_DATE': 'string',
            'STRIKE_PRICE': 'float64',
            'OPTION_TYPE': 'string'
        }
        
        # If streaming mode, skip building full DataFrame to save memory
        streaming_mode = (os.environ.get('DHAN_INSTR_MODE', '').lower() == 'stream') or is_render
        if streaming_mode:
            cls._df = None
        else:
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
        cls._derivative_index = {}
        
        if cls._df is not None:
            for _, row in cls._df.iterrows():
                symbol = str(row.get("SYMBOL_NAME", "")).strip().upper()
                sec_id = str(row.get("SECURITY_ID", "")).strip()

                if symbol:
                    cls._by_symbol[symbol] = row
                if sec_id:
                    cls._by_security_id[sec_id] = row

                # Build a fast derivative index when fields exist
                expiry = row.get('SM_EXPIRY_DATE')
                strike = row.get('STRIKE_PRICE')
                opt_type = row.get('OPTION_TYPE')
                if (not pd.isna(expiry)) and pd.notna(strike) and (not pd.isna(opt_type)) and str(opt_type).strip() != "":
                    key = (
                        symbol,
                        float(strike),
                        str(expiry),
                        str(opt_type).strip().upper()
                    )
                    cls._derivative_index[key] = row

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
        if cls._by_symbol is None:
            raise RuntimeError("Call DhanStore.load() first")

        key = symbol.strip().upper()
        row = cls._by_symbol.get(key)
        if row is None and cls._df is None and cls._csv_path:
            # Streaming find: scan CSV in chunks to find the first match
            for chunk in pd.read_csv(
                cls._csv_path,
                usecols=['SYMBOL_NAME','SECURITY_ID','EXCH_ID','LOT_SIZE','SM_EXPIRY_DATE','STRIKE_PRICE','OPTION_TYPE','INSTRUMENT_TYPE'],
                dtype={
                    'SYMBOL_NAME':'string','SECURITY_ID':'string','EXCH_ID':'string','LOT_SIZE':'float32',
                    'SM_EXPIRY_DATE':'string','STRIKE_PRICE':'float64','OPTION_TYPE':'string','INSTRUMENT_TYPE':'string'
                },
                chunksize=50000,
                engine='c'
            ):
                # Normalize symbol column to uppercase for compare
                chunk['SYMBOL_UP'] = chunk['SYMBOL_NAME'].str.upper()
                matches = chunk[chunk['SYMBOL_UP'] == key]
                if len(matches):
                    row = matches.iloc[0]
                    cls._by_symbol[key] = row
                    sec_id = str(row.get('SECURITY_ID','')).strip()
                    if sec_id:
                        cls._by_security_id[sec_id] = row
                    break
        if row is None:
            return None
        return DhanInstrument(row)

    @classmethod
    def lookup_security_id(cls, security_id: str):
        """
        Returns DhanInstrument by Dhan security ID.
        Returns None if not found.
        """
        if cls._by_security_id is None:
            raise RuntimeError("Call DhanStore.load() first")

        key = str(security_id).strip()
        row = cls._by_security_id.get(key)
        if row is None:
            return None
        return DhanInstrument(row)

    @classmethod
    def lookup_by_details(cls, symbol: str, strike_price: float = None, expiry_date: str = None, option_type: str = None):
        """
        Returns DhanInstrument by symbol with optional strike, expiry, and option type.
        Useful for SENSEX/BSXOPT where multiple instruments share the same symbol.
        
        Args:
            symbol: Trading symbol (e.g., "BSXOPT", "NIFTY")
            strike_price: Strike price (e.g., 85000)
            expiry_date: Expiry date in YYYY-MM-DD format (e.g., "2025-12-18")
            option_type: Option type - "CE" or "PE"
            
        Returns:
            DhanInstrument if found, None otherwise
        """
        if cls._by_symbol is None:
            raise RuntimeError("Call DhanStore.load() first")

        key_symbol = symbol.strip().upper()
        
        # If no additional filters, use standard lookup
        if strike_price is None and expiry_date is None and option_type is None:
            return cls.lookup_symbol(symbol)
        
        # Use fast derivative index if all filters provided
        if strike_price is not None and expiry_date is not None and option_type is not None:
            key = (key_symbol, float(strike_price), str(expiry_date), option_type.strip().upper())
            row = cls._derivative_index.get(key)
            if row is not None:
                return DhanInstrument(row)

        # Fallback
        if cls._df is not None:
            filtered = cls._df[cls._df['SYMBOL_NAME'].str.upper() == key_symbol]
            if strike_price is not None:
                try:
                    strike_val = float(strike_price)
                    filtered = filtered[filtered['STRIKE_PRICE'] == strike_val]
                except Exception:
                    pass
            if expiry_date is not None:
                filtered = filtered[filtered['SM_EXPIRY_DATE'] == str(expiry_date)]
            if option_type is not None:
                filtered = filtered[filtered['OPTION_TYPE'].str.upper() == option_type.strip().upper()]
            if len(filtered) == 0:
                return None
            return DhanInstrument(filtered.iloc[0])

        # Streaming mode: scan CSV in chunks with filters
        if cls._csv_path:
            opt_upper = option_type.strip().upper() if option_type else None
            try:
                for chunk in pd.read_csv(
                    cls._csv_path,
                    usecols=['SYMBOL_NAME','SECURITY_ID','EXCH_ID','LOT_SIZE','SM_EXPIRY_DATE','STRIKE_PRICE','OPTION_TYPE','INSTRUMENT_TYPE'],
                    dtype={
                        'SYMBOL_NAME':'string','SECURITY_ID':'string','EXCH_ID':'string','LOT_SIZE':'float32',
                        'SM_EXPIRY_DATE':'string','STRIKE_PRICE':'float64','OPTION_TYPE':'string','INSTRUMENT_TYPE':'string'
                    },
                    chunksize=50000,
                    engine='c'
                ):
                    chunk['SYMBOL_UP'] = chunk['SYMBOL_NAME'].str.upper()
                    filtered = chunk[chunk['SYMBOL_UP'] == key_symbol]
                    if strike_price is not None:
                        try:
                            strike_val = float(strike_price)
                            filtered = filtered[filtered['STRIKE_PRICE'] == strike_val]
                        except Exception:
                            pass
                    if expiry_date is not None:
                        filtered = filtered[filtered['SM_EXPIRY_DATE'] == str(expiry_date)]
                    if opt_upper is not None:
                        filtered = filtered[filtered['OPTION_TYPE'].str.upper() == opt_upper]
                    if len(filtered):
                        row = filtered.iloc[0]
                        # cache
                        sec_id = str(row.get('SECURITY_ID','')).strip()
                        if sec_id:
                            cls._by_security_id[sec_id] = row
                        sym = str(row.get('SYMBOL_NAME','')).strip().upper()
                        if sym:
                            cls._by_symbol[sym] = row
                        if strike_price is not None and expiry_date is not None and opt_upper is not None:
                            key = (sym, float(row.get('STRIKE_PRICE')), str(row.get('SM_EXPIRY_DATE')), opt_upper)
                            cls._derivative_index[key] = row
                        return DhanInstrument(row)
            except Exception:
                return None
        return None

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
        try:
            return int(float(row.raw.get("LOT_SIZE", 1)))
        except Exception:
            return 1

    @classmethod
    def segment(cls, symbol: str):
        """
        Returns the segment (NSE, NFO, BSE, BFO)
        """
        row = cls.lookup_symbol(symbol)
        if row is None:
            return None
        return row.raw.get("EXCH_ID")

    @classmethod
    def expiry(cls, symbol: str):
        """
        Returns expiry date (if F&O)
        """
        row = cls.lookup_symbol(symbol)
        if row is None:
            return None
        return row.raw.get("SM_EXPIRY_DATE")
