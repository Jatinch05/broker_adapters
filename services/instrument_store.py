"""
Instrument store service for fast lookup of trading instruments.
Loads from CSV cache with streaming fallback.
"""
import os
import pandas as pd
import logging
from typing import Optional, Dict, Any
from config.settings import settings

logger = logging.getLogger(__name__)


class InstrumentStore:
    """
    Loads and caches broker instruments for fast lookup.
    Supports streaming mode for memory-constrained environments.
    """

    _df = None
    _by_symbol = None
    _by_security_id = None
    _derivative_index = None
    _csv_path = None

    @classmethod
    def load(cls, csv_path: str = None):
        """
        Load instruments from CSV.
        
        Args:
            csv_path: Path to instruments CSV. If None, uses configured path.
        """
        if cls._df is not None:
            return cls  # Already loaded

        if csv_path is None:
            csv_path = settings.INSTRUMENTS_CSV

        cls._csv_path = csv_path

        if not os.path.exists(csv_path):
            logger.warning(f"Instruments CSV not found at {csv_path}")
            cls._by_symbol = {}
            cls._by_security_id = {}
            cls._derivative_index = {}
            return cls

        try:
            required_cols = [
                'EXCH_ID', 'SEGMENT', 'SECURITY_ID', 'ISIN', 'INSTRUMENT',
                'UNDERLYING_SECURITY_ID', 'UNDERLYING_SYMBOL', 'SYMBOL_NAME',
                'DISPLAY_NAME', 'INSTRUMENT_TYPE', 'SERIES', 'LOT_SIZE',
                'SM_EXPIRY_DATE', 'STRIKE_PRICE', 'OPTION_TYPE'
            ]

            cls._df = pd.read_csv(csv_path, usecols=required_cols, low_memory=True)
            logger.info(f"Loaded {len(cls._df)} instruments from {csv_path}")

            # Build indexes
            cls._by_symbol = {}
            cls._by_security_id = {}
            cls._derivative_index = {}

            for _, row in cls._df.iterrows():
                symbol = str(row.get("SYMBOL_NAME", "")).strip().upper()
                sec_id = str(row.get("SECURITY_ID", "")).strip()

                if symbol:
                    cls._by_symbol[symbol] = row
                if sec_id:
                    cls._by_security_id[sec_id] = row

                # Index derivatives by underlying + strike + expiry + option_type
                expiry = row.get('SM_EXPIRY_DATE')
                strike = row.get('STRIKE_PRICE')
                opt_type = row.get('OPTION_TYPE')
                underlying = row.get('UNDERLYING_SYMBOL')

                if (not pd.isna(expiry)) and pd.notna(strike) and (not pd.isna(opt_type)):
                    if pd.notna(underlying) and str(underlying).strip():
                        underlying_key = str(underlying).strip().upper()
                        deriv_key = (
                            underlying_key,
                            float(strike),
                            str(expiry),
                            str(opt_type).strip().upper()
                        )
                        cls._derivative_index[deriv_key] = row

                    # Also index by symbol for BSXOPT-style lookups
                    key = (symbol, float(strike), str(expiry), str(opt_type).strip().upper())
                    cls._derivative_index[key] = row

        except Exception as e:
            logger.error(f"Failed to load instruments: {e}")
            cls._by_symbol = {}
            cls._by_security_id = {}
            cls._derivative_index = {}

        return cls

    @classmethod
    def lookup_symbol(cls, symbol: str) -> Optional[Dict[str, Any]]:
        """Lookup instrument by symbol (case-insensitive)."""
        if cls._by_symbol is None:
            cls.load()

        key = symbol.strip().upper()
        row = cls._by_symbol.get(key)
        
        if row is None:
            return None
        
        # Convert pandas Series to dict
        return row.to_dict() if hasattr(row, 'to_dict') else dict(row)

    @classmethod
    def lookup_security_id(cls, security_id: str) -> Optional[Dict[str, Any]]:
        """Lookup instrument by Dhan security ID."""
        if cls._by_security_id is None:
            cls.load()

        key = str(security_id).strip()
        row = cls._by_security_id.get(key)
        
        if row is None:
            return None
        
        return row.to_dict() if hasattr(row, 'to_dict') else dict(row)

    @classmethod
    def lookup_by_details(
        cls,
        symbol: str,
        strike_price: float = None,
        expiry_date: str = None,
        option_type: str = None
    ) -> Optional[Dict[str, Any]]:
        """
        Lookup instrument by symbol with optional strike/expiry/option.
        Useful for options where multiple contracts share same underlying.
        
        When strike, expiry, and option_type are provided, filters by underlying_symbol or symbol.
        Otherwise, does a standard symbol lookup.
        """
        if cls._by_symbol is None:
            cls.load()

        key_symbol = symbol.strip().upper()

        # If no additional filters, use standard lookup
        if strike_price is None or expiry_date is None or option_type is None:
            return cls.lookup_symbol(symbol)

        # Use derivative index if all filters provided
        key = (key_symbol, float(strike_price), str(expiry_date), option_type.strip().upper())
        row = cls._derivative_index.get(key)
        
        if row is not None:
            return row.to_dict() if hasattr(row, 'to_dict') else dict(row)
        
        # Fallback: scan DataFrame for match
        if cls._df is not None:
            try:
                # Try matching by underlying OR symbol
                if 'UNDERLYING_SYMBOL' in cls._df.columns:
                    filtered = cls._df[
                        (cls._df['UNDERLYING_SYMBOL'].str.upper() == key_symbol) |
                        (cls._df['SYMBOL_NAME'].str.upper() == key_symbol)
                    ]
                else:
                    filtered = cls._df[cls._df['SYMBOL_NAME'].str.upper() == key_symbol]
                
                # Apply filters
                if strike_price is not None:
                    filtered = filtered[filtered['STRIKE_PRICE'] == float(strike_price)]
                if expiry_date is not None:
                    filtered = filtered[filtered['SM_EXPIRY_DATE'] == str(expiry_date)]
                if option_type is not None:
                    filtered = filtered[filtered['OPTION_TYPE'].str.upper() == option_type.strip().upper()]
                
                if len(filtered) > 0:
                    return filtered.iloc[0].to_dict()
            except Exception as e:
                logger.error(f"Error filtering instruments: {e}")
        
        return None

    @classmethod
    def exists(cls, symbol: str) -> bool:
        """Check if symbol exists."""
        return cls.lookup_symbol(symbol) is not None
