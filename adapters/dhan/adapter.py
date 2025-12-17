"""
Dhan broker adapter implementation.
"""
from typing import Dict, Any
from adapters.base import BrokerAdapter, BrokerCapabilities
from core.models import Order, OrderResult
from core.exceptions import BrokerAuthError, OrderPlacementError
from adapters.dhan.super_order import place_dhan_super_order
from services.instrument_store import InstrumentStore
import logging

logger = logging.getLogger(__name__)


class DhanAdapter(BrokerAdapter):
    """Dhan broker adapter."""
    
    def __init__(self, client_id: str, access_token: str):
        """Initialize with credentials."""
        self.client_id = client_id
        self.access_token = access_token
        self._authenticated = False
    
    def get_capabilities(self) -> BrokerCapabilities:
        """Return Dhan's capabilities."""
        return BrokerCapabilities(
            name="Dhan",
            segments=["NSE", "NFO", "BSE", "BFO"],
            order_types=["MARKET", "LIMIT"],
            products=["CNC", "MIS", "NRML", "MTF"],
            max_orders_per_second=25,
            supports_cancel=True,
            supports_modify=False,
        )
    
    def authenticate(self, client_id: str, access_token: str) -> bool:
        """
        Verify broker credentials by making a test call.
        In a real scenario, you'd call a Dhan endpoint to verify the token.
        For now, we just check if they're provided.
        """
        if not client_id or not access_token:
            raise BrokerAuthError("Dhan client_id and access_token required")
        
        self.client_id = client_id
        self.access_token = access_token
        self._authenticated = True
        logger.info(f"Dhan authenticated for client {client_id}")
        return True
    
    def place_order(self, order: Order) -> OrderResult:
        """Place an order with Dhan."""
        if not self._authenticated:
            raise BrokerAuthError("Not authenticated with Dhan")
        
        # Resolve instrument to security ID
        instrument = InstrumentStore.lookup_by_details(
            symbol=order.symbol,
            strike_price=None,  # Standard lookup
            expiry_date=None,
            option_type=None,
        )
        
        if not instrument:
            raise OrderPlacementError(f"Instrument {order.symbol} not found")
        
        security_id = instrument.get("SECURITY_ID")
        exchange = instrument.get("EXCH_ID")
        
        # Call Dhan API
        try:
            result = place_dhan_super_order(
                order=order,
                client_id=self.client_id,
                access_token=self.access_token,
                security_id=security_id,
                exchange_segment=exchange,
            )
            return result
        except OrderPlacementError as e:
            logger.error(f"Dhan order placement failed: {e}")
            raise
    
    def cancel_order(self, order_id: str) -> OrderResult:
        """Cancel an order with Dhan (not implemented yet)."""
        raise NotImplementedError("Dhan cancel not implemented")
    
    def get_instruments(self) -> Dict[str, Any]:
        """Return Dhan instruments (loads from CSV cache)."""
        InstrumentStore.load()
        return InstrumentStore._by_symbol or {}
    
    def normalize_error(self, broker_error: Exception) -> str:
        """Map Dhan error to standard code."""
        error_msg = str(broker_error).lower()
        if "invalid security" in error_msg or "not found" in error_msg:
            return "INVALID_SYMBOL"
        elif "rate limit" in error_msg:
            return "RATE_LIMIT_EXCEEDED"
        elif "unauthorized" in error_msg or "auth" in error_msg:
            return "AUTH_FAILED"
        return "UNKNOWN_ERROR"
