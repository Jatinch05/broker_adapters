"""
Multi-broker adapter contract.
All broker implementations should implement this interface.
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from core.models import Order, OrderResult


class BrokerCapabilities:
    """Describes what a broker supports."""
    
    def __init__(
        self,
        name: str,
        segments: List[str],  # e.g., ["NSE", "NFO", "BSE"]
        order_types: List[str],  # e.g., ["MARKET", "LIMIT"]
        products: List[str],  # e.g., ["CNC", "MIS"]
        max_orders_per_second: float = 10,
        supports_cancel: bool = True,
        supports_modify: bool = True,
    ):
        self.name = name
        self.segments = segments
        self.order_types = order_types
        self.products = products
        self.max_orders_per_second = max_orders_per_second
        self.supports_cancel = supports_cancel
        self.supports_modify = supports_modify


class BrokerAdapter(ABC):
    """
    Base class for broker adapters.
    All brokers (Dhan, Zerodha, Angel, etc.) should extend this.
    """

    @abstractmethod
    def get_capabilities(self) -> BrokerCapabilities:
        """Return what this broker supports."""
        pass

    @abstractmethod
    def authenticate(self, client_id: str, access_token: str) -> bool:
        """
        Verify broker credentials.
        Raise BrokerAuthError if invalid.
        """
        pass

    @abstractmethod
    def place_order(self, order: Order) -> OrderResult:
        """
        Place an order with the broker.
        Return OrderResult with order ID and status.
        Raise OrderPlacementError if failed.
        """
        pass

    @abstractmethod
    def cancel_order(self, order_id: str) -> OrderResult:
        """
        Cancel an order by ID.
        Return OrderResult with cancellation status.
        Raise OrderCancellationError if failed.
        """
        pass

    @abstractmethod
    def get_instruments(self) -> Dict[str, Any]:
        """
        Fetch or return instruments supported by this broker.
        Return dict keyed by symbol.
        Used for caching/validation.
        """
        pass

    @abstractmethod
    def normalize_error(self, broker_error: Exception) -> str:
        """
        Map broker-specific error to a standard error code.
        E.g., "DH-905" -> "INVALID_SYMBOL"
        """
        pass
