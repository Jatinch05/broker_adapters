"""
Core domain exceptions for broker adapters and services.
"""


class BrokerAdapterError(Exception):
    """Base exception for broker adapter errors."""
    pass


class BrokerConnectionError(BrokerAdapterError):
    """Raised when unable to connect to broker."""
    pass


class BrokerAuthError(BrokerAdapterError):
    """Raised when authentication with broker fails."""
    pass


class InstrumentNotFoundError(BrokerAdapterError):
    """Raised when instrument/security ID not found."""
    pass


class OrderPlacementError(BrokerAdapterError):
    """Raised when order placement fails."""
    pass


class OrderCancellationError(BrokerAdapterError):
    """Raised when order cancellation fails."""
    pass


class RateLimitExceededError(BrokerAdapterError):
    """Raised when broker rate limit is hit."""
    pass


class CircuitBreakerOpenError(BrokerAdapterError):
    """Raised when circuit breaker is open (broker degraded)."""
    pass


class ValidationError(Exception):
    """Raised when order validation fails."""
    pass


class PaymentError(Exception):
    """Raised when payment processing fails."""
    pass
