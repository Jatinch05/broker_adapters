"""
Core domain models shared across adapters.
"""
from enum import Enum
from typing import Optional, Dict, Any
from dataclasses import dataclass, field
from datetime import datetime


class OrderStatus(str, Enum):
    """Order status enum."""
    PENDING = "PENDING"
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"
    EXECUTED = "EXECUTED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


class TransactionType(str, Enum):
    """Buy or Sell."""
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    """Market, Limit, etc."""
    MARKET = "MARKET"
    LIMIT = "LIMIT"


class ProductType(str, Enum):
    """CNC, MIS, NRML, etc."""
    CNC = "CNC"
    MIS = "MIS"
    NRML = "NRML"
    MTF = "MTF"


@dataclass
class Order:
    """Minimal order representation."""
    user_id: str
    broker: str
    symbol: str
    txn_type: TransactionType
    qty: int
    order_type: OrderType
    price: Optional[float] = None
    product: ProductType = ProductType.CNC
    target_price: Optional[float] = None
    stop_loss_price: Optional[float] = None
    trailing_jump: Optional[float] = None
    tag: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class OrderResult:
    """Result from broker after order placement."""
    order_id: Optional[str] = None
    status: OrderStatus = OrderStatus.FAILED
    message: str = ""
    broker_response: Optional[Dict[str, Any]] = None
    error_code: Optional[str] = None
    executed_at: datetime = field(default_factory=datetime.utcnow)
