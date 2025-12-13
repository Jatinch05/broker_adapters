from __future__ import annotations
from typing import Dict, Any
from pydantic import BaseModel, field_validator, ValidationError


class OrderIntentBase(BaseModel):
    symbol: str
    exchange: str
    txn_type: str
    qty: int
    order_type: str
    price: float | None = None
    trigger_price: float | None = None

    # OPTIONAL FIELDS (brokers may ignore)
    product: str | None = None
    validity: str | None = None
    variety: str | None = None
    disclosed_qty: int | None = None
    tag: str | None = None

    @field_validator("symbol")
    def norm_symbol(cls, v):
        return v.strip().upper()

    @field_validator("exchange")
    def norm_exchange(cls, v):
        return v.strip().upper()

    @field_validator("txn_type")
    def norm_txn(cls, v):
        v = v.strip().upper()
        if v not in {"BUY", "SELL"}:
            raise ValueError("txn_type must be BUY or SELL")
        return v

    @field_validator("qty")
    def check_qty(cls, v):
        if v <= 0:
            raise ValueError("qty must be > 0")
        return v
    @field_validator("order_type")
    def norm_ot(cls, v):
        return v.strip().upper()

    @field_validator("price")
    def validate_price(cls, v, info):
        ot = info.data.get("order_type", "").upper()

        # If MARKET, price must be None
        if ot == "MARKET" and v is not None:
            raise ValueError("price must be None for MARKET orders")

        # If LIMIT, price required
        if ot == "LIMIT" and v is None:
            raise ValueError("price required for LIMIT")

        return v

    @field_validator("trigger_price")
    def validate_trigger(cls, v, info):
        ot = info.data.get("order_type", "").upper()

        # Generic rule: STOP/SL/SL-M require trigger price
        if ot in {"SL", "SL-M", "STOP", "STOP_LIMIT"} and v is None:
            raise ValueError(f"trigger_price required for {ot}")

        # If NOT a stop order, trigger must be None
        if ot in {"MARKET", "LIMIT"} and v is not None:
            raise ValueError(f"trigger_price must be None for {ot}")

        return v


class BaseValidator:
    """
    Minimal, broker-agnostic validation.
    Output: normalized OrderIntentBase
    """

    @staticmethod
    def validate_row(row: Dict[str, Any]) -> OrderIntentBase:
        try:
            oi = OrderIntentBase(**row)
        except ValidationError as e:
            raise ValueError(str(e))

        # Nothing broker-specific here
        return oi
