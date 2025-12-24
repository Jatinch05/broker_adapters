from __future__ import annotations

from typing import Optional
from pydantic import BaseModel, field_validator, model_validator


class DhanForeverOrderIntent(BaseModel):
    # Core
    symbol: str
    exchange: str
    txn_type: str
    qty: int
    order_type: str
    product: str

    # Forever specifics
    trigger_price: float
    price: Optional[float] = None  # for LIMIT orders
    order_flag: str = "SINGLE"     # SINGLE or OCO
    validity: str = "DAY"
    disclosed_quantity: int = 0

    # OCO optional legs
    price1: float | None = None
    trigger_price1: float | None = None
    quantity1: int | None = None

    # Optional correlation
    tag: Optional[str] = None

    # Optional derivative lookup fields
    strike_price: float | None = None
    expiry_date: str | None = None
    option_type: str | None = None

    @field_validator("symbol")
    def norm_symbol(cls, v: str):
        return v.strip().upper()

    @field_validator("exchange")
    def norm_exchange(cls, v: str):
        return v.strip().upper()

    @field_validator("txn_type")
    def validate_txn(cls, v: str):
        v = v.strip().upper()
        if v not in {"BUY", "SELL"}:
            raise ValueError("txn_type must be BUY or SELL for Forever Orders")
        return v

    @field_validator("order_type")
    def validate_order_type(cls, v: str):
        v = v.strip().upper()
        # Dhan forever supports LIMIT orders for triggers; allow MARKET/LIMIT if API evolves
        if v not in {"LIMIT", "MARKET"}:
            raise ValueError("order_type must be LIMIT or MARKET for Forever Orders")
        return v

    @field_validator("qty")
    def validate_qty(cls, v: int):
        if v <= 0:
            raise ValueError("qty must be > 0 for Forever Orders")
        return v

    @field_validator("product")
    def validate_product(cls, v: str):
        v = v.strip().upper()
        if not v:
            raise ValueError("product is required for Forever Orders")
        # Official docs for Create Forever Order list CNC / MTF.
        if v not in {"CNC", "MTF", "NORMAL"}:
            raise ValueError("Invalid product for Forever Order. Allowed: CNC, MTF")
        return v

    @field_validator("trigger_price")
    def validate_trigger(cls, v: float):
        if v <= 0:
            raise ValueError("trigger_price must be > 0 for Forever Orders")
        return v

    @field_validator("price")
    def validate_price(cls, v, info):
        # Docs: price required for Create Forever Order.
        if v is None:
            raise ValueError("price is required for Forever Orders")
        return v

    @field_validator("order_flag")
    def validate_flag(cls, v: str):
        v = v.strip().upper()
        # Docs: OCO or SINGLE for Create Forever Order.
        if v not in {"SINGLE", "OCO"}:
            raise ValueError("order_flag must be SINGLE or OCO")
        return v

    @field_validator("validity")
    def validate_validity(cls, v: str):
        v = v.strip().upper()
        if v not in {"DAY", "IOC"}:
            raise ValueError("validity must be DAY or IOC")
        return v

    @field_validator("disclosed_quantity")
    def validate_disclosed(cls, v: int, info):
        if v < 0:
            raise ValueError("disclosed_quantity must be >= 0")
        if v and v >= info.data.get("qty", 0):
            raise ValueError("disclosed_quantity must be less than qty")
        # Docs: disclosedQuantity (if used) should be at least 30% of quantity.
        qty = info.data.get("qty", 0) or 0
        if v and qty:
            import math

            min_dq = int(math.ceil(0.3 * float(qty)))
            if v < min_dq:
                raise ValueError(f"disclosed_quantity must be >= {min_dq} (30% of qty)")
        return v

    @field_validator("quantity1")
    def validate_quantity1(cls, v, info):
        flag = info.data.get("order_flag")
        if flag == "OCO":
            if v is None or v <= 0:
                raise ValueError("quantity1 must be > 0 for OCO forever orders")
        return v

    @field_validator("price1")
    def validate_price1(cls, v, info):
        if info.data.get("order_flag") == "OCO":
            if v is None:
                raise ValueError("price1 is required for OCO forever orders")
            if v <= 0:
                raise ValueError("price1 must be > 0 for OCO forever orders")
        return v

    @field_validator("trigger_price1")
    def validate_trigger1(cls, v, info):
        if info.data.get("order_flag") == "OCO":
            if v is None:
                raise ValueError("trigger_price1 is required for OCO forever orders")
            if v <= 0:
                raise ValueError("trigger_price1 must be > 0 for OCO forever orders")
        return v

    @model_validator(mode="after")
    def validate_oco_leg_requirements(self):
        if self.order_flag == "OCO":
            missing = []
            if self.price1 is None:
                missing.append("price1")
            if self.trigger_price1 is None:
                missing.append("trigger_price1")
            if self.quantity1 is None:
                missing.append("quantity1")
            if missing:
                raise ValueError(f"Missing OCO fields: {', '.join(missing)}")
        return self
