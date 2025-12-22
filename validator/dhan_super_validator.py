from __future__ import annotations

from typing import Dict, Any, List, Tuple, Hashable, Optional
import pandas as pd

from pydantic import BaseModel, ValidationError, field_validator


class DhanSuperOrderIntent(BaseModel):
    symbol: str
    exchange: str
    txn_type: str
    qty: int
    order_type: str
    price: float | None = None
    product: str

    # Optional correlation/tag
    tag: Optional[str] = None

    target_price: float
    stop_loss_price: float
    trailing_jump: float

    order_category: str

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
            raise ValueError("txn_type must be BUY or SELL for Super Orders")
        return v

    @field_validator("order_type")
    def validate_order_type(cls, v: str):
        v = v.strip().upper()
        if v not in {"MARKET", "LIMIT"}:
            raise ValueError("Super Orders support only MARKET or LIMIT order_type")
        return v

    @field_validator("qty")
    def validate_qty(cls, v: int):
        if v <= 0:
            raise ValueError("qty must be > 0 for Super Orders")
        return v

    @field_validator("product")
    def validate_product(cls, v: str):
        v = v.strip().upper()
        if v not in {"INTRADAY", "CNC", "MARGIN", "MTF", "NORMAL"}:
            raise ValueError(
                "Invalid product for Super Order. Allowed: INTRADAY, CNC, MARGIN, MTF"
            )
        return v

    @field_validator("tag")
    def norm_tag(cls, v: Optional[str]):
        if v is None:
            return None
        v = str(v).strip()
        return v or None

    @field_validator("order_category")
    def validate_category(cls, v: str):
        v = v.strip().upper()
        if v != "SUPER":
            raise ValueError("order_category must be SUPER for Super Orders")
        return v


    @field_validator("price")
    def validate_price(cls, v, info):
        order_type = info.data.get("order_type")
        if order_type == "LIMIT" and v is None:
            raise ValueError("price is required when order_type is LIMIT for Super Orders")
        if order_type == "MARKET" and v is not None:
            raise ValueError("price must be empty when order_type is MARKET for Super Orders")
        return v

    @field_validator("target_price")
    def validate_target(cls, v: float):
        if v <= 0:
            raise ValueError("target_price must be > 0 for Super Orders")
        return v

    @field_validator("stop_loss_price")
    def validate_sl(cls, v: float):
        if v <= 0:
            raise ValueError("stop_loss_price must be > 0 for Super Orders")
        return v

    @field_validator("trailing_jump")
    def validate_trailing(cls, v: float):
        if v < 0:
            raise ValueError("trailing_jump must be >= 0 for Super Orders")
        return v

    @field_validator("stop_loss_price")
    def validate_price_relationships(cls, sl: float, info):
        data = info.data
        tp = data.get("target_price")
        price = data.get("price")
        txn = data.get("txn_type")
        order_type = data.get("order_type")

        if order_type == "MARKET":
            if tp is not None and sl is not None and tp == sl:
                raise ValueError("target_price and stop_loss_price cannot be equal")
            return sl

        if price is None:
            return sl

        if txn == "BUY":
            if not (sl < price < tp):
                raise ValueError(
                    "For BUY Super Order (LIMIT): stop_loss_price < price < target_price"
                )
        else:  # SELL
            if not (tp < price < sl):
                raise ValueError(
                    "For SELL Super Order (LIMIT): target_price < price < stop_loss_price"
                )
        return sl


def validate_super_orders_df(
    df: pd.DataFrame,
) -> Tuple[List[DhanSuperOrderIntent], pd.DataFrame, List[Tuple[Hashable, str]]]:
    """
    Validate SUPER orders only.

    Assumes:
    - BaseValidator has already run
    - Dhan broker-common validation has already run

    Returns:
    - intents: parsed & validated Super Order intents
    - vdf: validated rows (dict form)
    - errors: list of (row_index, error_message)
    """

    intents: List[DhanSuperOrderIntent] = []
    errors: List[Tuple[Hashable, str]] = []
    out_rows: List[Dict[str, Any]] = []

    for idx, row in df.iterrows():
        try:
            intent = DhanSuperOrderIntent(**row.to_dict())
        except ValidationError as e:
            errors.append((idx, e.errors()[0]["msg"]))
            continue

        intents.append(intent)
        out_rows.append(intent.model_dump())

    return intents, pd.DataFrame(out_rows), errors
