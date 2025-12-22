import requests
import time
from typing import Dict, Any

from adapters.dhan.errors import DhanSuperOrderError as _CompatError
from adapters.dhan.http_session import get_session

DHAN_BASE_URL = "https://api.dhan.co"


class DhanForeverOrderError(Exception):
    pass


def place_dhan_forever_order(*, intent, security_id: str, exchange_segment: str, client_id: str, access_token: str) -> Dict[str, Any]:
    """
    Place a Dhan Forever Order using the REST API.

    Mirrors the SDK's /forever/orders payload. Fire-and-forget semantics.
    """
    url = f"{DHAN_BASE_URL}/v2/forever/orders"

    headers = {
        "Content-Type": "application/json",
        "access-token": access_token,
    }

    # Build payload per Dhan's docs/SDK
    payload: Dict[str, Any] = {
        "dhanClientId": client_id,
        "orderFlag": intent.order_flag,
        "transactionType": intent.txn_type,
        "exchangeSegment": exchange_segment,
        "productType": intent.product,
        "orderType": intent.order_type,
        "validity": intent.validity,
        "securityId": security_id,
        "quantity": int(intent.qty),
        "disclosedQuantity": int(intent.disclosed_quantity or 0),
        "price": float(intent.price),
        "triggerPrice": float(intent.trigger_price),
        "price1": float(intent.price1) if intent.price1 is not None else 0.0,
        "triggerPrice1": float(intent.trigger_price1) if intent.trigger_price1 is not None else 0.0,
        "quantity1": int(intent.quantity1) if intent.quantity1 is not None else 0,
    }

    if getattr(intent, "tag", None) is not None:
        payload["correlationId"] = intent.tag

    # Retry once on timeout/connection to avoid long hangs
    max_attempts = 2
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = get_session().post(url, json=payload, headers=headers, timeout=5)
            break
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_exc = exc
            if attempt == max_attempts:
                raise DhanForeverOrderError(f"Forever Order request failed: {exc}") from exc
            time.sleep(0.5)

    if resp.status_code != 200:
        raise DhanForeverOrderError(
            f"Forever Order failed (HTTP {resp.status_code}): {resp.text}"
        )

    data = resp.json()

    if "errorCode" in data or "errorType" in data:
        msg = data.get("errorMessage", data.get("errorType", "Unknown error"))
        raise DhanForeverOrderError(
            f"Forever Order rejected by broker: {msg} (Code: {data.get('errorCode', 'N/A')})"
        )

    if "orderId" not in data:
        # SDK may wrap differently, but keep consistent with super order handling
        raise DhanForeverOrderError(f"Unexpected response format: {data}")

    if data.get("orderStatus") == "REJECTED":
        raise DhanForeverOrderError(
            f"Forever Order rejected. Order ID: {data.get('orderId')}, Status: REJECTED"
        )

    return data
