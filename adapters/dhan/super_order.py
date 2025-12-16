import requests
from typing import Dict, Any
from adapters.dhan.errors import DhanSuperOrderError

DHAN_BASE_URL = "https://api.dhan.co"


def place_dhan_super_order(
    *,
    intent,
    security_id: str,
    exchange_segment: str,
    client_id: str,
    access_token: str,
) -> Dict[str, Any]:
    """
    Place a Dhan Super Order.

    Assumptions:
    - intent is a validated DhanSuperOrderIntent
    - security_id & exchange_segment are resolved
    - client_id & access_token are valid
    """

    url = f"{DHAN_BASE_URL}/v2/super/orders"

    headers = {
        "Content-Type": "application/json",
        "access-token": access_token,
    }

    payload = {
        "dhanClientId": client_id,
        "securityId": security_id,
        "exchangeSegment": exchange_segment,
        "transactionType": intent.txn_type,
        "quantity": intent.qty,
        "orderType": intent.order_type,
        "productType": intent.product,
        "targetPrice": intent.target_price,
        "stopLossPrice": intent.stop_loss_price,
        "trailingJump": intent.trailing_jump,
    }

    if intent.order_type == "LIMIT":
        payload["price"] = intent.price

    if intent.tag:
        payload["correlationId"] = intent.tag

    response = requests.post(
        url,
        json=payload,
        headers=headers,
        timeout=10,
    )

    if response.status_code != 200:
        raise DhanSuperOrderError(
            f"Super Order failed (HTTP {response.status_code}): {response.text}"
        )

    data = response.json()

    # Check if response contains an error
    if "errorCode" in data or "errorType" in data:
        error_msg = data.get("errorMessage", data.get("errorType", "Unknown error"))
        raise DhanSuperOrderError(
            f"Super Order rejected by broker: {error_msg} (Code: {data.get('errorCode', 'N/A')})"
        )

    # Check for orderId to confirm success
    if "orderId" not in data:
        raise DhanSuperOrderError(
            f"Unexpected response format: {data}"
        )

    # Check if order was rejected
    if data.get("orderStatus") == "REJECTED":
        raise DhanSuperOrderError(
            f"Super Order rejected. Order ID: {data.get('orderId')}, Status: REJECTED"
        )

    return data
