"""
Dhan Super Order placement adapter.
"""
import requests
import time
import logging
from typing import Dict, Any
from core.models import Order, OrderResult, OrderStatus
from core.exceptions import OrderPlacementError

logger = logging.getLogger(__name__)

DHAN_BASE_URL = "https://api.dhan.co"


def place_dhan_super_order(
    order: Order,
    client_id: str,
    access_token: str,
    security_id: str,
    exchange_segment: str,
) -> OrderResult:
    """
    Place a Dhan Super Order via their API.
    
    Args:
        order: Order object with all details
        client_id: Dhan client ID
        access_token: Dhan access token
        security_id: Dhan security ID (resolved from instrument lookup)
        exchange_segment: Exchange segment (NSE, NFO, BSE, BFO, etc.)
    
    Returns:
        OrderResult with order ID and status
    
    Raises:
        OrderPlacementError if placement fails
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
        "transactionType": order.txn_type.value,
        "quantity": order.qty,
        "orderType": order.order_type.value,
        "productType": order.product.value,
    }
    
    if order.order_type.value == "LIMIT" and order.price:
        payload["price"] = order.price
    
    if order.target_price:
        payload["targetPrice"] = order.target_price
    
    if order.stop_loss_price:
        payload["stopLossPrice"] = order.stop_loss_price
    
    if order.trailing_jump:
        payload["trailingJump"] = order.trailing_jump
    
    if order.tag:
        payload["correlationId"] = order.tag
    
    # Retry once on timeout/connection errors
    max_attempts = 2
    last_exc = None
    
    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.post(
                url,
                json=payload,
                headers=headers,
                timeout=5,
            )
            break
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_exc = exc
            if attempt == max_attempts:
                logger.error(f"Super Order request failed after {max_attempts} attempts: {exc}")
                raise OrderPlacementError(f"Connection failed: {exc}") from exc
            time.sleep(0.5)
    
    # Check HTTP status
    if response.status_code != 200:
        logger.error(f"Super Order HTTP {response.status_code}: {response.text}")
        raise OrderPlacementError(
            f"HTTP {response.status_code}: {response.text}"
        )
    
    data = response.json()
    
    # Check for broker-side errors
    if "errorCode" in data or "errorType" in data:
        error_msg = data.get("errorMessage", data.get("errorType", "Unknown error"))
        logger.error(f"Broker error: {error_msg}")
        raise OrderPlacementError(f"Broker rejected: {error_msg}")
    
    # Check for order ID
    if "orderId" not in data:
        logger.error(f"Unexpected response: {data}")
        raise OrderPlacementError(f"No orderId in response")
    
    # Check for REJECTED status
    if data.get("orderStatus") == "REJECTED":
        logger.error(f"Order rejected: {data}")
        raise OrderPlacementError(f"Order rejected by broker")
    
    logger.info(f"Order placed successfully: orderId={data.get('orderId')}, status={data.get('orderStatus')}")
    
    return OrderResult(
        order_id=data.get("orderId"),
        status=OrderStatus.ACCEPTED,
        message="Order accepted by broker",
        broker_response=data,
    )
