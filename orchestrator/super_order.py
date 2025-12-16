"""
Super Order Orchestrator for Dhan

Handles the complete flow of placing a super order:
1. Validate order intent
2. Resolve instrument details
3. Authenticate with Dhan
4. Place super order
"""
from typing import Dict, Any
from validator.dhan_super_validator import DhanSuperOrderIntent
from validator.instruments.dhan_store import DhanStore
from apis.dhan.auth import authenticate
from adapters.dhan.super_order import place_dhan_super_order


class DhanSuperOrderError(Exception):
    """Raised when super order placement fails"""
    pass


class DhanSuperOrderOrchestrator:
    """
    Orchestrates the complete super order placement flow.
    """

    def __init__(self, client_id: str, access_token: str):
        """
        Initialize orchestrator with Dhan credentials.

        Args:
            client_id: Dhan client ID
            access_token: Dhan access token
        """
        self.client_id = client_id
        self.access_token = access_token
        self._instruments_loaded = False

    def ensure_instruments_loaded(self):
        """Load instrument data if not already loaded"""
        if not self._instruments_loaded:
            DhanStore.load()
            self._instruments_loaded = True

    def place_super_order(self, order_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Place a Dhan Super Order.

        Args:
            order_data: Dictionary containing order details
                Required fields:
                - symbol: Trading symbol (e.g., "HDFCBANK")
                - exchange: Exchange (e.g., "NSE", "NFO")
                - txn_type: "BUY" or "SELL"
                - qty: Quantity
                - order_type: "MARKET" or "LIMIT"
                - price: Price (required for LIMIT, None for MARKET)
                - product: "CNC", "INTRADAY", "MARGIN", or "MTF"
                - target_price: Target price
                - stop_loss_price: Stop loss price
                - trailing_jump: Trailing stop jump amount
                - order_category: Must be "SUPER"
                Optional fields:
                - tag: Correlation ID for tracking

        Returns:
            Dictionary with order response:
                - orderId: Order ID from Dhan
                - orderStatus: Order status ("PENDING", "TRANSIT", etc.)

        Raises:
            DhanSuperOrderError: If order placement fails
        """
        try:
            # Step 1: Ensure instruments are loaded
            self.ensure_instruments_loaded()

            # Step 2: Validate and create intent
            intent = DhanSuperOrderIntent(**order_data)

            # Step 3: Resolve instrument details
            instrument = DhanStore.lookup_symbol(intent.symbol)
            if instrument is None:
                raise DhanSuperOrderError(
                    f"Symbol '{intent.symbol}' not found in Dhan instruments"
                )

            # Step 4: Validate exchange mapping
            exchange_mapping = {
                "NSE": "NSE_EQ",
                "BSE": "BSE_EQ",
                "NFO": "NSE_FNO",
                "BFO": "BSE_FNO",
                "MCX": "MCX",
            }

            expected_segment = exchange_mapping.get(intent.exchange)
            if expected_segment is None:
                raise DhanSuperOrderError(
                    f"Exchange '{intent.exchange}' not supported"
                )

            if instrument.exchange_segment != expected_segment:
                raise DhanSuperOrderError(
                    f"Exchange mismatch for {intent.symbol}: "
                    f"Expected {expected_segment}, got {instrument.exchange_segment}"
                )

            # Step 5: Validate lot size for derivatives
            if instrument.is_derivative:
                if intent.qty % instrument.lot_size != 0:
                    raise DhanSuperOrderError(
                        f"Quantity {intent.qty} must be a multiple of lot size {instrument.lot_size}"
                    )

            # Step 6: Place the super order
            result = place_dhan_super_order(
                intent=intent,
                security_id=instrument.security_id,
                exchange_segment=instrument.exchange_segment,
                client_id=self.client_id,
                access_token=self.access_token,
            )

            return result

        except Exception as e:
            if isinstance(e, DhanSuperOrderError):
                raise
            raise DhanSuperOrderError(f"Super order placement failed: {str(e)}") from e

    def place_super_order_from_intent(
        self, intent: DhanSuperOrderIntent
    ) -> Dict[str, Any]:
        """
        Place a super order from a validated intent object.

        Args:
            intent: Validated DhanSuperOrderIntent

        Returns:
            Dictionary with order response

        Raises:
            DhanSuperOrderError: If order placement fails
        """
        return self.place_super_order(intent.model_dump())
