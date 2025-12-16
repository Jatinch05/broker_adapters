"""
Dhan Broker Adapter - Main Entry Point

This module demonstrates how to place Dhan Super Orders using the broker adapter.

Super Orders are advanced bracket orders that include:
- Entry leg (main order)
- Target leg (take profit)
- Stop loss leg (with optional trailing)
"""
import os
from dotenv import load_dotenv
from orchestrator.super_order import DhanSuperOrderOrchestrator, DhanSuperOrderError
from validator.instruments.dhan_refresher import refresh_dhan_instruments


def setup_instruments():
    """
    Download and setup the latest Dhan instrument master.
    Run this once at the start or periodically to keep instruments updated.
    """
    print("Downloading latest Dhan instruments...")
    try:
        csv_path = refresh_dhan_instruments()
        print(f"✅ Instruments downloaded successfully: {csv_path}")
    except Exception as e:
        print(f"❌ Failed to download instruments: {e}")
        raise


def example_buy_limit_super_order():
    """
    Example: Place a BUY LIMIT Super Order
    
    Scenario:
    - Buy HDFCBANK at ₹1500
    - Target: ₹1600 (₹100 profit)
    - Stop Loss: ₹1400 (₹100 loss protection)
    - Trailing: ₹10 (trail SL by ₹10 as price moves in favor)
    """
    # Load credentials from environment
    load_dotenv()
    client_id = os.getenv("DHAN_CLIENT_ID")
    access_token = os.getenv("DHAN_ACCESS_TOKEN")

    if not client_id or not access_token:
        print("⚠️  Please set DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN in .env file")
        return

    # Create orchestrator
    orchestrator = DhanSuperOrderOrchestrator(client_id, access_token)

    # Define order
    order = {
        "symbol": "HDFCBANK",
        "exchange": "NSE",
        "txn_type": "BUY",
        "qty": 1,
        "order_type": "LIMIT",
        "price": 1500.0,
        "product": "CNC",
        "target_price": 1600.0,
        "stop_loss_price": 1400.0,
        "trailing_jump": 10.0,
        "order_category": "SUPER",
        "tag": "my_super_order_001"
    }

    try:
        print("\n📋 Placing BUY LIMIT Super Order...")
        print(f"Symbol: {order['symbol']}")
        print(f"Entry: ₹{order['price']}")
        print(f"Target: ₹{order['target_price']}")
        print(f"Stop Loss: ₹{order['stop_loss_price']}")
        print(f"Trailing: ₹{order['trailing_jump']}")
        
        result = orchestrator.place_super_order(order)
        
        print(f"\n✅ Super Order Placed Successfully!")
        print(f"Order ID: {result['orderId']}")
        print(f"Status: {result['orderStatus']}")
        
        return result

    except DhanSuperOrderError as e:
        print(f"\n❌ Super Order Failed: {e}")
        raise


def example_sell_market_super_order():
    """
    Example: Place a SELL MARKET Super Order
    
    Scenario:
    - Sell RELIANCE at market price
    - Target: ₹2400 (sell side target)
    - Stop Loss: ₹2600 (sell side protection)
    - No trailing
    """
    load_dotenv()
    client_id = os.getenv("DHAN_CLIENT_ID")
    access_token = os.getenv("DHAN_ACCESS_TOKEN")

    if not client_id or not access_token:
        print("⚠️  Please set DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN in .env file")
        return

    orchestrator = DhanSuperOrderOrchestrator(client_id, access_token)

    order = {
        "symbol": "RELIANCE",
        "exchange": "NSE",
        "txn_type": "SELL",
        "qty": 1,
        "order_type": "MARKET",
        "price": None,  # No price for MARKET orders
        "product": "INTRADAY",
        "target_price": 2400.0,
        "stop_loss_price": 2600.0,
        "trailing_jump": 0.0,  # No trailing
        "order_category": "SUPER",
    }

    try:
        print("\n📋 Placing SELL MARKET Super Order...")
        print(f"Symbol: {order['symbol']}")
        print(f"Target: ₹{order['target_price']}")
        print(f"Stop Loss: ₹{order['stop_loss_price']}")
        
        result = orchestrator.place_super_order(order)
        
        print(f"\n✅ Super Order Placed Successfully!")
        print(f"Order ID: {result['orderId']}")
        print(f"Status: {result['orderStatus']}")
        
        return result

    except DhanSuperOrderError as e:
        print(f"\n❌ Super Order Failed: {e}")
        raise


def example_futures_super_order():
    """
    Example: Place a Futures Super Order
    
    Note: Quantity must be a multiple of lot size
    """
    load_dotenv()
    client_id = os.getenv("DHAN_CLIENT_ID")
    access_token = os.getenv("DHAN_ACCESS_TOKEN")

    if not client_id or not access_token:
        print("⚠️  Please set DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN in .env file")
        return

    orchestrator = DhanSuperOrderOrchestrator(client_id, access_token)

    # Example: NIFTY Futures (lot size is typically 50)
    order = {
        "symbol": "NIFTY 27 FEB 2025 FUT",  # Adjust symbol as per actual instrument
        "exchange": "NFO",
        "txn_type": "BUY",
        "qty": 50,  # Must be multiple of lot size
        "order_type": "LIMIT",
        "price": 23500.0,
        "product": "MARGIN",
        "target_price": 23600.0,
        "stop_loss_price": 23400.0,
        "trailing_jump": 20.0,
        "order_category": "SUPER",
    }

    try:
        print("\n📋 Placing Futures Super Order...")
        print(f"Symbol: {order['symbol']}")
        print(f"Quantity: {order['qty']}")
        print(f"Entry: ₹{order['price']}")
        
        result = orchestrator.place_super_order(order)
        
        print(f"\n✅ Super Order Placed Successfully!")
        print(f"Order ID: {result['orderId']}")
        print(f"Status: {result['orderStatus']}")
        
        return result

    except DhanSuperOrderError as e:
        print(f"\n❌ Super Order Failed: {e}")
        raise


def main():
    """
    Main function - demonstrates complete super order flow
    """
    print("=" * 60)
    print("Dhan Super Order - Broker Adapter Demo")
    print("=" * 60)

    # Step 1: Setup instruments (download latest data)
    print("\n[1/2] Setting up instruments...")
    try:
        setup_instruments()
    except Exception as e:
        print(f"Failed to setup instruments: {e}")
        print("Please ensure you have internet connection.")
        return

    # Step 2: Place super orders
    print("\n[2/2] Ready to place super orders!")
    print("\nAvailable examples:")
    print("  1. BUY LIMIT Super Order (HDFCBANK)")
    print("  2. SELL MARKET Super Order (RELIANCE)")
    print("  3. Futures Super Order (NIFTY)")
    print("\n⚠️  Note: Uncomment the example you want to run below")
    print("⚠️  Make sure to set DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN in .env file")

    # Uncomment one of these to test:
    # example_buy_limit_super_order()
    # example_sell_market_super_order()
    # example_futures_super_order()

    print("\n✅ Setup complete! Edit main.py to run examples.")


if __name__ == "__main__":
    main()
