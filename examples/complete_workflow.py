"""
Complete Integration Example

This demonstrates the full super order placement workflow
from start to finish, including all validation steps.
"""
from orchestrator.super_order import DhanSuperOrderOrchestrator, DhanSuperOrderError


def complete_workflow_example():
    """
    Complete example showing the entire super order flow.
    
    This example demonstrates:
    1. Setup and initialization
    2. Order validation
    3. Instrument resolution
    4. API call
    5. Response handling
    6. Error handling
    """
    
    print("=" * 70)
    print("COMPLETE SUPER ORDER WORKFLOW EXAMPLE")
    print("=" * 70)
    
    # ========================================================================
    # STEP 1: Initialize Orchestrator
    # ========================================================================
    print("\n[STEP 1] Initializing orchestrator...")
    
    # In production, load from environment variables:
    # from dotenv import load_dotenv
    # import os
    # load_dotenv()
    # client_id = os.getenv("DHAN_CLIENT_ID")
    # access_token = os.getenv("DHAN_ACCESS_TOKEN")
    
    # For this example (replace with your credentials):
    client_id = "1000000003"
    access_token = "your_access_token_here"
    
    orchestrator = DhanSuperOrderOrchestrator(
        client_id=client_id,
        access_token=access_token
    )
    
    print("✅ Orchestrator initialized")
    
    
    # ========================================================================
    # STEP 2: Define Order Intent
    # ========================================================================
    print("\n[STEP 2] Defining order intent...")
    
    order_intent = {
        # Basic order details
        "symbol": "HDFCBANK",           # Trading symbol
        "exchange": "NSE",              # Exchange (NSE, BSE, NFO, etc.)
        "txn_type": "BUY",              # BUY or SELL
        "qty": 1,                       # Quantity
        
        # Order type and price
        "order_type": "LIMIT",          # LIMIT or MARKET
        "price": 1500.0,                # Entry price (None for MARKET)
        
        # Product type
        "product": "CNC",               # CNC, INTRADAY, MARGIN, or MTF
        
        # Super Order specific fields
        "target_price": 1600.0,         # Target (take profit)
        "stop_loss_price": 1400.0,      # Stop loss
        "trailing_jump": 10.0,          # Trailing amount (0 for no trail)
        
        # Required for super orders
        "order_category": "SUPER",
        
        # Optional
        "tag": "example_order_001"      # For tracking
    }
    
    print("📋 Order Details:")
    print(f"   Symbol: {order_intent['symbol']}")
    print(f"   Type: {order_intent['txn_type']} {order_intent['order_type']}")
    print(f"   Entry: ₹{order_intent['price']}")
    print(f"   Target: ₹{order_intent['target_price']} (+₹{order_intent['target_price'] - order_intent['price']})")
    print(f"   Stop Loss: ₹{order_intent['stop_loss_price']} (-₹{order_intent['price'] - order_intent['stop_loss_price']})")
    print(f"   Trailing: ₹{order_intent['trailing_jump']}")
    
    
    # ========================================================================
    # STEP 3: Validate and Place Order
    # ========================================================================
    print("\n[STEP 3] Placing order (with automatic validation)...")
    
    try:
        # The orchestrator handles:
        # - Loading instrument data
        # - Validating order intent
        # - Resolving symbol to security_id
        # - Checking exchange mapping
        # - Validating lot sizes (for derivatives)
        # - Making API call
        # - Handling errors
        
        result = orchestrator.place_super_order(order_intent)
        
        # ====================================================================
        # STEP 4: Handle Success
        # ====================================================================
        print("\n[STEP 4] Order placed successfully!")
        print("=" * 70)
        print("✅ SUCCESS")
        print("=" * 70)
        print(f"Order ID: {result['orderId']}")
        print(f"Status: {result['orderStatus']}")
        print("\nYour super order has been placed with Dhan.")
        print("Check your Dhan app for real-time updates.")
        
        return result
        
    except DhanSuperOrderError as e:
        # ====================================================================
        # STEP 4 (Alternative): Handle Errors
        # ====================================================================
        print("\n[STEP 4] Order placement failed!")
        print("=" * 70)
        print("❌ ERROR")
        print("=" * 70)
        print(f"Error: {e}")
        
        # Error types you might encounter:
        print("\nCommon error types:")
        print("  • Validation Error: Check price relationships, lot sizes")
        print("  • Symbol Not Found: Symbol might be incorrect")
        print("  • Exchange Mismatch: Symbol on different exchange")
        print("  • Authentication Error: Check credentials")
        print("  • Broker Rejection: Insufficient funds, trading halted, etc.")
        
        raise


def batch_order_example():
    """
    Example: Place multiple super orders in sequence
    """
    print("\n" + "=" * 70)
    print("BATCH ORDER EXAMPLE")
    print("=" * 70)
    
    orchestrator = DhanSuperOrderOrchestrator(
        client_id="1000000003",
        access_token="your_access_token_here"
    )
    
    # Define multiple orders
    orders = [
        {
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
            "tag": "batch_order_1"
        },
        {
            "symbol": "RELIANCE",
            "exchange": "NSE",
            "txn_type": "BUY",
            "qty": 1,
            "order_type": "LIMIT",
            "price": 2500.0,
            "product": "CNC",
            "target_price": 2600.0,
            "stop_loss_price": 2450.0,
            "trailing_jump": 5.0,
            "order_category": "SUPER",
            "tag": "batch_order_2"
        }
    ]
    
    results = []
    
    for i, order in enumerate(orders, 1):
        print(f"\n[Order {i}/{len(orders)}] Placing {order['symbol']}...")
        
        try:
            result = orchestrator.place_super_order(order)
            print(f"✅ Success: Order ID {result['orderId']}")
            results.append({
                "order": order,
                "result": result,
                "status": "success"
            })
            
        except DhanSuperOrderError as e:
            print(f"❌ Failed: {e}")
            results.append({
                "order": order,
                "error": str(e),
                "status": "failed"
            })
    
    # Summary
    print("\n" + "=" * 70)
    print("BATCH ORDER SUMMARY")
    print("=" * 70)
    
    successful = sum(1 for r in results if r["status"] == "success")
    failed = sum(1 for r in results if r["status"] == "failed")
    
    print(f"Total Orders: {len(orders)}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    
    return results


def conditional_order_example():
    """
    Example: Place order based on conditions
    """
    print("\n" + "=" * 70)
    print("CONDITIONAL ORDER EXAMPLE")
    print("=" * 70)
    
    orchestrator = DhanSuperOrderOrchestrator(
        client_id="1000000003",
        access_token="your_access_token_here"
    )
    
    # Hypothetical: Get current price (you'd use market data API)
    current_price = 1520.0
    symbol = "HDFCBANK"
    
    # Place order only if price is within range
    if 1480 <= current_price <= 1520:
        print(f"✅ Price {current_price} is in range, placing order...")
        
        # Calculate dynamic target and SL based on current price
        entry_price = current_price
        target_price = entry_price * 1.05  # 5% profit
        stop_loss_price = entry_price * 0.98  # 2% loss
        
        order = {
            "symbol": symbol,
            "exchange": "NSE",
            "txn_type": "BUY",
            "qty": 1,
            "order_type": "MARKET",  # Execute immediately
            "price": None,
            "product": "INTRADAY",
            "target_price": round(target_price, 2),
            "stop_loss_price": round(stop_loss_price, 2),
            "trailing_jump": 5.0,
            "order_category": "SUPER",
        }
        
        print(f"Entry: ₹{entry_price}")
        print(f"Target: ₹{order['target_price']} (+{((target_price/entry_price - 1) * 100):.1f}%)")
        print(f"SL: ₹{order['stop_loss_price']} ({((stop_loss_price/entry_price - 1) * 100):.1f}%)")
        
        try:
            result = orchestrator.place_super_order(order)
            print(f"\n✅ Order placed: {result['orderId']}")
            return result
        except DhanSuperOrderError as e:
            print(f"\n❌ Order failed: {e}")
            raise
    else:
        print(f"⚠️  Price {current_price} is out of range [1480-1520], skipping order")
        return None


if __name__ == "__main__":
    """
    Run examples
    
    Uncomment the example you want to run:
    """
    
    print("\n⚠️  WARNING: These examples use placeholder credentials")
    print("⚠️  Replace with your actual Dhan credentials before running\n")
    
    # Example 1: Complete workflow (recommended to start)
    # complete_workflow_example()
    
    # Example 2: Multiple orders in batch
    # batch_order_example()
    
    # Example 3: Conditional order placement
    # conditional_order_example()
    
    print("\n💡 TIP: Uncomment one of the examples above to run it")
    print("💡 TIP: Update credentials in the example before running")
