import time
from exchanges.okx.ws_client import OkxWebSocketClient

def handle_orderbook(message):
    """Process orderbook messages"""
    # Get data from the message
    arg = message.get("arg", {})
    inst_id = arg.get("instId", "")
    action = message.get("action", "")
    
    # Process different message types
    if action == "snapshot":
        data = message.get("data", [{}])[0]
        print(f"Orderbook snapshot for {inst_id}")
        if data.get("bids") and data.get("asks"):
            best_bid = data["bids"][0]
            best_ask = data["asks"][0]
            print(f"Best bid: {best_bid[0]} ({best_bid[1]})")
            print(f"Best ask: {best_ask[0]} ({best_ask[1]})")
    elif action == "update":
        data = message.get("data", [{}])[0]
        print(f"Orderbook update for {inst_id} - {len(data.get('bids', []))} bids, {len(data.get('asks', []))} asks changed")

def handle_trades(message):
    """Process trade messages"""
    arg = message.get("arg", {})
    inst_id = arg.get("instId", "")
    data = message.get("data", [])
    
    for trade in data:
        side = "Buy" if trade.get("side") == "buy" else "Sell"
        sz = trade.get("sz", "0")
        px = trade.get("px", "0")
        print(f"Trade: {side} {sz} {inst_id} @ {px}")

if __name__ == "__main__":
    # Create WebSocket client instance
    client = OkxWebSocketClient(
        testnet=True,          # Using testnet for demonstration
        ping_interval=20,      # Send ping every 20 seconds
        reconnect_interval=5   # Wait 5 seconds before reconnecting on disconnection
    )
    
    # Connect to WebSocket server
    print("Connecting to OKX WebSocket...")
    client.connect()
    
    # Wait for connection to establish
    time.sleep(2)
    
    # Subscribe to orderbook for BTC-USDT
    print("Subscribing to BTC-USDT orderbook...")
    client.subscribe_orderbook("BTC-USDT", depth="books5", callback=handle_orderbook)
    
    # Subscribe to trades for ETH-USDT
    print("Subscribing to ETH-USDT trades...")
    client.subscribe_trades("ETH-USDT", callback=handle_trades)
    
    print("WebSocket client running. Press Ctrl+C to stop.")
    print("Waiting for data (might take a few seconds)...")
    
    try:
        counter = 0
        while True:
            # Get some data directly from the client
            btc_data = client.get_data("BTC-USDT")
            eth_data = client.get_data("ETH-USDT")
            
            if counter % 5 == 0:  # Print status every 5 seconds
                print("\n--- Current Market Data ---")
                
                if btc_data:
                    print(f"BTC-USDT: Bid = {btc_data.get('bid')} ({btc_data.get('bid_qty')}), Ask = {btc_data.get('ask')} ({btc_data.get('ask_qty')})")
                else:
                    print("Waiting for BTC-USDT data...")
                    
                if eth_data:
                    print(f"ETH-USDT: Bid = {eth_data.get('bid')} ({eth_data.get('bid_qty')}), Ask = {eth_data.get('ask')} ({eth_data.get('ask_qty')})")
                else:
                    print("Waiting for ETH-USDT data...")
                    
            time.sleep(1)
            counter += 1
    except KeyboardInterrupt:
        print("Closing WebSocket connection...")
        client.close()
        print("Connection closed. Exiting.")
