
import time
from exchanges.bybit.ws_client import BybitWebSocketClient

def handle_orderbook(message):
    """Process orderbook messages"""
    # Print only necessary information to avoid overwhelming output
    if message['type'] == 'snapshot':
        print(f"Orderbook snapshot for {message['data']['s']}")
        print(f"Best bid: {message['data']['b'][0][0]} ({message['data']['b'][0][1]})")
        print(f"Best ask: {message['data']['a'][0][0]} ({message['data']['a'][0][1]})")
    elif message['type'] == 'delta':
        print(f"Orderbook update for {message['data']['s']} - {len(message['data']['b'])} bids, {len(message['data']['a'])} asks changed")

def handle_trades(message):
    """Process trade messages"""
    for trade in message['data']:
        side = "Buy" if trade['S'] == "Buy" else "Sell"
        print(f"Trade: {side} {trade['v']} {message['data'][0]['s']} @ {trade['p']}")

if __name__ == "__main__":
    # Create WebSocket client instance
    client = BybitWebSocketClient(
        channel_type="linear",  # Using linear channel (USDT perpetuals)
        testnet=True,          # Using testnet for demonstration
        ping_interval=20,      # Send ping every 20 seconds
        reconnect_delay=5      # Wait 5 seconds before reconnecting on disconnection
    )
    
    # Connect to WebSocket server
    client.connect()
    
    # Wait for connection to establish
    time.sleep(2)
    
    # Subscribe to orderbook for BTC/USDT
    client.subscribe_orderbook("BTCUSDT", depth=50, callback=handle_orderbook)
    
    # Subscribe to trades for ETH/USDT
    client.subscribe_trades("ETHUSDT", callback=handle_trades)
    
    try:
        print("WebSocket client running. Press Ctrl+C to stop.")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Closing WebSocket connection...")
        client.close()
