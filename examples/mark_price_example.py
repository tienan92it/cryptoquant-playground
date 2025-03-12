import sys
import time
from pathlib import Path

# Add parent directory to path to import our module
sys.path.append(str(Path(__file__).parent.parent))

from exchanges.binance.ws_client import BinanceWebSocketClient

def example_individual_futures():
    """Example using individual symbol mark price streams for futures"""
    print("\n=== Individual Symbol Mark Price Stream Example (Futures) ===")
    
    # Create client for specific symbols
    client = BinanceWebSocketClient(
        spot_symbols=["btcusdt", "ethusdt"],        # Spot symbols for book ticker
        futures_symbols=["btcusdt", "ethusdt"],     # Futures symbols for mark price
        mark_price_freq="1s"                        # 1 second updates
    )
    
    # Connect to WebSocket
    client.connect()
    
    # Give some time for data to arrive
    print("Waiting for data...")
    time.sleep(3)
    
    try:
        # Display mark price data for 10 seconds
        for _ in range(5):
            print("\nCurrent Mark Price Data:")
            mark_price_data = client.get_mark_price_data()
            for symbol, data in mark_price_data.items():
                print(f"{symbol}: Mark Price: {data['mark_price']}, Funding Rate: {data['funding_rate']}")
            
            print("\nCurrent Spot Data:")
            spot_data = client.get_spot_data()
            for symbol, data in spot_data.items():
                print(f"{symbol}: Bid: {data['bid']}, Ask: {data['ask']}")
                
            time.sleep(2)
    
    finally:
        # Always close the connection
        client.close()

def example_all_futures_market():
    """Example using all-market mark price stream for futures"""
    print("\n=== All Market Mark Price Stream Example (Futures) ===")
    
    # Create client with all-market stream
    client = BinanceWebSocketClient(
        use_all_market_stream=True,          # Use all-market mark price stream
        mark_price_freq="1s"                 # 1 second updates
    )
    
    # Connect to WebSocket
    client.connect()
    
    # Give some time for data to arrive
    print("Waiting for data...")
    time.sleep(3)
    
    try:
        # Specific symbols to monitor (even though we receive all)
        symbols_to_watch = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "DOGEUSDT"]
        
        # Display mark price data for 10 seconds
        for _ in range(5):
            print("\nCurrent Mark Price Data:")
            
            # Use the helper method to get specific symbols
            for symbol in symbols_to_watch:
                data = client.get_mark_price(symbol)
                if data:
                    print(f"{symbol}: Mark Price: {data['mark_price']}, " +
                          f"Funding Rate: {data['funding_rate']}, " +
                          f"Next Funding: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(data['next_funding_time']/1000))}")
                else:
                    print(f"{symbol}: No data available")
            
            time.sleep(2)
    
    finally:
        # Always close the connection
        client.close()

def example_testnet_usage():
    """Example using the testnet for futures"""
    print("\n=== Testnet Usage Example ===")
    
    # Create client with testnet enabled
    client = BinanceWebSocketClient(
        use_all_market_stream=True,
        mark_price_freq="3s",
        testnet=True                        # Enable testnet for futures
    )
    
    # Connect to WebSocket
    client.connect()
    
    # Give some time for data to arrive
    print("Waiting for data...")
    time.sleep(3)
    
    try:
        # Display mark price data from testnet
        for _ in range(3):
            print("\nCurrent Mark Price Data (Testnet):")
            mark_price_data = client.get_mark_price_data()
            
            # Show first 5 symbols
            for i, (symbol, data) in enumerate(mark_price_data.items()):
                if i >= 5:
                    break
                print(f"{symbol}: Mark Price: {data['mark_price']}, Funding Rate: {data['funding_rate']}")
            
            time.sleep(2)
    
    finally:
        # Always close the connection
        client.close()

if __name__ == "__main__":
    print("Binance WebSocket Client - Futures Mark Price Examples")
    
    # Run examples
    example_individual_futures()
    example_all_futures_market()
    example_testnet_usage()
    
    print("\nAll examples completed!")
