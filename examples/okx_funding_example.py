import sys
import time
from pathlib import Path
from datetime import datetime

# Add parent directory to path to import our module
sys.path.append(str(Path(__file__).parent.parent))

from exchanges.okx.ws_client import OkxWebSocketClient

def format_timestamp(timestamp_ms):
    """Format millisecond timestamp to readable date/time"""
    return datetime.fromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')

def format_countdown(target_time_ms):
    """Format countdown to next funding time"""
    now_ms = int(time.time() * 1000)
    seconds_remaining = max(0, (target_time_ms - now_ms) // 1000)
    
    hours = seconds_remaining // 3600
    minutes = (seconds_remaining % 3600) // 60
    seconds = seconds_remaining % 60
    
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

def handle_funding_rate(message):
    """Callback function for funding rate updates"""
    if 'data' in message:
        data = message['data'][0]
        inst_id = data.get('instId', 'Unknown')
        funding_rate = float(data.get('fundingRate', 0)) * 100
        next_time = int(data.get('nextFundingTime', 0))
        next_formatted = format_timestamp(next_time)
        countdown = format_countdown(next_time)
        
        print(f"Funding update - {inst_id}: Current rate: {funding_rate:.6f}%, Next funding: {next_formatted} (in {countdown})")

if __name__ == "__main__":
    print("OKX Funding Rate Monitor")
    
    # List of popular perpetual swaps
    symbols = [
        "BTC-USD-SWAP",
        "ETH-USD-SWAP",
        "SOL-USD-SWAP",
        "DOGE-USD-SWAP",
        "XRP-USD-SWAP",
    ]
    
    # Create client
    client = OkxWebSocketClient(testnet=False)
    client.connect()
    
    try:
        # Wait for connection to establish
        time.sleep(1)
        
        # Subscribe to funding rates for all symbols
        for symbol in symbols:
            client.subscribe_funding_rate(symbol, callback=handle_funding_rate)
            print(f"Subscribed to funding rates for {symbol}")
        
        # Loop to display funding data periodically
        print("\nWaiting for funding rate updates...")
        
        for _ in range(30):
            time.sleep(5)
            
            print("\nCurrent Funding Rates:")
            print("-" * 80)
            print(f"{'Symbol':<15} {'Rate':<10} {'Next Funding':<20} {'Countdown':<10} {'Premium':<10}")
            print("-" * 80)
            
            for symbol in symbols:
                funding_data = client.get_funding_rate_data(symbol)
                if funding_data:
                    rate = funding_data.get('funding_rate', 0) * 100
                    next_time = funding_data.get('next_funding_time', 0)
                    premium = funding_data.get('premium', 0) * 100
                    
                    next_formatted = format_timestamp(next_time) if next_time else "N/A"
                    countdown = format_countdown(next_time) if next_time else "N/A"
                    
                    print(f"{symbol:<15} {rate:>+.6f}% {next_formatted:<20} {countdown:<10} {premium:>+.6f}%")
                else:
                    print(f"{symbol:<15} {'No data':<10}")
            
    except KeyboardInterrupt:
        print("\nClosing connections...")
    finally:
        # Clean up
        client.close()
        print("Connections closed")
