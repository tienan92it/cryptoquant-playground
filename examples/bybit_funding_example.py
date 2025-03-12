import sys
import time
import json
from pathlib import Path
from datetime import datetime
from tabulate import tabulate
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("bybit_funding_example")

# Add parent directory to path to import our module
sys.path.append(str(Path(__file__).parent.parent))

from exchanges.bybit.ws_client import BybitWebSocketClient

def format_timestamp(timestamp_ms):
    """Format millisecond timestamp to readable date/time"""
    if not timestamp_ms:
        return "N/A"
    return datetime.fromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')

def format_countdown(target_time_ms):
    """Format countdown to next funding time"""
    if not target_time_ms:
        return "N/A"
        
    now_ms = int(time.time() * 1000)
    seconds_remaining = max(0, (target_time_ms - now_ms) // 1000)
    
    hours = seconds_remaining // 3600
    minutes = (seconds_remaining % 3600) // 60
    seconds = seconds_remaining % 60
    
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

def handle_ticker(message):
    """Callback function for ticker updates"""
    try:
        topic = message.get('topic', '')
        if not topic.startswith('tickers.'):
            return
            
        symbol = topic.split('.')[1]
        data = message.get('data', {})
        
        # Debug data content
        logger.info(f"Received ticker update for {symbol}")
        logger.info(f"Message type: {message.get('type', 'unknown')}")
        
        if isinstance(data, list) and data:
            data = data[0]  # Use the first item if it's a list
            
        # Print raw data to see what we're actually getting
        logger.info(f"Raw data fields: {list(data.keys())}")
        
        if data:
            # Try to extract funding rate data
            funding_rate = data.get('fundingRate', None)
            if funding_rate is not None:
                funding_rate = float(funding_rate) * 100  # Convert to percentage
                
                next_funding_time = int(data.get('nextFundingTime', 0))
                next_formatted = format_timestamp(next_funding_time)
                countdown = format_countdown(next_funding_time)
                
                print(f"Ticker update - {symbol}: Current funding rate: {funding_rate:.6f}%, Next funding: {next_formatted} (in {countdown})")
            else:
                print(f"No funding rate data for {symbol} in this update")
    except Exception as e:
        logger.error(f"Error processing ticker message: {e}")
        logger.error(f"Raw message: {message}")

if __name__ == "__main__":
    print("Bybit Funding Rate Monitor")
    
    # Enable debug logging in the Bybit client
    logging.getLogger("BybitWebSocketClient").setLevel(logging.DEBUG)
    
    # List of popular perpetual contracts
    # Make sure the symbols match Bybit's format exactly
    symbols = [
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT",
        "DOGEUSDT",
        "XRPUSDT",
    ]
    
    # Create client for linear perpetuals
    client = BybitWebSocketClient(channel_type="linear", testnet=False)
    client.connect()
    
    try:
        # Wait longer for connection to establish
        print("Waiting for connection to establish...")
        time.sleep(5)
        
        # Subscribe to ticker data for all symbols
        for symbol in symbols:
            print(f"Subscribing to ticker data for {symbol}...")
            success = client.subscribe_ticker(symbol, callback=handle_ticker)
            print(f"Subscription {'successful' if success else 'failed'} for {symbol}")
        
        # Wait for initial data
        print("\nWaiting for initial ticker updates...")
        time.sleep(10)  # Give more time for initial data to arrive
        
        # Display current data for diagnosis
        print("\nCurrent stored ticker data:")
        for symbol in symbols:
            ticker_data = client.get_ticker_data(symbol)
            print(f"\n{symbol}:")
            if ticker_data:
                for key, value in ticker_data.items():
                    print(f"  {key}: {value}")
            else:
                print("  No data available")
        
        # Loop to display funding data periodically
        for _ in range(30):  # Run for 30 iterations
            # Prepare data for table
            data_rows = []
            for symbol in symbols:
                ticker_data = client.get_ticker_data(symbol)
                
                if ticker_data:
                    funding_rate = ticker_data.get('funding_rate', 0) * 100  # Convert to percentage
                    mark_price = ticker_data.get('mark_price', 0)
                    next_funding_time = ticker_data.get('next_funding_time', 0)
                    
                    next_formatted = format_timestamp(next_funding_time)
                    countdown = format_countdown(next_funding_time)
                    
                    # Calculate estimated funding payment
                    position_size = 1000  # $1000 position
                    est_payment = (funding_rate / 100) * position_size * -1  # Negative for short position
                    
                    data_rows.append([
                        symbol,
                        f"{mark_price:.2f}" if mark_price else "N/A",
                        f"{funding_rate:+.6f}%" if funding_rate != 0 else "N/A",
                        next_formatted,
                        countdown,
                        f"${est_payment:.4f}" if funding_rate != 0 else "N/A"
                    ])
                else:
                    data_rows.append([symbol, "No data", "-", "-", "-", "-"])
            
            # Print table
            print("\n" + "=" * 80)
            print(f"BYBIT FUNDING RATE MONITOR - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 80)
            
            headers = ["Symbol", "Mark Price", "Funding Rate", "Next Funding", "Countdown", "Est. Payment ($1000)"]
            print(tabulate(data_rows, headers=headers, tablefmt="simple"))
            
            print("\nNote: Estimated payment shows what you would receive for a $1000 SHORT position")
            print("      (Negative value means you pay, positive means you receive)")
            
            # Sleep before next update
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\nClosing connection...")
    finally:
        client.close()
        print("Connection closed")
