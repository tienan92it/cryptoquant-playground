import sys
import time
import json
from pathlib import Path
from datetime import datetime
import traceback
from tabulate import tabulate
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("bybit_funding_example")

# Add parent directory to path to import our module
sys.path.append(str(Path(__file__).parent.parent))

from exchanges.bybit.ws_client import BybitWebSocketClient

# ANSI color codes for terminal output
COLORS = {
    'green': '\033[92m',
    'red': '\033[91m',
    'yellow': '\033[93m',
    'blue': '\033[94m',
    'magenta': '\033[95m',
    'cyan': '\033[96m',
    'white': '\033[97m',
    'bold': '\033[1m',
    'underline': '\033[4m',
    'end': '\033[0m'
}

def colorize(text, color):
    """Add color to terminal text"""
    return f"{COLORS.get(color, '')}{text}{COLORS['end']}"

def format_timestamp(timestamp_ms):
    """Format millisecond timestamp to readable date/time"""
    if not timestamp_ms or timestamp_ms == 0:
        return "N/A"
    return datetime.fromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')

def format_countdown(target_time_ms):
    """Format countdown to next funding time"""
    if not target_time_ms or target_time_ms == 0:
        return "N/A"
        
    now_ms = int(time.time() * 1000)
    seconds_remaining = max(0, (target_time_ms - now_ms) // 1000)
    
    hours = seconds_remaining // 3600
    minutes = (seconds_remaining % 3600) // 60
    seconds = seconds_remaining % 60
    
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

def format_funding_rate(rate):
    """Format funding rate with color based on value"""
    if rate is None:
        return "N/A"
    
    percentage = rate * 100
    
    if percentage > 0.01:  # Strong positive (> 0.01%)
        return colorize(f"{percentage:+.6f}%", 'red')
    elif percentage > 0:  # Positive
        return colorize(f"{percentage:+.6f}%", 'yellow')
    elif percentage < -0.01:  # Strong negative (< -0.01%)
        return colorize(f"{percentage:+.6f}%", 'green')
    else:  # Negative or zero
        return colorize(f"{percentage:+.6f}%", 'cyan')

# Maintain statistics for each symbol
stats = {}

def handle_ticker(message):
    """Callback function for ticker updates"""
    try:
        topic = message.get('topic', '')
        message_type = message.get('type', 'unknown')
        
        if not topic.startswith('tickers.'):
            return
            
        symbol = topic.split('.')[1]
        data = message.get('data', {})
        
        # Initialize symbol stats if needed
        if symbol not in stats:
            stats[symbol] = {
                'update_count': 0,
                'snapshot_count': 0,
                'delta_count': 0,
                'min_funding_rate': float('inf'),
                'max_funding_rate': float('-inf'),
                'latest_update': 0,
                'rate_changes': []
            }
        
        stats[symbol]['update_count'] += 1
        if message_type == 'snapshot':
            stats[symbol]['snapshot_count'] += 1
        elif message_type == 'delta':
            stats[symbol]['delta_count'] += 1
        
        # Log ticker update with colored type indicator
        update_type = colorize(f"[{message_type.upper()}]", 'magenta' if message_type == 'snapshot' else 'cyan')
        
        # Debug message showing raw data
        if isinstance(data, list) and data:
            data = data[0]  # Use first item if it's a list
            
        # Process funding rate
        if data:
            funding_rate = None
            
            # First check for the standardized field name
            if 'funding_rate' in data:
                funding_rate = float(data['funding_rate']) 
            # Then try camelCase (from API response)
            elif 'fundingRate' in data:
                funding_rate = float(data['fundingRate'])
                
            if funding_rate is not None:
                # Update stats
                stats[symbol]['min_funding_rate'] = min(stats[symbol]['min_funding_rate'], funding_rate)
                stats[symbol]['max_funding_rate'] = max(stats[symbol]['max_funding_rate'], funding_rate)
                stats[symbol]['latest_update'] = int(time.time() * 1000)
                
                # Track funding rate changes for historical review
                if len(stats[symbol]['rate_changes']) == 0 or stats[symbol]['rate_changes'][-1] != funding_rate:
                    stats[symbol]['rate_changes'].append(funding_rate)
                
                # Format next funding time if available
                next_funding_time = 0
                if 'next_funding_time' in data:
                    next_funding_time = int(data['next_funding_time'])
                elif 'nextFundingTime' in data:
                    next_funding_time = int(data['nextFundingTime'])
                    
                next_formatted = format_timestamp(next_funding_time)
                countdown = format_countdown(next_funding_time)
                
                # Log update with clear formatting
                logger.info(f"{update_type} {symbol}: Funding rate: {format_funding_rate(funding_rate)} | Next: {next_formatted} ({countdown})")
    except Exception as e:
        logger.error(f"Error processing ticker message: {e}")
        logger.error(traceback.format_exc())

def display_stats():
    """Display statistics about the collected funding rate data"""
    # Prepare data for table
    data_rows = []
    
    for symbol, symbol_stats in sorted(stats.items()):
        # Check if we have any valid data
        if symbol_stats['max_funding_rate'] == float('-inf'):
            continue
            
        # Get current ticker data
        ticker_data = client.get_ticker_data(symbol)
        
        if ticker_data:
            funding_rate = ticker_data.get('funding_rate', 0) * 100  # Convert to percentage
            next_funding_time = ticker_data.get('next_funding_time', 0)
            
            # Calculate stats
            min_rate = symbol_stats['min_funding_rate'] * 100
            max_rate = symbol_stats['max_funding_rate'] * 100
            update_count = symbol_stats['update_count']
            update_ratio = f"{symbol_stats['snapshot_count']}/{symbol_stats['delta_count']}"
            
            # Format values for display
            countdown = format_countdown(next_funding_time)
            
            # Add data row
            data_rows.append([
                symbol,
                format_funding_rate(ticker_data.get('funding_rate')),
                f"{min_rate:.6f}% / {max_rate:.6f}%",
                countdown,
                update_count,
                update_ratio,
                len(symbol_stats['rate_changes'])
            ])
    
    # Clear terminal and show header
    os.system('cls' if os.name == 'nt' else 'clear')
    print("\n" + "=" * 100)
    print(colorize(f" BYBIT FUNDING RATE MONITOR - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", 'bold'))
    print(colorize(" Verification of WebSocket Snapshot/Delta Processing", 'underline'))
    print("=" * 100)
    
    # Print connection information
    if client.connected:
        print(colorize("\n✓ WebSocket Connected", 'green'))
    else:
        print(colorize("\n✗ WebSocket Disconnected", 'red'))
        
    print(f"Subscribed symbols: {len(symbols)}")
    
    # Print stats table
    headers = ["Symbol", "Current Rate", "Min/Max Rate", "Next Funding", 
               "Updates", "Snapshot/Delta", "Rate Changes"]
    print("\nFUNDING RATE DATA:")
    print(tabulate(data_rows, headers=headers, tablefmt="pretty"))
    
    # Display legend
    print("\nColor Legend:")
    print(f"{format_funding_rate(0.0005)} - High positive rate (shorts pay longs)")
    print(f"{format_funding_rate(0.0001)} - Low positive rate")
    print(f"{format_funding_rate(-0.0001)} - Low negative rate")
    print(f"{format_funding_rate(-0.0005)} - High negative rate (longs pay shorts)")
    
    print("\nMessage Types:")
    print(f"{colorize('[SNAPSHOT]', 'magenta')} - Complete data refresh")
    print(f"{colorize('[DELTA]', 'cyan')} - Incremental update")
    
    return len(data_rows) > 0

if __name__ == "__main__":
    print(colorize("Bybit Funding Rate Monitor - WebSocket Verification", 'bold'))
    
    # Enable debug logging for more detailed info
    logging.getLogger("BybitWebSocketClient").setLevel(logging.INFO)
    
    # List of popular perpetual contracts to monitor
    symbols = [
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT",
        "DOGEUSDT",
        "XRPUSDT",
        "APTUSDT",
        "MATICUSDT",
        "BNBUSDT"
    ]
    
    # Create client for linear perpetuals
    client = BybitWebSocketClient(channel_type="linear", testnet=False)
    client.connect()
    
    try:
        # Wait for connection to establish
        print("Connecting to Bybit WebSocket...")
        time.sleep(5)
        
        print("Subscribing to ticker data...")
        # Subscribe to ticker data for all symbols
        for symbol in symbols:
            client.subscribe_ticker(symbol, callback=handle_ticker)
            print(f"  → {symbol}")
        
        print("\nWaiting for data... (first update may take a moment)")
        
        # Display loop
        refresh_interval = 5  # seconds
        total_runtime = 300  # seconds (5 minutes)
        start_time = time.time()
        
        while time.time() - start_time < total_runtime:
            # Sleep before refreshing display
            time.sleep(refresh_interval)
            
            # Display current stats
            if not display_stats():
                print(colorize("\nNo data received yet. Waiting...", 'yellow'))
            
            # Show recent log entries
            print("\nRecent Updates:")
            os.system("tail -n 5 bybit_funding_example.log 2>/dev/null || echo 'No log file available'")
            
            # Show time remaining
            remaining = total_runtime - (time.time() - start_time)
            print(f"\nRefreshing in {refresh_interval}s... (Runtime remaining: {int(remaining)}s)")
            
        print("\nMonitoring complete. Summary of data received:")
        # Final stats display
        display_stats()
            
    except KeyboardInterrupt:
        print(colorize("\nMonitoring stopped by user", 'yellow'))
    finally:
        print("Closing WebSocket connection...")
        client.close()
        print(colorize("Connection closed. Bye!", 'green'))
