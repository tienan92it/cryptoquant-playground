import sys
import time
from exchange_comparison import ExchangeComparison

def main():
    # Default symbols to compare
    default_symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    
    # Allow command-line arguments to specify symbols
    if len(sys.argv) > 1:
        symbols = [s.upper() for s in sys.argv[1:]]
        print(f"Comparing custom symbols: {symbols}")
    else:
        symbols = default_symbols
        print(f"Comparing default symbols: {symbols}")
    
    # Create and start the comparison tool
    comparison = ExchangeComparison(symbols, update_interval=2.0)
    comparison.start()
    
    try:
        # Keep the script running until interrupted
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down exchange comparison...")
        comparison.stop()
        print("Comparison tool stopped")

if __name__ == "__main__":
    main()
