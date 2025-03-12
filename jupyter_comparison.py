import sys
import time
from IPython.display import display, HTML
from exchange_comparison import ExchangeComparison

# Print a warning if not running in IPython environment
try:
    __IPYTHON__
    in_notebook = True
except NameError:
    in_notebook = False
    print("Warning: This script is optimized for Jupyter notebooks.")
    print("Some display features may not work correctly in a regular console.")

def main():
    """
    Run the exchange comparison tool in a Jupyter-friendly way
    """
    # Print HTML header
    if in_notebook:
        display(HTML("<h2>Crypto Exchange Price Comparison</h2>"))
        display(HTML("<p>Monitoring price differences between Binance and Bybit</p>"))
    
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
