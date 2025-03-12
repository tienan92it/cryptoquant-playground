import sys
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

# Add parent directory to path to import our module
sys.path.append(str(Path(__file__).parent.parent))

from exchanges.bybit.rest_client import BybitRestClient

if __name__ == "__main__":
    print("Bybit Symbol Validation Tool")
    print("=" * 80)
    
    # Create REST client
    client = BybitRestClient(testnet=False)
    
    try:
        # Fetch linear perpetual symbols
        print("Fetching linear perpetual symbols...")
        linear_symbols = client.get_all_perpetual_symbols(category="linear")
        print(f"Found {len(linear_symbols)} linear perpetual symbols:")
        for i, symbol in enumerate(linear_symbols):
            print(f"{i+1}. {symbol}")
        
        # Let user test specific symbols
        print("\nSymbol Validation Test:")
        while True:
            symbol_to_check = input("\nEnter symbol to check (or 'q' to quit): ")
            if symbol_to_check.lower() == 'q':
                break
                
            # Format and check
            formatted = symbol_to_check.upper().replace("-", "")
            is_valid = client._validate_symbol(formatted)
            print(f"Symbol '{formatted}' is {'valid' if is_valid else 'invalid'}")
            
            if is_valid:
                # Try to fetch funding history
                print("Fetching recent funding history...")
                history = client.get_funding_history(formatted, limit=3)
                if history:
                    print(f"Success! Found {len(history)} funding entries.")
                else:
                    print("No funding history found.")
        
    finally:
        client.close()
        print("Session closed.")
