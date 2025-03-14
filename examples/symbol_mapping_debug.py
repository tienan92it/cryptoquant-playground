import sys
import time
import json
import logging
from pathlib import Path
from tabulate import tabulate
import traceback

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("symbol_mapping_debug")

# Add parent directory to path to import our modules
sys.path.append(str(Path(__file__).parent.parent))

# Import our utilities
from utils.exchange_utils import (
    fetch_binance_futures_symbols,
    fetch_bybit_perpetual_symbols,
    fetch_okx_perpetual_symbols,
    create_symbol_mappings
)

from exchanges.bybit.rest_client import BybitRestClient
from exchanges.okx.rest_client import OkxRestClient

def compare_exchange_symbols(symbol_list: list, binance_symbols: list, bybit_symbols: list, okx_symbols: list) -> list:
    """Compare symbols across exchanges to see which are missing where"""
    results = []
    
    for symbol in symbol_list:
        standard = symbol.upper()
        
        # Check if it exists on each exchange
        binance_available = standard in binance_symbols
        
        # For Bybit, check both formats
        base = standard[:-4]  # Remove USDT suffix
        bybit_format1 = f"{base}-USDT"
        bybit_available = bybit_format1 in bybit_symbols or standard in bybit_symbols
        
        # For OKX, check with hyphen
        okx_format = f"{base}-USDT"
        okx_available = okx_format in okx_symbols
        
        results.append({
            "symbol": standard,
            "binance": "✅" if binance_available else "❌",
            "bybit": "✅" if bybit_available else "❌",
            "okx": "✅" if okx_available else "❌",
            "all_exchanges": "✅" if (binance_available and bybit_available and okx_available) else "❌",
        })
    
    return results

def main():
    """Run the symbol mapping debug tool"""
    print("\n🔍 Exchange Symbol Mapping Debug Tool 🔍")
    print("=======================================\n")
    
    # Initialize REST clients
    print("Initializing REST clients...")
    bybit_client = BybitRestClient(testnet=False)
    okx_client = OkxRestClient(testnet=False)
    
    try:
        # Fetch symbols from exchanges
        print("\nFetching symbols from exchanges...")
        
        binance_symbols = fetch_binance_futures_symbols() 
        print(f"✅ Fetched {len(binance_symbols)} symbols from Binance")
        
        bybit_symbols = fetch_bybit_perpetual_symbols(bybit_client)
        print(f"✅ Fetched {len(bybit_symbols)} symbols from Bybit")
        
        okx_symbols = fetch_okx_perpetual_symbols()
        print(f"✅ Fetched {len(okx_symbols)} symbols from OKX")
        
        # Get OKX instrument ID mapping
        okx_mapping = okx_client.get_instrument_id_mapping()
        
        # Create symbol mappings
        print("\nCreating cross-exchange symbol mappings...")
        common_symbols, mappings = create_symbol_mappings(
            binance_symbols,
            bybit_symbols,
            okx_symbols
        )
        print(f"✅ Found {len(common_symbols)} common symbols across exchanges")
        
        # Check for specific problematic symbols
        problem_symbols = ["CATIUSDT", "ETHUSDT", "BTCUSDT", "SOLUSDT"]
        print("\nDetailed check for specific symbols:")
        print("------------------------------------")
        
        for symbol in problem_symbols:
            std_symbol = symbol.upper()
            print(f"\nSymbol: {std_symbol}")
            
            # Check Binance
            if std_symbol in binance_symbols:
                print(f"  Binance: ✅ Available as {std_symbol}")
            else:
                print(f"  Binance: ❌ Not available")
            
            # Check Bybit
            base = std_symbol[:-4]
            bybit_format1 = f"{base}-USDT"
            bybit_format2 = std_symbol
            
            if bybit_format1 in bybit_symbols:
                print(f"  Bybit:   ✅ Available as {bybit_format1}")
            elif bybit_format2 in bybit_symbols:
                print(f"  Bybit:   ✅ Available as {bybit_format2}")
            else:
                print(f"  Bybit:   ❌ Not available as {bybit_format1} or {bybit_format2}")
            
            # Check OKX
            okx_format = f"{base}-USDT"
            okx_inst_id = okx_mapping.get(std_symbol, okx_mapping.get(okx_format))
            
            if okx_format in okx_symbols:
                print(f"  OKX:     ✅ Available as {okx_format}")
                print(f"  OKX ID:  {okx_inst_id}")
            else:
                print(f"  OKX:     ❌ Not available as {okx_format}")
            
            # Check mapping result  
            if std_symbol in mappings:
                mapping_info = mappings[std_symbol]
                print("  Mapping: ✅ Created")
                for exchange, ex_symbol in mapping_info.items():
                    if exchange not in ['standard']:
                        print(f"    {exchange}: {ex_symbol}")
            else:
                print("  Mapping: ❌ Not created")
        
        # Show a table of top 20 symbol availability across exchanges
        print("\nSymbol Availability Across Exchanges:")
        print("------------------------------------")
        
        # Compare symbols that start with popular prefixes
        test_symbols = []
        
        # Add BTC, ETH and other major coins
        for base in ["BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "ADA", "MATIC"]:
            test_symbols.append(f"{base}USDT")
            
        # Add some mid and small caps
        for base in ["NEAR", "UNI", "LINK", "DOT", "AVAX", "FTM", "GALA", "CATI"]:
            test_symbols.append(f"{base}USDT")
        
        # Compare these symbols across exchanges
        comparison = compare_exchange_symbols(test_symbols, binance_symbols, bybit_symbols, okx_symbols)
        
        # Display as a table
        print(tabulate(
            [[
                item["symbol"], 
                item["binance"], 
                item["bybit"], 
                item["okx"],
                item["all_exchanges"]
            ] for item in comparison],
            headers=["Symbol", "Binance", "Bybit", "OKX", "All"],
            tablefmt="pretty"
        ))
        
        # Print statistics
        all_available = sum(1 for item in comparison if item["all_exchanges"] == "✅")
        print(f"\nSymbols available on all exchanges: {all_available}/{len(comparison)}")
        
    except Exception as e:
        logger.error(f"Error in symbol mapping debug: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        # Clean up
        okx_client.close()
        bybit_client.close()
        
        print("\nDebug tool execution complete")

if __name__ == "__main__":
    main()
