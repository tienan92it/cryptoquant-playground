import sys
import time
import json
import logging
from pathlib import Path
from tabulate import tabulate
from datetime import datetime
import traceback

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("okx_instruments_example")

# Add parent directory to path to import our modules
sys.path.append(str(Path(__file__).parent.parent))

# Import our OKX REST client
from exchanges.okx.rest_client import OkxRestClient

def display_instruments(instruments, count=20):
    """Display instruments in a formatted table"""
    if not instruments:
        print("No instruments found")
        return
    
    # Extract relevant fields for display
    data = []
    for i, instrument in enumerate(instruments):
        if i >= count:
            break
            
        expiry = "Perpetual" if not instrument.get("expTime") else instrument.get("expTime")
            
        data.append([
            instrument.get("instId", ""),
            instrument.get("instType", ""),
            instrument.get("baseCcy", ""),
            instrument.get("quoteCcy", ""),
            instrument.get("settleCcy", ""),
            instrument.get("state", ""),
            expiry
        ])
    
    # Display as table
    print(tabulate(
        data,
        headers=["Instrument ID", "Type", "Base", "Quote", "Settle", "State", "Expiry"],
        tablefmt="pretty"
    ))
    print(f"\nShowing {min(count, len(instruments))} of {len(instruments)} instruments")

def get_perpetual_swaps(client):
    """Get all perpetual swap instruments"""
    instruments = client.get_instruments(inst_type="SWAP")
    
    # Filter for USDT perpetual swaps
    usdt_perps = [
        inst for inst in instruments 
        if inst.get("instId", "").endswith("-USDT-SWAP") and not inst.get("expTime")
    ]
    
    return usdt_perps

def get_non_perpetual_swaps(client):
    """Get all non-perpetual swap instruments (with expiry)"""
    instruments = client.get_instruments(inst_type="SWAP")
    
    # Filter for USDT non-perpetual swaps
    non_perps = [
        inst for inst in instruments 
        if inst.get("instId", "").endswith("-USDT-SWAP") and inst.get("expTime")
    ]
    
    return non_perps

def create_symbol_mappings(instruments):
    """
    Create mappings between different symbol formats
    
    Returns:
        Dictionary mapping standard format to OKX format
    """
    mappings = {}
    
    for inst in instruments:
        inst_id = inst.get("instId", "")
        base_ccy = inst.get("baseCcy", "")
        
        if not base_ccy or not inst_id:
            continue
        
        # Standard format options (with and without hyphen)
        standard_with_hyphen = f"{base_ccy}-USDT"
        standard_without_hyphen = f"{base_ccy}USDT"
        
        # Map both formats to the OKX instrument ID
        mappings[standard_with_hyphen] = inst_id
        mappings[standard_without_hyphen] = inst_id
    
    return mappings

def verify_subscription_format(mappings):
    """
    Verify the correct format for WebSocket subscriptions
    
    Args:
        mappings: Dictionary mapping standard symbols to OKX instrument IDs
    """
    # Sample list of common symbols in standard format
    common_symbols = ["BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT", "BNB-USDT"]
    
    print("\n=== SYMBOL MAPPING VERIFICATION ===")
    print("Standard Symbol  ->  OKX WebSocket Format")
    print("-" * 50)
    
    for symbol in common_symbols:
        okx_id = mappings.get(symbol, "Not Available")
        status = "✓" if okx_id != "Not Available" else "✗"
        print(f"{symbol:13}  ->  {okx_id} {status}")

def test_funding_rate_fetch(client, instruments):
    """
    Test fetching funding rates for a few instruments
    """
    print("\n=== FUNDING RATE TEST ===")
    
    if not instruments:
        print("No instruments available to test")
        return
    
    # Use the first few instruments
    test_instruments = instruments[:3]
    
    for inst in test_instruments:
        inst_id = inst.get("instId")
        
        try:
            # Use public API endpoint directly for now
            funding_info = client.get_funding_rate(inst_id)
            
            print(f"\nFunding data for {inst_id}:")
            print(f"Current Rate: {float(funding_info.get('fundingRate', '0'))*100:+.6f}%")
            print(f"Next Rate: {float(funding_info.get('nextFundingRate', '0'))*100:+.6f}%")
            
            # Calculate time to next funding
            next_time = int(funding_info.get('nextFundingTime', '0'))
            if next_time > 0:
                now = time.time() * 1000
                hours_remaining = (next_time - now) / (1000 * 60 * 60)
                print(f"Next Funding: {datetime.fromtimestamp(next_time/1000)} (in {hours_remaining:.2f} hours)")
                
        except Exception as e:
            print(f"Error fetching funding rate for {inst_id}: {str(e)}")

def test_ws_subscription_format():
    """
    Print examples of correct WebSocket subscription channels for OKX
    """
    print("\n=== WEBSOCKET SUBSCRIPTION FORMAT ===")
    print("For the funding-rate channel, the subscription format is:")
    print("channel: funding-rate")
    print("instId: BTC-USDT-SWAP (not BTC-USDT)")
    
    print("\nExample subscription message:")
    print(json.dumps({
        "op": "subscribe",
        "args": [
            {
                "channel": "funding-rate",
                "instId": "BTC-USDT-SWAP"
            }
        ]
    }, indent=2))
    
    print("\nThis matches the REST API instrument ID format, not the standard symbol format.")

if __name__ == "__main__":
    print("OKX Instruments and Symbol Verification Tool")
    print("===========================================")
    
    # Create OKX REST client
    client = OkxRestClient(testnet=False)
    
    try:
        # Get perpetual swap instruments
        print("\nFetching perpetual swap instruments from OKX...")
        perp_instruments = get_perpetual_swaps(client)
        print(f"Found {len(perp_instruments)} USDT perpetual swap instruments")
        
        # Display the first 10 instruments
        print("\nSample perpetual swap instruments:")
        display_instruments(perp_instruments, 10)
        
        # Check for any non-perpetual swaps (should be rare or none)
        non_perp = get_non_perpetual_swaps(client)
        if non_perp:
            print(f"\nFound {len(non_perp)} non-perpetual USDT swaps (with expiry):")
            display_instruments(non_perp, 5)
        
        # Create symbol mappings
        print("\nCreating symbol mappings...")
        symbol_mappings = create_symbol_mappings(perp_instruments)
        print(f"Created mappings for {len(symbol_mappings)} symbols")
        
        # Verify subscription format
        verify_subscription_format(symbol_mappings)
        
        # Test funding rate fetching
        test_funding_rate_fetch(client, perp_instruments)
        
        # Show WebSocket subscription format
        test_ws_subscription_format()
        
        # Output the mappings to use in our application
        print("\n=== RECOMMENDED CHANGES ===")
        print("To fix the OKX WebSocket subscription issues:")
        print("1. Update the symbol mappings in create_symbol_mappings() to use the full '-USDT-SWAP' suffix")
        print("2. Use these identifiers when subscribing to the funding-rate channel")
        print("3. Make sure we handle the proper mapping between standard symbols and OKX's format")
        
    except Exception as e:
        logger.error(f"Error in OKX instrument verification: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        client.close()
        print("\nVerification complete.")
