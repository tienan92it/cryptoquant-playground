import logging
import requests
import time
import traceback
from typing import Dict, List, Set, Tuple, Optional, Any

logger = logging.getLogger(__name__)

def fetch_binance_futures_symbols(exclude_symbols: List[str] = None, include_only: List[str] = None) -> List[str]:
    """
    Fetch all available futures symbols from Binance
    
    Args:
        exclude_symbols: List of symbols to exclude
        include_only: If provided, only include these symbols
        
    Returns:
        List of uppercase symbol strings
    """
    try:
        # Get exchange info from Binance Futures API
        response = requests.get('https://fapi.binance.com/fapi/v1/exchangeInfo')
        response.raise_for_status()
        exchange_info = response.json()
        
        # Filter for trading pairs ending with USDT
        all_symbols = []
        for symbol_info in exchange_info['symbols']:
            symbol = symbol_info['symbol']
            status = symbol_info['status']
            
            # Only include active USDT pairs
            if status == 'TRADING' and symbol.endswith('USDT'):
                # Apply exclude filter
                if exclude_symbols and symbol in exclude_symbols:
                    logger.debug(f"Excluding symbol {symbol} as configured")
                    continue
                
                # Apply include_only filter if specified
                if include_only and symbol not in include_only:
                    logger.debug(f"Skipping symbol {symbol} - not in include_only list")
                    continue
                
                all_symbols.append(symbol)
        
        logger.info(f"Found {len(all_symbols)} USDT-denominated futures symbols on Binance")
        return all_symbols
        
    except Exception as e:
        logger.error(f"Error fetching Binance futures symbols: {str(e)}")
        logger.error(traceback.format_exc())
        return []

def fetch_bybit_perpetual_symbols(bybit_rest_client) -> List[str]:
    """
    Fetch all available perpetual contract symbols from Bybit
    
    Args:
        bybit_rest_client: Initialized Bybit REST client
        
    Returns:
        List of symbol strings
    """
    try:
        symbols = bybit_rest_client.get_all_perpetual_symbols(category="linear")
        logger.info(f"Found {len(symbols)} perpetual symbols on Bybit")
        return symbols
    except Exception as e:
        logger.error(f"Error fetching Bybit symbols: {str(e)}")
        logger.error(traceback.format_exc())
        return []

def fetch_okx_perpetual_symbols() -> List[str]:
    """
    Fetch all available perpetual contract symbols from OKX
    
    Returns:
        List of OKX symbol strings (e.g., "BTC-USDT")
    """
    try:
        from exchanges.okx.rest_client import OkxRestClient
        
        # Create REST client
        okx_client = OkxRestClient(testnet=False)
        
        try:
            # Fetch perpetual symbols
            okx_symbols = okx_client.get_perpetual_symbols()
            logger.info(f"Fetched {len(okx_symbols)} perpetual symbols from OKX")
            
            # Also get the mapping from standard to OKX format
            okx_instrument_mapping = okx_client.get_instrument_id_mapping()
            logger.info(f"Created mapping for {len(okx_instrument_mapping)} OKX instruments")
            
            return okx_symbols
        finally:
            okx_client.close()
    except Exception as e:
        logger.error(f"Error fetching OKX symbols: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Return placeholder data for testing purposes
        okx_symbols = [
            "BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT", "DOGE-USDT", 
            "ADA-USDT", "DOT-USDT", "LTC-USDT", "BNB-USDT", "MATIC-USDT",
            "AVAX-USDT", "LINK-USDT", "UNI-USDT", "ATOM-USDT", "NEAR-USDT"
        ]
        
        logger.info(f"Using placeholder data with {len(okx_symbols)} OKX symbols")
        return okx_symbols

def create_symbol_mappings(
    binance_symbols: List[str], 
    bybit_symbols: List[str], 
    okx_symbols: List[str]
) -> Tuple[List[str], Dict[str, Dict[str, str]]]:
    """
    Create mappings between exchange-specific symbols and find common symbols
    
    Args:
        binance_symbols: List of Binance symbols
        bybit_symbols: List of Bybit symbols
        okx_symbols: List of OKX symbols
        
    Returns:
        Tuple containing (common_symbols, mappings)
    """
    # Initialize mappings dictionary (standard symbol -> exchange symbols)
    mappings = {}
    common_symbols = []
    
    # Log exchange symbol counts for debugging
    logger.info(f"Creating mappings between exchanges - Binance: {len(binance_symbols)}, Bybit: {len(bybit_symbols)}, OKX: {len(okx_symbols)}")
    
    # Convert OKX symbols to standard format for fast lookup
    okx_standard_map = {}
    for symbol in okx_symbols:
        if '-' in symbol:
            base, quote = symbol.split('-', 1)  # Split only on first hyphen
            standard_no_hyphen = f"{base}{quote}"
            okx_standard_map[standard_no_hyphen.upper()] = symbol
    
    # Use Binance format as the standard
    for std_symbol in binance_symbols:
        # We use uppercase without hyphens as standard format
        std_symbol = std_symbol.upper()
        
        # Initialize mapping for this symbol
        mapping = {'standard': std_symbol, 'binance': std_symbol}
        is_common = True
        
        # Find Bybit equivalent
        base = std_symbol[:-4]  # Remove USDT
        bybit_format1 = f"{base}-USDT"  # With hyphen
        bybit_format2 = std_symbol      # Without hyphen
        
        if bybit_format1 in bybit_symbols:
            mapping['bybit'] = bybit_format1
        elif bybit_format2 in bybit_symbols:
            mapping['bybit'] = bybit_format2
        else:
            # Consider as missing in Bybit
            is_common = bybit_symbols == []  # Only required if Bybit is used
        
        # Find OKX equivalent - more comprehensive approach
        if std_symbol in okx_standard_map:
            # Direct match found in our prepared map
            mapping['okx'] = okx_standard_map[std_symbol]
            mapping['okx_instid'] = f"{mapping['okx']}-SWAP"  # Add the -SWAP suffix for WebSocket
        else:
            # Try different formats
            okx_format1 = f"{base}-USDT"  # With hyphen
            
            if okx_format1 in okx_symbols:
                mapping['okx'] = okx_format1
                mapping['okx_instid'] = f"{okx_format1}-SWAP"
            else:
                # Consider as missing in OKX
                is_common = is_common and okx_symbols == []  # Only required if OKX is used
        
        # If this symbol is available on all required exchanges we're using
        if is_common:
            common_symbols.append(std_symbol)
            mappings[std_symbol] = mapping
            
        elif 'okx' not in mapping and okx_symbols:  # If OKX is being used but symbol is missing
            logger.debug(f"Symbol {std_symbol} not available on OKX")
    
    # Log information about mapping results
    exchanges_required = sum([1 for x in [binance_symbols, bybit_symbols, okx_symbols] if x])
    logger.info(f"Found {len(common_symbols)} symbols available across {exchanges_required} required exchanges")
    
    # Debug output for specific symbols if they're in the list
    test_symbols = ["BTCUSDT", "ETHUSDT", "CATIUSDT"]
    for symbol in test_symbols:
        if symbol in mappings:
            logger.info(f"Mapping for {symbol}: {mappings[symbol]}")
        else:
            logger.info(f"No mapping created for {symbol}")
    
    return common_symbols, mappings

def filter_symbols(symbols: List[str], config: Dict[str, Any]) -> List[str]:
    """
    Apply filters to a list of symbols based on configuration
    
    Args:
        symbols: List of symbols to filter
        config: Configuration dictionary containing filter settings
        
    Returns:
        Filtered list of symbols
    """
    exclude_symbols = [s.upper() for s in config.get('symbol_filters', {}).get('exclude', [])]
    include_only = [s.upper() for s in config.get('symbol_filters', {}).get('include_only', [])]
    
    filtered_symbols = []
    for symbol in symbols:
        # Apply exclude filter
        if symbol in exclude_symbols:
            logger.debug(f"Excluding symbol {symbol} as configured")
            continue
        
        # Apply include_only filter if specified
        if include_only and symbol not in include_only:
            logger.debug(f"Skipping symbol {symbol} - not in include_only list")
            continue
        
        # Additional filters can be added here (price, volume, etc.)
        
        filtered_symbols.append(symbol)
    
    return filtered_symbols

def initialize_websockets(config, symbols, symbol_mappings):
    """
    Initialize WebSocket connections to exchanges
    
    Args:
        config: Configuration dictionary
        symbols: List of symbols to subscribe to
        symbol_mappings: Dictionary mapping between exchange symbols
        
    Returns:
        Dictionary of WebSocket clients and connection status
    """
    # This would need to be implemented based on your exchange client classes
    # For now, returning a placeholder
    return {
        'binance_ws': None,
        'bybit_ws': None,
        'okx_ws': None
    }
