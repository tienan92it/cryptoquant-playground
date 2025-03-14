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
    # This is a placeholder - in production you'd implement the OKX API call
    okx_symbols = [
        "BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT", "DOGE-USDT", 
        "ADA-USDT", "DOT-USDT", "LTC-USDT", "BNB-USDT", "MATIC-USDT",
        "AVAX-USDT", "LINK-USDT", "UNI-USDT", "ATOM-USDT", "NEAR-USDT"
    ]
    
    logger.info(f"Fetched {len(okx_symbols)} perpetual symbols from OKX")
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
            is_common = False
        
        # Find OKX equivalent
        okx_format1 = f"{base}-USDT"  # With hyphen
        okx_format2 = std_symbol      # Without hyphen
        
        if okx_format1 in okx_symbols:
            mapping['okx'] = okx_format1
        elif okx_format2 in okx_symbols:
            mapping['okx'] = okx_format2
        else:
            # Only consider OKX if it's in the list of symbols
            if okx_symbols:  
                is_common = False
        
        # If this symbol is available on all exchanges we're using
        if is_common:
            common_symbols.append(std_symbol)
            mappings[std_symbol] = mapping
    
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
