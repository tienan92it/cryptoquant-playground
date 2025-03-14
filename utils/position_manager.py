import logging
import math
import traceback
from datetime import datetime
from typing import Dict, Any, Tuple

logger = logging.getLogger(__name__)

def round_down(value: float, decimals: int) -> float:
    """Round down value to specified decimal places"""
    factor = 10 ** decimals
    return math.floor(value * factor) / factor

def place_binance_futures_order(symbol: str, side: str, quantity: float, order_type: str = "MARKET") -> Dict[str, Any]:
    """
    Place an order on the Binance futures market (placeholder)
    
    Args:
        symbol: Trading pair symbol
        side: Order side ("BUY" or "SELL")
        quantity: Order quantity
        order_type: Order type (default "MARKET")
        
    Returns:
        Order result
    """
    logger.info(f"[MOCK] Binance Futures {side} order for {quantity} {symbol}")
    # In a real implementation, would call the Binance API here
    return {"orderId": f"mock-binance-order-id-{side.lower()}"}

def place_bybit_futures_order(symbol: str, side: str, quantity: float, order_type: str = "MARKET") -> Dict[str, Any]:
    """
    Place an order on the Bybit futures market (placeholder)
    
    Args:
        symbol: Trading pair symbol
        side: Order side ("BUY" or "SELL")
        quantity: Order quantity
        order_type: Order type (default "MARKET")
        
    Returns:
        Order result
    """
    logger.info(f"[MOCK] Bybit Futures {side} order for {quantity} {symbol}")
    # In a real implementation, would call the Bybit API here
    return {"orderId": f"mock-bybit-order-id-{side.lower()}"}

def place_okx_futures_order(symbol: str, side: str, quantity: float, order_type: str = "MARKET") -> Dict[str, Any]:
    """
    Place an order on the OKX futures market (placeholder)
    
    Args:
        symbol: Trading pair symbol
        side: Order side ("BUY" or "SELL")
        quantity: Order quantity
        order_type: Order type (default "MARKET")
        
    Returns:
        Order result
    """
    logger.info(f"[MOCK] OKX Futures {side} order for {quantity} {symbol}")
    # In a real implementation, would call the OKX API here
    return {"orderId": f"mock-okx-order-id-{side.lower()}"}

def execute_arbitrage(
    symbol: str,
    metrics: Dict[str, Any], 
    positions: Dict[str, Dict[str, Dict[str, Any]]],
    symbol_mappings: Dict[str, Dict[str, str]],
    exchanges_to_use: list = ['binance', 'bybit']
) -> bool:
    """
    Execute cross-exchange arbitrage for a symbol.
    
    Args:
        symbol: Trading pair symbol
        metrics: Dictionary containing calculated metrics
        positions: Current positions dictionary
        symbol_mappings: Dictionary mapping symbol names between exchanges
        exchanges_to_use: List of exchanges to use for arbitrage
            
    Returns:
        Boolean indicating success/failure
    """
    try:
        # Get the long and short exchanges from metrics
        long_exchange = metrics.get('long_exchange')
        short_exchange = metrics.get('short_exchange')
        
        # Make sure the required exchanges are in the list
        if long_exchange not in exchanges_to_use or short_exchange not in exchanges_to_use:
            logger.warning(f"Required exchanges {long_exchange}/{short_exchange} not in exchanges_to_use list")
            return False
        
        # Dictionary to store symbols for each exchange
        exchange_symbols = {
            'binance': symbol if 'binance' in symbol_mappings.get(symbol, {}) else None,
            'bybit': symbol_mappings.get(symbol, {}).get('bybit', ''),
            'okx': symbol_mappings.get(symbol, {}).get('okx', '')
        }
        
        # Determine quantities based on which exchange is long/short
        exchange_quantities = {}
        for exchange in exchanges_to_use:
            if exchange == long_exchange:
                exchange_quantities[exchange] = metrics.get('long_qty', 0)
            elif exchange == short_exchange:
                exchange_quantities[exchange] = metrics.get('short_qty', 0)
            else:
                exchange_quantities[exchange] = 0
        
        # Dictionary to store mark prices for each exchange
        exchange_mark_prices = {}
        for exchange in exchanges_to_use:
            price_key = f'{exchange}_mark_price'
            if price_key in metrics:
                exchange_mark_prices[exchange] = metrics[price_key]
            else:
                exchange_mark_prices[exchange] = 0
        
        # Execute long order first
        long_symbol = exchange_symbols[long_exchange]
        if not long_symbol:
            logger.warning(f"No symbol mapping for {symbol} on {long_exchange}, skipping")
            return False
            
        long_qty = exchange_quantities[long_exchange]
        if long_qty <= 0:
            logger.warning(f"Invalid long quantity for {symbol} on {long_exchange}, skipping")
            return False
            
        # Round quantity to appropriate precision
        long_qty = round_down(long_qty, 5)
        
        logger.info(f"Opening LONG position for {long_qty} {long_symbol} on {long_exchange}")
        
        # Place the long order
        long_result = None
        if long_exchange == 'binance':
            long_result = place_binance_futures_order(
                symbol=long_symbol,
                side="BUY",
                quantity=long_qty
            )
        elif long_exchange == 'bybit':
            long_result = place_bybit_futures_order(
                symbol=long_symbol,
                side="BUY",
                quantity=long_qty
            )
        elif long_exchange == 'okx':
            long_result = place_okx_futures_order(
                symbol=long_symbol,
                side="BUY",
                quantity=long_qty
            )
            
        if not long_result:
            logger.error(f"Failed to place long order on {long_exchange}")
            return False
            
        # Update position tracking for long side
        positions[symbol][long_exchange] = {
            'active': True,
            'side': 'LONG',
            'qty': long_qty,
            'entry_time': datetime.now(),
            'entry_price': exchange_mark_prices.get(long_exchange, 0)
        }
        
        # Now execute short order
        short_symbol = exchange_symbols[short_exchange]
        if not short_symbol:
            logger.warning(f"No symbol mapping for {symbol} on {short_exchange}, skipping")
            # Try to close the long position we just opened
            _close_single_position(
                symbol=symbol,
                exchange=long_exchange,
                position=positions[symbol][long_exchange],
                exchange_symbol=long_symbol
            )
            positions[symbol][long_exchange]['active'] = False
            return False
            
        short_qty = exchange_quantities[short_exchange]
        if short_qty <= 0:
            logger.warning(f"Invalid short quantity for {symbol} on {short_exchange}, skipping")
            # Try to close the long position we just opened
            _close_single_position(
                symbol=symbol,
                exchange=long_exchange,
                position=positions[symbol][long_exchange],
                exchange_symbol=long_symbol
            )
            positions[symbol][long_exchange]['active'] = False
            return False
            
        # Round quantity to appropriate precision
        short_qty = round_down(short_qty, 5)
        
        logger.info(f"Opening SHORT position for {short_qty} {short_symbol} on {short_exchange}")
        
        # Place the short order
        short_result = None
        if short_exchange == 'binance':
            short_result = place_binance_futures_order(
                symbol=short_symbol,
                side="SELL",
                quantity=short_qty
            )
        elif short_exchange == 'bybit':
            short_result = place_bybit_futures_order(
                symbol=short_symbol,
                side="SELL",
                quantity=short_qty
            )
        elif short_exchange == 'okx':
            short_result = place_okx_futures_order(
                symbol=short_symbol,
                side="SELL",
                quantity=short_qty
            )
            
        if not short_result:
            logger.error(f"Failed to place short order on {short_exchange}")
            # Try to close the long position we just opened
            _close_single_position(
                symbol=symbol,
                exchange=long_exchange,
                position=positions[symbol][long_exchange],
                exchange_symbol=long_symbol
            )
            positions[symbol][long_exchange]['active'] = False
            return False
            
        # Update position tracking for short side
        positions[symbol][short_exchange] = {
            'active': True,
            'side': 'SHORT',
            'qty': short_qty,
            'entry_time': datetime.now(),
            'entry_price': exchange_mark_prices.get(short_exchange, 0)
        }
        
        logger.info(f"Successfully opened cross-exchange arbitrage positions for {symbol}: LONG on {long_exchange}, SHORT on {short_exchange}")
        return True
        
    except Exception as e:
        logger.error(f"Error executing arbitrage for {symbol}: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Try to close any positions that might have been opened
        close_position(
            symbol=symbol, 
            positions=positions,
            symbol_mappings=symbol_mappings,
            exchanges_to_use=exchanges_to_use
        )
        
        return False

def _close_single_position(symbol: str, exchange: str, position: Dict[str, Any], exchange_symbol: str) -> bool:
    """
    Close a single position on one exchange
    
    Args:
        symbol: Original symbol
        exchange: Exchange to close position on
        position: Position data
        exchange_symbol: Symbol format for the specific exchange
        
    Returns:
        True if successful, False otherwise
    """
    if not position.get('active', False):
        return True  # Nothing to close
        
    try:
        qty = position['qty']
        side = position['side']
        
        # Determine closing side
        close_side = "SELL" if side == "LONG" else "BUY"
        
        logger.info(f"Closing {side} position for {qty} {exchange_symbol} on {exchange}")
        
        # Place closing order
        if exchange == 'binance':
            close_result = place_binance_futures_order(
                symbol=exchange_symbol,
                side=close_side,
                quantity=qty
            )
        elif exchange == 'bybit':
            close_result = place_bybit_futures_order(
                symbol=exchange_symbol,
                side=close_side,
                quantity=qty
            )
        elif exchange == 'okx':
            close_result = place_okx_futures_order(
                symbol=exchange_symbol,
                side=close_side,
                quantity=qty
            )
            
        # Record exit time
        position['active'] = False
        position['exit_time'] = datetime.now()
        
        logger.info(f"Successfully closed {side} position for {symbol} on {exchange}")
        return True
        
    except Exception as e:
        logger.error(f"Error closing {exchange} position for {symbol}: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def close_position(
    symbol: str, 
    positions: Dict[str, Dict[str, Dict[str, Any]]],
    symbol_mappings: Dict[str, Dict[str, str]],
    exchanges_to_use: list = ['binance', 'bybit']
) -> bool:
    """
    Close existing positions for a symbol across exchanges.
    
    Args:
        symbol: Trading pair symbol
        positions: Current positions dictionary
        symbol_mappings: Dictionary mapping symbol names between exchanges
        exchanges_to_use: List of exchanges to close positions on
        
    Returns:
        Boolean indicating success/failure
    """
    if symbol not in positions:
        return True  # No positions for this symbol
    
    success = True
    
    # Dictionary to store symbols for each exchange
    exchange_symbols = {
        'binance': symbol, 
        'bybit': symbol_mappings.get(symbol, {}).get('bybit', ''),
        'okx': symbol_mappings.get(symbol, {}).get('okx', '')
    }
    
    # Close positions on each exchange
    for exchange in exchanges_to_use:
        # Skip if not active or exchange not in position tracking
        if (exchange not in positions[symbol] or 
            not positions[symbol][exchange].get('active', False)):
            continue
            
        # Get the exchange-specific symbol
        exchange_symbol = exchange_symbols[exchange]
        if not exchange_symbol:
            logger.warning(f"No symbol mapping found for {symbol} on {exchange}, can't close position")
            success = False
            continue
            
        # Close the position
        position_closed = _close_single_position(
            symbol=symbol,
            exchange=exchange,
            position=positions[symbol][exchange],
            exchange_symbol=exchange_symbol
        )
        
        success = success and position_closed
    
    return success

def initialize_positions(symbols: list) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    Initialize position tracking dictionary for all symbols.
    
    Args:
        symbols: List of symbols to track
        
    Returns:
        Dictionary for tracking positions
    """
    positions = {}
    for symbol in symbols:
        positions[symbol] = {
            'binance': {'active': False, 'side': None, 'qty': 0, 'entry_time': None, 'entry_price': 0},
            'bybit': {'active': False, 'side': None, 'qty': 0, 'entry_time': None, 'entry_price': 0},
            'okx': {'active': False, 'side': None, 'qty': 0, 'entry_time': None, 'entry_price': 0}
        }
    return positions
