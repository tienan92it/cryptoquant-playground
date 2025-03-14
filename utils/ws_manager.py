import time
import logging
import traceback
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

def initialize_binance_websocket(symbols: List[str], max_retries: int = 3, retry_delay: int = 5):
    """
    Initialize and connect to Binance WebSocket
    
    Args:
        symbols: List of symbols to subscribe to
        max_retries: Maximum number of connection attempts
        retry_delay: Delay in seconds between retries
        
    Returns:
        WebSocket client object, connection status (bool)
    """
    from exchanges.binance.ws_client import BinanceWebSocketClient
    
    # Initialize connection attempts
    attempts = 0
    success = False
    client = None
    
    while not success and attempts < max_retries:
        try:
            attempts += 1
            logger.info(f"Connecting to Binance WebSocket (attempt {attempts}/{max_retries})...")
            
            # Connect to Binance WebSocket for real-time data
            client = BinanceWebSocketClient(
                futures_symbols=[s.lower() for s in symbols],
                mark_price_freq="1s",
                use_all_market_stream=True
            )
            client.connect()
            
            # Wait a bit to establish connection
            time.sleep(2)
            success = True
            logger.info("Binance WebSocket connected successfully")
        except Exception as e:
            logger.error(f"Error connecting to Binance WebSocket: {str(e)}")
            if attempts < max_retries:
                logger.info(f"Retrying Binance connection in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Failed to connect to Binance WebSocket after {max_retries} attempts")
                logger.error(traceback.format_exc())
    
    return client, success

def initialize_bybit_websocket(symbols: List[str], symbol_mappings: Dict[str, Dict[str, str]], 
                               max_retries: int = 3, retry_delay: int = 5):
    """
    Initialize and connect to Bybit WebSocket
    
    Args:
        symbols: List of symbols to subscribe to (in standard format)
        symbol_mappings: Dictionary mapping standard symbols to exchange-specific formats
        max_retries: Maximum number of connection attempts
        retry_delay: Delay in seconds between retries
        
    Returns:
        WebSocket client object, connection status (bool)
    """
    from exchanges.bybit.ws_client import BybitWebSocketClient
    
    # Initialize connection attempts
    attempts = 0
    success = False
    client = None
    
    while not success and attempts < max_retries:
        try:
            attempts += 1
            logger.info(f"Connecting to Bybit WebSocket (attempt {attempts}/{max_retries})...")
            
            # Initialize Bybit WebSocket client
            client = BybitWebSocketClient(channel_type="linear", testnet=False)
            client.connect()
            
            # Wait a bit to establish connection
            time.sleep(2)
            
            if client.connected:
                success = True
                logger.info("Bybit WebSocket connected successfully")
                
                # Subscribe to tickers for all symbols
                subscription_errors = 0
                for symbol in symbols:
                    try:
                        # Get the correct Bybit symbol from our mapping
                        bybit_symbol = symbol_mappings.get(symbol, {}).get('bybit')
                        if not bybit_symbol:
                            logger.warning(f"No Bybit mapping found for {symbol}, skipping subscription")
                            continue
                        
                        # Add a small delay between subscriptions to avoid overwhelming the API
                        time.sleep(0.1)
                        
                        # Subscribe to ticker channel
                        subscribe_result = client.subscribe_ticker(bybit_symbol)
                        if subscribe_result:
                            logger.debug(f"Successfully subscribed to ticker for {bybit_symbol} on Bybit")
                        else:
                            logger.warning(f"Failed to subscribe to {bybit_symbol} on Bybit")
                            subscription_errors += 1
                            
                    except Exception as e:
                        subscription_errors += 1
                        logger.warning(f"Error subscribing to {symbol} on Bybit: {str(e)}")
                
                if subscription_errors > 0:
                    logger.warning(f"Failed to subscribe to {subscription_errors} symbols on Bybit WebSocket. REST API will be used as fallback.")
            else:
                logger.warning("Bybit WebSocket connected but client reports disconnected state")
                success = False
                
        except Exception as e:
            logger.error(f"Error connecting to Bybit WebSocket: {str(e)}")
            if attempts < max_retries:
                logger.info(f"Retrying Bybit connection in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Failed to connect to Bybit WebSocket after {max_retries} attempts")
                logger.error(traceback.format_exc())
    
    return client, success

def initialize_okx_websocket(symbols: List[str], symbol_mappings: Dict[str, Dict[str, str]],
                             max_retries: int = 3, retry_delay: int = 5):
    """
    Initialize and connect to OKX WebSocket
    
    Args:
        symbols: List of symbols to subscribe to (in standard format)
        symbol_mappings: Dictionary mapping standard symbols to exchange-specific formats
        max_retries: Maximum number of connection attempts
        retry_delay: Delay in seconds between retries
        
    Returns:
        WebSocket client object, connection status (bool)
    """
    try:
        from exchanges.okx.ws_client import OkxWebSocketClient
        
        # Initialize connection attempts
        attempts = 0
        success = False
        client = None
        
        while not success and attempts < max_retries:
            try:
                attempts += 1
                logger.info(f"Connecting to OKX WebSocket (attempt {attempts}/{max_retries})...")
                
                # Initialize OKX WebSocket client
                client = OkxWebSocketClient(testnet=False)
                client.connect()
                
                # Wait a bit to establish connection
                time.sleep(2)
                
                if client.connected:
                    success = True
                    logger.info("OKX WebSocket connected successfully")
                    
                    # Subscribe to funding rates for all symbols
                    subscription_errors = 0
                    for symbol in symbols:
                        try:
                            # Get the correct OKX symbol from our mapping
                            okx_symbol = symbol_mappings.get(symbol, {}).get('okx')
                            if not okx_symbol:
                                logger.warning(f"No OKX mapping found for {symbol}, skipping subscription")
                                continue
                            
                            # Add a small delay between subscriptions to avoid overwhelming the API
                            time.sleep(0.1)
                            
                            # Subscribe to funding rate channel
                            subscribe_result = client.subscribe_funding_rate(okx_symbol)
                            if subscribe_result:
                                logger.debug(f"Successfully subscribed to funding rate for {okx_symbol} on OKX")
                            else:
                                logger.warning(f"Failed to subscribe to {okx_symbol} on OKX")
                                subscription_errors += 1
                                
                        except Exception as e:
                            subscription_errors += 1
                            logger.warning(f"Error subscribing to {symbol} on OKX: {str(e)}")
                    
                    if subscription_errors > 0:
                        logger.warning(f"Failed to subscribe to {subscription_errors} symbols on OKX WebSocket")
                else:
                    logger.warning("OKX WebSocket connected but client reports disconnected state")
                    success = False
                    
            except Exception as e:
                logger.error(f"Error connecting to OKX WebSocket: {str(e)}")
                if attempts < max_retries:
                    logger.info(f"Retrying OKX connection in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"Failed to connect to OKX WebSocket after {max_retries} attempts")
                    logger.error(traceback.format_exc())
        
        return client, success
    except ImportError:
        logger.warning("OKX WebSocket client not available, skipping initialization")
        return None, False

def initialize_all_websockets(symbols: List[str], symbol_mappings: Dict[str, Dict[str, str]], 
                              exchanges_to_use: List[str]):
    """
    Initialize and connect to all required WebSocket connections
    
    Args:
        symbols: List of symbols to subscribe to
        symbol_mappings: Dictionary mapping standard symbols to exchange-specific formats
        exchanges_to_use: List of exchanges to connect to
        
    Returns:
        Dictionary containing WebSocket clients and connection statuses
    """
    ws_clients = {}
    ws_connected = {}
    
    # Connect to Binance WebSocket
    if 'binance' in exchanges_to_use:
        ws_clients['binance'], ws_connected['binance'] = initialize_binance_websocket(symbols)
    
    # Connect to Bybit WebSocket
    if 'bybit' in exchanges_to_use:
        ws_clients['bybit'], ws_connected['bybit'] = initialize_bybit_websocket(symbols, symbol_mappings)
        
    # Connect to OKX WebSocket
    if 'okx' in exchanges_to_use:
        ws_clients['okx'], ws_connected['okx'] = initialize_okx_websocket(symbols, symbol_mappings)
    
    return ws_clients, ws_connected

def check_websocket_connections(ws_clients: Dict[str, Any], ws_connected: Dict[str, bool], 
                                symbols: List[str], symbol_mappings: Dict[str, Dict[str, str]]):
    """
    Check WebSocket connections and attempt reconnection if needed
    
    Args:
        ws_clients: Dictionary of WebSocket clients
        ws_connected: Dictionary of connection statuses
        symbols: List of symbols
        symbol_mappings: Dictionary mapping standard symbols to exchange-specific formats
        
    Returns:
        Updated ws_clients and ws_connected dictionaries
    """
    # Check Binance connection
    if 'binance' in ws_clients and (not ws_connected.get('binance', False) or not ws_clients['binance']):
        logger.warning("Binance WebSocket disconnected, attempting to reconnect...")
        try:
            if ws_clients.get('binance'):
                try:
                    ws_clients['binance'].close()
                except:
                    pass
            
            ws_clients['binance'], ws_connected['binance'] = initialize_binance_websocket(symbols)
        except Exception as e:
            logger.error(f"Failed to reconnect to Binance WebSocket: {str(e)}")
            ws_connected['binance'] = False
    
    # Check Bybit connection
    if 'bybit' in ws_clients and (not ws_connected.get('bybit', False) or not ws_clients['bybit'] or not ws_clients['bybit'].connected):
        logger.warning("Bybit WebSocket disconnected, attempting to reconnect...")
        try:
            if ws_clients.get('bybit'):
                try:
                    ws_clients['bybit'].close()
                except:
                    pass
            
            ws_clients['bybit'], ws_connected['bybit'] = initialize_bybit_websocket(symbols, symbol_mappings)
        except Exception as e:
            logger.error(f"Failed to reconnect to Bybit WebSocket: {str(e)}")
            ws_connected['bybit'] = False
            
    # Check OKX connection
    if 'okx' in ws_clients and (not ws_connected.get('okx', False) or not ws_clients['okx'] or not ws_clients['okx'].connected):
        logger.warning("OKX WebSocket disconnected, attempting to reconnect...")
        try:
            if ws_clients.get('okx'):
                try:
                    ws_clients['okx'].close()
                except:
                    pass
            
            ws_clients['okx'], ws_connected['okx'] = initialize_okx_websocket(symbols, symbol_mappings)
        except Exception as e:
            logger.error(f"Failed to reconnect to OKX WebSocket: {str(e)}")
            ws_connected['okx'] = False
    
    return ws_clients, ws_connected

def close_all_websockets(ws_clients: Dict[str, Any]):
    """
    Close all WebSocket connections
    
    Args:
        ws_clients: Dictionary of WebSocket clients
    """
    for exchange, client in ws_clients.items():
        if client:
            try:
                logger.info(f"Closing {exchange} WebSocket connection...")
                client.close()
            except Exception as e:
                logger.error(f"Error closing {exchange} WebSocket: {str(e)}")
