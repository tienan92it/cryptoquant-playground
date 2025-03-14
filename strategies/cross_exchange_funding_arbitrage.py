import sys
import time
import logging
import json
import math
import requests
from pathlib import Path
from datetime import datetime
from tabulate import tabulate
import traceback
import os
from operator import itemgetter
from typing import Dict, List, Any, Tuple, Optional

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from exchanges.binance.ws_client import BinanceWebSocketClient
from exchanges.bybit.rest_client import BybitRestClient
from exchanges.bybit.ws_client import BybitWebSocketClient

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("cross_exchange_funding_arbitrage.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("cross_exchange_funding_arbitrage")

class CrossExchangeFundingArbitrageStrategy:
    """
    Cross-exchange funding fee arbitrage strategy.
    
    This strategy:
    1. Finds funding rate discrepancies between exchanges (e.g., Binance and Bybit)
    2. Takes opposing positions on different exchanges to neutralize price risk
    3. Profits from the net funding rate difference between the positions
    """
    
    def __init__(self, config):
        """
        Initialize the cross-exchange funding arbitrage strategy.
        
        Args:
            config: Dictionary containing configuration parameters
        """
        self.config = config
        
        # Configuration parameters
        self.min_funding_spread = config.get('min_funding_spread', 0.0005)  # Min 0.05% funding spread between exchanges
        self.position_size_usd = config.get('position_size_usd', 1000)
        self.check_interval = config.get('check_interval', 30)
        self.max_positions = config.get('risk_management', {}).get('max_positions', 5)
        self.use_all_symbols = config.get('use_all_symbols', False)
        
        # Trading fees and slippage
        self.binance_fee_rate = config.get('futures_fee_rate', 0.0004)  # 0.04% on Binance
        self.bybit_fee_rate = config.get('bybit_fee_rate', 0.0006)  # 0.06% on Bybit
        self.slippage = config.get('slippage', 0.0003)  # 0.03% slippage estimate
        
        # Symbol filters
        self.min_price = config.get('symbol_filters', {}).get('min_price', 0)
        self.min_volume = config.get('symbol_filters', {}).get('min_volume_usd', 0)
        self.exclude_symbols = [s.upper() for s in config.get('symbol_filters', {}).get('exclude', [])]
        self.include_only = [s.upper() for s in config.get('symbol_filters', {}).get('include_only', [])]
        
        # Connection state tracking
        self.ws_connected = {
            'binance': False,
            'bybit': False
        }
        self.connection_attempts = {
            'binance': 0,
            'bybit': 0
        }
        self.max_retries = 3
        
        # Symbol mappings between exchanges
        self.binance_to_bybit = {}  # Will store Binance->Bybit symbol mappings
        self.bybit_to_binance = {}  # Will store Bybit->Binance symbol mappings
        
        # Initialize connection to exchanges
        logger.info("Initializing connections to exchanges...")
        
        # Initialize Bybit REST client first for symbol discovery
        self.bybit_rest = BybitRestClient(testnet=False)
        
        # Get available symbols on both exchanges
        logger.info("Fetching available symbols from all exchanges...")
        self.available_symbols = self._get_common_symbols()
        logger.info(f"Found {len(self.available_symbols)} common symbols available on both exchanges")
        
        # Filter symbols based on configuration
        if self.use_all_symbols:
            self.symbols = self._filter_symbols(self.available_symbols)
            logger.info(f"Using {len(self.symbols)} common symbols after filtering")
        else:
            # Use only the configured symbols that are available on both exchanges
            configured_symbols = [s.upper() for s in config.get('symbols', [])]
            self.symbols = [s for s in configured_symbols if s in self.available_symbols]
            logger.info(f"Using {len(self.symbols)} configured symbols that are available on both exchanges")
            
            # Warn about any configured symbols that aren't available
            unavailable = [s for s in configured_symbols if s not in self.available_symbols]
            if unavailable:
                logger.warning(f"The following configured symbols are not available on both exchanges: {unavailable}")
        
        # Initialize positions dict to track our positions
        self.positions = {}
        for symbol in self.symbols:
            self.positions[symbol] = {
                'binance': {'active': False, 'side': None, 'qty': 0, 'entry_time': None, 'entry_price': 0},
                'bybit': {'active': False, 'side': None, 'qty': 0, 'entry_time': None, 'entry_price': 0}
            }
        
        # Store metrics for display
        self.metrics = {}
        self.opportunities = []
        
        # Initialize and connect to WebSockets with proper error handling
        self._initialize_websockets()
        
        logger.info(f"Initialized cross-exchange funding arbitrage strategy with {len(self.symbols)} symbols")
        logger.info(f"Minimum funding spread threshold: {self.min_funding_spread}")
        logger.info(f"Maximum positions: {self.max_positions}")
        
        # Wait for initial data
        logger.info("Waiting for initial data...")
        time.sleep(15)
        
        # Verify data availability
        self._verify_data_availability()

    def _initialize_websockets(self):
        """Initialize and connect to WebSockets with proper error handling"""
        # Initialize connection attempts
        success = False
        
        while not success and self.connection_attempts['binance'] < self.max_retries:
            try:
                self.connection_attempts['binance'] += 1
                logger.info(f"Connecting to Binance WebSocket (attempt {self.connection_attempts['binance']}/{self.max_retries})...")
                
                # Connect to Binance WebSocket for real-time data
                self.binance_ws = BinanceWebSocketClient(
                    futures_symbols=[s.lower() for s in self.symbols],
                    mark_price_freq="1s",
                    use_all_market_stream=True
                )
                self.binance_ws.connect()
                
                # Wait a bit to establish connection
                time.sleep(2)
                self.ws_connected['binance'] = True
                success = True
                logger.info("Binance WebSocket connected successfully")
            except Exception as e:
                logger.error(f"Error connecting to Binance WebSocket: {str(e)}")
                if self.connection_attempts['binance'] < self.max_retries:
                    logger.info(f"Retrying Binance connection in 5 seconds...")
                    time.sleep(5)
                else:
                    logger.error(f"Failed to connect to Binance WebSocket after {self.max_retries} attempts")
                    raise
        
        # Reset for Bybit connection
        success = False
        
        while not success and self.connection_attempts['bybit'] < self.max_retries:
            try:
                self.connection_attempts['bybit'] += 1
                logger.info(f"Connecting to Bybit WebSocket (attempt {self.connection_attempts['bybit']}/{self.max_retries})...")
                
                # Initialize Bybit WebSocket client
                self.bybit_ws = BybitWebSocketClient(channel_type="linear", testnet=False)
                self.bybit_ws.connect()
                
                # Wait a bit to establish connection
                time.sleep(2)
                self.ws_connected['bybit'] = True
                success = True
                logger.info("Bybit WebSocket connected successfully")
            except Exception as e:
                logger.error(f"Error connecting to Bybit WebSocket: {str(e)}")
                if self.connection_attempts['bybit'] < self.max_retries:
                    logger.info(f"Retrying Bybit connection in 5 seconds...")
                    time.sleep(5)
                else:
                    logger.error(f"Failed to connect to Bybit WebSocket after {self.max_retries} attempts")
                    raise
        
        # Subscribe to Bybit data for the symbols
        if self.ws_connected['bybit']:
            self._subscribe_to_bybit_tickers()
        else:
            logger.warning("Skipping Bybit subscriptions as WebSocket is not connected")

    def _subscribe_to_bybit_tickers(self):
        """Subscribe to Bybit ticker data with improved error handling"""
        bybit_subscription_errors = 0
        
        for symbol in self.symbols:
            try:
                # Get the correct Bybit symbol format from our mapping
                bybit_symbol = self.binance_to_bybit.get(symbol)
                if not bybit_symbol:
                    logger.warning(f"No Bybit mapping found for {symbol}, skipping subscription")
                    continue
                
                # Add a small delay between subscriptions to avoid overwhelming the API
                time.sleep(0.1)
                
                # Subscribe and check result
                success = self.bybit_ws.subscribe_ticker(bybit_symbol)
                if success:
                    logger.debug(f"Successfully subscribed to ticker for {bybit_symbol} on Bybit")
                else:
                    logger.warning(f"Failed to subscribe to {bybit_symbol} on Bybit")
                    bybit_subscription_errors += 1
                    
            except Exception as e:
                bybit_subscription_errors += 1
                logger.warning(f"Error subscribing to {symbol} on Bybit: {str(e)}")
        
        if bybit_subscription_errors > 0:
            logger.warning(f"Failed to subscribe to {bybit_subscription_errors} symbols on Bybit WebSocket. REST API will be used as fallback.")

    def _fetch_all_binance_futures_symbols(self):
        """Fetch all available futures symbols from Binance"""
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
                    if symbol in self.exclude_symbols:
                        logger.debug(f"Excluding symbol {symbol} as configured")
                        continue
                    
                    # Apply include_only filter if specified
                    if self.include_only and symbol not in self.include_only:
                        logger.debug(f"Skipping symbol {symbol} - not in include_only list")
                        continue
                    
                    all_symbols.append(symbol)
            
            logger.info(f"Found {len(all_symbols)} USDT-denominated futures symbols")
            return all_symbols
            
        except Exception as e:
            logger.error(f"Error fetching futures symbols: {str(e)}")
            logger.error(traceback.format_exc())
            # Return empty list as fallback
            return []
    
    def _get_common_symbols(self) -> List[str]:
        """
        Get the list of common symbols available on both exchanges.
        
        Returns:
            List of symbol strings in Binance format
        """
        # Get Binance futures symbols
        binance_symbols = self._fetch_all_binance_futures_symbols()
        logger.info(f"Found {len(binance_symbols)} futures symbols on Binance")
        
        # Get Bybit perpetual symbols
        try:
            bybit_symbols = self.bybit_rest.get_all_perpetual_symbols(category="linear")
            logger.info(f"Found {len(bybit_symbols)} perpetual symbols on Bybit")
        except Exception as e:
            logger.error(f"Error fetching Bybit symbols: {str(e)}")
            bybit_symbols = []
        
        # Create mappings between exchange symbol formats
        binance_mapping = {}
        bybit_mapping = {}
        
        # Build standardized symbol mapping (both directions)
        common_symbols = []
        
        for binance_symbol in binance_symbols:
            # Try both possible Bybit formats: with and without hyphen
            base = binance_symbol[:-4]  # Remove USDT
            bybit_format1 = f"{base}-USDT"  # With hyphen
            bybit_format2 = binance_symbol  # Without hyphen
            
            if bybit_format1 in bybit_symbols:
                common_symbols.append(binance_symbol)
                binance_mapping[binance_symbol] = bybit_format1
                bybit_mapping[bybit_format1] = binance_symbol
            elif bybit_format2 in bybit_symbols:
                common_symbols.append(binance_symbol)
                binance_mapping[binance_symbol] = bybit_format2
                bybit_mapping[bybit_format2] = binance_symbol
        
        # Store mappings
        self.binance_to_bybit = binance_mapping
        self.bybit_to_binance = bybit_mapping
        
        logger.info(f"Found {len(common_symbols)} common symbols between Binance and Bybit")
        return common_symbols

    def _filter_symbols(self, symbols: List[str]) -> List[str]:
        """
        Apply filters to the symbol list.
        
        Args:
            symbols: List of symbols to filter
            
        Returns:
            Filtered list of symbols
        """
        filtered_symbols = []
        for symbol in symbols:
            # Apply exclude filter
            if symbol in self.exclude_symbols:
                logger.debug(f"Excluding symbol {symbol} as configured")
                continue
            
            # Apply include_only filter if specified
            if self.include_only and symbol not in self.include_only:
                logger.debug(f"Skipping symbol {symbol} - not in include_only list")
                continue
            
            # Apply minimum price filter if specified
            # (Would need to get price data - skipping for now to avoid extra API calls)
            
            # Apply minimum volume filter if specified
            # (Would need to get volume data - skipping for now to avoid extra API calls)
            
            filtered_symbols.append(symbol)
        
        return filtered_symbols
    
    def _convert_to_bybit_symbol(self, binance_symbol: str) -> str:
        """Convert Binance symbol format to Bybit"""
        # Bybit format is typically XXX-YYY while Binance is XXXYYY
        # For most USDT pairs, we need to add a hyphen
        if binance_symbol.endswith('USDT'):
            base = binance_symbol[:-4]
            bybit_symbol = f"{base}-USDT"
            
            # Some symbols might not use hyphens on Bybit, or may have different formats
            # In case of errors, we can add special case handling here
            
            return bybit_symbol
        return binance_symbol
    
    def _convert_from_bybit_symbol(self, bybit_symbol: str) -> str:
        """Convert Bybit symbol format to Binance"""
        # Convert Bybit XXX-YYY to Binance XXXYYY
        return bybit_symbol.replace('-', '')
    
    def _verify_data_availability(self):
        """Verify that we're receiving data for the symbols"""
        # Check Binance data
        binance_data = self.binance_ws.get_mark_price_data()
        binance_symbols_with_data = set(binance_data.keys())
        logger.info(f"Received Binance mark price data for {len(binance_symbols_with_data)} symbols")
        
        # Check which Bybit symbols we have data for
        bybit_data = self.bybit_ws.get_ticker_data()
        bybit_symbols_with_data = set()
        for bybit_symbol in bybit_data.keys():
            # Convert to Binance format using our mapping
            binance_symbol = self.bybit_to_binance.get(bybit_symbol)
            if binance_symbol:
                bybit_symbols_with_data.add(binance_symbol)
        
        logger.info(f"Received Bybit ticker data for {len(bybit_symbols_with_data)} symbols")
        
        # Find symbols that have data from both exchanges
        symbols_with_both_data = binance_symbols_with_data.intersection(bybit_symbols_with_data)
        
        # Find symbols that are missing data from either exchange
        missing_binance_data = set(self.symbols) - binance_symbols_with_data
        missing_bybit_data = set(self.symbols) - bybit_symbols_with_data
        
        if missing_binance_data:
            logger.warning(f"Missing Binance data for {len(missing_binance_data)} symbols: {list(missing_binance_data)[:5]}...")
            
        if missing_bybit_data:
            logger.warning(f"Missing Bybit data for {len(missing_bybit_data)} symbols: {list(missing_bybit_data)[:5]}...")
            
        if not symbols_with_both_data:
            logger.error("No symbols have data available on both exchanges!")
        else:
            logger.info(f"Found {len(symbols_with_both_data)} symbols with data on both exchanges")
            
            # Update symbols list to only include those with data from both exchanges
            common_symbols = list(symbols_with_both_data.intersection(self.symbols))
            
            if len(common_symbols) < len(self.symbols):
                logger.warning(f"Reducing symbol list from {len(self.symbols)} to {len(common_symbols)} symbols with available data")
                self.symbols = common_symbols
                
                # Update positions dictionary to include only common symbols
                new_positions = {}
                for symbol in self.symbols:
                    if symbol in self.positions:
                        new_positions[symbol] = self.positions[symbol]
                    else:
                        new_positions[symbol] = {
                            'binance': {'active': False, 'side': None, 'qty': 0, 'entry_time': None, 'entry_price': 0},
                            'bybit': {'active': False, 'side': None, 'qty': 0, 'entry_time': None, 'entry_price': 0}
                        }
                self.positions = new_positions

    def _check_ws_connections(self):
        """
        Check WebSocket connections and attempt reconnection if needed.
        Returns:
            bool: True if all connections are active
        """
        all_connected = True
        
        # Check Binance connection
        if not self.ws_connected['binance'] or not hasattr(self, 'binance_ws'):
            logger.warning("Binance WebSocket disconnected, attempting to reconnect...")
            try:
                if hasattr(self, 'binance_ws'):
                    # Try to properly close existing connection if it exists
                    try:
                        self.binance_ws.close()
                    except:
                        pass
                
                # Create new connection
                self.binance_ws = BinanceWebSocketClient(
                    futures_symbols=[s.lower() for s in self.symbols],
                    mark_price_freq="1s",
                    use_all_market_stream=True
                )
                self.binance_ws.connect()
                time.sleep(2)  # Give connection time to establish
                self.ws_connected['binance'] = True
                logger.info("Binance WebSocket reconnected successfully")
            except Exception as e:
                logger.error(f"Failed to reconnect to Binance WebSocket: {str(e)}")
                self.ws_connected['binance'] = False
                all_connected = False
        
        # Check Bybit connection
        if not self.ws_connected['bybit'] or not hasattr(self, 'bybit_ws') or not self.bybit_ws.connected:
            logger.warning("Bybit WebSocket disconnected, attempting to reconnect...")
            try:
                if hasattr(self, 'bybit_ws'):
                    # Try to properly close existing connection if it exists
                    try:
                        self.bybit_ws.close()
                    except:
                        pass
                
                # Create new connection
                self.bybit_ws = BybitWebSocketClient(channel_type="linear", testnet=False)
                self.bybit_ws.connect()
                time.sleep(2)  # Give connection time to establish
                
                if self.bybit_ws.connected:
                    self.ws_connected['bybit'] = True
                    logger.info("Bybit WebSocket reconnected successfully")
                    
                    # Re-subscribe to tickers
                    self._subscribe_to_bybit_tickers()
                else:
                    logger.error("Bybit WebSocket reconnection failed")
                    self.ws_connected['bybit'] = False
                    all_connected = False
            except Exception as e:
                logger.error(f"Failed to reconnect to Bybit WebSocket: {str(e)}")
                self.ws_connected['bybit'] = False
                all_connected = False
        
        return all_connected

    def calculate_metrics(self, symbol: str) -> Dict[str, Any]:
        """
        Calculate arbitrage metrics for a symbol across exchanges.
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
        
        Returns:
            Dictionary containing calculated metrics or None if data is missing
        """
        # Verify WebSocket connections are active
        self._check_ws_connections()
        
        # Get funding rate data from Binance
        binance_data = None
        if self.ws_connected['binance']:
            binance_data = self.binance_ws.get_mark_price(symbol)
        
        if not binance_data:
            logger.debug(f"No Binance data available for {symbol}")
            return None
            
        # Get the corresponding Bybit symbol from our mapping
        bybit_symbol = self.binance_to_bybit.get(symbol)
        if not bybit_symbol:
            logger.warning(f"No Bybit symbol mapping found for {symbol}")
            return None
        
        # Get funding rate data from Bybit - with fallback to REST API
        bybit_data = None
        
        # Try WebSocket first if connected
        if self.ws_connected['bybit']:
            bybit_data = self.bybit_ws.get_ticker_data(bybit_symbol)
        
        # If WebSocket data is not available, try REST API
        if not bybit_data:
            try:
                logger.debug(f"WebSocket data not available for {bybit_symbol}, trying REST API")
                bybit_data = self.bybit_rest.get_tickers(symbol=bybit_symbol)
                if bybit_data and len(bybit_data) > 0:
                    bybit_data = bybit_data[0]
                else:
                    logger.debug(f"No data found for {bybit_symbol} via REST API")
                    return None
            except Exception as e:
                logger.warning(f"Error fetching {bybit_symbol} data from Bybit REST API: {str(e)}")
                return None
        
        # Extract funding rates - updated WebSocket client provides consistent field names
        # but we keep fallbacks for REST API responses
        binance_funding_rate = float(binance_data.get('funding_rate', 0))
        
        # First try the standardized field name from the updated WebSocket client
        bybit_funding_rate = float(bybit_data.get('funding_rate', 0))
        
        # Fallback only if necessary (primarily for REST API responses)
        if bybit_funding_rate == 0 and 'fundingRate' in bybit_data:
            bybit_funding_rate = float(bybit_data.get('fundingRate', 0))
        
        if bybit_funding_rate == 0:
            logger.warning(f"Could not extract funding rate from Bybit data for {symbol}")
            return None
        
        # Calculate the funding rate spread
        funding_spread = binance_funding_rate - bybit_funding_rate
        abs_funding_spread = abs(funding_spread)
        
        # Determine which exchange to go long/short on
        if binance_funding_rate < bybit_funding_rate:
            # Long Binance (receives funding or pays less), Short Bybit (pays funding)
            binance_side = "LONG"
            bybit_side = "SHORT"
        else:
            # Short Binance (receives funding or pays less), Long Bybit (pays funding)
            binance_side = "SHORT"
            bybit_side = "LONG"
        
        # Extract mark prices
        binance_mark_price = float(binance_data['mark_price'])
        
        # Extract from Bybit depending on the format
        if 'mark_price' in bybit_data:
            bybit_mark_price = float(bybit_data['mark_price'])
        elif 'markPrice' in bybit_data:
            bybit_mark_price = float(bybit_data['markPrice'])
        else:
            bybit_mark_price = binance_mark_price  # Fallback to Binance price if Bybit price not available
        
        # Calculate next funding times
        binance_next_funding_time = binance_data.get('next_funding_time', 0)
        
        # Extract from Bybit depending on the format
        if 'next_funding_time' in bybit_data:
            bybit_next_funding_time = bybit_data.get('next_funding_time', 0)
        elif 'nextFundingTime' in bybit_data:
            bybit_next_funding_time = bybit_data.get('nextFundingTime', 0)
        else:
            bybit_next_funding_time = 0
        
        # Calculate time to next funding (use the earlier time)
        now_ms = int(time.time() * 1000)
        binance_time_to_funding_ms = max(0, binance_next_funding_time - now_ms)
        bybit_time_to_funding_ms = max(0, bybit_next_funding_time - now_ms)
        
        # Use the earlier time if both are valid, otherwise use the valid one
        if binance_time_to_funding_ms > 0 and bybit_time_to_funding_ms > 0:
            time_to_funding_ms = min(binance_time_to_funding_ms, bybit_time_to_funding_ms)
        elif binance_time_to_funding_ms > 0:
            time_to_funding_ms = binance_time_to_funding_ms
        elif bybit_time_to_funding_ms > 0:
            time_to_funding_ms = bybit_time_to_funding_ms
        else:
            time_to_funding_ms = 28800000  # Default to 8 hours (in milliseconds)
        
        time_to_funding_hours = time_to_funding_ms / (1000 * 60 * 60)
        
        # Calculate position sizes
        notional_value = self.position_size_usd
        binance_qty = notional_value / binance_mark_price
        bybit_qty = notional_value / bybit_mark_price
        
        # Calculate trading costs (entry and exit on both exchanges)
        binance_fee = notional_value * self.binance_fee_rate * 2  # Entry and exit fees
        bybit_fee = notional_value * self.bybit_fee_rate * 2  # Entry and exit fees
        slippage_cost = notional_value * self.slippage * 4  # Slippage for entry and exit on two exchanges
        total_trading_cost = binance_fee + bybit_fee + slippage_cost
        
        # Calculate expected profit for a single funding interval
        expected_profit_per_funding = abs_funding_spread * notional_value
        
        # Calculate the number of funding events needed to break even
        if expected_profit_per_funding > 0:
            break_even_events = max(1, math.ceil(total_trading_cost / expected_profit_per_funding))
        else:
            break_even_events = float('inf')  # Infinite if no profit
        
        # Calculate annualized returns
        # Assuming 3 funding events per day (8-hour funding interval)
        funding_events_per_year = 365 * 3
        
        # Only profitable if we can hold through enough funding events
        if break_even_events < float('inf') and expected_profit_per_funding > 0:
            # Calculate APR based on optimal holding period
            optimal_holding_periods = max(1, break_even_events)
            total_profit = (expected_profit_per_funding * optimal_holding_periods) - total_trading_cost
            holding_time_fraction = optimal_holding_periods / funding_events_per_year
            
            # Annual rate of return
            apr = (total_profit / notional_value) / holding_time_fraction if holding_time_fraction > 0 else 0
            apy = ((1 + (total_profit / notional_value)) ** (1 / holding_time_fraction) - 1) if holding_time_fraction > 0 else 0
        else:
            # Not profitable at any holding period
            apr = 0
            apy = 0
            
        # Determine next funding times in human-readable format
        binance_next_funding_str = datetime.fromtimestamp(binance_next_funding_time/1000).strftime('%Y-%m-%d %H:%M:%S') if binance_next_funding_time > 0 else "Unknown"
        bybit_next_funding_str = datetime.fromtimestamp(bybit_next_funding_time/1000).strftime('%Y-%m-%d %H:%M:%S') if bybit_next_funding_time > 0 else "Unknown"
        
        return {
            'symbol': symbol,
            'binance_funding_rate': binance_funding_rate,
            'bybit_funding_rate': bybit_funding_rate,
            'funding_spread': funding_spread,
            'abs_funding_spread': abs_funding_spread,
            'binance_side': binance_side,
            'bybit_side': bybit_side,
            'binance_mark_price': binance_mark_price,
            'bybit_mark_price': bybit_mark_price,
            'binance_next_funding': binance_next_funding_str,
            'bybit_next_funding': bybit_next_funding_str,
            'next_funding_time': min(binance_next_funding_time, bybit_next_funding_time) if binance_next_funding_time > 0 and bybit_next_funding_time > 0 else max(binance_next_funding_time, bybit_next_funding_time),
            'time_to_funding_hours': time_to_funding_hours,
            'binance_qty': binance_qty,
            'bybit_qty': bybit_qty,
            'notional_value': notional_value,
            'expected_profit_per_funding': expected_profit_per_funding,
            'total_trading_cost': total_trading_cost,
            'break_even_events': break_even_events,
            'is_profitable': break_even_events < float('inf') and expected_profit_per_funding > 0,
            'apr': apr,
            'apy': apy
        }
    
    def should_execute_arbitrage(self, metrics: Dict[str, Any]) -> bool:
        """
        Determine if arbitrage should be executed based on the metrics.
        
        Args:
            metrics: Dictionary containing calculated metrics
            
        Returns:
            Boolean indicating whether to execute the arbitrage
        """
        # Check if we have valid metrics
        if not metrics:
            return False
            
        # Check if funding spread is above threshold
        if metrics['abs_funding_spread'] < self.min_funding_spread:
            return False
            
        # Check if strategy would be profitable
        if not metrics['is_profitable']:
            return False
            
        return True
    
    def execute_arbitrage(self, symbol: str, metrics: Dict[str, Any]) -> bool:
        """
        Execute cross-exchange arbitrage for a symbol.
        
        Args:
            symbol: Trading pair symbol
            metrics: Dictionary containing calculated metrics
            
        Returns:
            Boolean indicating success/failure
        """
        binance_side = metrics['binance_side']
        bybit_side = metrics['bybit_side']
        
        # Get the corresponding Bybit symbol from our mapping
        bybit_symbol = self.binance_to_bybit.get(symbol)
        if not bybit_symbol:
            logger.error(f"No Bybit symbol mapping found for {symbol}, cannot execute arbitrage")
            return False
        
        logger.info(f"Executing cross-exchange arbitrage for {symbol}: {binance_side} on Binance, {bybit_side} on Bybit")
        
        try:
            # Round quantities to appropriate precision
            binance_qty = self._round_down(metrics['binance_qty'], 5)
            bybit_qty = self._round_down(metrics['bybit_qty'], 5)
            
            # Execute Binance trade
            binance_order_side = "BUY" if binance_side == "LONG" else "SELL"
            logger.info(f"Opening {binance_order_side} position for {binance_qty} {symbol} on Binance")
            
            binance_order_result = self._place_binance_futures_order(
                symbol=symbol,
                side=binance_order_side,
                quantity=binance_qty,
                order_type="MARKET"
            )
            
            # Execute Bybit trade using the mapped symbol
            bybit_order_side = "BUY" if bybit_side == "LONG" else "SELL"
            logger.info(f"Opening {bybit_order_side} position for {bybit_qty} {bybit_symbol} on Bybit")
            
            bybit_order_result = self._place_bybit_futures_order(
                symbol=bybit_symbol,
                side=bybit_order_side,
                quantity=bybit_qty,
                order_type="MARKET"
            )
            
            # Update position tracking
            self.positions[symbol]['binance'] = {
                'active': True,
                'side': binance_side,
                'qty': binance_qty,
                'entry_time': datetime.now(),
                'entry_price': metrics['binance_mark_price']
            }
            
            self.positions[symbol]['bybit'] = {
                'active': True,
                'side': bybit_side,
                'qty': bybit_qty,
                'entry_time': datetime.now(),
                'entry_price': metrics['bybit_mark_price']
            }
            
            logger.info(f"Successfully opened cross-exchange arbitrage positions for {symbol}")
            return True
            
        except Exception as e:
            logger.error(f"Error executing arbitrage for {symbol}: {str(e)}")
            logger.error(traceback.format_exc())
            
            # Try to close any position that might have been opened
            self.close_position(symbol)
            
            return False

    def close_position(self, symbol: str) -> bool:
        """
        Close existing positions for a symbol on both exchanges.
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            Boolean indicating success/failure
        """
        success = True
        
        # Get the corresponding Bybit symbol from our mapping
        bybit_symbol = self.binance_to_bybit.get(symbol)
        if not bybit_symbol:
            logger.error(f"No Bybit symbol mapping found for {symbol}, cannot close position")
            return False
        
        # Close Binance position if active
        if self.positions[symbol]['binance']['active']:
            try:
                qty = self.positions[symbol]['binance']['qty']
                side = self.positions[symbol]['binance']['side']
                
                # Determine close order side (opposite of position side)
                close_side = "SELL" if side == "LONG" else "BUY"
                
                logger.info(f"Closing {side} position for {qty} {symbol} on Binance with {close_side} order")
                
                close_result = self._place_binance_futures_order(
                    symbol=symbol,
                    side=close_side,
                    quantity=qty,
                    order_type="MARKET"
                )
                
                # Update position tracking
                self.positions[symbol]['binance']['active'] = False
                self.positions[symbol]['binance']['exit_time'] = datetime.now()
                
                logger.info(f"Successfully closed {side} position for {symbol} on Binance")
            except Exception as e:
                logger.error(f"Error closing Binance position for {symbol}: {str(e)}")
                logger.error(traceback.format_exc())
                success = False
        
        # Close Bybit position if active
        if self.positions[symbol]['bybit']['active']:
            try:
                qty = self.positions[symbol]['bybit']['qty']
                side = self.positions[symbol]['bybit']['side']
                
                # Determine close order side (opposite of position side)
                close_side = "SELL" if side == "LONG" else "BUY"
                
                logger.info(f"Closing {side} position for {qty} {bybit_symbol} on Bybit with {close_side} order")
                
                close_result = self._place_bybit_futures_order(
                    symbol=bybit_symbol,
                    side=close_side,
                    quantity=qty,
                    order_type="MARKET"
                )
                
                # Update position tracking
                self.positions[symbol]['bybit']['active'] = False
                self.positions[symbol]['bybit']['exit_time'] = datetime.now()
                
                logger.info(f"Successfully closed {side} position for {symbol} on Bybit")
            except Exception as e:
                logger.error(f"Error closing Bybit position for {symbol}: {str(e)}")
                logger.error(traceback.format_exc())
                success = False
        
        return success
    
    def _update_metrics(self):
        """
        Calculate and update metrics for all symbols.
        """
        new_metrics = {}
        
        for symbol in self.symbols:
            try:
                metrics = self.calculate_metrics(symbol)
                if metrics:
                    new_metrics[symbol] = metrics
            except Exception as e:
                logger.error(f"Error calculating metrics for {symbol}: {str(e)}")
        
        # Store the updated metrics
        self.metrics = new_metrics
        
        # Rank opportunities
        self._rank_opportunities()
    
    def _rank_opportunities(self):
        """
        Rank symbols by profitability for easier decision making.
        """
        ranked = []
        
        for symbol, metrics in self.metrics.items():
            # Only include profitable opportunities in ranking
            if metrics['is_profitable'] and metrics['abs_funding_spread'] >= self.min_funding_spread:
                ranked.append({
                    'symbol': symbol,
                    'funding_spread': metrics['funding_spread'],
                    'apr': metrics['apr'],
                    'break_even_events': metrics['break_even_events'],
                    'binance_side': metrics['binance_side'],
                    'bybit_side': metrics['bybit_side'],
                    'metrics': metrics
                })
        
        # Sort by APR (highest to lowest)
        self.opportunities = sorted(ranked, key=itemgetter('apr'), reverse=True)
    
    def _format_countdown(self, hours):
        """
        Format hours to a countdown timer (HH:MM:SS)
        
        Args:
            hours: Time in hours
            
        Returns:
            Formatted string as HH:MM:SS
        """
        total_seconds = int(hours * 3600)
        hours_part = total_seconds // 3600
        minutes_part = (total_seconds % 3600) // 60
        seconds_part = total_seconds % 60
        
        return f"{hours_part:02d}:{minutes_part:02d}:{seconds_part:02d}"
    
    def _display_metrics(self):
        """Display metrics in the terminal"""
        print(f"\n{'=' * 120}")
        print(f" CROSS-EXCHANGE FUNDING ARBITRAGE MONITOR - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'=' * 120}")
        
        # Show active positions summary
        active_positions = sum(
            1 for symbol in self.symbols 
            if symbol in self.positions 
            and self.positions[symbol]['binance']['active'] 
            and self.positions[symbol]['bybit']['active']
        )
        print(f"\nActive Positions: {active_positions}/{self.max_positions}")
        
        # Best opportunities table (top 15)
        if self.opportunities:
            print("\nBEST CROSS-EXCHANGE FUNDING OPPORTUNITIES:")
            
            # Prepare data for tabulate
            best_data = []
            for i, item in enumerate(self.opportunities[:15]):
                symbol = item['symbol']
                m = item['metrics']
                
                # Show if we have an active position
                position_info = ""
                if (self.positions[symbol]['binance']['active'] and 
                    self.positions[symbol]['bybit']['active']):
                    position_info = f"[ACTIVE]"
                
                # Highlight top opportunities
                prefix = "→ " if i < self.max_positions and not (
                    self.positions[symbol]['binance']['active'] and 
                    self.positions[symbol]['bybit']['active']
                ) else "  "
                
                # Format time to funding as countdown timer
                time_to_funding = self._format_countdown(m['time_to_funding_hours'])
                
                best_data.append([
                    prefix + symbol,
                    f"{m['funding_spread']*100:+.5f}%",
                    f"{m['binance_funding_rate']*100:+.5f}%",
                    f"{m['bybit_funding_rate']*100:+.5f}%",
                    f"{m['binance_side']}/{m['bybit_side']}",
                    time_to_funding,
                    f"${m['expected_profit_per_funding']:.4f}",
                    f"{m['break_even_events']} events",
                    f"{m['apr']*100:.2f}%",
                    position_info
                ])
            
            print(tabulate(
                best_data, 
                headers=["Symbol", "Spread", "Binance Rate", "Bybit Rate", "Sides", 
                        "Countdown", "Profit/Funding", "Break Even", "APR", "Position"]
            ))
        else:
            print("\nNo profitable opportunities found at current thresholds.")
        
        # Active positions table
        active_data = []
        for symbol in self.symbols:
            binance_pos = self.positions[symbol]['binance']
            bybit_pos = self.positions[symbol]['bybit']
            
            if binance_pos['active'] and bybit_pos['active']:
                m = self.metrics.get(symbol, {})
                if not m:
                    continue
                    
                # Calculate position metrics
                entry_time = binance_pos['entry_time'].strftime('%Y-%m-%d %H:%M:%S')
                duration = (datetime.now() - binance_pos['entry_time']).total_seconds() / 3600  # hours
                
                # Add position info
                active_data.append([
                    symbol,
                    f"{binance_pos['side']}/{bybit_pos['side']}",
                    f"{binance_pos['qty']:.5f}/{bybit_pos['qty']:.5f}",
                    f"${m['notional_value']:.2f}",
                    entry_time,
                    f"{duration:.2f} hrs",
                    f"${m['expected_profit_per_funding']:.4f}",
                    f"{m['apr']*100:.2f}%"
                ])
        
        if active_data:
            print("\n\nACTIVE POSITIONS:")
            print(tabulate(
                active_data, 
                headers=["Symbol", "Sides", "Quantities", "Notional Value", 
                        "Entry Time", "Duration", "Profit/Funding", "APR"]
            ))
        
        print(f"\n{'=' * 120}")
        print(f"Monitoring {len(self.metrics)} symbols - {len(self.opportunities)} profitable opportunities")
        print(f"{'=' * 120}")
    
    def _place_binance_futures_order(self, symbol, side, quantity, order_type):
        """
        Place an order on the Binance futures market (placeholder).
        In a real implementation, this would use the Binance API.
        """
        logger.info(f"[MOCK] Binance Futures {side} order for {quantity} {symbol}")
        # In a real implementation, would call the Binance API here
        return {"orderId": f"mock-binance-order-id-{side.lower()}"}
    
    def _place_bybit_futures_order(self, symbol, side, quantity, order_type):
        """
        Place an order on the Bybit futures market (placeholder).
        In a real implementation, this would use the Bybit API.
        """
        logger.info(f"[MOCK] Bybit Futures {side} order for {quantity} {symbol}")
        # In a real implementation, would call the Bybit API here
        return {"orderId": f"mock-bybit-order-id-{side.lower()}"}
    
    def _round_down(self, value, decimals):
        """Round down value to specified decimal places"""
        factor = 10 ** decimals
        return math.floor(value * factor) / factor
    
    def run(self):
        """
        Main loop for the cross-exchange funding arbitrage strategy.
        """
        logger.info("Starting cross-exchange funding arbitrage strategy")
        
        try:
            while True:
                # Clear the terminal for better display
                os.system('cls' if os.name == 'nt' else 'clear')
                
                # Calculate metrics for all symbols
                self._update_metrics()
                
                # Display metrics
                self._display_metrics()
                
                # Count current active positions
                active_positions = sum(
                    1 for symbol in self.symbols 
                    if symbol in self.positions 
                    and self.positions[symbol]['binance']['active'] 
                    and self.positions[symbol]['bybit']['active']
                )
                available_slots = self.max_positions - active_positions
                
                # Check existing positions first
                for symbol in self.symbols:
                    # Skip if symbol not in positions dictionary
                    if symbol not in self.positions:
                        continue
                        
                    # Check if we have active positions on both exchanges
                    binance_active = self.positions[symbol]['binance']['active']
                    bybit_active = self.positions[symbol]['bybit']['active']
                    
                    if binance_active and bybit_active:
                        try:
                            # Get latest metrics
                            metrics = self.metrics.get(symbol)
                            
                            if not metrics:
                                continue
                            
                            # Check if funding spread is still favorable
                            # Close positions if spread drops below half of the threshold
                            if metrics['abs_funding_spread'] < self.min_funding_spread / 2:
                                logger.info(f"Funding spread for {symbol} has decreased ({metrics['abs_funding_spread']:.6f}), closing positions")
                                self.close_position(symbol)
                                
                            # Check if sides need to be flipped
                            current_binance_side = self.positions[symbol]['binance']['side']
                            optimal_binance_side = metrics['binance_side']
                            
                            if current_binance_side != optimal_binance_side:
                                logger.info(f"Optimal sides for {symbol} have changed, closing positions")
                                self.close_position(symbol)
                                
                        except Exception as e:
                            logger.error(f"Error processing positions for {symbol}: {str(e)}")
                    
                    # If positions are inconsistent (only active on one exchange), close them
                    elif binance_active or bybit_active:
                        logger.warning(f"Inconsistent positions for {symbol}, closing all positions")
                        self.close_position(symbol)
                
                # Open new positions for top opportunities if we have slots available
                if available_slots > 0 and self.opportunities:
                    # Find top opportunities that we don't already have positions in
                    new_opportunities = [
                        item for item in self.opportunities 
                        if not self.positions[item['symbol']]['binance']['active']
                        and not self.positions[item['symbol']]['bybit']['active']
                    ]
                    
                    # Take positions in top N opportunities
                    for i, opportunity in enumerate(new_opportunities):
                        if i >= available_slots:
                            break
                            
                        symbol = opportunity['symbol']
                        metrics = opportunity['metrics']
                        
                        try:
                            logger.info(f"Opening cross-exchange positions for top opportunity: {symbol}")
                            self.execute_arbitrage(symbol, metrics)
                        except Exception as e:
                            logger.error(f"Error opening positions for {symbol}: {str(e)}")
                
                # Sleep before next check
                time.sleep(self.check_interval)
                
        except KeyboardInterrupt:
            logger.info("Strategy interrupted by user")
        finally:
            # Close any open positions
            for symbol in self.symbols:
                if (self.positions[symbol]['binance']['active'] or 
                   self.positions[symbol]['bybit']['active']):
                    logger.info(f"Closing positions for {symbol} due to shutdown")
                    self.close_position(symbol)
            
            # Close WebSocket connections
            self.binance_ws.close()
            self.bybit_ws.close()
            logger.info("Strategy shutdown complete")


# Configuration file loader
def load_config(config_path):
    """Load configuration from file"""
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading config: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Return default config
        return {
            "symbols": ["btcusdt", "ethusdt", "solusdt"],
            "position_size_usd": 1000,
            "min_funding_spread": 0.0005,  # 0.05% funding spread
            "futures_fee_rate": 0.0004,    # Binance fee rate
            "bybit_fee_rate": 0.0006,      # Bybit fee rate
            "slippage": 0.0003,            # 0.03% slippage
            "check_interval": 30,          # Check every 30 seconds
            "use_all_symbols": True,
            "risk_management": {
                "max_positions": 5,
                "max_drawdown": 0.05
            },
            "symbol_filters": {
                "min_price": 0,
                "min_volume_usd": 0,
                "exclude": ["BTCDOMUSDT", "DEFIUSDT"],
                "include_only": []
            }
        }


if __name__ == "__main__":
    # Load configuration
    config = load_config("config.json")
    
    # Add min_funding_spread if not present in config
    if 'min_funding_spread' not in config:
        config['min_funding_spread'] = 0.0005  # Default 0.05% spread threshold
    
    # Add bybit_fee_rate if not present in config
    if 'bybit_fee_rate' not in config:
        config['bybit_fee_rate'] = 0.0006  # Default 0.06% fee on Bybit
    
    # Create and run the strategy
    strategy = CrossExchangeFundingArbitrageStrategy(config)
    strategy.run()
