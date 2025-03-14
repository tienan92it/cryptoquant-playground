import sys
import time
import logging
import traceback
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

# Import exchange clients
from exchanges.binance.ws_client import BinanceWebSocketClient
from exchanges.bybit.rest_client import BybitRestClient
from exchanges.bybit.ws_client import BybitWebSocketClient

# Import utility modules
from utils.config_loader import load_config
from utils.exchange_utils import (
    fetch_binance_futures_symbols, 
    fetch_bybit_perpetual_symbols,
    create_symbol_mappings,
    filter_symbols
)
from utils.ws_manager import (
    initialize_all_websockets,
    check_websocket_connections,
    close_all_websockets
)
from utils.metrics_calculator import (
    calculate_funding_metrics,
    rank_opportunities,
    should_execute_arbitrage
)
from utils.display_utils import (
    display_funding_metrics,
    display_connection_status
)
from utils.position_manager import (
    execute_arbitrage,
    close_position,
    initialize_positions,
    round_down
)

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
    1. Finds funding rate discrepancies between exchanges (Binance, Bybit, OKX)
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
        
        # Load configuration parameters
        self.min_funding_spread = config.get('min_funding_spread', 0.0005)
        self.position_size_usd = config.get('position_size_usd', 1000)
        self.check_interval = config.get('check_interval', 30)
        self.max_positions = config.get('risk_management', {}).get('max_positions', 5)
        self.use_all_symbols = config.get('use_all_symbols', False)
        # Set default to include all three exchanges
        self.exchanges_to_use = config.get('exchanges', ['binance', 'bybit', 'okx'])
        
        # Ensure exchange names are lowercase for consistency
        self.exchanges_to_use = [e.lower() for e in self.exchanges_to_use]
        
        # Symbol filters
        self.exclude_symbols = [s.upper() for s in config.get('symbol_filters', {}).get('exclude', [])]
        self.include_only = [s.upper() for s in config.get('symbol_filters', {}).get('include_only', [])]
        
        logger.info(f"Initializing strategy with exchanges: {', '.join(self.exchanges_to_use)}")
        
        # Initialize exchange clients
        self._initialize_exchange_clients()
        
        # Discover common symbols across exchanges
        self._discover_common_symbols()
        
        # Initialize position tracking
        self.positions = initialize_positions(self.symbols)
        
        # Storage for metrics and opportunities
        self.metrics = {}
        self.opportunities = []
        
        logger.info(f"Initialized cross-exchange funding arbitrage strategy with {len(self.symbols)} symbols")
        logger.info(f"Minimum funding spread threshold: {self.min_funding_spread}")
        logger.info(f"Maximum positions: {self.max_positions}")
    
    def _initialize_exchange_clients(self):
        """Initialize REST clients for exchanges"""
        # Initialize REST clients
        if 'bybit' in self.exchanges_to_use:
            self.bybit_rest = BybitRestClient(testnet=False)
        
        # Initialize OKX REST client (when available)
        if 'okx' in self.exchanges_to_use:
            # This would be implemented if OKX REST client exists
            pass
    
    def _discover_common_symbols(self):
        """Discover common symbols across all exchanges"""
        # Fetch symbols from each exchange
        binance_symbols = []
        bybit_symbols = []
        okx_symbols = []
        
        if 'binance' in self.exchanges_to_use:
            binance_symbols = fetch_binance_futures_symbols(
                exclude_symbols=self.exclude_symbols,
                include_only=self.include_only
            )
        
        if 'bybit' in self.exchanges_to_use and hasattr(self, 'bybit_rest'):
            bybit_symbols = fetch_bybit_perpetual_symbols(self.bybit_rest)
        
        if 'okx' in self.exchanges_to_use:
            # Replace with actual API call when OKX client is available
            from utils.exchange_utils import fetch_okx_perpetual_symbols
            okx_symbols = fetch_okx_perpetual_symbols()
        
        # Find common symbols and create mappings
        self.available_symbols, self.symbol_mappings = create_symbol_mappings(
            binance_symbols, bybit_symbols, okx_symbols
        )
        
        logger.info(f"Found {len(self.available_symbols)} common symbols across selected exchanges")
        
        # Filter symbols based on configuration
        if self.use_all_symbols:
            self.symbols = filter_symbols(self.available_symbols, self.config)
            logger.info(f"Using {len(self.symbols)} common symbols after filtering")
        else:
            # Use only the configured symbols that are available
            configured_symbols = [s.upper() for s in self.config.get('symbols', [])]
            self.symbols = [s for s in configured_symbols if s in self.available_symbols]
            logger.info(f"Using {len(self.symbols)} configured symbols that are available across exchanges")
            
            # Log any configured symbols that aren't available
            unavailable = [s for s in configured_symbols if s not in self.available_symbols]
            if unavailable:
                logger.warning(f"The following configured symbols are not available on all exchanges: {unavailable}")
    
    def initialize_websockets(self):
        """Initialize WebSocket connections to exchanges"""
        # Initialize WebSockets using our utility function
        self.ws_clients, self.ws_connected = initialize_all_websockets(
            symbols=self.symbols,
            symbol_mappings=self.symbol_mappings,
            exchanges_to_use=self.exchanges_to_use
        )
        
        # Wait for initial data
        logger.info("Waiting for initial data...")
        time.sleep(15)
        
        # Verify data availability
        self._verify_data_availability()
    
    def _verify_data_availability(self):
        """Verify that we're receiving data for the symbols"""
        # Track symbols with data from each exchange
        data_available = {exchange: set() for exchange in self.exchanges_to_use}
        
        # Check Binance data
        if 'binance' in self.exchanges_to_use and 'binance' in self.ws_clients:
            binance_data = self.ws_clients['binance'].get_mark_price_data()
            for symbol in binance_data:
                data_available['binance'].add(symbol.upper())
        
        # Check Bybit data
        if 'bybit' in self.exchanges_to_use and 'bybit' in self.ws_clients:
            bybit_data = self.ws_clients['bybit'].get_ticker_data()
            for bybit_symbol in bybit_data:
                # Find the corresponding standard symbol
                for std_symbol, mapping in self.symbol_mappings.items():
                    if mapping.get('bybit') == bybit_symbol:
                        data_available['bybit'].add(std_symbol)
                        break
        
        # Check OKX data (when available)
        if 'okx' in self.exchanges_to_use and 'okx' in self.ws_clients:
            okx_data = self.ws_clients['okx'].get_funding_rate_data()
            for okx_symbol in okx_data:
                # Find the corresponding standard symbol
                for std_symbol, mapping in self.symbol_mappings.items():
                    if mapping.get('okx') == okx_symbol:
                        data_available['okx'].add(std_symbol)
                        break
        
        # Find symbols with data from all exchanges
        symbols_with_data = set(self.symbols)
        for exchange, symbols in data_available.items():
            logger.info(f"Received data for {len(symbols)} symbols from {exchange}")
            symbols_with_data &= symbols
        
        if not symbols_with_data:
            logger.error("No symbols have data available on all exchanges!")
        else:
            logger.info(f"Found {len(symbols_with_data)} symbols with data on all exchanges")
            
            # Update symbols list if needed
            if len(symbols_with_data) < len(self.symbols):
                logger.warning(f"Reducing symbol list from {len(self.symbols)} to {len(symbols_with_data)} symbols with available data")
                self.symbols = list(symbols_with_data)
                
                # Update positions dictionary
                new_positions = {}
                for symbol in self.symbols:
                    if symbol in self.positions:
                        new_positions[symbol] = self.positions[symbol]
                    else:
                        new_positions[symbol] = {
                            exchange: {'active': False, 'side': None, 'qty': 0, 'entry_time': None, 'entry_price': 0}
                            for exchange in self.exchanges_to_use
                        }
                self.positions = new_positions
    
    def update_metrics(self):
        """Calculate and update metrics for all symbols"""
        # First, ensure WebSocket connections are healthy
        if hasattr(self, 'ws_clients') and hasattr(self, 'ws_connected'):
            self.ws_clients, self.ws_connected = check_websocket_connections(
                self.ws_clients, 
                self.ws_connected, 
                self.symbols, 
                self.symbol_mappings
            )
        
        new_metrics = {}
        
        for symbol in self.symbols:
            try:
                # Get data from each exchange
                exchange_data = {}
                
                # Binance data
                if 'binance' in self.exchanges_to_use and self.ws_connected.get('binance'):
                    binance_data = self.ws_clients['binance'].get_mark_price(symbol)
                    if binance_data:
                        exchange_data['binance'] = binance_data
                
                # Bybit data with fallback to REST API
                if 'bybit' in self.exchanges_to_use:
                    bybit_symbol = self.symbol_mappings.get(symbol, {}).get('bybit')
                    if bybit_symbol:
                        bybit_data = None
                        # Try WebSocket first if connected
                        if self.ws_connected.get('bybit'):
                            bybit_data = self.ws_clients['bybit'].get_ticker_data(bybit_symbol)
                        
                        # Fall back to REST API if needed
                        if not bybit_data and hasattr(self, 'bybit_rest'):
                            try:
                                logger.debug(f"WebSocket data not available for {bybit_symbol}, trying REST API")
                                bybit_data = self.bybit_rest.get_tickers(symbol=bybit_symbol)
                                if bybit_data and len(bybit_data) > 0:
                                    bybit_data = bybit_data[0]
                            except Exception as e:
                                logger.warning(f"Error fetching {bybit_symbol} data from Bybit REST API: {str(e)}")
                        
                        if bybit_data:
                            exchange_data['bybit'] = bybit_data
                    else:
                        logger.debug(f"No Bybit mapping for {symbol}")
                
                # OKX data when available
                if 'okx' in self.exchanges_to_use and self.ws_connected.get('okx'):
                    okx_inst_id = self.symbol_mappings.get(symbol, {}).get('okx_instid')
                    if okx_inst_id:
                        okx_data = self.ws_clients['okx'].get_funding_rate_data(okx_inst_id)
                        if okx_data:
                            exchange_data['okx'] = okx_data
                    else:
                        logger.debug(f"No OKX mapping for {symbol}")
                
                # Calculate metrics based on available exchange data
                if len(exchange_data) >= 2:  # Need at least 2 exchanges to calculate metrics
                    metrics = calculate_funding_metrics(
                        symbol=symbol,
                        exchange_data=exchange_data,
                        config=self.config
                    )
                    if metrics:
                        new_metrics[symbol] = metrics
                else:
                    available = [ex for ex in exchange_data.keys()]
                    logger.debug(f"Insufficient exchange data for {symbol}. Available: {available}")
            except Exception as e:
                logger.error(f"Error calculating metrics for {symbol}: {str(e)}")
                logger.debug(traceback.format_exc())
        
        # Store the updated metrics
        self.metrics = new_metrics
        
        # Rank opportunities by profitability
        self.opportunities = rank_opportunities(self.metrics, self.min_funding_spread)
    
    def check_and_manage_positions(self):
        """
        Check existing positions and manage them based on current market conditions.
        Close positions if spread has decreased or sides have changed.
        """
        for symbol in self.symbols:
            # Skip symbols not in our position tracking
            if symbol not in self.positions:
                continue
                
            # Check if we have any active positions for this symbol
            active_positions = {
                exchange: position['active'] 
                for exchange, position in self.positions[symbol].items() 
                if position['active']
            }
            
            if active_positions:
                try:
                    # Get latest metrics
                    metrics = self.metrics.get(symbol)
                    
                    if not metrics:
                        continue
                    
                    # Check if funding spread is still favorable
                    # Close positions if spread drops below half of the threshold
                    if metrics['abs_funding_spread'] < self.min_funding_spread / 2:
                        logger.info(f"Funding spread for {symbol} has decreased ({metrics['abs_funding_spread']:.6f}), closing positions")
                        close_position(
                            symbol=symbol, 
                            positions=self.positions,
                            symbol_mappings=self.symbol_mappings,
                            exchanges_to_use=self.exchanges_to_use
                        )
                        continue
                        
                    # Check if the best exchange pair has changed
                    active_long_exchange = next((exchange for exchange, position in self.positions[symbol].items() 
                                             if position['active'] and position['side'] == 'LONG'), None)
                    active_short_exchange = next((exchange for exchange, position in self.positions[symbol].items() 
                                              if position['active'] and position['side'] == 'SHORT'), None)
                    
                    best_long_exchange = metrics.get('long_exchange')
                    best_short_exchange = metrics.get('short_exchange')
                    
                    # If the optimal exchanges have changed, close positions
                    if ((active_long_exchange and active_long_exchange != best_long_exchange) or
                        (active_short_exchange and active_short_exchange != best_short_exchange)):
                        logger.info(f"Optimal exchange pair for {symbol} has changed, closing positions")
                        close_position(
                            symbol=symbol, 
                            positions=self.positions,
                            symbol_mappings=self.symbol_mappings,
                            exchanges_to_use=self.exchanges_to_use
                        )
                        
                except Exception as e:
                    logger.error(f"Error processing positions for {symbol}: {str(e)}")
    
    def open_new_positions(self):
        """
        Find and open new arbitrage positions based on current opportunities.
        """
        # Count current active positions
        active_positions = sum(
            1 for symbol in self.symbols 
            if symbol in self.positions 
            and any(position['active'] for exchange, position in self.positions[symbol].items())
        )
        
        # Calculate how many new positions we can open
        available_slots = self.max_positions - active_positions
        
        # Open new positions if we have slots available
        if available_slots > 0 and self.opportunities:
            # Find top opportunities without active positions
            new_opportunities = [
                item for item in self.opportunities 
                if not any(self.positions[item['symbol']][exchange]['active'] 
                          for exchange in self.positions[item['symbol']])
            ]
            
            # Take positions in top N opportunities
            for i, opportunity in enumerate(new_opportunities):
                if i >= available_slots:
                    break
                    
                symbol = opportunity['symbol']
                metrics = opportunity['metrics']
                
                # Get the exchanges for this opportunity
                long_exchange = metrics['long_exchange']
                short_exchange = metrics['short_exchange']
                
                # Define the specific exchanges to use for this trade
                trade_exchanges = [long_exchange, short_exchange]
                
                try:
                    logger.info(f"Opening arbitrage positions for {symbol}: Long on {long_exchange}, Short on {short_exchange}")
                    
                    # Execute the trade using the specified exchanges
                    success = execute_arbitrage(
                        symbol=symbol,
                        metrics=metrics,
                        positions=self.positions,
                        symbol_mappings=self.symbol_mappings,
                        exchanges_to_use=trade_exchanges
                    )
                    
                    if not success:
                        logger.warning(f"Failed to execute arbitrage for {symbol}")
                except Exception as e:
                    logger.error(f"Error opening positions for {symbol}: {str(e)}")
    
    def run(self):
        """Main loop for the cross-exchange funding arbitrage strategy."""
        logger.info("Starting cross-exchange funding arbitrage strategy")
        
        # Initialize WebSocket connections
        self.initialize_websockets()
        
        try:
            while True:
                # Clear terminal for better display
                os.system('cls' if os.name == 'nt' else 'clear')
                
                # Calculate metrics for all symbols
                self.update_metrics()
                
                # Display metrics and connection status
                display_funding_metrics(
                    opportunities=self.opportunities,
                    metrics=self.metrics,
                    positions=self.positions,
                    max_positions=self.max_positions
                )
                
                # Show WebSocket connection status
                display_connection_status(self.ws_connected)
                
                # Check and manage existing positions
                self.check_and_manage_positions()
                
                # Open new positions for top opportunities
                self.open_new_positions()
                
                # Sleep before next check
                time.sleep(self.check_interval)
                
        except KeyboardInterrupt:
            logger.info("Strategy interrupted by user")
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {str(e)}")
            logger.error(traceback.format_exc())
        finally:
            # Close all open positions
            for symbol in self.symbols:
                if symbol in self.positions and any(
                    self.positions[symbol].get(exchange, {}).get('active', False) 
                    for exchange in self.exchanges_to_use
                ):
                    logger.info(f"Closing positions for {symbol} due to shutdown")
                    close_position(
                        symbol=symbol, 
                        positions=self.positions,
                        symbol_mappings=self.symbol_mappings,
                        exchanges_to_use=self.exchanges_to_use
                    )
            
            # Close WebSocket connections
            if hasattr(self, 'ws_clients'):
                close_all_websockets(self.ws_clients)
                
            logger.info("Strategy shutdown complete")


if __name__ == "__main__":
    # Load configuration
    config_path = "strategies/config.json"
    config = load_config(config_path)
    
    # Create and run the strategy
    strategy = CrossExchangeFundingArbitrageStrategy(config)
    strategy.run()
