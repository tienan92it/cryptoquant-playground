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

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from exchanges.binance.ws_client import BinanceWebSocketClient

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("funding_arbitrage.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("funding_arbitrage")

class FundingArbitrageStrategy:
    """
    Simple funding fee arbitrage strategy.
    
    This strategy:
    1. Opens a long position when funding rate is negative (shorts pay longs)
    2. Opens a short position when funding rate is positive (longs pay shorts)
    3. Collects funding fees without hedging price exposure
    """
    
    def __init__(self, config):
        """
        Initialize the funding arbitrage strategy.
        
        Args:
            config: Dictionary containing configuration parameters
        """
        self.config = config
        self.min_funding_threshold = config['min_funding_threshold']
        self.position_size_usd = config['position_size_usd']
        self.futures_fee_rate = config['futures_fee_rate']
        self.slippage = config['slippage']
        self.check_interval = config['check_interval']
        self.max_positions = config.get('risk_management', {}).get('max_positions', 5)
        self.use_all_symbols = config.get('use_all_symbols', False)
        
        # Symbol filters
        self.min_price = config.get('symbol_filters', {}).get('min_price', 0)
        self.min_volume = config.get('symbol_filters', {}).get('min_volume_usd', 0)
        self.exclude_symbols = [s.upper() for s in config.get('symbol_filters', {}).get('exclude', [])]
        self.include_only = [s.upper() for s in config.get('symbol_filters', {}).get('include_only', [])]
        
        # Get all trading symbols if configured
        if self.use_all_symbols:
            logger.info("Fetching all available futures symbols...")
            self.symbols = self._fetch_all_futures_symbols()
            logger.info(f"Found {len(self.symbols)} futures symbols")
        else:
            self.symbols = [s.upper() for s in config['symbols']]
            logger.info(f"Using {len(self.symbols)} predefined symbols")
        
        # Connect to WebSocket for real-time data
        self.ws_client = BinanceWebSocketClient(
            futures_symbols=[s.lower() for s in self.symbols],
            mark_price_freq="1s",
            use_all_market_stream=True
        )
        self.ws_client.connect()
        
        # Initialize positions dict to track our positions
        self.positions = {symbol: {'active': False, 'side': None, 'qty': 0} 
                          for symbol in self.symbols}
        
        # Store metrics for display
        self.metrics = {}
        self.ranked_symbols = []
        
        logger.info(f"Initialized funding arbitrage strategy with {len(self.symbols)} symbols")
        logger.info(f"Minimum funding threshold: {self.min_funding_threshold}")
        logger.info(f"Maximum positions: {self.max_positions}")
        
        # Wait for initial data
        logger.info("Waiting for initial data...")
        time.sleep(15)
        
        # Verify data availability
        self._verify_data_availability()
    
    def _fetch_all_futures_symbols(self):
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
    
    def _verify_data_availability(self):
        """Verify that we're receiving data for the symbols"""
        mark_price_data = self.ws_client.get_mark_price_data()
        
        logger.info(f"Received mark price data for {len(mark_price_data)} symbols")
        available_symbols = set(mark_price_data.keys())
        
        # Find symbols with no data
        missing_symbols = set(self.symbols) - available_symbols
        if missing_symbols:
            logger.warning(f"No data available for {len(missing_symbols)} symbols: {list(missing_symbols)[:5]}...")
        
        # Update symbol list to only include those with available data
        if self.use_all_symbols:
            logger.info("Updating symbol list to only include symbols with available data")
            self.symbols = list(available_symbols)
            
            # Update positions dictionary to include all symbols
            for symbol in self.symbols:
                if symbol not in self.positions:
                    self.positions[symbol] = {'active': False, 'side': None, 'qty': 0}
    
    def calculate_metrics(self, symbol):
        """
        Calculate arbitrage metrics for a symbol.
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
        
        Returns:
            Dictionary containing calculated metrics or None if data is missing
        """
        # Get mark price and funding rate data
        mark_price_data = self.ws_client.get_mark_price(symbol)
        if not mark_price_data:
            logger.warning(f"No mark price data for {symbol}")
            return None
            
        # Extract required values
        funding_rate = float(mark_price_data['funding_rate'])
        mark_price = float(mark_price_data['mark_price'])
        next_funding_time = mark_price_data['next_funding_time']
        
        # Calculate time until next funding
        now_ms = int(time.time() * 1000)
        time_to_funding_ms = max(0, next_funding_time - now_ms)
        time_to_funding_hours = time_to_funding_ms / (1000 * 60 * 60)
        
        # Determine the appropriate side
        side = "LONG" if funding_rate < 0 else "SHORT"
        
        # For calculations, always use absolute value of funding rate
        abs_funding_rate = abs(funding_rate)
        
        # Calculate position sizes
        notional_value = self.position_size_usd
        qty = notional_value / mark_price
        
        # Calculate expected profit from a single funding event
        expected_profit_per_funding = abs_funding_rate * notional_value
        profit_ratio_per_funding = expected_profit_per_funding / notional_value
        
        # Calculate trading costs (entry and exit)
        futures_fee = notional_value * self.futures_fee_rate * 2  # Entry and exit fees
        slippage_cost = notional_value * self.slippage * 2  # Slippage for entry and exit
        total_trading_cost = futures_fee + slippage_cost
        
        # Calculate profit after trading costs for a single funding event
        net_profit_per_funding = expected_profit_per_funding - total_trading_cost
        net_profit_ratio_per_funding = net_profit_per_funding / notional_value
        
        # Need to hold position for this many funding events to break even
        break_even_events = 0
        if expected_profit_per_funding > 0:
            break_even_events = max(1, math.ceil(total_trading_cost / expected_profit_per_funding))
        
        # Calculate annualized returns (assuming position held for optimal amount of time)
        funding_intervals_per_year = 1095  # 3 intervals per day, 365 days
        
        # Only profitable if we can hold through enough funding events
        if break_even_events > 0 and expected_profit_per_funding > 0:
            # Calculate APR based on holding for optimal funding periods
            optimal_holding_periods = max(1, break_even_events)
            total_profit = (expected_profit_per_funding * optimal_holding_periods) - total_trading_cost
            holding_time_fraction = optimal_holding_periods / funding_intervals_per_year
            
            # Annualized metrics
            apr = (total_profit / notional_value) / holding_time_fraction if holding_time_fraction > 0 else 0
            apy = ((1 + (total_profit / notional_value)) ** (1 / holding_time_fraction) - 1) if holding_time_fraction > 0 else 0
        else:
            # Not profitable at any holding period
            apr = 0
            apy = 0

        return {
            'symbol': symbol,
            'funding_rate': funding_rate,
            'abs_funding_rate': abs_funding_rate,
            'mark_price': mark_price,
            'position_side': side,
            'next_funding_time': datetime.fromtimestamp(next_funding_time/1000).strftime('%Y-%m-%d %H:%M:%S'),
            'time_to_funding_hours': time_to_funding_hours,
            'notional_value': notional_value,
            'qty': qty,
            'expected_profit_per_funding': expected_profit_per_funding,
            'total_trading_cost': total_trading_cost,
            'net_profit_per_funding': net_profit_per_funding,
            'break_even_events': break_even_events,
            'is_profitable': net_profit_per_funding > 0,
            'apr': apr,
            'apy': apy
        }

    def should_execute_arbitrage(self, metrics):
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
            
        # Check if funding rate is above threshold (in absolute value)
        if metrics['abs_funding_rate'] < self.min_funding_threshold:
            return False
            
        # Check if strategy would be profitable
        if not metrics['is_profitable']:
            return False
            
        return True
    
    def execute_arbitrage(self, symbol, metrics):
        """
        Execute the arbitrage strategy for a symbol.
        
        Args:
            symbol: Trading pair symbol
            metrics: Dictionary containing calculated metrics
            
        Returns:
            Boolean indicating success/failure
        """
        side = metrics['position_side']
        logger.info(f"Executing {side} position for {symbol} to capture funding")
        
        try:
            # Round quantity to appropriate precision
            qty = self._round_down(metrics['qty'], 5)
            
            # Execute trade based on side
            order_side = "BUY" if side == "LONG" else "SELL"
            logger.info(f"Opening {order_side} position for {qty} {symbol} on futures market")
            
            order_result = self._place_futures_order(
                symbol=symbol,
                side=order_side,
                quantity=qty,
                order_type="MARKET"
            )
            
            # Update position tracking
            self.positions[symbol]['active'] = True
            self.positions[symbol]['side'] = side
            self.positions[symbol]['qty'] = qty
            self.positions[symbol]['entry_time'] = datetime.now()
            self.positions[symbol]['entry_price'] = metrics['mark_price']
            self.positions[symbol]['metrics'] = metrics
            
            logger.info(f"Successfully opened {side} position for {symbol}")
            return True
            
        except Exception as e:
            logger.error(f"Error executing arbitrage for {symbol}: {str(e)}")
            logger.error(traceback.format_exc())
            return False
    
    def close_position(self, symbol):
        """
        Close an existing position for a symbol.
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            Boolean indicating success/failure
        """
        if not self.positions[symbol]['active']:
            logger.warning(f"No active position for {symbol}")
            return False
            
        try:
            qty = self.positions[symbol]['qty']
            side = self.positions[symbol]['side']
            
            # Determine close order side (opposite of position side)
            close_side = "SELL" if side == "LONG" else "BUY"
            
            logger.info(f"Closing {side} position for {qty} {symbol} with {close_side} order")
            
            close_result = self._place_futures_order(
                symbol=symbol,
                side=close_side,
                quantity=qty,
                order_type="MARKET"
            )
            
            # Update position tracking
            self.positions[symbol]['active'] = False
            self.positions[symbol]['exit_time'] = datetime.now()
            
            logger.info(f"Successfully closed {side} position for {symbol}")
            return True
            
        except Exception as e:
            logger.error(f"Error closing position for {symbol}: {str(e)}")
            logger.error(traceback.format_exc())
            return False
    
    def run(self):
        """
        Main loop for the funding arbitrage strategy.
        """
        logger.info("Starting funding arbitrage strategy")
        
        try:
            while True:
                # Clear the terminal for better display
                os.system('cls' if os.name == 'nt' else 'clear')
                
                # Calculate metrics for all symbols
                self._update_metrics()
                
                # Display metrics
                self._display_metrics()
                
                # Count current active positions
                active_positions = sum(1 for symbol in self.symbols if symbol in self.positions and self.positions[symbol]['active'])
                available_slots = self.max_positions - active_positions
                
                # Check existing positions first
                for symbol in self.symbols:
                    # Make sure the symbol exists in our positions dictionary
                    if symbol not in self.positions:
                        self.positions[symbol] = {'active': False, 'side': None, 'qty': 0}
                        
                    if self.positions[symbol]['active']:
                        try:
                            # Get latest metrics
                            metrics = self.metrics.get(symbol)
                            
                            if not metrics:
                                continue
                            
                            current_side = self.positions[symbol]['side']
                            optimal_side = metrics['position_side']
                            
                            # Close position if side should change or funding is no longer favorable
                            if current_side != optimal_side or abs(metrics['funding_rate']) < self.min_funding_threshold/2:
                                logger.info(f"Closing {current_side} position for {symbol}")
                                self.close_position(symbol)
                        except Exception as e:
                            logger.error(f"Error processing active position for {symbol}: {str(e)}")
                
                # Open new positions for top opportunities if we have slots available
                if available_slots > 0 and self.ranked_symbols:
                    # Find top opportunities that we don't already have positions in
                    new_opportunities = [item for item in self.ranked_symbols 
                                        if not self.positions[item['symbol']]['active']]
                    
                    # Take positions in top N opportunities
                    for i, opportunity in enumerate(new_opportunities):
                        if i >= available_slots:
                            break
                            
                        symbol = opportunity['symbol']
                        metrics = opportunity['metrics']
                        
                        try:
                            logger.info(f"Opening position for top opportunity: {symbol}")
                            self.execute_arbitrage(symbol, metrics)
                        except Exception as e:
                            logger.error(f"Error opening position for {symbol}: {str(e)}")
                
                # Sleep before next check
                time.sleep(self.check_interval)
                
        except KeyboardInterrupt:
            logger.info("Strategy interrupted by user")
        finally:
            # Close any open positions
            for symbol in self.symbols:
                if self.positions[symbol]['active']:
                    logger.info(f"Closing position for {symbol} due to shutdown")
                    self.close_position(symbol)
            
            # Close WebSocket connection
            self.ws_client.close()
            logger.info("Strategy shutdown complete")
    
    def _update_metrics(self):
        """Calculate and update metrics for all symbols"""
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
        
        # Rank symbols by APR
        self._rank_symbols()
    
    def _rank_symbols(self):
        """Rank symbols by profitability for easier decision making"""
        ranked = []
        
        for symbol, metrics in self.metrics.items():
            # Only include profitable opportunities in ranking
            if metrics['is_profitable']:
                ranked.append({
                    'symbol': symbol,
                    'funding_rate': metrics['funding_rate'],
                    'apr': metrics['apr'],
                    'break_even_events': metrics['break_even_events'],
                    'position_side': metrics['position_side'],
                    'metrics': metrics
                })
        
        # Sort by APR (highest to lowest)
        self.ranked_symbols = sorted(ranked, key=itemgetter('apr'), reverse=True)
    
    def _display_metrics(self):
        """Display metrics in the terminal"""
        print(f"\n{'=' * 100}")
        print(f" FUNDING ARBITRAGE MONITOR - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'=' * 100}")
        
        # Show active positions summary
        active_count = sum(1 for symbol in self.symbols if symbol in self.positions and self.positions[symbol]['active'])
        print(f"\nActive Positions: {active_count}/{self.max_positions}")
        
        # Best opportunities table (top 15)
        if self.ranked_symbols:
            print("\nBEST FUNDING OPPORTUNITIES:")
            
            # Prepare data for tabulate
            best_data = []
            for i, item in enumerate(self.ranked_symbols[:15]):
                symbol = item['symbol']
                m = item['metrics']
                
                # Show if we have an active position
                position_info = ""
                if self.positions[symbol]['active']:
                    position_info = f"[ACTIVE {self.positions[symbol]['side']}]"
                
                # Highlight by prefixing top opportunities
                prefix = "→ " if i < self.max_positions and not self.positions[symbol]['active'] else "  "
                
                best_data.append([
                    prefix + symbol,
                    f"{m['funding_rate']*100:.5f}%",
                    m['position_side'],
                    m['next_funding_time'],
                    f"{m['time_to_funding_hours']:.2f} hrs",
                    f"${m['expected_profit_per_funding']:.4f}",
                    f"{m['break_even_events']} events",
                    f"{m['apr']*100:.2f}%", 
                    position_info
                ])
            
            print(tabulate(best_data, headers=["Symbol", "Funding Rate", "Side", 
                                          "Next Funding", "Time to Funding", "Profit/Funding", 
                                          "Break Even", "APR", "Position"]))
        else:
            print("\nNo profitable opportunities found at current thresholds.")
        
        # Active positions table
        active_data = []
        for symbol in self.symbols:
            if self.positions[symbol]['active']:
                p = self.positions[symbol]
                m = self.metrics[symbol]
                
                # Calculate position metrics
                entry_time = p['entry_time'].strftime('%Y-%m-%d %H:%M:%S')
                duration = (datetime.now() - p['entry_time']).total_seconds() / 3600  # hours
                
                # Add position info
                active_data.append([
                    symbol,
                    p['side'],
                    f"{p['qty']:.5f}",
                    f"${m['notional_value']:.2f}",
                    entry_time,
                    f"{duration:.2f} hrs",
                    f"${m['expected_profit_per_funding']:.4f}",
                    f"{m['apr']*100:.2f}%"
                ])
        
        if active_data:
            print("\n\nACTIVE POSITIONS:")
            print(tabulate(active_data, headers=["Symbol", "Side", "Quantity", "Notional Value", 
                                            "Entry Time", "Duration", "Profit/Funding", "APR"]))
        
        print(f"\n{'=' * 100}")
        print(f"Monitoring {len(self.metrics)} symbols - {len(self.ranked_symbols)} profitable opportunities")
        print(f"{'=' * 100}")
    
    def _place_futures_order(self, symbol, side, quantity, order_type):
        """
        Place an order on the futures market (placeholder).
        In a real implementation, this would use the Binance API.
        """
        logger.info(f"[MOCK] Futures {side} order for {quantity} {symbol}")
        # In a real implementation, would call the Binance API here
        return {"orderId": f"mock-futures-order-id-{side.lower()}"}
    
    def _round_down(self, value, decimals):
        """Round down value to specified decimal places"""
        factor = 10 ** decimals
        return math.floor(value * factor) / factor


# Configuration file for the strategy
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
            "symbols": ["btcusdt", "ethusdt"],
            "position_size_usd": 1000,
            "min_funding_threshold": 0.0001,  # 0.01% funding rate
            "futures_fee_rate": 0.0004,  # 0.04% 
            "slippage": 0.0005,  # 0.05%
            "check_interval": 60,  # Check every minute
            "use_all_symbols": True,
            "risk_management": {
                "max_positions": 5
            },
            "symbol_filters": {
                "min_price": 0,
                "min_volume_usd": 0,
                "exclude": [],
                "include_only": []
            }
        }


if __name__ == "__main__":
    # Load configuration
    config = load_config("config.json")
    
    # Create and run the strategy
    strategy = FundingArbitrageStrategy(config)
    strategy.run()
