import time
import logging
import threading
from typing import Dict, List, Tuple, Set, Optional
import pandas as pd
from collections import defaultdict

# Import exchange clients
from exchanges.binance.ws_client import BinanceWebSocketClient
from exchanges.bybit.ws_client import BybitWebSocketClient
from exchanges.okx.ws_client import OkxWebSocketClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("TriangularArbitrage")

class TriangularArbitrage:
    """
    Triangular arbitrage strategy that monitors price differences across
    trading pairs to find arbitrage opportunities within a single exchange.
    """
    
    def __init__(
        self, 
        exchange: str = "binance",
        base_currencies: List[str] = None,
        quote_currencies: List[str] = None,
        min_profit_threshold: float = 0.001,  # Min 0.1% profit after fees
        update_interval: float = 1.0,
        max_trade_size: Dict[str, float] = None,
        fee_rate: float = 0.001,  # Default fee rate (0.1%)
        slippage: float = 0.0005,  # Default slippage estimate (0.05%)
        testnet: bool = True
    ):
        """
        Initialize triangular arbitrage strategy.
        
        Args:
            exchange: Exchange to monitor ("binance", "bybit", or "okx")
            base_currencies: List of base currencies to use (e.g., ["BTC", "ETH"])
            quote_currencies: List of quote currencies (e.g., ["USDT", "BUSD"])
            min_profit_threshold: Minimum profit threshold after fees to consider arbitrage
            update_interval: How often to check for arbitrage opportunities (seconds)
            max_trade_size: Maximum trade size per currency (Dict[currency, amount])
            fee_rate: Trading fee rate (as a decimal, e.g., 0.001 for 0.1%)
            slippage: Expected slippage rate (as a decimal)
            testnet: Whether to use testnet/sandbox environment
        """
        self.exchange_name = exchange.lower()
        self.base_currencies = base_currencies or ["BTC", "ETH", "SOL"]
        self.quote_currencies = quote_currencies or ["USDT", "BUSD", "USDC"]
        self.min_profit_threshold = min_profit_threshold
        self.update_interval = update_interval
        self.max_trade_size = max_trade_size or {"BTC": 0.01, "ETH": 0.1, "SOL": 1, "USDT": 1000}
        self.fee_rate = fee_rate
        self.slippage = slippage
        self.testnet = testnet
        
        # Exchange client
        self.client = self._initialize_client()
        self.is_running = False
        self.monitor_thread = None
        
        # Store trading pairs and orderbook data
        self.pairs = set()
        self.tradable_paths = []
        self.orderbook_data = {}
        
        # Opportunities tracking
        self.opportunities = []
        self.opportunity_lock = threading.Lock()
        
    def _initialize_client(self):
        """Initialize exchange client based on selected exchange"""
        if self.exchange_name == "binance":
            # For Binance, we need to create symbol list first
            self.pairs = self._generate_potential_pairs()
            return BinanceWebSocketClient([p.lower() for p in self.pairs])
        
        elif self.exchange_name == "bybit":
            return BybitWebSocketClient(channel_type="linear", testnet=self.testnet)
            
        elif self.exchange_name == "okx":
            return OkxWebSocketClient(testnet=self.testnet)
            
        else:
            raise ValueError(f"Unsupported exchange: {self.exchange_name}")
    
    def _generate_potential_pairs(self) -> Set[str]:
        """Generate potential trading pairs from base and quote currencies"""
        pairs = set()
        
        # Generate all combinations of base and quote currencies
        for base in self.base_currencies:
            for quote in self.quote_currencies:
                if base != quote:  # Avoid pairs like BTC/BTC
                    # Format based on exchange conventions
                    if self.exchange_name in ["binance", "bybit"]:
                        pairs.add(f"{base}{quote}")
                    elif self.exchange_name == "okx":
                        pairs.add(f"{base}-{quote}")
                        
        # Also add base-to-base pairs (e.g. BTC/ETH)
        for i, base1 in enumerate(self.base_currencies):
            for base2 in self.base_currencies[i+1:]:
                if self.exchange_name in ["binance", "bybit"]:
                    pairs.add(f"{base1}{base2}")
                    pairs.add(f"{base2}{base1}")
                elif self.exchange_name == "okx":
                    pairs.add(f"{base1}-{base2}")
                    pairs.add(f"{base2}-{base1}")
        
        return pairs
    
    def _okx_symbol_conversion(self, symbol: str, to_standard: bool = True) -> str:
        """Convert between OKX symbol format and standard format"""
        if to_standard:
            # Convert OKX format (BTC-USDT) to standard (BTCUSDT)
            return symbol.replace('-', '')
        else:
            # Convert standard (BTCUSDT) to OKX format (BTC-USDT)
            # Find the index where quote currency starts
            for quote in self.quote_currencies:
                if symbol.endswith(quote):
                    return f"{symbol[:-len(quote)]}-{quote}"
            
            # If not found, try to split between base currencies
            for base in self.base_currencies:
                if symbol.endswith(base):
                    return f"{symbol[:-len(base)]}-{base}"
                    
            # Default fallback
            return symbol
    
    def _handle_orderbook_data(self, message):
        """Process orderbook data from exchange websocket"""
        try:
            symbol = ""
            best_bid = 0
            best_ask = 0
            bid_qty = 0
            ask_qty = 0
            
            # Parse data based on exchange format
            if self.exchange_name == "binance":
                # Binance data comes as {data: {s: symbol, b: bid, a: ask, ...}}
                if 'data' in message:
                    data = message['data']
                    symbol = data.get('s', '')
                    best_bid = float(data.get('b', 0))
                    best_ask = float(data.get('a', 0))
                    bid_qty = float(data.get('B', 0))
                    ask_qty = float(data.get('A', 0))
                    
            elif self.exchange_name == "bybit":
                # Bybit orderbook format
                if message.get('type') in ['snapshot', 'delta'] and 'data' in message:
                    symbol = message['data']['s']
                    bids = message['data']['b']  # bids array
                    asks = message['data']['a']  # asks array
                    
                    if bids and asks:
                        best_bid = float(bids[0][0])
                        best_ask = float(asks[0][0])
                        bid_qty = float(bids[0][1])
                        ask_qty = float(asks[0][1])
            
            elif self.exchange_name == "okx":
                # OKX orderbook format
                arg = message.get("arg", {})
                inst_id = arg.get("instId", "")
                
                if message.get("action") in ["snapshot", "update"] and message.get("data"):
                    data = message.get("data")[0]
                    
                    if data.get("bids") and data.get("asks"):
                        symbol = self._okx_symbol_conversion(inst_id, to_standard=True)
                        best_bid = float(data["bids"][0][0])
                        best_ask = float(data["asks"][0][0])
                        bid_qty = float(data["bids"][0][1])
                        ask_qty = float(data["asks"][0][1])
            
            # Store orderbook data if valid
            if symbol and best_bid > 0 and best_ask > 0:
                self.orderbook_data[symbol] = {
                    'symbol': symbol,
                    'bid': best_bid,
                    'ask': best_ask,
                    'bid_qty': bid_qty,
                    'ask_qty': ask_qty,
                    'timestamp': time.time()
                }
                
        except Exception as e:
            logger.error(f"Error processing orderbook data: {e}")
    
    def find_triangular_paths(self) -> List[List[str]]:
        """
        Find all possible triangular paths from available pairs.
        Each path is a list of 3 trading pairs forming a cycle.
        """
        # Get all available symbols
        available_symbols = list(self.orderbook_data.keys())
        
        # Create an adjacency list to represent the available trading pairs
        graph = defaultdict(list)
        for symbol in available_symbols:
            # Extract base and quote currencies based on common patterns
            for quote in self.quote_currencies:
                if symbol.endswith(quote):
                    base = symbol[:-len(quote)]
                    graph[base].append((quote, symbol, "quote"))  # Direction: base -> quote
                    graph[quote].append((base, symbol, "base"))   # Direction: quote -> base
        
        # Find valid triangular paths
        paths = []
        for start_currency in set(self.base_currencies + self.quote_currencies):
            # Try every currency as a starting point
            self._find_paths_dfs(graph, start_currency, start_currency, [], [], paths, max_depth=3)
        
        # Update instance variable and return
        self.tradable_paths = paths
        return paths
    
    def _find_paths_dfs(self, graph, current, start, path, path_symbols, result, max_depth=3):
        """
        DFS helper to find valid triangular paths.
        
        Args:
            graph: Adjacency list representation of trading pairs
            current: Current currency node
            start: Starting currency node (to detect cycles)
            path: Current path of currencies
            path_symbols: Current path of trading pairs
            result: List to store valid paths
            max_depth: Maximum path length (3 for triangular)
        """
        path.append(current)
        
        # If we've found a cycle and the path is the right length
        if len(path) > 1 and current == start and len(path) <= max_depth + 1:
            if len(path) == max_depth + 1:  # +1 because we repeat the start at the end
                result.append(path_symbols.copy())
            return
            
        # If path is too long, stop exploring
        if len(path) > max_depth:
            path.pop()
            return
            
        # Continue DFS exploration
        for neighbor, symbol, direction in graph[current]:
            if len(path_symbols) < max_depth:  # Only add new paths if we're under max depth
                # Add the trading symbol to our path
                path_symbols.append((symbol, direction))
                self._find_paths_dfs(graph, neighbor, start, path, path_symbols, result, max_depth)
                path_symbols.pop()  # Backtrack
        
        path.pop()  # Backtrack
    
    def calculate_triangular_arbitrage(self):
        """
        Calculate triangular arbitrage opportunities from all possible paths.
        Updates self.opportunities with found opportunities.
        """
        opportunities = []
        
        # If no paths are found yet, attempt to discover them
        if not self.tradable_paths:
            self.find_triangular_paths()
            
        if not self.tradable_paths:
            logger.warning("No valid triangular paths found. Check available trading pairs.")
            return []
        
        # Check each path for arbitrage opportunity
        for path in self.tradable_paths:
            try:
                # Extract the three trading pairs and directions
                symbols = [p[0] for p in path]
                directions = [p[1] for p in path]
                
                # Ensure we have price data for all symbols in path
                if not all(symbol in self.orderbook_data for symbol in symbols):
                    continue
                
                # Get price data for each leg
                rates = []
                for i, (symbol, direction) in enumerate(path):
                    ticker = self.orderbook_data[symbol]
                    
                    if direction == "quote":  # We're selling the base currency
                        rate = ticker['bid']  # Use bid when selling
                    else:  # We're buying the base currency
                        rate = 1.0 / ticker['ask']  # Reciprocal of ask when buying
                        
                    rates.append(rate)
                
                # Calculate the product of exchange rates
                cross_rate = 1.0
                for rate in rates:
                    cross_rate *= rate
                
                # Calculate expected profit ratio
                profit_ratio = cross_rate - 1.0
                
                # Adjust for trading fees and slippage
                adjusted_profit_ratio = profit_ratio - (self.fee_rate * 3) - (self.slippage * 3)
                
                # If profitable after fees and slippage
                if adjusted_profit_ratio > self.min_profit_threshold:
                    opportunity = {
                        'path': symbols,
                        'directions': directions,
                        'rates': rates,
                        'gross_profit_ratio': profit_ratio,
                        'net_profit_ratio': adjusted_profit_ratio,
                        'fees': self.fee_rate * 3,
                        'timestamp': time.time(),
                        'exchange': self.exchange_name
                    }
                    
                    opportunities.append(opportunity)
                    logger.info(f"Found arbitrage opportunity: {symbols}, profit: {adjusted_profit_ratio:.6f}")
                    
            except Exception as e:
                logger.error(f"Error calculating arbitrage for path {path}: {e}")
        
        # Update opportunities with thread safety
        with self.opportunity_lock:
            self.opportunities = opportunities
            
        return opportunities
    
    def _monitor_arbitrage(self):
        """Background thread to continuously monitor for arbitrage opportunities"""
        while self.is_running:
            try:
                # Calculate triangular arbitrage
                self.calculate_triangular_arbitrage()
                
                # Sleep before next check
                time.sleep(self.update_interval)
                
            except Exception as e:
                logger.error(f"Error in arbitrage monitor: {e}")
                time.sleep(1)  # Avoid tight loop on error
    
    def start(self):
        """Start the triangular arbitrage strategy"""
        logger.info(f"Starting triangular arbitrage strategy on {self.exchange_name}")
        
        # Connect to exchange
        self.client.connect()
        
        # Wait for connection
        time.sleep(2)
        
        # Subscribe to orderbook data for all symbols
        self._subscribe_to_orderbooks()
        
        # Wait for initial data
        logger.info("Waiting for initial orderbook data...")
        time.sleep(5)
        
        # Find triangular paths
        paths = self.find_triangular_paths()
        logger.info(f"Found {len(paths)} potential triangular paths")
        
        # Start monitoring thread
        self.is_running = True
        self.monitor_thread = threading.Thread(target=self._monitor_arbitrage)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        
        logger.info("Triangular arbitrage strategy is running")
    
    def _subscribe_to_orderbooks(self):
        """Subscribe to orderbook data for relevant pairs"""
        # Generate pairs if not already done
        if not self.pairs:
            self.pairs = self._generate_potential_pairs()
            
        logger.info(f"Subscribing to {len(self.pairs)} trading pairs on {self.exchange_name}")
        
        try:
            if self.exchange_name == "binance":
                # Binance subscription is already handled in the client initialization
                pass
                
            elif self.exchange_name == "bybit":
                for symbol in self.pairs:
                    self.client.subscribe_orderbook(symbol, depth=1, callback=self._handle_orderbook_data)
                    
            elif self.exchange_name == "okx":
                for symbol in self.pairs:
                    # Convert to OKX format if needed
                    okx_symbol = symbol if '-' in symbol else self._okx_symbol_conversion(symbol, to_standard=False)
                    self.client.subscribe_orderbook(okx_symbol, depth="books5", callback=self._handle_orderbook_data)
                    
        except Exception as e:
            logger.error(f"Error subscribing to orderbooks: {e}")
    
    def get_opportunities(self):
        """Get current arbitrage opportunities"""
        with self.opportunity_lock:
            return self.opportunities.copy()
    
    def get_stats(self):
        """Get statistics about the strategy"""
        stats = {
            'exchange': self.exchange_name,
            'pairs_monitored': len(self.pairs),
            'paths_found': len(self.tradable_paths),
            'opportunity_count': len(self.opportunities),
            'running': self.is_running,
            'last_update': time.strftime('%H:%M:%S')
        }
        return stats
    
    def stop(self):
        """Stop the triangular arbitrage strategy"""
        logger.info(f"Stopping triangular arbitrage strategy on {self.exchange_name}")
        self.is_running = False
        
        if self.client:
            self.client.close()
            
        logger.info("Triangular arbitrage strategy stopped")


# Example usage
if __name__ == "__main__":
    # Create the strategy
    strategy = TriangularArbitrage(
        exchange="binance",
        base_currencies=["BTC", "ETH", "SOL"],
        quote_currencies=["USDT", "BUSD"],
        min_profit_threshold=0.002,  # 0.2% minimum profit
        testnet=True
    )
    
    # Start the strategy
    strategy.start()
    
    try:
        # Display opportunities periodically
        while True:
            opportunities = strategy.get_opportunities()
            if opportunities:
                print("\n=== Current Arbitrage Opportunities ===")
                df = pd.DataFrame(opportunities)
                print(df[['path', 'net_profit_ratio', 'timestamp']].to_string())
            else:
                print("No arbitrage opportunities found yet")
                
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\nStopping arbitrage strategy...")
        strategy.stop()
        print("Strategy stopped")
