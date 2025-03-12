import threading
import pandas as pd
import time
import logging
import os
import sys
from IPython.display import display, clear_output
from exchanges.binance.ws_client import BinanceWebSocketClient
from exchanges.bybit.ws_client import BybitWebSocketClient
from exchanges.okx.ws_client import OkxWebSocketClient
from terminal_display import (format_exchange_comparison_table, 
                             print_comparison_header, print_arbitrage_alerts,
                             is_color_supported)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("MultiExchangeComparison")

class MultiExchangeComparison:
    def __init__(self, symbols, update_interval=1.0):
        """
        Initialize the multi-exchange comparison tool.
        
        Args:
            symbols: List of symbols to monitor (e.g., ["BTCUSDT", "ETHUSDT"])
            update_interval: How often to update the display in seconds
        """
        self.symbols = [s.upper() for s in symbols]
        self.update_interval = update_interval
        
        # Initialize client connections
        self.binance_client = BinanceWebSocketClient([s.lower() for s in symbols])
        self.bybit_client = BybitWebSocketClient(channel_type="linear", testnet=False)
        self.okx_client = OkxWebSocketClient(testnet=False, ping_interval=20, reconnect_interval=5)
        
        # Display thread
        self.display_thread = None
        self.running = True
        
    def _to_okx_symbol(self, symbol):
        """Convert other exchange symbol format to OKX format"""
        # OKX uses BTC-USDT format instead of BTCUSDT
        if symbol.endswith('USDT'):
            return f"{symbol[:-4]}-USDT"
        elif symbol.endswith('USD'):
            return f"{symbol[:-3]}-USD"
        else:
            return symbol
    
    def _from_okx_symbol(self, symbol):
        """Convert from OKX symbol format to standard format"""
        # Convert BTC-USDT to BTCUSDT
        return symbol.replace('-', '')
        
    def handle_bybit_orderbook(self, message):
        """Process orderbook data from Bybit"""
        try:
            if message.get('type') in ['snapshot', 'delta']:
                symbol = message['data']['s']
                
                # Extract best bid and ask
                bids = message['data']['b']  # bids array
                asks = message['data']['a']  # asks array
                
                if bids and asks:
                    best_bid = float(bids[0][0])
                    best_ask = float(asks[0][0])
                    bid_qty = float(bids[0][1])
                    ask_qty = float(asks[0][1])
                    
                    # Check if data attribute exists, initialize if needed
                    if not hasattr(self.bybit_client, 'data'):
                        self.bybit_client.data = {}
                    
                    # Store data directly in the client for consistent access
                    self.bybit_client.data[symbol] = {
                        'symbol': symbol,
                        'bid': best_bid,
                        'ask': best_ask,
                        'bid_qty': bid_qty,
                        'ask_qty': ask_qty,
                        'timestamp': time.time()
                    }
        except Exception as e:
            logger.error(f"Error processing Bybit data: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def handle_okx_orderbook(self, message):
        """Process orderbook data from OKX"""
        try:
            arg = message.get("arg", {})
            inst_id = arg.get("instId", "")
            action = message.get("action", "")
            
            if action in ["snapshot", "update"] and message.get("data"):
                data = message.get("data")[0]
                
                # Extract best bid and ask
                bids = data.get("bids", [])
                asks = data.get("asks", [])
                
                if bids and asks:
                    best_bid = float(bids[0][0])
                    best_ask = float(asks[0][0])
                    bid_qty = float(bids[0][1])
                    ask_qty = float(asks[0][1])
                    
                    # Store data in standard format
                    std_symbol = self._from_okx_symbol(inst_id)
                    
                    # Store data 
                    if not hasattr(self.okx_client, 'data'):
                        self.okx_client.data = {}
                    
                    self.okx_client.data[std_symbol] = {
                        'symbol': std_symbol,
                        'bid': best_bid,
                        'ask': best_ask,
                        'bid_qty': bid_qty,
                        'ask_qty': ask_qty,
                        'timestamp': time.time()
                    }
        except Exception as e:
            logger.error(f"Error processing OKX data: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def start(self):
        """Start monitoring all exchanges"""
        logger.info("Starting multi-exchange comparison tool...")
        
        # Connect to all exchanges
        self.binance_client.connect()
        
        self.bybit_client.connect()
        for symbol in self.symbols:
            self.bybit_client.subscribe_orderbook(
                symbol=symbol, 
                depth=1,
                callback=self.handle_bybit_orderbook
            )
        
        self.okx_client.connect()
        for symbol in self.symbols:
            okx_symbol = self._to_okx_symbol(symbol)
            self.okx_client.subscribe_orderbook(
                symbol=okx_symbol,
                depth="books5",
                callback=self.handle_okx_orderbook  # Add the callback handler
            )
        
        # Wait for connections and initial data
        logger.info("Waiting for initial data...")
        time.sleep(5)
        
        # Start display thread
        self.display_thread = threading.Thread(target=self.update_display)
        self.display_thread.daemon = True
        self.display_thread.start()
        
        logger.info("Multi-exchange comparison tool started")
    
    def update_display(self):
        """Update the comparison display"""
        while self.running:
            try:
                # Get data from all exchanges
                binance_data = self.binance_client.get_data()
                bybit_data = self.bybit_client.get_data()
                
                # Get OKX data and convert symbols to standard format
                okx_raw_data = self.okx_client.get_data()
                okx_data = {}
                for okx_symbol, data in okx_raw_data.items():
                    std_symbol = self._from_okx_symbol(okx_symbol)
                    okx_data[std_symbol] = data
                
                # Create comparison data
                comparison_data = []
                
                for symbol in self.symbols:
                    # Get data for each exchange if available
                    binance_ticker = binance_data.get(symbol, {})
                    bybit_ticker = bybit_data.get(symbol, {})
                    okx_ticker = okx_data.get(symbol, {})
                    
                    if not any([binance_ticker, bybit_ticker, okx_ticker]):
                        continue
                    
                    # Calculate midpoints for each exchange where data is available
                    exchanges = {}
                    if binance_ticker:
                        exchanges["Binance"] = {
                            "bid": binance_ticker.get("bid", 0),
                            "ask": binance_ticker.get("ask", 0),
                            "mid": (binance_ticker.get("bid", 0) + binance_ticker.get("ask", 0)) / 2
                        }
                    
                    if bybit_ticker:
                        exchanges["Bybit"] = {
                            "bid": bybit_ticker.get("bid", 0),
                            "ask": bybit_ticker.get("ask", 0),
                            "mid": (bybit_ticker.get("bid", 0) + bybit_ticker.get("ask", 0)) / 2
                        }
                    
                    if okx_ticker:
                        exchanges["OKX"] = {
                            "bid": okx_ticker.get("bid", 0),
                            "ask": okx_ticker.get("ask", 0),
                            "mid": (okx_ticker.get("bid", 0) + okx_ticker.get("ask", 0)) / 2
                        }
                    
                    # Find best arbitrage opportunity
                    best_arb = self._find_best_arbitrage(exchanges)
                    
                    # Create comparison row
                    row = {
                        'Symbol': symbol,
                        'Arbitrage': 'Yes' if best_arb['profit_bps'] > 0 else 'No',
                        'Profit (bps)': round(best_arb['profit_bps'], 2),
                        'Direction': best_arb['direction']
                    }
                    
                    # Add exchange data
                    for exchange_name in ["Binance", "Bybit", "OKX"]:
                        if exchange_name in exchanges:
                            row[f'{exchange_name} Bid'] = exchanges[exchange_name]['bid']
                            row[f'{exchange_name} Ask'] = exchanges[exchange_name]['ask']
                            
                    comparison_data.append(row)
                
                # Create and display DataFrame
                if comparison_data:
                    # Detect environment
                    in_notebook = False
                    try:
                        get_ipython
                        in_notebook = True
                    except NameError:
                        in_notebook = False
                    
                    # Clear screen based on environment
                    if in_notebook:
                        clear_output(wait=True)
                    else:
                        os.system('cls' if os.name == 'nt' else 'clear')
                    
                    # Create DataFrame
                    df = pd.DataFrame(comparison_data)
                    
                    # Define column order
                    columns = ['Symbol']
                    for exchange in ["Binance", "Bybit", "OKX"]:
                        columns.extend([f'{exchange} Bid', f'{exchange} Ask'])
                    columns.extend(['Arbitrage', 'Direction', 'Profit (bps)'])
                    
                    # Reorder and filter columns that exist
                    existing_columns = [col for col in columns if col in df.columns]
                    df = df[existing_columns]
                    
                    if in_notebook:
                        # Format for notebook
                        self._display_notebook_table(df)
                    else:
                        # Format for terminal
                        print_comparison_header(time.strftime('%H:%M:%S'))
                        print_arbitrage_alerts(comparison_data)
                        print(format_exchange_comparison_table(df))
                else:
                    print("Waiting for data from exchanges...")
                    
            except Exception as e:
                logger.error(f"Error updating display: {e}")
                import traceback
                traceback.print_exc()
            
            time.sleep(self.update_interval)
    
    def _find_best_arbitrage(self, exchanges):
        """
        Find the best arbitrage opportunity among the exchanges.
        
        Args:
            exchanges: Dict of exchange data with bid/ask prices
            
        Returns:
            Dict containing best arbitrage details
        """
        best_arb = {
            'buy_exchange': None,
            'sell_exchange': None, 
            'profit': 0,
            'profit_bps': 0,
            'direction': '-'
        }
        
        # Check all exchange pairs for arbitrage
        exchange_names = list(exchanges.keys())
        
        for i, ex1 in enumerate(exchange_names):
            for j, ex2 in enumerate(exchange_names):
                if i == j:  # Skip same exchange
                    continue
                    
                # Check if we can buy on ex1 and sell on ex2
                if exchanges[ex1]['ask'] > 0 and exchanges[ex2]['bid'] > 0:
                    profit = exchanges[ex2]['bid'] - exchanges[ex1]['ask']
                    profit_bps = (profit / exchanges[ex1]['ask']) * 10000
                    
                    if profit_bps > best_arb['profit_bps']:
                        best_arb = {
                            'buy_exchange': ex1,
                            'sell_exchange': ex2,
                            'profit': profit,
                            'profit_bps': profit_bps,
                            'direction': f"Buy {ex1}, Sell {ex2}"
                        }
        
        return best_arb
    
    def _display_notebook_table(self, df):
        """Format and display table for Jupyter notebook"""
        # Format the table with styling
        format_dict = {}
        
        # Apply currency formatting to bid/ask columns
        for col in df.columns:
            if 'Bid' in col or 'Ask' in col:
                format_dict[col] = "${:.2f}"
                
        # Format profit column
        if 'Profit (bps)' in df.columns:
            format_dict['Profit (bps)'] = "{:.2f}"
            
        styled_df = df.style.format(format_dict)
        
        # Add styling for arbitrage opportunities
        if 'Arbitrage' in df.columns:
            styled_df = styled_df.map(
                lambda x: 'background-color: #c6efce' if x == 'Yes' else '',
                subset=['Arbitrage']
            )
        
        # Add styling for direction
        if 'Direction' in df.columns:
            styled_df = styled_df.map(
                lambda x: 'background-color: #c6efce' if x != '-' else '',
                subset=['Direction']
            )
        
        # Add styling for profit
        if 'Profit (bps)' in df.columns:
            styled_df = styled_df.map(
                lambda x: 'color: green; font-weight: bold' if x > 0 else '',
                subset=['Profit (bps)']
            )
        
        # Set table styles
        styled_df = styled_df.set_table_styles([
            {'selector': 'th', 'props': [('background-color', '#4472C4'), 
                                      ('color', 'white'),
                                      ('font-weight', 'bold'),
                                      ('border', '1px solid #4472C4')]},
            {'selector': 'td', 'props': [('border', '1px solid #ddd')]},
            {'selector': 'tr:nth-child(even)', 'props': [('background-color', '#f2f2f2')]},
            {'selector': 'tr:hover', 'props': [('background-color', '#e6f7ff')]},
        ])
        
        # Set caption
        styled_df = styled_df.set_caption(f"Multi-Exchange Comparison - {time.strftime('%H:%M:%S')}")
        
        # Print header
        print(f"Multi-Exchange Comparison ({time.strftime('%H:%M:%S')})")
        
        # Display the styled DataFrame
        display(styled_df)
    
    def stop(self):
        """Stop all connections and threads"""
        self.running = False
        
        # Close all client connections
        if self.binance_client:
            self.binance_client.close()
        
        if self.bybit_client:
            self.bybit_client.close()
            
        if self.okx_client:
            self.okx_client.close()
            
        logger.info("Multi-exchange comparison stopped")


# Example usage
if __name__ == "__main__":
    # List of symbols to compare
    symbols_to_compare = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    
    # Create and start the comparison tool
    comparison = MultiExchangeComparison(symbols_to_compare)
    comparison.start()
    
    try:
        # Keep the main thread running
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping multi-exchange comparison...")
        comparison.stop()
        print("Comparison stopped")
