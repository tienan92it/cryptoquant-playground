import threading
import pandas as pd
import time
from IPython.display import display, clear_output
import logging
import os
import sys
from exchanges.binance.ws_client import BinanceWebSocketClient
from exchanges.bybit.ws_client import BybitWebSocketClient
from terminal_display import (format_exchange_comparison_table, 
                             print_comparison_header, print_arbitrage_alerts,
                             is_color_supported)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ExchangeComparison")

class ExchangeComparison:
    def __init__(self, symbols, update_interval=1.0):
        """
        Initialize the exchange comparison tool.
        
        Args:
            symbols: List of symbols to monitor (e.g., ["BTCUSDT", "ETHUSDT"])
            update_interval: How often to update the display in seconds
        """
        self.symbols = [s.upper() for s in symbols]
        self.update_interval = update_interval
        self.binance_data = {}
        self.bybit_data = {}
        
        # Initialize clients
        self.binance_client = BinanceWebSocketClient([s.lower() for s in symbols])
        
        # Bybit client configured for linear contracts (USDT pairs)
        self.bybit_client = BybitWebSocketClient(channel_type="linear", testnet=False)
        
        # Display thread
        self.display_thread = None
        self.running = True
        
    def start(self):
        """Start monitoring both exchanges"""
        logger.info("Starting exchange comparison tool...")
        
        # Connect to Binance
        self.binance_client.connect()
        
        # Connect to Bybit and set up callbacks
        self.bybit_client.connect()
        for symbol in self.symbols:
            # For Bybit, we need to subscribe to the orderbook
            self.bybit_client.subscribe_orderbook(
                symbol=symbol, 
                depth=1,  # Just need top of book
                callback=self.handle_bybit_orderbook
            )
        
        # Wait for connections and initial data
        logger.info("Waiting for initial data...")
        time.sleep(3)
        
        # Start display thread
        self.display_thread = threading.Thread(target=self.update_display)
        self.display_thread.daemon = True
        self.display_thread.start()
        
        logger.info("Exchange comparison tool started")
        
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
                    
                    self.bybit_data[symbol] = {
                        'bid': best_bid,
                        'ask': best_ask,
                        'bid_qty': bid_qty,
                        'ask_qty': ask_qty,
                        'timestamp': time.time()
                    }
        except Exception as e:
            logger.error(f"Error processing Bybit data: {e}")
    
    def update_display(self):
        """Update the comparison display"""
        while self.running:
            try:
                # Get data from both exchanges
                binance_data = self.binance_client.get_data()
                
                # Create comparison data
                comparison_data = []
                
                for symbol in self.symbols:
                    binance_ticker = binance_data.get(symbol, {})
                    bybit_ticker = self.bybit_data.get(symbol, {})
                    
                    if binance_ticker and bybit_ticker:
                        # Calculate price differences
                        binance_mid = (binance_ticker['bid'] + binance_ticker['ask']) / 2
                        bybit_mid = (bybit_ticker['bid'] + bybit_ticker['ask']) / 2
                        
                        # Calculate arbitrage opportunities
                        bybit_binance_spread = bybit_ticker['bid'] - binance_ticker['ask']
                        binance_bybit_spread = binance_ticker['bid'] - bybit_ticker['ask']
                        
                        # Determine if there's a profitable arbitrage
                        arb_opportunity = "No"
                        arb_direction = "-"
                        arb_profit_bps = 0
                        
                        if bybit_binance_spread > 0:
                            arb_opportunity = "Yes"
                            arb_direction = "Buy Binance, Sell Bybit"
                            arb_profit_bps = (bybit_binance_spread / binance_ticker['ask']) * 10000
                        elif binance_bybit_spread > 0:
                            arb_opportunity = "Yes"
                            arb_direction = "Buy Bybit, Sell Binance"
                            arb_profit_bps = (binance_bybit_spread / bybit_ticker['ask']) * 10000
                        
                        comparison_data.append({
                            'Symbol': symbol,
                            'Binance Bid': binance_ticker['bid'],
                            'Binance Ask': binance_ticker['ask'],
                            'Bybit Bid': bybit_ticker['bid'],
                            'Bybit Ask': bybit_ticker['ask'],
                            'Mid Diff': bybit_mid - binance_mid,
                            'Mid Diff (bps)': ((bybit_mid / binance_mid) - 1) * 10000,
                            'Arbitrage': arb_opportunity,
                            'Direction': arb_direction,
                            'Profit (bps)': round(arb_profit_bps, 2)
                        })
                
                # Create and display DataFrame
                if comparison_data:
                    df = pd.DataFrame(comparison_data)
                    
                    # Detect environment: IPython/Jupyter or Terminal
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
                        # Clear terminal screen
                        os.system('cls' if os.name == 'nt' else 'clear')
                    
                    if in_notebook:
                        # Use Pandas styling for notebook display
                        # Format the numbers first
                        pd.set_option('display.float_format', '${:.4f}'.format)
                        
                        # Get a basic styled version of the table with formatting
                        styled_df = df.style.format({
                            'Binance Bid': "${:.2f}",
                            'Binance Ask': "${:.2f}",
                            'Bybit Bid': "${:.2f}",
                            'Bybit Ask': "${:.2f}",
                            'Mid Diff': "${:.4f}",
                            'Mid Diff (bps)': "{:.2f}",
                            'Profit (bps)': "{:.2f}"
                        })
                        
                        # Use background colors to highlight important information
                        styled_df = styled_df.map(
                            lambda x: 'background-color: #c6efce' if x == 'Yes' else '',
                            subset=['Arbitrage']
                        )
                        
                        # Add background color for direction
                        styled_df = styled_df.map(
                            lambda x: 'background-color: #c6efce' if x != '-' else '',
                            subset=['Direction']
                        )
                        
                        # Add color highlighting for price differences
                        styled_df = styled_df.map(
                            lambda x: 'color: green' if x > 0 else ('color: red' if x < 0 else ''),
                            subset=['Mid Diff', 'Mid Diff (bps)']
                        )
                        
                        # Add color for profit
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
                        styled_df = styled_df.set_caption(f"Exchange Comparison - {time.strftime('%H:%M:%S')}")
                        
                        # Print header
                        print(f"Exchange Comparison ({time.strftime('%H:%M:%S')})")
                        
                        # Display the styled DataFrame
                        display(styled_df)
                        
                    else:
                        # Terminal display with color codes and formatted table
                        print_comparison_header(time.strftime('%H:%M:%S'))
                        print_arbitrage_alerts(comparison_data)
                        print(format_exchange_comparison_table(df))
                else:
                    print("Waiting for data from both exchanges...")
                    
            except Exception as e:
                logger.error(f"Error updating display: {e}")
            
            time.sleep(self.update_interval)
    
    def stop(self):
        """Stop all connections and threads"""
        self.running = False
        if self.binance_client:
            self.binance_client.close()
        if self.bybit_client:
            self.bybit_client.close()
        logger.info("Exchange comparison stopped")


# Example usage
if __name__ == "__main__":
    # List of symbols to compare (must be available on both exchanges)
    symbols_to_compare = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    
    # Create and start the comparison tool
    comparison = ExchangeComparison(symbols_to_compare)
    comparison.start()
    
    try:
        # Keep the main thread running
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping exchange comparison...")
        comparison.stop()
        print("Comparison stopped")
