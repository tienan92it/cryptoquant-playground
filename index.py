import threading
import pandas as pd
import time
from IPython.display import display, clear_output
from exchanges.binance.ws_client import BinanceWebSocketClient
import sys
sys.path.append('/Users/antran/Desktop/Workspace.nosync/Quant/realtime-prices')
from strategies.triangular_arbitrage import TriangularArbitrage

# Define symbols to track (BTC and ETH against USDT)
symbols = ["solusdt"]

# Create and start the WebSocket client
# client = BinanceWebSocketClient(symbols)
# client.connect()

# Wait a moment for initial data
time.sleep(2)

# Function to continuously display the data
# def display_prices():
#     while True:
#         data = client.get_data()
#         if data:
#             # Create a DataFrame for display
#             df_data = []
#             for symbol, ticker in data.items():
#                 mid_price = (ticker['bid'] + ticker['ask']) / 2
#                 spread = ticker['ask'] - ticker['bid']
                
#                 df_data.append({
#                     'Symbol': symbol,
#                     'Bid': ticker['bid'],
#                     'Ask': ticker['ask'],
#                     'Mid Price': mid_price,
#                     'Spread': spread,
#                     'Bid Qty': ticker['bid_qty'],
#                     'Ask Qty': ticker['ask_qty']
#                 })
            
#             # Create and display DataFrame
#             df = pd.DataFrame(df_data)
#             clear_output(wait=True)
#             print("Real-time Binance Market Data:")
#             display(df)
        
#         time.sleep(1)

# # Start display in a separate thread
# display_thread = threading.Thread(target=display_prices)
# display_thread.daemon = True
# display_thread.start()

# Create strategy with custom parameters
strategy = TriangularArbitrage(
    exchange="bybit",  # Try a different exchange
    base_currencies=["BTC", "ETH", "SOL", "XRP", "ADA", "DOGE", "AXS"],  # Add more currencies
    quote_currencies=["USDT", "USDC"],
    min_profit_threshold=0.001,  # Lower threshold (0.1%)
    fee_rate=0.0005,  # Assuming lower fees (0.05%)
    testnet=True
)

# Start the strategy
strategy.start()

# Check for opportunities (can be called periodically)
opportunities = strategy.get_opportunities()
print(f"Found {len(opportunities)} opportunities")

# Get stats
stats = strategy.get_stats()
print(stats)

# Keep the notebook cell running
try:
    # The cell will keep running until interrupted
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Closing connection...")
    # client.close()
    # When done:
    strategy.stop()