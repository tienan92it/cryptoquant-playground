import websocket
import json
import threading
import time

class BinanceWebSocketClient:
    def __init__(self, spot_symbols=None, futures_symbols=None, mark_price_freq='3s', use_all_market_stream=False, testnet=False):
        """
        Initialize the Binance WebSocket client
        
        Args:
            spot_symbols: List of spot symbols for book ticker data (optional)
            futures_symbols: List of futures symbols for mark price data (optional)
            mark_price_freq: Frequency of mark price updates ('3s' or '1s')
            use_all_market_stream: Whether to use the all-market mark price stream
            testnet: Whether to use the testnet for futures
        """
        self.spot_symbols = [s.lower() for s in spot_symbols] if spot_symbols else []
        self.futures_symbols = [s.lower() for s in futures_symbols] if futures_symbols else []
        self.mark_price_freq = '1s' if mark_price_freq == '1s' else '3s'
        self.use_all_market_stream = use_all_market_stream
        self.testnet = testnet
        
        # Separate WebSocket connections
        self.spot_ws = None
        self.futures_ws = None
        
        # Data storage
        self.spot_data = {}
        self.mark_price_data = {}
        
        # Thread references
        self.spot_thread = None
        self.futures_thread = None
        
        # Futures endpoint
        self.futures_endpoint = "wss://testnet.binancefuture.com/ws" if testnet else "wss://fstream.binance.com/ws"
        
    def connect(self):
        """Connect to WebSocket endpoints"""
        # Connect to spot market if symbols are provided
        if self.spot_symbols:
            self._connect_spot()
            
        # Connect to futures market if symbols are provided or using all-market stream
        if self.futures_symbols or self.use_all_market_stream:
            self._connect_futures()
    
    def _connect_spot(self):
        """Connect to spot market WebSocket"""
        # Format streams for combined stream
        spot_streams = [f"{symbol}@bookTicker" for symbol in self.spot_symbols]
        streams = "/".join(spot_streams)
        socket_url = f"wss://stream.binance.com:9443/stream?streams={streams}"
        
        # Initialize spot websocket connection
        self.spot_ws = websocket.WebSocketApp(
            socket_url,
            on_message=self._on_spot_message,
            on_error=self._on_spot_error,
            on_close=self._on_spot_close,
            on_open=self._on_spot_open,
            on_ping=self._on_ping
        )
        
        # Start WebSocket connection in a thread
        self.spot_thread = threading.Thread(target=self.spot_ws.run_forever)
        self.spot_thread.daemon = True
        self.spot_thread.start()
        
    def _connect_futures(self):
        """Connect to futures market WebSocket"""
        # Determine the stream name based on configuration
        if self.use_all_market_stream:
            # All market stream
            suffix = "@1s" if self.mark_price_freq == '1s' else ""
            stream_name = f"!markPrice@arr{suffix}"
        else:
            # Individual symbol streams
            streams = []
            for symbol in self.futures_symbols:
                suffix = "@1s" if self.mark_price_freq == '1s' else ""
                streams.append(f"{symbol}@markPrice{suffix}")
            stream_name = "/".join(streams)
        
        # Initialize futures websocket connection
        socket_url = f"{self.futures_endpoint}/{stream_name}"
        
        self.futures_ws = websocket.WebSocketApp(
            socket_url,
            on_message=self._on_futures_message,
            on_error=self._on_futures_error,
            on_close=self._on_futures_close,
            on_open=self._on_futures_open,
            on_ping=self._on_ping
        )
        
        # Start WebSocket connection in a thread
        self.futures_thread = threading.Thread(target=self.futures_ws.run_forever)
        self.futures_thread.daemon = True
        self.futures_thread.start()
    
    # Spot WebSocket handlers
    def _on_spot_open(self, ws):
        print("Spot WebSocket connection opened")
    
    def _on_spot_message(self, ws, message):
        try:
            msg = json.loads(message)
            
            if 'data' in msg:
                data = msg['data']
                symbol = data['s'].upper()  # Store with uppercase symbol
                self.spot_data[symbol] = {
                    'symbol': symbol,
                    'bid': float(data['b']),
                    'bid_qty': float(data['B']),
                    'ask': float(data['a']),
                    'ask_qty': float(data['A']),
                    'timestamp': time.time()
                }
        except Exception as e:
            print(f"Error processing spot message: {e}")
    
    def _on_spot_error(self, ws, error):
        print(f"Spot WebSocket error: {error}")
    
    def _on_spot_close(self, ws, close_status_code, close_msg):
        print(f"Spot WebSocket connection closed: {close_status_code} - {close_msg}")
    
    # Futures WebSocket handlers
    def _on_futures_open(self, ws):
        print("Futures WebSocket connection opened")
    
    def _on_futures_message(self, ws, message):
        try:
            # Parse the message
            data = json.loads(message)
            
            # Process based on data structure
            if isinstance(data, list):
                # All-market stream format
                for item in data:
                    self._process_mark_price_data(item)
            elif isinstance(data, dict) and 'e' in data and data['e'] == 'markPriceUpdate':
                # Individual symbol stream format
                self._process_mark_price_data(data)
        except Exception as e:
            print(f"Error processing futures message: {e}")
    
    def _process_mark_price_data(self, data):
        """Process mark price data from WebSocket"""
        symbol = data['s'].upper()  # Ensure consistent uppercase
        self.mark_price_data[symbol] = {
            'symbol': symbol,
            'mark_price': float(data['p']),
            'index_price': float(data['i']),
            'estimated_settle_price': float(data['P']),
            'funding_rate': float(data['r']),
            'next_funding_time': data['T'],
            'timestamp': data['E']
        }
    
    def _on_futures_error(self, ws, error):
        print(f"Futures WebSocket error: {error}")
    
    def _on_futures_close(self, ws, close_status_code, close_msg):
        print(f"Futures WebSocket connection closed: {close_status_code} - {close_msg}")
    
    def _on_ping(self, ws, message):
        """Handle ping from server by responding with a pong containing the same payload"""
        ws.send(message, websocket.ABNF.OPCODE_PONG)
    
    # Data access methods
    def get_spot_data(self):
        """Get the latest spot book ticker data"""
        return self.spot_data
    
    def get_mark_price_data(self):
        """Get the latest mark price data for all subscribed futures symbols"""
        return self.mark_price_data
    
    def get_mark_price(self, symbol):
        """Get mark price data for a specific symbol
        
        Args:
            symbol: Symbol to get mark price data for (case insensitive)
        """
        symbol = symbol.upper()  # Ensure consistent format
        return self.mark_price_data.get(symbol)
    
    def get_spot_price(self, symbol):
        """Get spot price data for a specific symbol
        
        Args:
            symbol: Symbol to get spot price data for (case insensitive)
        """
        symbol = symbol.upper()  # Ensure consistent format
        return self.spot_data.get(symbol)
    
    def close(self):
        """Close all WebSocket connections"""
        if self.spot_ws:
            self.spot_ws.close()
            
        if self.futures_ws:
            self.futures_ws.close()