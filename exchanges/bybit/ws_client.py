import json
import time
import websocket
import threading
import logging
from typing import Dict, List, Callable, Optional, Union, Any

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("BybitWebSocketClient")

class BybitWebSocketClient:
    """
    WebSocket client for Bybit exchange.
    Handles connection, subscription, and message processing for Bybit WebSocket API.
    """
    
    # WebSocket endpoints
    MAINNET_URLS = {
        "spot": "wss://stream.bybit.com/v5/public/spot",
        "linear": "wss://stream.bybit.com/v5/public/linear",
        "inverse": "wss://stream.bybit.com/v5/public/inverse",
        "option": "wss://stream.bybit.com/v5/public/option"
    }
    
    TESTNET_URLS = {
        "spot": "wss://stream-testnet.bybit.com/v5/public/spot",
        "linear": "wss://stream-testnet.bybit.com/v5/public/linear",
        "inverse": "wss://stream-testnet.bybit.com/v5/public/inverse",
        "option": "wss://stream-testnet.bybit.com/v5/public/option"
    }
    
    def __init__(self, 
                 channel_type: str = "linear", 
                 testnet: bool = False, 
                 ping_interval: int = 20,
                 ping_timeout: int = 10, 
                 reconnect_delay: int = 5):
        """
        Initialize the WebSocket client.
        
        Args:
            channel_type: Type of channel to connect to ("spot", "linear", "inverse", "option")
            testnet: Whether to use testnet or mainnet
            ping_interval: Interval in seconds for sending ping messages
            ping_timeout: Timeout in seconds for ping response
            reconnect_delay: Delay in seconds before reconnection attempts
        """
        self.channel_type = channel_type.lower()
        self.testnet = testnet
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.reconnect_delay = reconnect_delay
        
        # Initialize data storage for orderbook
        self.data = {}
        
        # Validate channel type
        if self.channel_type not in self.MAINNET_URLS:
            raise ValueError(f"Invalid channel type: {channel_type}. Must be one of {list(self.MAINNET_URLS.keys())}")
        
        # Get appropriate URL based on environment and channel type
        urls = self.TESTNET_URLS if testnet else self.MAINNET_URLS
        self.ws_url = urls[self.channel_type]
        
        # WebSocket connection and status
        self.ws = None
        self.connected = False
        self.subscriptions = set()
        self.callbacks = {}
        self.default_callback = None
        
        # Threading for ping/pong and reconnection
        self.ping_thread = None
        self.thread_running = False
        
        # Connection ID from server
        self.conn_id = None
    
    def connect(self):
        """
        Establish WebSocket connection.
        """
        try:
            self.ws = websocket.WebSocketApp(
                self.ws_url,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
                on_open=self._on_open
            )
            
            # Start WebSocket connection in a separate thread
            self.ws_thread = threading.Thread(target=self.ws.run_forever)
            self.ws_thread.daemon = True
            self.ws_thread.start()
            
            logger.info(f"Connecting to {self.ws_url}")
        except Exception as e:
            logger.error(f"Error connecting to WebSocket: {e}")
            self._reconnect()
    
    def _on_open(self, ws):
        """
        Called when WebSocket connection is established.
        """
        self.connected = True
        logger.info(f"WebSocket connection established to {self.ws_url}")
        
        # Start the ping thread
        self.thread_running = True
        self.ping_thread = threading.Thread(target=self._ping_loop)
        self.ping_thread.daemon = True
        self.ping_thread.start()
        
        # Resubscribe to all topics if there were any
        self._resubscribe()
    
    def _on_message(self, ws, message):
        """
        Called when a message is received from the WebSocket server.
        """
        try:
            data = json.loads(message)
            
            # Handle pong response
            if "op" in data and data.get("op") == "ping":
                logger.debug("Received pong response")
                if "conn_id" in data:
                    self.conn_id = data["conn_id"]
                return
            
            # Check if it's a subscription confirmation
            if "op" in data and data.get("op") == "subscribe":
                logger.info(f"Subscription success: {data}")
                return
            
            # Regular data message
            if "topic" in data:
                topic = data["topic"]
                topic_base = topic.split(".")[0]
                
                # Check for specific topic callback
                if topic in self.callbacks:
                    self.callbacks[topic](data)
                # Check for topic type callback (e.g., 'orderbook')
                elif topic_base in self.callbacks:
                    self.callbacks[topic_base](data)
                # Use default callback if available
                elif self.default_callback:
                    self.default_callback(data)
                else:
                    logger.info(f"Received message for topic {topic}, but no callback registered")
            else:
                # Handle other message types
                logger.debug(f"Received message: {data}")
                if self.default_callback:
                    self.default_callback(data)
                
        except json.JSONDecodeError:
            logger.error(f"Failed to decode message: {message}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def _on_error(self, ws, error):
        """
        Called when an error occurs on the WebSocket connection.
        """
        logger.error(f"WebSocket error: {error}")
    
    def _on_close(self, ws, close_status_code, close_msg):
        """
        Called when the WebSocket connection is closed.
        """
        self.connected = False
        self.thread_running = False
        
        # Log closure details
        if close_status_code or close_msg:
            logger.info(f"WebSocket connection closed with code {close_status_code}: {close_msg}")
        else:
            logger.info("WebSocket connection closed")
        
        # Try to reconnect
        self._reconnect()
    
    def _reconnect(self):
        """
        Attempt to reconnect to the WebSocket server.
        """
        if not self.connected:
            logger.info(f"Attempting to reconnect in {self.reconnect_delay} seconds...")
            time.sleep(self.reconnect_delay)
            self.connect()
    
    def _ping_loop(self):
        """
        Send ping messages at regular intervals to keep the connection alive.
        """
        ping_count = 0
        while self.thread_running:
            try:
                if self.connected:
                    ping_message = json.dumps({
                        "req_id": f"ping_{ping_count}",
                        "op": "ping"
                    })
                    self.ws.send(ping_message)
                    logger.debug(f"Ping sent ({ping_count})")
                    ping_count += 1
            except Exception as e:
                logger.error(f"Error sending ping: {e}")
                
            # Sleep for ping interval
            for _ in range(self.ping_interval):
                if not self.thread_running:
                    break
                time.sleep(1)
    
    def subscribe(self, topics: Union[str, List[str]], callback: Optional[Callable] = None):
        """
        Subscribe to one or more topics.
        
        Args:
            topics: A single topic string or a list of topic strings
            callback: Optional callback function for processing messages
        """
        if isinstance(topics, str):
            topics = [topics]
        
        # Validate topics before subscribing
        if not topics:
            logger.error("No topics provided for subscription")
            return False
        
        # Prevent exceeding args length (21,000 chars) as per documentation
        args_str = json.dumps(topics)
        if len(args_str) > 21000:
            logger.error(f"Topics length exceeds 21,000 characters: {len(args_str)}")
            return False
        
        # Apply callback if provided
        if callback:
            for topic in topics:
                # Extract base topic (e.g., 'orderbook' from 'orderbook.50.BTCUSDT')
                base_topic = topic.split('.')[0]
                if '.' in topic:
                    self.callbacks[topic] = callback
                else:
                    self.callbacks[base_topic] = callback
        
        if not self.connected:
            # Store subscriptions for later if not connected
            self.subscriptions.update(topics)
            logger.info(f"Will subscribe to {topics} once connected")
            self.connect()
            return False
        
        try:
            # Send subscription request
            request = {
                "req_id": f"sub_{int(time.time() * 1000)}",
                "op": "subscribe",
                "args": topics
            }
            self.ws.send(json.dumps(request))
            self.subscriptions.update(topics)
            logger.info(f"Subscribed to: {topics}")
            return True
        except Exception as e:
            logger.error(f"Error subscribing to topics {topics}: {e}")
            return False
    
    def unsubscribe(self, topics: Union[str, List[str]]):
        """
        Unsubscribe from one or more topics.
        
        Args:
            topics: A single topic string or a list of topic strings
        """
        if isinstance(topics, str):
            topics = [topics]
        
        if not topics:
            logger.error("No topics provided for unsubscription")
            return False
            
        if not self.connected:
            logger.warning("Cannot unsubscribe, not connected")
            return False
        
        try:
            # Send unsubscription request
            request = {
                "req_id": f"unsub_{int(time.time() * 1000)}",
                "op": "unsubscribe",
                "args": topics
            }
            self.ws.send(json.dumps(request))
            
            # Remove from our subscription list
            self.subscriptions.difference_update(topics)
            
            # Remove callbacks for these topics
            for topic in topics:
                base_topic = topic.split('.')[0]
                if topic in self.callbacks:
                    del self.callbacks[topic]
                
            logger.info(f"Unsubscribed from: {topics}")
            return True
        except Exception as e:
            logger.error(f"Error unsubscribing from topics {topics}: {e}")
            return False
    
    def _resubscribe(self):
        """
        Resubscribe to all previously subscribed topics after reconnection.
        """
        if not self.subscriptions:
            return
            
        current_subs = list(self.subscriptions)
        logger.info(f"Resubscribing to {len(current_subs)} topics")
        
        # Break into chunks to avoid exceeding message size limits
        chunk_size = 50
        for i in range(0, len(current_subs), chunk_size):
            chunk = current_subs[i:i+chunk_size]
            request = {
                "req_id": f"resub_{int(time.time() * 1000)}",
                "op": "subscribe",
                "args": chunk
            }
            self.ws.send(json.dumps(request))
            logger.info(f"Resubscribed to chunk of {len(chunk)} topics")
    
    def set_default_callback(self, callback: Callable):
        """
        Set a default callback for messages without a specific topic callback.
        
        Args:
            callback: The callback function to use
        """
        self.default_callback = callback
    
    def subscribe_orderbook(self, symbol: str, depth: int = 50, callback: Optional[Callable] = None):
        """
        Subscribe to orderbook updates for a specific symbol.
        
        Args:
            symbol: The trading pair symbol (e.g., "BTCUSDT")
            depth: Orderbook depth (1, 50, 200, or 500 for linear/inverse, 1, 50, 200 for spot)
            callback: Callback function to process orderbook messages
        """
        valid_depths = {
            "spot": [1, 50, 200],
            "linear": [1, 50, 200, 500],
            "inverse": [1, 50, 200, 500],
            "option": [25, 100]
        }
        
        if depth not in valid_depths.get(self.channel_type, []):
            logger.error(f"Invalid depth {depth} for {self.channel_type}. Valid depths: {valid_depths.get(self.channel_type)}")
            return False
        
        topic = f"orderbook.{depth}.{symbol}"
        return self.subscribe(topic, callback)
    
    def subscribe_trades(self, symbol: str, callback: Optional[Callable] = None):
        """
        Subscribe to public trade updates for a specific symbol.
        
        Args:
            symbol: The trading pair symbol (e.g., "BTCUSDT")
            callback: Callback function to process trade messages
        """
        topic = f"publicTrade.{symbol}"
        return self.subscribe(topic, callback)
    
    def close(self):
        """
        Close the WebSocket connection.
        """
        self.thread_running = False
        if self.ws:
            self.ws.close()
        self.connected = False
        logger.info("WebSocket connection closed")
    
    def get_data(self, symbol: Optional[str] = None):
        """
        Get the latest orderbook data.
        
        Args:
            symbol: Optional symbol to get data for. If None, returns all data.
        
        Returns:
            dict: The latest orderbook data
        """
        if symbol:
            # Return data for a specific symbol if available
            return self.data.get(symbol, {})
        else:
            # Return all data
            return self.data

# Example usage
if __name__ == "__main__":
    def handle_orderbook(message):
        print(f"Received orderbook: {message['type']} for {message['data']['s']}")
        if message['type'] == 'snapshot':
            print(f"Best bid: {message['data']['b'][0][0]}, Best ask: {message['data']['a'][0][0]}")
    
    client = BybitWebSocketClient(testnet=True, channel_type="linear")
    client.connect()
    time.sleep(2)  # Give time to establish connection
    
    # Subscribe to BTC orderbook
    client.subscribe_orderbook("BTCUSDT", depth=50, callback=handle_orderbook)
    
    # Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        client.close()
