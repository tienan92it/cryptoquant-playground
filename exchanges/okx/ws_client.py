import json
import time
import websocket
import threading
import logging
import binascii
from typing import Dict, List, Callable, Optional, Union, Any
from dataclasses import dataclass, field

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("OkxWebSocketClient")

# Global orderbook storage
order_books = {}

@dataclass
class OrderBookLevel:
    """Represents a single level in the orderbook"""
    price: float
    quantity: float
    order_count: int
    price_string: str
    quantity_string: str
    order_count_string: str

    @staticmethod
    def _is_valid_operand(other):
        return (hasattr(other, "price") and
                hasattr(other, "quantity"))

    def __lt__(self, other):
        if not self._is_valid_operand(other):
            return NotImplemented
        return self.price < other.price

    def __eq__(self, other):
        if not self._is_valid_operand(other):
            return NotImplemented
        return self.price == other.price


@dataclass
class OrderBook:
    """Represents an orderbook for a specific instrument"""
    inst_id: str
    _bids: List[OrderBookLevel] = field(default_factory=lambda: list())
    _asks: List[OrderBookLevel] = field(default_factory=lambda: list())
    timestamp: int = 0
    exch_check_sum: int = 0

    def set_bids_on_snapshot(self, order_book_level_list: List[OrderBookLevel]):
        self._bids = sorted(order_book_level_list, reverse=True)

    def set_asks_on_snapshot(self, order_book_level_list: List[OrderBookLevel]):
        self._asks = sorted(order_book_level_list, reverse=False)

    def set_bids_on_update(self, order_book_level: OrderBookLevel):
        if not self._bids or self._bids[-1] > order_book_level:
            self._bids.append(order_book_level)
        else:
            for i in range(len(self._bids)):
                if order_book_level > self._bids[i]:
                    self._bids.insert(i, order_book_level)
                    break
                elif order_book_level == self._bids[i]:
                    if order_book_level.quantity == 0:
                        self._bids.pop(i)
                    else:
                        self._bids[i] = order_book_level
                    break

    def set_asks_on_update(self, order_book_level: OrderBookLevel):
        if not self._asks or self._asks[-1] < order_book_level:
            self._asks.append(order_book_level)
        else:
            for i in range(len(self._asks)):
                if order_book_level < self._asks[i]:
                    self._asks.insert(i, order_book_level)
                    break
                elif order_book_level == self._asks[i]:
                    if order_book_level.quantity == 0:
                        self._asks.pop(i)
                    else:
                        self._asks[i] = order_book_level
                    break

    def set_timestamp(self, timestamp: int):
        self.timestamp = timestamp

    def set_exch_check_sum(self, checksum: int):
        self.exch_check_sum = checksum

    def _current_check_sum(self):
        bid_ask_string = ""
        for i in range(max(len(self._bids), len(self._asks))):
            if len(self._bids) > i:
                bid_ask_string += f"{self._bids[i].price_string}:{self._bids[i].quantity_string}:"
            if len(self._asks) > i:
                bid_ask_string += f"{self._asks[i].price_string}:{self._asks[i].quantity_string}:"
            if i + 1 >= 25:
                break
        if bid_ask_string:
            bid_ask_string = bid_ask_string[:-1]
        crc = binascii.crc32(bid_ask_string.encode()) & 0xffffffff  # Calculate CRC32 as unsigned integer
        crc_signed = crc if crc < 0x80000000 else crc - 0x100000000  # Convert to signed integer
        return crc_signed

    def do_check_sum(self) -> bool:
        if not self.exch_check_sum:
            return True  # ignore check sum
        current_crc = self._current_check_sum()
        return current_crc == self.exch_check_sum

    def _check_empty_array(self, order_book_array):
        if not order_book_array:
            raise IndexError(f"Orderbook for {self.inst_id}: either bids or asks array not initiated.")

    def best_bid(self) -> OrderBookLevel:
        self._check_empty_array(self._bids)
        return self._bids[0]

    def best_ask(self) -> OrderBookLevel:
        self._check_empty_array(self._asks)
        return self._asks[0]

    def best_bid_price(self) -> float:
        self._check_empty_array(self._bids)
        return self._bids[0].price

    def best_ask_price(self) -> float:
        self._check_empty_array(self._asks)
        return self._asks[0].price

    def bid_by_level(self, level: int) -> OrderBookLevel:
        self._check_empty_array(self._bids)
        if level <= 0:
            level = 1
        if level > len(self._bids):
            level = 0
        return self._bids[level - 1]

    def ask_by_level(self, level: int) -> OrderBookLevel:
        self._check_empty_array(self._asks)
        if level <= 0:
            level = 1
        if level > len(self._asks):
            level = 0
        return self._asks[level - 1]

    def middle_price(self):
        self._check_empty_array(self._bids)
        self._check_empty_array(self._asks)
        return (self._bids[0].price + self._asks[0].price) / 2


class OkxWebSocketClient:
    """
    WebSocket client for OKX exchange using the standard websocket library.
    Handles connection, subscription, and processing of orderbook data.
    """
    
    # WebSocket endpoints
    WS_PUBLIC_URL = "wss://ws.okx.com:8443/ws/v5/public"
    WS_PRIVATE_URL = "wss://ws.okx.com:8443/ws/v5/private"
    
    # For testnet
    WS_PUBLIC_TESTNET_URL = "wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999"
    WS_PRIVATE_TESTNET_URL = "wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999"
    
    def __init__(self, 
                 testnet: bool = False,
                 ping_interval: int = 20,
                 ping_timeout: int = 10,
                 reconnect_interval: int = 5):
        """
        Initialize the OKX WebSocket client.
        
        Args:
            testnet: Whether to use testnet instead of mainnet
            ping_interval: Interval in seconds for sending ping messages
            ping_timeout: Timeout in seconds for ping response
            reconnect_interval: Interval in seconds to wait before reconnecting after a disconnect
        """
        self.testnet = testnet
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.reconnect_interval = reconnect_interval
        
        # Select URL based on environment
        self.ws_url = self.WS_PUBLIC_TESTNET_URL if testnet else self.WS_PUBLIC_URL
        
        # WebSocket connection and status
        self.ws = None
        self.connected = False
        self.subscriptions = {}  # {symbol: channel}
        self.callbacks = {}  # Store callbacks for different channels and symbols
        
        # Threading for ping/pong and reconnection
        self.ping_thread = None
        self.checksum_thread = None
        self.thread_running = False
    
    def _on_message(self, ws, message):
        """
        Called when a message is received from the WebSocket server.
        """
        try:
            # Parse message
            message_data = json.loads(message)
            
            # Handle ping messages
            if "event" in message_data and message_data["event"] == "ping":
                self._send_pong()
                return

            # Handle subscription confirmations
            if "event" in message_data and message_data["event"] == "subscribe":
                logger.info(f"Subscription confirmed: {message_data.get('arg', {})}")
                return
            
            # Process the orderbook data
            self._process_orderbook_message(message_data)
            
            # Call specific callback if registered
            arg = message_data.get('arg', {})
            channel = arg.get('channel', '')
            inst_id = arg.get('instId', '')
            
            callback_key = f"{channel}:{inst_id}"
            if callback_key in self.callbacks:
                self.callbacks[callback_key](message_data)
            
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
        
        # Start checksum verification thread
        self._start_checksum_thread()
        
        # Resubscribe to all topics if there were any
        self._resubscribe()
    
    def _reconnect(self):
        """
        Attempt to reconnect to the WebSocket server.
        """
        if not self.connected:
            logger.info(f"Attempting to reconnect in {self.reconnect_delay} seconds...")
            time.sleep(self.reconnect_interval)
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
    
    def _send_pong(self):
        """
        Send a pong response to the server's ping.
        """
        try:
            pong_message = json.dumps({
                "op": "pong"
            })
            self.ws.send(pong_message)
            logger.debug("Pong sent")
        except Exception as e:
            logger.error(f"Error sending pong: {e}")
    
    def _process_orderbook_message(self, message):
        """
        Process orderbook snapshot or update message and update internal state.
        """
        arg = message.get("arg")
        if not arg or not arg.get("channel"):
            return
        if message.get("event") == "subscribe":
            return
        
        if arg.get("channel") in ["books5", "books", "bbo-tbt", "books50-l2-tbt", "books-l2-tbt"]:
            inst_id = arg.get("instId")
            action = message.get("action")
            
            if inst_id not in order_books:
                order_books[inst_id] = OrderBook(inst_id=inst_id)
            
            data = message.get("data")[0]
            
            # Process asks
            if data.get("asks"):
                if action == "snapshot" or not action:
                    ask_list = [OrderBookLevel(
                        price=float(level_info[0]),
                        quantity=float(level_info[1]),
                        order_count=int(level_info[3]),
                        price_string=level_info[0],
                        quantity_string=level_info[1],
                        order_count_string=level_info[3],
                    ) for level_info in data["asks"]]
                    order_books[inst_id].set_asks_on_snapshot(ask_list)
                if action == "update":
                    for level_info in data["asks"]:
                        order_books[inst_id].set_asks_on_update(
                            OrderBookLevel(
                                price=float(level_info[0]),
                                quantity=float(level_info[1]),
                                order_count=int(level_info[3]),
                                price_string=level_info[0],
                                quantity_string=level_info[1],
                                order_count_string=level_info[3],
                            )
                        )
            
            # Process bids
            if data.get("bids"):
                if action == "snapshot" or not action:
                    bid_list = [OrderBookLevel(
                        price=float(level_info[0]),
                        quantity=float(level_info[1]),
                        order_count=int(level_info[3]),
                        price_string=level_info[0],
                        quantity_string=level_info[1],
                        order_count_string=level_info[3],
                    ) for level_info in data["bids"]]
                    order_books[inst_id].set_bids_on_snapshot(bid_list)
                if action == "update":
                    for level_info in data["bids"]:
                        order_books[inst_id].set_bids_on_update(
                            OrderBookLevel(
                                price=float(level_info[0]),
                                quantity=float(level_info[1]),
                                order_count=int(level_info[3]),
                                price_string=level_info[0],
                                quantity_string=level_info[1],
                                order_count_string=level_info[3],
                            )
                        )
            
            # Update timestamp and checksum
            if data.get("ts"):
                order_books[inst_id].set_timestamp(int(data["ts"]))
            if data.get("checksum"):
                order_books[inst_id].set_exch_check_sum(data["checksum"])
    
    def connect(self):
        """
        Connect to the OKX WebSocket server.
        """
        try:
            logger.info(f"Connecting to OKX WebSocket at {self.ws_url}")
            
            # Create WebSocket using standard websocket library
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
            
        except Exception as e:
            logger.error(f"Error connecting to OKX WebSocket: {e}")
            self.connected = False
    
    def _resubscribe(self):
        """
        Resubscribe to all previous subscriptions after reconnection.
        """
        for symbol, channel in self.subscriptions.items():
            self._subscribe_orderbook(symbol, channel)
    
    def subscribe_orderbook(self, symbol: str, depth: str = "books5", callback: Optional[Callable] = None):
        """
        Subscribe to orderbook updates for a specific symbol.
        
        Args:
            symbol: Symbol to subscribe to (e.g., "BTC-USDT")
            depth: Depth channel ("books5", "books", "bbo-tbt", "books50-l2-tbt", "books-l2-tbt")
            callback: Optional callback function for processing messages
        
        Returns:
            bool: True if subscription was successful, False otherwise
        """
        if depth not in ["books", "books5", "bbo-tbt", "books-l2-tbt", "books50-l2-tbt"]:
            logger.error(f"Invalid depth channel: {depth}")
            return False
        
        # Store the callback if provided
        if callback:
            callback_key = f"{depth}:{symbol}"
            self.callbacks[callback_key] = callback
        
        # Store subscription for reconnection
        self.subscriptions[symbol] = depth
        
        # Subscribe if connected
        if self.connected:
            return self._subscribe_orderbook(symbol, depth)
        else:
            # Connect first
            self.connect()
            return True
    
    def _subscribe_orderbook(self, symbol: str, channel: str):
        """
        Internal method to subscribe to orderbook channel.
        """
        try:
            if not self.connected:
                logger.warning("Not connected, cannot subscribe")
                return False
            
            # Prepare subscription args
            request = {
                "op": "subscribe",
                "args": [
                    {
                        "channel": channel,
                        "instId": symbol
                    }
                ]
            }
            
            # Send subscription request
            self.ws.send(json.dumps(request))
            logger.info(f"Subscribed to {channel} for {symbol}")
            return True
        
        except Exception as e:
            logger.error(f"Error subscribing to {channel} for {symbol}: {e}")
            return False
    
    def subscribe_trades(self, symbol: str, callback: Optional[Callable] = None):
        """
        Subscribe to trade updates for a specific symbol.
        
        Args:
            symbol: Symbol to subscribe to (e.g., "BTC-USDT")
            callback: Optional callback function for processing messages
        
        Returns:
            bool: True if subscription was successful, False otherwise
        """
        channel = "trades"
        
        # Store the callback if provided
        if callback:
            callback_key = f"{channel}:{symbol}"
            self.callbacks[callback_key] = callback
        
        # Store subscription for reconnection
        self.subscriptions[symbol] = channel
        
        # Subscribe if connected
        if self.connected:
            try:
                # Prepare subscription args
                request = {
                    "op": "subscribe",
                    "args": [
                        {
                            "channel": channel,
                            "instId": symbol
                        }
                    ]
                }
                
                # Send subscription request
                self.ws.send(json.dumps(request))
                logger.info(f"Subscribed to {channel} for {symbol}")
                return True
            except Exception as e:
                logger.error(f"Error subscribing to {channel} for {symbol}: {e}")
                return False
        else:
            # Connect first
            self.connect()
            return True
    
    def unsubscribe_orderbook(self, symbol: str, depth: str = "books5"):
        """
        Unsubscribe from orderbook updates.
        """
        if not self.connected:
            logger.warning("Cannot unsubscribe, not connected")
            return False
        
        try:
            # Prepare unsubscription request
            request = {
                "op": "unsubscribe",
                "args": [
                    {
                        "channel": depth,
                        "instId": symbol
                    }
                ]
            }
            
            # Send unsubscription request
            self.ws.send(json.dumps(request))
            
            # Remove from our records
            if symbol in self.subscriptions:
                del self.subscriptions[symbol]
            
            # Remove callback if registered
            callback_key = f"{depth}:{symbol}"
            if callback_key in self.callbacks:
                del self.callbacks[callback_key]
            
            logger.info(f"Unsubscribed from {depth} for {symbol}")
            return True
            
        except Exception as e:
            logger.error(f"Error unsubscribing from {depth} for {symbol}: {e}")
            return False
    
    def unsubscribe_trades(self, symbol: str):
        """
        Unsubscribe from trade updates.
        """
        if not self.connected:
            logger.warning("Cannot unsubscribe, not connected")
            return False
        
        channel = "trades"
        try:
            # Prepare unsubscription request
            request = {
                "op": "unsubscribe",
                "args": [
                    {
                        "channel": channel,
                        "instId": symbol
                    }
                ]
            }
            
            # Send unsubscription request
            self.ws.send(json.dumps(request))
            
            # Remove from our records
            if symbol in self.subscriptions:
                del self.subscriptions[symbol]
            
            # Remove callback if registered
            callback_key = f"{channel}:{symbol}"
            if callback_key in self.callbacks:
                del self.callbacks[callback_key]
            
            logger.info(f"Unsubscribed from {channel} for {symbol}")
            return True
            
        except Exception as e:
            logger.error(f"Error unsubscribing from {channel} for {symbol}: {e}")
            return False
    
    def _start_checksum_thread(self):
        """
        Start a thread to verify checksums periodically.
        """
        self.thread_running = True
        self.checksum_thread = threading.Thread(target=self._checksum_verification_loop)
        self.checksum_thread.daemon = True
        self.checksum_thread.start()
    
    def _checksum_verification_loop(self):
        """
        Periodically verify checksums and reconnect if needed.
        """
        while self.thread_running:
            try:
                for inst_id, order_book in order_books.items():
                    if not order_book.do_check_sum():
                        logger.warning(f"Checksum mismatch for {inst_id}, reconnecting...")
                        
                        # Get the channel for this symbol
                        channel = self.subscriptions.get(inst_id, "books5")
                        
                        # Unsubscribe and resubscribe
                        self.unsubscribe_orderbook(inst_id, channel)
                        time.sleep(1)
                        self.subscribe_orderbook(inst_id, channel)
                        break
                    
            except Exception as e:
                logger.error(f"Error in checksum verification: {e}")
                
            # Sleep before next check
            time.sleep(5)
    
    def get_data(self, symbol: Optional[str] = None):
        """
        Get the latest orderbook data.
        
        Args:
            symbol: Optional symbol to get data for. If None, returns all data.
        
        Returns:
            dict: The latest orderbook data
        """
        if symbol:
            try:
                if symbol in order_books:
                    order_book = order_books[symbol]
                    
                    # Extract best bid and ask
                    best_bid = order_book.best_bid()
                    best_ask = order_book.best_ask()
                    
                    return {
                        'symbol': symbol,
                        'bid': best_bid.price,
                        'bid_qty': best_bid.quantity,
                        'ask': best_ask.price,
                        'ask_qty': best_ask.quantity,
                        'timestamp': order_book.timestamp
                    }
            except Exception as e:
                logger.error(f"Error retrieving data for {symbol}: {e}")
                
            return {}
        else:
            # Format all symbols to match the get_data return format of other clients
            result = {}
            for sym, order_book in order_books.items():
                try:
                    # Extract best bid and ask
                    best_bid = order_book.best_bid()
                    best_ask = order_book.best_ask()
                    
                    result[sym] = {
                        'symbol': sym,
                        'bid': best_bid.price,
                        'bid_qty': best_bid.quantity,
                        'ask': best_ask.price,
                        'ask_qty': best_ask.quantity,
                        'timestamp': order_book.timestamp
                    }
                except Exception as e:
                    logger.error(f"Error retrieving data for {sym}: {e}")
            
            return result
    
    def close(self):
        """
        Close the WebSocket connection.
        """
        self.thread_running = False
        if self.ws:
            # Unsubscribe from all topics
            for symbol, channel in list(self.subscriptions.items()):
                if channel in ["books5", "books", "bbo-tbt", "books50-l2-tbt", "books-l2-tbt"]:
                    self.unsubscribe_orderbook(symbol, channel)
                elif channel == "trades":
                    self.unsubscribe_trades(symbol)
            
            # Close connection
            self.ws.close()
            self.connected = False
            
        logger.info("OKX WebSocket connection closed")


# Example usage
if __name__ == "__main__":
    def handle_orderbook(message):
        arg = message.get("arg", {})
        inst_id = arg.get("instId", "")
        print(f"Received orderbook update for {inst_id}")
    
    client = OkxWebSocketClient(testnet=False)
    client.connect()
    
    # Give some time for connection to establish
    time.sleep(2)
    
    # Subscribe to BTC-USDT orderbook
    client.subscribe_orderbook("BTC-USDT", "books5", callback=handle_orderbook)
    
    try:
        while True:
            data = client.get_data("BTC-USDT")
            if data:
                print(f"Best bid: {data['bid']}, Best ask: {data['ask']}")
            time.sleep(2)
    except KeyboardInterrupt:
        client.close()
        print("Client closed")
