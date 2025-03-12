import requests
import logging
import time
import hmac
import hashlib
import json
from urllib.parse import urlencode
from typing import Dict, List, Optional, Union, Any

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("BybitRestClient")

class BybitRestClient:
    """
    REST API client for Bybit exchange.
    Provides access to various market data endpoints including funding rate history.
    """
    
    # API Base URLs
    MAINNET_URL = "https://api.bybit.com"
    TESTNET_URL = "https://api-testnet.bybit.com"
    
    # API endpoints
    ENDPOINTS = {
        "funding_history": "/v5/market/funding/history",
        "instruments_info": "/v5/market/instruments-info",
        "tickers": "/v5/market/tickers",
    }
    
    def __init__(self, testnet: bool = False, api_key: str = None, api_secret: str = None):
        """
        Initialize the Bybit REST client.
        
        Args:
            testnet: Whether to use testnet instead of mainnet
            api_key: API key for authenticated endpoints (optional)
            api_secret: API secret for authenticated endpoints (optional)
        """
        self.testnet = testnet
        self.api_key = api_key
        self.api_secret = api_secret
        
        # Select base URL based on environment
        self.base_url = self.TESTNET_URL if testnet else self.MAINNET_URL
        
        # Session for connection pooling
        self.session = requests.Session()
        
        # Rate limiting parameters
        self.last_request_time = 0
        self.min_request_interval = 0.05  # 50ms minimum between requests
    
    def _generate_signature(self, params: Dict, timestamp: int) -> str:
        """
        Generate signature for authenticated requests.
        
        Args:
            params: Request parameters
            timestamp: Current timestamp in milliseconds
            
        Returns:
            Signature string
        """
        if not self.api_secret:
            return ""
            
        param_str = str(timestamp) + self.api_key + "5000" + urlencode(params)
        return hmac.new(
            bytes(self.api_secret, "utf-8"),
            bytes(param_str, "utf-8"),
            hashlib.sha256
        ).hexdigest()
    
    def _add_auth_headers(self, params: Dict) -> Dict:
        """
        Add authentication headers to request.
        
        Args:
            params: Request parameters
            
        Returns:
            Headers dictionary
        """
        headers = {}
        
        if self.api_key and self.api_secret:
            timestamp = int(time.time() * 1000)
            signature = self._generate_signature(params, timestamp)
            
            headers = {
                "X-BAPI-API-KEY": self.api_key,
                "X-BAPI-SIGN": signature,
                "X-BAPI-SIGN-TYPE": "2",
                "X-BAPI-TIMESTAMP": str(timestamp),
                "X-BAPI-RECV-WINDOW": "5000",
                "Content-Type": "application/json"
            }
        
        return headers
    
    def _rate_limit(self):
        """Apply rate limiting to avoid API rate limits"""
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        
        if elapsed < self.min_request_interval:
            sleep_time = self.min_request_interval - elapsed
            time.sleep(sleep_time)
            
        self.last_request_time = time.time()
    
    def _make_request(self, method: str, endpoint: str, params: Dict = None, 
                      auth_required: bool = False) -> Dict:
        """
        Make a request to the Bybit REST API.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint
            params: Query string parameters
            auth_required: Whether authentication is required
            
        Returns:
            Response data as dictionary
        """
        # Apply rate limiting
        self._rate_limit()
        
        url = f"{self.base_url}{endpoint}"
        headers = {}
        
        if params is None:
            params = {}
            
        # Add authentication if required
        if auth_required:
            headers = self._add_auth_headers(params)
        
        try:
            # Make the request
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                headers=headers
            )
            
            # Check for HTTP errors
            response.raise_for_status()
            
            # Parse JSON response
            response_data = response.json()
            
            # Check for API errors
            if response_data.get("retCode") != 0:
                logger.error(f"API error: {response_data.get('retMsg')}")
                logger.error(f"Parameters: {params}")
                logger.error(f"Full response: {response_data}")
            
            return response_data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {e}")
            logger.error(f"URL: {url}, Parameters: {params}")
            return {"retCode": -1, "retMsg": f"Request failed: {str(e)}"}
        except ValueError as e:
            logger.error(f"Error parsing response: {e}")
            return {"retCode": -1, "retMsg": f"Parse error: {str(e)}"}
    
    def get_funding_history(self, symbol: str, category: str = "linear", 
                          start_time: Optional[int] = None, end_time: Optional[int] = None,
                          limit: int = 200) -> List[Dict]:
        """
        Get historical funding rates for a symbol.
        
        Args:
            symbol: Symbol name (e.g., "BTCUSDT")
            category: Product type ("linear" or "inverse")
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds
            limit: Number of records to return (max 200)
            
        Returns:
            List of funding rate records
        """
        # Bybit expects uppercase symbols with no formatting changes
        symbol_formatted = symbol.upper().replace("-", "")
        
        params = {
            "category": category,
            "symbol": symbol_formatted,
            "limit": min(limit, 200)  # Ensure limit is within allowed range
        }
        
        logger.info(f"Getting funding history for {symbol_formatted}")
        
        # Add optional parameters if provided
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time
        
        response = self._make_request("GET", self.ENDPOINTS["funding_history"], params=params)
        
        if response.get("retCode") == 0 and "result" in response:
            result = response["result"]
            if "list" in result:
                return result["list"]
        
        logger.error(f"Error getting funding history: {response.get('retMsg', 'Unknown error')}")
        return []
    
    def get_instruments_info(self, symbol: Optional[str] = None, category: str = "linear", 
                           status: str = "Trading", base_coin: Optional[str] = None,
                           limit: int = 500, cursor: str = None) -> List[Dict]:
        """
        Get instrument specifications.
        
        Args:
            symbol: Symbol name (e.g., "BTCUSDT")
            category: Product type ("spot", "linear", "inverse", "option")
            status: Symbol status filter (default "Trading")
            base_coin: Base coin filter
            limit: Number of records to return (max 1000)
            cursor: Cursor for pagination
            
        Returns:
            List of instrument specifications
        """
        params = {
            "category": category,
            "limit": min(limit, 1000),
            "status": status
        }
        
        # Add optional parameters if provided
        if symbol:
            params["symbol"] = symbol.upper()
        if base_coin:
            params["baseCoin"] = base_coin.upper()
        if cursor:
            params["cursor"] = cursor
        
        response = self._make_request("GET", self.ENDPOINTS["instruments_info"], params=params)
        
        if response.get("retCode") == 0 and "result" in response:
            result = response["result"]
            instruments = result.get("list", [])
            
            # Handle pagination if there's a next page
            if "nextPageCursor" in result and result["nextPageCursor"]:
                next_cursor = result["nextPageCursor"]
                if next_cursor:
                    # Fetch next page
                    next_page = self.get_instruments_info(
                        symbol=symbol, 
                        category=category,
                        status=status, 
                        base_coin=base_coin,
                        limit=limit, 
                        cursor=next_cursor
                    )
                    instruments.extend(next_page)
            
            return instruments
        
        logger.error(f"Error getting instruments info: {response.get('retMsg', 'Unknown error')}")
        return []
    
    def get_tickers(self, symbol: Optional[str] = None, category: str = "linear") -> List[Dict]:
        """
        Get market tickers including funding rates.
        
        Args:
            symbol: Symbol name (e.g., "BTCUSDT")
            category: Product type ("spot", "linear", "inverse", "option")
            
        Returns:
            List of ticker data
        """
        params = {"category": category}
        
        if symbol:
            params["symbol"] = symbol.upper()
        
        response = self._make_request("GET", self.ENDPOINTS["tickers"], params=params)
        
        if response.get("retCode") == 0 and "result" in response:
            result = response["result"]
            if "list" in result:
                return result["list"]
        
        logger.error(f"Error getting tickers: {response.get('retMsg', 'Unknown error')}")
        return []
    
    def get_funding_interval(self, symbol: str, category: str = "linear") -> int:
        """
        Get funding interval in minutes for a symbol.
        
        Args:
            symbol: Symbol name (e.g., "BTCUSDT")
            category: Product type ("linear" or "inverse")
            
        Returns:
            Funding interval in minutes (default 480 - 8 hours)
        """
        try:
            info = self.get_instruments_info(symbol=symbol, category=category)
            if info and len(info) > 0:
                interval = info[0].get("fundingInterval")
                if interval is not None:
                    return int(interval)
                else:
                    logger.warning(f"No funding interval data for {symbol}, using default (480)")
            else:
                logger.warning(f"No instrument info found for {symbol}, using default (480)")
            
            return 480  # Default funding interval (8 hours)
            
        except Exception as e:
            logger.error(f"Error getting funding interval: {e}")
            logger.error(traceback.format_exc())
            return 480  # Default funding interval (8 hours)
    
    def get_all_perpetual_symbols(self, category: str = "linear") -> List[str]:
        """
        Get all available perpetual contract symbols.
        
        Args:
            category: Product type ("linear" or "inverse")
            
        Returns:
            List of symbol names
        """
        try:
            info = self.get_instruments_info(category=category, status="Trading")
            return [item["symbol"] for item in info if item["contractType"] == "LinearPerpetual"]
            
        except Exception as e:
            logger.error(f"Error getting perpetual symbols: {e}")
            return []
    
    def get_funding_stats(self, symbol: str, days: int = 7):
        """
        Get funding rate statistics for a symbol over a period.
        
        Args:
            symbol: Symbol name (e.g., "BTCUSDT")
            days: Number of days to look back
            
        Returns:
            Dictionary with funding rate statistics
        """
        try:
            # Calculate time range
            end_time = int(time.time() * 1000)
            start_time = end_time - (days * 24 * 60 * 60 * 1000)
            
            # Get funding history
            history = self.get_funding_history(
                symbol=symbol,
                start_time=start_time,
                end_time=end_time,
                limit=200
            )
            
            if not history:
                return {
                    "symbol": symbol,
                    "count": 0,
                    "avg_rate": 0,
                    "min_rate": 0,
                    "max_rate": 0,
                    "latest_rate": 0,
                    "sum": 0
                }
            
            # Calculate statistics
            rates = [float(entry["fundingRate"]) for entry in history]
            latest_rate = float(history[0]["fundingRate"]) if history else 0
            
            return {
                "symbol": symbol,
                "count": len(rates),
                "avg_rate": sum(rates) / len(rates) if rates else 0,
                "min_rate": min(rates) if rates else 0,
                "max_rate": max(rates) if rates else 0,
                "latest_rate": latest_rate,
                "sum": sum(rates) if rates else 0,
                "history": history
            }
            
        except Exception as e:
            logger.error(f"Error calculating funding stats for {symbol}: {e}")
            return {"symbol": symbol, "error": str(e)}
    
    def find_best_funding_opportunities(self, category: str = "linear", min_threshold: float = 0.0001,
                                       days: int = 7) -> List[Dict]:
        """
        Find the best funding rate opportunities across all symbols.
        
        Args:
            category: Product type ("linear" or "inverse")
            min_threshold: Minimum absolute funding rate to consider
            days: Number of days to analyze
            
        Returns:
            List of opportunities sorted by average funding rate
        """
        try:
            # Get all available symbols
            symbols = self.get_all_perpetual_symbols(category)
            logger.info(f"Analyzing funding rates for {len(symbols)} symbols")
            
            opportunities = []
            
            for symbol in symbols:
                # Validate the symbol before requesting funding history
                if not self._validate_symbol(symbol, category):
                    logger.warning(f"Symbol {symbol} appears invalid, skipping")
                    continue
                    
                logger.info(f"Analyzing funding for {symbol}")
                stats = self.get_funding_stats(symbol, days)
                if not stats.get("error") and stats["count"] > 0:
                    # Consider absolute value of average rate for ranking
                    abs_avg_rate = abs(stats["avg_rate"])
                    
                    # Only include if above threshold
                    if abs_avg_rate >= min_threshold:
                        # Determine position side based on average rate
                        side = "LONG" if stats["avg_rate"] < 0 else "SHORT"
                        
                        opportunities.append({
                            "symbol": symbol,
                            "side": side,
                            "avg_rate": stats["avg_rate"],
                            "abs_avg_rate": abs_avg_rate,
                            "min_rate": stats["min_rate"],
                            "max_rate": stats["max_rate"],
                            "latest_rate": stats["latest_rate"],
                            "count": stats["count"],
                            "annual_yield": abs_avg_rate * 365 * 3,  # Approximate annual yield
                        })
            
            # Sort by absolute average rate (descending)
            return sorted(opportunities, key=lambda x: x["abs_avg_rate"], reverse=True)
            
        except Exception as e:
            logger.error(f"Error finding funding opportunities: {e}")
            return []
    
    def _validate_symbol(self, symbol: str, category: str = "linear") -> bool:
        """
        Validate if a symbol exists on Bybit.
        
        Args:
            symbol: Symbol name (e.g., "BTCUSDT")
            category: Product type ("linear" or "inverse")
            
        Returns:
            Bool indicating if the symbol is valid
        """
        try:
            symbol_formatted = symbol.upper().replace("-", "")
            info = self.get_instruments_info(symbol=symbol_formatted, category=category)
            return len(info) > 0
        except Exception as e:
            logger.error(f"Error validating symbol {symbol}: {e}")
            return False

    def close(self):
        """Close the session."""
        self.session.close()

# Example usage
if __name__ == "__main__":
    client = BybitRestClient()
    
    try:
        # Get funding history for BTC
        history = client.get_funding_history("BTCUSDT", limit=10)
        print(f"Recent BTC funding rates:")
        for entry in history:
            timestamp = datetime.fromtimestamp(int(entry["fundingRateTimestamp"]) / 1000)
            print(f"{timestamp}: {float(entry['fundingRate']) * 100:.6f}%")
        
        # Get funding intervals
        interval = client.get_funding_interval("BTCUSDT")
        print(f"\nBTCUSDT funding interval: {interval} minutes ({interval/60:.1f} hours)")
        
        # Find best opportunities
        print("\nFinding best funding opportunities...")
        opportunities = client.find_best_funding_opportunities(min_threshold=0.0001)
        print(f"Found {len(opportunities)} opportunities")
        
        for i, opp in enumerate(opportunities[:10]):  # Show top 10
            print(f"{i+1}. {opp['symbol']} ({opp['side']}): {opp['avg_rate']*100:+.6f}% " +
                  f"(APR: {opp['annual_yield']*100:.2f}%)")
    
    finally:
        client.close()
