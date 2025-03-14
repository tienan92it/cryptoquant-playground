import requests
import logging
import time
from typing import Dict, List, Optional, Union, Any
from urllib.parse import urlencode

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("OkxRestClient")

class OkxRestClient:
    """
    REST client for OKX exchange.
    Provides access to OKX public API endpoints for market data.
    """
    
    # API Base URLs
    BASE_URL = "https://www.okx.com"
    TESTNET_BASE_URL = "https://www.okx.com"  # Using same URL but will add demo flag in headers
    
    def __init__(self, testnet: bool = False, api_key: str = None, api_secret: str = None, passphrase: str = None):
        """
        Initialize the OKX REST client.
        
        Args:
            testnet: Whether to use testnet (demo trading)
            api_key: API key for authenticated endpoints (optional)
            api_secret: API secret for authenticated endpoints (optional)
            passphrase: API passphrase for authenticated endpoints (optional)
        """
        self.testnet = testnet
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        
        # Base URL selection
        self.base_url = self.TESTNET_BASE_URL if testnet else self.BASE_URL
        
        # HTTP Session for connection pooling
        self.session = requests.Session()
        
        # Rate limiting parameters
        self.last_request_time = 0
        self.min_request_interval = 0.1  # 100ms minimum between requests
        
        logger.info(f"Initialized OKX REST client {'for demo trading' if testnet else 'for production'}")
    
    def _get_headers(self, endpoint: str = None, method: str = None, body: str = None) -> Dict:
        """
        Generate headers for API request.
        For public endpoints, only basic headers are needed.
        For private endpoints, authentication headers would be added.
        
        Args:
            endpoint: API endpoint
            method: HTTP method
            body: Request body for POST requests
            
        Returns:
            Dictionary of headers
        """
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "OKX-Python-Client"
        }
        
        # Add demo trading flag if using testnet
        if self.testnet:
            headers["x-simulated-trading"] = "1"
        
        # For now, we're only implementing public endpoints so no auth is needed
        # If auth is needed later, it would be added here
        
        return headers
    
    def _rate_limit(self):
        """Apply rate limiting to avoid API rate limits"""
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        
        if elapsed < self.min_request_interval:
            sleep_time = self.min_request_interval - elapsed
            time.sleep(sleep_time)
            
        self.last_request_time = time.time()
    
    def _request(self, method: str, endpoint: str, params: Dict = None, data: Dict = None) -> Dict:
        """
        Make a request to OKX API.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint
            params: URL parameters
            data: Request body for POST requests
            
        Returns:
            Response data as dictionary
        """
        # Apply rate limiting
        self._rate_limit()
        
        url = f"{self.base_url}{endpoint}"
        headers = self._get_headers(endpoint, method, data)
        
        try:
            # Make the request
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                json=data,
                headers=headers
            )
            
            # Check for HTTP errors
            response.raise_for_status()
            
            # Parse JSON response
            response_data = response.json()
            
            # Check for API errors
            if response_data.get("code") != "0":
                logger.error(f"API error: {response_data.get('msg', 'Unknown error')}")
                logger.error(f"Request: {method} {url} {params}")
                
            return response_data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {e}")
            logger.error(f"URL: {url}, Parameters: {params}")
            return {"code": "-1", "msg": f"Request failed: {str(e)}", "data": []}
        except ValueError as e:
            logger.error(f"Error parsing response: {e}")
            return {"code": "-1", "msg": f"Parse error: {str(e)}", "data": []}
    
    def get_instruments(self, inst_type: str, uly: str = None, 
                       inst_family: str = None, inst_id: str = None) -> List[Dict]:
        """
        Get instrument information from OKX.
        
        Args:
            inst_type: Instrument type (SPOT, MARGIN, SWAP, FUTURES, OPTION)
            uly: Underlying (conditional, required for some FUTURES/SWAP/OPTION)
            inst_family: Instrument family (conditional, required for some FUTURES/SWAP/OPTION)
            inst_id: Instrument ID (optional)
            
        Returns:
            List of instrument data dictionaries
        """
        # Parameter validation
        if not inst_type:
            logger.error("Instrument type (instType) is required")
            return []
            
        if inst_type not in ["SPOT", "MARGIN", "SWAP", "FUTURES", "OPTION"]:
            logger.error(f"Invalid instrument type: {inst_type}")
            return []
        
        # Build parameters
        params = {"instType": inst_type}
        
        if uly:
            params["uly"] = uly
        if inst_family:
            params["instFamily"] = inst_family
        if inst_id:
            params["instId"] = inst_id
        
        # Make request
        logger.info(f"Fetching {inst_type} instruments from OKX")
        response = self._request("GET", "/api/v5/public/instruments", params=params)
        
        # Check for success and return data
        if response.get("code") == "0" and "data" in response:
            instruments = response["data"]
            logger.info(f"Fetched {len(instruments)} {inst_type} instruments from OKX")
            return instruments
            
        # Return empty list on error
        return []
    
    def get_perpetual_symbols(self) -> List[str]:
        """
        Get all perpetual swap symbols from OKX.
        
        Returns:
            List of symbol strings in standard format (BASE-USDT)
        """
        instruments = self.get_instruments(inst_type="SWAP")
        
        # Filter for perpetual swaps (perpetual futures)
        perpetual_symbols = []
        for instrument in instruments:
            inst_id = instrument.get("instId", "")
            
            # Check if it's a perpetual swap (no expiry time)
            if not instrument.get("expTime") and inst_id.endswith("-USDT-SWAP"):
                # Extract the trading pair part and convert to standard format
                base_ccy = instrument.get("baseCcy", "")
                if base_ccy:
                    standard_symbol = f"{base_ccy}-USDT"
                    perpetual_symbols.append(standard_symbol)
        
        logger.info(f"Found {len(perpetual_symbols)} perpetual swap symbols on OKX")
        return perpetual_symbols

    def get_funding_rate(self, inst_id: str) -> Dict[str, Any]:
        """
        Get funding rate for a specific instrument.
        
        Args:
            inst_id: Instrument ID like "BTC-USDT-SWAP"
            
        Returns:
            Dictionary with funding rate information
        """
        params = {
            "instType": "SWAP",
            "instId": inst_id
        }
        
        response = self._request("GET", "/api/v5/public/funding-rate", params=params)
        
        if response.get("code") == "0" and "data" in response and response["data"]:
            return response["data"][0]
        else:
            logger.error(f"Failed to get funding rate for {inst_id}: {response.get('msg', 'Unknown error')}")
            # Return placeholder data
            return {
                "fundingRate": "0.0001",
                "nextFundingRate": "0.0001",
                "nextFundingTime": str(int(time.time() * 1000) + 28800000)  # 8 hours from now
            }

    def get_instrument_id_mapping(self) -> Dict[str, str]:
        """
        Get a mapping from standard symbol format to OKX instrument ID format.
        
        Returns:
            Dict mapping standard symbols (e.g. "BTC-USDT") to OKX instId (e.g. "BTC-USDT-SWAP")
        """
        instruments = self.get_instruments(inst_type="SWAP")
        
        # Create mapping from standard symbol to OKX instrument ID
        mappings = {}
        for inst in instruments:
            inst_id = inst.get("instId", "")
            base_ccy = inst.get("baseCcy", "")
            
            if not base_ccy or not inst_id or not inst_id.endswith("-USDT-SWAP"):
                continue
                
            # Standard format (with hyphen)
            standard_symbol = f"{base_ccy}-USDT"
            mappings[standard_symbol] = inst_id
            
            # Also add format without hyphen
            standard_symbol_no_hyphen = f"{base_ccy}USDT" 
            mappings[standard_symbol_no_hyphen] = inst_id
        
        return mappings
    
    def close(self):
        """Close the session."""
        self.session.close()
        logger.info("OKX REST client session closed")


# Example usage
if __name__ == "__main__":
    client = OkxRestClient(testnet=False)
    
    try:
        # Get SPOT instruments
        spot_instruments = client.get_instruments(inst_type="SPOT", inst_id="BTC-USDT")
        if spot_instruments:
            print(f"Found {len(spot_instruments)} SPOT instruments:")
            for instrument in spot_instruments[:3]:  # Show first 3
                print(f"  {instrument['instId']}: {instrument.get('baseCcy', '')}-{instrument.get('quoteCcy', '')}")
        
        # Get SWAP (perpetual futures) instruments
        swap_instruments = client.get_instruments(inst_type="SWAP")
        if swap_instruments:
            print(f"\nFound {len(swap_instruments)} SWAP instruments")
            # Show first 3 perpetual swaps
            swap_count = 0
            for instrument in swap_instruments:
                if swap_count >= 3:
                    break
                if instrument['instId'].endswith("-USDT-SWAP"):
                    print(f"  {instrument['instId']}")
                    swap_count += 1
        
        # Get perpetual symbols in standard format
        perpetual_symbols = client.get_perpetual_symbols()
        print(f"\nPerpetual symbols: {perpetual_symbols[:5]} ...")
        
    finally:
        client.close()
