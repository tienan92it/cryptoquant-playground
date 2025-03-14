import json
import logging
import traceback
from typing import Dict, Any

logger = logging.getLogger(__name__)

def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load configuration from a JSON file with fallback to default values
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        Dictionary containing configuration parameters
    """
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Fill in default values for required parameters if missing
        config = _apply_default_values(config)
        logger.info(f"Successfully loaded configuration from {config_path}")
        return config
        
    except Exception as e:
        logger.error(f"Error loading config: {str(e)}")
        logger.error(traceback.format_exc())
        return _get_default_config()

def _apply_default_values(config: Dict[str, Any]) -> Dict[str, Any]:
    """Apply default values to config for any missing required parameters"""
    
    # Required parameters with default values
    if 'min_funding_spread' not in config:
        config['min_funding_spread'] = 0.0005  # Default 0.05% spread threshold
    
    if 'futures_fee_rate' not in config:
        config['futures_fee_rate'] = 0.0004  # Default 0.04% fee on Binance
        
    if 'bybit_fee_rate' not in config:
        config['bybit_fee_rate'] = 0.0006  # Default 0.06% fee on Bybit
        
    if 'okx_fee_rate' not in config:
        config['okx_fee_rate'] = 0.0005  # Default 0.05% fee on OKX
    
    if 'symbol_filters' not in config:
        config['symbol_filters'] = {}
        
    if 'exclude' not in config.get('symbol_filters', {}):
        config['symbol_filters']['exclude'] = ["BTCDOMUSDT", "DEFIUSDT"]
        
    if 'include_only' not in config.get('symbol_filters', {}):
        config['symbol_filters']['include_only'] = []
    
    return config

def _get_default_config() -> Dict[str, Any]:
    """Return a default configuration"""
    return {
        "symbols": ["btcusdt", "ethusdt", "solusdt"],
        "position_size_usd": 1000,
        "min_funding_spread": 0.0005,  # 0.05% funding spread
        "futures_fee_rate": 0.0004,    # Binance fee rate
        "bybit_fee_rate": 0.0006,      # Bybit fee rate
        "okx_fee_rate": 0.0005,        # OKX fee rate
        "slippage": 0.0003,            # 0.03% slippage
        "check_interval": 30,          # Check every 30 seconds
        "use_all_symbols": True,
        "exchanges": ["binance", "bybit"],
        "risk_management": {
            "max_positions": 5,
            "max_drawdown": 0.05
        },
        "symbol_filters": {
            "min_price": 0,
            "min_volume_usd": 0,
            "exclude": ["BTCDOMUSDT", "DEFIUSDT"],
            "include_only": []
        }
    }
