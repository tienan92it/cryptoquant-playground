import logging
import math
import time
from typing import Dict, List, Any, Tuple, Optional
from datetime import datetime
from operator import itemgetter

logger = logging.getLogger(__name__)

def calculate_funding_metrics(
    symbol: str,
    binance_data: Dict[str, Any],
    bybit_data: Dict[str, Any],
    config: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Calculate arbitrage metrics between exchanges for a single symbol
    
    Args:
        symbol: Symbol to calculate metrics for
        binance_data: Funding and price data from Binance
        bybit_data: Funding and price data from Bybit
        config: Strategy configuration
        
    Returns:
        Dictionary of calculated metrics or None if data is insufficient
    """
    if not binance_data or not bybit_data:
        return None
    
    # Extract funding rates
    binance_funding_rate = float(binance_data.get('funding_rate', 0))
    bybit_funding_rate = float(bybit_data.get('funding_rate', 0))
    
    # Fallback for Bybit if using older field name
    if bybit_funding_rate == 0 and 'fundingRate' in bybit_data:
        bybit_funding_rate = float(bybit_data.get('fundingRate', 0))
        
    # Calculate spread
    funding_spread = binance_funding_rate - bybit_funding_rate
    abs_funding_spread = abs(funding_spread)
    
    # Determine trading sides
    binance_side = "LONG" if binance_funding_rate < bybit_funding_rate else "SHORT"
    bybit_side = "SHORT" if binance_side == "LONG" else "LONG"
    
    # Extract mark prices
    binance_mark_price = float(binance_data.get('mark_price', 0))
    
    # Extract Bybit mark price with fallback
    if 'mark_price' in bybit_data:
        bybit_mark_price = float(bybit_data['mark_price'])
    elif 'markPrice' in bybit_data:
        bybit_mark_price = float(bybit_data['markPrice'])
    else:
        bybit_mark_price = binance_mark_price  # Fallback
    
    # Calculate next funding times
    now_ms = int(time.time() * 1000)
    binance_next_funding_time = binance_data.get('next_funding_time', 0)
    
    # Extract Bybit next funding time with fallback
    if 'next_funding_time' in bybit_data:
        bybit_next_funding_time = bybit_data.get('next_funding_time', 0)
    elif 'nextFundingTime' in bybit_data:
        bybit_next_funding_time = bybit_data.get('nextFundingTime', 0)
    else:
        bybit_next_funding_time = 0
    
    # Calculate time until next funding
    binance_time_to_funding_ms = max(0, binance_next_funding_time - now_ms)
    bybit_time_to_funding_ms = max(0, bybit_next_funding_time - now_ms)
    
    # Use the earliest funding time
    if binance_time_to_funding_ms > 0 and bybit_time_to_funding_ms > 0:
        time_to_funding_ms = min(binance_time_to_funding_ms, bybit_time_to_funding_ms)
    elif binance_time_to_funding_ms > 0:
        time_to_funding_ms = binance_time_to_funding_ms
    elif bybit_time_to_funding_ms > 0:
        time_to_funding_ms = bybit_time_to_funding_ms
    else:
        time_to_funding_ms = 28800000  # Default to 8 hours
    
    time_to_funding_hours = time_to_funding_ms / (1000 * 60 * 60)
    
    # Calculate position sizes
    notional_value = config.get('position_size_usd', 1000)
    binance_qty = notional_value / binance_mark_price if binance_mark_price > 0 else 0
    bybit_qty = notional_value / bybit_mark_price if bybit_mark_price > 0 else 0
    
    # Calculate trading costs
    binance_fee = notional_value * config.get('futures_fee_rate', 0.0004) * 2  # Entry and exit
    bybit_fee = notional_value * config.get('bybit_fee_rate', 0.0006) * 2      # Entry and exit
    slippage_cost = notional_value * config.get('slippage', 0.0003) * 4        # Total slippage
    total_trading_cost = binance_fee + bybit_fee + slippage_cost
    
    # Calculate expected profit
    expected_profit_per_funding = abs_funding_spread * notional_value
    
    # Calculate break-even events
    if expected_profit_per_funding > 0:
        break_even_events = max(1, math.ceil(total_trading_cost / expected_profit_per_funding))
    else:
        break_even_events = float('inf')
    
    # Calculate annualized returns
    funding_events_per_year = 365 * 3  # Assuming 3 funding events per day
    
    # Only calculate if profitable
    if break_even_events < float('inf') and expected_profit_per_funding > 0:
        optimal_holding_periods = max(1, break_even_events)
        total_profit = (expected_profit_per_funding * optimal_holding_periods) - total_trading_cost
        holding_time_fraction = optimal_holding_periods / funding_events_per_year
        
        # Calculate APR/APY
        apr = (total_profit / notional_value) / holding_time_fraction if holding_time_fraction > 0 else 0
        apy = ((1 + (total_profit / notional_value)) ** (1 / holding_time_fraction) - 1) if holding_time_fraction > 0 else 0
    else:
        apr = 0
        apy = 0
    
    # Format funding times for display
    binance_next_funding_str = format_timestamp(binance_next_funding_time)
    bybit_next_funding_str = format_timestamp(bybit_next_funding_time)
    
    # Compile and return all metrics
    return {
        'symbol': symbol,
        'binance_funding_rate': binance_funding_rate,
        'bybit_funding_rate': bybit_funding_rate,
        'funding_spread': funding_spread,
        'abs_funding_spread': abs_funding_spread,
        'binance_side': binance_side,
        'bybit_side': bybit_side,
        'binance_mark_price': binance_mark_price,
        'bybit_mark_price': bybit_mark_price,
        'binance_next_funding': binance_next_funding_str,
        'bybit_next_funding': bybit_next_funding_str,
        'next_funding_time': min(binance_next_funding_time, bybit_next_funding_time) if binance_next_funding_time > 0 and bybit_next_funding_time > 0 else max(binance_next_funding_time, bybit_next_funding_time),
        'time_to_funding_hours': time_to_funding_hours,
        'binance_qty': binance_qty,
        'bybit_qty': bybit_qty,
        'notional_value': notional_value,
        'expected_profit_per_funding': expected_profit_per_funding,
        'total_trading_cost': total_trading_cost,
        'break_even_events': break_even_events,
        'is_profitable': break_even_events < float('inf') and expected_profit_per_funding > 0,
        'apr': apr,
        'apy': apy
    }

def format_timestamp(timestamp_ms):
    """Format millisecond timestamp to readable date/time"""
    if timestamp_ms <= 0:
        return "Unknown"
    return datetime.fromtimestamp(timestamp_ms/1000).strftime('%Y-%m-%d %H:%M:%S')

def rank_opportunities(metrics: Dict[str, Dict[str, Any]], min_funding_spread: float) -> List[Dict[str, Any]]:
    """
    Rank arbitrage opportunities by profitability
    
    Args:
        metrics: Dictionary of metrics keyed by symbol
        min_funding_spread: Minimum funding spread to consider
        
    Returns:
        List of opportunities sorted by APR
    """
    ranked = []
    
    for symbol, m in metrics.items():
        # Only include profitable opportunities
        if m['is_profitable'] and m['abs_funding_spread'] >= min_funding_spread:
            ranked.append({
                'symbol': symbol,
                'funding_spread': m['funding_spread'],
                'apr': m['apr'],
                'break_even_events': m['break_even_events'],
                'binance_side': m['binance_side'],
                'bybit_side': m['bybit_side'],
                'metrics': m
            })
    
    # Sort by APR (highest to lowest)
    return sorted(ranked, key=itemgetter('apr'), reverse=True)

def should_execute_arbitrage(metrics: Dict[str, Any], min_spread: float) -> bool:
    """
    Determine if arbitrage should be executed based on metrics
    
    Args:
        metrics: Calculated metrics for a symbol
        min_spread: Minimum funding spread threshold
        
    Returns:
        Boolean indicating whether arbitrage is viable
    """
    # Must have metrics
    if not metrics:
        return False
    
    # Funding spread must be above threshold
    if metrics['abs_funding_spread'] < min_spread:
        return False
    
    # Strategy must be profitable
    if not metrics['is_profitable']:
        return False
    
    return True
