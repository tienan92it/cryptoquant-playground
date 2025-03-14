import logging
import math
import time
from typing import Dict, List, Any, Tuple, Optional
from datetime import datetime
from operator import itemgetter

logger = logging.getLogger(__name__)

def calculate_funding_metrics(
    symbol: str,
    exchange_data: Dict[str, Dict[str, Any]],
    config: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Calculate arbitrage metrics between exchanges for a single symbol
    
    Args:
        symbol: Symbol to calculate metrics for
        exchange_data: Dictionary with funding and price data from all exchanges
        config: Strategy configuration
        
    Returns:
        Dictionary of calculated metrics or None if data is insufficient
    """
    # Need data from at least 2 exchanges to calculate metrics
    if len(exchange_data) < 2:
        return None
    
    # Store all funding rates
    funding_rates = {}
    mark_prices = {}
    next_funding_times = {}
    
    # Extract data from each exchange
    for exchange, data in exchange_data.items():
        if not data:
            continue
            
        # Extract funding rate based on available field names
        funding_rate = None
        if 'funding_rate' in data:
            funding_rate = float(data['funding_rate'])
        elif 'fundingRate' in data:
            funding_rate = float(data['fundingRate'])
            
        if funding_rate is not None:
            funding_rates[exchange] = funding_rate
            
        # Extract mark price
        mark_price = None
        if 'mark_price' in data:
            mark_price = float(data['mark_price'])
        elif 'markPrice' in data:
            mark_price = float(data['markPrice'])
            
        if mark_price is not None and mark_price > 0:
            mark_prices[exchange] = mark_price
            
        # Extract next funding time
        next_funding_time = None
        if 'next_funding_time' in data:
            next_funding_time = int(data['next_funding_time'])
        elif 'nextFundingTime' in data:
            next_funding_time = int(data['nextFundingTime'])
            
        if next_funding_time is not None and next_funding_time > 0:
            next_funding_times[exchange] = next_funding_time
    
    # Skip if we don't have funding rates from at least 2 exchanges
    if len(funding_rates) < 2:
        return None
        
    # Calculate all possible funding rate pairs and find the best one
    exchange_pairs = []
    
    for long_exchange in funding_rates:
        for short_exchange in funding_rates:
            if long_exchange == short_exchange:
                continue
                
            # Calculate funding rate spread (long pays negative rate, short pays positive rate)
            spread = funding_rates[short_exchange] - funding_rates[long_exchange]
            
            # Store this pair if both exchanges have mark prices
            if long_exchange in mark_prices and short_exchange in mark_prices:
                exchange_pairs.append({
                    'long_exchange': long_exchange,
                    'short_exchange': short_exchange,
                    'funding_spread': spread,
                    'abs_funding_spread': abs(spread)
                })
                
    # If no valid pairs found, return None
    if not exchange_pairs:
        return None
        
    # Find the pair with the highest absolute funding spread
    best_pair = max(exchange_pairs, key=lambda x: x['abs_funding_spread'])
    
    # Get the exchange names for this pair
    long_exchange = best_pair['long_exchange']
    short_exchange = best_pair['short_exchange']
    
    # Calculate trading parameters for the best pair
    funding_spread = best_pair['funding_spread']
    abs_funding_spread = best_pair['abs_funding_spread']
    
    # Calculate trading costs
    notional_value = config.get('position_size_usd', 1000)
    
    # Calculate quantities based on mark prices
    long_qty = notional_value / mark_prices[long_exchange] if mark_prices[long_exchange] > 0 else 0
    short_qty = notional_value / mark_prices[short_exchange] if mark_prices[short_exchange] > 0 else 0
    
    # Calculate trading costs for each exchange
    fee_rates = {
        'binance': config.get('futures_fee_rate', 0.0004),
        'bybit': config.get('bybit_fee_rate', 0.0006),
        'okx': config.get('okx_fee_rate', 0.0005)
    }
    
    # Entry and exit fees for both exchanges
    long_fee = notional_value * fee_rates.get(long_exchange, 0.0006) * 2
    short_fee = notional_value * fee_rates.get(short_exchange, 0.0006) * 2
    
    # Calculate slippage (entry and exit on two exchanges)
    slippage_cost = notional_value * config.get('slippage', 0.0003) * 4
    
    # Total trading cost
    total_trading_cost = long_fee + short_fee + slippage_cost
    
    # Calculate expected profit for a single funding interval
    expected_profit_per_funding = abs_funding_spread * notional_value
    
    # Calculate break-even events
    if expected_profit_per_funding > 0:
        break_even_events = max(1, math.ceil(total_trading_cost / expected_profit_per_funding))
    else:
        break_even_events = float('inf')
        
    # Calculate time until next funding
    now_ms = int(time.time() * 1000)
    
    # Use the earliest funding time from either exchange
    next_funding_time = 0
    long_funding_time = next_funding_times.get(long_exchange, 0)
    short_funding_time = next_funding_times.get(short_exchange, 0)
    
    if long_funding_time > 0 and short_funding_time > 0:
        next_funding_time = min(long_funding_time, short_funding_time)
    elif long_funding_time > 0:
        next_funding_time = long_funding_time
    elif short_funding_time > 0:
        next_funding_time = short_funding_time
    else:
        next_funding_time = now_ms + 28800000  # Default to 8 hours if no funding time available
        
    time_to_funding_ms = max(0, next_funding_time - now_ms)
    time_to_funding_hours = time_to_funding_ms / (1000 * 60 * 60)
    
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
    next_funding_str = format_timestamp(next_funding_time)
    
    # Create the complete metrics dictionary
    metrics = {
        'symbol': symbol,
        'pair': f"{long_exchange.upper()}-{short_exchange.upper()}",
        'funding_spread': funding_spread,
        'abs_funding_spread': abs_funding_spread,
        'long_exchange': long_exchange,
        'short_exchange': short_exchange,
        'time_to_funding_hours': time_to_funding_hours,
        'next_funding_time': next_funding_time,
        'next_funding_str': next_funding_str,
        'notional_value': notional_value,
        'long_qty': long_qty,
        'short_qty': short_qty,
        'expected_profit_per_funding': expected_profit_per_funding,
        'total_trading_cost': total_trading_cost,
        'break_even_events': break_even_events,
        'is_profitable': break_even_events < float('inf') and expected_profit_per_funding > 0,
        'apr': apr,
        'apy': apy
    }
    
    # Add exchange-specific data
    for exchange in exchange_data.keys():
        if exchange in funding_rates:
            metrics[f'{exchange}_funding_rate'] = funding_rates[exchange]
        if exchange in mark_prices:
            metrics[f'{exchange}_mark_price'] = mark_prices[exchange]
    
    return metrics

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
        # Only include profitable opportunities with sufficient spread
        if m['is_profitable'] and m['abs_funding_spread'] >= min_funding_spread:
            ranked.append({
                'symbol': symbol,
                'pair': m['pair'],
                'funding_spread': m['funding_spread'],
                'apr': m['apr'],
                'break_even_events': m['break_even_events'],
                'long_exchange': m['long_exchange'],
                'short_exchange': m['short_exchange'],
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
