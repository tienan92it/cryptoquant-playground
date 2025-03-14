import os
import math
import time
from tabulate import tabulate
from datetime import datetime
from typing import List, Dict, Any

def format_countdown(hours: float) -> str:
    """
    Format hours to a countdown timer (HH:MM:SS)
    
    Args:
        hours: Time in hours
        
    Returns:
        Formatted string as HH:MM:SS
    """
    total_seconds = int(hours * 3600)
    hours_part = total_seconds // 3600
    minutes_part = (total_seconds % 3600) // 60
    seconds_part = total_seconds % 60
    
    return f"{hours_part:02d}:{minutes_part:02d}:{seconds_part:02d}"

def display_funding_metrics(
    opportunities: List[Dict[str, Any]],
    metrics: Dict[str, Dict[str, Any]],
    positions: Dict[str, Dict[str, Dict[str, Any]]],
    max_positions: int
) -> None:
    """
    Display arbitrage metrics and opportunities in the terminal
    
    Args:
        opportunities: Ranked list of opportunities
        metrics: Dictionary of metrics by symbol
        positions: Dictionary tracking current positions
        max_positions: Maximum number of positions allowed
    """
    # Clear terminal for better display
    os.system('cls' if os.name == 'nt' else 'clear')
    
    # Print header
    print(f"\n{'=' * 120}")
    print(f" CROSS-EXCHANGE FUNDING ARBITRAGE MONITOR - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 120}")
    
    # Show active positions summary
    active_positions = sum(
        1 for symbol in positions 
        if any(position['active'] for exchange, position in positions[symbol].items())
    )
    print(f"\nActive Positions: {active_positions}/{max_positions}")
    
    # Best opportunities table (top 15)
    if opportunities:
        print("\nBEST CROSS-EXCHANGE FUNDING OPPORTUNITIES:")
        
        # Prepare data for tabulate
        best_data = []
        for i, item in enumerate(opportunities[:15]):
            symbol = item['symbol']
            m = item['metrics']
            
            # Show if we have an active position for this symbol
            position_info = ""
            if symbol in positions and any(position['active'] for exchange, position in positions[symbol].items()):
                active_exchanges = [exchange.upper() for exchange, position in positions[symbol].items() if position['active']]
                position_info = f"[{'+'.join(active_exchanges)}]"
            
            # Highlight top opportunities with an arrow
            prefix = "→ " if i < max_positions and not position_info else "  "
            
            # Format time to funding as countdown timer
            time_to_funding = format_countdown(m['time_to_funding_hours'])
            
            # Get funding rates for all exchanges
            funding_rates = {}
            for exchange in ['binance', 'bybit', 'okx']:
                funding_key = f'{exchange}_funding_rate'
                if funding_key in m:
                    funding_rates[exchange] = f"{m[funding_key]*100:+.4f}%"
                else:
                    funding_rates[exchange] = "N/A"
            
            # Show the exchange pair and the funding spread
            pair_info = f"{m['long_exchange'].upper()}-{m['short_exchange'].upper()}"
            
            best_data.append([
                prefix + symbol,
                pair_info,
                f"{m['funding_spread']*100:+.4f}%",
                funding_rates.get('binance', 'N/A'),
                funding_rates.get('bybit', 'N/A'),
                funding_rates.get('okx', 'N/A'),
                time_to_funding,
                f"${m['expected_profit_per_funding']:.4f}",
                f"{m['break_even_events']:.1f}",
                f"{m['apr']*100:.2f}%",
                position_info
            ])
        
        print(tabulate(
            best_data, 
            headers=["Symbol", "Best Pair", "Spread", "Binance", "Bybit", "OKX", 
                    "Countdown", "Profit/Fund", "Break Even", "APR", "Position"]
        ))
    else:
        print("\nNo profitable opportunities found at current thresholds.")
    
    # Active positions table
    active_data = []
    for symbol, exchanges in positions.items():
        # Skip if no active positions for this symbol
        if not any(position['active'] for exchange, position in exchanges.items()):
            continue
            
        # Get metrics for this symbol
        m = metrics.get(symbol, {})
        if not m:
            continue
            
        # Get active exchanges for this symbol
        active_exchanges = [(exchange, position) for exchange, position in exchanges.items() if position['active']]
        
        # Skip if no active positions
        if not active_exchanges:
            continue
            
        # Calculate position duration (from the earliest entry)
        entry_times = [position['entry_time'] for _, position in active_exchanges if position['entry_time']]
        if entry_times:
            earliest_entry = min(entry_times)
            duration = (datetime.now() - earliest_entry).total_seconds() / 3600  # hours
            entry_time_str = earliest_entry.strftime('%Y-%m-%d %H:%M:%S')
        else:
            entry_time_str = "Unknown"
            duration = 0
            
        # Format exchange sides and quantities
        exchange_info = ", ".join([f"{exchange.upper()} {position['side']}" for exchange, position in active_exchanges])
        
        # Add position info
        active_data.append([
            symbol,
            exchange_info,
            f"${m.get('notional_value', 0):.2f}",
            entry_time_str,
            f"{duration:.2f} hrs",
            f"${m.get('expected_profit_per_funding', 0):.4f}",
            f"{m.get('apr', 0)*100:.2f}%"
        ])
    
    if active_data:
        print("\n\nACTIVE POSITIONS:")
        print(tabulate(
            active_data, 
            headers=["Symbol", "Exchanges", "Notional Value", "Entry Time", "Duration", "Profit/Fund", "APR"]
        ))
    
    # Footer
    print(f"\n{'=' * 120}")
    print(f"Monitoring {len(metrics)} symbols - {len(opportunities)} profitable opportunities")
    print(f"{'=' * 120}")

def display_connection_status(ws_connected: Dict[str, bool]) -> None:
    """
    Display WebSocket connection status
    
    Args:
        ws_connected: Dictionary of connection status by exchange
    """
    status_text = []
    
    for exchange, connected in ws_connected.items():
        status = "✓ Connected" if connected else "✗ Disconnected"
        color_code = "\033[92m" if connected else "\033[91m"  # Green if connected, red if not
        status_text.append(f"{exchange.capitalize()}: {color_code}{status}\033[0m")
    
    print(f"\nWebSocket Status: {' | '.join(status_text)}")
