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
        if positions[symbol]['binance']['active'] and positions[symbol]['bybit']['active']
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
            
            # Show if we have an active position
            position_info = ""
            if (positions[symbol]['binance']['active'] and positions[symbol]['bybit']['active']):
                position_info = "[ACTIVE]"
            
            # Highlight top opportunities with an arrow
            prefix = "→ " if i < max_positions and not (
                positions[symbol]['binance']['active'] and 
                positions[symbol]['bybit']['active']
            ) else "  "
            
            # Format time to funding as countdown timer
            time_to_funding = format_countdown(m['time_to_funding_hours'])
            
            best_data.append([
                prefix + symbol,
                f"{m['funding_spread']*100:+.5f}%",
                f"{m['binance_funding_rate']*100:+.5f}%",
                f"{m['bybit_funding_rate']*100:+.5f}%",
                f"{m['binance_side']}/{m['bybit_side']}",
                time_to_funding,
                f"${m['expected_profit_per_funding']:.4f}",
                f"{m['break_even_events']} events",
                f"{m['apr']*100:.2f}%",
                position_info
            ])
        
        print(tabulate(
            best_data, 
            headers=["Symbol", "Spread", "Binance Rate", "Bybit Rate", "Sides", 
                    "Countdown", "Profit/Funding", "Break Even", "APR", "Position"]
        ))
    else:
        print("\nNo profitable opportunities found at current thresholds.")
    
    # Active positions table
    active_data = []
    for symbol, pos in positions.items():
        binance_pos = pos['binance']
        bybit_pos = pos['bybit']
        
        if binance_pos['active'] and bybit_pos['active']:
            m = metrics.get(symbol, {})
            if not m:
                continue
                
            # Calculate position metrics
            entry_time = binance_pos['entry_time'].strftime('%Y-%m-%d %H:%M:%S')
            duration = (datetime.now() - binance_pos['entry_time']).total_seconds() / 3600  # hours
            
            # Add position info
            active_data.append([
                symbol,
                f"{binance_pos['side']}/{bybit_pos['side']}",
                f"{binance_pos['qty']:.5f}/{bybit_pos['qty']:.5f}",
                f"${m['notional_value']:.2f}",
                entry_time,
                f"{duration:.2f} hrs",
                f"${m['expected_profit_per_funding']:.4f}",
                f"{m['apr']*100:.2f}%"
            ])
    
    if active_data:
        print("\n\nACTIVE POSITIONS:")
        print(tabulate(
            active_data, 
            headers=["Symbol", "Sides", "Quantities", "Notional Value", 
                    "Entry Time", "Duration", "Profit/Funding", "APR"]
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
