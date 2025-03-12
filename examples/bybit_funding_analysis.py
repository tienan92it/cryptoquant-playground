import sys
import time
from pathlib import Path
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from tabulate import tabulate
import logging

# Add parent directory to path to import our module
sys.path.append(str(Path(__file__).parent.parent))

from exchanges.bybit.rest_client import BybitRestClient

def plot_funding_history(symbol, history):
    """Plot funding rate history for a symbol"""
    # Convert to DataFrame for easier manipulation
    df = pd.DataFrame(history)
    df['funding_rate'] = df['fundingRate'].astype(float) * 100  # Convert to percentage
    df['timestamp'] = pd.to_datetime(df['fundingRateTimestamp'].astype(int), unit='ms')
    
    # Sort by timestamp
    df = df.sort_values('timestamp')
    
    # Create plot
    plt.figure(figsize=(12, 6))
    plt.plot(df['timestamp'], df['funding_rate'], marker='o', linestyle='-', linewidth=2, markersize=5)
    plt.axhline(y=0, color='r', linestyle='--', alpha=0.3)
    plt.title(f'Funding Rate History for {symbol}')
    plt.xlabel('Date')
    plt.ylabel('Funding Rate (%)')
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Calculate and display statistics
    avg_rate = df['funding_rate'].mean()
    cumulative = df['funding_rate'].sum()
    plt.axhline(y=avg_rate, color='g', linestyle='--', alpha=0.5)
    plt.text(df['timestamp'].iloc[0], avg_rate, f' Avg: {avg_rate:.4f}%', 
             verticalalignment='bottom', color='green')
    
    plt.show()
    
    # Return statistics
    return {
        'avg_rate': avg_rate,
        'min_rate': df['funding_rate'].min(),
        'max_rate': df['funding_rate'].max(),
        'cumulative': cumulative,
        'count': len(df),
        'period_days': (df['timestamp'].iloc[-1] - df['timestamp'].iloc[0]).days + 1
    }

def analyze_top_opportunities(client, top_n=10):
    """Find and analyze top funding opportunities"""
    print("Finding the best funding opportunities across all symbols...")
    opportunities = client.find_best_funding_opportunities(min_threshold=0.00005)
    
    if not opportunities:
        print("No opportunities found above threshold.")
        return
    
    print(f"\nTop {top_n} Funding Rate Opportunities:")
    print("=" * 80)
    
    table_data = []
    for i, opp in enumerate(opportunities[:top_n]):
        symbol = opp['symbol']
        side = opp['side']
        avg_rate = opp['avg_rate'] * 100  # Convert to percentage
        annual_yield = opp['annual_yield'] * 100  # Convert to percentage
        
        # Get funding interval
        interval_mins = client.get_funding_interval(symbol)
        interval_hours = interval_mins / 60
        
        table_data.append([
            i+1,
            symbol,
            side,
            f"{avg_rate:+.6f}%",
            f"{opp['latest_rate']*100:+.6f}%",
            f"{annual_yield:.2f}%",
            f"{interval_hours:.1f}h",
            opp['count']
        ])
    
    headers = ["#", "Symbol", "Side", "Avg Rate", "Latest Rate", "Est. APR", "Interval", "Samples"]
    print(tabulate(table_data, headers=headers, tablefmt="simple"))
    
    return opportunities[:top_n]

if __name__ == "__main__":
    print("Bybit Funding Rate Analysis Tool")
    print("=" * 80)
    
    # Enable more verbose logging
    logging.basicConfig(level=logging.INFO)
    
    # Create REST client
    client = BybitRestClient(testnet=False)
    
    try:
        # Analyze top opportunities
        print("Fetching available symbols...")
        top_opportunities = analyze_top_opportunities(client, top_n=10)
        
        if not top_opportunities:
            sys.exit(0)
        
        # Ask if user wants to plot a specific symbol
        while True:
            print("\nOptions:")
            print("1. Analyze a specific symbol")
            print("2. Plot funding history for one of the top opportunities")
            print("3. Exit")
            
            choice = input("Enter your choice (1-3): ")
            
            if choice == "1":
                symbol_input = input("Enter symbol (e.g., BTCUSDT): ")
                symbol = symbol_input.upper().replace("-", "")  # Format symbol
                
                # Validate the symbol first
                if not client._validate_symbol(symbol):
                    print(f"Error: Symbol {symbol} does not appear to be valid on Bybit.")
                    print("Make sure you're using the correct format (e.g., BTCUSDT for BTC/USDT)")
                    continue
                
                days = int(input("Number of days to analyze (1-30): "))
                days = min(max(1, days), 30)  # Ensure within reasonable range
                
                # Get funding history
                print(f"\nFetching funding history for {symbol} over the past {days} days...")
                end_time = int(time.time() * 1000)
                start_time = end_time - (days * 24 * 60 * 60 * 1000)
                history = client.get_funding_history(symbol, start_time=start_time, end_time=end_time, limit=200)
                
                if not history:
                    print(f"No funding history found for {symbol}.")
                    continue
                
                # Plot and get statistics
                stats = plot_funding_history(symbol, history)
                
                # Display additional statistics
                print(f"\nFunding Rate Statistics for {symbol}:")
                print(f"- Average Rate: {stats['avg_rate']:.6f}%")
                print(f"- Min Rate: {stats['min_rate']:.6f}%")
                print(f"- Max Rate: {stats['max_rate']:.6f}%")
                print(f"- Cumulative (Sum of Rates): {stats['cumulative']:.6f}%")
                print(f"- Number of Funding Events: {stats['count']}")
                print(f"- Period: {stats['period_days']} days")
                
                # Calculate annualized returns
                if stats['period_days'] > 0:
                    # Estimate annual return based on observed rates
                    annual_est = stats['cumulative'] * (365 / stats['period_days'])
                    print(f"- Estimated Annual Yield: {annual_est:.2f}%")
                    
                    # Get funding interval
                    interval = client.get_funding_interval(symbol)
                    print(f"- Funding Interval: {interval} minutes ({interval/60:.1f} hours)")
                
            elif choice == "2":
                if not top_opportunities:
                    print("No top opportunities available.")
                    continue
                
                # Display numbered list of top opportunities
                print("\nSelect a symbol to analyze:")
                for i, opp in enumerate(top_opportunities):
                    print(f"{i+1}. {opp['symbol']} ({opp['side']}): {opp['avg_rate']*100:+.6f}%")
                
                try:
                    selection = int(input("Enter number: "))
                    if selection < 1 or selection > len(top_opportunities):
                        print("Invalid selection.")
                        continue
                        
                    selected = top_opportunities[selection-1]
                    symbol = selected['symbol']
                    
                    # Get 30-day history
                    print(f"\nFetching 30-day funding history for {symbol}...")
                    end_time = int(time.time() * 1000)
                    start_time = end_time - (30 * 24 * 60 * 60 * 1000)
                    history = client.get_funding_history(symbol, start_time=start_time, end_time=end_time, limit=200)
                    
                    if not history:
                        print(f"No funding history found for {symbol}.")
                        continue
                        
                    # Plot and get statistics
                    stats = plot_funding_history(symbol, history)
                    print(f"\nRecommended position: {selected['side']}")
                    
                except ValueError:
                    print("Please enter a valid number.")
                
            elif choice == "3":
                break
            else:
                print("Invalid choice. Please enter 1, 2, or 3.")
    
    except KeyboardInterrupt:
        print("\nAnalysis interrupted by user.")
    except Exception as e:
        print(f"\nError during analysis: {e}")
    finally:
        # Clean up
        client.close()
        print("\nAnalysis complete. Session closed.")
