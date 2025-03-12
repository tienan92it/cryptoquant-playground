"""
Terminal display utilities for formatting DataFrames in the console
"""
import pandas as pd
import os
from tabulate import tabulate
import sys

# ANSI color codes
class Colors:
    RESET = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    BLACK = '\033[30m'
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    MAGENTA = '\033[35m'
    CYAN = '\033[36m'
    WHITE = '\033[37m'
    BRIGHT_BLACK = '\033[90m'
    BRIGHT_RED = '\033[91m'
    BRIGHT_GREEN = '\033[92m'
    BRIGHT_YELLOW = '\033[93m'
    BRIGHT_BLUE = '\033[94m'
    BRIGHT_MAGENTA = '\033[95m'
    BRIGHT_CYAN = '\033[96m'
    BRIGHT_WHITE = '\033[97m'
    
    # Background colors
    BG_BLACK = '\033[40m'
    BG_RED = '\033[41m'
    BG_GREEN = '\033[42m'
    BG_YELLOW = '\033[43m'
    BG_BLUE = '\033[44m'
    BG_MAGENTA = '\033[45m'
    BG_CYAN = '\033[46m'
    BG_WHITE = '\033[47m'

def is_color_supported():
    """Check if the terminal supports colors"""
    # Check if output is redirected to a file
    if not sys.stdout.isatty():
        return False
    
    # Check for NO_COLOR environment variable
    if 'NO_COLOR' in os.environ:
        return False
    
    # Check for TERM environment variable
    term = os.environ.get('TERM', '')
    if term == 'dumb':
        return False
        
    # Most terminals support colors these days
    return True

def format_with_color(value, format_spec='', color=None, condition=None):
    """
    Format a value with color based on a condition.
    
    Args:
        value: The value to format
        format_spec: Format specification string (e.g., '.2f')
        color: Color to use if condition is True
        condition: Function that takes value and returns True/False
        
    Returns:
        Formatted string with color codes if supported
    """
    if not is_color_supported() or color is None:
        return f"{value:{format_spec}}"
        
    if condition is None or condition(value):
        return f"{color}{value:{format_spec}}{Colors.RESET}"
    else:
        return f"{value:{format_spec}}"

def format_exchange_comparison_table(df):
    """
    Format a DataFrame for display in the terminal with colors and borders.
    
    Args:
        df: Pandas DataFrame with exchange comparison data
        
    Returns:
        Formatted string ready for terminal display
    """
    # Make a copy to avoid modifying the original
    formatted_df = df.copy()
    
    # Format numeric columns
    if is_color_supported():
        # Apply colors and formatting to specific columns
        for col in formatted_df.columns:
            if col == 'Symbol':
                formatted_df[col] = formatted_df[col].apply(
                    lambda x: f"{Colors.BOLD}{x}{Colors.RESET}"
                )
            elif 'Bid' in col:
                formatted_df[col] = formatted_df[col].apply(
                    lambda x: f"${x:.2f}"
                )
            elif 'Ask' in col:
                formatted_df[col] = formatted_df[col].apply(
                    lambda x: f"${x:.2f}"
                )
            elif col == 'Mid Diff':
                formatted_df[col] = formatted_df[col].apply(
                    lambda x: f"{Colors.GREEN}${x:.4f}{Colors.RESET}" if x > 0 else 
                             (f"{Colors.RED}${x:.4f}{Colors.RESET}" if x < 0 else f"${x:.4f}")
                )
            elif col == 'Mid Diff (bps)':
                formatted_df[col] = formatted_df[col].apply(
                    lambda x: f"{Colors.GREEN}{x:.2f}{Colors.RESET}" if x > 0 else 
                             (f"{Colors.RED}{x:.2f}{Colors.RESET}" if x < 0 else f"{x:.2f}")
                )
            elif col == 'Arbitrage':
                formatted_df[col] = formatted_df[col].apply(
                    lambda x: f"{Colors.BG_GREEN}{Colors.BLACK} {x} {Colors.RESET}" if x == 'Yes' else x
                )
            elif col == 'Direction':
                formatted_df[col] = formatted_df[col].apply(
                    lambda x: f"{Colors.BOLD}{x}{Colors.RESET}" if x != '-' else x
                )
            elif col == 'Profit (bps)':
                formatted_df[col] = formatted_df[col].apply(
                    lambda x: f"{Colors.BOLD}{Colors.GREEN}{x:.2f}{Colors.RESET}" if x > 0 else f"{x:.2f}"
                )
    else:
        # Basic formatting without colors
        for col in formatted_df.columns:
            if 'Bid' in col or 'Ask' in col:
                formatted_df[col] = formatted_df[col].apply(lambda x: f"${x:.2f}")
            elif col == 'Mid Diff':
                formatted_df[col] = formatted_df[col].apply(lambda x: f"${x:.4f}")
            elif col == 'Mid Diff (bps)' or col == 'Profit (bps)':
                formatted_df[col] = formatted_df[col].apply(lambda x: f"{x:.2f}")
    
    # Format using tabulate with a nice grid
    table = tabulate(
        formatted_df, 
        headers=formatted_df.columns,
        tablefmt="grid",
        showindex=False
    )
    
    return table

def print_comparison_header(timestamp):
    """Print a header for the comparison table"""
    width = os.get_terminal_size().columns
    header = f" Exchange Comparison - {timestamp} "
    
    if is_color_supported():
        padding = "=" * ((width - len(header)) // 2)
        print(f"{Colors.BLUE}{padding}{Colors.BOLD}{header}{Colors.RESET}{Colors.BLUE}{padding}{Colors.RESET}")
    else:
        padding = "=" * ((width - len(header)) // 2)
        print(f"{padding}{header}{padding}")

def print_arbitrage_alerts(comparison_data):
    """Print alerts for arbitrage opportunities"""
    if not is_color_supported():
        for row in comparison_data:
            if row.get('Arbitrage') == 'Yes':
                symbol = row.get('Symbol', '')
                direction = row.get('Direction', '')
                profit = row.get('Profit (bps)', 0)
                print(f"!!! ARBITRAGE OPPORTUNITY: {symbol} - {direction} - Profit: {profit:.2f} bps !!!")
    else:
        for row in comparison_data:
            if row.get('Arbitrage') == 'Yes':
                symbol = row.get('Symbol', '')
                direction = row.get('Direction', '')
                profit = row.get('Profit (bps)', 0)
                print(f"{Colors.BG_YELLOW}{Colors.BLACK}!!! ARBITRAGE OPPORTUNITY: {symbol} - {direction} - Profit: {profit:.2f} bps !!!{Colors.RESET}")
