# Cross-Exchange Funding Arbitrage

A sophisticated cryptocurrency arbitrage system that exploits funding rate differentials across multiple exchanges. The system monitors funding rates on Binance, Bybit, and OKX simultaneously, identifying profitable arbitrage opportunities to generate passive income.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Strategy Details](#strategy-details)
- [Metrics Explained](#metrics-explained)
- [Risk Management](#risk-management)
- [Debugging Tools](#debugging-tools)
- [Contributing](#contributing)

## Overview

The Cross-Exchange Funding Arbitrage system leverages the fact that perpetual futures contracts on different exchanges often have different funding rates. By opening opposing positions (long and short) on different exchanges, traders can maintain a market-neutral position while earning the difference in funding rates.

This system:
1. Monitors funding rates across Binance, Bybit, and OKX exchanges in real-time
2. Identifies the largest funding rate discrepancies
3. Executes market-neutral arbitrage positions
4. Manages positions to optimize profitability

## Features

- **Real-time Monitoring**: WebSocket connections to multiple exchanges for up-to-date funding rates
- **Multi-Exchange Support**: Works with Binance, Bybit, and OKX with a unified API abstraction
- **Intelligent Arbitrage**: Automatically finds the most profitable exchange pairs
- **Position Management**: Tracks and manages positions with smart entry/exit logic
- **Interactive Terminal UI**: Visualizes opportunities and active positions
- **Risk Management**: Configurable risk parameters and position limits
- **Automatic Symbol Mapping**: Maps symbols across exchanges automatically
- **Failover Mechanisms**: Falls back to REST API when WebSocket connections fail

## Project Structure

```
/realtime-prices/
├── exchanges/
│   ├── binance/
│   │   └── ws_client.py           # Binance WebSocket client
│   ├── bybit/
│   │   ├── rest_client.py         # Bybit REST API client
│   │   └── ws_client.py           # Bybit WebSocket client
│   └── okx/
│       ├── rest_client.py         # OKX REST API client
│       └── ws_client.py           # OKX WebSocket client
├── strategies/
│   ├── cross_exchange_funding_arbitrage.py  # Main arbitrage strategy
│   └── config.json                # Strategy configuration
├── utils/
│   ├── config_loader.py           # Configuration loading utilities
│   ├── display_utils.py           # Terminal display utilities
│   ├── exchange_utils.py          # Exchange data utilities
│   ├── metrics_calculator.py      # Arbitrage metrics calculation
│   ├── position_manager.py        # Position management utilities
│   └── ws_manager.py              # WebSocket connection management
└── examples/
    ├── bybit_funding_example.py   # Bybit funding rate example
    ├── okx_instruments_example.py # OKX instrument verification
    └── symbol_mapping_debug.py    # Symbol mapping debugging utilities
```

## Installation

### Prerequisites

- Python 3.8 or higher
- pip package manager

### Setup

1. Clone the repository:
```bash
git clone https://github.com/yourusername/realtime-prices.git
cd realtime-prices
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure your API keys (see [Configuration](#configuration) section)

## Configuration

The system is configured using the `strategies/config.json` file. Here's an explanation of the key parameters:

```json
{
  "symbols": ["btcusdt", "ethusdt", "solusdt"],   // Specific symbols to monitor
  "use_all_symbols": true,                        // Use all available symbols
  "position_size_usd": 1000,                      // Size of each position in USD
  "min_funding_spread": 0.0005,                   // Minimum funding rate difference (0.05%)
  "futures_fee_rate": 0.0004,                     // Binance fee rate
  "bybit_fee_rate": 0.0006,                       // Bybit fee rate
  "okx_fee_rate": 0.0005,                         // OKX fee rate
  "slippage": 0.0003,                             // Estimated slippage
  "check_interval": 10,                           // Time between checks (seconds)
  "exchanges": ["binance", "bybit", "okx"],       // Exchanges to use
  
  "symbol_filters": {
    "min_price": 0.1,                             // Minimum price filter
    "min_volume_usd": 1000000,                    // Minimum 24h volume
    "exclude": ["BTCDOMUSDT", "DEFIUSDT"],        // Symbols to exclude
    "include_only": []                            // If specified, only include these
  },
  
  "api_key": "YOUR_API_KEY_HERE",                 // Exchange API key (optional for demo mode)
  "api_secret": "YOUR_API_SECRET_HERE",           // Exchange API secret (optional for demo mode)
  
  "risk_management": {
    "max_positions": 5,                           // Maximum open positions
    "max_drawdown": 0.05,                         // Max allowed drawdown
    "stop_loss_pct": 0.02                         // Stop-loss percentage
  }
}
```

## Usage

### Running the Strategy

To run the cross-exchange funding arbitrage strategy:

```bash
python strategies/cross_exchange_funding_arbitrage.py
```

### Demo Mode (No Trading)

By default, the system operates in "demo mode" without executing real trades. To enable live trading, you'll need to:

1. Add your exchange API keys to the configuration
2. Modify the placement functions in `position_manager.py` to connect to exchange APIs
3. Set appropriate risk parameters

### Example Output

When running, the terminal will display:

```
================================================================================
 CROSS-EXCHANGE FUNDING ARBITRAGE MONITOR - 2023-04-20 14:30:45
================================================================================

Active Positions: 2/5

BEST CROSS-EXCHANGE FUNDING OPPORTUNITIES:
Symbol    Best Pair      Spread    Binance   Bybit     OKX      Countdown    Profit/Fund    Break Even    APR      Position
--------  ------------  ---------  --------  --------  -------  -----------  -------------  -----------  ------  ----------
→ BTCUSDT  BINANCE-BYBIT  +0.0273%  +0.0100%  -0.0173%  -0.0120%  03:45:16     $0.2730        3.6         89.74%
→ ETHUSDT  BINANCE-OKX    +0.0230%  +0.0100%  -0.0120%  -0.0130%  03:45:16     $0.2300        4.3         75.63%
  SOLUSDT  BYBIT-OKX      +0.0210%  -0.0050%  +0.0160%  -0.0050%  04:15:32     $0.2100        4.7         69.48%  [BYBIT+OKX]
  BNBUSDT  BYBIT-OKX      +0.0195%  +0.0050%  +0.0145%  -0.0050%  03:45:16     $0.1950        5.1         63.98%  [BYBIT+OKX]

ACTIVE POSITIONS:
Symbol    Exchanges           Notional Value  Entry Time            Duration    Profit/Fund    APR
--------  -----------------  ---------------  -------------------  ----------  -------------  ------
SOLUSDT   BYBIT SHORT, OKX LONG      $1000.00  2023-04-20 10:15:23    4.25 hrs        $0.2100  69.48%
BNBUSDT   BYBIT SHORT, OKX LONG      $1000.00  2023-04-20 12:30:15    2.00 hrs        $0.1950  63.98%

================================================================================
Monitoring 34 symbols - 15 profitable opportunities
================================================================================

WebSocket Status: Binance: ✓ Connected | Bybit: ✓ Connected | OKX: ✓ Connected
```

## Strategy Details

The cross-exchange funding arbitrage strategy works as follows:

1. **Data Collection**: The system connects to WebSocket APIs of multiple exchanges to get real-time funding rates.

2. **Opportunity Identification**: For each symbol, it calculates the funding rate differential between all exchange pairs and selects the pair with the largest spread.

3. **Profitability Analysis**: The system calculates:
   - Expected profit from funding rate spread
   - Trading costs (fees, slippage)
   - Break-even number of funding periods
   - Annualized returns (APR)

4. **Position Management**:
   - Opens positions on the best opportunities
   - Monitors active positions for changes in funding rates
   - Closes positions when the spread becomes unfavorable or a better opportunity arises

## Metrics Explained

- **Funding Spread**: The difference in funding rates between two exchanges
- **Profit/Fund**: Expected profit per funding period (typically 8 hours)
- **Break Even**: Number of funding periods needed to recover trading costs
- **APR**: Annualized percentage return (assumes reinvestment)

## Risk Management

The strategy includes several risk management features:

- **Position Limits**: Maximum number of concurrent positions
- **Spread Threshold**: Minimum required spread to enter a position
- **Auto-close Logic**: Close positions when spread decreases significantly
- **Exchange Diversification**: Positions spread across multiple exchanges

## Debugging Tools

The project includes utility scripts to help with debugging:

### Symbol Mapping Debug Tool

This tool helps diagnose symbol availability issues across exchanges:

```bash
python examples/symbol_mapping_debug.py
```

### OKX Instrument Verification

Verify OKX instrument information and WebSocket subscription formats:

```bash
python examples/okx_instruments_example.py
```

## Contributing

Contributions to improve the system are welcome. Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Commit your changes (`git commit -m 'Add some amazing feature'`)
5. Push to the branch (`git push origin feature/amazing-feature`)
6. Open a Pull Request

## Disclaimer

This software is for educational and research purposes only. Use it at your own risk. Always test thoroughly with small amounts before using with significant capital.

---

**Note**: This strategy relies on the existence of funding rate differentials between exchanges. These opportunities may change or disappear over time as markets become more efficient.