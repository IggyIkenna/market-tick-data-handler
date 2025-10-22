# Deribit Options Access Solution

## The Problem
Deribit options are **NOT** accessed as individual option symbols in Tardis.dev. Instead, they are accessed through the `options_chain` data type.

## Current Issue
Our script filters for these data types:
```python
data_types = [
    "trades",
    "book_snapshot_5", 
    "quotes",
    "derivative_ticker",
    "liquidations"
]
```

But Deribit options require:
```python
data_types = [
    "trades",
    "book_snapshot_5", 
    "quotes",
    "derivative_ticker",
    "liquidations",
    "options_chain"  # ← This is missing!
]
```

## Solution

### 1. Update Data Types for Deribit
```python
def get_data_types_for_exchange(exchange):
    """Get appropriate data types for each exchange"""
    base_types = ["trades", "book_snapshot_5", "quotes"]
    
    if exchange == "deribit":
        return base_types + ["derivative_ticker", "liquidations", "options_chain"]
    else:
        return base_types + ["derivative_ticker", "liquidations"]
```

### 2. Access Deribit Options
```python
# Deribit options are accessed via:
# https://datasets.tardis.dev/v1/deribit/options_chain/YYYY/MM/DD.csv.gz

# The options_chain data contains:
# - All available options for that date
# - Strike prices, expiry dates, option types (C/P)
# - Greeks, implied volatility, etc.
```

### 3. Update Our Script
```python
# In check_instrument_availability.py
def get_available_symbols(exchange, start_date, end_date, data_types):
    # ... existing code ...
    
    # Special handling for Deribit options
    if exchange == "deribit" and "options_chain" in data_types:
        # Add the OPTIONS symbol to available symbols
        available_symbols["OPTIONS"] = {
            'type': 'options_chain',
            'available_from': start_date.strftime("%Y-%m-%d"),
            'available_to': end_date.strftime("%Y-%m-%d"),
            'data_types': ['options_chain'],
            'exchange': exchange,
            'note': 'Deribit options accessed via options_chain data type'
        }
```

## Why This Happens

1. **Deribit's API Structure**: Deribit doesn't expose individual option symbols in their API
2. **Tardis Implementation**: Tardis follows Deribit's structure - options are accessed as a chain
3. **Our Filter**: We excluded `options_chain` from our data types

## Benefits of Including Options Chain

1. **Comprehensive Options Data**: Access to all Deribit options for any date
2. **Rich Metadata**: Strike prices, expiry dates, Greeks, IV
3. **Backtesting Support**: Historical options data for strategy testing
4. **Risk Management**: Options data for portfolio risk analysis

## Implementation

### Quick Fix
```python
# Add to our data_types list:
data_types = [
    "trades",
    "book_snapshot_5", 
    "quotes",
    "derivative_ticker",
    "liquidations",
    "options_chain"  # Add this for Deribit
]
```

### Proper Fix
```python
# Exchange-specific data types:
EXCHANGE_DATA_TYPES = {
    "binance": ["trades", "book_snapshot_5", "quotes"],
    "binance-futures": ["trades", "book_snapshot_5", "derivative_ticker", "liquidations", "quotes"],
    "deribit": ["trades", "book_snapshot_5", "quotes", "derivative_ticker", "liquidations", "options_chain"],
    "bybit": ["trades", "book_snapshot_5", "derivative_ticker", "liquidations", "quotes"],
    "bybit-spot": ["trades", "book_snapshot_5", "quotes"],
    "okex": ["trades", "book_snapshot_5", "quotes"],
    "okex-futures": ["trades", "book_snapshot_5", "derivative_ticker", "liquidations", "quotes"],
    "okex-swap": ["trades", "book_snapshot_5", "derivative_ticker", "liquidations", "quotes"],
    "upbit": ["trades", "book_snapshot_5", "quotes"]
}
```

This will give us access to Deribit's extensive options data!
