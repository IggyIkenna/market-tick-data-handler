# Examples

This directory contains essential example scripts and utilities for working with the market data tick handler package.

## Core Examples

### Package Usage
- `package_usage_examples.py` - Comprehensive package usage examples

### Authentication
- `authentication_examples.py` - Different authentication modes (auto, secret_manager, env_vars, mock)
- `secret_manager_example.py` - Secret Manager integration example

### Instrument Discovery
- `inspect_instrument.py` - Inspect instrument definitions from GCS
- `list_instruments.py` - List available instruments
- `tardis_lookup_example.py` - Tardis API integration example

### Processing Examples
- `integration_example.py` - Package integration example
- `live_streaming_processor.py` - Live streaming processor example
- `production_candle_processor.py` - Production candle processing example
- `demo_single_instrument.py` - Single instrument demo
- `single_instrument_processor.py` - Single instrument processing
- `live_streaming_usage_guide.py` - Live streaming usage guide

### Performance Documentation
- `parquet_performance_results.md` - Comprehensive Parquet performance analysis and comparison

## Available Tools

### inspect_instrument.py

A command-line utility to inspect instrument definitions from GCS. This tool allows you to query any instrument ID and get its full attribute details.

#### Usage

```bash
# Basic usage - inspect an instrument for 2023-05-23
python examples/inspect_instrument.py BINANCE-FUTURES:PERPETUAL:SOL-USDT

# Specify a different date
python examples/inspect_instrument.py BINANCE-FUTURES:PERPETUAL:SOL-USDT --date 2023-05-24

# Show summary of all instruments for the date
python examples/inspect_instrument.py BINANCE-FUTURES:PERPETUAL:SOL-USDT --date 2023-05-23 --summary

# Enable verbose logging
python examples/inspect_instrument.py BINANCE-FUTURES:PERPETUAL:SOL-USDT --verbose
```

#### Examples

```bash
# Inspect a Binance Futures perpetual
python examples/inspect_instrument.py BINANCE-FUTURES:PERPETUAL:SOL-USDT

# Inspect a Deribit option
python examples/inspect_instrument.py DERIBIT:OPTION:BTC-USD-241225-50000-CALL

# Inspect a spot pair
python examples/inspect_instrument.py BINANCE:SPOT_PAIR:BTC-USDT

# Inspect a Bybit perpetual
python examples/inspect_instrument.py BYBIT:PERPETUAL:ETH-USDT
```

#### Output

The tool displays comprehensive instrument information including:

- **Core Identification**: Instrument key, venue, instrument type
- **Asset Information**: Base asset, quote asset, settle asset
- **Availability Window**: When the instrument is available
- **Data Types**: What data types are available (trades, book_snapshot_5, etc.)
- **Exchange Mappings**: Raw symbols and Tardis API mappings
- **Metadata**: Data provider, venue type, asset class
- **Trading Parameters**: Contract details, inverse status, etc.
- **CCXT Integration**: CCXT symbol and exchange mappings

#### Requirements

- Valid GCS credentials configured in `.env` file
- Access to the `market-data-tick` GCS bucket
- Python dependencies installed (see main `requirements.txt`)

#### Error Handling

The tool provides helpful error messages and suggestions when:
- Instrument is not found
- Invalid date format
- GCS access issues
- Configuration problems

When an exact match isn't found, it will show similar instruments to help you find the correct instrument ID.

