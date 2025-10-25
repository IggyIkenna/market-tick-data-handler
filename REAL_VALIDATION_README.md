# Real Validation Framework

This implements the **Three Rules Validation System** using actual Tardis API, Google Cloud Storage, and Binance data as specified in the documentation.

## 🔍 Three Rules Validation System

Based on the market-data-handler repository, this validates:

1. **Timestamp Alignment Rule** - Ensures timestamps align between sources
2. **OHLC Preservation Rule** - Validates OHLC values are preserved correctly  
3. **Volume Consistency Rule** - Checks volume consistency between sources

## 🚀 Quick Start

### 1. Set Environment Variables

```bash
# Required API keys
export TARDIS_API_KEY='TD.your_actual_tardis_key'
export GCS_BUCKET='your_actual_bucket_name'
export BINANCE_API_KEY='your_actual_binance_key'
export BINANCE_SECRET_KEY='your_actual_binance_secret'

# Google Cloud (already set)
export GCP_PROJECT_ID='central-element-323112'
export GCP_CREDENTIALS_PATH='/workspace/central-element-323112-e35fb0ddafe2.json'
export USE_SECRET_MANAGER='false'
```

### 2. Test Environment

```bash
# Test that everything is set up correctly
python3 test_real_validation.py
```

### 3. Run Real Validation

```bash
# Validate BTC-USDT across all timeframes
python3 real_validation.py

# Validate specific symbol
python3 real_validation.py --symbol ETH-USDT

# Validate specific timeframes
python3 real_validation.py --timeframes 1m,5m,1h

# Validate last 6 hours
python3 real_validation.py --hours 6

# Check configuration only
python3 real_validation.py --dry-run
```

## 📊 What It Validates

### Cross-Source Data Validation
- Compares Binance CCXT data with Tardis-derived data
- Validates across all timeframes (1m to 24h)
- Ensures data consistency between sources

### Timestamp Validation
- Checks timestamp alignment between sources
- Validates timestamp stability within data
- Ensures proper interval spacing

### Aggregation Validation
- Validates that aggregated timeframes match direct retrieval
- Tests 1m → 5m, 1m → 1h, 1h → 1d aggregation
- Ensures OHLC preservation during aggregation

### Streaming Integration
- Real-time validation of streaming data
- Buffered validation for performance
- Integration with existing streaming architecture

## 🔧 Configuration

The validation framework uses the existing configuration system:

- **Tardis API**: For historical tick data
- **Google Cloud Storage**: For processed candle data
- **Binance API**: For live market data via CCXT
- **Validation Tolerances**: Configurable precision levels

## 📈 Output

The validation provides detailed results:

```
🔍 Real Three Rules Validation System
==================================================
This uses actual Tardis API, Google Cloud Storage, and Binance data
==================================================

📊 Validating BTC-USDT across timeframes: ['1m', '5m', '15m', '1h', '4h', '1d']
📅 Date range: 2024-01-15 10:00:00+00:00 to 2024-01-16 10:00:00+00:00

📋 Detailed Results:
--------------------------------------------------

1️⃣ Timestamp Alignment Rule:
   ✅ 1m: PASS - Timestamp alignment validated for 1m
   ✅ 5m: PASS - Timestamp alignment validated for 5m
   ✅ 1h: PASS - Timestamp alignment validated for 1h

2️⃣ OHLC Preservation Rule:
   ✅ 1m: PASS - OHLC preservation validated for 1m
   ✅ 5m: PASS - OHLC preservation validated for 5m
   ✅ 1h: PASS - OHLC preservation validated for 1h

3️⃣ Volume Consistency Rule:
   ✅ 1m: PASS - Volume consistency validated for 1m
   ✅ 5m: PASS - Volume consistency validated for 5m
   ✅ 1h: PASS - Volume consistency validated for 1h

4️⃣ Aggregation Consistency Rule:
   ✅ 1m_to_5m: PASS - Aggregation consistency validated from 1m to 5m
   ✅ 1m_to_1h: PASS - Aggregation consistency validated from 1m to 1h

📊 Summary:
   Total Tests: 12
   Passed: 12
   Failed: 0
   Warnings: 0
   Success Rate: 100.0%

🎉 Three Rules Validation completed successfully!
```

## 🏗️ Architecture

The validation framework integrates with the existing architecture:

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Binance API   │    │  Tardis API      │    │  Google Cloud   │
│   (via CCXT)    │    │  (Historical)    │    │  Storage        │
└─────────┬───────┘    └─────────┬────────┘    └─────────┬───────┘
          │                      │                       │
          ▼                      ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                Three Rules Validator                           │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐              │
│  │ Timestamp   │ │ OHLC        │ │ Volume      │              │
│  │ Alignment   │ │ Preservation│ │ Consistency │              │
│  └─────────────┘ └─────────────┘ └─────────────┘              │
└─────────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────┐
│                Validation Results                               │
│  • Cross-source consistency                                    │
│  • Timestamp alignment                                         │
│  • OHLC preservation                                           │
│  • Volume consistency                                          │
│  • Aggregation validation                                      │
└─────────────────────────────────────────────────────────────────┘
```

## 🔗 Integration

This validation framework integrates with:

- **Existing Streaming Architecture**: Real-time validation of streaming data
- **Data Downloader**: Uses existing Tardis and GCS connectors
- **Configuration System**: Uses existing config management
- **Logging System**: Integrates with existing logging infrastructure

## 📚 Documentation

- [Validation Framework Overview](docs/VALIDATION_FRAMEWORK_README.md)
- [Streaming Integration](docs/STREAMING_VALIDATION_INTEGRATION.md)
- [Testing Analysis](docs/TESTING_ANALYSIS.md)

## 🧪 Testing

The framework includes comprehensive testing:

```bash
# Test environment setup
python3 test_real_validation.py

# Run validation tests
python3 real_validation.py --dry-run

# Full validation
python3 real_validation.py
```

## 🎯 Key Features

- **Real Data**: Uses actual Tardis API, GCS, and Binance data
- **Three Rules**: Implements the proven validation system from market-data-handler
- **Cross-Source**: Validates consistency between different data sources
- **Multi-Timeframe**: Tests all timeframes from 1m to 24h
- **Aggregation**: Validates aggregation consistency
- **Streaming**: Real-time validation integration
- **Configurable**: Adjustable tolerances and parameters
- **Comprehensive**: Detailed reporting and error analysis
