# Validation Framework

A comprehensive validation framework for the market tick data handler, providing robust testing and validation capabilities for data integrity, timestamp stability, and aggregation consistency.

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Run All Tests

```bash
python run_validation_tests.py --test-type all
```

### 3. Run Specific Tests

```bash
# Cross-source validation (Binance vs Tardis)
python run_validation_tests.py --test-type cross-source --symbol BTC-USDT --timeframe 1m

# Timestamp validation
python run_validation_tests.py --test-type timestamp --data-file data.parquet

# Aggregation validation
python run_validation_tests.py --test-type aggregation --base-file 1m.parquet --agg-file 5m.parquet
```

## 📁 Framework Structure

```
src/validation/
├── __init__.py                 # Package initialization
├── validation_results.py       # Result structures and enums
├── cross_source_validator.py   # Binance vs Tardis validation
├── timestamp_validator.py      # Timestamp stability validation
└── aggregation_validator.py    # Aggregation consistency validation

tests/
├── __init__.py
├── test_cross_source_validation.py
├── test_timestamp_validation.py
└── test_aggregation_validation.py

examples/
├── compare_binance_vs_tardis.py
├── validate_timestamp_stability.py
└── validate_aggregation_consistency.py
```

## 🔧 Core Components

### 1. Cross-Source Validator

Validates data consistency between Binance (via CCXT) and Tardis-derived candles.

**Key Features:**
- Three Rules Validation System
- Timestamp alignment verification
- OHLC preservation checks
- Volume consistency validation
- Aggregation consistency testing

**Usage:**
```python
from src.validation.cross_source_validator import CrossSourceValidator

validator = CrossSourceValidator(data_client, tardis_connector)
result = await validator.validate_timeframe_consistency(
    symbol="BTC-USDT",
    timeframe="1m",
    start_date=start_time,
    end_date=end_time
)
```

### 2. Timestamp Validator

Ensures timestamp stability and consistency across data sources.

**Key Features:**
- Timestamp ordering validation
- Interval consistency checks
- Duplicate detection
- Gap analysis
- Timezone consistency
- Data freshness validation
- Clock synchronization checks

**Usage:**
```python
from src.validation.timestamp_validator import TimestampValidator

validator = TimestampValidator()
result = validator.validate_timestamp_stability(
    timestamps=timestamps,
    expected_interval_seconds=60.0
)
```

### 3. Aggregation Validator

Validates the correctness of candle aggregation across timeframes.

**Key Features:**
- OHLC preservation validation
- Volume aggregation verification
- Timeframe boundary checks
- Aggregation consistency testing

**Usage:**
```python
from src.validation.aggregation_validator import AggregationValidator

validator = AggregationValidator()
result = validator.validate_aggregation_consistency(
    source_candles=base_candles,
    target_candles=aggregated_candles
)
```

## 📊 Validation Results

### ValidationStatus Enum
- `PASS`: Test passed successfully
- `FAIL`: Test failed with errors
- `WARNING`: Test passed with warnings
- `SKIP`: Test was skipped

### ValidationResult
Individual test result containing:
- Test name and status
- Execution time
- Message and details
- Timestamp information

### ValidationReport
Aggregated report containing:
- Multiple validation results
- Summary statistics
- Success rate calculation
- Export capabilities (JSON, HTML, CSV)

## 🧪 Testing

### Unit Tests

Run all unit tests:
```bash
python -m pytest tests/ -v
```

Run specific test categories:
```bash
# Cross-source tests
python -m pytest tests/test_cross_source_validation.py -v

# Timestamp tests
python -m pytest tests/test_timestamp_validation.py -v

# Aggregation tests
python -m pytest tests/test_aggregation_validation.py -v
```

### Integration Tests

Run integration tests with real data:
```bash
# Cross-source integration
python examples/compare_binance_vs_tardis.py --symbol BTC-USDT --timeframe 1m

# Timestamp integration
python examples/validate_timestamp_stability.py --data-file data.parquet

# Aggregation integration
python examples/validate_aggregation_consistency.py --base-file 1m.parquet --agg-file 5m.parquet
```

## 📈 Example Scripts

### 1. Compare Binance vs Tardis

```bash
# Live data comparison
python examples/compare_binance_vs_tardis.py --symbol BTC-USDT --timeframe 1m

# Historical data comparison
python examples/compare_binance_vs_tardis.py \
    --symbol BTC-USDT \
    --timeframe 1m \
    --start-date 2024-01-01 \
    --end-date 2024-01-02 \
    --enable-aggregation \
    --enable-timestamp-validation
```

### 2. Validate Timestamp Stability

```bash
# Basic timestamp validation
python examples/validate_timestamp_stability.py --data-file data.parquet

# Advanced validation with custom tolerances
python examples/validate_timestamp_stability.py \
    --data-file data.parquet \
    --max-gap-seconds 300 \
    --max-drift-seconds 5 \
    --enable-freshness-check
```

### 3. Validate Aggregation Consistency

```bash
# Single timeframe validation
python examples/validate_aggregation_consistency.py \
    --base-file 1m.parquet \
    --agg-file 5m.parquet

# Multiple timeframes validation
python examples/validate_aggregation_consistency.py \
    --base-file 1m.parquet \
    --agg-files 5m.parquet 15m.parquet 1h.parquet
```

## ⚙️ Configuration

### Environment Variables

```bash
# GCP Configuration
export GCP_PROJECT_ID="your-project-id"
export GCP_BUCKET="your-bucket-name"
export GOOGLE_APPLICATION_CREDENTIALS="path/to/credentials.json"

# Tardis Configuration
export TARDIS_API_KEY="your-tardis-api-key"

# Binance Configuration
export BINANCE_API_KEY="your-binance-api-key"
export BINANCE_SECRET_KEY="your-binance-secret-key"
```

### Test Configuration

Create a custom test configuration file:

```json
{
    "cross_source": {
        "max_candles": 1000,
        "tolerance_pct": 0.01,
        "timeout_seconds": 30
    },
    "timestamp": {
        "max_gap_seconds": 300,
        "max_drift_seconds": 5,
        "freshness_hours": 1
    },
    "aggregation": {
        "tolerance_pct": 0.001,
        "volume_tolerance_pct": 0.01
    }
}
```

Use with test runner:
```bash
python run_validation_tests.py --test-type all --config test_config.json
```

## 📋 Validation Rules

### Three Rules Validation System

1. **Timestamp Alignment**: Candles from different sources must have aligned timestamps
2. **OHLC Preservation**: OHLC values must be preserved during aggregation
3. **Volume Consistency**: Volume must be correctly summed during aggregation

### Timestamp Validation Rules

1. **Ordering**: Timestamps must be in ascending order
2. **Intervals**: Timestamps must follow expected intervals
3. **Uniqueness**: No duplicate timestamps allowed
4. **Gaps**: Gaps must be within acceptable limits
5. **Timezone**: All timestamps must be in UTC
6. **Freshness**: Data must be recent enough

### Aggregation Validation Rules

1. **OHLC Preservation**: 
   - Open = First candle's open
   - High = Maximum high value
   - Low = Minimum low value
   - Close = Last candle's close
2. **Volume Aggregation**: Volume = Sum of all volumes
3. **Timeframe Boundaries**: Aggregated candles must align with timeframe boundaries

## 🚨 Error Handling

### Common Issues

1. **Missing Data**: Handle missing candles gracefully
2. **Network Timeouts**: Implement retry logic
3. **Rate Limiting**: Respect API rate limits
4. **Data Format Issues**: Validate data format before processing

### Error Recovery

```python
try:
    result = await validator.validate_timeframe_consistency(...)
except ValidationError as e:
    logger.error(f"Validation failed: {e}")
    # Implement recovery logic
except Exception as e:
    logger.error(f"Unexpected error: {e}")
    # Implement fallback logic
```

## 📊 Monitoring and Alerting

### Metrics

- Validation success rate
- Test execution time
- Data quality scores
- Error rates by category

### Alerts

- Failed validations
- Data quality degradation
- Performance issues
- System errors

## 🔄 Continuous Integration

### GitHub Actions

```yaml
name: Validation Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: 3.9
      - name: Install dependencies
        run: pip install -r requirements.txt
      - name: Run validation tests
        run: python run_validation_tests.py --test-type unit
```

## 📚 API Reference

### CrossSourceValidator

```python
class CrossSourceValidator:
    async def validate_timeframe_consistency(
        self,
        symbol: str,
        timeframe: str,
        start_date: datetime,
        end_date: datetime,
        max_candles: int = 1000
    ) -> ValidationResult
    
    async def validate_aggregation_consistency(
        self,
        symbol: str,
        base_timeframe: str,
        target_timeframe: str,
        start_date: datetime,
        end_date: datetime
    ) -> ValidationResult
```

### TimestampValidator

```python
class TimestampValidator:
    def validate_timestamp_stability(
        self,
        timestamps: List[datetime],
        expected_interval_seconds: float,
        test_name: str = "timestamp_stability"
    ) -> ValidationResult
    
    def validate_timezone_consistency(
        self,
        timestamps: List[datetime],
        test_name: str = "timezone_consistency"
    ) -> ValidationResult
```

### AggregationValidator

```python
class AggregationValidator:
    def validate_aggregation_consistency(
        self,
        source_candles: List[OHLCV],
        target_candles: List[OHLCV],
        test_name: str = "aggregation_consistency"
    ) -> ValidationResult
    
    def validate_ohlc_preservation(
        self,
        base_candles: List[OHLCV],
        aggregated_candles: List[OHLCV],
        test_name: str = "ohlc_preservation"
    ) -> ValidationResult
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For questions and support:
- Create an issue on GitHub
- Check the documentation
- Review existing test cases
- Contact the development team