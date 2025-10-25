# Streaming Validation Integration

Integration between the validation framework and the unified streaming architecture, providing real-time validation capabilities for streaming data.

## 🎯 Overview

The streaming validation integration provides:

- **Real-time validation** of streaming candles as they are processed
- **Cross-source validation** against external data sources (Binance)
- **Timestamp stability validation** for streaming data
- **Aggregation consistency validation** across timeframes
- **Performance monitoring** and error handling
- **Callback system** for validation results

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                Streaming Validation Architecture              │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Unified Streaming Architecture                             │
│  ├─ Node.js Ingestion Layer                                │
│  │   ├─ live_tick_streamer.js                              │
│  │   └─ WebSocket data from Tardis.dev                     │
│  │                                                         │
│  ├─ Python Processing Layer                                │
│  │   ├─ streaming_service/                                 │
│  │   │   ├─ tick_processor/                                │
│  │   │   ├─ candle_processor/                              │
│  │   │   └─ hft_features/                                  │
│  │   └─ Validation Integration                             │
│  │       ├─ StreamingValidator                             │
│  │       ├─ StreamingServiceIntegration                    │
│  │       └─ Validation Callbacks                           │
│  │                                                         │
│  └─ Validation Framework                                   │
│      ├─ CrossSourceValidator                               │
│      ├─ TimestampValidator                                 │
│      └─ AggregationValidator                               │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## 🔧 Components

### 1. StreamingValidator

Core validator for streaming data with real-time capabilities.

**Key Features:**
- Real-time candle validation
- Configurable validation intervals
- Candle buffering for batch validation
- Cross-source validation integration
- Timestamp stability validation
- Aggregation consistency validation

**Usage:**
```python
from src.validation.streaming_validator import StreamingValidator, StreamingValidationConfig

# Create configuration
config = StreamingValidationConfig(
    validation_interval_seconds=60,
    max_candles_per_validation=100,
    enable_cross_source_validation=True,
    enable_timestamp_validation=True,
    enable_aggregation_validation=True
)

# Create validator
validator = StreamingValidator(
    cross_source_validator=cross_source_validator,
    timestamp_validator=timestamp_validator,
    aggregation_validator=aggregation_validator,
    config=config
)

# Validate single candle
result = await validator.validate_streaming_candle(
    candle=candle,
    symbol="BTC-USDT",
    timeframe="1m"
)
```

### 2. StreamingServiceIntegration

Integration layer between streaming service and validation framework.

**Key Features:**
- Async validation processing
- Error handling and recovery
- Callback system for results
- Performance monitoring
- Queue-based processing

**Usage:**
```python
from src.validation.streaming_integration import StreamingServiceIntegration, StreamingIntegrationConfig

# Create integration
config = StreamingIntegrationConfig(
    enable_real_time_validation=True,
    validation_interval_seconds=60,
    enable_async_validation=True
)

integration = StreamingServiceIntegration(streaming_validator, config)

# Start integration
await integration.start()

# Validate candle
result = await integration.validate_candle(
    candle=candle,
    symbol="BTC-USDT",
    timeframe="1m"
)

# Stop integration
await integration.stop()
```

### 3. StreamingServiceValidator

High-level validator interface for the streaming service.

**Key Features:**
- Simple interface for streaming service
- Automatic configuration
- Built-in error handling
- Statistics tracking

**Usage:**
```python
from src.validation.streaming_integration import StreamingServiceValidator

# Create validator
validator = StreamingServiceValidator(
    cross_source_validator=cross_source_validator,
    timestamp_validator=timestamp_validator,
    aggregation_validator=aggregation_validator
)

# Start validator
await validator.start()

# Validate candle
result = await validator.validate_candle(candle, "BTC-USDT", "1m")

# Get statistics
stats = validator.get_stats()
```

## 🚀 Integration with Streaming Service

### 1. Basic Integration

```python
# In streaming service
from src.validation.streaming_integration import StreamingServiceValidator

class LiveCandleProcessor:
    def __init__(self):
        self.validator = StreamingServiceValidator(
            cross_source_validator=cross_source_validator,
            timestamp_validator=timestamp_validator,
            aggregation_validator=aggregation_validator
        )
    
    async def start(self):
        await self.validator.start()
    
    async def process_candle(self, candle):
        # Validate candle
        validation_result = await self.validator.validate_candle(
            candle, candle.symbol, candle.timeframe
        )
        
        if validation_result.status == ValidationStatus.FAIL:
            logger.error(f"Validation failed: {validation_result.message}")
            return None
        
        # Process candle
        processed_candle = self.process_candle_logic(candle)
        return processed_candle
    
    async def stop(self):
        await self.validator.stop()
```

### 2. Advanced Integration with Callbacks

```python
class AdvancedLiveCandleProcessor:
    def __init__(self):
        self.validator = StreamingServiceValidator(...)
        self.setup_validation_callbacks()
    
    def setup_validation_callbacks(self):
        # Add validation result callback
        self.validator.add_validation_callback(self.on_validation_result)
        
        # Add error callback
        self.validator.add_error_callback(self.on_validation_error)
    
    def on_validation_result(self, result: ValidationResult):
        """Handle validation results"""
        if result.status == ValidationStatus.FAIL:
            # Send alert
            self.send_alert(f"Validation failed: {result.message}")
        elif result.status == ValidationStatus.WARNING:
            # Log warning
            logger.warning(f"Validation warning: {result.message}")
    
    def on_validation_error(self, error: Exception, failure_count: int):
        """Handle validation errors"""
        logger.error(f"Validation error #{failure_count}: {error}")
        
        if failure_count >= 5:
            # Too many failures, disable validation temporarily
            self.disable_validation()
```

### 3. Batch Validation

```python
class BatchCandleProcessor:
    def __init__(self):
        self.validator = StreamingServiceValidator(...)
        self.candle_batch = []
    
    async def add_candle(self, candle):
        """Add candle to batch"""
        self.candle_batch.append(candle)
        
        # Validate batch when full
        if len(self.candle_batch) >= 100:
            await self.validate_batch()
    
    async def validate_batch(self):
        """Validate batch of candles"""
        if not self.candle_batch:
            return
        
        # Get symbol and timeframe from first candle
        symbol = self.candle_batch[0].symbol
        timeframe = self.candle_batch[0].timeframe
        
        # Validate batch
        report = await self.validator.validate_candle_batch(
            candles=self.candle_batch,
            symbol=symbol,
            timeframe=timeframe
        )
        
        # Process results
        if report.get_status() == ValidationStatus.PASS:
            logger.info(f"Batch validation passed: {report.passed_tests} tests")
        else:
            logger.error(f"Batch validation failed: {report.failed_tests} tests")
        
        # Clear batch
        self.candle_batch.clear()
```

## ⚙️ Configuration

### StreamingValidationConfig

```python
@dataclass
class StreamingValidationConfig:
    # Validation intervals
    validation_interval_seconds: int = 60
    max_candles_per_validation: int = 100
    
    # Tolerances
    price_tolerance_pct: float = 0.01
    volume_tolerance_pct: float = 0.05
    timestamp_tolerance_seconds: float = 5.0
    
    # Cross-source validation
    enable_cross_source_validation: bool = True
    cross_source_interval_minutes: int = 5
    
    # Timestamp validation
    enable_timestamp_validation: bool = True
    max_gap_seconds: int = 300
    
    # Aggregation validation
    enable_aggregation_validation: bool = True
    aggregation_timeframes: List[str] = None
```

### StreamingIntegrationConfig

```python
@dataclass
class StreamingIntegrationConfig:
    # Validation settings
    enable_real_time_validation: bool = True
    validation_interval_seconds: int = 60
    max_candles_per_validation: int = 100
    
    # Callback settings
    enable_validation_callbacks: bool = True
    callback_timeout_seconds: int = 30
    
    # Error handling
    max_consecutive_failures: int = 5
    failure_cooldown_seconds: int = 300
    
    # Performance
    enable_async_validation: bool = True
    validation_queue_size: int = 1000
```

## 📊 Monitoring and Statistics

### Validation Statistics

```python
# Get validation statistics
stats = validator.get_stats()

print(f"Total candles validated: {stats['total_candles_validated']}")
print(f"Success rate: {stats['success_rate']:.1f}%")
print(f"Consecutive failures: {stats['consecutive_failures']}")
print(f"Is running: {stats['is_running']}")
```

### Performance Monitoring

```python
# Monitor validation performance
def monitor_validation_performance():
    stats = validator.get_stats()
    
    if stats['success_rate'] < 95:
        logger.warning(f"Low validation success rate: {stats['success_rate']:.1f}%")
    
    if stats['consecutive_failures'] > 3:
        logger.error(f"High consecutive failures: {stats['consecutive_failures']}")
    
    if stats['validation_errors'] > 10:
        logger.error(f"High validation error count: {stats['validation_errors']}")
```

## 🧪 Testing

### Unit Tests

```bash
# Run streaming validation tests
python -m pytest tests/test_streaming_validation.py -v

# Run specific test categories
python -m pytest tests/test_streaming_validation.py::TestStreamingValidator -v
python -m pytest tests/test_streaming_validation.py::TestStreamingServiceIntegration -v
```

### Integration Tests

```bash
# Run integration tests
python -m pytest tests/test_streaming_validation.py -m integration -v

# Run with real streaming data
python -m pytest tests/test_streaming_validation.py::TestStreamingValidationIntegrationReal -v
```

### Performance Tests

```python
# Test validation performance
import asyncio
import time

async def test_validation_performance():
    validator = StreamingServiceValidator(...)
    await validator.start()
    
    start_time = time.time()
    
    # Validate many candles
    for i in range(1000):
        candle = create_test_candle()
        await validator.validate_candle(candle, "BTC-USDT", "1m")
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"Validated 1000 candles in {duration:.2f} seconds")
    print(f"Rate: {1000/duration:.1f} candles/second")
    
    await validator.stop()
```

## 🚨 Error Handling

### Common Error Scenarios

1. **Validation Failures**
   - Invalid OHLC data
   - Timestamp issues
   - Cross-source mismatches

2. **System Errors**
   - Network timeouts
   - API rate limits
   - Memory issues

3. **Configuration Errors**
   - Invalid settings
   - Missing dependencies
   - Permission issues

### Error Recovery

```python
class RobustStreamingProcessor:
    def __init__(self):
        self.validator = StreamingServiceValidator(...)
        self.setup_error_handling()
    
    def setup_error_handling(self):
        # Add error callback
        self.validator.add_error_callback(self.handle_validation_error)
    
    def handle_validation_error(self, error: Exception, failure_count: int):
        """Handle validation errors with recovery"""
        logger.error(f"Validation error #{failure_count}: {error}")
        
        if failure_count >= 5:
            # Too many failures, restart validator
            asyncio.create_task(self.restart_validator())
        elif failure_count >= 3:
            # Reduce validation frequency
            self.reduce_validation_frequency()
    
    async def restart_validator(self):
        """Restart validator after failures"""
        logger.info("Restarting validator...")
        await self.validator.stop()
        await asyncio.sleep(5)  # Wait before restart
        await self.validator.start()
        logger.info("Validator restarted")
    
    def reduce_validation_frequency(self):
        """Reduce validation frequency to reduce load"""
        logger.info("Reducing validation frequency")
        # Update configuration to reduce validation frequency
        pass
```

## 📈 Performance Optimization

### 1. Async Processing

```python
# Enable async validation for better performance
config = StreamingIntegrationConfig(
    enable_async_validation=True,
    validation_queue_size=1000
)
```

### 2. Batch Validation

```python
# Validate candles in batches for better performance
async def validate_candle_batch(candles):
    report = await validator.validate_candle_batch(
        candles=candles,
        symbol="BTC-USDT",
        timeframe="1m"
    )
    return report
```

### 3. Caching

```python
# Cache validation results for repeated candles
class CachedStreamingValidator:
    def __init__(self):
        self.cache = {}
        self.cache_ttl = 300  # 5 minutes
    
    async def validate_candle(self, candle):
        cache_key = f"{candle.symbol}_{candle.timeframe}_{candle.timestamp}"
        
        if cache_key in self.cache:
            cached_result, timestamp = self.cache[cache_key]
            if time.time() - timestamp < self.cache_ttl:
                return cached_result
        
        # Validate and cache result
        result = await self.validator.validate_streaming_candle(candle)
        self.cache[cache_key] = (result, time.time())
        
        return result
```

## 🔄 Integration with Unified Streaming Architecture

### 1. Mode Integration

```python
# Serve mode integration
class ServeModeIntegration:
    def __init__(self):
        self.validator = StreamingServiceValidator(...)
    
    async def serve_candles(self, symbol, timeframe):
        """Serve validated candles to consumers"""
        async for candle in self.get_streaming_candles(symbol, timeframe):
            # Validate candle
            validation_result = await self.validator.validate_candle(
                candle, symbol, timeframe
            )
            
            if validation_result.status == ValidationStatus.PASS:
                # Serve validated candle
                yield candle
            else:
                # Log validation failure
                logger.warning(f"Validation failed: {validation_result.message}")

# Persist mode integration
class PersistModeIntegration:
    def __init__(self):
        self.validator = StreamingServiceValidator(...)
    
    async def persist_candles(self, candles, symbol, timeframe):
        """Persist validated candles to BigQuery"""
        # Validate batch
        report = await self.validator.validate_candle_batch(
            candles, symbol, timeframe
        )
        
        if report.get_status() == ValidationStatus.PASS:
            # Persist to BigQuery
            await self.persist_to_bigquery(candles)
        else:
            # Log validation failures
            logger.error(f"Batch validation failed: {report.failed_tests} failures")
```

### 2. Data Type Integration

```python
# Integration with data type router
class DataTypeValidationRouter:
    def __init__(self):
        self.validators = {
            'trades': StreamingServiceValidator(...),
            'book_snapshots': StreamingServiceValidator(...),
            'liquidations': StreamingServiceValidator(...),
            'derivative_ticker': StreamingServiceValidator(...),
            'options_chain': StreamingServiceValidator(...)
        }
    
    async def validate_data_type(self, data_type, data):
        """Validate data based on type"""
        validator = self.validators.get(data_type)
        if not validator:
            logger.warning(f"No validator for data type: {data_type}")
            return None
        
        return await validator.validate_candle(data)
```

## 📚 API Reference

### StreamingValidator

```python
class StreamingValidator:
    async def validate_streaming_candle(
        self,
        candle: OHLCV,
        symbol: str,
        timeframe: str
    ) -> ValidationResult
    
    async def validate_streaming_service_output(
        self,
        streaming_candles: List[OHLCV],
        symbol: str,
        timeframe: str
    ) -> ValidationReport
    
    def get_validation_stats(self) -> Dict[str, Any]
    def clear_candle_buffer(self, symbol: str = None, timeframe: str = None)
```

### StreamingServiceIntegration

```python
class StreamingServiceIntegration:
    async def start(self)
    async def stop(self)
    
    async def validate_candle(
        self,
        candle: OHLCV,
        symbol: str,
        timeframe: str,
        callback: Optional[Callable] = None
    ) -> ValidationResult
    
    async def validate_candle_batch(
        self,
        candles: List[OHLCV],
        symbol: str,
        timeframe: str,
        callback: Optional[Callable] = None
    ) -> ValidationReport
    
    def add_validation_callback(self, callback: Callable)
    def add_error_callback(self, callback: Callable)
    def get_stats(self) -> Dict[str, Any]
    def reset_stats(self)
```

### StreamingServiceValidator

```python
class StreamingServiceValidator:
    async def start(self)
    async def stop(self)
    
    async def validate_candle(
        self,
        candle: OHLCV,
        symbol: str,
        timeframe: str
    ) -> ValidationResult
    
    async def validate_candle_batch(
        self,
        candles: List[OHLCV],
        symbol: str,
        timeframe: str
    ) -> ValidationReport
    
    def get_stats(self) -> Dict[str, Any]
    def add_validation_callback(self, callback: Callable)
    def add_error_callback(self, callback: Callable)
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.