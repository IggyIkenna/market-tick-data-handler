# Enhanced Utilities Documentation

This document describes the new enhanced utilities added to the Market Data Tick Handler for improved logging, error handling, performance monitoring, and configuration validation.

## Overview

The enhanced utilities provide:
- **Structured Logging**: Consistent, JSON-formatted logging with context
- **Enhanced Error Handling**: Comprehensive error classification and recovery strategies
- **Performance Monitoring**: Real-time performance metrics and system resource monitoring
- **Configuration Validation**: Detailed validation with helpful error messages

## 1. Structured Logging (`src/utils/logger.py`)

### Features
- JSON-formatted log output for better parsing and analysis
- Contextual logging with additional fields
- Performance monitoring integration
- Configurable log levels and destinations

### Usage

```python
from src.utils.logger import setup_structured_logging, log_with_context, PerformanceLogger

# Setup structured logging
logger = setup_structured_logging(
    log_level="INFO",
    console_output=True,
    include_timestamp=True,
    include_level=True
)

# Log with context
log_with_context(logger, logging.INFO, "Processing data", 
                operation="data_processing", 
                item_count=1000)

# Performance monitoring
with PerformanceLogger(logger, "api_call", endpoint="/instruments"):
    # Your operation here
    result = await api_call()
```

### Log Format
```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "level": "INFO",
  "logger": "src.main",
  "message": "Processing data",
  "operation": "data_processing",
  "item_count": 1000,
  "performance": {
    "operation": "api_call",
    "duration_seconds": 1.234,
    "status": "completed"
  }
}
```

## 2. Enhanced Error Handling (`src/utils/error_handler.py`)

### Features
- Automatic error classification (Network, API, Authentication, etc.)
- Severity levels (Low, Medium, High, Critical)
- Recovery strategies (Retry, Skip, Fail Fast, Fallback)
- Structured error context and reporting

### Usage

```python
from src.utils.error_handler import ErrorHandler, ErrorContext, error_handler, ErrorCategory

# Manual error handling
error_handler = ErrorHandler()
context = ErrorContext(operation="data_download", component="tardis_connector")
enhanced_error = error_handler.handle_error(original_error, context)

# Automatic error handling with decorator
@error_handler(category=ErrorCategory.NETWORK, max_retries=3)
async def download_data():
    # Your operation here
    pass

# Safe execution
from src.utils.error_handler import safe_execute
result = safe_execute(
    operation=lambda: risky_operation(),
    default_return=None,
    reraise=False
)
```

### Error Categories
- **NETWORK**: Connection, timeout, DNS issues
- **API**: HTTP errors, status codes, response issues
- **AUTHENTICATION**: Token, credential, permission issues
- **RATE_LIMIT**: Rate limiting, quota exceeded
- **DATA_VALIDATION**: Invalid data, parsing errors
- **STORAGE**: File, disk, GCS issues
- **CONFIGURATION**: Missing config, invalid settings
- **SYSTEM**: Memory, process, thread issues
- **BUSINESS_LOGIC**: Application-specific errors

### Recovery Strategies
- **RETRY**: Automatic retry with exponential backoff
- **SKIP**: Skip the operation and continue
- **FAIL_FAST**: Stop immediately on error
- **FALLBACK**: Use alternative approach
- **MANUAL_INTERVENTION**: Require human intervention

## 3. Performance Monitoring (`src/utils/performance_monitor.py`)

### Features
- Real-time performance metrics collection
- System resource monitoring (CPU, memory, disk, network)
- Operation timing and statistics
- Performance data export

### Usage

```python
from src.utils.performance_monitor import (
    performance_monitor, 
    get_performance_monitor, 
    start_performance_monitoring,
    record_performance
)

# Start system monitoring
start_performance_monitoring(interval=30)

# Automatic performance monitoring
@performance_monitor("data_processing", item_count=1000)
async def process_data():
    # Your operation here
    pass

# Manual performance recording
with record_performance("api_call", endpoint="/instruments"):
    result = await api_call()

# Get performance statistics
monitor = get_performance_monitor()
stats = monitor.get_operation_stats("data_processing")
print(f"Average duration: {stats['total_duration'] / stats['count']:.3f}s")
```

### Metrics Collected
- **Operation Metrics**: Duration, success/failure rates, error counts
- **System Metrics**: CPU usage, memory usage, disk usage, network I/O
- **Custom Metrics**: Any additional metrics you specify

### Performance Data Export
```python
# Export metrics to JSON file
monitor = get_performance_monitor()
monitor.export_metrics("performance_report.json")
```

## 4. Enhanced Configuration Validation (`config.py`)

### Features
- Detailed validation with helpful error messages
- Performance recommendations
- Environment variable guidance
- Comprehensive error reporting

### Example Validation Errors

```python
# Before (basic validation)
ValueError("Max concurrent must be positive")

# After (enhanced validation)
ValueError("""
Tardis configuration validation failed:
  - Max concurrent seems too high (150), consider reducing to avoid rate limiting
  - Timeout seems too high (600s), consider reducing for better performance
  - Rate limit per VM seems too low (50000), consider increasing for better throughput
""")
```

### Configuration Recommendations
- **Timeout**: Warns if > 300s, suggests reducing for better performance
- **Max Retries**: Warns if > 10, suggests reducing for faster failure detection
- **Max Concurrent**: Warns if > 100, suggests reducing to avoid rate limiting
- **Max Parallel Uploads**: Warns if > 50, suggests reducing to avoid overwhelming GCS
- **Rate Limit**: Warns if < 100,000, suggests increasing for better throughput

## 5. Integration with Main Application

The enhanced utilities are integrated into the main application (`src/main.py`) with:

### Structured Logging Setup
```python
from src.utils.logger import setup_structured_logging
logger = setup_structured_logging(
    log_level=os.getenv('LOG_LEVEL', 'INFO'),
    console_output=True,
    include_timestamp=True,
    include_level=True
)
```

### Error Handling Integration
```python
from src.utils.error_handler import ErrorHandler, ErrorContext
error_handler = ErrorHandler(logger)
context = ErrorContext(operation="main", component="main.py")

try:
    # Main application logic
    pass
except Exception as e:
    enhanced_error = error_handler.handle_error(e, context)
    logger.error(f"Fatal error: {enhanced_error.message}")
    logger.error(f"Error category: {enhanced_error.category.value}")
    logger.error(f"Severity: {enhanced_error.severity.value}")
    logger.error(f"Recovery strategy: {enhanced_error.recovery_strategy.value}")
```

### Performance Monitoring Integration
```python
from src.utils.performance_monitor import start_performance_monitoring, get_performance_monitor

# Start monitoring
start_performance_monitoring(interval=60)

# Export metrics at the end
performance_monitor = get_performance_monitor()
performance_monitor.export_metrics(f"performance_metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
```

## 6. Benefits

### For Developers
- **Better Debugging**: Structured logs with context make debugging easier
- **Performance Insights**: Real-time performance monitoring helps identify bottlenecks
- **Error Classification**: Automatic error classification helps with troubleshooting
- **Configuration Validation**: Clear error messages help with setup issues

### For Operations
- **Monitoring**: System resource monitoring helps with capacity planning
- **Alerting**: Error severity levels enable proper alerting strategies
- **Performance Tracking**: Historical performance data helps with optimization
- **Troubleshooting**: Structured error information speeds up incident response

### For Users
- **Better Error Messages**: Clear, actionable error messages
- **Performance Transparency**: Performance metrics help understand system behavior
- **Reliability**: Enhanced error handling improves system reliability
- **Configuration Guidance**: Better validation helps with proper configuration

## 7. Migration Guide

### From Basic Logging
```python
# Before
logger.info(f"Processing {count} items")

# After
log_with_context(logger, logging.INFO, f"Processing {count} items", 
                item_count=count, operation="data_processing")
```

### From Basic Error Handling
```python
# Before
try:
    operation()
except Exception as e:
    logger.error(f"Error: {e}")
    raise

# After
@error_handler(category=ErrorCategory.NETWORK, max_retries=3)
def operation():
    # Your operation here
    pass
```

### From Basic Performance Monitoring
```python
# Before
start_time = time.time()
result = operation()
duration = time.time() - start_time
logger.info(f"Operation took {duration:.3f}s")

# After
@performance_monitor("operation")
def operation():
    # Your operation here
    pass
```

## 8. Best Practices

### Logging
- Use structured logging for all important operations
- Include relevant context in log messages
- Use appropriate log levels (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- Export logs for analysis in production

### Error Handling
- Use appropriate error categories for better classification
- Implement proper recovery strategies
- Log errors with full context
- Monitor error rates and patterns

### Performance Monitoring
- Start system monitoring early in the application lifecycle
- Use performance decorators for key operations
- Export performance data regularly
- Monitor system resources and set up alerts

### Configuration
- Use enhanced validation for all configuration
- Provide clear error messages for configuration issues
- Document configuration requirements
- Test configuration validation in CI/CD

## 9. Future Enhancements

Potential future improvements:
- **Distributed Tracing**: Add distributed tracing for microservices
- **Metrics Export**: Export metrics to Prometheus/Grafana
- **Alerting Integration**: Integrate with alerting systems
- **Dashboard**: Create a performance monitoring dashboard
- **Machine Learning**: Use ML for anomaly detection in performance metrics
