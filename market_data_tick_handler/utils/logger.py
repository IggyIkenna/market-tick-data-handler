"""
Structured Logging Utilities for Market Data Tick Handler

Provides consistent, structured logging with proper formatting, error context,
and performance monitoring capabilities.
"""

import logging
import json
import time
import functools
from typing import Dict, Any, Optional, Union
from datetime import datetime, timezone
from pathlib import Path
import sys


class StructuredFormatter(logging.Formatter):
    """Custom formatter for structured logging with consistent formatting"""
    
    def __init__(self, include_timestamp: bool = True, include_level: bool = True):
        self.include_timestamp = include_timestamp
        self.include_level = include_level
        super().__init__()
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record with structured information"""
        # Base log entry
        log_entry = {
            'timestamp': datetime.now(timezone.utc).isoformat() if self.include_timestamp else None,
            'level': record.levelname if self.include_level else None,
            'logger': record.name,
            'message': record.getMessage(),
        }
        
        # Add extra fields if present
        if hasattr(record, 'extra_fields'):
            log_entry.update(record.extra_fields)
        
        # Add exception info if present
        if record.exc_info:
            log_entry['exception'] = {
                'type': record.exc_info[0].__name__ if record.exc_info[0] else None,
                'message': str(record.exc_info[1]) if record.exc_info[1] else None,
                'traceback': self.formatException(record.exc_info) if record.exc_info else None
            }
        
        # Add performance metrics if present
        if hasattr(record, ''):
            log_entry['performance'] = record.performance_metrics
        
        # Filter out None values
        log_entry = {k: v for k, v in log_entry.items() if v is not None}
        
        return json.dumps(log_entry, default=str, ensure_ascii=False)


class PerformanceLogger:
    """Context manager for performance logging"""
    
    def __init__(self, logger: logging.Logger, operation: str, **extra_fields):
        self.logger = logger
        self.operation = operation
        self.extra_fields = extra_fields
        self.start_time = None
        self.end_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        self.logger.info(
            f"Starting {self.operation}",
            extra={'extra_fields': {**self.extra_fields, 'operation': self.operation, 'status': 'started'}}
        )
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.time()
        duration = self.end_time - self.start_time
        
        performance_metrics = {
            'operation': self.operation,
            'duration_seconds': round(duration, 3),
            'status': 'completed' if exc_type is None else 'failed'
        }
        
        if exc_type is not None:
            performance_metrics['error'] = {
                'type': exc_type.__name__,
                'message': str(exc_val)
            }
        
        log_level = logging.INFO if exc_type is None else logging.ERROR
        self.logger.log(
            log_level,
            f"Completed {self.operation} in {duration:.3f}s",
            extra={
                'extra_fields': {**self.extra_fields, 'performance_metrics': performance_metrics}
            }
        )


def performance_monitor(operation: str = None, **extra_fields):
    """Decorator for automatic performance monitoring"""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger(func.__module__)
            op_name = operation or f"{func.__module__}.{func.__name__}"
            
            with PerformanceLogger(logger, op_name, **extra_fields):
                return func(*args, **kwargs)
        return wrapper
    return decorator


def setup_structured_logging(
    log_level: str = "INFO",
    log_file: Optional[Union[str, Path]] = None,
    console_output: bool = True,
    include_timestamp: bool = True,
    include_level: bool = True,
    gcp_logging: bool = False
) -> logging.Logger:
    """
    Setup structured logging with consistent formatting
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional file path for log output
        console_output: Whether to output to console
        include_timestamp: Whether to include timestamp in logs
        include_level: Whether to include log level in logs
    
    Returns:
        Configured logger instance
    """
    # Create formatter
    formatter = StructuredFormatter(include_timestamp, include_level)
    
    # Get root logger
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Console handler
    if console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # File handler
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    # Google Cloud Logging handler
    if gcp_logging:
        try:
            from google.cloud import logging as cloud_logging
            client = cloud_logging.Client()
            client.setup_logging()
            # Store the client for proper cleanup
            logger._gcp_client = client
        except ImportError:
            logger.warning("google-cloud-logging not available, skipping GCP logging setup")
        except Exception as e:
            logger.warning(f"Failed to setup GCP logging: {e}")
    
    # Set specific loggers to reduce noise
    logging.getLogger('google.cloud').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.WARNING)
    
    # Register cleanup function for proper shutdown
    import atexit
    atexit.register(_cleanup_logging)
    
    return logger


def _cleanup_logging():
    """Clean up logging handlers on shutdown to prevent threading issues"""
    logger = logging.getLogger()
    
    # Flush all handlers
    for handler in logger.handlers:
        try:
            handler.flush()
        except Exception:
            pass
    
    # Close GCP client if it exists
    if hasattr(logger, '_gcp_client'):
        try:
            logger._gcp_client.close()
        except Exception:
            pass


def log_with_context(logger: logging.Logger, level: int, message: str, **context):
    """Log message with additional context fields"""
    logger.log(level, message, extra={'extra_fields': context})


def log_error_with_context(logger: logging.Logger, message: str, error: Exception, **context):
    """Log error with full context and exception details"""
    error_context = {
        'error_type': type(error).__name__,
        'error_message': str(error),
        **context
    }
    
    logger.error(
        message,
        extra={'extra_fields': error_context},
        exc_info=True
    )


def log_performance_metrics(logger: logging.Logger, operation: str, metrics: Dict[str, Any], **context):
    """Log performance metrics with context"""
    performance_data = {
        'operation': operation,
        'metrics': metrics,
        **context
    }
    
    logger.info(
        f"Performance metrics for {operation}",
        extra={'extra_fields': {'performance_metrics': performance_data}}
    )


# Convenience functions for common logging patterns
def log_operation_start(logger: logging.Logger, operation: str, **context):
    """Log the start of an operation"""
    log_with_context(logger, logging.INFO, f"Starting {operation}", status='started', **context)


def log_operation_success(logger: logging.Logger, operation: str, **context):
    """Log successful completion of an operation"""
    log_with_context(logger, logging.INFO, f"Completed {operation}", status='success', **context)


def log_operation_failure(logger: logging.Logger, operation: str, error: Exception, **context):
    """Log failure of an operation"""
    log_error_with_context(logger, f"Failed {operation}", error, status='failed', **context)


def log_data_processing(logger: logging.Logger, operation: str, count: int, **context):
    """Log data processing operations with counts"""
    log_with_context(
        logger, 
        logging.INFO, 
        f"Processed {count} items in {operation}", 
        operation=operation,
        item_count=count,
        **context
    )


def log_api_call(logger: logging.Logger, method: str, url: str, status_code: int, duration: float, **context):
    """Log API calls with performance metrics"""
    log_with_context(
        logger,
        logging.INFO,
        f"API call: {method} {url} - {status_code} ({duration:.3f}s)",
        api_method=method,
        api_url=url,
        status_code=status_code,
        duration_seconds=duration,
        **context
    )
