"""
Enhanced Error Handling Utilities for Market Data Tick Handler

Provides comprehensive error handling with proper classification, recovery strategies,
and structured error reporting.
"""

import logging
import asyncio
import functools
from typing import Dict, Any, Optional, Union, Callable, Type, List
from datetime import datetime, timezone
from enum import Enum
import traceback
import sys


class ErrorSeverity(Enum):
    """Error severity levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ErrorCategory(Enum):
    """Error categories for classification"""
    NETWORK = "network"
    API = "api"
    AUTHENTICATION = "authentication"
    RATE_LIMIT = "rate_limit"
    DATA_VALIDATION = "data_validation"
    STORAGE = "storage"
    CONFIGURATION = "configuration"
    SYSTEM = "system"
    BUSINESS_LOGIC = "business_logic"


class ErrorRecoveryStrategy(Enum):
    """Error recovery strategies"""
    RETRY = "retry"
    SKIP = "skip"
    FAIL_FAST = "fail_fast"
    FALLBACK = "fallback"
    MANUAL_INTERVENTION = "manual_intervention"


class ErrorContext:
    """Context information for errors"""
    
    def __init__(self, **kwargs):
        self.timestamp = datetime.now(timezone.utc)
        self.operation = kwargs.get('operation')
        self.component = kwargs.get('component')
        self.user_id = kwargs.get('user_id')
        self.request_id = kwargs.get('request_id')
        self.session_id = kwargs.get('session_id')
        self.additional_data = kwargs.get('additional_data', {})
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging"""
        return {
            'timestamp': self.timestamp.isoformat(),
            'operation': self.operation,
            'component': self.component,
            'user_id': self.user_id,
            'request_id': self.request_id,
            'session_id': self.session_id,
            'additional_data': self.additional_data
        }


class EnhancedError(Exception):
    """Enhanced error with structured information"""
    
    def __init__(
        self,
        message: str,
        category: ErrorCategory,
        severity: ErrorSeverity = ErrorSeverity.MEDIUM,
        recovery_strategy: ErrorRecoveryStrategy = ErrorRecoveryStrategy.RETRY,
        context: Optional[ErrorContext] = None,
        original_error: Optional[Exception] = None,
        retry_count: int = 0,
        max_retries: int = 3,
        **kwargs
    ):
        super().__init__(message)
        self.message = message
        self.category = category
        self.severity = severity
        self.recovery_strategy = recovery_strategy
        self.context = context or ErrorContext()
        self.original_error = original_error
        self.retry_count = retry_count
        self.max_retries = max_retries
        self.additional_info = kwargs
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging"""
        return {
            'error_type': self.__class__.__name__,
            'message': self.message,
            'category': self.category.value,
            'severity': self.severity.value,
            'recovery_strategy': self.recovery_strategy.value,
            'context': self.context.to_dict(),
            'retry_count': self.retry_count,
            'max_retries': self.max_retries,
            'original_error': {
                'type': type(self.original_error).__name__ if self.original_error else None,
                'message': str(self.original_error) if self.original_error else None
            },
            'additional_info': self.additional_info
        }


class ErrorHandler:
    """Centralized error handling with recovery strategies"""
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
        self.error_counts: Dict[str, int] = {}
        self.recovery_strategies: Dict[ErrorCategory, ErrorRecoveryStrategy] = {
            ErrorCategory.NETWORK: ErrorRecoveryStrategy.RETRY,
            ErrorCategory.API: ErrorRecoveryStrategy.RETRY,
            ErrorCategory.RATE_LIMIT: ErrorRecoveryStrategy.RETRY,
            ErrorCategory.AUTHENTICATION: ErrorRecoveryStrategy.MANUAL_INTERVENTION,
            ErrorCategory.DATA_VALIDATION: ErrorRecoveryStrategy.SKIP,
            ErrorCategory.STORAGE: ErrorRecoveryStrategy.RETRY,
            ErrorCategory.CONFIGURATION: ErrorRecoveryStrategy.MANUAL_INTERVENTION,
            ErrorCategory.SYSTEM: ErrorRecoveryStrategy.FAIL_FAST,
            ErrorCategory.BUSINESS_LOGIC: ErrorRecoveryStrategy.SKIP
        }
    
    def handle_error(
        self,
        error: Exception,
        context: Optional[ErrorContext] = None,
        custom_recovery: Optional[ErrorRecoveryStrategy] = None
    ) -> EnhancedError:
        """Handle and classify an error"""
        # Classify the error
        category = self._classify_error(error)
        severity = self._determine_severity(error, category)
        recovery_strategy = custom_recovery or self.recovery_strategies.get(category, ErrorRecoveryStrategy.RETRY)
        
        # Create enhanced error
        enhanced_error = EnhancedError(
            message=str(error),
            category=category,
            severity=severity,
            recovery_strategy=recovery_strategy,
            context=context,
            original_error=error
        )
        
        # Log the error
        self._log_error(enhanced_error)
        
        # Update error counts
        self._update_error_counts(category)
        
        return enhanced_error
    
    def _classify_error(self, error: Exception) -> ErrorCategory:
        """Classify error based on type and message"""
        error_type = type(error).__name__
        error_message = str(error).lower()
        
        # Network-related errors
        if any(keyword in error_message for keyword in ['connection', 'timeout', 'network', 'dns', 'socket']):
            return ErrorCategory.NETWORK
        
        # API-related errors
        if any(keyword in error_message for keyword in ['api', 'http', 'status', 'response']):
            return ErrorCategory.API
        
        # Authentication errors
        if any(keyword in error_message for keyword in ['auth', 'unauthorized', 'forbidden', 'token', 'credential']):
            return ErrorCategory.AUTHENTICATION
        
        # Rate limiting
        if any(keyword in error_message for keyword in ['rate limit', 'too many requests', '429']):
            return ErrorCategory.RATE_LIMIT
        
        # Data validation
        if any(keyword in error_message for keyword in ['validation', 'invalid', 'malformed', 'parse']):
            return ErrorCategory.DATA_VALIDATION
        
        # Storage errors
        if any(keyword in error_message for keyword in ['storage', 'file', 'disk', 'gcs', 'bucket']):
            return ErrorCategory.STORAGE
        
        # Configuration errors
        if any(keyword in error_message for keyword in ['config', 'setting', 'environment', 'missing']):
            return ErrorCategory.CONFIGURATION
        
        # System errors
        if any(keyword in error_message for keyword in ['system', 'memory', 'process', 'thread']):
            return ErrorCategory.SYSTEM
        
        return ErrorCategory.BUSINESS_LOGIC
    
    def _determine_severity(self, error: Exception, category: ErrorCategory) -> ErrorSeverity:
        """Determine error severity based on category and context"""
        if category in [ErrorCategory.AUTHENTICATION, ErrorCategory.CONFIGURATION]:
            return ErrorSeverity.CRITICAL
        elif category in [ErrorCategory.SYSTEM, ErrorCategory.STORAGE]:
            return ErrorSeverity.HIGH
        elif category in [ErrorCategory.NETWORK, ErrorCategory.API, ErrorCategory.RATE_LIMIT]:
            return ErrorSeverity.MEDIUM
        else:
            return ErrorSeverity.LOW
    
    def _log_error(self, enhanced_error: EnhancedError):
        """Log enhanced error with structured information"""
        error_data = enhanced_error.to_dict()
        
        if enhanced_error.severity == ErrorSeverity.CRITICAL:
            self.logger.critical(f"Critical error: {enhanced_error.message}", extra={'error_data': error_data})
        elif enhanced_error.severity == ErrorSeverity.HIGH:
            self.logger.error(f"High severity error: {enhanced_error.message}", extra={'error_data': error_data})
        elif enhanced_error.severity == ErrorSeverity.MEDIUM:
            self.logger.warning(f"Medium severity error: {enhanced_error.message}", extra={'error_data': error_data})
        else:
            self.logger.info(f"Low severity error: {enhanced_error.message}", extra={'error_data': error_data})
    
    def _update_error_counts(self, category: ErrorCategory):
        """Update error counts for monitoring"""
        key = f"{category.value}_errors"
        self.error_counts[key] = self.error_counts.get(key, 0) + 1
    
    def get_error_summary(self) -> Dict[str, Any]:
        """Get summary of error counts"""
        return {
            'total_errors': sum(self.error_counts.values()),
            'error_breakdown': self.error_counts.copy(),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }


def error_handler(
    category: Optional[ErrorCategory] = None,
    severity: Optional[ErrorSeverity] = None,
    recovery_strategy: Optional[ErrorRecoveryStrategy] = None,
    max_retries: int = 3,
    reraise: bool = True
):
    """Decorator for automatic error handling"""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            error_handler_instance = ErrorHandler()
            context = ErrorContext(
                operation=func.__name__,
                component=func.__module__
            )
            
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    enhanced_error = error_handler_instance.handle_error(e, context)
                    
                    if attempt < max_retries and enhanced_error.recovery_strategy == ErrorRecoveryStrategy.RETRY:
                        await asyncio.sleep(2 ** attempt)  # Exponential backoff
                        continue
                    elif enhanced_error.recovery_strategy == ErrorRecoveryStrategy.SKIP:
                        self.logger.warning(f"Skipping operation due to error: {enhanced_error.message}")
                        return None
                    elif enhanced_error.recovery_strategy == ErrorRecoveryStrategy.FAIL_FAST:
                        break
                    else:
                        break
            
            if reraise:
                raise enhanced_error
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            error_handler_instance = ErrorHandler()
            context = ErrorContext(
                operation=func.__name__,
                component=func.__module__
            )
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    enhanced_error = error_handler_instance.handle_error(e, context)
                    
                    if attempt < max_retries and enhanced_error.recovery_strategy == ErrorRecoveryStrategy.RETRY:
                        import time
                        time.sleep(2 ** attempt)  # Exponential backoff
                        continue
                    elif enhanced_error.recovery_strategy == ErrorRecoveryStrategy.SKIP:
                        self.logger.warning(f"Skipping operation due to error: {enhanced_error.message}")
                        return None
                    elif enhanced_error.recovery_strategy == ErrorRecoveryStrategy.FAIL_FAST:
                        break
                    else:
                        break
            
            if reraise:
                raise enhanced_error
        
        # Return appropriate wrapper based on function type
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


def safe_execute(
    operation: Callable,
    context: Optional[ErrorContext] = None,
    default_return: Any = None,
    reraise: bool = False
) -> Any:
    """Safely execute an operation with error handling"""
    error_handler_instance = ErrorHandler()
    
    try:
        return operation()
    except Exception as e:
        enhanced_error = error_handler_instance.handle_error(e, context)
        
        if reraise:
            raise enhanced_error
        
        return default_return


# Convenience functions for common error scenarios
def handle_network_error(error: Exception, context: Optional[ErrorContext] = None) -> EnhancedError:
    """Handle network-related errors"""
    return ErrorHandler().handle_error(error, context, ErrorRecoveryStrategy.RETRY)


def handle_api_error(error: Exception, context: Optional[ErrorContext] = None) -> EnhancedError:
    """Handle API-related errors"""
    return ErrorHandler().handle_error(error, context, ErrorRecoveryStrategy.RETRY)


def handle_data_validation_error(error: Exception, context: Optional[ErrorContext] = None) -> EnhancedError:
    """Handle data validation errors"""
    return ErrorHandler().handle_error(error, context, ErrorRecoveryStrategy.SKIP)


def handle_critical_error(error: Exception, context: Optional[ErrorContext] = None) -> EnhancedError:
    """Handle critical errors that require immediate attention"""
    return ErrorHandler().handle_error(error, context, ErrorRecoveryStrategy.MANUAL_INTERVENTION)
