"""
Streaming Service Integration

Integration layer between the validation framework and the unified streaming architecture.
Provides real-time validation capabilities for the streaming service.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass
import json

from .streaming_validator import StreamingValidator, StreamingValidationConfig
from .validation_results import ValidationReport, ValidationResult, ValidationStatus
from .cross_source_validator import OHLCV

logger = logging.getLogger(__name__)


@dataclass
class StreamingIntegrationConfig:
    """Configuration for streaming service integration"""
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


class StreamingServiceIntegration:
    """
    Integration layer for the streaming service to use validation framework.
    
    This class provides:
    - Real-time validation of streaming candles
    - Integration with the unified streaming architecture
    - Callback system for validation results
    - Error handling and recovery
    - Performance monitoring
    """
    
    def __init__(
        self,
        streaming_validator: StreamingValidator,
        config: StreamingIntegrationConfig = None
    ):
        self.validator = streaming_validator
        self.config = config or StreamingIntegrationConfig()
        
        # State management
        self.is_running = False
        self.consecutive_failures = 0
        self.last_failure_time = None
        self.validation_queue = asyncio.Queue(maxsize=self.config.validation_queue_size)
        
        # Callbacks
        self.validation_callbacks: List[Callable] = []
        self.error_callbacks: List[Callable] = []
        
        # Statistics
        self.stats = {
            'total_candles_validated': 0,
            'successful_validations': 0,
            'failed_validations': 0,
            'validation_errors': 0,
            'callback_errors': 0,
            'start_time': None
        }
    
    async def start(self):
        """Start the streaming integration service"""
        if self.is_running:
            logger.warning("Streaming integration is already running")
            return
        
        self.is_running = True
        self.stats['start_time'] = datetime.utcnow()
        
        logger.info("🚀 Starting streaming service integration")
        
        # Start validation queue processor
        if self.config.enable_async_validation:
            asyncio.create_task(self._process_validation_queue())
        
        logger.info("✅ Streaming service integration started")
    
    async def stop(self):
        """Stop the streaming integration service"""
        if not self.is_running:
            return
        
        self.is_running = False
        logger.info("🛑 Stopping streaming service integration")
        
        # Wait for queue to empty
        if not self.validation_queue.empty():
            logger.info("⏳ Waiting for validation queue to empty...")
            await self.validation_queue.join()
        
        logger.info("✅ Streaming service integration stopped")
    
    async def validate_candle(
        self,
        candle: OHLCV,
        symbol: str,
        timeframe: str,
        callback: Optional[Callable] = None
    ) -> ValidationResult:
        """
        Validate a single streaming candle.
        
        Args:
            candle: The candle to validate
            symbol: Trading symbol
            timeframe: Candle timeframe
            callback: Optional callback for validation result
            
        Returns:
            ValidationResult with validation status
        """
        try:
            # Check if we should skip validation due to recent failures
            if self._should_skip_validation():
                return ValidationResult(
                    test_name="streaming_candle_validation",
                    status=ValidationStatus.SKIP,
                    message="Validation skipped due to recent failures",
                    details={
                        'consecutive_failures': self.consecutive_failures,
                        'last_failure_time': self.last_failure_time.isoformat() if self.last_failure_time else None
                    }
                )
            
            # Validate the candle
            if self.config.enable_async_validation:
                # Add to queue for async processing
                await self.validation_queue.put({
                    'candle': candle,
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'callback': callback,
                    'timestamp': datetime.utcnow()
                })
                
                # Return immediate result
                return ValidationResult(
                    test_name="streaming_candle_validation",
                    status=ValidationStatus.PASS,
                    message="Candle queued for validation",
                    details={
                        'symbol': symbol,
                        'timeframe': timeframe,
                        'queued_at': datetime.utcnow().isoformat()
                    }
                )
            else:
                # Synchronous validation
                result = await self.validator.validate_streaming_candle(
                    candle, symbol, timeframe
                )
                
                # Update statistics
                self._update_stats(result)
                
                # Call callback if provided
                if callback:
                    await self._call_callback(callback, result)
                
                return result
                
        except Exception as e:
            logger.error(f"Error validating candle: {e}")
            self._handle_validation_error(e)
            
            return ValidationResult(
                test_name="streaming_candle_validation",
                status=ValidationStatus.FAIL,
                message=f"Candle validation failed: {str(e)}",
                details={'error': str(e)}
            )
    
    async def validate_candle_batch(
        self,
        candles: List[OHLCV],
        symbol: str,
        timeframe: str,
        callback: Optional[Callable] = None
    ) -> ValidationReport:
        """
        Validate a batch of streaming candles.
        
        Args:
            candles: List of candles to validate
            symbol: Trading symbol
            timeframe: Candle timeframe
            callback: Optional callback for validation report
            
        Returns:
            ValidationReport with validation results
        """
        try:
            # Check if we should skip validation
            if self._should_skip_validation():
                return ValidationReport(
                    report_id=f"skipped_validation_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
                    start_time=datetime.utcnow(),
                    end_time=datetime.utcnow()
                )
            
            # Validate batch
            report = await self.validator.validate_streaming_service_output(
                candles, symbol, timeframe
            )
            
            # Update statistics
            self._update_stats_from_report(report)
            
            # Call callback if provided
            if callback:
                await self._call_callback(callback, report)
            
            return report
            
        except Exception as e:
            logger.error(f"Error validating candle batch: {e}")
            self._handle_validation_error(e)
            
            # Return error report
            error_report = ValidationReport(
                report_id=f"error_validation_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
                start_time=datetime.utcnow(),
                end_time=datetime.utcnow()
            )
            
            error_result = ValidationResult(
                test_name="batch_validation",
                status=ValidationStatus.FAIL,
                message=f"Batch validation failed: {str(e)}",
                details={'error': str(e)}
            )
            error_report.add_result(error_result)
            
            return error_report
    
    async def _process_validation_queue(self):
        """Process validation queue asynchronously"""
        while self.is_running:
            try:
                # Get next validation task
                task = await asyncio.wait_for(
                    self.validation_queue.get(),
                    timeout=1.0
                )
                
                # Process validation
                result = await self.validator.validate_streaming_candle(
                    task['candle'],
                    task['symbol'],
                    task['timeframe']
                )
                
                # Update statistics
                self._update_stats(result)
                
                # Call callback if provided
                if task['callback']:
                    await self._call_callback(task['callback'], result)
                
                # Mark task as done
                self.validation_queue.task_done()
                
            except asyncio.TimeoutError:
                # No tasks in queue, continue
                continue
            except Exception as e:
                logger.error(f"Error processing validation queue: {e}")
                self._handle_validation_error(e)
                
                # Mark task as done even if it failed
                if not self.validation_queue.empty():
                    self.validation_queue.task_done()
    
    def _should_skip_validation(self) -> bool:
        """Check if validation should be skipped due to recent failures"""
        if self.consecutive_failures < self.config.max_consecutive_failures:
            return False
        
        if self.last_failure_time is None:
            return False
        
        time_since_failure = (datetime.utcnow() - self.last_failure_time).total_seconds()
        return time_since_failure < self.config.failure_cooldown_seconds
    
    def _handle_validation_error(self, error: Exception):
        """Handle validation errors and update failure tracking"""
        self.consecutive_failures += 1
        self.last_failure_time = datetime.utcnow()
        self.stats['validation_errors'] += 1
        
        logger.error(f"Validation error #{self.consecutive_failures}: {error}")
        
        # Call error callbacks
        for callback in self.error_callbacks:
            try:
                callback(error, self.consecutive_failures)
            except Exception as e:
                logger.error(f"Error in error callback: {e}")
                self.stats['callback_errors'] += 1
    
    def _update_stats(self, result: ValidationResult):
        """Update statistics from validation result"""
        self.stats['total_candles_validated'] += 1
        
        if result.status == ValidationStatus.PASS:
            self.stats['successful_validations'] += 1
            # Reset consecutive failures on success
            if self.consecutive_failures > 0:
                self.consecutive_failures = 0
        else:
            self.stats['failed_validations'] += 1
    
    def _update_stats_from_report(self, report: ValidationReport):
        """Update statistics from validation report"""
        self.stats['total_candles_validated'] += report.total_tests
        self.stats['successful_validations'] += report.passed_tests
        self.stats['failed_validations'] += report.failed_tests
        
        # Reset consecutive failures if report passed
        if report.get_status() == ValidationStatus.PASS:
            if self.consecutive_failures > 0:
                self.consecutive_failures = 0
    
    async def _call_callback(self, callback: Callable, result: Any):
        """Call a callback with timeout"""
        try:
            if asyncio.iscoroutinefunction(callback):
                await asyncio.wait_for(
                    callback(result),
                    timeout=self.config.callback_timeout_seconds
                )
            else:
                callback(result)
        except asyncio.TimeoutError:
            logger.error(f"Callback timed out after {self.config.callback_timeout_seconds} seconds")
            self.stats['callback_errors'] += 1
        except Exception as e:
            logger.error(f"Error in callback: {e}")
            self.stats['callback_errors'] += 1
    
    def add_validation_callback(self, callback: Callable[[ValidationResult], None]):
        """Add a callback for validation results"""
        self.validation_callbacks.append(callback)
    
    def add_error_callback(self, callback: Callable[[Exception, int], None]):
        """Add a callback for validation errors"""
        self.error_callbacks.append(callback)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current integration statistics"""
        total = self.stats['total_candles_validated']
        if total == 0:
            success_rate = 0.0
        else:
            success_rate = (self.stats['successful_validations'] / total) * 100
        
        return {
            **self.stats,
            'success_rate': success_rate,
            'consecutive_failures': self.consecutive_failures,
            'is_running': self.is_running,
            'queue_size': self.validation_queue.qsize() if hasattr(self.validation_queue, 'qsize') else 0
        }
    
    def reset_stats(self):
        """Reset integration statistics"""
        self.stats = {
            'total_candles_validated': 0,
            'successful_validations': 0,
            'failed_validations': 0,
            'validation_errors': 0,
            'callback_errors': 0,
            'start_time': datetime.utcnow()
        }
        self.consecutive_failures = 0
        self.last_failure_time = None


class StreamingServiceValidator:
    """
    High-level validator for the streaming service.
    
    This class provides a simple interface for the streaming service
    to validate its output before serving or persisting.
    """
    
    def __init__(
        self,
        cross_source_validator,
        timestamp_validator,
        aggregation_validator,
        config: StreamingIntegrationConfig = None
    ):
        # Create streaming validator
        streaming_config = StreamingValidationConfig(
            validation_interval_seconds=60,
            max_candles_per_validation=100,
            enable_cross_source_validation=True,
            enable_timestamp_validation=True,
            enable_aggregation_validation=True
        )
        
        self.validator = StreamingValidator(
            cross_source_validator,
            timestamp_validator,
            aggregation_validator,
            streaming_config
        )
        
        # Create integration
        self.integration = StreamingServiceIntegration(
            self.validator,
            config or StreamingIntegrationConfig()
        )
    
    async def start(self):
        """Start the streaming service validator"""
        await self.integration.start()
    
    async def stop(self):
        """Stop the streaming service validator"""
        await self.integration.stop()
    
    async def validate_candle(
        self,
        candle: OHLCV,
        symbol: str,
        timeframe: str
    ) -> ValidationResult:
        """Validate a single candle from the streaming service"""
        return await self.integration.validate_candle(candle, symbol, timeframe)
    
    async def validate_candle_batch(
        self,
        candles: List[OHLCV],
        symbol: str,
        timeframe: str
    ) -> ValidationReport:
        """Validate a batch of candles from the streaming service"""
        return await self.integration.validate_candle_batch(candles, symbol, timeframe)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get validation statistics"""
        return self.integration.get_stats()
    
    def add_validation_callback(self, callback: Callable[[ValidationResult], None]):
        """Add a callback for validation results"""
        self.integration.add_validation_callback(callback)
    
    def add_error_callback(self, callback: Callable[[Exception, int], None]):
        """Add a callback for validation errors"""
        self.integration.add_error_callback(callback)
