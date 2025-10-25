"""
Streaming Service Validator

Validates real-time streaming data from the unified streaming architecture.
Integrates with the streaming service to provide real-time validation capabilities.
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass
import pandas as pd

from .validation_results import ValidationResult, ValidationStatus, ValidationReport
from .cross_source_validator import CrossSourceValidator, OHLCV
from .timestamp_validator import TimestampValidator
from .aggregation_validator import AggregationValidator

logger = logging.getLogger(__name__)


@dataclass
class StreamingValidationConfig:
    """Configuration for streaming validation"""
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
    
    def __post_init__(self):
        if self.aggregation_timeframes is None:
            self.aggregation_timeframes = ['1m', '5m', '15m', '1h']


class StreamingValidator:
    """
    Validates real-time streaming data from the unified streaming architecture.
    
    This validator integrates with the streaming service to provide:
    - Real-time cross-source validation (streaming vs Binance)
    - Timestamp stability validation
    - Aggregation consistency validation
    - Live data quality monitoring
    """
    
    def __init__(
        self,
        cross_source_validator: CrossSourceValidator,
        timestamp_validator: TimestampValidator,
        aggregation_validator: AggregationValidator,
        config: StreamingValidationConfig = None
    ):
        self.cross_source_validator = cross_source_validator
        self.timestamp_validator = timestamp_validator
        self.aggregation_validator = aggregation_validator
        self.config = config or StreamingValidationConfig()
        
        # Validation state
        self.last_validation_time = None
        self.candle_buffer: Dict[str, List[OHLCV]] = {}
        self.validation_callbacks: List[Callable] = []
        
        # Statistics
        self.validation_stats = {
            'total_validations': 0,
            'passed_validations': 0,
            'failed_validations': 0,
            'last_validation_time': None
        }
    
    def add_validation_callback(self, callback: Callable[[ValidationReport], None]):
        """Add a callback to be called when validation results are available"""
        self.validation_callbacks.append(callback)
    
    async def validate_streaming_candle(
        self,
        candle: OHLCV,
        symbol: str,
        timeframe: str
    ) -> ValidationResult:
        """
        Validate a single streaming candle in real-time.
        
        Args:
            candle: The streaming candle to validate
            symbol: Trading symbol
            timeframe: Candle timeframe
            
        Returns:
            ValidationResult with validation status
        """
        try:
            # Add to buffer
            key = f"{symbol}_{timeframe}"
            if key not in self.candle_buffer:
                self.candle_buffer[key] = []
            
            self.candle_buffer[key].append(candle)
            
            # Keep only recent candles
            cutoff_time = datetime.utcnow() - timedelta(hours=1)
            self.candle_buffer[key] = [
                c for c in self.candle_buffer[key] 
                if c.timestamp > cutoff_time
            ]
            
            # Run timestamp validation
            if self.config.enable_timestamp_validation:
                timestamp_result = self._validate_timestamp(candle, symbol, timeframe)
                if timestamp_result.status == ValidationStatus.FAIL:
                    return timestamp_result
            
            # Check if we should run full validation
            if self._should_run_validation():
                return await self._run_full_validation(symbol, timeframe)
            
            # Basic validation passed
            return ValidationResult(
                test_name="streaming_candle_validation",
                status=ValidationStatus.PASS,
                message="Streaming candle validation passed",
                details={
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'timestamp': candle.timestamp.isoformat(),
                    'validation_type': 'basic'
                }
            )
            
        except Exception as e:
            logger.error(f"Error validating streaming candle: {e}")
            return ValidationResult(
                test_name="streaming_candle_validation",
                status=ValidationStatus.FAIL,
                message=f"Streaming candle validation failed: {str(e)}",
                details={'error': str(e)}
            )
    
    def _validate_timestamp(self, candle: OHLCV, symbol: str, timeframe: str) -> ValidationResult:
        """Validate timestamp stability for a single candle"""
        try:
            # Check if timestamp is recent
            now = datetime.utcnow()
            time_diff = abs((now - candle.timestamp).total_seconds())
            
            if time_diff > self.config.max_gap_seconds:
                return ValidationResult(
                    test_name="timestamp_freshness",
                    status=ValidationStatus.WARNING,
                    message=f"Timestamp is {time_diff:.1f} seconds old",
                    details={
                        'timestamp': candle.timestamp.isoformat(),
                        'age_seconds': time_diff,
                        'max_gap_seconds': self.config.max_gap_seconds
                    }
                )
            
            # Check timezone
            if candle.timestamp.tzinfo != timezone.utc:
                return ValidationResult(
                    test_name="timestamp_timezone",
                    status=ValidationStatus.FAIL,
                    message="Timestamp is not in UTC timezone",
                    details={
                        'timestamp': candle.timestamp.isoformat(),
                        'timezone': str(candle.timestamp.tzinfo)
                    }
                )
            
            return ValidationResult(
                test_name="timestamp_validation",
                status=ValidationStatus.PASS,
                message="Timestamp validation passed",
                details={
                    'timestamp': candle.timestamp.isoformat(),
                    'age_seconds': time_diff
                }
            )
            
        except Exception as e:
            return ValidationResult(
                test_name="timestamp_validation",
                status=ValidationStatus.FAIL,
                message=f"Timestamp validation failed: {str(e)}",
                details={'error': str(e)}
            )
    
    def _should_run_validation(self) -> bool:
        """Check if we should run full validation based on config"""
        if self.last_validation_time is None:
            return True
        
        time_since_last = (datetime.utcnow() - self.last_validation_time).total_seconds()
        return time_since_last >= self.config.validation_interval_seconds
    
    async def _run_full_validation(self, symbol: str, timeframe: str) -> ValidationResult:
        """Run full validation suite for streaming data"""
        try:
            # Create validation report
            report = ValidationReport(
                report_id=f"streaming_validation_{symbol}_{timeframe}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
                start_time=datetime.utcnow(),
                end_time=datetime.utcnow()
            )
            
            # Get recent candles
            key = f"{symbol}_{timeframe}"
            recent_candles = self.candle_buffer.get(key, [])
            
            if not recent_candles:
                return ValidationResult(
                    test_name="streaming_validation",
                    status=ValidationStatus.SKIP,
                    message="No recent candles available for validation",
                    details={'symbol': symbol, 'timeframe': timeframe}
                )
            
            # Limit candles for performance
            recent_candles = recent_candles[-self.config.max_candles_per_validation:]
            
            # Run timestamp stability validation
            if self.config.enable_timestamp_validation:
                timestamps = [c.timestamp for c in recent_candles]
                timestamp_result = self.timestamp_validator.validate_timestamp_stability(
                    timestamps=timestamps,
                    expected_interval_seconds=self._get_interval_seconds(timeframe),
                    test_name="streaming_timestamp_stability"
                )
                report.add_result(timestamp_result)
            
            # Run cross-source validation
            if self.config.enable_cross_source_validation:
                cross_source_result = await self._validate_cross_source(
                    symbol, timeframe, recent_candles
                )
                report.add_result(cross_source_result)
            
            # Run aggregation validation
            if self.config.enable_aggregation_validation:
                aggregation_result = await self._validate_aggregation(
                    symbol, timeframe, recent_candles
                )
                report.add_result(aggregation_result)
            
            # Update statistics
            self._update_validation_stats(report)
            
            # Call callbacks
            for callback in self.validation_callbacks:
                try:
                    callback(report)
                except Exception as e:
                    logger.error(f"Error in validation callback: {e}")
            
            # Return overall result
            overall_status = report.get_status()
            return ValidationResult(
                test_name="streaming_validation",
                status=overall_status,
                message=f"Streaming validation {overall_status.value.lower()}",
                details={
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'total_tests': report.total_tests,
                    'passed_tests': report.passed_tests,
                    'failed_tests': report.failed_tests,
                    'success_rate': report.get_success_rate()
                }
            )
            
        except Exception as e:
            logger.error(f"Error in full validation: {e}")
            return ValidationResult(
                test_name="streaming_validation",
                status=ValidationStatus.FAIL,
                message=f"Full validation failed: {str(e)}",
                details={'error': str(e)}
            )
        finally:
            self.last_validation_time = datetime.utcnow()
    
    async def _validate_cross_source(
        self,
        symbol: str,
        timeframe: str,
        streaming_candles: List[OHLCV]
    ) -> ValidationResult:
        """Validate streaming candles against external source (Binance)"""
        try:
            if not streaming_candles:
                return ValidationResult(
                    test_name="cross_source_validation",
                    status=ValidationStatus.SKIP,
                    message="No streaming candles available for cross-source validation"
                )
            
            # Get recent time range
            start_time = min(c.timestamp for c in streaming_candles)
            end_time = max(c.timestamp for c in streaming_candles)
            
            # Validate against Binance
            result = await self.cross_source_validator.validate_timeframe_consistency(
                symbol=symbol,
                timeframe=timeframe,
                start_date=start_time,
                end_date=end_time,
                max_candles=self.config.max_candles_per_validation
            )
            
            return result
            
        except Exception as e:
            return ValidationResult(
                test_name="cross_source_validation",
                status=ValidationStatus.FAIL,
                message=f"Cross-source validation failed: {str(e)}",
                details={'error': str(e)}
            )
    
    async def _validate_aggregation(
        self,
        symbol: str,
        timeframe: str,
        streaming_candles: List[OHLCV]
    ) -> ValidationResult:
        """Validate aggregation consistency for streaming candles"""
        try:
            if not streaming_candles:
                return ValidationResult(
                    test_name="aggregation_validation",
                    status=ValidationStatus.SKIP,
                    message="No streaming candles available for aggregation validation"
                )
            
            # For now, just validate basic aggregation rules
            # In a full implementation, this would validate against aggregated timeframes
            
            # Check OHLC consistency
            for candle in streaming_candles:
                if candle.high < candle.low:
                    return ValidationResult(
                        test_name="aggregation_validation",
                        status=ValidationStatus.FAIL,
                        message="Invalid OHLC: high < low",
                        details={
                            'symbol': symbol,
                            'timeframe': timeframe,
                            'timestamp': candle.timestamp.isoformat(),
                            'high': candle.high,
                            'low': candle.low
                        }
                    )
                
                if candle.high < candle.open or candle.high < candle.close:
                    return ValidationResult(
                        test_name="aggregation_validation",
                        status=ValidationStatus.FAIL,
                        message="Invalid OHLC: high < open or close",
                        details={
                            'symbol': symbol,
                            'timeframe': timeframe,
                            'timestamp': candle.timestamp.isoformat(),
                            'high': candle.high,
                            'open': candle.open,
                            'close': candle.close
                        }
                    )
            
            return ValidationResult(
                test_name="aggregation_validation",
                status=ValidationStatus.PASS,
                message="Aggregation validation passed",
                details={
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'candles_validated': len(streaming_candles)
                }
            )
            
        except Exception as e:
            return ValidationResult(
                test_name="aggregation_validation",
                status=ValidationStatus.FAIL,
                message=f"Aggregation validation failed: {str(e)}",
                details={'error': str(e)}
            )
    
    def _get_interval_seconds(self, timeframe: str) -> float:
        """Get interval in seconds for a timeframe"""
        timeframe_map = {
            '1s': 1.0,
            '5s': 5.0,
            '15s': 15.0,
            '1m': 60.0,
            '5m': 300.0,
            '15m': 900.0,
            '1h': 3600.0,
            '4h': 14400.0,
            '1d': 86400.0
        }
        return timeframe_map.get(timeframe, 60.0)
    
    def _update_validation_stats(self, report: ValidationReport):
        """Update validation statistics"""
        self.validation_stats['total_validations'] += 1
        self.validation_stats['last_validation_time'] = datetime.utcnow()
        
        if report.get_status() == ValidationStatus.PASS:
            self.validation_stats['passed_validations'] += 1
        else:
            self.validation_stats['failed_validations'] += 1
    
    def get_validation_stats(self) -> Dict[str, Any]:
        """Get current validation statistics"""
        total = self.validation_stats['total_validations']
        if total == 0:
            success_rate = 0.0
        else:
            success_rate = (self.validation_stats['passed_validations'] / total) * 100
        
        return {
            **self.validation_stats,
            'success_rate': success_rate
        }
    
    def clear_candle_buffer(self, symbol: str = None, timeframe: str = None):
        """Clear candle buffer for specific symbol/timeframe or all"""
        if symbol and timeframe:
            key = f"{symbol}_{timeframe}"
            if key in self.candle_buffer:
                del self.candle_buffer[key]
        else:
            self.candle_buffer.clear()
    
    async def validate_streaming_service_output(
        self,
        streaming_candles: List[OHLCV],
        symbol: str,
        timeframe: str
    ) -> ValidationReport:
        """
        Validate a batch of streaming service output candles.
        
        This method is designed to be called by the streaming service
        to validate its output before serving or persisting.
        """
        try:
            report = ValidationReport(
                report_id=f"streaming_service_validation_{symbol}_{timeframe}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
                start_time=datetime.utcnow(),
                end_time=datetime.utcnow()
            )
            
            if not streaming_candles:
                error_result = ValidationResult(
                    test_name="streaming_service_validation",
                    status=ValidationStatus.FAIL,
                    message="No candles provided for validation",
                    details={'symbol': symbol, 'timeframe': timeframe}
                )
                report.add_result(error_result)
                return report
            
            # Validate each candle
            for i, candle in enumerate(streaming_candles):
                candle_result = await self.validate_streaming_candle(candle, symbol, timeframe)
                candle_result.test_name = f"candle_validation_{i}"
                report.add_result(candle_result)
            
            # Run batch validation
            batch_result = await self._run_full_validation(symbol, timeframe)
            report.add_result(batch_result)
            
            report.end_time = datetime.utcnow()
            return report
            
        except Exception as e:
            logger.error(f"Error validating streaming service output: {e}")
            error_result = ValidationResult(
                test_name="streaming_service_validation",
                status=ValidationStatus.FAIL,
                message=f"Streaming service validation failed: {str(e)}",
                details={'error': str(e)}
            )
            report.add_result(error_result)
            return report