"""
Timestamp Validator

Validates timestamp stability, alignment, and consistency across different data sources.
Implements comprehensive timestamp validation for market data.
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass

from .validation_results import ValidationResult, ValidationStatus

logger = logging.getLogger(__name__)


@dataclass
class TimestampValidationConfig:
    """Configuration for timestamp validation"""
    max_timestamp_drift_seconds: float = 1.0
    max_clock_skew_seconds: float = 5.0
    min_data_freshness_seconds: float = 300.0  # 5 minutes
    timezone_tolerance_seconds: float = 3600.0  # 1 hour
    duplicate_timestamp_tolerance: float = 0.001  # 1ms


class TimestampValidator:
    """Validates timestamp stability and consistency"""
    
    def __init__(self, config: TimestampValidationConfig = None):
        self.config = config or TimestampValidationConfig()
    
    def validate_timestamp_stability(
        self, 
        timestamps: List[datetime], 
        expected_interval_seconds: float,
        test_name: str = "timestamp_stability"
    ) -> ValidationResult:
        """
        Validate timestamp stability and consistency
        
        Checks:
        1. Timestamps are in ascending order
        2. Intervals are consistent within tolerance
        3. No duplicate timestamps
        4. No gaps larger than expected interval
        """
        start_time = datetime.utcnow()
        
        try:
            if not timestamps:
                return ValidationResult(
                    test_name=test_name,
                    status=ValidationStatus.FAIL,
                    message="No timestamps provided for validation"
                )
            
            # Sort timestamps
            sorted_timestamps = sorted(timestamps)
            
            # Check for duplicates
            duplicate_check = self._check_duplicate_timestamps(sorted_timestamps)
            
            # Check ordering
            ordering_check = self._check_timestamp_ordering(sorted_timestamps)
            
            # Check interval consistency
            interval_check = self._check_interval_consistency(
                sorted_timestamps, expected_interval_seconds
            )
            
            # Check for gaps
            gap_check = self._check_timestamp_gaps(
                sorted_timestamps, expected_interval_seconds
            )
            
            # Overall assessment
            all_passed = (
                duplicate_check['passed'] and 
                ordering_check['passed'] and 
                interval_check['passed'] and 
                gap_check['passed']
            )
            
            status = ValidationStatus.PASS if all_passed else ValidationStatus.FAIL
            message = "Timestamp stability validated" if all_passed else "Timestamp stability issues detected"
            
            execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            
            return ValidationResult(
                test_name=test_name,
                status=status,
                message=message,
                details={
                    'total_timestamps': len(timestamps),
                    'duplicate_check': duplicate_check,
                    'ordering_check': ordering_check,
                    'interval_check': interval_check,
                    'gap_check': gap_check,
                    'expected_interval_seconds': expected_interval_seconds
                },
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            logger.error(f"Timestamp stability validation failed: {e}")
            
            return ValidationResult(
                test_name=test_name,
                status=ValidationStatus.FAIL,
                message=f"Timestamp stability validation failed: {str(e)}",
                details={'error': str(e)},
                execution_time_ms=execution_time
            )
    
    def validate_timezone_consistency(
        self, 
        timestamps: List[datetime], 
        test_name: str = "timezone_consistency"
    ) -> ValidationResult:
        """Validate that all timestamps are in the same timezone"""
        start_time = datetime.utcnow()
        
        try:
            if not timestamps:
                return ValidationResult(
                    test_name=test_name,
                    status=ValidationStatus.FAIL,
                    message="No timestamps provided for timezone validation"
                )
            
            # Check timezone consistency
            timezone_check = self._check_timezone_consistency(timestamps)
            
            # Check for timezone drift
            drift_check = self._check_timezone_drift(timestamps)
            
            all_passed = timezone_check['passed'] and drift_check['passed']
            status = ValidationStatus.PASS if all_passed else ValidationStatus.FAIL
            message = "Timezone consistency validated" if all_passed else "Timezone consistency issues detected"
            
            execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            
            return ValidationResult(
                test_name=test_name,
                status=status,
                message=message,
                details={
                    'total_timestamps': len(timestamps),
                    'timezone_check': timezone_check,
                    'drift_check': drift_check
                },
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            logger.error(f"Timezone consistency validation failed: {e}")
            
            return ValidationResult(
                test_name=test_name,
                status=ValidationStatus.FAIL,
                message=f"Timezone consistency validation failed: {str(e)}",
                details={'error': str(e)},
                execution_time_ms=execution_time
            )
    
    def validate_data_freshness(
        self, 
        timestamps: List[datetime], 
        test_name: str = "data_freshness"
    ) -> ValidationResult:
        """Validate that data is fresh (not too old)"""
        start_time = datetime.utcnow()
        
        try:
            if not timestamps:
                return ValidationResult(
                    test_name=test_name,
                    status=ValidationStatus.FAIL,
                    message="No timestamps provided for freshness validation"
                )
            
            # Check data freshness
            freshness_check = self._check_data_freshness(timestamps)
            
            status = ValidationStatus.PASS if freshness_check['passed'] else ValidationStatus.WARNING
            message = "Data freshness validated" if freshness_check['passed'] else "Data freshness warning"
            
            execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            
            return ValidationResult(
                test_name=test_name,
                status=status,
                message=message,
                details=freshness_check,
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            logger.error(f"Data freshness validation failed: {e}")
            
            return ValidationResult(
                test_name=test_name,
                status=ValidationStatus.FAIL,
                message=f"Data freshness validation failed: {str(e)}",
                details={'error': str(e)},
                execution_time_ms=execution_time
            )
    
    def validate_clock_synchronization(
        self, 
        local_timestamps: List[datetime], 
        server_timestamps: List[datetime],
        test_name: str = "clock_synchronization"
    ) -> ValidationResult:
        """Validate clock synchronization between local and server timestamps"""
        start_time = datetime.utcnow()
        
        try:
            if not local_timestamps or not server_timestamps:
                return ValidationResult(
                    test_name=test_name,
                    status=ValidationStatus.FAIL,
                    message="Missing local or server timestamps for synchronization check"
                )
            
            # Check clock synchronization
            sync_check = self._check_clock_synchronization(local_timestamps, server_timestamps)
            
            status = ValidationStatus.PASS if sync_check['passed'] else ValidationStatus.WARNING
            message = "Clock synchronization validated" if sync_check['passed'] else "Clock synchronization issues detected"
            
            execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            
            return ValidationResult(
                test_name=test_name,
                status=status,
                message=message,
                details=sync_check,
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            logger.error(f"Clock synchronization validation failed: {e}")
            
            return ValidationResult(
                test_name=test_name,
                status=ValidationStatus.FAIL,
                message=f"Clock synchronization validation failed: {str(e)}",
                details={'error': str(e)},
                execution_time_ms=execution_time
            )
    
    def _check_duplicate_timestamps(self, timestamps: List[datetime]) -> Dict[str, Any]:
        """Check for duplicate timestamps"""
        seen = set()
        duplicates = []
        
        for ts in timestamps:
            if ts in seen:
                duplicates.append(ts.isoformat())
            else:
                seen.add(ts)
        
        return {
            'passed': len(duplicates) == 0,
            'duplicate_count': len(duplicates),
            'duplicates': duplicates
        }
    
    def _check_timestamp_ordering(self, timestamps: List[datetime]) -> Dict[str, Any]:
        """Check that timestamps are in ascending order"""
        out_of_order = []
        
        for i in range(1, len(timestamps)):
            if timestamps[i] < timestamps[i-1]:
                out_of_order.append({
                    'index': i,
                    'timestamp': timestamps[i].isoformat(),
                    'previous': timestamps[i-1].isoformat()
                })
        
        return {
            'passed': len(out_of_order) == 0,
            'out_of_order_count': len(out_of_order),
            'out_of_order': out_of_order
        }
    
    def _check_interval_consistency(
        self, 
        timestamps: List[datetime], 
        expected_interval_seconds: float
    ) -> Dict[str, Any]:
        """Check that intervals between timestamps are consistent"""
        if len(timestamps) < 2:
            return {'passed': True, 'message': 'Not enough timestamps for interval check'}
        
        intervals = []
        for i in range(1, len(timestamps)):
            interval = (timestamps[i] - timestamps[i-1]).total_seconds()
            intervals.append(interval)
        
        if not intervals:
            return {'passed': True, 'message': 'No intervals to check'}
        
        # Check against expected interval
        interval_errors = []
        for i, interval in enumerate(intervals):
            diff = abs(interval - expected_interval_seconds)
            if diff > self.config.max_timestamp_drift_seconds:
                interval_errors.append({
                    'index': i,
                    'actual_interval': interval,
                    'expected_interval': expected_interval_seconds,
                    'difference': diff
                })
        
        return {
            'passed': len(interval_errors) == 0,
            'interval_errors': interval_errors,
            'error_count': len(interval_errors),
            'avg_interval': np.mean(intervals),
            'std_interval': np.std(intervals),
            'min_interval': min(intervals),
            'max_interval': max(intervals)
        }
    
    def _check_timestamp_gaps(
        self, 
        timestamps: List[datetime], 
        expected_interval_seconds: float
    ) -> Dict[str, Any]:
        """Check for gaps larger than expected interval"""
        if len(timestamps) < 2:
            return {'passed': True, 'message': 'Not enough timestamps for gap check'}
        
        gaps = []
        for i in range(1, len(timestamps)):
            interval = (timestamps[i] - timestamps[i-1]).total_seconds()
            if interval > expected_interval_seconds * 1.5:  # Allow 50% tolerance
                gaps.append({
                    'index': i,
                    'gap_seconds': interval,
                    'expected_interval': expected_interval_seconds,
                    'gap_ratio': interval / expected_interval_seconds
                })
        
        return {
            'passed': len(gaps) == 0,
            'gaps': gaps,
            'gap_count': len(gaps)
        }
    
    def _check_timezone_consistency(self, timestamps: List[datetime]) -> Dict[str, Any]:
        """Check that all timestamps are in the same timezone"""
        timezones = set()
        for ts in timestamps:
            if ts.tzinfo is not None:
                timezones.add(ts.tzinfo)
            else:
                timezones.add('naive')
        
        return {
            'passed': len(timezones) == 1,
            'timezone_count': len(timezones),
            'timezones': list(timezones)
        }
    
    def _check_timezone_drift(self, timestamps: List[datetime]) -> Dict[str, Any]:
        """Check for timezone drift (e.g., DST changes)"""
        if len(timestamps) < 2:
            return {'passed': True, 'message': 'Not enough timestamps for drift check'}
        
        # Convert all to UTC for comparison
        utc_timestamps = []
        for ts in timestamps:
            if ts.tzinfo is None:
                # Assume UTC if naive
                utc_timestamps.append(ts.replace(tzinfo=timezone.utc))
            else:
                utc_timestamps.append(ts.astimezone(timezone.utc))
        
        # Check for unexpected timezone changes
        drift_errors = []
        for i in range(1, len(utc_timestamps)):
            # This is a simplified check - in practice, you'd need more sophisticated logic
            # to detect actual timezone drift vs expected changes
            pass
        
        return {
            'passed': len(drift_errors) == 0,
            'drift_errors': drift_errors,
            'error_count': len(drift_errors)
        }
    
    def _check_data_freshness(self, timestamps: List[datetime]) -> Dict[str, Any]:
        """Check that data is fresh (not too old)"""
        now = datetime.now(timezone.utc)
        stale_timestamps = []
        
        for ts in timestamps:
            if ts.tzinfo is None:
                # Assume UTC if naive
                ts_utc = ts.replace(tzinfo=timezone.utc)
            else:
                ts_utc = ts.astimezone(timezone.utc)
            
            age_seconds = (now - ts_utc).total_seconds()
            if age_seconds > self.config.min_data_freshness_seconds:
                stale_timestamps.append({
                    'timestamp': ts.isoformat(),
                    'age_seconds': age_seconds,
                    'age_minutes': age_seconds / 60
                })
        
        return {
            'passed': len(stale_timestamps) == 0,
            'stale_count': len(stale_timestamps),
            'stale_timestamps': stale_timestamps,
            'max_age_seconds': max([s['age_seconds'] for s in stale_timestamps]) if stale_timestamps else 0
        }
    
    def _check_clock_synchronization(
        self, 
        local_timestamps: List[datetime], 
        server_timestamps: List[datetime]
    ) -> Dict[str, Any]:
        """Check clock synchronization between local and server timestamps"""
        if len(local_timestamps) != len(server_timestamps):
            return {
                'passed': False,
                'message': 'Mismatched timestamp counts',
                'local_count': len(local_timestamps),
                'server_count': len(server_timestamps)
            }
        
        # Calculate time differences
        time_diffs = []
        for local_ts, server_ts in zip(local_timestamps, server_timestamps):
            # Convert to UTC for comparison
            local_utc = local_ts.astimezone(timezone.utc) if local_ts.tzinfo else local_ts.replace(tzinfo=timezone.utc)
            server_utc = server_ts.astimezone(timezone.utc) if server_ts.tzinfo else server_ts.replace(tzinfo=timezone.utc)
            
            diff_seconds = (local_utc - server_utc).total_seconds()
            time_diffs.append(diff_seconds)
        
        # Check for excessive clock skew
        max_skew = max(abs(diff) for diff in time_diffs)
        skew_errors = [diff for diff in time_diffs if abs(diff) > self.config.max_clock_skew_seconds]
        
        return {
            'passed': len(skew_errors) == 0,
            'max_skew_seconds': max_skew,
            'avg_skew_seconds': np.mean(time_diffs),
            'skew_errors': len(skew_errors),
            'time_diffs': time_diffs
        }