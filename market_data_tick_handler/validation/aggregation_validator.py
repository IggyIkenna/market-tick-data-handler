"""
Aggregation Validator

Validates aggregation consistency and correctness across different timeframes.
Implements comprehensive aggregation validation for market data.
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass

from .validation_results import ValidationResult, ValidationStatus
from .cross_source_validator import OHLCV

logger = logging.getLogger(__name__)


@dataclass
class AggregationValidationConfig:
    """Configuration for aggregation validation"""
    price_tolerance: float = 0.0001  # 0.01% price difference tolerance
    volume_tolerance: float = 0.01   # 1% volume difference tolerance
    timestamp_tolerance_seconds: float = 1.0
    min_candles_for_validation: int = 10


class AggregationValidator:
    """Validates aggregation consistency and correctness"""
    
    def __init__(self, config: AggregationValidationConfig = None):
        self.config = config or AggregationValidationConfig()
    
    def validate_ohlc_preservation(
        self, 
        base_candles: List[OHLCV], 
        aggregated_candles: List[OHLCV],
        test_name: str = "ohlc_preservation"
    ) -> ValidationResult:
        """
        Validate that OHLC values are correctly preserved during aggregation
        
        Rules:
        1. Open = First candle's open
        2. High = Maximum of all highs
        3. Low = Minimum of all lows  
        4. Close = Last candle's close
        5. Volume = Sum of all volumes
        """
        start_time = datetime.utcnow()
        
        try:
            if not base_candles or not aggregated_candles:
                return ValidationResult(
                    test_name=test_name,
                    status=ValidationStatus.FAIL,
                    message="Missing base or aggregated candles for OHLC validation"
                )
            
            # Group base candles by aggregated timeframe boundaries
            grouped_candles = self._group_candles_by_timeframe(base_candles, aggregated_candles)
            
            if not grouped_candles:
                return ValidationResult(
                    test_name=test_name,
                    status=ValidationStatus.FAIL,
                    message="No matching candles found between base and aggregated data"
                )
            
            # Validate each group
            validation_errors = []
            for boundary, group in grouped_candles.items():
                # Find corresponding aggregated candle
                agg_candle = self._find_aggregated_candle(aggregated_candles, boundary)
                if not agg_candle:
                    continue
                
                # Validate OHLC preservation
                errors = self._validate_ohlc_group(group, agg_candle)
                if errors:
                    validation_errors.extend(errors)
            
            # Overall assessment
            passed = len(validation_errors) == 0
            status = ValidationStatus.PASS if passed else ValidationStatus.FAIL
            message = "OHLC preservation validated" if passed else "OHLC preservation errors detected"
            
            execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            
            return ValidationResult(
                test_name=test_name,
                status=status,
                message=message,
                details={
                    'total_groups': len(grouped_candles),
                    'validation_errors': validation_errors,
                    'error_count': len(validation_errors),
                    'base_candles': len(base_candles),
                    'aggregated_candles': len(aggregated_candles)
                },
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            logger.error(f"OHLC preservation validation failed: {e}")
            
            return ValidationResult(
                test_name=test_name,
                status=ValidationStatus.FAIL,
                message=f"OHLC preservation validation failed: {str(e)}",
                details={'error': str(e)},
                execution_time_ms=execution_time
            )
    
    def validate_volume_aggregation(
        self, 
        base_candles: List[OHLCV], 
        aggregated_candles: List[OHLCV],
        test_name: str = "volume_aggregation"
    ) -> ValidationResult:
        """Validate that volume is correctly aggregated (sum of base volumes)"""
        start_time = datetime.utcnow()
        
        try:
            if not base_candles or not aggregated_candles:
                return ValidationResult(
                    test_name=test_name,
                    status=ValidationStatus.FAIL,
                    message="Missing base or aggregated candles for volume validation"
                )
            
            # Group base candles by aggregated timeframe boundaries
            grouped_candles = self._group_candles_by_timeframe(base_candles, aggregated_candles)
            
            if not grouped_candles:
                return ValidationResult(
                    test_name=test_name,
                    status=ValidationStatus.FAIL,
                    message="No matching candles found between base and aggregated data"
                )
            
            # Validate volume aggregation
            volume_errors = []
            for boundary, group in grouped_candles.items():
                agg_candle = self._find_aggregated_candle(aggregated_candles, boundary)
                if not agg_candle:
                    continue
                
                # Calculate expected volume (sum of base volumes)
                expected_volume = sum(candle.volume for candle in group)
                actual_volume = agg_candle.volume
                
                # Check volume difference
                if expected_volume > 0:
                    volume_diff_pct = abs(actual_volume - expected_volume) / expected_volume
                    if volume_diff_pct > self.config.volume_tolerance:
                        volume_errors.append({
                            'boundary': boundary.isoformat(),
                            'expected_volume': expected_volume,
                            'actual_volume': actual_volume,
                            'difference_pct': volume_diff_pct * 100,
                            'base_candles_count': len(group)
                        })
            
            # Overall assessment
            passed = len(volume_errors) == 0
            status = ValidationStatus.PASS if passed else ValidationStatus.FAIL
            message = "Volume aggregation validated" if passed else "Volume aggregation errors detected"
            
            execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            
            return ValidationResult(
                test_name=test_name,
                status=status,
                message=message,
                details={
                    'total_groups': len(grouped_candles),
                    'volume_errors': volume_errors,
                    'error_count': len(volume_errors),
                    'base_candles': len(base_candles),
                    'aggregated_candles': len(aggregated_candles)
                },
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            logger.error(f"Volume aggregation validation failed: {e}")
            
            return ValidationResult(
                test_name=test_name,
                status=ValidationStatus.FAIL,
                message=f"Volume aggregation validation failed: {str(e)}",
                details={'error': str(e)},
                execution_time_ms=execution_time
            )
    
    def validate_timeframe_boundaries(
        self, 
        candles: List[OHLCV], 
        expected_timeframe: str,
        test_name: str = "timeframe_boundaries"
    ) -> ValidationResult:
        """Validate that candles align with expected timeframe boundaries"""
        start_time = datetime.utcnow()
        
        try:
            if not candles:
                return ValidationResult(
                    test_name=test_name,
                    status=ValidationStatus.FAIL,
                    message="No candles provided for boundary validation"
                )
            
            # Check boundary alignment
            boundary_errors = []
            for candle in candles:
                expected_boundary = self._get_timeframe_boundary(candle.timestamp, expected_timeframe)
                if candle.timestamp != expected_boundary:
                    boundary_errors.append({
                        'timestamp': candle.timestamp.isoformat(),
                        'expected_boundary': expected_boundary.isoformat(),
                        'offset_seconds': (candle.timestamp - expected_boundary).total_seconds()
                    })
            
            # Overall assessment
            passed = len(boundary_errors) == 0
            status = ValidationStatus.PASS if passed else ValidationStatus.FAIL
            message = "Timeframe boundaries validated" if passed else "Timeframe boundary misalignment detected"
            
            execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            
            return ValidationResult(
                test_name=test_name,
                status=status,
                message=message,
                details={
                    'total_candles': len(candles),
                    'boundary_errors': boundary_errors,
                    'error_count': len(boundary_errors),
                    'expected_timeframe': expected_timeframe
                },
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            logger.error(f"Timeframe boundary validation failed: {e}")
            
            return ValidationResult(
                test_name=test_name,
                status=ValidationStatus.FAIL,
                message=f"Timeframe boundary validation failed: {str(e)}",
                details={'error': str(e)},
                execution_time_ms=execution_time
            )
    
    def validate_aggregation_consistency(
        self, 
        source_candles: List[OHLCV], 
        target_candles: List[OHLCV],
        test_name: str = "aggregation_consistency"
    ) -> ValidationResult:
        """Validate that target candles are consistent with source candles when aggregated"""
        start_time = datetime.utcnow()
        
        try:
            if not source_candles or not target_candles:
                return ValidationResult(
                    test_name=test_name,
                    status=ValidationStatus.FAIL,
                    message="Missing source or target candles for consistency validation"
                )
            
            # Determine target timeframe from target candles
            if not target_candles:
                return ValidationResult(
                    test_name=test_name,
                    status=ValidationStatus.FAIL,
                    message="No target candles to determine timeframe"
                )
            
            target_timeframe = target_candles[0].timeframe
            
            # Aggregate source candles to target timeframe
            aggregated_source = self._aggregate_candles(source_candles, target_timeframe)
            
            if not aggregated_source:
                return ValidationResult(
                    test_name=test_name,
                    status=ValidationStatus.FAIL,
                    message="Failed to aggregate source candles"
                )
            
            # Compare aggregated source with target
            comparison = self._compare_candle_sets(aggregated_source, target_candles)
            
            # Overall assessment
            passed = comparison['passed']
            status = ValidationStatus.PASS if passed else ValidationStatus.FAIL
            message = "Aggregation consistency validated" if passed else "Aggregation inconsistency detected"
            
            execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            
            return ValidationResult(
                test_name=test_name,
                status=status,
                message=message,
                details={
                    'source_candles': len(source_candles),
                    'target_candles': len(target_candles),
                    'aggregated_source': len(aggregated_source),
                    'comparison': comparison,
                    'target_timeframe': target_timeframe
                },
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            logger.error(f"Aggregation consistency validation failed: {e}")
            
            return ValidationResult(
                test_name=test_name,
                status=ValidationStatus.FAIL,
                message=f"Aggregation consistency validation failed: {str(e)}",
                details={'error': str(e)},
                execution_time_ms=execution_time
            )
    
    def _group_candles_by_timeframe(
        self, 
        base_candles: List[OHLCV], 
        aggregated_candles: List[OHLCV]
    ) -> Dict[datetime, List[OHLCV]]:
        """Group base candles by aggregated timeframe boundaries"""
        if not aggregated_candles:
            return {}
        
        # Determine target timeframe from aggregated candles
        target_timeframe = aggregated_candles[0].timeframe
        
        # Group base candles by target timeframe boundaries
        grouped = {}
        for candle in base_candles:
            boundary = self._get_timeframe_boundary(candle.timestamp, target_timeframe)
            if boundary not in grouped:
                grouped[boundary] = []
            grouped[boundary].append(candle)
        
        return grouped
    
    def _find_aggregated_candle(self, aggregated_candles: List[OHLCV], boundary: datetime) -> Optional[OHLCV]:
        """Find aggregated candle for a specific boundary"""
        for candle in aggregated_candles:
            if abs((candle.timestamp - boundary).total_seconds()) <= self.config.timestamp_tolerance_seconds:
                return candle
        return None
    
    def _validate_ohlc_group(self, group: List[OHLCV], agg_candle: OHLCV) -> List[Dict[str, Any]]:
        """Validate OHLC preservation for a group of candles"""
        if not group:
            return []
        
        # Sort group by timestamp
        group.sort(key=lambda x: x.timestamp)
        
        errors = []
        
        # Check Open (first candle's open)
        expected_open = group[0].open
        if abs(agg_candle.open - expected_open) / expected_open > self.config.price_tolerance:
            errors.append({
                'field': 'open',
                'expected': expected_open,
                'actual': agg_candle.open,
                'difference_pct': abs(agg_candle.open - expected_open) / expected_open * 100
            })
        
        # Check High (maximum of all highs)
        expected_high = max(candle.high for candle in group)
        if abs(agg_candle.high - expected_high) / expected_high > self.config.price_tolerance:
            errors.append({
                'field': 'high',
                'expected': expected_high,
                'actual': agg_candle.high,
                'difference_pct': abs(agg_candle.high - expected_high) / expected_high * 100
            })
        
        # Check Low (minimum of all lows)
        expected_low = min(candle.low for candle in group)
        if abs(agg_candle.low - expected_low) / expected_low > self.config.price_tolerance:
            errors.append({
                'field': 'low',
                'expected': expected_low,
                'actual': agg_candle.low,
                'difference_pct': abs(agg_candle.low - expected_low) / expected_low * 100
            })
        
        # Check Close (last candle's close)
        expected_close = group[-1].close
        if abs(agg_candle.close - expected_close) / expected_close > self.config.price_tolerance:
            errors.append({
                'field': 'close',
                'expected': expected_close,
                'actual': agg_candle.close,
                'difference_pct': abs(agg_candle.close - expected_close) / expected_close * 100
            })
        
        return errors
    
    def _get_timeframe_boundary(self, timestamp: datetime, timeframe: str) -> datetime:
        """Get the timeframe boundary for a given timestamp"""
        if timeframe == '15s':
            return timestamp.replace(second=(timestamp.second // 15) * 15, microsecond=0)
        elif timeframe == '1m':
            return timestamp.replace(second=0, microsecond=0)
        elif timeframe == '5m':
            return timestamp.replace(minute=(timestamp.minute // 5) * 5, second=0, microsecond=0)
        elif timeframe == '15m':
            return timestamp.replace(minute=(timestamp.minute // 15) * 15, second=0, microsecond=0)
        elif timeframe == '1h':
            return timestamp.replace(minute=0, second=0, microsecond=0)
        elif timeframe == '4h':
            return timestamp.replace(hour=(timestamp.hour // 4) * 4, minute=0, second=0, microsecond=0)
        elif timeframe == '1d':
            return timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            return timestamp
    
    def _aggregate_candles(self, candles: List[OHLCV], target_timeframe: str) -> List[OHLCV]:
        """Aggregate candles to target timeframe"""
        if not candles:
            return []
        
        # Group candles by target timeframe boundaries
        grouped = {}
        for candle in candles:
            boundary = self._get_timeframe_boundary(candle.timestamp, target_timeframe)
            if boundary not in grouped:
                grouped[boundary] = []
            grouped[boundary].append(candle)
        
        # Aggregate each group
        aggregated = []
        for boundary, group in grouped.items():
            if not group:
                continue
                
            # Sort by timestamp
            group.sort(key=lambda x: x.timestamp)
            
            # Create aggregated candle
            agg_candle = OHLCV(
                timestamp=boundary,
                open=group[0].open,
                high=max(c.high for c in group),
                low=min(c.low for c in group),
                close=group[-1].close,
                volume=sum(c.volume for c in group),
                symbol=group[0].symbol,
                timeframe=target_timeframe,
                source=group[0].source
            )
            
            aggregated.append(agg_candle)
        
        return sorted(aggregated, key=lambda x: x.timestamp)
    
    def _compare_candle_sets(self, candles1: List[OHLCV], candles2: List[OHLCV]) -> Dict[str, Any]:
        """Compare two sets of candles"""
        if not candles1 or not candles2:
            return {'passed': False, 'message': 'One or both candle sets empty'}
        
        # Create timestamp lookup
        dict1 = {c.timestamp: c for c in candles1}
        dict2 = {c.timestamp: c for c in candles2}
        
        # Find common timestamps
        common_timestamps = set(dict1.keys()) & set(dict2.keys())
        
        if not common_timestamps:
            return {'passed': False, 'message': 'No common timestamps found'}
        
        # Compare OHLCV values
        errors = []
        for timestamp in sorted(common_timestamps):
            c1 = dict1[timestamp]
            c2 = dict2[timestamp]
            
            for field in ['open', 'high', 'low', 'close', 'volume']:
                val1 = getattr(c1, field)
                val2 = getattr(c2, field)
                
                tolerance = self.config.volume_tolerance if field == 'volume' else self.config.price_tolerance
                if abs(val1 - val2) / val1 > tolerance:
                    errors.append({
                        'timestamp': timestamp.isoformat(),
                        'field': field,
                        'value1': val1,
                        'value2': val2,
                        'difference_pct': abs(val1 - val2) / val1 * 100
                    })
        
        return {
            'passed': len(errors) == 0,
            'common_candles': len(common_timestamps),
            'total_candles1': len(candles1),
            'total_candles2': len(candles2),
            'errors': errors,
            'error_count': len(errors)
        }
