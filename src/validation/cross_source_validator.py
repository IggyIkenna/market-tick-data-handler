"""
Cross-Source Data Validator

Validates data consistency between Binance CCXT and Tardis-derived candles.
Implements the three rules validation system for timestamp alignment and aggregation consistency.
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
import ccxt
import asyncio

from .validation_results import ValidationResult, ValidationStatus, ValidationReport
from ..data_downloader.tardis_connector import TardisConnector
from ..data_downloader.data_client import DataClient

logger = logging.getLogger(__name__)


@dataclass
class OHLCV:
    """OHLCV candle data structure"""
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    symbol: str
    timeframe: str
    source: str  # 'binance' or 'tardis'


class CrossSourceValidator:
    """Validates data consistency between Binance CCXT and Tardis-derived candles"""
    
    def __init__(self, data_client: DataClient, tardis_connector: TardisConnector = None):
        self.data_client = data_client
        self.tardis_connector = tardis_connector
        self.binance_exchange = ccxt.binance({
            'apiKey': '',  # Not needed for public data
            'secret': '',
            'sandbox': False,
            'enableRateLimit': True,
        })
        
        # Validation tolerances
        self.price_tolerance = 0.0001  # 0.01% price difference tolerance
        self.volume_tolerance = 0.01   # 1% volume difference tolerance
        self.timestamp_tolerance_seconds = 1  # 1 second timestamp tolerance
    
    async def validate_timeframe_consistency(
        self, 
        symbol: str, 
        timeframe: str, 
        start_date: datetime, 
        end_date: datetime,
        max_candles: int = 1000
    ) -> ValidationResult:
        """
        Validate that Binance and Tardis candles match for the same timeframe
        
        Rule 1: Timestamp Alignment - Candles should have aligned timestamps
        Rule 2: OHLC Preservation - OHLC values should match within tolerance
        Rule 3: Volume Consistency - Volume should match within tolerance
        """
        test_name = f"timeframe_consistency_{symbol}_{timeframe}"
        start_time = datetime.utcnow()
        
        try:
            # Get Binance data
            binance_candles = await self._get_binance_candles(
                symbol, timeframe, start_date, end_date, max_candles
            )
            
            # Get Tardis data
            tardis_candles = await self._get_tardis_candles(
                symbol, timeframe, start_date, end_date, max_candles
            )
            
            if not binance_candles or not tardis_candles:
                return ValidationResult(
                    test_name=test_name,
                    status=ValidationStatus.FAIL,
                    message="No data available from one or both sources",
                    details={
                        'binance_count': len(binance_candles) if binance_candles else 0,
                        'tardis_count': len(tardis_candles) if tardis_candles else 0
                    }
                )
            
            # Align timestamps and compare
            aligned_data = self._align_candles(binance_candles, tardis_candles)
            
            if not aligned_data:
                return ValidationResult(
                    test_name=test_name,
                    status=ValidationStatus.FAIL,
                    message="No aligned candles found between sources",
                    details={
                        'binance_count': len(binance_candles),
                        'tardis_count': len(tardis_candles)
                    }
                )
            
            # Apply three rules validation
            validation_results = self._apply_three_rules(aligned_data)
            
            execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            
            # Determine overall status
            if validation_results['rule_1_passed'] and validation_results['rule_2_passed'] and validation_results['rule_3_passed']:
                status = ValidationStatus.PASS
                message = "All three rules passed - data consistency validated"
            elif validation_results['rule_1_passed'] and validation_results['rule_2_passed']:
                status = ValidationStatus.WARNING
                message = "Rules 1&2 passed, Rule 3 failed - volume inconsistency detected"
            elif validation_results['rule_1_passed']:
                status = ValidationStatus.FAIL
                message = "Rule 1 passed, Rules 2&3 failed - OHLC and volume issues detected"
            else:
                status = ValidationStatus.FAIL
                message = "Rule 1 failed - timestamp alignment issues detected"
            
            return ValidationResult(
                test_name=test_name,
                status=status,
                message=message,
                details={
                    'aligned_candles': len(aligned_data),
                    'binance_candles': len(binance_candles),
                    'tardis_candles': len(tardis_candles),
                    'validation_results': validation_results,
                    'timeframe': timeframe,
                    'symbol': symbol,
                    'date_range': {
                        'start': start_date.isoformat(),
                        'end': end_date.isoformat()
                    }
                },
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            logger.error(f"Validation failed for {test_name}: {e}")
            
            return ValidationResult(
                test_name=test_name,
                status=ValidationStatus.FAIL,
                message=f"Validation failed with error: {str(e)}",
                details={'error': str(e)},
                execution_time_ms=execution_time
            )
    
    async def validate_aggregation_consistency(
        self,
        symbol: str,
        base_timeframe: str,
        aggregated_timeframe: str,
        start_date: datetime,
        end_date: datetime,
        max_candles: int = 1000
    ) -> ValidationResult:
        """
        Validate that aggregated timeframes match between sources
        
        Example: Binance 1m aggregated to 5m should match Tardis 5m
        """
        test_name = f"aggregation_consistency_{symbol}_{base_timeframe}_to_{aggregated_timeframe}"
        start_time = datetime.utcnow()
        
        try:
            # Get base timeframe data from both sources
            binance_base = await self._get_binance_candles(
                symbol, base_timeframe, start_date, end_date, max_candles
            )
            tardis_base = await self._get_tardis_candles(
                symbol, base_timeframe, start_date, end_date, max_candles
            )
            
            # Get aggregated timeframe data from both sources
            binance_agg = await self._get_binance_candles(
                symbol, aggregated_timeframe, start_date, end_date, max_candles
            )
            tardis_agg = await self._get_tardis_candles(
                symbol, aggregated_timeframe, start_date, end_date, max_candles
            )
            
            if not all([binance_base, tardis_base, binance_agg, tardis_agg]):
                return ValidationResult(
                    test_name=test_name,
                    status=ValidationStatus.FAIL,
                    message="Missing data from one or more sources",
                    details={
                        'binance_base_count': len(binance_base) if binance_base else 0,
                        'tardis_base_count': len(tardis_base) if tardis_base else 0,
                        'binance_agg_count': len(binance_agg) if binance_agg else 0,
                        'tardis_agg_count': len(tardis_agg) if tardis_agg else 0
                    }
                )
            
            # Aggregate base timeframes
            binance_aggregated = self._aggregate_candles(binance_base, aggregated_timeframe)
            tardis_aggregated = self._aggregate_candles(tardis_base, aggregated_timeframe)
            
            # Compare aggregated vs direct aggregated
            binance_comparison = self._compare_candles(binance_aggregated, binance_agg)
            tardis_comparison = self._compare_candles(tardis_aggregated, tardis_agg)
            
            # Cross-source comparison
            cross_comparison = self._compare_candles(binance_agg, tardis_agg)
            
            execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            
            # Determine status
            all_passed = (
                binance_comparison['passed'] and 
                tardis_comparison['passed'] and 
                cross_comparison['passed']
            )
            
            status = ValidationStatus.PASS if all_passed else ValidationStatus.FAIL
            message = "Aggregation consistency validated" if all_passed else "Aggregation inconsistency detected"
            
            return ValidationResult(
                test_name=test_name,
                status=status,
                message=message,
                details={
                    'binance_aggregation': binance_comparison,
                    'tardis_aggregation': tardis_comparison,
                    'cross_source': cross_comparison,
                    'base_timeframe': base_timeframe,
                    'aggregated_timeframe': aggregated_timeframe,
                    'symbol': symbol
                },
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            logger.error(f"Aggregation validation failed for {test_name}: {e}")
            
            return ValidationResult(
                test_name=test_name,
                status=ValidationStatus.FAIL,
                message=f"Aggregation validation failed: {str(e)}",
                details={'error': str(e)},
                execution_time_ms=execution_time
            )
    
    async def validate_timestamp_alignment(
        self,
        symbol: str,
        timeframe: str,
        start_date: datetime,
        end_date: datetime,
        max_candles: int = 1000
    ) -> ValidationResult:
        """Validate timestamp alignment between sources"""
        test_name = f"timestamp_alignment_{symbol}_{timeframe}"
        start_time = datetime.utcnow()
        
        try:
            binance_candles = await self._get_binance_candles(
                symbol, timeframe, start_date, end_date, max_candles
            )
            tardis_candles = await self._get_tardis_candles(
                symbol, timeframe, start_date, end_date, max_candles
            )
            
            if not binance_candles or not tardis_candles:
                return ValidationResult(
                    test_name=test_name,
                    status=ValidationStatus.FAIL,
                    message="No data available for timestamp alignment check"
                )
            
            # Check timestamp alignment
            alignment_results = self._check_timestamp_alignment(binance_candles, tardis_candles)
            
            execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            
            status = ValidationStatus.PASS if alignment_results['aligned'] else ValidationStatus.FAIL
            message = "Timestamps properly aligned" if alignment_results['aligned'] else "Timestamp misalignment detected"
            
            return ValidationResult(
                test_name=test_name,
                status=status,
                message=message,
                details=alignment_results,
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            logger.error(f"Timestamp alignment validation failed for {test_name}: {e}")
            
            return ValidationResult(
                test_name=test_name,
                status=ValidationStatus.FAIL,
                message=f"Timestamp alignment validation failed: {str(e)}",
                details={'error': str(e)},
                execution_time_ms=execution_time
            )
    
    async def _get_binance_candles(
        self, 
        symbol: str, 
        timeframe: str, 
        start_date: datetime, 
        end_date: datetime,
        max_candles: int
    ) -> List[OHLCV]:
        """Get candles from Binance via CCXT"""
        try:
            # Convert symbol format (e.g., 'BTC-USDT' for Binance)
            binance_symbol = symbol.replace(':', '/').split('/')[-1] if ':' in symbol else symbol
            
            # Convert timeframe to CCXT format
            timeframe_map = {
                '15s': '15s',
                '1m': '1m',
                '5m': '5m',
                '15m': '15m',
                '1h': '1h',
                '4h': '4h',
                '1d': '1d'
            }
            ccxt_timeframe = timeframe_map.get(timeframe, timeframe)
            
            # Calculate limit based on timeframe and date range
            days_diff = (end_date - start_date).days
            if timeframe == '15s':
                limit = min(days_diff * 24 * 4 * 60, max_candles)  # 4 candles per minute
            elif timeframe == '1m':
                limit = min(days_diff * 24 * 60, max_candles)
            elif timeframe == '5m':
                limit = min(days_diff * 24 * 12, max_candles)
            elif timeframe == '1h':
                limit = min(days_diff * 24, max_candles)
            else:
                limit = max_candles
            
            # Fetch OHLCV data
            ohlcv_data = self.binance_exchange.fetch_ohlcv(
                binance_symbol, 
                ccxt_timeframe, 
                since=int(start_date.timestamp() * 1000),
                limit=limit
            )
            
            candles = []
            for ohlcv in ohlcv_data:
                timestamp = datetime.fromtimestamp(ohlcv[0] / 1000, tz=timezone.utc)
                if start_date <= timestamp <= end_date:
                    candles.append(OHLCV(
                        timestamp=timestamp,
                        open=ohlcv[1],
                        high=ohlcv[2],
                        low=ohlcv[3],
                        close=ohlcv[4],
                        volume=ohlcv[5],
                        symbol=symbol,
                        timeframe=timeframe,
                        source='binance'
                    ))
            
            logger.info(f"Retrieved {len(candles)} Binance candles for {symbol} {timeframe}")
            return candles
            
        except Exception as e:
            logger.error(f"Failed to get Binance candles: {e}")
            return []
    
    async def _get_tardis_candles(
        self, 
        symbol: str, 
        timeframe: str, 
        start_date: datetime, 
        end_date: datetime,
        max_candles: int
    ) -> List[OHLCV]:
        """Get candles from Tardis data"""
        try:
            # This would integrate with your existing Tardis data loading
            # For now, return empty list as placeholder
            logger.info(f"Tardis candle retrieval not yet implemented for {symbol} {timeframe}")
            return []
            
        except Exception as e:
            logger.error(f"Failed to get Tardis candles: {e}")
            return []
    
    def _align_candles(self, binance_candles: List[OHLCV], tardis_candles: List[OHLCV]) -> List[Dict[str, Any]]:
        """Align candles by timestamp for comparison"""
        aligned = []
        
        # Create lookup dictionaries
        binance_dict = {c.timestamp: c for c in binance_candles}
        tardis_dict = {c.timestamp: c for c in tardis_candles}
        
        # Find common timestamps
        common_timestamps = set(binance_dict.keys()) & set(tardis_dict.keys())
        
        for timestamp in sorted(common_timestamps):
            aligned.append({
                'timestamp': timestamp,
                'binance': binance_dict[timestamp],
                'tardis': tardis_dict[timestamp]
            })
        
        return aligned
    
    def _apply_three_rules(self, aligned_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply the three rules validation system"""
        results = {
            'rule_1_passed': True,  # Timestamp alignment
            'rule_2_passed': True,  # OHLC preservation
            'rule_3_passed': True,  # Volume consistency
            'rule_1_details': {},
            'rule_2_details': {},
            'rule_3_details': {}
        }
        
        if not aligned_data:
            results['rule_1_passed'] = False
            results['rule_2_passed'] = False
            results['rule_3_passed'] = False
            return results
        
        # Rule 1: Timestamp alignment (already validated by alignment)
        results['rule_1_passed'] = True
        results['rule_1_details'] = {'aligned_candles': len(aligned_data)}
        
        # Rule 2: OHLC preservation
        ohlc_errors = []
        for data in aligned_data:
            binance = data['binance']
            tardis = data['tardis']
            
            # Check each OHLC value
            for field in ['open', 'high', 'low', 'close']:
                binance_val = getattr(binance, field)
                tardis_val = getattr(tardis, field)
                
                if abs(binance_val - tardis_val) / binance_val > self.price_tolerance:
                    ohlc_errors.append({
                        'timestamp': data['timestamp'].isoformat(),
                        'field': field,
                        'binance': binance_val,
                        'tardis': tardis_val,
                        'difference_pct': abs(binance_val - tardis_val) / binance_val * 100
                    })
        
        results['rule_2_passed'] = len(ohlc_errors) == 0
        results['rule_2_details'] = {
            'ohlc_errors': ohlc_errors,
            'error_count': len(ohlc_errors)
        }
        
        # Rule 3: Volume consistency
        volume_errors = []
        for data in aligned_data:
            binance = data['binance']
            tardis = data['tardis']
            
            if abs(binance.volume - tardis.volume) / binance.volume > self.volume_tolerance:
                volume_errors.append({
                    'timestamp': data['timestamp'].isoformat(),
                    'binance_volume': binance.volume,
                    'tardis_volume': tardis.volume,
                    'difference_pct': abs(binance.volume - tardis.volume) / binance.volume * 100
                })
        
        results['rule_3_passed'] = len(volume_errors) == 0
        results['rule_3_details'] = {
            'volume_errors': volume_errors,
            'error_count': len(volume_errors)
        }
        
        return results
    
    def _aggregate_candles(self, candles: List[OHLCV], target_timeframe: str) -> List[OHLCV]:
        """Aggregate candles to target timeframe"""
        if not candles:
            return []
        
        # Group candles by target timeframe boundaries
        grouped = {}
        
        for candle in candles:
            # Calculate target timeframe boundary
            boundary = self._get_timeframe_boundary(candle.timestamp, target_timeframe)
            
            if boundary not in grouped:
                grouped[boundary] = []
            grouped[boundary].append(candle)
        
        # Aggregate each group
        aggregated = []
        for boundary, group_candles in grouped.items():
            if not group_candles:
                continue
                
            # Sort by timestamp
            group_candles.sort(key=lambda x: x.timestamp)
            
            # Aggregate OHLCV
            agg_candle = OHLCV(
                timestamp=boundary,
                open=group_candles[0].open,
                high=max(c.high for c in group_candles),
                low=min(c.low for c in group_candles),
                close=group_candles[-1].close,
                volume=sum(c.volume for c in group_candles),
                symbol=group_candles[0].symbol,
                timeframe=target_timeframe,
                source=group_candles[0].source
            )
            
            aggregated.append(agg_candle)
        
        return sorted(aggregated, key=lambda x: x.timestamp)
    
    def _get_timeframe_boundary(self, timestamp: datetime, timeframe: str) -> datetime:
        """Get the timeframe boundary for a given timestamp"""
        if timeframe == '15s':
            # Round down to nearest 15 seconds
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
    
    def _compare_candles(self, candles1: List[OHLCV], candles2: List[OHLCV]) -> Dict[str, Any]:
        """Compare two sets of candles"""
        if not candles1 or not candles2:
            return {'passed': False, 'message': 'One or both candle sets empty'}
        
        # Align by timestamp
        dict1 = {c.timestamp: c for c in candles1}
        dict2 = {c.timestamp: c for c in candles2}
        
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
                
                if abs(val1 - val2) / val1 > self.price_tolerance:
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
    
    def _check_timestamp_alignment(self, candles1: List[OHLCV], candles2: List[OHLCV]) -> Dict[str, Any]:
        """Check timestamp alignment between two candle sets"""
        if not candles1 or not candles2:
            return {'aligned': False, 'message': 'One or both candle sets empty'}
        
        # Get timestamps
        timestamps1 = {c.timestamp for c in candles1}
        timestamps2 = {c.timestamp for c in candles2}
        
        # Check overlap
        common_timestamps = timestamps1 & timestamps2
        total_timestamps = timestamps1 | timestamps2
        
        overlap_ratio = len(common_timestamps) / len(total_timestamps) if total_timestamps else 0
        
        # Check for timestamp differences within tolerance
        timestamp_diffs = []
        for ts1 in timestamps1:
            for ts2 in timestamps2:
                diff_seconds = abs((ts1 - ts2).total_seconds())
                if diff_seconds <= self.timestamp_tolerance_seconds:
                    timestamp_diffs.append(diff_seconds)
        
        return {
            'aligned': overlap_ratio >= 0.8,  # At least 80% overlap
            'overlap_ratio': overlap_ratio,
            'common_timestamps': len(common_timestamps),
            'total_timestamps': len(total_timestamps),
            'avg_timestamp_diff': np.mean(timestamp_diffs) if timestamp_diffs else float('inf'),
            'max_timestamp_diff': max(timestamp_diffs) if timestamp_diffs else float('inf')
        }