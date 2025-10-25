#!/usr/bin/env python3
"""
Three Rules Validation System

Implements the comprehensive validation system from market-data-handler:
1. Timestamp Alignment Rule
2. OHLC Preservation Rule  
3. Volume Consistency Rule

This validates Binance CCXT data against Tardis-derived data across all timeframes.
"""

import sys
import os
import asyncio
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import logging

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

# Set up environment variables
os.environ['TARDIS_API_KEY'] = 'TD.your_tardis_api_key'
os.environ['GCP_PROJECT_ID'] = 'central-element-323112'
os.environ['GCS_BUCKET'] = 'your-gcs-bucket'
os.environ['GCP_CREDENTIALS_PATH'] = '/workspace/central-element-323112-e35fb0ddafe2.json'
os.environ['USE_SECRET_MANAGER'] = 'false'

import pandas as pd
import numpy as np
from src.validation.cross_source_validator import CrossSourceValidator
from src.validation.validation_results import ValidationStatus, ValidationResult, ValidationReport
from src.data_downloader.data_client import DataClient
from src.data_downloader.tardis_connector import TardisConnector
from config import get_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ThreeRulesValidator:
    """
    Implements the Three Rules Validation System for cross-source data validation.
    
    Based on the market-data-handler repository validation framework.
    """
    
    def __init__(self, data_client: DataClient, tardis_connector: TardisConnector):
        self.data_client = data_client
        self.tardis_connector = tardis_connector
        self.cross_source_validator = CrossSourceValidator(data_client, tardis_connector)
        
        # Validation tolerances
        self.timestamp_tolerance_seconds = 1.0
        self.price_tolerance_percent = 0.01  # 0.01%
        self.volume_tolerance_percent = 0.05  # 0.05%
    
    async def validate_three_rules(
        self,
        symbol: str,
        timeframes: List[str],
        start_date: datetime,
        end_date: datetime,
        max_candles_per_timeframe: int = 1000
    ) -> ValidationReport:
        """
        Validate the three rules across multiple timeframes.
        
        Args:
            symbol: Trading symbol (e.g., 'BTC-USDT')
            timeframes: List of timeframes to validate (e.g., ['1m', '5m', '15m', '1h', '4h', '1d'])
            start_date: Start date for validation
            end_date: End date for validation
            max_candles_per_timeframe: Maximum candles to fetch per timeframe
            
        Returns:
            ValidationReport with results for all timeframes and rules
        """
        
        logger.info(f"🔍 Starting Three Rules Validation for {symbol}")
        logger.info(f"   Timeframes: {timeframes}")
        logger.info(f"   Date Range: {start_date} to {end_date}")
        
        report = ValidationReport(
            report_id=f"three_rules_validation_{symbol}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            start_time=datetime.now(timezone.utc),
            end_time=datetime.now(timezone.utc)
        )
        
        # Validate each timeframe
        for timeframe in timeframes:
            logger.info(f"📊 Validating timeframe: {timeframe}")
            
            try:
                # Get data from both sources
                binance_candles = await self.cross_source_validator._get_binance_candles(
                    symbol=symbol,
                    timeframe=timeframe,
                    start_date=start_date,
                    end_date=end_date,
                    max_candles=max_candles_per_timeframe
                )
                
                tardis_candles = await self.cross_source_validator._get_tardis_candles(
                    symbol=symbol,
                    timeframe=timeframe,
                    start_date=start_date,
                    end_date=end_date,
                    max_candles=max_candles_per_timeframe
                )
                
                if not binance_candles:
                    logger.warning(f"⚠️ No Binance data for {symbol} {timeframe}")
                    report.add_result(ValidationResult(
                        test_name=f"binance_data_availability_{timeframe}",
                        status=ValidationStatus.FAIL,
                        message=f"No Binance data available for {timeframe}",
                        details={"timeframe": timeframe, "candles_count": 0}
                    ))
                    continue
                
                if not tardis_candles:
                    logger.warning(f"⚠️ No Tardis data for {symbol} {timeframe}")
                    report.add_result(ValidationResult(
                        test_name=f"tardis_data_availability_{timeframe}",
                        status=ValidationStatus.WARNING,
                        message=f"No Tardis data available for {timeframe}",
                        details={"timeframe": timeframe, "candles_count": 0}
                    ))
                    # Continue with Binance-only validation
                    tardis_candles = []
                
                # Rule 1: Timestamp Alignment
                timestamp_result = await self._validate_timestamp_alignment(
                    binance_candles, tardis_candles, timeframe
                )
                report.add_result(timestamp_result)
                
                # Rule 2: OHLC Preservation (only if we have both sources)
                if tardis_candles:
                    ohlc_result = await self._validate_ohlc_preservation(
                        binance_candles, tardis_candles, timeframe
                    )
                    report.add_result(ohlc_result)
                
                # Rule 3: Volume Consistency (only if we have both sources)
                if tardis_candles:
                    volume_result = await self._validate_volume_consistency(
                        binance_candles, tardis_candles, timeframe
                    )
                    report.add_result(volume_result)
                
                # Additional: Aggregation consistency for smaller timeframes
                if timeframe in ['5m', '15m', '1h', '4h', '1d']:
                    base_timeframe = self._get_base_timeframe(timeframe)
                    if base_timeframe:
                        agg_result = await self._validate_aggregation_consistency(
                            symbol, base_timeframe, timeframe, start_date, end_date
                        )
                        report.add_result(agg_result)
                
            except Exception as e:
                logger.error(f"❌ Error validating {timeframe}: {e}")
                report.add_result(ValidationResult(
                    test_name=f"validation_error_{timeframe}",
                    status=ValidationStatus.FAIL,
                    message=f"Error during validation: {str(e)}",
                    details={"timeframe": timeframe, "error": str(e)}
                ))
        
        report.end_time = datetime.now(timezone.utc)
        
        # Log summary
        logger.info("📊 Three Rules Validation Summary:")
        logger.info(f"   Total Tests: {report.total_tests}")
        logger.info(f"   Passed: {report.passed_tests}")
        logger.info(f"   Failed: {report.failed_tests}")
        logger.info(f"   Warnings: {report.warning_tests}")
        logger.info(f"   Success Rate: {report.get_success_rate():.1f}%")
        
        return report
    
    async def _validate_timestamp_alignment(
        self,
        binance_candles: List,
        tardis_candles: List,
        timeframe: str
    ) -> ValidationResult:
        """
        Rule 1: Timestamp Alignment
        
        Validates that timestamps from both sources align within tolerance.
        """
        try:
            if not binance_candles:
                return ValidationResult(
                    test_name=f"timestamp_alignment_{timeframe}",
                    status=ValidationStatus.FAIL,
                    message="No Binance candles for timestamp validation",
                    details={"timeframe": timeframe}
                )
            
            # Extract timestamps
            binance_timestamps = [c.timestamp for c in binance_candles]
            
            # Check timestamp stability within Binance data
            timestamp_issues = []
            expected_interval = self._get_interval_seconds(timeframe)
            
            for i in range(1, len(binance_timestamps)):
                actual_interval = (binance_timestamps[i] - binance_timestamps[i-1]).total_seconds()
                if abs(actual_interval - expected_interval) > self.timestamp_tolerance_seconds:
                    timestamp_issues.append({
                        'index': i,
                        'expected': expected_interval,
                        'actual': actual_interval,
                        'difference': actual_interval - expected_interval
                    })
            
            # If we have Tardis data, check alignment
            alignment_issues = []
            if tardis_candles:
                tardis_timestamps = [c.timestamp for c in tardis_candles]
                
                # Find common timestamps
                binance_set = set(binance_timestamps)
                tardis_set = set(tardis_timestamps)
                common_timestamps = binance_set & tardis_set
                
                if len(common_timestamps) < min(len(binance_timestamps), len(tardis_timestamps)) * 0.8:
                    alignment_issues.append({
                        'binance_count': len(binance_timestamps),
                        'tardis_count': len(tardis_timestamps),
                        'common_count': len(common_timestamps),
                        'overlap_percent': len(common_timestamps) / min(len(binance_timestamps), len(tardis_timestamps)) * 100
                    })
            
            # Determine result
            if not timestamp_issues and not alignment_issues:
                status = ValidationStatus.PASS
                message = f"Timestamp alignment validated for {timeframe}"
            else:
                status = ValidationStatus.FAIL
                message = f"Timestamp alignment issues found for {timeframe}"
            
            return ValidationResult(
                test_name=f"timestamp_alignment_{timeframe}",
                status=status,
                message=message,
                details={
                    'timeframe': timeframe,
                    'binance_candles': len(binance_candles),
                    'tardis_candles': len(tardis_candles) if tardis_candles else 0,
                    'timestamp_issues': timestamp_issues,
                    'alignment_issues': alignment_issues
                }
            )
            
        except Exception as e:
            return ValidationResult(
                test_name=f"timestamp_alignment_{timeframe}",
                status=ValidationStatus.FAIL,
                message=f"Timestamp alignment validation failed: {str(e)}",
                details={'timeframe': timeframe, 'error': str(e)}
            )
    
    async def _validate_ohlc_preservation(
        self,
        binance_candles: List,
        tardis_candles: List,
        timeframe: str
    ) -> ValidationResult:
        """
        Rule 2: OHLC Preservation
        
        Validates that OHLC values are preserved correctly between sources.
        """
        try:
            if not binance_candles or not tardis_candles:
                return ValidationResult(
                    test_name=f"ohlc_preservation_{timeframe}",
                    status=ValidationStatus.WARNING,
                    message="Insufficient data for OHLC preservation validation",
                    details={"timeframe": timeframe}
                )
            
            # Align candles by timestamp
            aligned_candles = self._align_candles_by_timestamp(binance_candles, tardis_candles)
            
            if not aligned_candles:
                return ValidationResult(
                    test_name=f"ohlc_preservation_{timeframe}",
                    status=ValidationStatus.FAIL,
                    message="No aligned candles for OHLC preservation validation",
                    details={"timeframe": timeframe}
                )
            
            # Check OHLC preservation
            ohlc_issues = []
            
            for aligned in aligned_candles:
                binance_candle = aligned['binance']
                tardis_candle = aligned['tardis']
                
                # Check each OHLC value
                for field in ['open', 'high', 'low', 'close']:
                    binance_value = getattr(binance_candle, field)
                    tardis_value = getattr(tardis_candle, field)
                    
                    if binance_value != 0:  # Avoid division by zero
                        diff_percent = abs(binance_value - tardis_value) / binance_value * 100
                        if diff_percent > self.price_tolerance_percent:
                            ohlc_issues.append({
                                'timestamp': binance_candle.timestamp.isoformat(),
                                'field': field,
                                'binance_value': binance_value,
                                'tardis_value': tardis_value,
                                'difference_percent': diff_percent
                            })
            
            # Determine result
            if not ohlc_issues:
                status = ValidationStatus.PASS
                message = f"OHLC preservation validated for {timeframe}"
            else:
                status = ValidationStatus.FAIL
                message = f"OHLC preservation issues found for {timeframe}"
            
            return ValidationResult(
                test_name=f"ohlc_preservation_{timeframe}",
                status=status,
                message=message,
                details={
                    'timeframe': timeframe,
                    'aligned_candles': len(aligned_candles),
                    'ohlc_issues': ohlc_issues,
                    'tolerance_percent': self.price_tolerance_percent
                }
            )
            
        except Exception as e:
            return ValidationResult(
                test_name=f"ohlc_preservation_{timeframe}",
                status=ValidationStatus.FAIL,
                message=f"OHLC preservation validation failed: {str(e)}",
                details={'timeframe': timeframe, 'error': str(e)}
            )
    
    async def _validate_volume_consistency(
        self,
        binance_candles: List,
        tardis_candles: List,
        timeframe: str
    ) -> ValidationResult:
        """
        Rule 3: Volume Consistency
        
        Validates that volume values are consistent between sources.
        """
        try:
            if not binance_candles or not tardis_candles:
                return ValidationResult(
                    test_name=f"volume_consistency_{timeframe}",
                    status=ValidationStatus.WARNING,
                    message="Insufficient data for volume consistency validation",
                    details={"timeframe": timeframe}
                )
            
            # Align candles by timestamp
            aligned_candles = self._align_candles_by_timestamp(binance_candles, tardis_candles)
            
            if not aligned_candles:
                return ValidationResult(
                    test_name=f"volume_consistency_{timeframe}",
                    status=ValidationStatus.FAIL,
                    message="No aligned candles for volume consistency validation",
                    details={"timeframe": timeframe}
                )
            
            # Check volume consistency
            volume_issues = []
            
            for aligned in aligned_candles:
                binance_candle = aligned['binance']
                tardis_candle = aligned['tardis']
                
                binance_volume = binance_candle.volume
                tardis_volume = tardis_candle.volume
                
                if binance_volume != 0:  # Avoid division by zero
                    diff_percent = abs(binance_volume - tardis_volume) / binance_volume * 100
                    if diff_percent > self.volume_tolerance_percent:
                        volume_issues.append({
                            'timestamp': binance_candle.timestamp.isoformat(),
                            'binance_volume': binance_volume,
                            'tardis_volume': tardis_volume,
                            'difference_percent': diff_percent
                        })
            
            # Determine result
            if not volume_issues:
                status = ValidationStatus.PASS
                message = f"Volume consistency validated for {timeframe}"
            else:
                status = ValidationStatus.FAIL
                message = f"Volume consistency issues found for {timeframe}"
            
            return ValidationResult(
                test_name=f"volume_consistency_{timeframe}",
                status=status,
                message=message,
                details={
                    'timeframe': timeframe,
                    'aligned_candles': len(aligned_candles),
                    'volume_issues': volume_issues,
                    'tolerance_percent': self.volume_tolerance_percent
                }
            )
            
        except Exception as e:
            return ValidationResult(
                test_name=f"volume_consistency_{timeframe}",
                status=ValidationStatus.FAIL,
                message=f"Volume consistency validation failed: {str(e)}",
                details={'timeframe': timeframe, 'error': str(e)}
            )
    
    async def _validate_aggregation_consistency(
        self,
        symbol: str,
        base_timeframe: str,
        aggregated_timeframe: str,
        start_date: datetime,
        end_date: datetime
    ) -> ValidationResult:
        """
        Validate aggregation consistency between timeframes.
        """
        try:
            # Get base timeframe data
            base_candles = await self.cross_source_validator._get_binance_candles(
                symbol=symbol,
                timeframe=base_timeframe,
                start_date=start_date,
                end_date=end_date,
                max_candles=1000
            )
            
            if not base_candles:
                return ValidationResult(
                    test_name=f"aggregation_consistency_{base_timeframe}_to_{aggregated_timeframe}",
                    status=ValidationStatus.FAIL,
                    message=f"No base timeframe data for aggregation validation",
                    details={"base_timeframe": base_timeframe, "aggregated_timeframe": aggregated_timeframe}
                )
            
            # Aggregate base candles to target timeframe
            aggregated_candles = self._aggregate_candles(base_candles, aggregated_timeframe)
            
            # Get direct aggregated timeframe data
            direct_candles = await self.cross_source_validator._get_binance_candles(
                symbol=symbol,
                timeframe=aggregated_timeframe,
                start_date=start_date,
                end_date=end_date,
                max_candles=1000
            )
            
            if not direct_candles:
                return ValidationResult(
                    test_name=f"aggregation_consistency_{base_timeframe}_to_{aggregated_timeframe}",
                    status=ValidationStatus.FAIL,
                    message=f"No direct aggregated timeframe data",
                    details={"base_timeframe": base_timeframe, "aggregated_timeframe": aggregated_timeframe}
                )
            
            # Compare aggregated vs direct
            alignment_issues = self._compare_aggregated_candles(aggregated_candles, direct_candles)
            
            if not alignment_issues:
                status = ValidationStatus.PASS
                message = f"Aggregation consistency validated from {base_timeframe} to {aggregated_timeframe}"
            else:
                status = ValidationStatus.FAIL
                message = f"Aggregation consistency issues found from {base_timeframe} to {aggregated_timeframe}"
            
            return ValidationResult(
                test_name=f"aggregation_consistency_{base_timeframe}_to_{aggregated_timeframe}",
                status=status,
                message=message,
                details={
                    'base_timeframe': base_timeframe,
                    'aggregated_timeframe': aggregated_timeframe,
                    'base_candles': len(base_candles),
                    'aggregated_candles': len(aggregated_candles),
                    'direct_candles': len(direct_candles),
                    'alignment_issues': alignment_issues
                }
            )
            
        except Exception as e:
            return ValidationResult(
                test_name=f"aggregation_consistency_{base_timeframe}_to_{aggregated_timeframe}",
                status=ValidationStatus.FAIL,
                message=f"Aggregation consistency validation failed: {str(e)}",
                details={'base_timeframe': base_timeframe, 'aggregated_timeframe': aggregated_timeframe, 'error': str(e)}
            )
    
    def _align_candles_by_timestamp(self, binance_candles: List, tardis_candles: List) -> List[Dict[str, Any]]:
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
    
    def _aggregate_candles(self, candles: List, target_timeframe: str) -> List:
        """Aggregate candles to target timeframe"""
        if not candles:
            return []
        
        # Group candles by target timeframe intervals
        target_interval = self._get_interval_seconds(target_timeframe)
        grouped = {}
        
        for candle in candles:
            # Calculate the start of the target interval
            interval_start = self._get_interval_start(candle.timestamp, target_interval)
            
            if interval_start not in grouped:
                grouped[interval_start] = []
            grouped[interval_start].append(candle)
        
        # Aggregate each group
        aggregated = []
        for interval_start, group_candles in grouped.items():
            if not group_candles:
                continue
            
            # Sort by timestamp
            group_candles.sort(key=lambda x: x.timestamp)
            
            # Create aggregated candle
            agg_candle = type(group_candles[0])(
                timestamp=interval_start,
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
    
    def _compare_aggregated_candles(self, aggregated: List, direct: List) -> List[Dict[str, Any]]:
        """Compare aggregated candles with direct candles"""
        issues = []
        
        # Create lookup for direct candles
        direct_dict = {c.timestamp: c for c in direct}
        
        for agg_candle in aggregated:
            if agg_candle.timestamp in direct_dict:
                direct_candle = direct_dict[agg_candle.timestamp]
                
                # Check OHLC values
                for field in ['open', 'high', 'low', 'close', 'volume']:
                    agg_value = getattr(agg_candle, field)
                    direct_value = getattr(direct_candle, field)
                    
                    if agg_value != 0:
                        diff_percent = abs(agg_value - direct_value) / agg_value * 100
                        if diff_percent > self.price_tolerance_percent:
                            issues.append({
                                'timestamp': agg_candle.timestamp.isoformat(),
                                'field': field,
                                'aggregated_value': agg_value,
                                'direct_value': direct_value,
                                'difference_percent': diff_percent
                            })
        
        return issues
    
    def _get_interval_seconds(self, timeframe: str) -> int:
        """Get interval in seconds for timeframe"""
        intervals = {
            '1m': 60,
            '5m': 300,
            '15m': 900,
            '1h': 3600,
            '4h': 14400,
            '1d': 86400
        }
        return intervals.get(timeframe, 60)
    
    def _get_interval_start(self, timestamp: datetime, interval_seconds: int) -> datetime:
        """Get the start of the interval containing the timestamp"""
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        seconds_since_epoch = int((timestamp - epoch).total_seconds())
        interval_start_seconds = (seconds_since_epoch // interval_seconds) * interval_seconds
        return epoch + timedelta(seconds=interval_start_seconds)
    
    def _get_base_timeframe(self, timeframe: str) -> Optional[str]:
        """Get the base timeframe for aggregation validation"""
        base_timeframes = {
            '5m': '1m',
            '15m': '1m',
            '1h': '1m',
            '4h': '1m',
            '1d': '1h'
        }
        return base_timeframes.get(timeframe)


async def main():
    """Main function to run Three Rules Validation"""
    
    print("🔍 Three Rules Validation System")
    print("=" * 50)
    print("This implements the comprehensive validation from market-data-handler:")
    print("1. Timestamp Alignment Rule")
    print("2. OHLC Preservation Rule")
    print("3. Volume Consistency Rule")
    print("=" * 50)
    
    try:
        # Load configuration
        config = get_config()
        
        # Initialize services
        data_client = DataClient(config.gcp.bucket, config)
        tardis_connector = TardisConnector()
        
        # Initialize validator
        validator = ThreeRulesValidator(data_client, tardis_connector)
        
        # Test parameters
        symbol = "BTC-USDT"
        timeframes = ["1m", "5m", "15m", "1h", "4h", "1d"]
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=1)  # Last 24 hours
        
        print(f"📊 Validating {symbol} across timeframes: {timeframes}")
        print(f"📅 Date range: {start_time} to {end_time}")
        
        # Run validation
        report = await validator.validate_three_rules(
            symbol=symbol,
            timeframes=timeframes,
            start_date=start_time,
            end_date=end_time,
            max_candles_per_timeframe=1000
        )
        
        # Print detailed results
        print("\n📋 Detailed Results:")
        print("-" * 50)
        
        for result in report.results:
            status_emoji = "✅" if result.status == ValidationStatus.PASS else "❌" if result.status == ValidationStatus.FAIL else "⚠️"
            print(f"{status_emoji} {result.test_name}: {result.status.value}")
            print(f"   {result.message}")
            if result.details and len(str(result.details)) < 200:
                print(f"   Details: {result.details}")
        
        print(f"\n📊 Summary:")
        print(f"   Total Tests: {report.total_tests}")
        print(f"   Passed: {report.passed_tests}")
        print(f"   Failed: {report.failed_tests}")
        print(f"   Warnings: {report.warning_tests}")
        print(f"   Success Rate: {report.get_success_rate():.1f}%")
        
        if report.get_success_rate() >= 80:
            print("\n🎉 Three Rules Validation completed successfully!")
            return 0
        else:
            print("\n⚠️ Three Rules Validation completed with issues.")
            return 1
            
    except Exception as e:
        print(f"\n❌ Three Rules Validation failed: {e}")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)