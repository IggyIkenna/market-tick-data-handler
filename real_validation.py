#!/usr/bin/env python3
"""
Real Validation Framework

Implements the Three Rules Validation System using actual Tardis API, 
Google Cloud Storage, and Binance data as specified in the documentation.

Three Rules:
1. Timestamp Alignment Rule - Ensures timestamps align between sources
2. OHLC Preservation Rule - Validates OHLC values are preserved correctly  
3. Volume Consistency Rule - Checks volume consistency between sources

Usage:
    python3 real_validation.py                           # Run with current env vars
    python3 real_validation.py --symbol ETH-USDT        # Test specific symbol
    python3 real_validation.py --timeframes 1m,5m,1h    # Test specific timeframes
    python3 real_validation.py --hours 6                # Test last 6 hours
"""

import sys
import os
import asyncio
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import logging

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

# Set up environment variables for real services
os.environ['GCP_CREDENTIALS_PATH'] = '/workspace/central-element-323112-e35fb0ddafe2.json'

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RealThreeRulesValidator:
    """
    Real Three Rules Validation System
    
    Validates Binance CCXT data against Tardis-derived data using actual services.
    Based on the market-data-handler repository validation framework.
    """
    
    def __init__(self):
        self.data_client = None
        self.tardis_connector = None
        self.cross_source_validator = None
        
        # Validation tolerances
        self.timestamp_tolerance_seconds = 1.0
        self.price_tolerance_percent = 0.01  # 0.01%
        self.volume_tolerance_percent = 0.05  # 0.05%
    
    async def initialize(self):
        """Initialize real services"""
        try:
            from market_data_tick_handler.data_client.data_client import DataClient
            from market_data_tick_handler.data_downloader.tardis_connector import TardisConnector
            from market_data_tick_handler.validation.cross_source_validator import CrossSourceValidator
            from config import get_config
            
            # Load configuration
            config = get_config()
            
            # Initialize services
            self.data_client = DataClient(config.gcp.bucket, config)
            self.tardis_connector = TardisConnector()
            self.cross_source_validator = CrossSourceValidator(self.data_client, self.tardis_connector)
            
            logger.info("✅ Real services initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize services: {e}")
            return False
    
    async def validate_three_rules(
        self,
        symbol: str,
        timeframes: List[str],
        start_date: datetime,
        end_date: datetime,
        max_candles_per_timeframe: int = 1000
    ) -> Dict[str, Any]:
        """
        Validate the three rules across multiple timeframes using real data.
        
        Args:
            symbol: Trading symbol (e.g., 'BTC-USDT')
            timeframes: List of timeframes to validate (e.g., ['1m', '5m', '15m', '1h', '4h', '1d'])
            start_date: Start date for validation
            end_date: End date for validation
            max_candles_per_timeframe: Maximum candles to fetch per timeframe
            
        Returns:
            Dictionary with validation results
        """
        
        logger.info(f"🔍 Starting Three Rules Validation for {symbol}")
        logger.info(f"   Timeframes: {timeframes}")
        logger.info(f"   Date Range: {start_date} to {end_date}")
        
        results = {
            'symbol': symbol,
            'timeframes': timeframes,
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'timestamp_rule': {},
            'ohlc_rule': {},
            'volume_rule': {},
            'aggregation_rule': {},
            'summary': {
                'total_tests': 0,
                'passed': 0,
                'failed': 0,
                'warnings': 0
            }
        }
        
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
                    results['timestamp_rule'][timeframe] = {
                        'status': 'FAIL',
                        'message': f'No Binance data available for {timeframe}',
                        'binance_candles': 0,
                        'tardis_candles': len(tardis_candles) if tardis_candles else 0
                    }
                    results['summary']['total_tests'] += 1
                    results['summary']['failed'] += 1
                    continue
                
                # Rule 1: Timestamp Alignment
                timestamp_result = await self._validate_timestamp_alignment(
                    binance_candles, tardis_candles, timeframe
                )
                results['timestamp_rule'][timeframe] = timestamp_result
                results['summary']['total_tests'] += 1
                if timestamp_result['status'] == 'PASS':
                    results['summary']['passed'] += 1
                elif timestamp_result['status'] == 'WARNING':
                    results['summary']['warnings'] += 1
                else:
                    results['summary']['failed'] += 1
                
                # Rule 2: OHLC Preservation (only if we have both sources)
                if tardis_candles:
                    ohlc_result = await self._validate_ohlc_preservation(
                        binance_candles, tardis_candles, timeframe
                    )
                    results['ohlc_rule'][timeframe] = ohlc_result
                    results['summary']['total_tests'] += 1
                    if ohlc_result['status'] == 'PASS':
                        results['summary']['passed'] += 1
                    elif ohlc_result['status'] == 'WARNING':
                        results['summary']['warnings'] += 1
                    else:
                        results['summary']['failed'] += 1
                else:
                    logger.warning(f"⚠️ No Tardis data for {symbol} {timeframe} - skipping OHLC validation")
                    results['ohlc_rule'][timeframe] = {
                        'status': 'WARNING',
                        'message': f'No Tardis data available for {timeframe}',
                        'binance_candles': len(binance_candles),
                        'tardis_candles': 0
                    }
                    results['summary']['total_tests'] += 1
                    results['summary']['warnings'] += 1
                
                # Rule 3: Volume Consistency (only if we have both sources)
                if tardis_candles:
                    volume_result = await self._validate_volume_consistency(
                        binance_candles, tardis_candles, timeframe
                    )
                    results['volume_rule'][timeframe] = volume_result
                    results['summary']['total_tests'] += 1
                    if volume_result['status'] == 'PASS':
                        results['summary']['passed'] += 1
                    elif volume_result['status'] == 'WARNING':
                        results['summary']['warnings'] += 1
                    else:
                        results['summary']['failed'] += 1
                else:
                    logger.warning(f"⚠️ No Tardis data for {symbol} {timeframe} - skipping volume validation")
                    results['volume_rule'][timeframe] = {
                        'status': 'WARNING',
                        'message': f'No Tardis data available for {timeframe}',
                        'binance_candles': len(binance_candles),
                        'tardis_candles': 0
                    }
                    results['summary']['total_tests'] += 1
                    results['summary']['warnings'] += 1
                
                # Additional: Aggregation consistency for smaller timeframes
                if timeframe in ['5m', '15m', '1h', '4h', '1d']:
                    base_timeframe = self._get_base_timeframe(timeframe)
                    if base_timeframe:
                        agg_result = await self._validate_aggregation_consistency(
                            symbol, base_timeframe, timeframe, start_date, end_date
                        )
                        results['aggregation_rule'][f"{base_timeframe}_to_{timeframe}"] = agg_result
                        results['summary']['total_tests'] += 1
                        if agg_result['status'] == 'PASS':
                            results['summary']['passed'] += 1
                        elif agg_result['status'] == 'WARNING':
                            results['summary']['warnings'] += 1
                        else:
                            results['summary']['failed'] += 1
                
            except Exception as e:
                logger.error(f"❌ Error validating {timeframe}: {e}")
                results['timestamp_rule'][timeframe] = {
                    'status': 'FAIL',
                    'message': f'Error during validation: {str(e)}',
                    'error': str(e)
                }
                results['summary']['total_tests'] += 1
                results['summary']['failed'] += 1
        
        # Calculate success rate
        if results['summary']['total_tests'] > 0:
            results['summary']['success_rate'] = (
                (results['summary']['passed'] + results['summary']['warnings'] * 0.5) / 
                results['summary']['total_tests'] * 100
            )
        else:
            results['summary']['success_rate'] = 0.0
        
        # Log summary
        logger.info("📊 Three Rules Validation Summary:")
        logger.info(f"   Total Tests: {results['summary']['total_tests']}")
        logger.info(f"   Passed: {results['summary']['passed']}")
        logger.info(f"   Failed: {results['summary']['failed']}")
        logger.info(f"   Warnings: {results['summary']['warnings']}")
        logger.info(f"   Success Rate: {results['summary']['success_rate']:.1f}%")
        
        return results
    
    async def _validate_timestamp_alignment(
        self,
        binance_candles: List,
        tardis_candles: List,
        timeframe: str
    ) -> Dict[str, Any]:
        """Rule 1: Timestamp Alignment"""
        try:
            if not binance_candles:
                return {
                    'status': 'FAIL',
                    'message': 'No Binance candles for timestamp validation',
                    'timeframe': timeframe
                }
            
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
                status = 'PASS'
                message = f'Timestamp alignment validated for {timeframe}'
            else:
                status = 'FAIL'
                message = f'Timestamp alignment issues found for {timeframe}'
            
            return {
                'status': status,
                'message': message,
                'timeframe': timeframe,
                'binance_candles': len(binance_candles),
                'tardis_candles': len(tardis_candles) if tardis_candles else 0,
                'timestamp_issues': timestamp_issues,
                'alignment_issues': alignment_issues
            }
            
        except Exception as e:
            return {
                'status': 'FAIL',
                'message': f'Timestamp alignment validation failed: {str(e)}',
                'timeframe': timeframe,
                'error': str(e)
            }
    
    async def _validate_ohlc_preservation(
        self,
        binance_candles: List,
        tardis_candles: List,
        timeframe: str
    ) -> Dict[str, Any]:
        """Rule 2: OHLC Preservation"""
        try:
            if not binance_candles or not tardis_candles:
                return {
                    'status': 'WARNING',
                    'message': 'Insufficient data for OHLC preservation validation',
                    'timeframe': timeframe
                }
            
            # Align candles by timestamp
            aligned_candles = self._align_candles_by_timestamp(binance_candles, tardis_candles)
            
            if not aligned_candles:
                return {
                    'status': 'FAIL',
                    'message': 'No aligned candles for OHLC preservation validation',
                    'timeframe': timeframe
                }
            
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
                status = 'PASS'
                message = f'OHLC preservation validated for {timeframe}'
            else:
                status = 'FAIL'
                message = f'OHLC preservation issues found for {timeframe}'
            
            return {
                'status': status,
                'message': message,
                'timeframe': timeframe,
                'aligned_candles': len(aligned_candles),
                'ohlc_issues': ohlc_issues,
                'tolerance_percent': self.price_tolerance_percent
            }
            
        except Exception as e:
            return {
                'status': 'FAIL',
                'message': f'OHLC preservation validation failed: {str(e)}',
                'timeframe': timeframe,
                'error': str(e)
            }
    
    async def _validate_volume_consistency(
        self,
        binance_candles: List,
        tardis_candles: List,
        timeframe: str
    ) -> Dict[str, Any]:
        """Rule 3: Volume Consistency"""
        try:
            if not binance_candles or not tardis_candles:
                return {
                    'status': 'WARNING',
                    'message': 'Insufficient data for volume consistency validation',
                    'timeframe': timeframe
                }
            
            # Align candles by timestamp
            aligned_candles = self._align_candles_by_timestamp(binance_candles, tardis_candles)
            
            if not aligned_candles:
                return {
                    'status': 'FAIL',
                    'message': 'No aligned candles for volume consistency validation',
                    'timeframe': timeframe
                }
            
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
                status = 'PASS'
                message = f'Volume consistency validated for {timeframe}'
            else:
                status = 'FAIL'
                message = f'Volume consistency issues found for {timeframe}'
            
            return {
                'status': status,
                'message': message,
                'timeframe': timeframe,
                'aligned_candles': len(aligned_candles),
                'volume_issues': volume_issues,
                'tolerance_percent': self.volume_tolerance_percent
            }
            
        except Exception as e:
            return {
                'status': 'FAIL',
                'message': f'Volume consistency validation failed: {str(e)}',
                'timeframe': timeframe,
                'error': str(e)
            }
    
    async def _validate_aggregation_consistency(
        self,
        symbol: str,
        base_timeframe: str,
        aggregated_timeframe: str,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, Any]:
        """Validate aggregation consistency between timeframes"""
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
                return {
                    'status': 'FAIL',
                    'message': f'No base timeframe data for aggregation validation',
                    'base_timeframe': base_timeframe,
                    'aggregated_timeframe': aggregated_timeframe
                }
            
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
                return {
                    'status': 'FAIL',
                    'message': f'No direct aggregated timeframe data',
                    'base_timeframe': base_timeframe,
                    'aggregated_timeframe': aggregated_timeframe
                }
            
            # Compare aggregated vs direct
            alignment_issues = self._compare_aggregated_candles(aggregated_candles, direct_candles)
            
            if not alignment_issues:
                status = 'PASS'
                message = f'Aggregation consistency validated from {base_timeframe} to {aggregated_timeframe}'
            else:
                status = 'FAIL'
                message = f'Aggregation consistency issues found from {base_timeframe} to {aggregated_timeframe}'
            
            return {
                'status': status,
                'message': message,
                'base_timeframe': base_timeframe,
                'aggregated_timeframe': aggregated_timeframe,
                'base_candles': len(base_candles),
                'aggregated_candles': len(aggregated_candles),
                'direct_candles': len(direct_candles),
                'alignment_issues': alignment_issues
            }
            
        except Exception as e:
            return {
                'status': 'FAIL',
                'message': f'Aggregation consistency validation failed: {str(e)}',
                'base_timeframe': base_timeframe,
                'aggregated_timeframe': aggregated_timeframe,
                'error': str(e)
            }
    
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
    """Main function to run Real Three Rules Validation"""
    
    parser = argparse.ArgumentParser(description='Real Three Rules Validation System')
    parser.add_argument('--symbol', default='BTC-USDT', help='Trading symbol to validate (default: BTC-USDT)')
    parser.add_argument('--timeframes', default='1m,5m,15m,1h,4h,1d', help='Comma-separated timeframes (default: 1m,5m,15m,1h,4h,1d)')
    parser.add_argument('--hours', type=int, default=24, help='Hours of data to validate (default: 24)')
    parser.add_argument('--dry-run', action='store_true', help='Check configuration only')
    
    args = parser.parse_args()
    
    print("🔍 Real Three Rules Validation System")
    print("=" * 50)
    print("This uses actual Tardis API, Google Cloud Storage, and Binance data")
    print("=" * 50)
    
    # Check environment
    required_vars = ['GCP_PROJECT_ID', 'GCS_BUCKET', 'GCP_CREDENTIALS_PATH']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    # Check if Tardis API key is available (either from env or secret manager)
    if not os.getenv('TARDIS_API_KEY') and not os.getenv('USE_SECRET_MANAGER'):
        missing_vars.append('TARDIS_API_KEY (or USE_SECRET_MANAGER=true)')
    
    if missing_vars:
        print(f"❌ Missing environment variables: {missing_vars}")
        print("   Please check your .env file configuration")
        print("\nRequired environment variables in .env:")
        print("   GCP_PROJECT_ID=central-element-323112")
        print("   GCS_BUCKET=market-data-tick")
        print("   USE_SECRET_MANAGER=true")
        print("   TARDIS_SECRET_NAME=tardis-api-key")
        return 1
    
    if args.dry_run:
        print("✅ Environment variables are set correctly")
        print("   Run without --dry-run to perform actual validation")
        return 0
    
    try:
        # Initialize validator
        validator = RealThreeRulesValidator()
        
        if not await validator.initialize():
            print("❌ Failed to initialize services")
            return 1
        
        # Parse timeframes
        timeframes = [tf.strip() for tf in args.timeframes.split(',')]
        
        # Calculate date range
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=args.hours)
        
        print(f"📊 Validating {args.symbol} across timeframes: {timeframes}")
        print(f"📅 Date range: {start_time} to {end_time}")
        print()
        
        # Run validation
        results = await validator.validate_three_rules(
            symbol=args.symbol,
            timeframes=timeframes,
            start_date=start_time,
            end_date=end_time,
            max_candles_per_timeframe=1000
        )
        
        # Print detailed results
        print("\n📋 Detailed Results:")
        print("-" * 50)
        
        # Timestamp Rule Results
        print("\n1️⃣ Timestamp Alignment Rule:")
        for timeframe, result in results['timestamp_rule'].items():
            status_emoji = "✅" if result['status'] == 'PASS' else "❌" if result['status'] == 'FAIL' else "⚠️"
            print(f"   {status_emoji} {timeframe}: {result['status']} - {result['message']}")
            if 'binance_candles' in result:
                print(f"      Binance: {result['binance_candles']} candles, Tardis: {result['tardis_candles']} candles")
        
        # OHLC Rule Results
        print("\n2️⃣ OHLC Preservation Rule:")
        for timeframe, result in results['ohlc_rule'].items():
            status_emoji = "✅" if result['status'] == 'PASS' else "❌" if result['status'] == 'FAIL' else "⚠️"
            print(f"   {status_emoji} {timeframe}: {result['status']} - {result['message']}")
            if 'aligned_candles' in result:
                print(f"      Aligned candles: {result['aligned_candles']}")
        
        # Volume Rule Results
        print("\n3️⃣ Volume Consistency Rule:")
        for timeframe, result in results['volume_rule'].items():
            status_emoji = "✅" if result['status'] == 'PASS' else "❌" if result['status'] == 'FAIL' else "⚠️"
            print(f"   {status_emoji} {timeframe}: {result['status']} - {result['message']}")
            if 'aligned_candles' in result:
                print(f"      Aligned candles: {result['aligned_candles']}")
        
        # Aggregation Rule Results
        if results['aggregation_rule']:
            print("\n4️⃣ Aggregation Consistency Rule:")
            for rule, result in results['aggregation_rule'].items():
                status_emoji = "✅" if result['status'] == 'PASS' else "❌" if result['status'] == 'FAIL' else "⚠️"
                print(f"   {status_emoji} {rule}: {result['status']} - {result['message']}")
        
        # Summary
        print(f"\n📊 Summary:")
        print(f"   Total Tests: {results['summary']['total_tests']}")
        print(f"   Passed: {results['summary']['passed']}")
        print(f"   Failed: {results['summary']['failed']}")
        print(f"   Warnings: {results['summary']['warnings']}")
        print(f"   Success Rate: {results['summary']['success_rate']:.1f}%")
        
        if results['summary']['success_rate'] >= 80:
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
