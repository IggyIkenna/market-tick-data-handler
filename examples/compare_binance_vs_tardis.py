#!/usr/bin/env python3
"""
Compare Binance vs Tardis Data

Practical script to compare Binance CCXT data with Tardis-derived candles.
Implements comprehensive validation using the three rules system.

Usage:
    python examples/compare_binance_vs_tardis.py --symbol BTC-USDT --timeframe 1m --start-date 2024-01-15 --end-date 2024-01-16
    python examples/compare_binance_vs_tardis.py --symbol ETH-USDT --timeframe 5m --start-date 2024-01-15 --end-date 2024-01-16 --validate-aggregation
    python examples/compare_binance_vs_tardis.py --symbol BTC-USDT --timeframe 1m --live --duration 60
"""

import sys
import os
import argparse
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
import pandas as pd
import json

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config import get_config
from market_data_tick_handler.validation.cross_source_validator import CrossSourceValidator
from market_data_tick_handler.validation.timestamp_validator import TimestampValidator
from market_data_tick_handler.validation.aggregation_validator import AggregationValidator
from market_data_tick_handler.validation.validation_results import ValidationReport
from market_data_tick_handler.data_client.data_client import DataClient
from market_data_tick_handler.data_downloader.tardis_connector import TardisConnector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Compare Binance vs Tardis Data - Comprehensive Validation',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Compare 1m candles for BTC-USDT
  python examples/compare_binance_vs_tardis.py --symbol BTC-USDT --timeframe 1m --start-date 2024-01-15 --end-date 2024-01-16
  
  # Compare with aggregation validation
  python examples/compare_binance_vs_tardis.py --symbol ETH-USDT --timeframe 5m --start-date 2024-01-15 --end-date 2024-01-16 --validate-aggregation
  
  # Live comparison for 60 minutes
  python examples/compare_binance_vs_tardis.py --symbol BTC-USDT --timeframe 1m --live --duration 60
  
  # Compare multiple timeframes
  python examples/compare_binance_vs_tardis.py --symbol BTC-USDT --timeframes 1m 5m 15m --start-date 2024-01-15 --end-date 2024-01-16
  
  # Save results to file
  python examples/compare_binance_vs_tardis.py --symbol BTC-USDT --timeframe 1m --start-date 2024-01-15 --end-date 2024-01-16 --output results.json
        """
    )
    
    # Symbol and timeframe
    parser.add_argument(
        '--symbol',
        type=str,
        required=True,
        help='Trading symbol (e.g., BTC-USDT, ETH-USDT)'
    )
    parser.add_argument(
        '--timeframe',
        type=str,
        help='Single timeframe to compare (e.g., 1m, 5m, 15m, 1h)'
    )
    parser.add_argument(
        '--timeframes',
        nargs='+',
        help='Multiple timeframes to compare'
    )
    
    # Date range
    parser.add_argument(
        '--start-date',
        type=str,
        help='Start date in YYYY-MM-DD format'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        help='End date in YYYY-MM-DD format'
    )
    
    # Live mode
    parser.add_argument(
        '--live',
        action='store_true',
        help='Compare live data instead of historical'
    )
    parser.add_argument(
        '--duration',
        type=int,
        default=60,
        help='Duration in minutes for live comparison (default: 60)'
    )
    
    # Validation options
    parser.add_argument(
        '--validate-aggregation',
        action='store_true',
        help='Validate aggregation consistency between timeframes'
    )
    parser.add_argument(
        '--validate-timestamps',
        action='store_true',
        help='Validate timestamp stability and alignment'
    )
    parser.add_argument(
        '--max-candles',
        type=int,
        default=1000,
        help='Maximum number of candles to fetch (default: 1000)'
    )
    
    # Output options
    parser.add_argument(
        '--output',
        type=str,
        help='Output file path for results (JSON format)'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Show detailed validation results'
    )
    
    # Configuration
    parser.add_argument(
        '--env-file',
        type=str,
        help='Path to environment file (.env)'
    )
    
    return parser.parse_args()


def parse_date(date_str: str) -> datetime:
    """Parse date string to timezone-aware datetime"""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    except ValueError as e:
        raise ValueError(f"Invalid date format '{date_str}'. Use YYYY-MM-DD format.") from e


def load_environment_file(env_file: str):
    """Load environment variables from file"""
    if env_file and os.path.exists(env_file):
        from dotenv import load_dotenv
        load_dotenv(env_file)
        logger.info(f"Loaded environment variables from {env_file}")
    else:
        logger.info("Using system environment variables")


async def run_historical_comparison(
    validator: CrossSourceValidator,
    symbol: str,
    timeframes: list,
    start_date: datetime,
    end_date: datetime,
    max_candles: int,
    validate_aggregation: bool,
    validate_timestamps: bool
) -> ValidationReport:
    """Run historical data comparison"""
    
    report_id = f"historical_comparison_{symbol}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
    report = ValidationReport(
        report_id=report_id,
        start_time=datetime.utcnow(),
        end_time=datetime.utcnow()
    )
    
    logger.info(f"🔍 Starting historical comparison for {symbol}")
    logger.info(f"📅 Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    logger.info(f"⏱️  Timeframes: {', '.join(timeframes)}")
    
    # Validate each timeframe
    for timeframe in timeframes:
        logger.info(f"📊 Validating {timeframe} candles...")
        
        # Timeframe consistency validation
        result = await validator.validate_timeframe_consistency(
            symbol=symbol,
            timeframe=timeframe,
            start_date=start_date,
            end_date=end_date,
            max_candles=max_candles
        )
        report.add_result(result)
        
        # Timestamp alignment validation
        if validate_timestamps:
            timestamp_result = await validator.validate_timestamp_alignment(
                symbol=symbol,
                timeframe=timeframe,
                start_date=start_date,
                end_date=end_date,
                max_candles=max_candles
            )
            report.add_result(timestamp_result)
    
    # Aggregation consistency validation
    if validate_aggregation and len(timeframes) > 1:
        logger.info("🔄 Validating aggregation consistency...")
        
        # Find base and aggregated timeframes
        base_timeframe = min(timeframes, key=lambda x: get_timeframe_seconds(x))
        aggregated_timeframes = [tf for tf in timeframes if tf != base_timeframe]
        
        for agg_timeframe in aggregated_timeframes:
            agg_result = await validator.validate_aggregation_consistency(
                symbol=symbol,
                base_timeframe=base_timeframe,
                aggregated_timeframe=agg_timeframe,
                start_date=start_date,
                end_date=end_date,
                max_candles=max_candles
            )
            report.add_result(agg_result)
    
    report.end_time = datetime.utcnow()
    return report


async def run_live_comparison(
    validator: CrossSourceValidator,
    symbol: str,
    timeframe: str,
    duration_minutes: int,
    validate_timestamps: bool
) -> ValidationReport:
    """Run live data comparison"""
    
    report_id = f"live_comparison_{symbol}_{timeframe}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    report = ValidationReport(
        report_id=report_id,
        start_time=datetime.utcnow(),
        end_time=datetime.utcnow()
    )
    
    logger.info(f"🔴 Starting live comparison for {symbol} {timeframe}")
    logger.info(f"⏱️  Duration: {duration_minutes} minutes")
    
    # Calculate date range for live data
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(minutes=duration_minutes)
    
    # Timeframe consistency validation
    result = await validator.validate_timeframe_consistency(
        symbol=symbol,
        timeframe=timeframe,
        start_date=start_time,
        end_date=end_time,
        max_candles=duration_minutes + 10  # Add buffer
    )
    report.add_result(result)
    
    # Timestamp alignment validation
    if validate_timestamps:
        timestamp_result = await validator.validate_timestamp_alignment(
            symbol=symbol,
            timeframe=timeframe,
            start_date=start_time,
            end_date=end_time,
            max_candles=duration_minutes + 10
        )
        report.add_result(timestamp_result)
    
    report.end_time = datetime.utcnow()
    return report


def get_timeframe_seconds(timeframe: str) -> int:
    """Get timeframe in seconds for sorting"""
    timeframe_map = {
        '15s': 15,
        '1m': 60,
        '5m': 300,
        '15m': 900,
        '1h': 3600,
        '4h': 14400,
        '1d': 86400
    }
    return timeframe_map.get(timeframe, 60)


def print_detailed_results(report: ValidationReport):
    """Print detailed validation results"""
    print("\n" + "="*80)
    print("📊 DETAILED VALIDATION RESULTS")
    print("="*80)
    
    for result in report.results:
        status_emoji = {
            'PASS': '✅',
            'FAIL': '❌',
            'WARNING': '⚠️',
            'SKIP': '⏭️'
        }.get(result.status.value, '❓')
        
        print(f"\n{status_emoji} {result.test_name}")
        print(f"   Status: {result.status.value}")
        print(f"   Message: {result.message}")
        print(f"   Execution Time: {result.execution_time_ms:.2f}ms")
        
        if result.details:
            print("   Details:")
            for key, value in result.details.items():
                if isinstance(value, (dict, list)) and len(str(value)) > 100:
                    print(f"     {key}: {type(value).__name__} ({len(value) if hasattr(value, '__len__') else 'N/A'} items)")
                else:
                    print(f"     {key}: {value}")


def save_results(report: ValidationReport, output_path: str):
    """Save validation results to file"""
    try:
        results_dict = report.to_dict()
        
        with open(output_path, 'w') as f:
            json.dump(results_dict, f, indent=2, default=str)
        
        logger.info(f"💾 Results saved to: {output_path}")
        
    except Exception as e:
        logger.error(f"❌ Failed to save results: {e}")


async def main():
    """Main entry point"""
    try:
        # Parse arguments
        args = parse_arguments()
        
        # Load environment file if specified
        if args.env_file:
            load_environment_file(args.env_file)
        
        # Load configuration
        config = get_config()
        
        # Determine timeframes
        if args.timeframes:
            timeframes = args.timeframes
        elif args.timeframe:
            timeframes = [args.timeframe]
        else:
            raise ValueError("Must specify either --timeframe or --timeframes")
        
        # Validate arguments
        if args.live:
            if not args.timeframe:
                raise ValueError("Must specify --timeframe for live comparison")
            if args.start_date or args.end_date:
                raise ValueError("Cannot specify --start-date or --end-date with --live")
        else:
            if not args.start_date or not args.end_date:
                raise ValueError("Must specify --start-date and --end-date for historical comparison")
        
        # Create data client and connectors
        data_client = DataClient(config.gcp.bucket)
        tardis_connector = TardisConnector()  # You'll need to implement this
        
        # Create validators
        cross_source_validator = CrossSourceValidator(data_client, tardis_connector)
        
        # Run comparison
        if args.live:
            report = await run_live_comparison(
                validator=cross_source_validator,
                symbol=args.symbol,
                timeframe=args.timeframe,
                duration_minutes=args.duration,
                validate_timestamps=args.validate_timestamps
            )
        else:
            start_date = parse_date(args.start_date)
            end_date = parse_date(args.end_date)
            
            if start_date >= end_date:
                raise ValueError("Start date must be before end date")
            
            report = await run_historical_comparison(
                validator=cross_source_validator,
                symbol=args.symbol,
                timeframes=timeframes,
                start_date=start_date,
                end_date=end_date,
                max_candles=args.max_candles,
                validate_aggregation=args.validate_aggregation,
                validate_timestamps=args.validate_timestamps
            )
        
        # Print results
        report.print_summary()
        
        if args.verbose:
            print_detailed_results(report)
        
        # Save results if requested
        if args.output:
            save_results(report, args.output)
        
        # Exit with appropriate code
        if report.get_status().value == 'PASS':
            logger.info("✅ All validations passed!")
            sys.exit(0)
        else:
            logger.warning(f"⚠️ {report.failed_tests} validations failed")
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"❌ Comparison failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    asyncio.run(main())
