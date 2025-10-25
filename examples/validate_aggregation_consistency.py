#!/usr/bin/env python3
"""
Validate Aggregation Consistency

Comprehensive aggregation validation script for market data.
Validates OHLC preservation, volume aggregation, and timeframe boundaries.

Usage:
    python examples/validate_aggregation_consistency.py --base-file 1m_candles.parquet --agg-file 5m_candles.parquet --base-timeframe 1m --agg-timeframe 5m
    python examples/validate_aggregation_consistency.py --base-file 1m_candles.parquet --agg-file 5m_candles.parquet --base-timeframe 1m --agg-timeframe 5m --validate-ohlc --validate-volume
    python examples/validate_aggregation_consistency.py --data-file all_candles.parquet --timeframes 1m 5m 15m 1h --validate-all
"""

import sys
import os
import argparse
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
import pandas as pd
import json

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.validation.aggregation_validator import AggregationValidator, AggregationValidationConfig
from src.validation.cross_source_validator import OHLCV
from src.validation.validation_results import ValidationReport

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
        description='Validate Aggregation Consistency - Comprehensive Aggregation Validation',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Validate aggregation between 1m and 5m candles
  python examples/validate_aggregation_consistency.py --base-file 1m_candles.parquet --agg-file 5m_candles.parquet --base-timeframe 1m --agg-timeframe 5m
  
  # Validate specific aspects
  python examples/validate_aggregation_consistency.py --base-file 1m_candles.parquet --agg-file 5m_candles.parquet --base-timeframe 1m --agg-timeframe 5m --validate-ohlc --validate-volume
  
  # Validate all timeframes from a single file
  python examples/validate_aggregation_consistency.py --data-file all_candles.parquet --timeframes 1m 5m 15m 1h --validate-all
  
  # Validate with custom tolerances
  python examples/validate_aggregation_consistency.py --base-file 1m_candles.parquet --agg-file 5m_candles.parquet --base-timeframe 1m --agg-timeframe 5m --price-tolerance 0.001 --volume-tolerance 0.05
        """
    )
    
    # Data files
    parser.add_argument(
        '--base-file',
        type=str,
        help='Parquet file containing base timeframe candles'
    )
    parser.add_argument(
        '--agg-file',
        type=str,
        help='Parquet file containing aggregated timeframe candles'
    )
    parser.add_argument(
        '--data-file',
        type=str,
        help='Parquet file containing all timeframe candles'
    )
    
    # Timeframes
    parser.add_argument(
        '--base-timeframe',
        type=str,
        help='Base timeframe (e.g., 1m, 5m)'
    )
    parser.add_argument(
        '--agg-timeframe',
        type=str,
        help='Aggregated timeframe (e.g., 5m, 15m, 1h)'
    )
    parser.add_argument(
        '--timeframes',
        nargs='+',
        help='Multiple timeframes to validate (for --data-file mode)'
    )
    
    # Validation options
    parser.add_argument(
        '--validate-ohlc',
        action='store_true',
        help='Validate OHLC preservation'
    )
    parser.add_argument(
        '--validate-volume',
        action='store_true',
        help='Validate volume aggregation'
    )
    parser.add_argument(
        '--validate-boundaries',
        action='store_true',
        help='Validate timeframe boundaries'
    )
    parser.add_argument(
        '--validate-all',
        action='store_true',
        help='Validate all aspects (OHLC, volume, boundaries)'
    )
    
    # Configuration
    parser.add_argument(
        '--price-tolerance',
        type=float,
        default=0.0001,
        help='Price difference tolerance (default: 0.0001)'
    )
    parser.add_argument(
        '--volume-tolerance',
        type=float,
        default=0.01,
        help='Volume difference tolerance (default: 0.01)'
    )
    parser.add_argument(
        '--timestamp-tolerance',
        type=float,
        default=1.0,
        help='Timestamp tolerance in seconds (default: 1.0)'
    )
    
    # Column names
    parser.add_argument(
        '--timestamp-column',
        type=str,
        default='timestamp',
        help='Name of timestamp column (default: timestamp)'
    )
    parser.add_argument(
        '--timeframe-column',
        type=str,
        default='timeframe',
        help='Name of timeframe column (default: timeframe)'
    )
    parser.add_argument(
        '--symbol-column',
        type=str,
        default='symbol',
        help='Name of symbol column (default: symbol)'
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
    
    return parser.parse_args()


def load_data_file(file_path: str) -> pd.DataFrame:
    """Load data from parquet file"""
    try:
        df = pd.read_parquet(file_path)
        logger.info(f"📁 Loaded {len(df)} records from {file_path}")
        return df
        
    except Exception as e:
        logger.error(f"❌ Failed to load data file {file_path}: {e}")
        raise


def df_to_ohlcv_candles(df: pd.DataFrame, timeframe: str, symbol: str = "BTC-USDT") -> list:
    """Convert DataFrame to OHLCV candle list"""
    candles = []
    
    for _, row in df.iterrows():
        candle = OHLCV(
            timestamp=pd.to_datetime(row['timestamp']),
            open=float(row['open']),
            high=float(row['high']),
            low=float(row['low']),
            close=float(row['close']),
            volume=float(row['volume']),
            symbol=symbol,
            timeframe=timeframe,
            source='data'
        )
        candles.append(candle)
    
    return candles


def run_aggregation_validation(
    validator: AggregationValidator,
    base_candles: list,
    agg_candles: list,
    base_timeframe: str,
    agg_timeframe: str,
    validate_ohlc: bool,
    validate_volume: bool,
    validate_boundaries: bool
) -> ValidationReport:
    """Run aggregation validation"""
    
    report_id = f"aggregation_validation_{base_timeframe}_to_{agg_timeframe}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    report = ValidationReport(
        report_id=report_id,
        start_time=datetime.utcnow(),
        end_time=datetime.utcnow()
    )
    
    logger.info(f"🔄 Starting aggregation validation: {base_timeframe} -> {agg_timeframe}")
    logger.info(f"📊 Base candles: {len(base_candles)}, Aggregated candles: {len(agg_candles)}")
    
    # OHLC preservation validation
    if validate_ohlc:
        logger.info("📈 Validating OHLC preservation...")
        ohlc_result = validator.validate_ohlc_preservation(
            base_candles=base_candles,
            aggregated_candles=agg_candles,
            test_name=f"ohlc_preservation_{base_timeframe}_to_{agg_timeframe}"
        )
        report.add_result(ohlc_result)
    
    # Volume aggregation validation
    if validate_volume:
        logger.info("📊 Validating volume aggregation...")
        volume_result = validator.validate_volume_aggregation(
            base_candles=base_candles,
            aggregated_candles=agg_candles,
            test_name=f"volume_aggregation_{base_timeframe}_to_{agg_timeframe}"
        )
        report.add_result(volume_result)
    
    # Timeframe boundary validation
    if validate_boundaries:
        logger.info("⏰ Validating timeframe boundaries...")
        boundary_result = validator.validate_timeframe_boundaries(
            candles=agg_candles,
            expected_timeframe=agg_timeframe,
            test_name=f"timeframe_boundaries_{agg_timeframe}"
        )
        report.add_result(boundary_result)
    
    # Aggregation consistency validation
    logger.info("🔄 Validating aggregation consistency...")
    consistency_result = validator.validate_aggregation_consistency(
        source_candles=base_candles,
        target_candles=agg_candles,
        test_name=f"aggregation_consistency_{base_timeframe}_to_{agg_timeframe}"
    )
    report.add_result(consistency_result)
    
    report.end_time = datetime.utcnow()
    return report


def run_multi_timeframe_validation(
    validator: AggregationValidator,
    df: pd.DataFrame,
    timeframes: list,
    validate_ohlc: bool,
    validate_volume: bool,
    validate_boundaries: bool
) -> ValidationReport:
    """Run validation across multiple timeframes"""
    
    report_id = f"multi_timeframe_validation_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    report = ValidationReport(
        report_id=report_id,
        start_time=datetime.utcnow(),
        end_time=datetime.utcnow()
    )
    
    logger.info(f"🔄 Starting multi-timeframe validation: {', '.join(timeframes)}")
    
    # Get unique symbols
    symbols = df['symbol'].unique() if 'symbol' in df.columns else ['BTC-USDT']
    
    for symbol in symbols:
        logger.info(f"📊 Validating symbol: {symbol}")
        
        # Filter data for this symbol
        symbol_df = df[df['symbol'] == symbol] if 'symbol' in df.columns else df
        
        # Group by timeframe
        timeframe_groups = symbol_df.groupby('timeframe')
        
        # Get base timeframe (shortest)
        base_timeframe = min(timeframes, key=lambda x: get_timeframe_seconds(x))
        
        if base_timeframe not in timeframe_groups.groups:
            logger.warning(f"⚠️ Base timeframe {base_timeframe} not found for {symbol}")
            continue
        
        base_df = timeframe_groups.get_group(base_timeframe)
        base_candles = df_to_ohlcv_candles(base_df, base_timeframe, symbol)
        
        # Validate against each aggregated timeframe
        for agg_timeframe in timeframes:
            if agg_timeframe == base_timeframe:
                continue
            
            if agg_timeframe not in timeframe_groups.groups:
                logger.warning(f"⚠️ Timeframe {agg_timeframe} not found for {symbol}")
                continue
            
            agg_df = timeframe_groups.get_group(agg_timeframe)
            agg_candles = df_to_ohlcv_candles(agg_df, agg_timeframe, symbol)
            
            # Run validation
            timeframe_report = run_aggregation_validation(
                validator=validator,
                base_candles=base_candles,
                agg_candles=agg_candles,
                base_timeframe=base_timeframe,
                agg_timeframe=agg_timeframe,
                validate_ohlc=validate_ohlc,
                validate_volume=validate_volume,
                validate_boundaries=validate_boundaries
            )
            
            # Add results to main report
            for result in timeframe_report.results:
                report.add_result(result)
    
    report.end_time = datetime.utcnow()
    return report


def get_timeframe_seconds(timeframe: str) -> int:
    """Convert timeframe string to seconds"""
    timeframe_map = {
        '15s': 15,
        '1m': 60,
        '5m': 300,
        '15m': 900,
        '1h': 3600,
        '4h': 14400,
        '1d': 86400
    }
    
    if timeframe not in timeframe_map:
        raise ValueError(f"Unsupported timeframe: {timeframe}")
    
    return timeframe_map[timeframe]


def print_detailed_results(report: ValidationReport):
    """Print detailed validation results"""
    print("\n" + "="*80)
    print("📊 DETAILED AGGREGATION VALIDATION RESULTS")
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


def main():
    """Main entry point"""
    try:
        # Parse arguments
        args = parse_arguments()
        
        # Validate arguments
        if args.data_file:
            if not args.timeframes or len(args.timeframes) < 2:
                raise ValueError("Must specify at least 2 timeframes for multi-timeframe validation")
        else:
            if not args.base_file or not args.agg_file:
                raise ValueError("Must specify both --base-file and --agg-file for single timeframe validation")
            if not args.base_timeframe or not args.agg_timeframe:
                raise ValueError("Must specify both --base-timeframe and --agg-timeframe for single timeframe validation")
        
        # Determine validation options
        validate_ohlc = args.validate_ohlc or args.validate_all
        validate_volume = args.validate_volume or args.validate_all
        validate_boundaries = args.validate_boundaries or args.validate_all
        
        # Create validator with custom configuration
        config = AggregationValidationConfig(
            price_tolerance=args.price_tolerance,
            volume_tolerance=args.volume_tolerance,
            timestamp_tolerance_seconds=args.timestamp_tolerance
        )
        validator = AggregationValidator(config)
        
        # Run validation
        if args.data_file:
            # Multi-timeframe validation
            df = load_data_file(args.data_file)
            
            report = run_multi_timeframe_validation(
                validator=validator,
                df=df,
                timeframes=args.timeframes,
                validate_ohlc=validate_ohlc,
                validate_volume=validate_volume,
                validate_boundaries=validate_boundaries
            )
        else:
            # Single timeframe validation
            base_df = load_data_file(args.base_file)
            agg_df = load_data_file(args.agg_file)
            
            base_candles = df_to_ohlcv_candles(base_df, args.base_timeframe)
            agg_candles = df_to_ohlcv_candles(agg_df, args.agg_timeframe)
            
            report = run_aggregation_validation(
                validator=validator,
                base_candles=base_candles,
                agg_candles=agg_candles,
                base_timeframe=args.base_timeframe,
                agg_timeframe=args.agg_timeframe,
                validate_ohlc=validate_ohlc,
                validate_volume=validate_volume,
                validate_boundaries=validate_boundaries
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
            logger.info("✅ All aggregation validations passed!")
            sys.exit(0)
        else:
            logger.warning(f"⚠️ {report.failed_tests} validations failed")
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"❌ Aggregation validation failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()