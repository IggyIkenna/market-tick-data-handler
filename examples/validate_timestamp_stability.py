#!/usr/bin/env python3
"""
Validate Timestamp Stability

Comprehensive timestamp validation script for market data.
Validates timestamp stability, alignment, and consistency.

Usage:
    python examples/validate_timestamp_stability.py --data-file data.parquet --timeframe 1m
    python examples/validate_timestamp_stability.py --data-file data.parquet --timeframe 1m --check-freshness
    python examples/validate_timestamp_stability.py --data-file data.parquet --timeframe 1m --check-sync --local-file local.parquet --server-file server.parquet
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

from market_data_tick_handler.validation.timestamp_validator import TimestampValidator, TimestampValidationConfig
from market_data_tick_handler.validation.validation_results import ValidationReport

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
        description='Validate Timestamp Stability - Comprehensive Timestamp Validation',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic timestamp stability validation
  python examples/validate_timestamp_stability.py --data-file data.parquet --timeframe 1m
  
  # Validate with freshness check
  python examples/validate_timestamp_stability.py --data-file data.parquet --timeframe 1m --check-freshness
  
  # Validate clock synchronization
  python examples/validate_timestamp_stability.py --local-file local.parquet --server-file server.parquet --check-sync
  
  # Validate with custom tolerances
  python examples/validate_timestamp_stability.py --data-file data.parquet --timeframe 1m --max-drift 2.0 --max-skew 10.0
  
  # Save results to file
  python examples/validate_timestamp_stability.py --data-file data.parquet --timeframe 1m --output results.json
        """
    )
    
    # Data files
    parser.add_argument(
        '--data-file',
        type=str,
        help='Parquet file containing timestamp data'
    )
    parser.add_argument(
        '--local-file',
        type=str,
        help='Parquet file containing local timestamps'
    )
    parser.add_argument(
        '--server-file',
        type=str,
        help='Parquet file containing server timestamps'
    )
    
    # Validation options
    parser.add_argument(
        '--timeframe',
        type=str,
        help='Expected timeframe (e.g., 1m, 5m, 15m, 1h)'
    )
    parser.add_argument(
        '--check-freshness',
        action='store_true',
        help='Check data freshness'
    )
    parser.add_argument(
        '--check-sync',
        action='store_true',
        help='Check clock synchronization between local and server timestamps'
    )
    parser.add_argument(
        '--check-timezone',
        action='store_true',
        help='Check timezone consistency'
    )
    
    # Configuration
    parser.add_argument(
        '--max-drift',
        type=float,
        default=1.0,
        help='Maximum timestamp drift in seconds (default: 1.0)'
    )
    parser.add_argument(
        '--max-skew',
        type=float,
        default=5.0,
        help='Maximum clock skew in seconds (default: 5.0)'
    )
    parser.add_argument(
        '--min-freshness',
        type=float,
        default=300.0,
        help='Minimum data freshness in seconds (default: 300.0)'
    )
    parser.add_argument(
        '--timestamp-column',
        type=str,
        default='timestamp',
        help='Name of timestamp column (default: timestamp)'
    )
    parser.add_argument(
        '--local-timestamp-column',
        type=str,
        default='local_timestamp',
        help='Name of local timestamp column (default: local_timestamp)'
    )
    parser.add_argument(
        '--server-timestamp-column',
        type=str,
        default='server_timestamp',
        help='Name of server timestamp column (default: server_timestamp)'
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


def load_data_file(file_path: str, timestamp_column: str) -> pd.DataFrame:
    """Load data from parquet file"""
    try:
        df = pd.read_parquet(file_path)
        
        if timestamp_column not in df.columns:
            raise ValueError(f"Timestamp column '{timestamp_column}' not found in file")
        
        # Convert timestamp column to datetime
        df[timestamp_column] = pd.to_datetime(df[timestamp_column])
        
        logger.info(f"📁 Loaded {len(df)} records from {file_path}")
        return df
        
    except Exception as e:
        logger.error(f"❌ Failed to load data file {file_path}: {e}")
        raise


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


def run_timestamp_stability_validation(
    validator: TimestampValidator,
    df: pd.DataFrame,
    timestamp_column: str,
    timeframe: str
) -> ValidationReport:
    """Run timestamp stability validation"""
    
    report_id = f"timestamp_stability_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    report = ValidationReport(
        report_id=report_id,
        start_time=datetime.utcnow(),
        end_time=datetime.utcnow()
    )
    
    logger.info("🔍 Starting timestamp stability validation...")
    
    # Extract timestamps
    timestamps = df[timestamp_column].dt.to_pydatetime().tolist()
    expected_interval = get_timeframe_seconds(timeframe)
    
    # Run validation
    result = validator.validate_timestamp_stability(
        timestamps=timestamps,
        expected_interval_seconds=expected_interval,
        test_name=f"timestamp_stability_{timeframe}"
    )
    report.add_result(result)
    
    # Timezone consistency check
    timezone_result = validator.validate_timezone_consistency(
        timestamps=timestamps,
        test_name=f"timezone_consistency_{timeframe}"
    )
    report.add_result(timezone_result)
    
    # Data freshness check
    freshness_result = validator.validate_data_freshness(
        timestamps=timestamps,
        test_name=f"data_freshness_{timeframe}"
    )
    report.add_result(freshness_result)
    
    report.end_time = datetime.utcnow()
    return report


def run_clock_synchronization_validation(
    validator: TimestampValidator,
    local_df: pd.DataFrame,
    server_df: pd.DataFrame,
    local_column: str,
    server_column: str
) -> ValidationReport:
    """Run clock synchronization validation"""
    
    report_id = f"clock_synchronization_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    report = ValidationReport(
        report_id=report_id,
        start_time=datetime.utcnow(),
        end_time=datetime.utcnow()
    )
    
    logger.info("🕐 Starting clock synchronization validation...")
    
    # Extract timestamps
    local_timestamps = local_df[local_column].dt.to_pydatetime().tolist()
    server_timestamps = server_df[server_column].dt.to_pydatetime().tolist()
    
    # Run validation
    result = validator.validate_clock_synchronization(
        local_timestamps=local_timestamps,
        server_timestamps=server_timestamps,
        test_name="clock_synchronization"
    )
    report.add_result(result)
    
    report.end_time = datetime.utcnow()
    return report


def print_detailed_results(report: ValidationReport):
    """Print detailed validation results"""
    print("\n" + "="*80)
    print("📊 DETAILED TIMESTAMP VALIDATION RESULTS")
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
        if args.check_sync:
            if not args.local_file or not args.server_file:
                raise ValueError("Must specify both --local-file and --server-file for clock synchronization check")
        else:
            if not args.data_file:
                raise ValueError("Must specify --data-file for timestamp stability validation")
            if not args.timeframe:
                raise ValueError("Must specify --timeframe for timestamp stability validation")
        
        # Create validator with custom configuration
        config = TimestampValidationConfig(
            max_timestamp_drift_seconds=args.max_drift,
            max_clock_skew_seconds=args.max_skew,
            min_data_freshness_seconds=args.min_freshness
        )
        validator = TimestampValidator(config)
        
        # Run validation
        if args.check_sync:
            # Clock synchronization validation
            local_df = load_data_file(args.local_file, args.local_timestamp_column)
            server_df = load_data_file(args.server_file, args.server_timestamp_column)
            
            report = run_clock_synchronization_validation(
                validator=validator,
                local_df=local_df,
                server_df=server_df,
                local_column=args.local_timestamp_column,
                server_column=args.server_timestamp_column
            )
        else:
            # Timestamp stability validation
            df = load_data_file(args.data_file, args.timestamp_column)
            
            report = run_timestamp_stability_validation(
                validator=validator,
                df=df,
                timestamp_column=args.timestamp_column,
                timeframe=args.timeframe
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
            logger.info("✅ All timestamp validations passed!")
            sys.exit(0)
        else:
            logger.warning(f"⚠️ {report.failed_tests} validations failed")
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"❌ Timestamp validation failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
