#!/usr/bin/env python3
"""
Run Validation Tests

Comprehensive test runner for the validation framework.
Runs all validation tests and generates detailed reports.

Usage:
    python run_validation_tests.py --test-type all
    python run_validation_tests.py --test-type cross-source --symbol BTC-USDT --timeframe 1m
    python run_validation_tests.py --test-type timestamp --data-file test_data.parquet
    python run_validation_tests.py --test-type aggregation --base-file 1m.parquet --agg-file 5m.parquet
"""

import sys
import os
import argparse
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
import subprocess
import json

# Add project root to path for imports
project_root = Path(__file__).parent
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
        description='Run Validation Tests - Comprehensive Test Runner',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all tests
  python run_validation_tests.py --test-type all
  
  # Run cross-source validation tests
  python run_validation_tests.py --test-type cross-source --symbol BTC-USDT --timeframe 1m
  
  # Run timestamp validation tests
  python run_validation_tests.py --test-type timestamp --data-file test_data.parquet
  
  # Run aggregation validation tests
  python run_validation_tests.py --test-type aggregation --base-file 1m.parquet --agg-file 5m.parquet
  
  # Run with custom configuration
  python run_validation_tests.py --test-type all --config custom_config.json
        """
    )
    
    # Test type
    parser.add_argument(
        '--test-type',
        type=str,
        choices=['all', 'cross-source', 'timestamp', 'aggregation', 'unit', 'integration'],
        required=True,
        help='Type of tests to run'
    )
    
    # Cross-source test parameters
    parser.add_argument(
        '--symbol',
        type=str,
        help='Trading symbol for cross-source tests (e.g., BTC-USDT)'
    )
    parser.add_argument(
        '--timeframe',
        type=str,
        help='Timeframe for cross-source tests (e.g., 1m, 5m)'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        help='Start date for historical tests (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        help='End date for historical tests (YYYY-MM-DD)'
    )
    
    # Data file parameters
    parser.add_argument(
        '--data-file',
        type=str,
        help='Data file for timestamp/aggregation tests'
    )
    parser.add_argument(
        '--base-file',
        type=str,
        help='Base timeframe file for aggregation tests'
    )
    parser.add_argument(
        '--agg-file',
        type=str,
        help='Aggregated timeframe file for aggregation tests'
    )
    
    # Test configuration
    parser.add_argument(
        '--config',
        type=str,
        help='Custom test configuration file (JSON)'
    )
    parser.add_argument(
        '--max-candles',
        type=int,
        default=1000,
        help='Maximum number of candles to fetch (default: 1000)'
    )
    parser.add_argument(
        '--parallel',
        action='store_true',
        help='Run tests in parallel where possible'
    )
    
    # Output options
    parser.add_argument(
        '--output',
        type=str,
        help='Output directory for test results'
    )
    parser.add_argument(
        '--format',
        type=str,
        choices=['json', 'html', 'csv'],
        default='json',
        help='Output format for test results (default: json)'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Show detailed test output'
    )
    
    return parser.parse_args()


def load_test_config(config_file: str) -> dict:
    """Load test configuration from file"""
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        logger.info(f"📁 Loaded test configuration from {config_file}")
        return config
    except Exception as e:
        logger.error(f"❌ Failed to load test configuration: {e}")
        return {}


def run_unit_tests() -> bool:
    """Run unit tests using pytest"""
    logger.info("🧪 Running unit tests...")
    
    try:
        # Run pytest on the tests directory
        cmd = [
            sys.executable, '-m', 'pytest',
            'tests/',
            '-v',
            '--tb=short',
            '--disable-warnings'
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info("✅ All unit tests passed!")
            return True
        else:
            logger.error("❌ Some unit tests failed!")
            logger.error(result.stdout)
            logger.error(result.stderr)
            return False
            
    except Exception as e:
        logger.error(f"❌ Failed to run unit tests: {e}")
        return False


async def run_cross_source_tests(
    symbol: str,
    timeframe: str,
    start_date: str = None,
    end_date: str = None,
    max_candles: int = 1000
) -> ValidationReport:
    """Run cross-source validation tests"""
    logger.info(f"🔄 Running cross-source tests for {symbol} {timeframe}")
    
    # Load configuration
    config = get_config()
    
    # Create validators
    data_client = DataClient(config.gcp.bucket)
    tardis_connector = TardisConnector()
    validator = CrossSourceValidator(data_client, tardis_connector)
    
    # Create test report
    report_id = f"cross_source_tests_{symbol}_{timeframe}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    report = ValidationReport(
        report_id=report_id,
        start_time=datetime.utcnow(),
        end_time=datetime.utcnow()
    )
    
    try:
        if start_date and end_date:
            # Historical test
            start_dt = datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            end_dt = datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            
            result = await validator.validate_timeframe_consistency(
                symbol=symbol,
                timeframe=timeframe,
                start_date=start_dt,
                end_date=end_dt,
                max_candles=max_candles
            )
        else:
            # Live test
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=1)
            
            result = await validator.validate_timeframe_consistency(
                symbol=symbol,
                timeframe=timeframe,
                start_date=start_time,
                end_date=end_time,
                max_candles=max_candles
            )
        
        report.add_result(result)
        
    except Exception as e:
        logger.error(f"❌ Cross-source test failed: {e}")
        from market_data_tick_handler.validation.validation_results import ValidationResult, ValidationStatus
        error_result = ValidationResult(
            test_name="cross_source_test",
            status=ValidationStatus.FAIL,
            message=f"Cross-source test failed: {str(e)}",
            details={'error': str(e)}
        )
        report.add_result(error_result)
    
    report.end_time = datetime.utcnow()
    return report


def run_timestamp_tests(data_file: str) -> ValidationReport:
    """Run timestamp validation tests"""
    logger.info(f"⏰ Running timestamp tests with {data_file}")
    
    # Create validator
    validator = TimestampValidator()
    
    # Create test report
    report_id = f"timestamp_tests_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    report = ValidationReport(
        report_id=report_id,
        start_time=datetime.utcnow(),
        end_time=datetime.utcnow()
    )
    
    try:
        # Load data
        import pandas as pd
        df = pd.read_parquet(data_file)
        
        # Extract timestamps
        timestamps = df['timestamp'].dt.to_pydatetime().tolist()
        
        # Run timestamp stability test
        stability_result = validator.validate_timestamp_stability(
            timestamps=timestamps,
            expected_interval_seconds=60.0,
            test_name="timestamp_stability_test"
        )
        report.add_result(stability_result)
        
        # Run timezone consistency test
        timezone_result = validator.validate_timezone_consistency(
            timestamps=timestamps,
            test_name="timezone_consistency_test"
        )
        report.add_result(timezone_result)
        
        # Run data freshness test
        freshness_result = validator.validate_data_freshness(
            timestamps=timestamps,
            test_name="data_freshness_test"
        )
        report.add_result(freshness_result)
        
    except Exception as e:
        logger.error(f"❌ Timestamp test failed: {e}")
        from market_data_tick_handler.validation.validation_results import ValidationResult, ValidationStatus
        error_result = ValidationResult(
            test_name="timestamp_test",
            status=ValidationStatus.FAIL,
            message=f"Timestamp test failed: {str(e)}",
            details={'error': str(e)}
        )
        report.add_result(error_result)
    
    report.end_time = datetime.utcnow()
    return report


def run_aggregation_tests(base_file: str, agg_file: str) -> ValidationReport:
    """Run aggregation validation tests"""
    logger.info(f"📊 Running aggregation tests with {base_file} and {agg_file}")
    
    # Create validator
    validator = AggregationValidator()
    
    # Create test report
    report_id = f"aggregation_tests_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    report = ValidationReport(
        report_id=report_id,
        start_time=datetime.utcnow(),
        end_time=datetime.utcnow()
    )
    
    try:
        # Load data
        import pandas as pd
        from market_data_tick_handler.validation.cross_source_validator import OHLCV
        
        base_df = pd.read_parquet(base_file)
        agg_df = pd.read_parquet(agg_file)
        
        # Convert to OHLCV candles
        base_candles = []
        for _, row in base_df.iterrows():
            candle = OHLCV(
                timestamp=pd.to_datetime(row['timestamp']),
                open=float(row['open']),
                high=float(row['high']),
                low=float(row['low']),
                close=float(row['close']),
                volume=float(row['volume']),
                symbol=row.get('symbol', 'BTC-USDT'),
                timeframe=row.get('timeframe', '1m'),
                source='test'
            )
            base_candles.append(candle)
        
        agg_candles = []
        for _, row in agg_df.iterrows():
            candle = OHLCV(
                timestamp=pd.to_datetime(row['timestamp']),
                open=float(row['open']),
                high=float(row['high']),
                low=float(row['low']),
                close=float(row['close']),
                volume=float(row['volume']),
                symbol=row.get('symbol', 'BTC-USDT'),
                timeframe=row.get('timeframe', '5m'),
                source='test'
            )
            agg_candles.append(candle)
        
        # Run OHLC preservation test
        ohlc_result = validator.validate_ohlc_preservation(
            base_candles=base_candles,
            aggregated_candles=agg_candles,
            test_name="ohlc_preservation_test"
        )
        report.add_result(ohlc_result)
        
        # Run volume aggregation test
        volume_result = validator.validate_volume_aggregation(
            base_candles=base_candles,
            aggregated_candles=agg_candles,
            test_name="volume_aggregation_test"
        )
        report.add_result(volume_result)
        
        # Run aggregation consistency test
        consistency_result = validator.validate_aggregation_consistency(
            source_candles=base_candles,
            target_candles=agg_candles,
            test_name="aggregation_consistency_test"
        )
        report.add_result(consistency_result)
        
    except Exception as e:
        logger.error(f"❌ Aggregation test failed: {e}")
        from market_data_tick_handler.validation.validation_results import ValidationResult, ValidationStatus
        error_result = ValidationResult(
            test_name="aggregation_test",
            status=ValidationStatus.FAIL,
            message=f"Aggregation test failed: {str(e)}",
            details={'error': str(e)}
        )
        report.add_result(error_result)
    
    report.end_time = datetime.utcnow()
    return report


def save_test_results(report: ValidationReport, output_dir: str, format: str):
    """Save test results to file"""
    try:
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        
        if format == 'json':
            output_file = os.path.join(output_dir, f"test_results_{timestamp}.json")
            with open(output_file, 'w') as f:
                json.dump(report.to_dict(), f, indent=2, default=str)
        
        elif format == 'html':
            output_file = os.path.join(output_dir, f"test_results_{timestamp}.html")
            # Generate HTML report
            html_content = generate_html_report(report)
            with open(output_file, 'w') as f:
                f.write(html_content)
        
        elif format == 'csv':
            output_file = os.path.join(output_dir, f"test_results_{timestamp}.csv")
            df = report.to_dataframe()
            df.to_csv(output_file, index=False)
        
        logger.info(f"💾 Test results saved to: {output_file}")
        
    except Exception as e:
        logger.error(f"❌ Failed to save test results: {e}")


def generate_html_report(report: ValidationReport) -> str:
    """Generate HTML report"""
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Validation Test Results - {report.report_id}</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            .header {{ background-color: #f0f0f0; padding: 20px; border-radius: 5px; }}
            .summary {{ margin: 20px 0; }}
            .test-result {{ margin: 10px 0; padding: 10px; border-left: 4px solid #ccc; }}
            .pass {{ border-left-color: #4CAF50; background-color: #f1f8e9; }}
            .fail {{ border-left-color: #f44336; background-color: #ffebee; }}
            .warning {{ border-left-color: #ff9800; background-color: #fff3e0; }}
            .details {{ margin-left: 20px; font-size: 0.9em; color: #666; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>Validation Test Results</h1>
            <p><strong>Report ID:</strong> {report.report_id}</p>
            <p><strong>Start Time:</strong> {report.start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
            <p><strong>End Time:</strong> {report.end_time.strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
            <p><strong>Duration:</strong> {(report.end_time - report.start_time).total_seconds():.2f} seconds</p>
        </div>
        
        <div class="summary">
            <h2>Summary</h2>
            <p><strong>Total Tests:</strong> {report.total_tests}</p>
            <p><strong>Passed:</strong> {report.passed_tests}</p>
            <p><strong>Failed:</strong> {report.failed_tests}</p>
            <p><strong>Warnings:</strong> {report.warning_tests}</p>
            <p><strong>Skipped:</strong> {report.skipped_tests}</p>
            <p><strong>Success Rate:</strong> {report.get_success_rate():.1f}%</p>
        </div>
        
        <div class="test-results">
            <h2>Test Results</h2>
    """
    
    for result in report.results:
        status_class = result.status.value.lower()
        html += f"""
            <div class="test-result {status_class}">
                <h3>{result.test_name}</h3>
                <p><strong>Status:</strong> {result.status.value}</p>
                <p><strong>Message:</strong> {result.message}</p>
                <p><strong>Execution Time:</strong> {result.execution_time_ms:.2f}ms</p>
        """
        
        if result.details:
            html += "<div class='details'><h4>Details:</h4><ul>"
            for key, value in result.details.items():
                html += f"<li><strong>{key}:</strong> {value}</li>"
            html += "</ul></div>"
        
        html += "</div>"
    
    html += """
        </div>
    </body>
    </html>
    """
    
    return html


async def main():
    """Main entry point"""
    try:
        # Parse arguments
        args = parse_arguments()
        
        # Load test configuration if provided
        test_config = {}
        if args.config:
            test_config = load_test_config(args.config)
        
        # Create output directory
        output_dir = args.output or "test_results"
        os.makedirs(output_dir, exist_ok=True)
        
        # Run tests based on type
        if args.test_type == 'all':
            logger.info("🚀 Running all validation tests...")
            
            # Run unit tests
            unit_success = run_unit_tests()
            
            # Run other tests if data is available
            reports = []
            
            if args.symbol and args.timeframe:
                cross_source_report = await run_cross_source_tests(
                    symbol=args.symbol,
                    timeframe=args.timeframe,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    max_candles=args.max_candles
                )
                reports.append(cross_source_report)
            
            if args.data_file:
                timestamp_report = run_timestamp_tests(args.data_file)
                reports.append(timestamp_report)
            
            if args.base_file and args.agg_file:
                aggregation_report = run_aggregation_tests(args.base_file, args.agg_file)
                reports.append(aggregation_report)
            
            # Combine all reports
            if reports:
                combined_report = ValidationReport(
                    report_id=f"combined_tests_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
                    start_time=min(r.start_time for r in reports),
                    end_time=max(r.end_time for r in reports)
                )
                
                for report in reports:
                    for result in report.results:
                        combined_report.add_result(result)
                
                combined_report.print_summary()
                
                if args.verbose:
                    for result in combined_report.results:
                        print(f"\n{result.test_name}: {result.status.value} - {result.message}")
                
                # Save results
                save_test_results(combined_report, output_dir, args.format)
                
                # Exit with appropriate code
                if combined_report.get_status().value == 'PASS':
                    logger.info("✅ All validation tests passed!")
                    sys.exit(0)
                else:
                    logger.warning(f"⚠️ {combined_report.failed_tests} tests failed")
                    sys.exit(1)
            else:
                if unit_success:
                    logger.info("✅ All unit tests passed!")
                    sys.exit(0)
                else:
                    logger.error("❌ Some unit tests failed!")
                    sys.exit(1)
        
        elif args.test_type == 'unit':
            success = run_unit_tests()
            sys.exit(0 if success else 1)
        
        elif args.test_type == 'cross-source':
            if not args.symbol or not args.timeframe:
                logger.error("❌ Must specify --symbol and --timeframe for cross-source tests")
                sys.exit(1)
            
            report = await run_cross_source_tests(
                symbol=args.symbol,
                timeframe=args.timeframe,
                start_date=args.start_date,
                end_date=args.end_date,
                max_candles=args.max_candles
            )
            
            report.print_summary()
            save_test_results(report, output_dir, args.format)
            sys.exit(0 if report.get_status().value == 'PASS' else 1)
        
        elif args.test_type == 'timestamp':
            if not args.data_file:
                logger.error("❌ Must specify --data-file for timestamp tests")
                sys.exit(1)
            
            report = run_timestamp_tests(args.data_file)
            report.print_summary()
            save_test_results(report, output_dir, args.format)
            sys.exit(0 if report.get_status().value == 'PASS' else 1)
        
        elif args.test_type == 'aggregation':
            if not args.base_file or not args.agg_file:
                logger.error("❌ Must specify --base-file and --agg-file for aggregation tests")
                sys.exit(1)
            
            report = run_aggregation_tests(args.base_file, args.agg_file)
            report.print_summary()
            save_test_results(report, output_dir, args.format)
            sys.exit(0 if report.get_status().value == 'PASS' else 1)
        
        else:
            logger.error(f"❌ Unknown test type: {args.test_type}")
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"❌ Test runner failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    asyncio.run(main())
