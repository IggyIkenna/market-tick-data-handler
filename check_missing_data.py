#!/usr/bin/env python3
"""
Standalone Missing Data Checker

This script provides a convenient way to check for missing data in your GCS bucket
without running the full pipeline. It compares instrument definitions against
actual tick data availability.

Usage:
    python check_missing_data.py --start-date 2023-05-23 --end-date 2023-05-25
    python check_missing_data.py --start-date 2023-05-23 --end-date 2023-05-25 --venues deribit binance
    python check_missing_data.py --start-date 2023-05-23 --end-date 2023-05-25 --data-types trades book_snapshot_5
    python check_missing_data.py --start-date 2023-05-23 --end-date 2023-05-25 --output missing_data_report.csv
"""

import sys
import os
import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

# Add project root to path for imports
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from config import get_config
from src.data_validator.data_validator import DataValidator

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
        description='Standalone Missing Data Checker - Check for missing tick data in GCS',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check all data for a date range
  python check_missing_data.py --start-date 2023-05-23 --end-date 2023-05-25
  
  # Check specific venues
  python check_missing_data.py --start-date 2023-05-23 --end-date 2023-05-25 --venues deribit binance
  
  # Check specific data types
  python check_missing_data.py --start-date 2023-05-23 --end-date 2023-05-25 --data-types trades book_snapshot_5
  
  # Save results to CSV
  python check_missing_data.py --start-date 2023-05-23 --end-date 2023-05-25 --output missing_data_report.csv
  
  # Check specific instrument types
  python check_missing_data.py --start-date 2023-05-23 --end-date 2023-05-25 --instrument-types option perpetual
        """
    )
    
    # Date range
    parser.add_argument(
        '--start-date',
        type=str,
        required=True,
        help='Start date in YYYY-MM-DD format'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        required=True,
        help='End date in YYYY-MM-DD format'
    )
    
    # Filtering options
    parser.add_argument(
        '--venues',
        nargs='+',
        help='Venues to check (e.g., deribit binance bybit)'
    )
    parser.add_argument(
        '--instrument-types',
        nargs='+',
        help='Instrument types to check (e.g., option perpetual spot)'
    )
    parser.add_argument(
        '--data-types',
        nargs='+',
        help='Data types to check (e.g., trades book_snapshot_5)'
    )
    
    # Output options
    parser.add_argument(
        '--output', '-o',
        type=str,
        help='Output CSV file path for missing data report'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Show detailed missing data information'
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

def print_summary_report(report: dict):
    """Print a formatted summary report"""
    print("\n" + "="*60)
    print("📊 MISSING DATA SUMMARY REPORT")
    print("="*60)
    
    print(f"Status: {report['status'].upper()}")
    print(f"Coverage: {report['coverage_percentage']:.1f}%")
    print(f"Missing entries: {report['missing_count']}")
    print(f"Total days checked: {report.get('total_days', 'N/A')}")
    print(f"Summary: {report['summary']}")
    
    if report['status'] == 'complete':
        print("\n✅ All expected data is available!")
    else:
        print(f"\n⚠️ Found {report['missing_count']} missing data entries")
        
        # Show daily breakdown
        if 'daily_coverage' in report and report['daily_coverage']:
            print("\n📅 Daily Missing Data Breakdown:")
            for day in report['daily_coverage'][:10]:  # Show first 10 days
                print(f"  {day['date']}: {day['missing_count']} missing")
            if len(report['daily_coverage']) > 10:
                print(f"  ... and {len(report['daily_coverage']) - 10} more days")
        
        # Show instrument breakdown
        if 'instrument_coverage' in report and report['instrument_coverage']:
            print("\n🎯 Most Problematic Instruments:")
            for instrument in report['instrument_coverage'][:10]:  # Show top 10
                print(f"  {instrument['instrument_key']}: {instrument['missing_days']} missing days")
            if len(report['instrument_coverage']) > 10:
                print(f"  ... and {len(report['instrument_coverage']) - 10} more instruments")
        
        # Show data type breakdown
        if 'data_type_coverage' in report and report['data_type_coverage']:
            print("\n📊 Missing Data by Type:")
            for data_type in report['data_type_coverage']:
                print(f"  {data_type['data_type']}: {data_type['missing_count']} missing")
    
    print("="*60)

def main():
    """Main entry point"""
    try:
        # Parse arguments
        args = parse_arguments()
        
        # Load environment file if specified
        if args.env_file:
            load_environment_file(args.env_file)
        
        # Load configuration
        config = get_config()
        
        # Parse dates
        start_date = parse_date(args.start_date)
        end_date = parse_date(args.end_date)
        
        if start_date > end_date:
            raise ValueError("Start date must be before or equal to end date")
        
        # Create data validator
        validator = DataValidator(config.gcp.bucket)
        
        logger.info(f"🔍 Checking missing data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        if args.venues:
            logger.info(f"🏢 Filtering by venues: {args.venues}")
        if args.instrument_types:
            logger.info(f"📊 Filtering by instrument types: {args.instrument_types}")
        if args.data_types:
            logger.info(f"📈 Filtering by data types: {args.data_types}")
        
        # Generate missing data report
        report = validator.generate_missing_data_report(
            start_date=start_date,
            end_date=end_date,
            venues=args.venues,
            instrument_types=args.instrument_types,
            data_types=args.data_types
        )
        
        # Print summary report
        print_summary_report(report)
        
        # Get detailed missing data if requested
        if args.verbose or args.output:
            logger.info("📋 Getting detailed missing data...")
            
            if args.data_types:
                missing_df = validator.check_missing_data_by_type(
                    start_date, end_date, args.venues, args.instrument_types, args.data_types
                )
            else:
                missing_df = validator.check_missing_data(
                    start_date, end_date, args.venues, args.instrument_types
                )
            
            if not missing_df.empty:
                if args.verbose:
                    print("\n📋 DETAILED MISSING DATA:")
                    print("-" * 60)
                    print(missing_df.to_string(index=False))
                
                if args.output:
                    missing_df.to_csv(args.output, index=False)
                    logger.info(f"💾 Saved detailed report to: {args.output}")
            else:
                logger.info("✅ No missing data found - detailed report not needed")
        
        # Exit with appropriate code
        if report['status'] == 'complete':
            logger.info("✅ All data is complete!")
            sys.exit(0)
        else:
            logger.warning(f"⚠️ Found {report['missing_count']} missing data entries")
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"❌ Missing data check failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()
