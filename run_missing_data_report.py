#!/usr/bin/env python3
"""
Script to generate missing data reports for a date range and upload to GCS.

This script runs the missing data validation day by day from 2023-05-23 to 2025-10-21,
uploading each day's missing data report to GCS as a parquet file.

Usage:
    python run_missing_data_report.py --start-date 2023-05-23 --end-date 2025-10-21
    python run_missing_data_report.py --start-date 2023-05-23 --end-date 2023-05-25 --data-types trades book_snapshot_5
"""

import sys
import os
import argparse
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
import asyncio

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
        description='Generate missing data reports for a date range and upload to GCS',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate reports for full date range
  python run_missing_data_report.py --start-date 2023-05-23 --end-date 2025-10-21

  # Generate reports for specific date range with data types
  python run_missing_data_report.py --start-date 2023-05-23 --end-date 2023-05-25 --data-types trades book_snapshot_5

  # Generate reports for specific venues
  python run_missing_data_report.py --start-date 2023-05-23 --end-date 2023-05-25 --venues deribit binance
        """
    )
    
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
    parser.add_argument(
        '--venues',
        nargs='+',
        help='List of venues to filter (e.g., deribit binance)'
    )
    parser.add_argument(
        '--instrument-types',
        nargs='+',
        help='List of instrument types to filter (e.g., option perpetual)'
    )
    parser.add_argument(
        '--data-types',
        nargs='+',
        help='List of data types to check (e.g., trades book_snapshot_5). Defaults to trades and book_snapshot_5.'
    )
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Logging level'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run without uploading to GCS (for testing)'
    )
    
    return parser.parse_args()

def setup_logging(log_level: str):
    """Setup logging configuration"""
    logging.getLogger().setLevel(getattr(logging, log_level.upper()))
    logging.getLogger('google.cloud').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)

def parse_date(date_str: str) -> datetime:
    """Parse date string to timezone-aware datetime"""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    except ValueError as e:
        raise ValueError(f"Invalid date format '{date_str}'. Use YYYY-MM-DD format.") from e

async def generate_missing_data_reports(start_date: datetime, end_date: datetime, 
                                      venues: list = None, instrument_types: list = None,
                                      data_types: list = None, dry_run: bool = False):
    """Generate missing data reports for each day in the date range"""
    
    config = get_config()
    data_validator = DataValidator(config.gcp.bucket)
    
    if data_types is None:
        data_types = ['trades', 'book_snapshot_5']
    
    logger.info("============================================================")
    logger.info("📊 MISSING DATA REPORT GENERATION")
    logger.info("============================================================")
    logger.info(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    logger.info(f"Venues: {venues or 'all'}")
    logger.info(f"Instrument types: {instrument_types or 'all'}")
    logger.info(f"Data types: {data_types}")
    logger.info(f"Dry run: {dry_run}")
    logger.info("============================================================")
    
    total_days = (end_date - start_date).days + 1
    processed_days = 0
    days_with_missing_data = 0
    total_missing_entries = 0
    
    current_date = start_date
    while current_date <= end_date:
        processed_days += 1
        date_str = current_date.strftime('%Y-%m-%d')
        
        logger.info(f"📅 Processing {date_str} ({processed_days}/{total_days})...")
        
        try:
            if dry_run:
                logger.info(f"🔍 [DRY RUN] Would generate missing data report for {date_str}")
                # Simulate some missing data for dry run
                if processed_days % 3 == 0:  # Every 3rd day has missing data
                    days_with_missing_data += 1
                    total_missing_entries += 100
                    logger.info(f"🔍 [DRY RUN] Would find ~100 missing entries for {date_str}")
                else:
                    logger.info(f"🔍 [DRY RUN] No missing data for {date_str}")
            else:
                # Generate actual missing data report
                report = data_validator.generate_missing_data_report(
                    start_date=current_date,
                    end_date=current_date,
                    venues=venues,
                    instrument_types=instrument_types,
                    data_types=data_types,
                    upload_to_gcs=True
                )
                
                if report['missing_count'] > 0:
                    days_with_missing_data += 1
                    total_missing_entries += report['missing_count']
                    logger.info(f"📤 Uploaded missing data report for {date_str}: {report['missing_count']} missing entries")
                else:
                    logger.info(f"✅ No missing data for {date_str}")
            
            # Progress update every 10 days
            if processed_days % 10 == 0:
                progress = (processed_days / total_days) * 100
                logger.info(f"📊 Progress: {processed_days}/{total_days} days ({progress:.1f}%)")
                
        except Exception as e:
            logger.error(f"❌ Error processing {date_str}: {e}")
        
        current_date += timedelta(days=1)
    
    # Final summary
    logger.info("============================================================")
    logger.info("🎉 MISSING DATA REPORT GENERATION COMPLETED")
    logger.info("============================================================")
    logger.info(f"📊 Total days processed: {processed_days}")
    logger.info(f"📈 Days with missing data: {days_with_missing_data}")
    logger.info(f"❌ Total missing entries: {total_missing_entries}")
    logger.info(f"📋 Coverage: {((processed_days - days_with_missing_data) / processed_days * 100):.1f}% complete")
    
    if not dry_run:
        logger.info("📤 All missing data reports uploaded to GCS")
        logger.info("💡 Use 'download-missing' mode to download only missing data")

async def main():
    """Main entry point for the missing data report generation script"""
    args = parse_arguments()
    setup_logging(args.log_level)

    try:
        start_date = parse_date(args.start_date)
        end_date = parse_date(args.end_date)

        if start_date > end_date:
            raise ValueError("Start date must be before or equal to end date")

        await generate_missing_data_reports(
            start_date=start_date,
            end_date=end_date,
            venues=args.venues,
            instrument_types=args.instrument_types,
            data_types=args.data_types,
            dry_run=args.dry_run
        )

    except ValueError as e:
        logger.error(f"❌ Configuration Error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ An unexpected error occurred: {e}", exc_info=args.log_level == 'DEBUG')
        sys.exit(1)

if __name__ == '__main__':
    asyncio.run(main())
