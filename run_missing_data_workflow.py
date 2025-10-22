#!/usr/bin/env python3
"""
Centralized workflow for missing data report generation and downloads.

This script runs the complete workflow:
1. Generate missing data reports for a date range and upload to GCS
2. Download only the missing data based on those reports

This approach is more efficient than each download checking for missing data individually.

Usage:
    python run_missing_data_workflow.py --start-date 2023-05-23 --end-date 2023-05-25
    python run_missing_data_workflow.py --start-date 2023-05-23 --end-date 2025-10-21 --skip-reports
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
from src.data_downloader.download_orchestrator import DownloadOrchestrator

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
        description='Centralized workflow for missing data report generation and downloads',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full workflow: generate reports then download missing data
  python run_missing_data_workflow.py --start-date 2023-05-23 --end-date 2023-05-25

  # Skip report generation, only download missing data (reports must exist in GCS)
  python run_missing_data_workflow.py --start-date 2023-05-23 --end-date 2023-05-25 --skip-reports

  # Generate reports only (no downloads)
  python run_missing_data_workflow.py --start-date 2023-05-23 --end-date 2023-05-25 --reports-only

  # Download only (reports must exist in GCS)
  python run_missing_data_workflow.py --start-date 2023-05-23 --end-date 2023-05-25 --download-only
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
        '--max-instruments',
        type=int,
        help='Maximum number of instruments to process per day'
    )
    parser.add_argument(
        '--shard-index',
        type=int,
        help='Shard index for distributed processing (0-based)'
    )
    parser.add_argument(
        '--total-shards',
        type=int,
        help='Total number of shards for distributed processing'
    )
    parser.add_argument(
        '--skip-reports',
        action='store_true',
        help='Skip report generation, only download missing data'
    )
    parser.add_argument(
        '--reports-only',
        action='store_true',
        help='Generate reports only, do not download missing data'
    )
    parser.add_argument(
        '--download-only',
        action='store_true',
        help='Download missing data only, do not generate reports'
    )
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Logging level'
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
                                      data_types: list = None):
    """Generate missing data reports for each day in the date range"""
    
    config = get_config()
    data_validator = DataValidator(config.gcp.bucket)
    
    if data_types is None:
        data_types = ['trades', 'book_snapshot_5']
    
    logger.info("============================================================")
    logger.info("📊 STEP 1: MISSING DATA REPORT GENERATION")
    logger.info("============================================================")
    logger.info(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    logger.info(f"Venues: {venues or 'all'}")
    logger.info(f"Instrument types: {instrument_types or 'all'}")
    logger.info(f"Data types: {data_types}")
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
    logger.info("📤 All missing data reports uploaded to GCS")
    
    return {
        'total_days': processed_days,
        'days_with_missing_data': days_with_missing_data,
        'total_missing_entries': total_missing_entries
    }

async def download_missing_data(start_date: datetime, end_date: datetime,
                              venues: list = None, instrument_types: list = None,
                              data_types: list = None, max_instruments: int = None,
                              shard_index: int = None, total_shards: int = None):
    """Download missing data based on reports in GCS"""
    
    config = get_config()
    download_orchestrator = DownloadOrchestrator(config.gcp.bucket, config.tardis.api_key)
    
    if data_types is None:
        data_types = ['trades', 'book_snapshot_5']
    
    logger.info("============================================================")
    logger.info("📥 STEP 2: MISSING DATA DOWNLOAD")
    logger.info("============================================================")
    logger.info(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    logger.info(f"Venues: {venues or 'all'}")
    logger.info(f"Instrument types: {instrument_types or 'all'}")
    logger.info(f"Data types: {data_types}")
    logger.info(f"Max instruments per day: {max_instruments or 'unlimited'}")
    if shard_index is not None and total_shards is not None:
        logger.info(f"Sharding: shard {shard_index}/{total_shards}")
    logger.info("============================================================")
    
    total_days = (end_date - start_date).days + 1
    processed_days = 0
    total_downloads = 0
    total_failed = 0
    
    current_date = start_date
    while current_date <= end_date:
        processed_days += 1
        date_str = current_date.strftime('%Y-%m-%d')
        
        logger.info(f"📅 Downloading missing data for {date_str} ({processed_days}/{total_days})...")
        
        try:
            download_result = await download_orchestrator.download_missing_data(
                date=current_date,
                venues=venues,
                instrument_types=instrument_types,
                data_types=data_types,
                max_instruments=max_instruments,
                shard_index=shard_index,
                total_shards=total_shards
            )
            
            if download_result['status'] == 'success':
                total_downloads += download_result['processed']
                total_failed += download_result['failed']
                logger.info(f"✅ Downloaded missing data for {date_str}: {download_result['processed']} processed, {download_result['failed']} failed")
            elif download_result['status'] == 'no_missing_data':
                logger.info(f"✅ No missing data for {date_str}")
            else:
                logger.warning(f"⚠️ No targets found for {date_str}")
                
        except Exception as e:
            logger.error(f"❌ Error downloading missing data for {date_str}: {e}")
            total_failed += 1
        
        current_date += timedelta(days=1)
    
    # Final summary
    logger.info("============================================================")
    logger.info("🎉 MISSING DATA DOWNLOAD COMPLETED")
    logger.info("============================================================")
    logger.info(f"📊 Total days processed: {processed_days}")
    logger.info(f"📈 Total downloads: {total_downloads}")
    logger.info(f"❌ Total failed: {total_failed}")
    logger.info(f"📋 Success rate: {((total_downloads / (total_downloads + total_failed)) * 100):.1f}%" if (total_downloads + total_failed) > 0 else "N/A")
    
    return {
        'total_days': processed_days,
        'total_downloads': total_downloads,
        'total_failed': total_failed
    }

async def main():
    """Main entry point for the missing data workflow"""
    args = parse_arguments()
    setup_logging(args.log_level)

    try:
        start_date = parse_date(args.start_date)
        end_date = parse_date(args.end_date)

        if start_date > end_date:
            raise ValueError("Start date must be before or equal to end date")

        # Determine workflow steps
        generate_reports = not args.skip_reports and not args.download_only
        download_missing = not args.reports_only

        logger.info("🚀 MISSING DATA WORKFLOW STARTED")
        logger.info(f"📅 Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        logger.info(f"📊 Generate reports: {generate_reports}")
        logger.info(f"📥 Download missing data: {download_missing}")

        results = {}

        # Step 1: Generate missing data reports
        if generate_reports:
            report_results = await generate_missing_data_reports(
                start_date=start_date,
                end_date=end_date,
                venues=args.venues,
                instrument_types=args.instrument_types,
                data_types=args.data_types
            )
            results['reports'] = report_results

        # Step 2: Download missing data
        if download_missing:
            download_results = await download_missing_data(
                start_date=start_date,
                end_date=end_date,
                venues=args.venues,
                instrument_types=args.instrument_types,
                data_types=args.data_types,
                max_instruments=args.max_instruments,
                shard_index=args.shard_index,
                total_shards=args.total_shards
            )
            results['downloads'] = download_results

        # Final summary
        logger.info("============================================================")
        logger.info("🎉 MISSING DATA WORKFLOW COMPLETED")
        logger.info("============================================================")
        if 'reports' in results:
            logger.info(f"📊 Reports: {results['reports']['total_days']} days, {results['reports']['total_missing_entries']} missing entries")
        if 'downloads' in results:
            logger.info(f"📥 Downloads: {results['downloads']['total_downloads']} successful, {results['downloads']['total_failed']} failed")

    except ValueError as e:
        logger.error(f"❌ Configuration Error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ An unexpected error occurred: {e}", exc_info=args.log_level == 'DEBUG')
        sys.exit(1)

if __name__ == '__main__':
    asyncio.run(main())
