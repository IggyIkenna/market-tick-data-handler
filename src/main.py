#!/usr/bin/env python3
"""
Market Data Tick Handler - Main Entry Point

Centralized entry point for all market data operations including:
- Instrument definition generation and GCS upload (single by_date partition)
- Tick data download and upload (single by_date partition)
- Data validation and orchestration

Uses optimized single partition strategy for maximum efficiency:
- Instrument definitions: by_date/day-{date}/instruments.parquet
- Tick data: by_date/day-{date}/data_type-{type}/{instrument_key}.parquet

Usage:
    python -m market_data_tick_handler.main. --mode instruments --start-date 2023-05-23 --end-date 2023-05-25
    python -m market_data_tick_handler.main. --mode download --start-date 2023-05-23 --end-date 2023-05-25
    python -m market_data_tick_handler.main. --mode full-pipeline-ticks --start-date 2023-05-23 --end-date 2023-05-25
"""

import sys
import os
import argparse
import asyncio
import logging
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config import get_config
from src.instrument_processor.canonical_key_generator import CanonicalInstrumentKeyGenerator
from src.instrument_processor.gcs_uploader import InstrumentGCSUploader
from src.data_downloader.download_orchestrator import DownloadOrchestrator
from src.data_validator.data_validator import DataValidator
# MarketDataOrchestrator removed - using DownloadOrchestrator directly

# Import enhanced utilities
from src.utils.logger import setup_structured_logging, PerformanceLogger, log_operation_start, log_operation_success, log_operation_failure
from src.utils.error_handler import ErrorHandler, ErrorContext, error_handler, ErrorCategory
from src.utils.performance_monitor import performance_monitor, get_performance_monitor, start_performance_monitoring

# Configure structured logging
logger = setup_structured_logging(
    log_level=os.getenv('LOG_LEVEL', 'INFO'),
    console_output=True,
    include_timestamp=True,
    include_level=True,
    gcp_logging=os.getenv('LOG_DESTINATION', 'local') in ['gcp', 'both']
)

class ModeHandler:
    """Base class for different operation modes"""
    
    def __init__(self, config):
        self.config = config
        self.gcs_bucket = config.gcp.bucket
        self.tardis_api_key = config.tardis.api_key
    
    async def run(self, **kwargs):
        """Override in subclasses to implement specific mode logic"""
        raise NotImplementedError

class InstrumentGenerationHandler(ModeHandler):
    """Handles instrument definition generation and GCS upload"""
    
    def __init__(self, config):
        super().__init__(config)
        self.generator = CanonicalInstrumentKeyGenerator(self.tardis_api_key)
        self.gcs_uploader = InstrumentGCSUploader(self.gcs_bucket)
    
    async def run(self, start_date: datetime, end_date: datetime, 
                  exchanges: List[str] = None, max_workers: int = 4, **kwargs):
        """Generate instrument definitions and upload to GCS with multithreading"""
        
        # Calculate total days for progress tracking
        total_days = (end_date - start_date).days + 1
        
        logger.info(f"🎯 Starting instrument generation from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        logger.info(f"📅 Processing {total_days} days total")
        logger.info(f"🏢 Processing {len(exchanges)} exchanges per day")
        logger.info(f"🚀 Using {max_workers} workers for parallel processing")
        
        if exchanges is None:
            # This should not happen as exchanges are now derived from venues
            raise ValueError("Exchanges parameter is required - this should be derived from venues")
        
        results = {
            'total_days': 0,
            'processed_days': 0,
            'total_instruments': 0,
            'total_parsing_failures': 0,
            'errors': [],
            'all_instruments': []
        }
        
        # Process each day
        current_date = start_date
        day_counter = 0
        while current_date <= end_date:
            day_counter += 1
            results['total_days'] += 1
            logger.info(f"📅 Processing day {day_counter}/{total_days}: {current_date.strftime('%Y-%m-%d')}")
            
            try:
                # Process all exchanges in parallel using ThreadPoolExecutor
                daily_instruments = []
                daily_parsing_failures = 0
                
                def process_exchange(exchange):
                    """Process a single exchange - runs in thread"""
                    thread_id = threading.current_thread().ident
                    exchange_idx = exchanges.index(exchange) + 1
                    logger.info(f"  🏢 Processing exchange {exchange_idx}/{len(exchanges)}: {exchange} (thread {thread_id})")
                    
                    try:
                        exchange_data, exchange_stats = self.generator.process_exchange_symbols(
                            exchange, current_date, current_date
                        )
                        
                        if exchange_data:
                            # Convert to DataFrame
                            import pandas as pd
                            df = pd.DataFrame(list(exchange_data.values()))
                            logger.info(f"    ✅ Generated {len(df)} instruments for {exchange}")
                            
                            return {
                                'exchange': exchange,
                                'data': df,
                                'stats': exchange_stats,
                                'success': True
                            }
                        else:
                            logger.warning(f"    ⚠️ No data for {exchange}")
                            return {
                                'exchange': exchange,
                                'data': None,
                                'stats': exchange_stats,
                                'success': False
                            }
                    except Exception as e:
                        logger.error(f"    ❌ Error processing {exchange}: {e}")
                        return {
                            'exchange': exchange,
                            'data': None,
                            'stats': None,
                            'success': False,
                            'error': str(e)
                        }
                
                # Execute exchanges in parallel
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    # Submit all exchange processing tasks
                    future_to_exchange = {
                        executor.submit(process_exchange, exchange): exchange 
                        for exchange in exchanges
                    }
                    
                    # Collect results as they complete
                    for future in as_completed(future_to_exchange):
                        exchange = future_to_exchange[future]
                        try:
                            result = future.result()
                            
                            if result['success'] and result['data'] is not None:
                                daily_instruments.append(result['data'])
                                instruments_count = len(result['data'])
                                total_instruments_today = sum(len(df) for df in daily_instruments)
                                logger.info(f"    📊 {result['exchange']}: {instruments_count} instruments (daily total: {total_instruments_today})")
                                
                                # Track parsing failures
                                if result['stats'] and result['stats'].get('failed_parsing', 0) > 0:
                                    daily_parsing_failures += result['stats']['failed_parsing']
                                    logger.warning(f"    ⚠️  {result['exchange']}: {result['stats']['failed_parsing']} parsing failures")
                            else:
                                if 'error' in result:
                                    results['errors'].append(f"Exchange {exchange}: {result['error']}")
                                    
                        except Exception as e:
                            logger.error(f"❌ Future failed for {exchange}: {e}")
                            results['errors'].append(f"Exchange {exchange}: {e}")
                
                # Combine all exchanges for this day
                if daily_instruments:
                    import pandas as pd
                    # Filter out empty DataFrames and clean all-NA columns to avoid FutureWarning
                    non_empty_instruments = []
                    for df in daily_instruments:
                        if not df.empty:
                            # Drop columns that are all NaN to avoid FutureWarning
                            df_clean = df.dropna(axis=1, how='all')
                            if not df_clean.empty:
                                non_empty_instruments.append(df_clean)
                    
                    if non_empty_instruments:
                        combined_df = pd.concat(non_empty_instruments, ignore_index=True)
                    else:
                        # All DataFrames were empty, create empty DataFrame with expected columns
                        combined_df = pd.DataFrame()
                    results['total_instruments'] += len(combined_df)
                    
                    logger.info(f"  📊 Total instruments for {current_date.strftime('%Y-%m-%d')}: {len(combined_df)}")
                    
                    # Upload to GCS with single partition strategy
                    gcs_path = self.gcs_uploader.upload_instrument_definitions(combined_df, current_date)
                    logger.info(f"  ✅ Uploaded {len(combined_df)} instruments to GCS: {gcs_path}")
                    
                    # Collect for final aggregate
                    daily_instruments_copy = combined_df.copy()
                    daily_instruments_copy['date'] = current_date.strftime('%Y-%m-%d')
                    results['all_instruments'].append(daily_instruments_copy)
                    
                    results['total_parsing_failures'] += daily_parsing_failures
                    results['processed_days'] += 1
                else:
                    logger.warning(f"  ⚠️ No instruments generated for {current_date.strftime('%Y-%m-%d')}")
                    
            except Exception as e:
                error_msg = f'Error processing {current_date.strftime("%Y-%m-%d")}: {e}'
                logger.error(f"  ❌ {error_msg}")
                results['errors'].append(error_msg)
            
            # Move to next day
            from datetime import timedelta
            current_date = current_date + timedelta(days=1)
        
        # Create and upload final aggregate file
        if results['all_instruments']:
            logger.info(f"📦 Creating final aggregate file...")
            try:
                import pandas as pd
                all_instruments_df = pd.concat(results['all_instruments'], ignore_index=True)
                logger.info(f"📊 Total instruments in aggregate: {len(all_instruments_df)}")
                
                aggregate_path = self.gcs_uploader.upload_aggregate_definitions(
                    all_instruments_df, start_date, end_date
                )
                logger.info(f"✅ Final aggregate uploaded: {aggregate_path}")
                
            except Exception as e:
                logger.error(f"❌ Failed to create final aggregate: {e}")
                logger.warning(f"⚠️ Individual daily files are still available in GCS")
                logger.warning(f"⚠️ You can skip the aggregate file and use daily partitions instead")
                results['errors'].append(f"Aggregate creation: {e}")
                # Don't fail the entire operation for aggregate file issues
        
        # Print summary
        logger.info('🎉 INSTRUMENT GENERATION COMPLETED')
        logger.info(f"📊 Total days: {results['total_days']}")
        logger.info(f"✅ Processed days: {results['processed_days']}")
        logger.info(f"📈 Total instruments: {results['total_instruments']}")
        logger.info(f"⚠️ Parsing failures: {results['total_parsing_failures']}")
        logger.info(f"❌ Errors: {len(results['errors'])}")
        
        return results

class TickDataDownloadHandler(ModeHandler):
    """Handles tick data download and upload"""
    
    def __init__(self, config):
        super().__init__(config)
        self.download_orchestrator = DownloadOrchestrator(
            self.gcs_bucket, 
            self.tardis_api_key,
            max_workers=config.tardis.download_max_workers
        )
    
    async def run(self, start_date: datetime, end_date: datetime,
                  venues: List[str] = None, instrument_types: List[str] = None,
                  data_types: List[str] = None, max_instruments: int = None,
                  shard_index: int = None, total_shards: int = None, 
                  force_download: bool = False, **kwargs):
        """Download tick data and upload to GCS"""
        
        # Calculate total days for progress tracking
        total_days = (end_date - start_date).days + 1
        
        if force_download:
            logger.info(f"🚀 Starting FORCE tick data download from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
            logger.info(f"⚠️  FORCE mode: Will download all data regardless of existing files")
        else:
            logger.info(f"📥 Starting tick data download from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
            logger.info(f"🔍 Missing data mode: Will only download missing data")
        
        logger.info(f"📅 Processing {total_days} days total")
        logger.info(f"🎯 Data types: {data_types or ['trades', 'book_snapshot_5', 'derivative_ticker', 'liquidations', 'options_chain']}")
        logger.info(f"🏢 Venues: {venues or 'all'}")
        
        if data_types is None:
            data_types = ['trades', 'book_snapshot_5', 'derivative_ticker', 'liquidations', 'options_chain']
        
        results = {
            'total_days': 0,
            'processed_days': 0,
            'total_downloads': 0,
            'failed_downloads': 0,
            'errors': []
        }
        
        # Process each day
        current_date = start_date
        day_counter = 0
        while current_date <= end_date:
            day_counter += 1
            results['total_days'] += 1
            logger.info(f"📅 Processing day {day_counter}/{total_days}: {current_date.strftime('%Y-%m-%d')}")
            
            try:
                if force_download:
                    # Force download: download all data regardless of existing files
                    logger.info(f"🚀 Force downloading all data for {current_date.strftime('%Y-%m-%d')}")
                    download_result = await self.download_orchestrator.download_and_upload_data(
                        date=current_date,
                        venues=venues,
                        instrument_types=instrument_types,
                        data_types=data_types,
                        max_instruments=max_instruments,
                        shard_index=shard_index,
                        total_shards=total_shards
                    )
                else:
                    # Missing data mode: only download missing data
                    logger.info(f"🔍 Checking for missing data on {current_date.strftime('%Y-%m-%d')}")
                    download_result = await self.download_orchestrator.download_missing_data(
                        date=current_date,
                        venues=venues,
                        instrument_types=instrument_types,
                        data_types=data_types,
                        max_instruments=max_instruments
                    )
                
                results['total_downloads'] += download_result.get('processed', 0)
                results['failed_downloads'] += download_result.get('failed', 0)
                results['processed_days'] += 1
                
                if force_download:
                    logger.info(f"✅ Force downloaded data for {current_date.strftime('%Y-%m-%d')}")
                else:
                    logger.info(f"✅ Downloaded missing data for {current_date.strftime('%Y-%m-%d')}")
                
            except Exception as e:
                error_msg = f'Error downloading data for {current_date.strftime("%Y-%m-%d")}: {e}'
                logger.error(f"❌ {error_msg}")
                results['errors'].append(error_msg)
            
            # Move to next day
            from datetime import timedelta
            current_date = current_date + timedelta(days=1)
        
        # Print summary
        logger.info('🎉 TICK DATA DOWNLOAD COMPLETED')
        logger.info(f"📊 Total days: {results['total_days']}")
        logger.info(f"✅ Processed days: {results['processed_days']}")
        logger.info(f"📈 Total downloads: {results['total_downloads']}")
        logger.info(f"❌ Failed downloads: {results['failed_downloads']}")
        logger.info(f"❌ Errors: {len(results['errors'])}")
        
        return results

class MissingDataDownloadHandler(ModeHandler):
    """Handles downloading only missing data based on missing data reports"""
    
    def __init__(self, config):
        super().__init__(config)
        self.download_orchestrator = DownloadOrchestrator(
            self.gcs_bucket, 
            self.tardis_api_key,
            max_workers=config.tardis.download_max_workers
        )
    
    async def run(self, start_date: datetime, end_date: datetime,
                  venues: List[str] = None, instrument_types: List[str] = None,
                  data_types: List[str] = None, max_instruments: int = None,
                  shard_index: int = None, total_shards: int = None, **kwargs):
        """Download only missing data based on missing data reports from GCS"""
        
        # Calculate total days for progress tracking
        total_days = (end_date - start_date).days + 1
        
        logger.info(f"📥 Starting missing data download from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        logger.info(f"📅 Processing {total_days} days total")
        logger.info(f"🎯 Data types: {data_types or ['trades', 'book_snapshot_5', 'derivative_ticker', 'liquidations', 'options_chain']}")
        logger.info(f"🏢 Venues: {venues or 'all'}")
        
        if data_types is None:
            data_types = ['trades', 'book_snapshot_5', 'derivative_ticker', 'liquidations', 'options_chain']
        
        results = {
            'total_days': 0,
            'processed_days': 0,
            'total_downloads': 0,
            'failed_downloads': 0,
            'errors': []
        }
        
        # Process each day
        current_date = start_date
        day_counter = 0
        while current_date <= end_date:
            day_counter += 1
            results['total_days'] += 1
            logger.info(f"📅 Processing day {day_counter}/{total_days}: {current_date.strftime('%Y-%m-%d')} (missing data)")
            
            try:
                download_result = await self.download_orchestrator.download_missing_data(
                    date=current_date,
                    venues=venues,
                    instrument_types=instrument_types,
                    data_types=data_types,
                    max_instruments=max_instruments,
                    shard_index=kwargs.get('shard_index'),
                    total_shards=kwargs.get('total_shards')
                )
                
                if download_result['status'] == 'success':
                    results['processed_days'] += 1
                    results['total_downloads'] += download_result['processed']
                    results['failed_downloads'] += download_result['failed']
                    logger.info(f"✅ Downloaded missing data for {current_date.strftime('%Y-%m-%d')}: {download_result['processed']} processed, {download_result['failed']} failed")
                elif download_result['status'] == 'no_missing_data':
                    logger.info(f"✅ No missing data for {current_date.strftime('%Y-%m-%d')}")
                    results['processed_days'] += 1
                else:
                    logger.warning(f"⚠️ No targets found for {current_date.strftime('%Y-%m-%d')}")
                    results['processed_days'] += 1
                    
            except Exception as e:
                logger.error(f"❌ Error processing {current_date.strftime('%Y-%m-%d')}: {e}")
                results['errors'].append(f"{current_date.strftime('%Y-%m-%d')}: {str(e)}")
            
            current_date += timedelta(days=1)
        
        logger.info('🎉 MISSING DATA DOWNLOAD COMPLETED')
        logger.info(f"📊 Total days: {results['total_days']}")
        logger.info(f"✅ Processed days: {results['processed_days']}")
        logger.info(f"📈 Total downloads: {results['total_downloads']}")
        logger.info(f"❌ Failed downloads: {results['failed_downloads']}")
        logger.info(f"❌ Errors: {len(results['errors'])}")
        
        return results

class CheckGapsHandler(ModeHandler):
    """Handles checking for file existence gaps in date ranges"""
    
    def __init__(self, config):
        super().__init__(config)
        from google.cloud import storage
        self.client = storage.Client()
        self.bucket = self.client.bucket(self.gcs_bucket)
        self.data_validator = DataValidator(self.gcs_bucket)
    
    async def run(self, start_date: datetime, end_date: datetime,
                  venues: List[str] = None, instrument_types: List[str] = None,
                  data_types: List[str] = None, max_instruments: int = None,
                  shard_index: int = None, total_shards: int = None, **kwargs):
        """Check for missing instrument definitions only - light check for gaps"""
        
        # Calculate total days for progress tracking
        total_days = (end_date - start_date).days + 1
        
        logger.info(f"🔍 Checking for missing instrument definitions from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        logger.info(f"📅 Validating {total_days} days total")
        
        results = {
            'total_days': 0,
            'days_with_instrument_definitions': 0,
            'gaps': [],
            'summary': {}
        }
        
        # Check each day for instrument definitions only
        current_date = start_date
        day_counter = 0
        while current_date <= end_date:
            day_counter += 1
            results['total_days'] += 1
            date_str = current_date.strftime('%Y-%m-%d')
            logger.info(f"📅 Validating day {day_counter}/{total_days}: {date_str}")
            
            # Check for instrument definitions
            instrument_def_path = f"instrument_availability/by_date/day-{date_str}/instruments.parquet"
            instrument_blob = self.bucket.blob(instrument_def_path)
            if instrument_blob.exists():
                instrument_blob.reload()  # Reload to get size
                has_instrument_defs = (instrument_blob.size or 0) > 0
            else:
                has_instrument_defs = False
            
            if has_instrument_defs:
                results['days_with_instrument_definitions'] += 1
                logger.info(f"✅ Instrument definitions exist for {date_str}")
            else:
                # Record gaps - only instrument definitions are critical gaps
                results['gaps'].append({
                    'date': date_str,
                    'type': 'instrument_definitions',
                    'path': instrument_def_path,
                    'size': (instrument_blob.size or 0) if instrument_blob.exists() else 0
                })
                logger.warning(f"❌ Missing instrument definitions for {date_str}")
            
            current_date += timedelta(days=1)
        
        # Calculate summary
        results['summary'] = {
            'instrument_definitions_coverage': (results['days_with_instrument_definitions'] / results['total_days']) * 100,
            'total_gaps': len(results['gaps'])
        }
        
        logger.info('🎉 INSTRUMENT DEFINITIONS GAP CHECK COMPLETED')
        logger.info(f"📊 Total days: {results['total_days']}")
        logger.info(f"📋 Days with instrument definitions: {results['days_with_instrument_definitions']} ({results['summary']['instrument_definitions_coverage']:.1f}%)")
        logger.info(f"❌ Total gaps found: {results['summary']['total_gaps']}")
        
        if results['gaps']:
            logger.info("🔍 Gap details (missing instrument definitions):")
            for gap in results['gaps'][:10]:  # Show first 10 gaps
                logger.info(f"   - {gap['date']}: {gap['type']} (size: {gap['size']} bytes)")
            if len(results['gaps']) > 10:
                logger.info(f"   ... and {len(results['gaps']) - 10} more gaps")
        else:
            logger.info("✅ No gaps found - all days have instrument definitions")
        
        return results

class DataValidationHandler(ModeHandler):
    """Handles data validation and missing data checking"""
    
    def __init__(self, config):
        super().__init__(config)
        self.data_validator = DataValidator(self.gcs_bucket)
    
    async def run(self, start_date: datetime, end_date: datetime,
                  venues: List[str] = None, instrument_types: List[str] = None,
                  data_types: List[str] = None, **kwargs):
        """Validate data completeness and check for missing data"""
        
        # Calculate total days for progress tracking
        total_days = (end_date - start_date).days + 1
        
        logger.info(f"🔍 Starting data validation from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        logger.info(f"📅 Validating {total_days} days total")
        logger.info(f"🎯 Data types: {data_types or ['trades', 'book_snapshot_5', 'derivative_ticker', 'options_chain', 'liquidations']}")
        logger.info(f"🏢 Venues: {venues or 'all'}")
        
        if data_types is None:
            # Check for all available data types based on instrument definitions
            data_types = ['trades', 'book_snapshot_5', 'derivative_ticker', 'options_chain', 'liquidations']
        
        results = {
            'total_days': 0,
            'validation_status': 'unknown',
            'missing_data_count': 0,
            'coverage_percentage': 0.0,
            'missing_data_details': [],
            'summary': {}
        }
        
        try:
            # Generate comprehensive missing data report
            report = self.data_validator.generate_missing_data_report(
                start_date=start_date,
                end_date=end_date,
                venues=venues,
                instrument_types=instrument_types,
                data_types=data_types
            )
            
            results['validation_status'] = report['status']
            results['missing_data_count'] = report['missing_count']
            results['coverage_percentage'] = report['coverage_percentage']
            results['summary'] = report
            
            # Get detailed missing data if any
            if data_types:
                missing_df = self.data_validator.check_missing_data_by_type(
                    start_date, end_date, venues, instrument_types, data_types
                )
            else:
                missing_df = self.data_validator.check_missing_data(
                    start_date, end_date, venues, instrument_types
                )
            
            if not missing_df.empty:
                results['missing_data_details'] = missing_df.to_dict('records')
                
                # Save missing data to CSV file
                csv_filename = f"missing_data_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"
                csv_path = f"data/{csv_filename}"
                missing_df.to_csv(csv_path, index=False)
                logger.info(f"📄 Missing data saved to: {csv_path}")
            
            # Print summary
            logger.info('🎉 DATA VALIDATION COMPLETED')
            logger.info(f"📊 Status: {results['validation_status']}")
            logger.info(f"📈 Coverage: {results['coverage_percentage']:.1f}%")
            logger.info(f"❌ Missing entries: {results['missing_data_count']}")
            
            if results['missing_data_count'] > 0:
                logger.warning(f"⚠️ Found {results['missing_data_count']} missing data entries")
                logger.info("💡 Use --verbose flag to see detailed missing data list")
                if not missing_df.empty:
                    logger.info(f"📄 Detailed missing data saved to: data/{csv_filename}")
            else:
                logger.info("✅ All expected data is available")
            
        except Exception as e:
            logger.error(f"❌ Data validation failed: {e}")
            results['validation_status'] = 'failed'
            results['error'] = str(e)
        
        return results

class MissingReportsHandler(ModeHandler):
    """Handles missing data report generation and upload to GCS"""
    
    def __init__(self, config):
        super().__init__(config)
        self.data_validator = DataValidator(self.gcs_bucket)
    
    async def run(self, start_date: datetime, end_date: datetime,
                  venues: List[str] = None, instrument_types: List[str] = None,
                  data_types: List[str] = None, **kwargs):
        """Generate missing data reports for the date range and upload to GCS"""
        
        # Calculate total days for progress tracking
        total_days = (end_date - start_date).days + 1
        
        logger.info(f"📊 Starting missing data report generation from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        logger.info(f"📅 Processing {total_days} days total")
        logger.info(f"🎯 Data types: {data_types or ['trades', 'book_snapshot_5', 'derivative_ticker', 'options_chain', 'liquidations']}")
        logger.info(f"🏢 Venues: {venues or 'all'}")
        
        if data_types is None:
            data_types = ['trades', 'book_snapshot_5', 'derivative_ticker', 'options_chain', 'liquidations']
        
        results = {
            'total_days': total_days,
            'processed_days': 0,
            'reports_generated': 0,
            'reports_uploaded': 0,
            'total_missing_entries': 0,
            'errors': []
        }
        
        # Process each day
        current_date = start_date
        day_counter = 0
        
        while current_date <= end_date:
            day_counter += 1
            results['processed_days'] = day_counter
            
            logger.info(f"📅 Processing day {day_counter}/{total_days}: {current_date.strftime('%Y-%m-%d')}")
            
            try:
                # Generate missing data report for this day (includes GCS upload)
                report = self.data_validator.generate_missing_data_report(
                    start_date=current_date,
                    end_date=current_date,
                    venues=venues,
                    instrument_types=instrument_types,
                    data_types=data_types,
                    upload_to_gcs=True
                )
                
                # Always count as uploaded (even if empty) to overwrite previous files
                results['reports_uploaded'] += 1
                results['total_missing_entries'] += report['missing_count']
                
                if report['status'] == 'incomplete':
                    logger.info(f"  ✅ Generated missing data report: {report['missing_count']} missing entries")
                else:
                    logger.info(f"  ✅ No missing data for {current_date.strftime('%Y-%m-%d')} (uploaded empty report)")
                
                results['reports_generated'] += 1
                
            except Exception as e:
                error_msg = f"Failed to process {current_date.strftime('%Y-%m-%d')}: {e}"
                logger.error(f"  ❌ {error_msg}")
                results['errors'].append(error_msg)
            
            # Move to next day
            current_date += timedelta(days=1)
        
        # Print summary
        logger.info('🎉 MISSING DATA REPORTS COMPLETED')
        logger.info(f"📊 Total days: {results['total_days']}")
        logger.info(f"✅ Processed days: {results['processed_days']}")
        logger.info(f"📄 Reports generated: {results['reports_generated']}")
        logger.info(f"☁️ Reports uploaded: {results['reports_uploaded']}")
        logger.info(f"📈 Total missing entries: {results['total_missing_entries']}")
        logger.info(f"❌ Errors: {len(results['errors'])}")
        
        return results

class FullPipelineHandler(ModeHandler):
    """Handles the complete pipeline: instruments -> download -> validate"""
    
    def __init__(self, config):
        super().__init__(config)
        self.download_orchestrator = DownloadOrchestrator(
            self.gcs_bucket, 
            self.tardis_api_key,
            max_workers=config.tardis.download_max_workers
        )
    
    async def run(self, start_date: datetime, end_date: datetime,
                  exchanges: List[str] = None, venues: List[str] = None,
                  instrument_types: List[str] = None, data_types: List[str] = None, **kwargs):
        """Run the complete pipeline using DownloadOrchestrator directly"""
        logger.info(f"🚀 Starting full pipeline from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        results = {
            'instrument_generation': {},
            'data_download': {},
            'validation': {},
            'status': 'success'
        }
        
        try:
            # Step 1: Generate instrument definitions
            logger.info("Step 1: Generating instrument definitions")
            instrument_handler = InstrumentGenerationHandler(self.config)
            instrument_results = await instrument_handler.run(
                start_date=start_date,
                end_date=end_date,
                exchanges=exchanges
            )
            results['instrument_generation'] = instrument_results
            
            # Step 2: Download data using DownloadOrchestrator directly
            logger.info("Step 2: Downloading market data")
            download_results = await self._download_data_with_orchestrator(
                start_date=start_date,
                end_date=end_date,
                venues=venues,
                instrument_types=instrument_types,
                data_types=data_types,
                max_instruments=kwargs.get('max_instruments')
            )
            results['data_download'] = download_results
            
            # Step 3: Validate data completeness
            logger.info("Step 3: Validating data completeness")
            validation_handler = DataValidationHandler(self.config)
            validation_results = await validation_handler.run(
                start_date=start_date,
                end_date=end_date,
                venues=venues,
                instrument_types=instrument_types,
                data_types=data_types
            )
            results['validation'] = validation_results
            
            logger.info('🎉 FULL PIPELINE COMPLETED')
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e}")
            results['status'] = 'failed'
            results['error'] = str(e)
        
        return results
    
    async def _download_data_with_orchestrator(self, start_date: datetime, end_date: datetime,
                                             venues: List[str] = None, instrument_types: List[str] = None,
                                             data_types: List[str] = None, max_instruments: int = None,
                                             shard_index: int = None, total_shards: int = None) -> dict:
        """Use DownloadOrchestrator directly for better functionality"""
        logger.info(f"📥 Starting tick data download from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        if data_types is None:
            data_types = ['trades', 'book_snapshot_5', 'derivative_ticker', 'liquidations', 'options_chain']
        
        results = {
            'total_days': 0,
            'processed_days': 0,
            'total_downloads': 0,
            'failed_downloads': 0,
            'errors': []
        }
        
        # Process each day using DownloadOrchestrator directly
        current_date = start_date
        while current_date <= end_date:
            results['total_days'] += 1
            logger.info(f"📅 Processing {current_date.strftime('%Y-%m-%d')}...")
            
            try:
                download_result = await self.download_orchestrator.download_and_upload_data(
                    date=current_date,
                    venues=venues,
                    instrument_types=instrument_types,
                    data_types=data_types,
                    max_instruments=max_instruments,
                    shard_index=shard_index,
                    total_shards=total_shards
                )
                
                results['total_downloads'] += download_result.get('processed', 0)
                results['failed_downloads'] += download_result.get('failed', 0)
                results['processed_days'] += 1
                
                logger.info(f"✅ Downloaded data for {current_date.strftime('%Y-%m-%d')}")
                
            except Exception as e:
                error_msg = f'Error downloading data for {current_date.strftime("%Y-%m-%d")}: {e}'
                logger.error(f"❌ {error_msg}")
                results['errors'].append(error_msg)
            
            # Move to next day
            from datetime import timedelta
            current_date = current_date + timedelta(days=1)
        
        # Print summary
        logger.info('🎉 TICK DATA DOWNLOAD COMPLETED')
        logger.info(f"📊 Total days: {results['total_days']}")
        logger.info(f"✅ Processed days: {results['processed_days']}")
        logger.info(f"📈 Total downloads: {results['total_downloads']}")
        logger.info(f"❌ Failed downloads: {results['failed_downloads']}")
        logger.info(f"❌ Errors: {len(results['errors'])}")
        
        return results

class CandleProcessingHandler(ModeHandler):
    """Handles candle processing mode"""
    
    def __init__(self, config):
        super().__init__(config)
        self.gcs_bucket = config.gcp.bucket
    
    async def run(self, start_date, end_date, venues, instrument_types, data_types, **kwargs):
        """Process historical candles for date range"""
        
        logger.info(f"🕯️ Starting candle processing from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        # Import here to avoid circular imports
        from src.data_client.data_client import DataClient
        from src.candle_processor.historical_candle_processor import HistoricalCandleProcessor, ProcessingConfig
        from src.candle_processor.aggregated_candle_processor import AggregatedCandleProcessor, AggregationConfig
        from src.candle_processor.book_snapshot_processor import BookSnapshotProcessor, BookSnapshotConfig
        
        # Initialize components
        data_client = DataClient(self.gcs_bucket, self.config)
        
        # Configure processors
        processing_config = ProcessingConfig(
            timeframes=['15s', '1m'],
            enable_hft_features=True,
            enable_book_snapshots=True,
            data_types=data_types
        )
        
        aggregation_config = AggregationConfig(
            timeframes=['5m', '15m', '1h', '4h', '24h'],
            enable_hft_features=True
        )
        
        book_config = BookSnapshotConfig(
            timeframes=['15s', '1m', '5m', '15m', '1h', '4h', '24h'],
            levels=5
        )
        
        # Initialize processors
        historical_processor = HistoricalCandleProcessor(data_client, processing_config)
        aggregated_processor = AggregatedCandleProcessor(data_client, aggregation_config)
        book_processor = BookSnapshotProcessor(data_client, book_config)
        
        # Get list of instruments to process
        # This would need to be implemented based on your instrument selection logic
        instruments = await self._get_instruments_to_process(venues, instrument_types, start_date, end_date)
        
        results = {
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
            'instruments_processed': 0,
            'total_candles_generated': 0,
            'errors': []
        }
        
        # Process each day
        current_date = start_date
        while current_date <= end_date:
            logger.info(f"📅 Processing {current_date.strftime('%Y-%m-%d')}")
            
            for instrument_id in instruments:
                try:
                    # Process historical candles (15s, 1m)
                    historical_result = await historical_processor.process_day(
                        instrument_id, current_date, self.gcs_bucket
                    )
                    
                    # Process aggregated candles (5m, 15m, 1h, 4h, 24h)
                    aggregated_result = await aggregated_processor.process_day(
                        instrument_id, current_date, self.gcs_bucket
                    )
                    
                    # Process book snapshots
                    book_result = await book_processor.process_day(
                        instrument_id, current_date, self.gcs_bucket
                    )
                    
                    # Aggregate results
                    results['instruments_processed'] += 1
                    results['total_candles_generated'] += (
                        sum(tf.get('candle_count', 0) for tf in historical_result['timeframes'].values()) +
                        sum(tf.get('candle_count', 0) for tf in aggregated_result['timeframes'].values()) +
                        sum(tf.get('snapshot_count', 0) for tf in book_result['timeframes'].values())
                    )
                    
                    results['errors'].extend(historical_result.get('errors', []))
                    results['errors'].extend(aggregated_result.get('errors', []))
                    results['errors'].extend(book_result.get('errors', []))
                    
                except Exception as e:
                    error_msg = f"Failed to process {instrument_id} on {current_date.strftime('%Y-%m-%d')}: {e}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)
            
            # Move to next day
            from datetime import timedelta
            current_date += timedelta(days=1)
        
        logger.info(f"✅ Candle processing completed: {results['instruments_processed']} instruments, {results['total_candles_generated']} candles generated")
        return results
    
    async def _get_instruments_to_process(self, venues, instrument_types, start_date, end_date):
        """Get list of instruments to process - placeholder implementation"""
        # This would need to be implemented based on your instrument selection logic
        # For now, return a placeholder
        return ['BINANCE:SPOT_PAIR:BTC-USDT', 'BINANCE:SPOT_PAIR:ETH-USDT']

class StreamingTicksHandler(ModeHandler):
    """Handles streaming raw tick data to BigQuery"""
    
    def __init__(self, config):
        super().__init__(config)
    
    async def run(self, symbol: str = 'BTC-USDT', duration: int = 300, **kwargs):
        """Start Node.js tick streaming service"""
        import subprocess
        import os
        
        # Change to streaming directory
        streaming_dir = Path(__file__).parent.parent / 'streaming'
        if not streaming_dir.exists():
            raise FileNotFoundError(f"Streaming directory not found: {streaming_dir}")
        
        # Set environment variables
        env = os.environ.copy()
        env.update({
            'GCP_PROJECT_ID': self.config.gcp.project_id,
            'GOOGLE_APPLICATION_CREDENTIALS': self.config.gcp.credentials_path,
            'BIGQUERY_DATASET': 'market_data_streaming'
        })
        
        # Build command
        cmd = ['node', 'live_tick_streamer.js', '--mode', 'ticks', '--symbol', symbol]
        if duration > 0:
            cmd.extend(['--duration', str(duration)])
        
        logger.info(f"🚀 Starting tick streaming for {symbol}")
        logger.info(f"📡 Command: {' '.join(cmd)}")
        
        try:
            # Run Node.js streaming service
            process = subprocess.Popen(
                cmd,
                cwd=streaming_dir,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True
            )
            
            # Stream output
            for line in process.stdout:
                print(line.rstrip())
            
            process.wait()
            return {'status': 'completed', 'symbol': symbol, 'duration': duration}
            
        except Exception as e:
            logger.error(f"❌ Error running tick streaming: {e}")
            raise

class StreamingCandlesHandler(ModeHandler):
    """Handles streaming real-time candles with HFT features"""
    
    def __init__(self, config):
        super().__init__(config)
    
    async def run(self, symbol: str = 'BTC-USDT', duration: int = 300, **kwargs):
        """Start Node.js candle streaming service"""
        import subprocess
        import os
        
        # Change to streaming directory
        streaming_dir = Path(__file__).parent.parent / 'streaming'
        if not streaming_dir.exists():
            raise FileNotFoundError(f"Streaming directory not found: {streaming_dir}")
        
        # Set environment variables
        env = os.environ.copy()
        env.update({
            'GCP_PROJECT_ID': self.config.gcp.project_id,
            'GOOGLE_APPLICATION_CREDENTIALS': self.config.gcp.credentials_path,
            'BIGQUERY_DATASET': 'market_data_streaming'
        })
        
        # Build command
        cmd = ['node', 'live_tick_streamer.js', '--mode', 'candles', '--symbol', symbol]
        if duration > 0:
            cmd.extend(['--duration', str(duration)])
        
        logger.info(f"🕯️ Starting candle streaming for {symbol}")
        logger.info(f"📡 Command: {' '.join(cmd)}")
        
        try:
            # Run Node.js streaming service
            process = subprocess.Popen(
                cmd,
                cwd=streaming_dir,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True
            )
            
            # Stream output
            for line in process.stdout:
                print(line.rstrip())
            
            process.wait()
            return {'status': 'completed', 'symbol': symbol, 'duration': duration}
            
        except Exception as e:
            logger.error(f"❌ Error running candle streaming: {e}")
            raise

class BigQueryUploadHandler(ModeHandler):
    """Handles BigQuery upload mode"""
    
    def __init__(self, config):
        super().__init__(config)
        self.gcs_bucket = config.gcp.bucket
    
    async def run(self, start_date, end_date, venues, instrument_types, data_types, **kwargs):
        """Upload processed candles to BigQuery"""
        
        logger.info(f"📤 Starting BigQuery upload from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        # Import here to avoid circular imports
        from src.data_client.data_client import DataClient
        from src.bigquery_uploader.candle_uploader import CandleUploader, UploadConfig
        from src.bigquery_uploader.upload_orchestrator import UploadOrchestrator, OrchestrationConfig
        
        # Initialize components
        data_client = DataClient(self.gcs_bucket, self.config)
        
        # Configure upload
        upload_config = UploadConfig(
            project_id=self.config.gcp.project_id,
            dataset_id="market_data_candles",
            timeframes=['15s', '1m', '5m', '15m', '1h', '4h', '24h'],
            batch_size=1000
        )
        
        orchestration_config = OrchestrationConfig(
            max_concurrent_days=5,
            max_concurrent_timeframes=3,
            retry_failed_days=True
        )
        
        # Initialize uploader
        uploader = CandleUploader(data_client, upload_config)
        orchestrator = UploadOrchestrator(data_client, upload_config, orchestration_config)
        
        # Upload data
        result = await orchestrator.upload_date_range(
            start_date=start_date,
            end_date=end_date,
            timeframes=upload_config.timeframes,
            overwrite=False
        )
        
        logger.info(f"✅ BigQuery upload completed: {result['successful_days']}/{result['total_days']} days successful, {result['total_rows_uploaded']} rows uploaded")
        return result


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Market Data Tick Handler - Centralized entry point for all operations',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate instrument definitions
  python -m market_data_tick_handler.main. --mode instruments --start-date 2023-05-23 --end-date 2023-05-25
  
  # Download tick data
  python -m market_data_tick_handler.main. --mode download --start-date 2023-05-23 --end-date 2023-05-25 --venues deribit --data-types trades book_snapshot_5
  
  # Run full pipeline
  python -m market_data_tick_handler.main. --mode full-pipeline-ticks --start-date 2023-05-23 --end-date 2023-05-25
  
  # Use environment file
  python -m market_data_tick_handler.main. --mode instruments --env-file .env.production
        """
    )
    
    # Mode selection
    parser.add_argument(
        '--mode', 
        choices=['instruments', 'missing-tick-reports', 'download', 'validate', 'check-gaps', 'full-pipeline-ticks', 'candle-processing', 'bigquery-upload', 'streaming-ticks', 'streaming-candles'],
        required=True,
        help='Operation mode: instruments (generate definitions), missing-tick-reports (generate missing data reports), download (download missing data by default, use --force to override), validate (check missing data), check-gaps (check file existence gaps), full-pipeline-ticks (complete flow), candle-processing (generate historical candles), bigquery-upload (upload candles to BigQuery), streaming-ticks (stream raw tick data to BigQuery), streaming-candles (stream real-time candles with HFT features)'
    )
    
    # Date range
    parser.add_argument(
        '--start-date',
        type=str,
        help='Start date in YYYY-MM-DD format (required for batch modes, optional for streaming)'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        help='End date in YYYY-MM-DD format (required for batch modes, optional for streaming)'
    )
    
    # Streaming specific options
    parser.add_argument(
        '--symbol',
        type=str,
        help='Trading symbol for streaming modes (e.g., BTC-USDT)'
    )
    parser.add_argument(
        '--duration',
        type=int,
        help='Duration in seconds for streaming (0 = infinite)'
    )
    
    # Configuration
    parser.add_argument(
        '--env-file',
        type=str,
        help='Path to environment file (.env)'
    )
    parser.add_argument(
        '--config-file',
        type=str,
        help='Path to configuration file (YAML)'
    )
    
    # Filtering options
    parser.add_argument(
        '--exchanges',
        nargs='+',
        help='Exchanges to process (for instruments mode)'
    )
    parser.add_argument(
        '--venues',
        nargs='+',
        help='Venues to process (for download mode)'
    )
    parser.add_argument(
        '--instrument-types',
        nargs='+',
        help='Instrument types to process'
    )
    parser.add_argument(
        '--data-types',
        nargs='+',
        help='Data types to download (e.g., trades book_snapshot_5)'
    )
    parser.add_argument(
        '--max-instruments',
        type=int,
        help='Maximum number of instruments to process'
    )
    parser.add_argument(
        '--force-download',
        action='store_true',
        help='Force download even if data already exists (overrides missing data check)'
    )
    parser.add_argument(
        '--max-workers',
        type=int,
        default=4,
        help='Maximum number of worker threads for parallel processing (default: 4)'
    )
    
    
    # Sharding options
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
    
    # Logging
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Logging level'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    return parser.parse_args()

def setup_logging(log_level: str, verbose: bool = False):
    """Setup logging configuration (legacy function - now using structured logging)"""
    if verbose:
        log_level = 'DEBUG'
    
    logging.getLogger().setLevel(getattr(logging, log_level.upper()))
    
    # Set specific loggers
    logging.getLogger('google.cloud').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)

def load_environment_file(env_file: str):
    """Load environment variables from file"""
    if env_file and os.path.exists(env_file):
        from dotenv import load_dotenv
        load_dotenv(env_file)
        logger.info(f"Loaded environment variables from {env_file}")
    else:
        logger.info("Using system environment variables")

def parse_date(date_str: str) -> datetime:
    """Parse date string to timezone-aware datetime"""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    except ValueError as e:
        raise ValueError(f"Invalid date format '{date_str}'. Use YYYY-MM-DD format.") from e

async def main():
    """Main entry point with enhanced monitoring and error handling"""
    error_handler = ErrorHandler(logger)
    context = ErrorContext(operation="main", component="main.py")
    
    try:
        # Start performance monitoring
        start_performance_monitoring(interval=60)
        
        # Parse arguments
        args = parse_arguments()
        
        # Setup logging (already configured above with structured logging)
        # setup_logging(args.log_level, args.verbose)  # Replaced with structured logging
        
        # Load environment file if specified
        if args.env_file:
            load_environment_file(args.env_file)
        
        # Load configuration
        config = get_config()
        
        # Parse dates (required for batch modes, optional for streaming)
        if args.mode not in ['streaming-ticks', 'streaming-candles']:
            if not args.start_date:
                raise ValueError("--start-date is required for this mode")
            if not args.end_date:
                raise ValueError("--end-date is required for this mode")
            
        # Parse dates for batch modes
        if args.mode not in ['streaming-ticks', 'streaming-candles']:
            start_date = parse_date(args.start_date)
            end_date = parse_date(args.end_date)
            
            if start_date > end_date:
                raise ValueError("Start date must be before or equal to end date")
        else:
            # For streaming modes, dates are not used
            start_date = None
            end_date = None
        
        # Set defaults for venues and data types if not provided
        # Use canonical venue names as single source of truth
        default_venues = ['BINANCE', 'BINANCE-FUTURES', 'DERIBIT', 'BYBIT', 'BYBIT-SPOT', 'OKX', 'OKX-FUTURES', 'OKX-SWAP']
        default_data_types = ['trades', 'book_snapshot_5', 'derivative_ticker', 'liquidations', 'options_chain']
        
        venues = args.venues if args.venues else default_venues
        data_types = args.data_types if args.data_types else default_data_types
        
        # Convert canonical venues to Tardis exchange names for instrument generation
        venue_to_exchange_mapping = {
            'BINANCE': 'binance',
            'BINANCE-FUTURES': 'binance-futures',
            'DERIBIT': 'deribit',
            'BYBIT': 'bybit',
            'BYBIT-SPOT': 'bybit-spot',
            'OKX': 'okex',
            'OKX-FUTURES': 'okex-futures',
            'OKX-SWAP': 'okex-swap'
        }
        exchanges = [venue_to_exchange_mapping.get(venue, venue.lower()) for venue in venues]
        
        logger.info(f"🏢 Using venues: {venues}")
        logger.info(f"📊 Using data types: {data_types}")
        if args.mode == 'instruments':
            logger.info(f"🔄 Using exchanges: {exchanges}")
        
        # Create appropriate handler based on mode
        if args.mode == 'instruments':
            handler = InstrumentGenerationHandler(config)
            result = await handler.run(
                start_date=start_date,
                end_date=end_date,
                exchanges=exchanges,
                max_workers=args.max_workers
            )
        elif args.mode == 'download':
            # Download mode with missing data checking (default) or force download
            handler = TickDataDownloadHandler(config)
            result = await handler.run(
                start_date=start_date,
                end_date=end_date,
                venues=venues,
                instrument_types=args.instrument_types,
                data_types=data_types,
                max_instruments=args.max_instruments,
                shard_index=args.shard_index,
                total_shards=args.total_shards,
                force_download=args.force_download
            )
        elif args.mode == 'validate':
            handler = DataValidationHandler(config)
            result = await handler.run(
                start_date=start_date,
                end_date=end_date,
                venues=venues,
                instrument_types=args.instrument_types,
                data_types=data_types
            )
        elif args.mode == 'missing-tick-reports':
            handler = MissingReportsHandler(config)
            result = await handler.run(
                start_date=start_date,
                end_date=end_date,
                venues=venues,
                instrument_types=args.instrument_types,
                data_types=data_types
            )
        elif args.mode == 'check-gaps':
            handler = CheckGapsHandler(config)
            result = await handler.run(
                start_date=start_date,
                end_date=end_date,
                venues=venues,
                instrument_types=args.instrument_types,
                data_types=data_types
            )
        elif args.mode == 'full-pipeline-ticks':
            handler = FullPipelineHandler(config)
            result = await handler.run(
                start_date=start_date,
                end_date=end_date,
                exchanges=args.exchanges,
                venues=venues,
                instrument_types=args.instrument_types,
                data_types=data_types
            )
        elif args.mode == 'candle-processing':
            handler = CandleProcessingHandler(config)
            result = await handler.run(
                start_date=start_date,
                end_date=end_date,
                venues=venues,
                instrument_types=args.instrument_types,
                data_types=data_types
            )
        elif args.mode == 'bigquery-upload':
            handler = BigQueryUploadHandler(config)
            result = await handler.run(
                start_date=start_date,
                end_date=end_date,
                venues=venues,
                instrument_types=args.instrument_types,
                data_types=data_types
            )
        elif args.mode == 'streaming-ticks':
            handler = StreamingTicksHandler(config)
            symbol = args.symbol or 'BTC-USDT'
            duration = args.duration or 300
            result = await handler.run(symbol=symbol, duration=duration)
        elif args.mode == 'streaming-candles':
            handler = StreamingCandlesHandler(config)
            symbol = args.symbol or 'BTC-USDT'
            duration = args.duration or 300
            result = await handler.run(symbol=symbol, duration=duration)
        else:
            raise ValueError(f"Unknown mode: {args.mode}")
        
        # Print final result
        logger.info("✅ Operation completed successfully")
        
        # Export performance metrics
        performance_monitor = get_performance_monitor()
        performance_monitor.export_metrics(f"performance_metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        
        # Print performance summary
        perf_summary = get_performance_monitor().get_operation_stats()
        if perf_summary:
            logger.info("📊 Performance Summary:")
            for operation, stats in perf_summary.items():
                if stats['count'] > 0:
                    avg_duration = stats['total_duration'] / stats['count']
                    logger.info(f"  {operation}: {stats['count']} calls, avg {avg_duration:.3f}s")
        
        return result
        
    except Exception as e:
        enhanced_error = error_handler.handle_error(e, context)
        logger.error(f"❌ Fatal error: {enhanced_error.message}")
        logger.error(f"🔍 Error category: {enhanced_error.category.value}")
        logger.error(f"⚠️ Severity: {enhanced_error.severity.value}")
        logger.error(f"🔄 Recovery strategy: {enhanced_error.recovery_strategy.value}")
        
        # Export error summary
        error_summary = error_handler.get_error_summary()
        logger.error(f"📊 Error summary: {error_summary}")
        
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    asyncio.run(main())
