"""
Download Orchestrator Module

Function 3: Download Tardis CSV.gz data locally then push to GCS 
following UNIVERSAL_PARTITIONING_STRATEGY.md format.
"""

import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
from google.cloud import storage
import tempfile
import time
from typing import List, Dict, Any, Optional
import os
import gc  # For garbage collection

from .instrument_reader import InstrumentReader
from .tardis_connector import TardisConnector
from ..utils.memory_monitor import get_memory_monitor, log_memory_status

logger = logging.getLogger(__name__)

class DownloadOrchestrator:
    """Orchestrates downloading Tardis data and uploading to GCS"""
    
    def __init__(self, gcs_bucket: str, api_key: str, max_parallel_downloads: int = None, max_parallel_uploads: int = None, max_workers: int = None):
        self.gcs_bucket = gcs_bucket
        self.api_key = api_key
        self.instrument_reader = InstrumentReader(gcs_bucket)
        self.tardis_connector = TardisConnector(api_key=api_key)
        # Use shared GCS client for connection reuse and optimal performance
        from ..utils.gcs_client import get_shared_gcs_client, get_shared_gcs_bucket
        self.gcs_client = get_shared_gcs_client()
        self.bucket = get_shared_gcs_bucket(gcs_bucket)
        
        # Performance optimization settings
        from ..config import get_config
        config = get_config()
        self.max_parallel_downloads = max_parallel_downloads or config.tardis.max_concurrent
        self.max_parallel_uploads = max_parallel_uploads or config.tardis.max_parallel_uploads
        self.max_workers = max_workers or config.tardis.download_max_workers
        self.batch_size = config.service.batch_size  # Use BATCH_SIZE environment variable
        
        # Memory monitoring and batching
        self.memory_monitor = get_memory_monitor(threshold_percent=75.0)  # Lower threshold for proactive cleanup
        self.upload_batch = []  # Accumulate upload tasks
        self.upload_batch_size = 0
        self.performance_metrics = {
            'total_download_time': 0.0,
            'total_upload_time': 0.0,
            'total_files_processed': 0,
            'total_data_size_mb': 0.0,
            'batch_times': [],
            'memory_usage_samples': []
        }
    
    
    async def download_and_upload_data(self, date: datetime, 
                                     venues: list = None,
                                     instrument_types: list = None,
                                     data_types: list = None,
                                     max_instruments: int = None,
                                     start_date: datetime = None,
                                     end_date: datetime = None,
                                     shard_index: int = None,
                                     total_shards: int = None) -> dict:
        """
        Download Tardis data for instruments and upload to GCS
        
        Args:
            date: Date to download data for
            venues: List of venues to include
            instrument_types: List of instrument types to include
            data_types: List of data types to download (e.g., ['trades', 'book_snapshot_5'])
            max_instruments: Maximum number of instruments to process
            start_date: Start date for instrument availability file (defaults to 2023-05-23)
            end_date: End date for instrument availability file (defaults to 2023-05-23)
            shard_index: Shard index for distributed processing (0-based)
            total_shards: Total number of shards for distributed processing
            
        Returns:
            dict: Download results summary
        """
        logger.info(f"Starting download and upload for {date.strftime('%Y-%m-%d')}")
        
        # Get download targets
        targets = self.instrument_reader.get_download_targets(
            date=date,
            venues=venues,
            instrument_types=instrument_types,
            max_instruments=max_instruments,
            start_date=start_date,
            end_date=end_date
        )
        
        # Apply sharding if specified
        if shard_index is not None and total_shards is not None:
            targets = self._apply_sharding(targets, shard_index, total_shards)
            logger.info(f"Applied sharding: shard {shard_index}/{total_shards}, processing {len(targets)} instruments")
        
        if not targets:
            logger.warning("No download targets found")
            return {'status': 'no_targets', 'processed': 0}
        
        logger.info(f"Processing {len(targets)} instruments with {self.max_workers} workers and {self.max_parallel_downloads} parallel downloads")
        
        # Performance tracking
        start_time = time.time()
        results = {
            'status': 'success',
            'processed': 0,
            'failed': 0,
            'uploaded_files': [],
            'total_instruments': len(targets),
            'start_time': start_time
        }
        
        try:
            # Process instruments in batches to manage memory
            for batch_start in range(0, len(targets), self.batch_size):
                batch_end = min(batch_start + self.batch_size, len(targets))
                batch_targets = targets[batch_start:batch_end]
                
                batch_num = batch_start//self.batch_size + 1
                total_batches = (len(targets) + self.batch_size - 1) // self.batch_size
                logger.info(f"📦 Processing batch {batch_num}/{total_batches}: instruments {batch_start+1}-{batch_end} of {len(targets)} ({(batch_num-1)/total_batches*100:.1f}% complete)")
                
                # Process batch with parallel downloads
                batch_results = await self._process_batch_parallel(batch_targets, date, data_types)
                
                # Update results
                results['processed'] += batch_results['processed']
                results['failed'] += batch_results['failed']
                results['uploaded_files'].extend(batch_results['uploaded_files'])
                
                # Log progress
                elapsed = time.time() - start_time
                rate = results['processed'] / elapsed if elapsed > 0 else 0
                eta = (len(targets) - results['processed'] - results['failed']) / rate if rate > 0 else 0
                logger.info(f"🚀 Progress: {results['processed'] + results['failed']}/{len(targets)} processed ({(results['processed'] + results['failed'])/len(targets)*100:.1f}%), {rate:.1f} files/sec, ETA: {eta/60:.1f} min")
                
                # Force garbage collection after each batch to prevent memory leaks
                gc.collect()
                log_memory_status(f"after_batch_{batch_num}")
                
        finally:
            # Always close the connector, even if an error occurs
            await self.tardis_connector.close()
            
            # Final cleanup
            self.upload_batch.clear()
            self.upload_batch_size = 0
            gc.collect()
        
        # Log performance summary
        self.log_performance_summary()
        
        logger.info(f"Download and upload completed: {results['processed']} processed, {results['failed']} failed")
        return results
    
    async def download_missing_data(self, date: datetime, 
                                  venues: list = None,
                                  instrument_types: list = None,
                                  data_types: list = None,
                                  max_instruments: int = None,
                                  shard_index: int = None,
                                  total_shards: int = None) -> dict:
        """
        Download only missing data based on missing data reports from GCS
        
        Args:
            date: Date to download missing data for
            venues: List of venues to include
            instrument_types: List of instrument types to include
            data_types: List of data types to download
            max_instruments: Maximum number of instruments to process
            shard_index: Shard index for distributed processing
            total_shards: Total number of shards for distributed processing
            
        Returns:
            dict: Download results summary
        """
        logger.info(f"Starting missing data download for {date.strftime('%Y-%m-%d')}")
        
        # Import DataValidator to read missing data reports
        from market_data_tick_handler.data_validator.data_validator import DataValidator
        data_validator = DataValidator(self.gcs_bucket)
        
        # Get missing data report from GCS
        missing_df = data_validator.get_missing_data_from_gcs(date)
        
        if missing_df.empty:
            logger.info(f"No missing data found for {date.strftime('%Y-%m-%d')}")
            return {'status': 'no_missing_data', 'processed': 0}
        
        # Convert missing data to download targets
        targets = self._convert_missing_data_to_targets(missing_df, venues, instrument_types, data_types)
        
        if not targets:
            logger.info("No download targets found after filtering")
            return {'status': 'no_targets', 'processed': 0}
        
        # Apply sharding if specified
        if shard_index is not None and total_shards is not None:
            targets = self._apply_sharding(targets, shard_index, total_shards)
            logger.info(f"Applied sharding: shard {shard_index}/{total_shards}, processing {len(targets)} missing instruments")
        
        # Apply max_instruments limit
        if max_instruments and len(targets) > max_instruments:
            targets = targets[:max_instruments]
            logger.info(f"Limited to {max_instruments} instruments")
        
        logger.info(f"🎯 Processing {len(targets)} missing instruments with {self.max_workers} workers and {self.max_parallel_downloads} parallel downloads")
        logger.info(f"⚙️  Configuration: Batch size={self.batch_size}, Memory threshold=85%, Upload parallelism={self.max_parallel_uploads}")
        
        # Performance tracking
        start_time = time.time()
        results = {
            'status': 'success',
            'processed': 0,
            'failed': 0,
            'uploaded_files': [],
            'total_missing_instruments': len(targets),
            'start_time': start_time
        }
        
        try:
            # Process instruments in batches to manage memory
            for batch_start in range(0, len(targets), self.batch_size):
                batch_end = min(batch_start + self.batch_size, len(targets))
                batch_targets = targets[batch_start:batch_end]
                
                batch_num = batch_start//self.batch_size + 1
                total_batches = (len(targets) + self.batch_size - 1) // self.batch_size
                logger.info(f"📦 Processing batch {batch_num}/{total_batches}: instruments {batch_start+1}-{batch_end} of {len(targets)} ({(batch_num-1)/total_batches*100:.1f}% complete)")
                
                # Process batch with parallel downloads
                batch_results = await self._process_batch_parallel(batch_targets, date, data_types)
                
                # Update results
                results['processed'] += batch_results['processed']
                results['failed'] += batch_results['failed']
                results['uploaded_files'].extend(batch_results['uploaded_files'])
                
                # Log progress
                elapsed = time.time() - start_time
                rate = results['processed'] / elapsed if elapsed > 0 else 0
                eta = (len(targets) - results['processed'] - results['failed']) / rate if rate > 0 else 0
                logger.info(f"🚀 Progress: {results['processed'] + results['failed']}/{len(targets)} processed ({(results['processed'] + results['failed'])/len(targets)*100:.1f}%), {rate:.1f} files/sec, ETA: {eta/60:.1f} min")
                
                # Force garbage collection after each batch to prevent memory leaks
                gc.collect()
                log_memory_status(f"after_missing_batch_{batch_num}")
                
        finally:
            # Always close the connector, even if an error occurs
            await self.tardis_connector.close()
            
            # Final cleanup
            self.upload_batch.clear()
            self.upload_batch_size = 0
            gc.collect()
        
        # Log performance summary
        self.log_performance_summary()
        
        logger.info(f"Missing data download completed: {results['processed']} processed, {results['failed']} failed")
        return results
    
    def _convert_missing_data_to_targets(self, missing_df: pd.DataFrame, venues: list = None, 
                                       instrument_types: list = None, data_types: list = None) -> List[Dict]:
        """Convert missing data DataFrame to download targets using instrument definitions"""
        targets = []
        
        # Filter by data types if specified (this can be done before loading instrument definitions)
        if data_types and 'data_type' in missing_df.columns:
            missing_df = missing_df[missing_df['data_type'].isin(data_types)]
        
        # Get instrument definitions to get the correct tardis_exchange and tardis_symbol
        from market_data_tick_handler.data_downloader.instrument_reader import InstrumentReader
        instrument_reader = InstrumentReader(self.gcs_bucket)
        
        # Get unique instrument keys from missing data
        unique_instrument_keys = missing_df['instrument_key'].unique()
        
        # Load instrument definitions for the date (we'll use the first missing data date)
        if not missing_df.empty and 'date' in missing_df.columns:
            # Get the date from the missing data
            date_str = missing_df['date'].iloc[0]
            date = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        elif not missing_df.empty:
            # Missing data DataFrame doesn't have 'date' column - log available columns for debugging
            available_columns = list(missing_df.columns)
            logger.error(f"Missing data DataFrame missing 'date' column. Available columns: {available_columns}")
            logger.error(f"Missing data DataFrame shape: {missing_df.shape}")
            logger.error(f"First few rows: {missing_df.head().to_dict()}")
            return []
        else:
            # Empty missing data DataFrame
            logger.warning("Missing data DataFrame is empty")
            return []
        
        # Load instrument definitions for the specific date
        instruments_df = instrument_reader.get_instruments_for_date(date, start_date=date, end_date=date)
        
        if not instruments_df.empty:
            # Create a mapping from instrument_key to tardis_exchange, tardis_symbol, and data_types
            instrument_mapping = instruments_df.set_index('instrument_key')[['tardis_exchange', 'tardis_symbol', 'data_types']].to_dict('index')
            
            # Create venue-to-exchange mapping for proper filtering
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
            
            # Track statistics for better logging
            found_count = 0
            expired_count = 0
            missing_count = 0
            venue_filtered_count = 0
            instrument_type_filtered_count = 0
            
            # Convert to download targets using the instrument definitions
            for _, row in missing_df.iterrows():
                instrument_key = row['instrument_key']
                
                if instrument_key in instrument_mapping:
                    mapping = instrument_mapping[instrument_key]
                    
                    # Apply venue filtering using the instrument definitions
                    if venues:
                        # Convert venue names to exchange names for comparison
                        venue_exchanges = [venue_to_exchange_mapping.get(v, v.lower()) for v in venues]
                        if mapping['tardis_exchange'] not in venue_exchanges:
                            venue_filtered_count += 1
                            continue
                    
                    # Apply instrument type filtering
                    if instrument_types:
                        # Check if any of the specified instrument types are in the instrument key
                        instrument_type_match = any(itype in instrument_key for itype in instrument_types)
                        if not instrument_type_match:
                            instrument_type_filtered_count += 1
                            continue
                    
                    targets.append({
                        'instrument_key': instrument_key,
                        'tardis_exchange': mapping['tardis_exchange'],
                        'tardis_symbol': mapping['tardis_symbol'],
                        'data_types': mapping['data_types'],
                        'data_type': row.get('data_type', 'trades')
                    })
                    found_count += 1
                else:
                    # Check if it's an expired instrument (common reason for missing definitions)
                    is_expired = False
                    if 'OPTION' in instrument_key or 'FUTURE' in instrument_key:
                        # Extract date from option/future key (e.g., "230605" means 2023-06-05)
                        parts = instrument_key.split('-')
                        current_date_str = date.strftime('%y%m%d')  # Convert 2023-06-02 to 230602
                        for part in parts:
                            if len(part) == 6 and part.isdigit():
                                # Check if expiry is before current date (options expire at 8am UTC)
                                # So on expiry date itself, we still have data before 8am
                                if part < current_date_str:
                                    is_expired = True
                                    break
                    
                    if is_expired:
                        logger.debug(f"Skipping expired instrument (not in current definitions): {instrument_key}")
                        expired_count += 1
                    else:
                        logger.warning(f"Could not find instrument definition for {instrument_key}")
                        missing_count += 1
            
            # Log summary statistics
            logger.info(f"📊 Missing data conversion summary:")
            logger.info(f"  ✅ Found definitions: {found_count}")
            logger.info(f"  🏢 Filtered by venue: {venue_filtered_count}")
            logger.info(f"  📋 Filtered by instrument type: {instrument_type_filtered_count}")
            logger.info(f"  🕐 Expired instruments: {expired_count}")
            logger.info(f"  ❌ Missing definitions: {missing_count}")
            total_processed = found_count + venue_filtered_count + instrument_type_filtered_count + expired_count + missing_count
            logger.info(f"  📈 Success rate: {found_count/total_processed*100:.1f}%" if total_processed > 0 else "  📈 Success rate: N/A")
        else:
            logger.error("Could not load instrument definitions")
        
        return targets
    
    async def _process_batch_parallel(self, batch_targets: List[Dict], date: datetime, data_types: List[str]) -> Dict[str, Any]:
        """Process a batch of instruments with parallel downloads and batched uploads"""
        batch_start_time = time.time()
        logger.info(f"🚀 Processing batch with {self.max_workers} workers, {len(batch_targets)} instruments")
        
        # Use max_workers for semaphore (controls concurrent downloads)
        semaphore = asyncio.Semaphore(self.max_workers)
        
        async def process_single_instrument(target: Dict) -> Dict[str, Any]:
            async with semaphore:
                download_start = time.time()
                try:
                    # Validate data types against instrument definition
                    requested_data_types = data_types or ['trades', 'book_snapshot_5', 'derivative_ticker', 'liquidations', 'options_chain']
                    instrument_data_types = target.get('data_types', '').split(',') if target.get('data_types') else []
                    
                    # Filter to only valid data types for this instrument
                    valid_data_types = []
                    for data_type in requested_data_types:
                        if data_type in instrument_data_types:
                            valid_data_types.append(data_type)
                        else:
                            logger.info(f"Skipping {data_type} for {target['instrument_key']} - not available in instrument definition (available: {instrument_data_types})")
                    
                    if not valid_data_types:
                        logger.warning(f"No valid data types for {target['instrument_key']}, skipping download")
                        return {
                            'success': False,
                            'error': 'No valid data types',
                            'instrument_key': target['instrument_key']
                        }
                    
                    # Download data
                    download_results = await self.tardis_connector.download_daily_data_direct(
                        tardis_exchange=target['tardis_exchange'],
                        tardis_symbol=target['tardis_symbol'],
                        date=date,
                        data_types=valid_data_types
                    )
                    
                    download_time = time.time() - download_start
                    self.performance_metrics['total_download_time'] += download_time
                    
                    # Upload immediately for single instruments or small batches
                    if self.batch_size <= 1:
                        # Upload immediately for single instruments
                        uploaded_files = await self._upload_to_gcs(download_results, target, date)
                        logger.info(f"📤 Uploaded {len(uploaded_files)} files immediately for {target['instrument_key']}")
                    else:
                        # Add to upload batch for larger batches
                        upload_task = {
                            'download_results': download_results,
                            'target': target,
                            'date': date,
                            'download_time': download_time
                        }
                        
                        # Add to batch and check if we should upload
                        await self._add_to_upload_batch(upload_task)
                    
                    return {
                        'success': True,
                        'instrument_key': target['instrument_key'],
                        'download_time': download_time,
                        'uploaded_files': uploaded_files if self.batch_size <= 1 else []
                    }
                    
                except Exception as e:
                    logger.error(f"❌ Failed to process {target['instrument_key']}: {e}")
                    return {
                        'success': False,
                        'instrument_key': target['instrument_key'],
                        'error': str(e)
                    }
        
        # Process all instruments in the batch concurrently
        tasks = [process_single_instrument(target) for target in batch_targets]
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Log individual instrument progress
        processed = 0
        failed = 0
        uploaded_files = []
        
        for i, result in enumerate(batch_results):
            instrument_key = batch_targets[i]['instrument_key']
            
            if isinstance(result, Exception):
                failed += 1
                logger.error(f"  ❌ {i+1}/{len(batch_targets)}: {instrument_key} - {result}")
            elif result and result.get('success', False):
                processed += 1
                logger.info(f"  ✅ {i+1}/{len(batch_targets)}: {instrument_key}")
                if result.get('uploaded_files'):
                    uploaded_files.extend(result['uploaded_files'])
            else:
                failed += 1
                logger.warning(f"  ⚠️  {i+1}/{len(batch_targets)}: {instrument_key} - {result.get('error', 'Unknown error') if result else 'No result'}")
        
        # Upload any remaining files in the batch
        await self._flush_upload_batch()
        
        # Results already aggregated above, no need for additional counting
        
        batch_time = time.time() - batch_start_time
        self.performance_metrics['batch_times'].append(batch_time)
        
        # Log memory status
        log_memory_status(f"batch_complete_{len(batch_targets)}_instruments")
        
        # Clear any remaining references to free memory
        del batch_results
        gc.collect()
        
        logger.info(f"✅ Batch completed: {processed} processed, {failed} failed, {batch_time:.2f}s")
        
        return {
            'processed': processed,
            'failed': failed,
            'uploaded_files': uploaded_files,
            'batch_time': batch_time
        }
    
    def _apply_sharding(self, targets: List[Dict], shard_index: int, total_shards: int) -> List[Dict]:
        """Apply sharding to distribute instruments across multiple VMs"""
        if shard_index < 0 or shard_index >= total_shards:
            raise ValueError(f"Shard index {shard_index} must be between 0 and {total_shards-1}")
        
        sharded_targets = []
        for target in targets:
            # Use hash of instrument_key for consistent sharding
            instrument_key = target['instrument_key']
            hash_value = hash(instrument_key)
            target_shard = abs(hash_value) % total_shards
            
            if target_shard == shard_index:
                sharded_targets.append(target)
        
        return sharded_targets
    
    async def _add_to_upload_batch(self, upload_task: Dict[str, Any]):
        """Add upload task to batch and check if we should upload"""
        self.upload_batch.append(upload_task)
        self.upload_batch_size += 1
        
        # Check if we should upload the batch
        should_upload = (
            self.upload_batch_size >= self.batch_size or  # Batch size reached
            self.memory_monitor.is_memory_threshold_exceeded()  # Memory threshold exceeded
        )
        
        if should_upload:
            await self._flush_upload_batch()
    
    async def _flush_upload_batch(self):
        """Upload all files in the current batch to GCS"""
        if not self.upload_batch:
            return
        
        upload_start_time = time.time()
        logger.info(f"📤 Uploading batch of {len(self.upload_batch)} instruments to GCS")
        
        # Create upload tasks for parallel execution
        upload_tasks = []
        for upload_task in self.upload_batch:
            task = self._upload_single_to_gcs(
                upload_task['download_results'],
                upload_task['target'],
                upload_task['date']
            )
            upload_tasks.append(task)
        
        # Execute all uploads in parallel with connection pooling and timeout
        try:
            upload_results = await asyncio.wait_for(
                asyncio.gather(*upload_tasks, return_exceptions=True),
                timeout=300  # 5 minute timeout for batch upload
            )
        except asyncio.TimeoutError:
            logger.error("❌ Batch upload timed out after 5 minutes")
            # Cancel remaining tasks
            for task in upload_tasks:
                if not task.done():
                    task.cancel()
            upload_results = [Exception("Upload timeout")] * len(upload_tasks)
        
        # Process results
        successful_uploads = 0
        failed_uploads = 0
        total_files = 0
        
        for i, result in enumerate(upload_results):
            if isinstance(result, Exception):
                failed_uploads += 1
                logger.error(f"❌ Upload failed for {self.upload_batch[i]['target']['instrument_key']}: {result}")
            else:
                successful_uploads += 1
                total_files += len(result)
        
        upload_time = time.time() - upload_start_time
        self.performance_metrics['total_upload_time'] += upload_time
        self.performance_metrics['total_files_processed'] += total_files
        
        logger.info(f"✅ Batch upload completed: {successful_uploads} successful, {failed_uploads} failed, "
                   f"{total_files} files, {upload_time:.2f}s")
        
        # Clear the batch and force garbage collection
        self.upload_batch.clear()
        self.upload_batch_size = 0
        gc.collect()
        
        # Log memory status after upload
        log_memory_status("after_batch_upload")
    
    async def _upload_single_to_gcs(self, download_results: dict, target: dict, date: datetime) -> list:
        """Upload a single instrument's data to GCS (used in batched uploads)"""
        uploaded_files = []
        
        for data_type, df in download_results.items():
            # Skip if df is None (indicating download failure) or empty list (no data available)
            if df is None:
                # Download failed - this should be tracked as an error
                raise Exception(f"Download failed for {data_type}")
            elif isinstance(df, list) and len(df) == 0:
                # No data available - skip upload but don't treat as error
                continue
            
            # No need to add metadata columns - they're available from:
            # - data_type: in GCS path structure
            # - date: in timestamp columns
            # - venue: extractable from instrument_key
            # - instrument_key: filename
            
            # Log if this is an empty file
            if df.empty:
                logger.info(f"📭 Uploading empty {data_type} file with schema for {target['instrument_key']}")
            
            # Create single partition path (optimized strategy)
            gcs_path = self._create_gcs_path(target, date, data_type)
            
            try:
                # Upload to GCS
                blob = self.bucket.blob(gcs_path)
                
                with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp_file:
                    # Sort by timestamp for optimal row group distribution
                    if 'timestamp' in df.columns:
                        df = df.sort_values('timestamp')
                    elif 'local_timestamp' in df.columns:
                        df = df.sort_values('local_timestamp')
                    
                    # Use optimized Parquet settings for efficient sparse data access
                    df.to_parquet(
                        tmp_file.name, 
                        index=False, 
                        engine='pyarrow',
                        compression='snappy',
                        row_group_size=100000,  # ~1MB per row group for efficient filtering
                        data_page_size=1024 * 1024,  # 1MB data pages
                        use_dictionary=True,  # Dictionary encoding for repeated values
                        write_statistics=True,  # Enable statistics for predicate pushdown
                        use_deprecated_int96_timestamps=False  # Use modern timestamp format
                    )
                    # Get actual file size and calculate appropriate timeout
                    import os
                    file_size = os.path.getsize(tmp_file.name)
                    file_size_mb = file_size / 1024 / 1024
                    logger.info(f"📁 Parquet file size: {file_size_mb:.2f} MB")
                    
                    # Calculate timeout based on file size with configurable adaptive rates
                    from ..config import get_config
                    config = get_config()
                    
                    if file_size_mb < 10:
                        upload_rate = config.gcp.upload_rate_small
                        buffer_time = config.gcp.upload_buffer_small
                    elif file_size_mb < 100:
                        upload_rate = config.gcp.upload_rate_medium
                        buffer_time = config.gcp.upload_buffer_medium
                    else:
                        upload_rate = config.gcp.upload_rate_large
                        buffer_time = config.gcp.upload_buffer_large
                    
                    calculated_timeout = max(config.gcp.upload_timeout_base, int(file_size_mb / upload_rate) + buffer_time)
                    logger.info(f"⏱️  Using timeout: {calculated_timeout}s for {file_size_mb:.1f}MB file (rate: {upload_rate}MB/s)")
                    
                    # Add timeout and retry logic for GCS upload
                    try:
                        logger.info(f"🚀 Starting GCS upload for {gcs_path}...")
                        blob.upload_from_filename(tmp_file.name, timeout=calculated_timeout)
                        logger.info(f"✅ GCS upload completed for {gcs_path}")
                    except Exception as upload_error:
                        logger.error(f"GCS upload failed for {gcs_path}: {upload_error}")
                        # Retry once with a longer timeout
                        retry_timeout = min(calculated_timeout * 2, 600)  # Max 10 minutes
                        try:
                            logger.info(f"🔄 Retrying GCS upload for {gcs_path} with {retry_timeout}s timeout...")
                            blob.upload_from_filename(tmp_file.name, timeout=retry_timeout)
                            logger.info(f"✅ GCS upload retry succeeded for {gcs_path}")
                        except Exception as retry_error:
                            logger.error(f"GCS upload retry failed for {gcs_path}: {retry_error}")
                            raise
                
                uploaded_files.append(f"gs://{self.gcs_bucket}/{gcs_path}")
                logger.debug(f"📤 Uploaded {data_type} data: {gcs_path}")
                
            except Exception as e:
                logger.error(f"❌ Failed to upload {data_type} data: {e}")
                # Re-raise the exception so the batch upload can track it as a failure
                raise
        
        return uploaded_files
    
    async def _upload_to_gcs(self, download_results: dict, target: dict, date: datetime) -> list:
        """Upload downloaded data to GCS with optimized single partition strategy"""
        uploaded_files = []
        
        for data_type, df in download_results.items():
            # Skip if df is None (indicating download failure) or empty list (no data available)
            if df is None:
                # Download failed - this should be tracked as an error
                raise Exception(f"Download failed for {data_type}")
            elif isinstance(df, list) and len(df) == 0:
                # No data available - skip upload but don't treat as error
                continue
            
            # No need to add metadata columns - they're available from:
            # - data_type: in GCS path structure
            # - date: in timestamp columns
            # - venue: extractable from instrument_key
            # - instrument_key: filename
            
            # Log if this is an empty file
            if df.empty:
                logger.info(f"📭 Uploading empty {data_type} file with schema for {target['instrument_key']}")
            
            # Create single partition path (optimized strategy)
            gcs_path = self._create_gcs_path(target, date, data_type)
            
            try:
                # Upload to GCS
                blob = self.bucket.blob(gcs_path)
                
                with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp_file:
                    # Sort by timestamp for optimal row group distribution
                    if 'timestamp' in df.columns:
                        df = df.sort_values('timestamp')
                    elif 'local_timestamp' in df.columns:
                        df = df.sort_values('local_timestamp')
                    
                    # Use optimized Parquet settings for efficient sparse data access
                    df.to_parquet(
                        tmp_file.name, 
                        index=False, 
                        engine='pyarrow',
                        compression='snappy',
                        row_group_size=100000,  # ~1MB per row group for efficient filtering
                        data_page_size=1024 * 1024,  # 1MB data pages
                        use_dictionary=True,  # Dictionary encoding for repeated values
                        write_statistics=True,  # Enable statistics for predicate pushdown
                        use_deprecated_int96_timestamps=False  # Use modern timestamp format
                    )
                    # Get actual file size and calculate appropriate timeout
                    import os
                    file_size = os.path.getsize(tmp_file.name)
                    file_size_mb = file_size / 1024 / 1024
                    logger.info(f"📁 Parquet file size: {file_size_mb:.2f} MB")
                    
                    # Calculate timeout based on file size with configurable adaptive rates
                    from ..config import get_config
                    config = get_config()
                    
                    if file_size_mb < 10:
                        upload_rate = config.gcp.upload_rate_small
                        buffer_time = config.gcp.upload_buffer_small
                    elif file_size_mb < 100:
                        upload_rate = config.gcp.upload_rate_medium
                        buffer_time = config.gcp.upload_buffer_medium
                    else:
                        upload_rate = config.gcp.upload_rate_large
                        buffer_time = config.gcp.upload_buffer_large
                    
                    calculated_timeout = max(config.gcp.upload_timeout_base, int(file_size_mb / upload_rate) + buffer_time)
                    logger.info(f"⏱️  Using timeout: {calculated_timeout}s for {file_size_mb:.1f}MB file (rate: {upload_rate}MB/s)")
                    
                    # Add timeout and retry logic for GCS upload
                    try:
                        logger.info(f"🚀 Starting GCS upload for {gcs_path}...")
                        blob.upload_from_filename(tmp_file.name, timeout=calculated_timeout)
                        logger.info(f"✅ GCS upload completed for {gcs_path}")
                    except Exception as upload_error:
                        logger.error(f"GCS upload failed for {gcs_path}: {upload_error}")
                        # Retry once with a longer timeout
                        retry_timeout = min(calculated_timeout * 2, 600)  # Max 10 minutes
                        try:
                            logger.info(f"🔄 Retrying GCS upload for {gcs_path} with {retry_timeout}s timeout...")
                            blob.upload_from_filename(tmp_file.name, timeout=retry_timeout)
                            logger.info(f"✅ GCS upload retry succeeded for {gcs_path}")
                        except Exception as retry_error:
                            logger.error(f"GCS upload retry failed for {gcs_path}: {retry_error}")
                            raise
                
                uploaded_files.append(f"gs://{self.gcs_bucket}/{gcs_path}")
                logger.info(f"📤 Uploaded {data_type} data: {gcs_path}")
                
            except Exception as e:
                logger.error(f"❌ Failed to upload {data_type} data: {e}")
        
        return uploaded_files
    
    def _create_gcs_path(self, target: dict, date: datetime, data_type: str) -> str:
        """Create single GCS path with optimized by_date partition strategy"""
        instrument_key = target['instrument_key']
        date_str = date.strftime('%Y-%m-%d')
        
        # Single by_date partition: day-{date}/data_type-{data_type}/{instrument_key}.parquet
        return f"raw_tick_data/by_date/day-{date_str}/data_type-{data_type}/{instrument_key}.parquet"
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get comprehensive performance metrics"""
        metrics = self.performance_metrics.copy()
        
        # Calculate derived metrics
        if metrics['batch_times']:
            metrics['avg_batch_time'] = sum(metrics['batch_times']) / len(metrics['batch_times'])
            metrics['total_batch_time'] = sum(metrics['batch_times'])
        else:
            metrics['avg_batch_time'] = 0.0
            metrics['total_batch_time'] = 0.0
        
        # Calculate throughput
        if metrics['total_download_time'] > 0:
            metrics['download_throughput_files_per_sec'] = metrics['total_files_processed'] / metrics['total_download_time']
        else:
            metrics['download_throughput_files_per_sec'] = 0.0
        
        if metrics['total_upload_time'] > 0:
            metrics['upload_throughput_files_per_sec'] = metrics['total_files_processed'] / metrics['total_upload_time']
        else:
            metrics['upload_throughput_files_per_sec'] = 0.0
        
        # Add memory info
        memory_info = self.memory_monitor.get_memory_info()
        metrics['memory_info'] = memory_info
        
        # Add configuration info
        metrics['config'] = {
            'max_workers': self.max_workers,
            'max_parallel_downloads': self.max_parallel_downloads,
            'max_parallel_uploads': self.max_parallel_uploads,
            'batch_size': self.batch_size
        }
        
        return metrics
    
    def log_performance_summary(self):
        """Log a comprehensive performance summary"""
        metrics = self.get_performance_metrics()
        
        logger.info("📊 PERFORMANCE SUMMARY")
        logger.info(f"  Configuration:")
        logger.info(f"    Max Workers: {metrics['config']['max_workers']}")
        logger.info(f"    Max Parallel Downloads: {metrics['config']['max_parallel_downloads']}")
        logger.info(f"    Batch Size: {metrics['config']['batch_size']}")
        logger.info(f"  Timing:")
        logger.info(f"    Total Download Time: {metrics['total_download_time']:.2f}s")
        logger.info(f"    Total Upload Time: {metrics['total_upload_time']:.2f}s")
        logger.info(f"    Total Batch Time: {metrics['total_batch_time']:.2f}s")
        logger.info(f"    Average Batch Time: {metrics['avg_batch_time']:.2f}s")
        logger.info(f"  Throughput:")
        logger.info(f"    Download: {metrics['download_throughput_files_per_sec']:.2f} files/sec")
        logger.info(f"    Upload: {metrics['upload_throughput_files_per_sec']:.2f} files/sec")
        logger.info(f"  Files:")
        logger.info(f"    Total Files Processed: {metrics['total_files_processed']}")
        logger.info(f"    Total Data Size: {metrics['total_data_size_mb']:.2f} MB")
        
        if 'memory_info' in metrics and 'error' not in metrics['memory_info']:
            memory = metrics['memory_info']
            logger.info(f"  Memory:")
            logger.info(f"    Current Usage: {memory['usage_percent']:.1f}%")
            logger.info(f"    Available: {memory['available_gb']:.2f} GB")
            logger.info(f"    Threshold Exceeded: {memory['threshold_exceeded']}")
    
    def reset_performance_metrics(self):
        """Reset performance metrics for a new run"""
        self.performance_metrics = {
            'total_download_time': 0.0,
            'total_upload_time': 0.0,
            'total_files_processed': 0,
            'total_data_size_mb': 0.0,
            'batch_times': [],
            'memory_usage_samples': []
        }
