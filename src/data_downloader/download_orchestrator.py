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
from typing import List, Dict, Any

from .instrument_reader import InstrumentReader
from .tardis_connector import TardisConnector

logger = logging.getLogger(__name__)

class DownloadOrchestrator:
    """Orchestrates downloading Tardis data and uploading to GCS"""
    
    def __init__(self, gcs_bucket: str, api_key: str, max_parallel_downloads: int = None, max_parallel_uploads: int = None):
        self.gcs_bucket = gcs_bucket
        self.api_key = api_key
        self.instrument_reader = InstrumentReader(gcs_bucket)
        self.tardis_connector = TardisConnector(api_key=api_key)
        self.gcs_client = storage.Client()
        self.bucket = self.gcs_client.bucket(gcs_bucket)
        
        # Performance optimization settings
        from config import get_config
        config = get_config()
        self.max_parallel_downloads = max_parallel_downloads or config.tardis.max_concurrent
        self.max_parallel_uploads = max_parallel_uploads or config.tardis.max_parallel_uploads
        self.batch_size = 100  # Process instruments in batches to manage memory
    
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
        
        logger.info(f"Processing {len(targets)} instruments with {self.max_parallel_downloads} parallel downloads")
        
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
                
                logger.info(f"Processing batch {batch_start//self.batch_size + 1}: instruments {batch_start+1}-{batch_end} of {len(targets)}")
                
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
                logger.info(f"Progress: {results['processed'] + results['failed']}/{len(targets)} processed, {rate:.1f} files/sec, ETA: {eta/60:.1f} min")
                
        finally:
            # Always close the connector, even if an error occurs
            await self.tardis_connector.close()
        
        logger.info(f"Download and upload completed: {results['processed']} processed, {results['failed']} failed")
        return results
    
    async def _process_batch_parallel(self, batch_targets: List[Dict], date: datetime, data_types: List[str]) -> Dict[str, Any]:
        """Process a batch of instruments with parallel downloads and uploads"""
        semaphore = asyncio.Semaphore(self.max_parallel_downloads)
        
        async def process_single_instrument(target: Dict) -> Dict[str, Any]:
            async with semaphore:
                try:
                    # Download data
                    download_results = await self.tardis_connector.download_daily_data_direct(
                        tardis_exchange=target['tardis_exchange'],
                        tardis_symbol=target['tardis_symbol'],
                        date=date,
                        data_types=data_types or ['trades', 'book_snapshot_5']
                    )
                    
                    # Upload to GCS with proper partitioning
                    uploaded_files = await self._upload_to_gcs(
                        download_results, target, date
                    )
                    
                    return {
                        'success': True,
                        'instrument_key': target['instrument_key'],
                        'uploaded_files': uploaded_files
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
        
        # Aggregate results
        processed = 0
        failed = 0
        uploaded_files = []
        
        for result in batch_results:
            if isinstance(result, Exception):
                failed += 1
                logger.error(f"❌ Task failed with exception: {result}")
            elif result['success']:
                processed += 1
                uploaded_files.extend(result['uploaded_files'])
            else:
                failed += 1
        
        return {
            'processed': processed,
            'failed': failed,
            'uploaded_files': uploaded_files
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
    
    async def _upload_to_gcs(self, download_results: dict, target: dict, date: datetime) -> list:
        """Upload downloaded data to GCS with optimized single partition strategy"""
        uploaded_files = []
        
        for data_type, df in download_results.items():
            if df.empty:
                continue
            
            # Add partitioning columns
            df['year'] = date.year
            df['month'] = date.month
            df['day'] = date.day
            df['date'] = date.strftime('%Y-%m-%d')
            df['venue'] = target['tardis_exchange']
            df['instrument_key'] = target['instrument_key']
            df['data_type'] = data_type
            
            # Create single partition path (optimized strategy)
            gcs_path = self._create_gcs_path(target, date, data_type)
            
            try:
                # Upload to GCS
                blob = self.bucket.blob(gcs_path)
                
                with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp_file:
                    df.to_parquet(tmp_file.name, index=False, compression='snappy')
                    blob.upload_from_filename(tmp_file.name)
                
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
