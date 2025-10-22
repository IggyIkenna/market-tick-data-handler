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

from .instrument_reader import InstrumentReader
from .tardis_connector import TardisConnector

logger = logging.getLogger(__name__)

class DownloadOrchestrator:
    """Orchestrates downloading Tardis data and uploading to GCS"""
    
    def __init__(self, gcs_bucket: str, api_key: str):
        self.gcs_bucket = gcs_bucket
        self.api_key = api_key
        self.instrument_reader = InstrumentReader(gcs_bucket)
        self.tardis_connector = TardisConnector(api_key=api_key)
        self.gcs_client = storage.Client()
        self.bucket = self.gcs_client.bucket(gcs_bucket)
    
    async def download_and_upload_data(self, date: datetime, 
                                     venues: list = None,
                                     instrument_types: list = None,
                                     data_types: list = None,
                                     max_instruments: int = None,
                                     start_date: datetime = None,
                                     end_date: datetime = None) -> dict:
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
        
        if not targets:
            logger.warning("No download targets found")
            return {'status': 'no_targets', 'processed': 0}
        
        logger.info(f"Processing {len(targets)} instruments")
        
        # Download data for each target
        results = {
            'status': 'success',
            'processed': 0,
            'failed': 0,
            'uploaded_files': []
        }
        
        try:
            for target in targets:
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
                    
                    results['uploaded_files'].extend(uploaded_files)
                    results['processed'] += 1
                    
                    logger.info(f"✅ Processed {target['instrument_key']}")
                    
                except Exception as e:
                    logger.error(f"❌ Failed to process {target['instrument_key']}: {e}")
                    results['failed'] += 1
        finally:
            # Always close the connector, even if an error occurs
            await self.tardis_connector.close()
        
        logger.info(f"Download and upload completed: {results['processed']} processed, {results['failed']} failed")
        return results
    
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
