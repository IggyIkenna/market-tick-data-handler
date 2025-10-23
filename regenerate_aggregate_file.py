#!/usr/bin/env python3
"""
Regenerate aggregate instrument definitions file from existing daily files

This script reads all daily instrument definition files from GCS and creates
the aggregate file that failed during the main instrument generation process.
"""

import pandas as pd
from datetime import datetime, timedelta
from google.cloud import storage
from src.instrument_processor.gcs_uploader import InstrumentGCSUploader
import logging
import tempfile
import io

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AggregateFileRegenerator:
    """Regenerate aggregate instrument definitions from daily files"""
    
    def __init__(self, gcs_bucket: str):
        self.gcs_bucket = gcs_bucket
        self.client = storage.Client()
        self.bucket = self.client.bucket(gcs_bucket)
        self.uploader = InstrumentGCSUploader(gcs_bucket)
    
    def get_daily_file_paths(self, start_date: datetime, end_date: datetime) -> list:
        """Get all daily instrument file paths in the date range"""
        paths = []
        current_date = start_date
        
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            path = f"instrument_availability/by_date/day-{date_str}/instruments.parquet"
            paths.append((current_date, path))
            current_date += timedelta(days=1)
        
        return paths
    
    def load_daily_file(self, path: str) -> pd.DataFrame:
        """Load a single daily instrument file from GCS"""
        try:
            blob = self.bucket.blob(path)
            if not blob.exists():
                logger.warning(f"⚠️ File not found: {path}")
                return pd.DataFrame()
            
            # Download to memory with timeout
            logger.debug(f"📥 Downloading {path}...")
            data = blob.download_as_bytes(timeout=60)  # 1 minute timeout per file
            
            # Read parquet from bytes
            df = pd.read_parquet(io.BytesIO(data))
            logger.info(f"📥 Loaded {len(df)} instruments from {path}")
            return df
            
        except Exception as e:
            logger.error(f"❌ Failed to load {path}: {e}")
            return pd.DataFrame()
    
    def regenerate_aggregate(self, start_date: datetime, end_date: datetime) -> str:
        """Regenerate aggregate file from daily files"""
        logger.info(f"🔄 Regenerating aggregate file from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        # Get all daily file paths
        daily_paths = self.get_daily_file_paths(start_date, end_date)
        logger.info(f"📅 Found {len(daily_paths)} daily files to process")
        
        # Load all daily files in batches
        all_dataframes = []
        successful_loads = 0
        batch_size = 50  # Process 50 files at a time
        
        for batch_start in range(0, len(daily_paths), batch_size):
            batch_end = min(batch_start + batch_size, len(daily_paths))
            batch_paths = daily_paths[batch_start:batch_end]
            
            logger.info(f"📦 Processing batch {batch_start//batch_size + 1}: files {batch_start+1}-{batch_end} of {len(daily_paths)}")
            
            for i, (date, path) in enumerate(batch_paths, batch_start + 1):
                logger.info(f"📥 Loading file {i}/{len(daily_paths)}: {date.strftime('%Y-%m-%d')}")
                
                df = self.load_daily_file(path)
                if not df.empty:
                    all_dataframes.append(df)
                    successful_loads += 1
                else:
                    logger.warning(f"⚠️ Empty file for {date.strftime('%Y-%m-%d')}")
            
            # Log batch progress
            logger.info(f"✅ Batch {batch_start//batch_size + 1} complete: {len(batch_paths)} files processed")
        
        if not all_dataframes:
            raise ValueError("No valid daily files found to create aggregate")
        
        logger.info(f"📊 Successfully loaded {successful_loads}/{len(daily_paths)} daily files")
        
        # Combine all dataframes
        logger.info("🔗 Combining all daily files into aggregate...")
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        logger.info(f"📊 Total instruments in aggregate: {len(combined_df):,}")
        
        # Upload aggregate file with extended timeout
        logger.info("📤 Uploading aggregate file to GCS...")
        aggregate_path = self.uploader.upload_aggregate_definitions(
            combined_df, start_date, end_date
        )
        
        logger.info(f"✅ Successfully regenerated aggregate file: {aggregate_path}")
        return aggregate_path

def main():
    """Main function to regenerate aggregate file"""
    import sys
    
    try:
        # Check if test mode is requested
        test_mode = '--test' in sys.argv
        
        if test_mode:
            # Test with just a few days
            start_date = datetime(2023, 5, 23)
            end_date = datetime(2023, 5, 25)  # Just 3 days for testing
            logger.info("🧪 Running in TEST MODE - processing only 3 days")
        else:
            # Full date range from the original run
            start_date = datetime(2023, 5, 23)
            end_date = datetime(2025, 10, 20)
        
        logger.info("🚀 Starting aggregate file regeneration...")
        logger.info(f"📅 Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        regenerator = AggregateFileRegenerator('market-data-tick')
        aggregate_path = regenerator.regenerate_aggregate(start_date, end_date)
        
        logger.info("🎉 Aggregate file regeneration completed successfully!")
        logger.info(f"📁 Aggregate file: {aggregate_path}")
        
    except Exception as e:
        logger.error(f"❌ Regeneration failed: {e}")
        raise

if __name__ == "__main__":
    main()
