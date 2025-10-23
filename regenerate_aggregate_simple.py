#!/usr/bin/env python3
"""
Simple aggregate file regeneration using gsutil commands
This approach downloads files locally first, then combines them.
"""

import subprocess
import pandas as pd
from datetime import datetime, timedelta
import tempfile
import os
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_gsutil_command(cmd, timeout=300):
    """Run gsutil command with timeout"""
    try:
        result = subprocess.run(
            cmd, 
            shell=True, 
            capture_output=True, 
            text=True, 
            timeout=timeout
        )
        return result.returncode == 0, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        logger.error(f"⏰ Command timed out: {cmd}")
        return False, "", "Timeout"
    except Exception as e:
        logger.error(f"❌ Command failed: {cmd} - {e}")
        return False, "", str(e)

def get_daily_files_list(start_date: datetime, end_date: datetime):
    """Get list of daily files using gsutil ls"""
    logger.info("📋 Getting list of daily files from GCS...")
    
    # Use gsutil to list all files in the date range
    pattern = "gs://market-data-tick/instrument_availability/by_date/day-*/instruments.parquet"
    success, stdout, stderr = run_gsutil_command(f"gsutil ls {pattern}")
    
    if not success:
        logger.error(f"❌ Failed to list files: {stderr}")
        return []
    
    # Filter files by date range
    files = []
    current_date = start_date
    
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        expected_path = f"gs://market-data-tick/instrument_availability/by_date/day-{date_str}/instruments.parquet"
        
        if expected_path in stdout:
            files.append((current_date, expected_path))
        else:
            logger.warning(f"⚠️ Missing file for {date_str}")
        
        current_date += timedelta(days=1)
    
    logger.info(f"📅 Found {len(files)} files in date range")
    return files

def download_and_combine_files(files, max_files=None):
    """Download files and combine them into aggregate"""
    if max_files:
        files = files[:max_files]
        logger.info(f"🧪 Processing only first {max_files} files for testing")
    
    logger.info(f"📥 Downloading and combining {len(files)} files...")
    
    # Create temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        all_dataframes = []
        
        for i, (date, gcs_path) in enumerate(files, 1):
            logger.info(f"📥 Processing {i}/{len(files)}: {date.strftime('%Y-%m-%d')}")
            
            # Download file
            local_file = temp_path / f"instruments_{date.strftime('%Y%m%d')}.parquet"
            success, stdout, stderr = run_gsutil_command(
                f"gsutil cp {gcs_path} {local_file}", 
                timeout=120
            )
            
            if not success:
                logger.error(f"❌ Failed to download {gcs_path}: {stderr}")
                continue
            
            # Load and combine
            try:
                df = pd.read_parquet(local_file)
                if not df.empty:
                    all_dataframes.append(df)
                    logger.info(f"✅ Loaded {len(df)} instruments from {date.strftime('%Y-%m-%d')}")
                else:
                    logger.warning(f"⚠️ Empty file for {date.strftime('%Y-%m-%d')}")
            except Exception as e:
                logger.error(f"❌ Failed to read {local_file}: {e}")
        
        if not all_dataframes:
            raise ValueError("No valid files found")
        
        # Combine all dataframes
        logger.info("🔗 Combining all files...")
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        logger.info(f"📊 Total instruments: {len(combined_df):,}")
        
        return combined_df

def upload_aggregate_file(df, start_date: datetime, end_date: datetime):
    """Upload aggregate file to GCS"""
    logger.info("📤 Uploading aggregate file to GCS...")
    
    # Create temporary file
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
        df.to_parquet(temp_file.name, index=False, compression='snappy')
        temp_path = temp_file.name
    
    try:
        # Upload to GCS
        aggregate_path = f"gs://market-data-tick/instrument_availability/aggregate/instruments_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.parquet"
        
        success, stdout, stderr = run_gsutil_command(
            f"gsutil cp {temp_path} {aggregate_path}",
            timeout=600  # 10 minutes for large file
        )
        
        if success:
            logger.info(f"✅ Successfully uploaded: {aggregate_path}")
            return aggregate_path
        else:
            logger.error(f"❌ Upload failed: {stderr}")
            raise Exception(f"Upload failed: {stderr}")
    
    finally:
        # Clean up temp file
        os.unlink(temp_path)

def main():
    """Main function"""
    import sys
    
    try:
        # Check if test mode
        test_mode = '--test' in sys.argv
        max_files = 5 if test_mode else None
        
        if test_mode:
            logger.info("🧪 Running in TEST MODE - processing only 5 files")
        
        # Date range
        start_date = datetime(2023, 5, 23)
        end_date = datetime(2025, 10, 20)
        
        logger.info("🚀 Starting aggregate file regeneration...")
        logger.info(f"📅 Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        # Get list of files
        files = get_daily_files_list(start_date, end_date)
        if not files:
            raise ValueError("No files found in date range")
        
        # Download and combine
        combined_df = download_and_combine_files(files, max_files)
        
        # Upload aggregate
        aggregate_path = upload_aggregate_file(combined_df, start_date, end_date)
        
        logger.info("🎉 Aggregate file regeneration completed successfully!")
        logger.info(f"📁 Aggregate file: {aggregate_path}")
        
    except Exception as e:
        logger.error(f"❌ Regeneration failed: {e}")
        raise

if __name__ == "__main__":
    main()
