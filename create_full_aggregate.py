#!/usr/bin/env python3
"""
Create full aggregate file by processing date ranges in batches
This approach avoids listing all files at once and processes them in manageable chunks.
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

def download_file_if_exists(gcs_path, local_path):
    """Download file if it exists, return True if successful"""
    result = subprocess.run(
        f"gsutil -q cp {gcs_path} {local_path}",
        shell=True,
        capture_output=True,
        text=True,
        timeout=60
    )
    return result.returncode == 0

def process_date_range(start_date, end_date, temp_dir):
    """Process a range of dates and return combined DataFrame"""
    logger.info(f"📅 Processing {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    all_dataframes = []
    current_date = start_date
    processed = 0
    skipped = 0
    
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        gcs_path = f"gs://market-data-tick/instrument_availability/by_date/day-{date_str}/instruments.parquet"
        local_file = temp_dir / f"instruments_{date_str}.parquet"
        
        if download_file_if_exists(gcs_path, local_file):
            try:
                df = pd.read_parquet(local_file)
                if not df.empty:
                    all_dataframes.append(df)
                    processed += 1
                    if processed % 50 == 0:
                        logger.info(f"  📊 Processed {processed} files...")
                else:
                    skipped += 1
            except Exception as e:
                logger.warning(f"  ⚠️ Failed to read {date_str}: {e}")
                skipped += 1
        else:
            skipped += 1
        
        current_date += timedelta(days=1)
    
    logger.info(f"  ✅ Range complete: {processed} processed, {skipped} skipped")
    
    if all_dataframes:
        return pd.concat(all_dataframes, ignore_index=True)
    else:
        return pd.DataFrame()

def main():
    """Create full aggregate file"""
    try:
        logger.info("🚀 Creating full aggregate file...")
        
        # Create temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Process in monthly batches to avoid memory issues
            start_date = datetime(2023, 5, 23)
            end_date = datetime(2025, 10, 20)
            
            all_dataframes = []
            current_start = start_date
            
            while current_start <= end_date:
                # Calculate end of month
                if current_start.month == 12:
                    current_end = datetime(current_start.year + 1, 1, 1) - timedelta(days=1)
                else:
                    current_end = datetime(current_start.year, current_start.month + 1, 1) - timedelta(days=1)
                
                # Don't go past the final end date
                current_end = min(current_end, end_date)
                
                # Process this month
                month_df = process_date_range(current_start, current_end, temp_path)
                if not month_df.empty:
                    all_dataframes.append(month_df)
                    logger.info(f"📊 Month {current_start.strftime('%Y-%m')}: {len(month_df):,} instruments")
                
                # Move to next month
                current_start = current_end + timedelta(days=1)
            
            if not all_dataframes:
                raise ValueError("No data found")
            
            # Combine all monthly dataframes
            logger.info("🔗 Combining all monthly data...")
            final_df = pd.concat(all_dataframes, ignore_index=True)
            logger.info(f"📊 Total instruments: {len(final_df):,}")
            
            # Save aggregate file
            aggregate_file = temp_path / "full_aggregate.parquet"
            final_df.to_parquet(aggregate_file, index=False, compression='snappy')
            
            # Upload to GCS
            gcs_path = f"gs://market-data-tick/instrument_availability/aggregate/instruments_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.parquet"
            logger.info(f"📤 Uploading full aggregate to {gcs_path}")
            
            result = subprocess.run(
                f"gsutil cp {aggregate_file} {gcs_path}",
                shell=True,
                capture_output=True,
                text=True,
                timeout=600  # 10 minutes for large file
            )
            
            if result.returncode == 0:
                logger.info("🎉 Full aggregate file created successfully!")
                logger.info(f"📁 Aggregate file: {gcs_path}")
                logger.info(f"📊 Total instruments: {len(final_df):,}")
            else:
                logger.error(f"❌ Upload failed: {result.stderr}")
        
    except Exception as e:
        logger.error(f"❌ Failed: {e}")
        raise

if __name__ == "__main__":
    main()
