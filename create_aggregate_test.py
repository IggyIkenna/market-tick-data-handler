#!/usr/bin/env python3
"""
Create a test aggregate file from just a few daily files
This is a simple test to verify the concept works before scaling up.
"""

import subprocess
import pandas as pd
from datetime import datetime
import tempfile
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def download_specific_file(gcs_path, local_path):
    """Download a specific file from GCS"""
    logger.info(f"📥 Downloading {gcs_path}")
    result = subprocess.run(
        f"gsutil cp {gcs_path} {local_path}",
        shell=True,
        capture_output=True,
        text=True,
        timeout=60
    )
    return result.returncode == 0

def main():
    """Create test aggregate from a few files"""
    try:
        # Test with just 3 specific files we know exist
        test_files = [
            "gs://market-data-tick/instrument_availability/by_date/day-2023-05-23/instruments.parquet",
            "gs://market-data-tick/instrument_availability/by_date/day-2023-06-01/instruments.parquet", 
            "gs://market-data-tick/instrument_availability/by_date/day-2024-01-01/instruments.parquet"
        ]
        
        logger.info("🧪 Creating test aggregate from 3 specific files...")
        
        # Create temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            all_dataframes = []
            
            for i, gcs_path in enumerate(test_files, 1):
                local_file = f"{temp_dir}/file_{i}.parquet"
                
                if download_specific_file(gcs_path, local_file):
                    try:
                        df = pd.read_parquet(local_file)
                        all_dataframes.append(df)
                        logger.info(f"✅ Loaded {len(df)} instruments from file {i}")
                    except Exception as e:
                        logger.error(f"❌ Failed to read {local_file}: {e}")
                else:
                    logger.error(f"❌ Failed to download {gcs_path}")
            
            if not all_dataframes:
                raise ValueError("No files loaded successfully")
            
            # Combine dataframes
            logger.info("🔗 Combining files...")
            combined_df = pd.concat(all_dataframes, ignore_index=True)
            logger.info(f"📊 Total instruments: {len(combined_df):,}")
            
            # Save test aggregate
            test_aggregate = f"{temp_dir}/test_aggregate.parquet"
            combined_df.to_parquet(test_aggregate, index=False, compression='snappy')
            
            # Upload to GCS
            gcs_path = "gs://market-data-tick/instrument_availability/aggregate/test_aggregate.parquet"
            logger.info(f"📤 Uploading test aggregate to {gcs_path}")
            
            result = subprocess.run(
                f"gsutil cp {test_aggregate} {gcs_path}",
                shell=True,
                capture_output=True,
                text=True,
                timeout=300
            )
            
            if result.returncode == 0:
                logger.info("✅ Test aggregate uploaded successfully!")
                logger.info(f"📁 Test file: {gcs_path}")
            else:
                logger.error(f"❌ Upload failed: {result.stderr}")
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        raise

if __name__ == "__main__":
    main()
