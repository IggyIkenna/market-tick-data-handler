"""
GCS Uploader for Instrument Definitions

Handles uploading instrument definitions to GCS with optimized single partition strategy.
Uses by_date partition only for maximum simplicity and efficiency.
"""

import pandas as pd
from datetime import datetime, timezone
from google.cloud import storage
import tempfile
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class InstrumentGCSUploader:
    """Uploads instrument definitions to GCS with optimized single partition strategy"""
    
    def __init__(self, gcs_bucket: str):
        self.gcs_bucket = gcs_bucket
        self.client = storage.Client()
        self.bucket = self.client.bucket(gcs_bucket)
    
    def upload_instrument_definitions(self, df: pd.DataFrame, date: datetime) -> str:
        """
        Upload instrument definitions with single by_date partition
        
        Args:
            df: DataFrame with instrument definitions
            date: Date for partitioning
            
        Returns:
            str: GCS path of the uploaded file
        """
        logger.info(f"Uploading {len(df)} instrument definitions for {date.strftime('%Y-%m-%d')}")
        
        # Convert datetime objects to strings for Parquet compatibility
        df_clean = self._prepare_dataframe_for_parquet(df)
        
        # Single by_date partition (optimized strategy)
        by_date_path = f"instrument_availability/by_date/day-{date.strftime('%Y-%m-%d')}/instruments.parquet"
        
        try:
            buffer = tempfile.NamedTemporaryFile(suffix='.parquet')
            df_clean.to_parquet(buffer.name, index=False, compression='snappy')
            blob = self.bucket.blob(by_date_path)
            blob.upload_from_filename(buffer.name)
            
            gcs_path = f"gs://{self.gcs_bucket}/{by_date_path}"
            logger.info(f"✅ Uploaded instrument definitions: {by_date_path}")
            return gcs_path
            
        except Exception as e:
            logger.error(f"❌ Failed to upload instrument definitions: {e}")
            raise
    
    def upload_aggregate_definitions(self, df: pd.DataFrame, start_date: datetime, end_date: datetime) -> str:
        """
        Upload aggregate instrument definitions for a date range
        
        Args:
            df: DataFrame with all instrument definitions for the range
            start_date: Start date of the range
            end_date: End date of the range
            
        Returns:
            str: GCS path of the aggregate file
        """
        logger.info(f"Uploading aggregate definitions for {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        # Convert datetime objects to strings for Parquet compatibility
        df_clean = self._prepare_dataframe_for_parquet(df)
        
        # Create aggregate path with date range
        aggregate_path = f"instrument_availability/aggregate/instruments_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.parquet"
        
        try:
            # Upload to GCS
            blob = self.bucket.blob(aggregate_path)
            
            # Convert DataFrame to bytes
            buffer = tempfile.NamedTemporaryFile(suffix='.parquet')
            df_clean.to_parquet(buffer.name, index=False, compression='snappy')
            
            # Upload to GCS
            blob.upload_from_filename(buffer.name)
            
            logger.info(f"✅ Uploaded aggregate definitions: {aggregate_path}")
            return f"gs://{self.gcs_bucket}/{aggregate_path}"
            
        except Exception as e:
            logger.error(f"❌ Failed to upload aggregate definitions: {e}")
            raise
    
    def _prepare_dataframe_for_parquet(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare DataFrame for Parquet upload by converting datetime objects to strings
        
        Args:
            df: DataFrame with potential datetime objects
            
        Returns:
            DataFrame with datetime objects converted to ISO strings
        """
        df_clean = df.copy()
        
        # Convert datetime columns to ISO strings
        for col in df_clean.columns:
            # Check for datetime-like columns (object dtype with datetime objects or datetime64 dtype)
            if df_clean[col].dtype == 'object':
                # Check if column contains datetime objects by sampling non-null values
                non_null_values = df_clean[col].dropna()
                if not non_null_values.empty:
                    sample_value = non_null_values.iloc[0]
                    if hasattr(sample_value, 'isoformat'):
                        logger.info(f"Converting datetime column '{col}' to ISO strings")
                        df_clean[col] = df_clean[col].apply(
                            lambda x: x.isoformat() if x and hasattr(x, 'isoformat') else str(x) if x else ''
                        )
            elif 'datetime' in str(df_clean[col].dtype):
                # Handle pandas datetime64 columns
                logger.info(f"Converting datetime64 column '{col}' to ISO strings")
                df_clean[col] = df_clean[col].apply(
                    lambda x: x.isoformat() if pd.notna(x) else ''
                )
        
        return df_clean
    
    def _create_partitions(self, date: datetime) -> dict:
        """Create partition paths with proper filtering per partition type"""
        partitions = {}
        
        # Different partitioning strategies for different query patterns:
        
        # 1. By Date Partition (date-based queries)
        partitions['by_date'] = f"instrument_availability/by_date/day-{date.strftime('%Y-%m-%d')}/instruments.parquet"
        
        # 2. By Venue Partition (venue-based queries) - group by venue, then date
        partitions['by_venue'] = f"instrument_availability/by_venue/venue-all/day-{date.strftime('%Y-%m-%d')}/instruments.parquet"
        
        # 3. By Type Partition (type-based queries) - group by type, then date  
        partitions['by_type'] = f"instrument_availability/by_type/type-all/day-{date.strftime('%Y-%m-%d')}/instruments.parquet"
        
        # 4. Single aggregated file (for easy access)
        partitions['aggregated'] = f"instrument_availability/instruments_{date.strftime('%Y%m%d')}.parquet"
        
        return partitions
