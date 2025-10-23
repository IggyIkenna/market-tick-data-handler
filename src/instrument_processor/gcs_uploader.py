"""
GCS Uploader for Instrument Definitions

Handles uploading instrument definitions to GCS with optimized single partition strategy.
Uses by_date partition only for maximum simplicity and efficiency.
"""

import pandas as pd
from datetime import datetime, timezone
from google.cloud import storage
from google.cloud.storage import retry
import tempfile
from pathlib import Path
import logging
from typing import List, Dict, Any
from pydantic import ValidationError
import io

from ..models import InstrumentDefinition

logger = logging.getLogger(__name__)

class WarningCaptureHandler(logging.Handler):
    """Custom logging handler to capture warning messages"""
    def __init__(self):
        super().__init__()
        self.warnings = []
    
    def emit(self, record):
        if record.levelno == logging.WARNING:
            self.warnings.append(self.format(record))
    
    def get_warnings(self):
        return self.warnings.copy()
    
    def clear_warnings(self):
        self.warnings.clear()

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
        
        # Convert datetime objects to strings for Parquet compatibility BEFORE validation
        df_clean = self._prepare_dataframe_for_parquet(df)
        
        # Validate instrument definitions against Pydantic model
        validation_results = self._validate_instrument_definitions(df_clean)
        if validation_results['errors']:
            logger.error(f"❌ Validation failed for {len(validation_results['errors'])} instruments")
            for error in validation_results['errors'][:5]:  # Show first 5 errors
                logger.error(f"  - {error}")
            if len(validation_results['errors']) > 5:
                logger.error(f"  ... and {len(validation_results['errors']) - 5} more errors")
            raise ValueError(f"Validation failed for {len(validation_results['errors'])} instruments")
        
        if validation_results['warnings']:
            logger.warning(f"⚠️ Validation warnings for {len(validation_results['warnings'])} instruments")
            for warning in validation_results['warnings'][:3]:  # Show first 3 warnings
                logger.warning(f"  - {warning}")
            if len(validation_results['warnings']) > 3:
                logger.warning(f"  ... and {len(validation_results['warnings']) - 3} more warnings")
        
        # Single by_date partition (optimized strategy)
        by_date_path = f"instrument_availability/by_date/day-{date.strftime('%Y-%m-%d')}/instruments.parquet"
        
        try:
            buffer = tempfile.NamedTemporaryFile(suffix='.parquet')
            df_clean.to_parquet(buffer.name, index=False, compression='snappy')
            blob = self.bucket.blob(by_date_path)
            blob.upload_from_filename(
                buffer.name,
                timeout=300,  # 5 minutes timeout for daily files
                retry=retry.DEFAULT_RETRY.with_deadline(300)
            )
            
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
            # Upload to GCS with timeout configuration for large files
            blob = self.bucket.blob(aggregate_path)
            
            # Convert DataFrame to bytes
            buffer = tempfile.NamedTemporaryFile(suffix='.parquet')
            df_clean.to_parquet(buffer.name, index=False, compression='snappy')
            
            # Upload to GCS with extended timeout for large files
            # Large files need more time and chunked upload
            logger.info(f"📤 Uploading large aggregate file ({len(df_clean):,} rows) to GCS...")
            blob.upload_from_filename(
                buffer.name,
                timeout=600,  # 10 minutes timeout for large files
                retry=retry.DEFAULT_RETRY.with_deadline(600)  # 10 minutes total deadline
            )
            
            logger.info(f"✅ Uploaded aggregate definitions: {aggregate_path}")
            return f"gs://{self.gcs_bucket}/{aggregate_path}"
            
        except Exception as e:
            logger.error(f"❌ Failed to upload aggregate definitions: {e}")
            raise
    
    def _prepare_dataframe_for_parquet(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare DataFrame for Parquet upload by converting datetime objects to ISO strings
        
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
                    # Check if any values are datetime objects
                    has_datetime_objects = any(
                        isinstance(val, (datetime, pd.Timestamp)) 
                        for val in non_null_values if val is not None
                    )
                    
                    if has_datetime_objects:
                        logger.info(f"Converting mixed datetime/string column '{col}' to ISO strings")
                        df_clean[col] = df_clean[col].apply(
                            lambda x: x.isoformat() if isinstance(x, (datetime, pd.Timestamp)) else x
                        )
            elif 'datetime' in str(df_clean[col].dtype):
                # Handle pandas datetime64 columns
                logger.info(f"Converting datetime64 column '{col}' to ISO strings")
                df_clean[col] = df_clean[col].apply(
                    lambda x: x.isoformat() if pd.notna(x) else None
                )
        
        return df_clean
    
    def _validate_instrument_definitions(self, df: pd.DataFrame) -> Dict[str, List[str]]:
        """
        Validate instrument definitions against Pydantic model
        
        Args:
            df: DataFrame with instrument definitions
            
        Returns:
            Dict with 'errors' and 'warnings' lists
        """
        errors = []
        warnings = []
        
        logger.info(f"Validating {len(df)} instrument definitions against Pydantic model")
        
        # Set up warning capture for the models logger
        models_logger = logging.getLogger('src.models')
        warning_handler = WarningCaptureHandler()
        warning_handler.setLevel(logging.WARNING)
        warning_handler.setFormatter(logging.Formatter('%(message)s'))
        
        # Add handler temporarily
        models_logger.addHandler(warning_handler)
        models_logger.setLevel(logging.WARNING)
        
        try:
            for idx, row in df.iterrows():
                try:
                    # Clear warnings from previous validation
                    warning_handler.clear_warnings()
                    
                    # Convert DataFrame row to dict
                    row_dict = row.to_dict()
                    instrument_key = row_dict.get('instrument_key', f'row_{idx}')
                    
                    # Create InstrumentDefinition instance (this will validate)
                    instrument = InstrumentDefinition.from_dict(row_dict)
                    
                    # Capture warnings from this validation
                    validation_warnings = warning_handler.get_warnings()
                    for warning in validation_warnings:
                        warnings.append(f"Row {idx} ({instrument_key}): {warning}")
                    
                    # Check for missing required fields
                    missing_fields = instrument.validate_required_fields()
                    if missing_fields:
                        errors.append(f"Row {idx} ({instrument_key}): Missing required fields: {missing_fields}")
                    
                except ValidationError as e:
                    # Handle Pydantic validation errors with detailed field information
                    instrument_key = row_dict.get('instrument_key', f'row_{idx}')
                    error_details = []
                    for error in e.errors():
                        field = error.get('loc', ['unknown'])[-1] if error.get('loc') else 'unknown'
                        msg = error.get('msg', 'validation error')
                        input_value = error.get('input', 'N/A')
                        error_details.append(f"{field}: {msg} (value: {input_value})")
                    
                    errors.append(f"Row {idx} ({instrument_key}): Pydantic validation failed - {'; '.join(error_details)}")
                    
                except Exception as e:
                    # Handle other exceptions
                    instrument_key = row_dict.get('instrument_key', f'row_{idx}')
                    errors.append(f"Row {idx} ({instrument_key}): Unexpected error - {str(e)}")
        
        finally:
            # Remove the warning handler
            models_logger.removeHandler(warning_handler)
        
        logger.info(f"Validation complete: {len(errors)} errors, {len(warnings)} warnings")
        
        return {
            'errors': errors,
            'warnings': warnings
        }
    
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
