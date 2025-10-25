"""
Query Handler for Data Retrieval

Handles GCS data retrieval, validation, and filtering for both instrument definitions
and tick data queries with comprehensive error handling and observability.
"""

import asyncio
import logging
import pandas as pd
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Tuple, Set
from pathlib import Path
import io
from google.cloud import storage
from google.cloud.exceptions import NotFound

from ..utils.gcs_client import get_shared_gcs_client, get_shared_gcs_bucket
from ..utils.logger import log_operation_start, log_operation_success, log_operation_failure
import logging
from ..utils.error_handler import ErrorHandler, ErrorContext, ErrorCategory
from ..models import InstrumentDefinition

logger = logging.getLogger(__name__)

class QueryHandler:
    """Handles data retrieval and validation for library usage"""
    
    def __init__(self, gcs_bucket: str, config):
        self.gcs_bucket = gcs_bucket
        self.config = config
        self.client = get_shared_gcs_client()
        self.bucket = get_shared_gcs_bucket(gcs_bucket)
        self.error_handler = ErrorHandler(logger)
    
    async def get_instrument_definitions(
        self, 
        start_date: Optional[datetime] = None, 
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Retrieve instrument definitions with optional date filtering
        
        Args:
            start_date: Optional start date for filtering
            end_date: Optional end date for filtering
            
        Returns:
            Dict containing data, metadata, and file paths
        """
        context = ErrorContext(operation="get_instrument_definitions", component="query_handler")
        log_operation_start(logger, "get_instrument_definitions", start_date=start_date, end_date=end_date)
        
        try:
            # Determine if we need aggregate or daily files
            if start_date is None and end_date is None:
                # Get aggregate file
                return await self._get_aggregate_instruments()
            else:
                # Get daily files and filter
                return await self._get_daily_instruments(start_date, end_date)
                
        except Exception as e:
            log_operation_failure(logger, "get_instrument_definitions", error=str(e))
            enhanced_error = self.error_handler.handle_error(e, context)
            raise enhanced_error
    
    async def get_tick_data(
        self,
        instrument_ids: List[str],
        start_date: datetime,
        end_date: datetime,
        data_types: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Retrieve tick data for specific instruments over date range
        
        Args:
            instrument_ids: List of instrument keys to retrieve
            start_date: Start date (required)
            end_date: End date (required)
            data_types: Optional list of data types (e.g., ['trades', 'book_snapshot_5'])
            
        Returns:
            Dict containing file paths organized by date and data type
        """
        context = ErrorContext(operation="get_tick_data", component="query_handler")
        log_operation_start(logger, "get_tick_data", 
            instrument_ids=instrument_ids[:5],  # Log first 5 for brevity
            start_date=start_date,
            end_date=end_date,
            data_types=data_types
        )
        
        try:
            # Validate date range
            if start_date > end_date:
                raise ValueError("Start date must be before or equal to end date")
            
            if (end_date - start_date).days > self.config.query_api.max_date_range_days:
                raise ValueError(f"Date range exceeds maximum of {self.config.query_api.max_date_range_days} days")
            
            # Validate instrument count
            if len(instrument_ids) > self.config.query_api.max_instruments_per_query:
                raise ValueError(f"Too many instruments requested. Maximum: {self.config.query_api.max_instruments_per_query}")
            
            # Validate instrument IDs exist
            await self._validate_instrument_ids(instrument_ids, start_date, end_date)
            
            # Set default data types if not provided
            if data_types is None:
                data_types = ['trades', 'market_snapshot_5']
            
            # Get file paths for all requested data
            file_paths = await self._get_tick_data_paths(instrument_ids, start_date, end_date, data_types)
            
            log_operation_success(logger, "get_tick_data", files_found=len(file_paths))
            
            return {
                "file_paths": file_paths,
                "metadata": {
                    "instrument_count": len(instrument_ids),
                    "date_range": f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
                    "data_types": data_types,
                    "total_files": len(file_paths)
                }
            }
            
        except Exception as e:
            log_operation_failure(logger, "get_tick_data", error=str(e))
            enhanced_error = self.error_handler.handle_error(e, context)
            raise enhanced_error
    
    async def _get_aggregate_instruments(self) -> Dict[str, Any]:
        """Get aggregate instrument definitions file"""
        # Look for the most recent aggregate file
        blobs = self.client.list_blobs(self.gcs_bucket, prefix="instrument_availability/aggregate/")
        aggregate_files = [blob for blob in blobs if blob.name.endswith('.parquet')]
        
        if not aggregate_files:
            raise FileNotFoundError("No aggregate instrument definitions found in GCS")
        
        # Get the most recent file
        latest_file = max(aggregate_files, key=lambda x: x.time_created)
        
        return {
            "file_path": f"gs://{self.gcs_bucket}/{latest_file.name}",
            "blob_name": latest_file.name,
            "file_size": latest_file.size or 0,
            "created": latest_file.time_created,
            "metadata": {
                "type": "aggregate",
                "file_count": 1
            }
        }
    
    async def _get_daily_instruments(
        self, 
        start_date: Optional[datetime], 
        end_date: Optional[datetime]
    ) -> Dict[str, Any]:
        """Get daily instrument definition files and filter by date range"""
        # Generate list of dates to check
        if start_date is None:
            start_date = datetime.now(timezone.utc) - timedelta(days=30)  # Default to last 30 days
        if end_date is None:
            end_date = datetime.now(timezone.utc)
        
        file_paths = []
        missing_dates = []
        
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            blob_name = f"instrument_availability/by_date/day-{date_str}/instruments.parquet"
            blob = self.bucket.blob(blob_name)
            
            if blob.exists():
                file_paths.append({
                    "date": date_str,
                    "file_path": f"gs://{self.gcs_bucket}/{blob_name}",
                    "blob_name": blob_name,
                    "file_size": blob.size or 0
                })
            else:
                missing_dates.append(date_str)
            
            current_date += timedelta(days=1)
        
        if not file_paths:
            raise FileNotFoundError(f"No instrument definition files found for date range {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        if missing_dates:
            logger.warning(f"Missing instrument definitions for {len(missing_dates)} dates: {missing_dates[:5]}{'...' if len(missing_dates) > 5 else ''}")
        
        return {
            "file_paths": file_paths,
            "missing_dates": missing_dates,
            "metadata": {
                "type": "daily",
                "file_count": len(file_paths),
                "missing_count": len(missing_dates),
                "date_range": f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
            }
        }
    
    async def _validate_instrument_ids(
        self, 
        instrument_ids: List[str], 
        start_date: datetime, 
        end_date: datetime
    ) -> None:
        """Validate that all instrument IDs exist in the date range"""
        logger.info(f"Validating {len(instrument_ids)} instrument IDs")
        
        # Get instrument definitions for the date range
        instruments_data = await self._get_daily_instruments(start_date, end_date)
        
        # Collect all valid instrument IDs from the files
        valid_instrument_ids = set()
        
        for file_info in instruments_data["file_paths"]:
            try:
                # Read the parquet file to get instrument IDs
                blob = self.bucket.blob(file_info["blob_name"])
                parquet_data = blob.download_as_bytes()
                
                # Read parquet from bytes
                df = pd.read_parquet(io.BytesIO(parquet_data))
                
                if 'instrument_key' in df.columns:
                    file_instrument_ids = set(df['instrument_key'].dropna().tolist())
                    valid_instrument_ids.update(file_instrument_ids)
                    
            except Exception as e:
                logger.warning(f"Failed to read instrument definitions from {file_info['blob_name']}: {e}")
                continue
        
        # Check which requested instrument IDs are missing
        requested_ids = set(instrument_ids)
        missing_ids = requested_ids - valid_instrument_ids
        
        if missing_ids:
            missing_list = list(missing_ids)[:10]  # Show first 10
            error_msg = f"Invalid instrument IDs: {missing_list}"
            if len(missing_ids) > 10:
                error_msg += f" (and {len(missing_ids) - 10} more)"
            raise ValueError(error_msg)
        
        logger.info(f"✅ All {len(instrument_ids)} instrument IDs are valid")
    
    async def _get_tick_data_paths(
        self,
        instrument_ids: List[str],
        start_date: datetime,
        end_date: datetime,
        data_types: List[str]
    ) -> List[Dict[str, Any]]:
        """Get file paths for tick data from market-data-tick bucket"""
        file_paths = []
        current_date = start_date
        
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            
            for data_type in data_types:
                # Construct file path based on actual GCS structure
                # Pattern: raw_tick_data/by_date/day-2023-05-23/data_type-trades/
                blob_pattern = f"raw_tick_data/by_date/day-{date_str}/data_type-{data_type}/"
                
                # List all blobs matching the pattern
                blobs = list(self.client.list_blobs("market-data-tick", prefix=blob_pattern))
                
                for blob in blobs:
                    if blob.name.endswith('.csv.zst') or blob.name.endswith('.parquet'):  # Process compressed CSV and parquet files
                        # Extract instrument ID from filename if possible, otherwise use first requested
                        instrument_id = instrument_ids[0] if instrument_ids else "unknown"
                        
                        file_paths.append({
                            "date": date_str,
                            "data_type": data_type,
                            "instrument_id": instrument_id,
                            "file_path": f"gs://market-data-tick/{blob.name}",
                            "blob_name": blob.name,
                            "file_size": blob.size or 0
                        })
            
            current_date += timedelta(days=1)
        
        return file_paths
    
    def _map_instrument_to_symbol(self, instrument_key: str) -> str:
        """Map instrument key to symbol used in file names"""
        # Extract the symbol part from instrument key
        # e.g., BINANCE:SPOT_PAIR:BTC-USDT -> BTCUSD_PERP
        if ':' in instrument_key:
            parts = instrument_key.split(':')
            if len(parts) >= 3:
                symbol_part = parts[2]  # BTC-USDT
                # Convert to the format used in files
                if symbol_part == 'BTC-USDT':
                    return 'BTCUSD_PERP'
                elif symbol_part == 'ETH-USDT':
                    return 'ETHUSD_PERP'
                elif symbol_part == 'LTC-USDT':
                    return 'LTCUSD_PERP'
                elif symbol_part == 'ADA-USDT':
                    return 'ADAUSD_PERP'
                # Add more mappings as needed
                else:
                    # Generic mapping: remove hyphens and add _PERP
                    return symbol_part.replace('-', '') + '_PERP'
        
        # Fallback: return as-is
        return instrument_key
    
    async def read_parquet_file(self, blob_name: str) -> pd.DataFrame:
        """Read a parquet file from GCS and return as DataFrame"""
        try:
            blob = self.bucket.blob(blob_name)
            parquet_data = blob.download_as_bytes()
            return pd.read_parquet(io.BytesIO(parquet_data))
        except Exception as e:
            logger.error(f"Failed to read parquet file {blob_name}: {e}")
            raise
    
    def generate_signed_url(self, blob_name: str, expiry_hours: int = None) -> str:
        """Generate a signed URL for a GCS blob"""
        if expiry_hours is None:
            expiry_hours = self.config.query_api.signed_url_expiry_hours
        
        blob = self.bucket.blob(blob_name)
        
        # Generate signed URL with expiration
        from datetime import timedelta
        expiration = datetime.utcnow() + timedelta(hours=expiry_hours)
        
        return blob.generate_signed_url(
            version="v4",
            expiration=expiration,
            method="GET"
        )
    
    async def check_data_availability(
        self, 
        start_date: datetime, 
        end_date: datetime
    ) -> Dict[str, Any]:
        """Check what data is available for a date range"""
        available_dates = []
        missing_dates = []
        
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            
            # Check for instrument definitions
            instrument_blob = self.bucket.blob(f"instrument_availability/by_date/day-{date_str}/instruments.parquet")
            
            if instrument_blob.exists():
                available_dates.append(date_str)
            else:
                missing_dates.append(date_str)
            
            current_date += timedelta(days=1)
        
        return {
            "available_dates": available_dates,
            "missing_dates": missing_dates,
            "coverage_percentage": (len(available_dates) / (len(available_dates) + len(missing_dates))) * 100 if (available_dates or missing_dates) else 0
        }
