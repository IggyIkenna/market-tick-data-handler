"""
Candle Uploader for BigQuery

Uploads processed candle data from GCS parquet files to BigQuery for analytics.
Supports both backfill mode (historical) and daily mode (incremental).
"""

import logging
import pandas as pd
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import asyncio

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from ..data_client.data_client import DataClient

logger = logging.getLogger(__name__)

@dataclass
class UploadConfig:
    """Configuration for BigQuery uploads"""
    project_id: str
    dataset_id: str = "market_data_candles"
    timeframes: List[str] = None
    batch_size: int = 1000
    max_retries: int = 3
    
    def __post_init__(self):
        if self.timeframes is None:
            self.timeframes = ['15s', '1m', '5m', '15m', '1h', '4h', '24h']

class CandleUploader:
    """Uploads candle data to BigQuery"""
    
    def __init__(self, data_client: DataClient, config: UploadConfig):
        self.data_client = data_client
        self.config = config
        self.bq_client = bigquery.Client(project=config.project_id)
        
        # Table schemas for different timeframes
        self.table_schemas = self._get_table_schemas()
    
    async def upload_day(
        self, 
        date: datetime,
        timeframes: Optional[List[str]] = None,
        overwrite: bool = False
    ) -> Dict[str, Any]:
        """
        Upload candle data for a specific day
        
        Args:
            date: Date to upload (UTC)
            timeframes: List of timeframes to upload (defaults to config)
            overwrite: Whether to overwrite existing data
            
        Returns:
            Dictionary with upload results
        """
        logger.info(f"📤 Uploading candle data for {date.strftime('%Y-%m-%d')}")
        
        timeframes = timeframes or self.config.timeframes
        results = {
            'date': date.strftime('%Y-%m-%d'),
            'timeframes': {},
            'errors': []
        }
        
        try:
            # Ensure dataset exists
            await self._ensure_dataset_exists()
            
            # Upload each timeframe
            for timeframe in timeframes:
                try:
                    upload_result = await self._upload_timeframe(date, timeframe, overwrite)
                    results['timeframes'][timeframe] = upload_result
                    
                    logger.info(f"✅ Uploaded {timeframe} candles for {date.strftime('%Y-%m-%d')}")
                    
                except Exception as e:
                    error_msg = f"Failed to upload {timeframe} candles: {e}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)
            
            return results
            
        except Exception as e:
            error_msg = f"Failed to upload day {date.strftime('%Y-%m-%d')}: {e}"
            logger.error(error_msg)
            results['errors'].append(error_msg)
            return results
    
    async def upload_date_range(
        self, 
        start_date: datetime, 
        end_date: datetime,
        timeframes: Optional[List[str]] = None,
        overwrite: bool = False
    ) -> Dict[str, Any]:
        """
        Upload candle data for a date range
        
        Args:
            start_date: Start date (UTC)
            end_date: End date (UTC)
            timeframes: List of timeframes to upload
            overwrite: Whether to overwrite existing data
            
        Returns:
            Dictionary with upload results
        """
        logger.info(f"📤 Uploading candle data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        timeframes = timeframes or self.config.timeframes
        results = {
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
            'days_processed': 0,
            'total_errors': 0,
            'timeframes': {tf: {'total_rows': 0, 'errors': 0} for tf in timeframes}
        }
        
        current_date = start_date
        while current_date <= end_date:
            try:
                day_result = await self.upload_day(current_date, timeframes, overwrite)
                
                results['days_processed'] += 1
                
                # Aggregate results
                for timeframe in timeframes:
                    if timeframe in day_result['timeframes']:
                        tf_result = day_result['timeframes'][timeframe]
                        results['timeframes'][timeframe]['total_rows'] += tf_result.get('rows_uploaded', 0)
                        results['timeframes'][timeframe]['errors'] += len(tf_result.get('errors', []))
                
                results['total_errors'] += len(day_result.get('errors', []))
                
            except Exception as e:
                logger.error(f"Failed to upload day {current_date.strftime('%Y-%m-%d')}: {e}")
                results['total_errors'] += 1
            
            # Move to next day
            from datetime import timedelta
            current_date += timedelta(days=1)
        
        logger.info(f"✅ Upload completed: {results['days_processed']} days processed, {results['total_errors']} errors")
        return results
    
    async def _upload_timeframe(
        self, 
        date: datetime, 
        timeframe: str, 
        overwrite: bool
    ) -> Dict[str, Any]:
        """Upload candles for a specific timeframe"""
        
        date_str = date.strftime('%Y-%m-%d')
        table_id = f"{self.config.project_id}.{self.config.dataset_id}.candles_{timeframe.replace('m', 'min').replace('h', 'hour').replace('s', 'sec')}"
        
        # Get all candle files for this timeframe and date
        candle_files = await self._get_candle_files(date, timeframe)
        
        if not candle_files:
            return {
                'timeframe': timeframe,
                'rows_uploaded': 0,
                'files_processed': 0,
                'errors': []
            }
        
        # Load and combine all candle data
        all_candles = []
        errors = []
        
        for file_info in candle_files:
            try:
                df = self.data_client.read_parquet_file(file_info['blob_name'])
                
                if not df.empty:
                    # Add metadata columns
                    df['date'] = date_str
                    df['timeframe'] = timeframe
                    df['instrument_id'] = file_info['instrument_id']
                    
                    all_candles.append(df)
                    
            except Exception as e:
                error_msg = f"Failed to read {file_info['blob_name']}: {e}"
                logger.warning(error_msg)
                errors.append(error_msg)
        
        if not all_candles:
            return {
                'timeframe': timeframe,
                'rows_uploaded': 0,
                'files_processed': len(candle_files),
                'errors': errors
            }
        
        # Combine all data
        combined_df = pd.concat(all_candles, ignore_index=True)
        
        # Ensure timestamp column is properly formatted
        if 'timestamp' in combined_df.columns:
            combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'])
        
        # Upload to BigQuery
        rows_uploaded = await self._upload_dataframe_to_bigquery(
            combined_df, table_id, overwrite
        )
        
        return {
            'timeframe': timeframe,
            'rows_uploaded': rows_uploaded,
            'files_processed': len(candle_files),
            'errors': errors
        }
    
    async def _get_candle_files(self, date: datetime, timeframe: str) -> List[Dict[str, Any]]:
        """Get list of candle files for a specific date and timeframe"""
        
        date_str = date.strftime('%Y-%m-%d')
        blob_pattern = f"processed_candles/by_date/day-{date_str}/timeframe-{timeframe}/"
        
        # List all blobs matching the pattern
        blobs = list(self.data_client.client.list_blobs(
            self.data_client.gcs_bucket, 
            prefix=blob_pattern
        ))
        
        files = []
        for blob in blobs:
            if blob.name.endswith('.parquet'):
                # Extract instrument ID from filename
                filename = blob.name.split('/')[-1]
                instrument_id = filename.replace('.parquet', '')
                
                files.append({
                    'blob_name': blob.name,
                    'instrument_id': instrument_id,
                    'file_size': blob.size or 0
                })
        
        return files
    
    async def _upload_dataframe_to_bigquery(
        self, 
        df: pd.DataFrame, 
        table_id: str, 
        overwrite: bool
    ) -> int:
        """Upload DataFrame to BigQuery table"""
        
        # Ensure table exists with proper schema (historical/batch mode)
        await self._ensure_table_exists(table_id, df.columns.tolist(), is_streaming=False)
        
        # Configure job
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE" if overwrite else "WRITE_APPEND",
            create_disposition="CREATE_IF_NEEDED",
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
            ]
        )
        
        # Upload in batches
        total_rows = 0
        batch_size = self.config.batch_size
        
        for i in range(0, len(df), batch_size):
            batch_df = df.iloc[i:i + batch_size]
            
            try:
                job = self.bq_client.load_table_from_dataframe(
                    batch_df, table_id, job_config=job_config
                )
                
                # Wait for job to complete
                job.result()
                total_rows += len(batch_df)
                
                logger.info(f"📤 Uploaded batch {i//batch_size + 1}: {len(batch_df)} rows to {table_id}")
                
            except Exception as e:
                logger.error(f"Failed to upload batch {i//batch_size + 1} to {table_id}: {e}")
                raise
        
        return total_rows
    
    async def _ensure_dataset_exists(self):
        """Ensure BigQuery dataset exists"""
        
        dataset_id = f"{self.config.project_id}.{self.config.dataset_id}"
        
        try:
            self.bq_client.get_dataset(dataset_id)
            logger.info(f"✅ Dataset {dataset_id} exists")
        except NotFound:
            # Create dataset
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "US"  # Set location as needed
            
            dataset = self.bq_client.create_dataset(dataset, timeout=30)
            logger.info(f"✅ Created dataset {dataset_id}")
    
    async def _ensure_table_exists(self, table_id: str, columns: List[str], is_streaming: bool = False):
        """Ensure BigQuery table exists with proper schema and partitioning"""
        
        try:
            table = self.bq_client.get_table(table_id)
            logger.info(f"✅ Table {table_id} exists")
        except NotFound:
            # Create table with schema
            schema = self._get_schema_for_columns(columns)
            
            table = bigquery.Table(table_id, schema=schema)
            
            if is_streaming:
                # Live streaming tables: 5-minute partitioning with 30-day TTL
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.HOUR,  # Hourly partitions for 5min granularity
                    field="timestamp_out",
                    expiration_ms=30 * 24 * 60 * 60 * 1000  # 30 days TTL
                )
            else:
                # Historical/batch tables: daily partitioning with no TTL
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="timestamp_out"
                    # No expiration_ms = no TTL
                )
            
            # Clustering for optimal query performance
            table.clustering_fields = ["exchange", "symbol"]
            
            table = self.bq_client.create_table(table)
            logger.info(f"✅ Created table {table_id}")
            logger.info(f"  Partitioning: {'hourly (30d TTL)' if is_streaming else 'daily (no TTL)'} on timestamp_out")
            logger.info(f"  Clustering: exchange, symbol")
    
    def _get_schema_for_columns(self, columns: List[str]) -> List[bigquery.SchemaField]:
        """Get BigQuery schema for given columns"""
        
        schema = []
        
        # Define column types
        column_types = {
            'symbol': 'STRING',
            'exchange': 'STRING',
            'timeframe': 'STRING',
            'timestamp': 'TIMESTAMP',
            'timestamp_out': 'TIMESTAMP',
            'date': 'DATE',
            'instrument_id': 'STRING',
            'open': 'FLOAT',
            'high': 'FLOAT',
            'low': 'FLOAT',
            'close': 'FLOAT',
            'volume': 'FLOAT',
            'trade_count': 'INTEGER',
            'vwap': 'FLOAT'
        }
        
        # Add HFT feature columns
        hft_columns = [
            'buy_volume_sum', 'sell_volume_sum', 'size_avg', 'price_vwap',
            'delay_median', 'delay_max', 'delay_min', 'delay_mean',
            'liquidation_buy_volume', 'liquidation_sell_volume', 'liquidation_count',
            'funding_rate', 'index_price', 'mark_price', 'open_interest', 'predicted_funding_rate',
            'oi_change', 'liquidation_with_rising_oi', 'liquidation_with_falling_oi',
            'skew_25d_put_call_ratio', 'atm_mark_iv'
        ]
        
        for col in hft_columns:
            column_types[col] = 'FLOAT'
        
        # Add book snapshot columns
        for level in range(1, 6):  # 5 levels
            column_types[f'bid_price_{level}'] = 'FLOAT'
            column_types[f'bid_volume_{level}'] = 'FLOAT'
            column_types[f'ask_price_{level}'] = 'FLOAT'
            column_types[f'ask_volume_{level}'] = 'FLOAT'
            column_types[f'bid_distance_{level}'] = 'FLOAT'
            column_types[f'ask_distance_{level}'] = 'FLOAT'
            column_types[f'bid_volume_ratio_{level}'] = 'FLOAT'
            column_types[f'ask_volume_ratio_{level}'] = 'FLOAT'
        
        # Add derived book features
        book_features = [
            'mid_price', 'spread_abs', 'spread_bps', 'bid_ask_ratio',
            'total_bid_volume', 'total_ask_volume', 'volume_imbalance',
            'bid_vwap', 'ask_vwap', 'vwap_spread',
            'bid_levels_filled', 'ask_levels_filled', 'book_imbalance'
        ]
        
        for col in book_features:
            column_types[col] = 'FLOAT'
        
        # Create schema fields
        for col in columns:
            field_type = column_types.get(col, 'STRING')
            schema.append(bigquery.SchemaField(col, field_type))
        
        return schema
    
    def _get_table_schemas(self) -> Dict[str, List[bigquery.SchemaField]]:
        """Get predefined table schemas for different timeframes"""
        # This could be expanded with specific schemas per timeframe
        return {}
