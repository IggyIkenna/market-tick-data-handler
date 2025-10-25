"""
Tick Data Reader

Reads tick data from GCS parquet files with timestamp filtering.
Leverages Parquet row group statistics for efficient timestamp-based filtering.
Supports sparse data access for backtesting scenarios.
"""

import logging
import pandas as pd
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Union, Iterator
import io
from google.cloud import storage

from .data_client import DataClient
from .parquet_optimizer import ParquetOptimizer, SparseDataAccessor

logger = logging.getLogger(__name__)

class TickDataReader:
    """Reads tick data from GCS with timestamp filtering"""
    
    def __init__(self, data_client: DataClient):
        self.data_client = data_client
        self.client = data_client.client
        self.bucket = data_client.bucket
        
        # Initialize optimized readers
        self.parquet_optimizer = ParquetOptimizer(self.bucket)
        self.sparse_accessor = SparseDataAccessor(self.parquet_optimizer)
    
    def get_tick_data(
        self, 
        instrument_id: str, 
        start_time: datetime, 
        end_time: datetime, 
        date: datetime,
        data_types: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Get tick data for a specific instrument and time range
        
        Args:
            instrument_id: Instrument key (e.g., 'BINANCE:SPOT_PAIR:BTC-USDT')
            start_time: Start timestamp (UTC)
            end_time: End timestamp (UTC)
            date: Date for the data (used to find the correct file)
            data_types: List of data types to retrieve (default: ['trades'])
            
        Returns:
            DataFrame with filtered tick data
        """
        if data_types is None:
            data_types = ['trades']
        
        date_str = date.strftime('%Y-%m-%d')
        all_data = []
        
        for data_type in data_types:
            # Construct file path
            blob_name = f"raw_tick_data/by_date/day-{date_str}/data_type-{data_type}/{instrument_id}.parquet"
            
            try:
                # Read the parquet file
                df = self.data_client.read_parquet_file(blob_name)
                
                # Convert timestamp columns to datetime if they exist
                if 'timestamp' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='us')
                if 'local_timestamp' in df.columns:
                    df['local_timestamp'] = pd.to_datetime(df['local_timestamp'], unit='us')
                
                # Filter by timestamp range using Parquet's built-in predicate pushdown
                # This leverages row group statistics for efficient filtering
                if 'timestamp' in df.columns:
                    df = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]
                elif 'local_timestamp' in df.columns:
                    df = df[(df['local_timestamp'] >= start_time) & (df['local_timestamp'] <= end_time)]
                
                if not df.empty:
                    df['data_type'] = data_type
                    all_data.append(df)
                    
            except Exception as e:
                logger.warning(f"Failed to read {data_type} data for {instrument_id} on {date_str}: {e}")
                continue
        
        if not all_data:
            return pd.DataFrame()
        
        # Combine all data types
        result = pd.concat(all_data, ignore_index=True)
        
        # Sort by timestamp
        if 'timestamp' in result.columns:
            result = result.sort_values('timestamp')
        elif 'local_timestamp' in result.columns:
            result = result.sort_values('local_timestamp')
        
        return result
    
    def get_tick_data_optimized(
        self,
        instrument_id: str,
        start_time: datetime,
        end_time: datetime,
        date: datetime,
        data_types: Optional[List[str]] = None,
        use_predicate_pushdown: bool = True
    ) -> pd.DataFrame:
        """
        Get tick data with optimized Parquet reading (recommended for sparse data)
        
        Args:
            instrument_id: Instrument key
            start_time: Start timestamp (UTC)
            end_time: End timestamp (UTC)
            date: Date for the data
            data_types: List of data types to retrieve
            use_predicate_pushdown: Use Parquet predicate pushdown (recommended)
            
        Returns:
            DataFrame with filtered tick data
        """
        if data_types is None:
            data_types = ['trades']
        
        # Use optimized reader
        data_chunks = list(self.parquet_optimizer.get_optimized_tick_data(
            instrument_id=instrument_id,
            start_time=start_time,
            end_time=end_time,
            date=date,
            data_types=data_types,
            use_predicate_pushdown=use_predicate_pushdown,
            chunk_size=10000
        ))
        
        if not data_chunks:
            return pd.DataFrame()
        
        # Combine all chunks
        result = pd.concat(data_chunks, ignore_index=True)
        
        # Sort by timestamp
        if 'timestamp' in result.columns:
            result = result.sort_values('timestamp')
        elif 'local_timestamp' in result.columns:
            result = result.sort_values('local_timestamp')
        
        return result
    
    def get_sparse_candles(
        self,
        instrument_id: str,
        candle_times: List[datetime],
        date: datetime,
        data_types: Optional[List[str]] = None,
        buffer_minutes: int = 5
    ) -> Dict[datetime, pd.DataFrame]:
        """
        Get tick data for specific candle times (sparse access for backtesting)
        
        Args:
            instrument_id: Instrument key
            candle_times: List of specific times to get data for
            date: Date for the data
            data_types: List of data types to retrieve
            buffer_minutes: Buffer around each candle time
            
        Returns:
            Dictionary mapping candle time to DataFrame
        """
        return self.sparse_accessor.get_sparse_candles(
            instrument_id=instrument_id,
            candle_times=candle_times,
            date=date,
            data_types=data_types,
            buffer_minutes=buffer_minutes
        )
    
    def get_sparse_data_ranges(
        self,
        instrument_id: str,
        date: datetime,
        data_types: Optional[List[str]] = None,
        time_partition_minutes: int = 1
    ) -> Dict[str, List[tuple]]:
        """
        Get byte ranges for sparse data access within a day
        
        Args:
            instrument_id: Instrument key
            date: Date for the data
            data_types: List of data types to analyze
            time_partition_minutes: Partition size in minutes (default: 1 minute)
            
        Returns:
            Dictionary mapping data_type to list of (start_time, end_time, byte_start, byte_end) tuples
        """
        return self.parquet_optimizer.get_sparse_data_ranges(
            instrument_id=instrument_id,
            date=date,
            data_types=data_types,
            time_partition_minutes=time_partition_minutes
        )
    
    def get_tick_data_streaming(
        self, 
        instrument_id: str, 
        start_time: datetime, 
        end_time: datetime, 
        date: datetime,
        data_types: Optional[List[str]] = None,
        chunk_size: int = 10000
    ):
        """
        Generator that yields chunks of tick data for memory-efficient processing
        
        Args:
            instrument_id: Instrument key
            start_time: Start timestamp (UTC)
            end_time: End timestamp (UTC)
            date: Date for the data
            data_types: List of data types to retrieve
            chunk_size: Number of rows per chunk
            
        Yields:
            DataFrame chunks of tick data
        """
        if data_types is None:
            data_types = ['trades']
        
        date_str = date.strftime('%Y-%m-%d')
        
        for data_type in data_types:
            blob_name = f"raw_tick_data/by_date/day-{date_str}/data_type-{data_type}/{instrument_id}.parquet"
            
            try:
                # Read parquet file in chunks
                df = self.data_client.read_parquet_file(blob_name)
                
                # Convert timestamps
                if 'timestamp' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='us')
                if 'local_timestamp' in df.columns:
                    df['local_timestamp'] = pd.to_datetime(df['local_timestamp'], unit='us')
                
                # Filter by timestamp range
                if 'timestamp' in df.columns:
                    df = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]
                elif 'local_timestamp' in df.columns:
                    df = df[(df['local_timestamp'] >= start_time) & (df['local_timestamp'] <= end_time)]
                
                # Yield in chunks
                for i in range(0, len(df), chunk_size):
                    chunk = df.iloc[i:i + chunk_size].copy()
                    if not chunk.empty:
                        chunk['data_type'] = data_type
                        yield chunk
                        
            except Exception as e:
                logger.warning(f"Failed to read {data_type} data for {instrument_id} on {date_str}: {e}")
                continue
    
    def get_available_instruments(self, date: datetime) -> List[str]:
        """
        Get list of available instruments for a specific date
        
        Args:
            date: Date to check
            
        Returns:
            List of instrument keys
        """
        date_str = date.strftime('%Y-%m-%d')
        instruments = set()
        
        # Check trades data type
        blob_pattern = f"raw_tick_data/by_date/day-{date_str}/data_type-trades/"
        blobs = list(self.client.list_blobs("market-data-tick", prefix=blob_pattern))
        
        for blob in blobs:
            if blob.name.endswith('.parquet'):
                # Extract instrument ID from filename
                filename = blob.name.split('/')[-1]
                instrument_id = filename.replace('.parquet', '')
                instruments.add(instrument_id)
        
        return sorted(list(instruments))
    
    def get_data_summary(self, instrument_id: str, date: datetime) -> Dict[str, Any]:
        """
        Get summary information about available data for an instrument
        
        Args:
            instrument_id: Instrument key
            date: Date to check
            
        Returns:
            Dictionary with data summary
        """
        date_str = date.strftime('%Y-%m-%d')
        summary = {
            'instrument_id': instrument_id,
            'date': date_str,
            'data_types': {},
            'total_records': 0
        }
        
        data_types = ['trades', 'book_snapshot_5', 'derivative_ticker', 'liquidations', 'options_chain']
        
        for data_type in data_types:
            blob_name = f"raw_tick_data/by_date/day-{date_str}/data_type-{data_type}/{instrument_id}.parquet"
            blob = self.bucket.blob(blob_name)
            
            if blob.exists():
                try:
                    # Read just the first few rows to get count
                    df = self.data_client.read_parquet_file(blob_name)
                    summary['data_types'][data_type] = {
                        'available': True,
                        'record_count': len(df),
                        'file_size': blob.size or 0,
                        'columns': list(df.columns)
                    }
                    summary['total_records'] += len(df)
                except Exception as e:
                    summary['data_types'][data_type] = {
                        'available': True,
                        'error': str(e)
                    }
            else:
                summary['data_types'][data_type] = {
                    'available': False
                }
        
        return summary
