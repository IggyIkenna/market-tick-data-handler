"""
Parquet Optimization for Sparse Data Access

This module provides optimized Parquet reading with:
- Row group statistics for predicate pushdown
- Time-based partitioning within daily files
- Byte-range indexing for sparse data access
- Memory-efficient streaming for large datasets
"""

import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Tuple, Iterator
import io
from google.cloud import storage
from google.cloud.exceptions import NotFound

logger = logging.getLogger(__name__)


class ParquetOptimizer:
    """Optimized Parquet reader with sparse data support"""
    
    def __init__(self, bucket: storage.Bucket):
        self.bucket = bucket
        self._metadata_cache = {}
    
    def get_optimized_tick_data(
        self,
        instrument_id: str,
        start_time: datetime,
        end_time: datetime,
        date: datetime,
        data_types: Optional[List[str]] = None,
        use_predicate_pushdown: bool = True,
        chunk_size: int = 10000
    ) -> Iterator[pd.DataFrame]:
        """
        Get tick data with optimized Parquet reading
        
        Args:
            instrument_id: Instrument key
            start_time: Start timestamp (UTC)
            end_time: End timestamp (UTC)
            date: Date for the data
            data_types: List of data types to retrieve
            use_predicate_pushdown: Use Parquet predicate pushdown (recommended)
            chunk_size: Number of rows per chunk for streaming
            
        Yields:
            DataFrame chunks of filtered tick data
        """
        if data_types is None:
            data_types = ['trades']
        
        date_str = date.strftime('%Y-%m-%d')
        
        for data_type in data_types:
            blob_name = f"raw_tick_data/by_date/day-{date_str}/data_type-{data_type}/{instrument_id}.parquet"
            
            try:
                if use_predicate_pushdown:
                    yield from self._read_with_predicate_pushdown(
                        blob_name, start_time, end_time, data_type, chunk_size
                    )
                else:
                    yield from self._read_with_pandas_filtering(
                        blob_name, start_time, end_time, data_type, chunk_size
                    )
                    
            except Exception as e:
                logger.warning(f"Failed to read {data_type} data for {instrument_id} on {date_str}: {e}")
                continue
    
    def _read_with_predicate_pushdown(
        self,
        blob_name: str,
        start_time: datetime,
        end_time: datetime,
        data_type: str,
        chunk_size: int
    ) -> Iterator[pd.DataFrame]:
        """Read with Parquet predicate pushdown for optimal performance"""
        
        # Download the Parquet file
        blob = self.bucket.blob(blob_name)
        parquet_data = blob.download_as_bytes()
        
        # Create PyArrow file from bytes
        parquet_file = pa.BufferReader(parquet_data)
        parquet_table = pq.ParquetFile(parquet_file)
        
        # Get metadata for row group statistics
        metadata = parquet_table.metadata
        schema = parquet_table.schema
        
        # Find timestamp column
        timestamp_col = None
        for i, field in enumerate(schema):
            if field.name in ['timestamp', 'local_timestamp']:
                timestamp_col = field.name
                break
        
        if not timestamp_col:
            logger.warning(f"No timestamp column found in {blob_name}")
            return
        
        # Convert timestamps to microseconds for comparison
        start_us = int(start_time.timestamp() * 1_000_000)
        end_us = int(end_time.timestamp() * 1_000_000)
        
        # Filter row groups using metadata statistics
        relevant_row_groups = []
        for i in range(metadata.num_row_groups):
            row_group = metadata.row_group(i)
            col_stats = row_group.column(0)  # Assuming timestamp is first column
            
            if col_stats.statistics:
                min_val = col_stats.statistics.min
                max_val = col_stats.statistics.max
                
                # Check if this row group overlaps with our time range
                if not (max_val < start_us or min_val > end_us):
                    relevant_row_groups.append(i)
        
        if not relevant_row_groups:
            logger.info(f"No relevant row groups found for time range {start_time} to {end_time}")
            return
        
        # Read only relevant row groups
        for row_group_idx in relevant_row_groups:
            table = parquet_table.read_row_group(row_group_idx)
            df = table.to_pandas()
            
            # Convert timestamp columns
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='us')
            if 'local_timestamp' in df.columns:
                df['local_timestamp'] = pd.to_datetime(df['local_timestamp'], unit='us')
            
            # Apply final timestamp filter
            if 'timestamp' in df.columns:
                df = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]
            elif 'local_timestamp' in df.columns:
                df = df[(df['local_timestamp'] >= start_time) & (df['local_timestamp'] <= end_time)]
            
            if not df.empty:
                df['data_type'] = data_type
                
                # Yield in chunks
                for i in range(0, len(df), chunk_size):
                    chunk = df.iloc[i:i + chunk_size].copy()
                    if not chunk.empty:
                        yield chunk
    
    def _read_with_pandas_filtering(
        self,
        blob_name: str,
        start_time: datetime,
        end_time: datetime,
        data_type: str,
        chunk_size: int
    ) -> Iterator[pd.DataFrame]:
        """Fallback method using pandas filtering"""
        
        # Download and read the entire file
        blob = self.bucket.blob(blob_name)
        parquet_data = blob.download_as_bytes()
        
        df = pd.read_parquet(io.BytesIO(parquet_data))
        
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
        
        if not df.empty:
            df['data_type'] = data_type
            
            # Yield in chunks
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i:i + chunk_size].copy()
                if not chunk.empty:
                    yield chunk
    
    def get_sparse_data_ranges(
        self,
        instrument_id: str,
        date: datetime,
        data_types: Optional[List[str]] = None,
        time_partition_minutes: int = 1
    ) -> Dict[str, List[Tuple[datetime, datetime, int, int]]]:
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
        if data_types is None:
            data_types = ['trades']
        
        date_str = date.strftime('%Y-%m-%d')
        ranges = {}
        
        for data_type in data_types:
            blob_name = f"raw_tick_data/by_date/day-{date_str}/data_type-{data_type}/{instrument_id}.parquet"
            
            try:
                blob = self.bucket.blob(blob_name)
                parquet_data = blob.download_as_bytes()
                
                # Create PyArrow file
                parquet_file = pa.BufferReader(parquet_data)
                parquet_table = pq.ParquetFile(parquet_file)
                
                # Get metadata
                metadata = parquet_table.metadata
                schema = parquet_table.schema
                
                # Find timestamp column
                timestamp_col = None
                for i, field in enumerate(schema):
                    if field.name in ['timestamp', 'local_timestamp']:
                        timestamp_col = field.name
                        break
                
                if not timestamp_col:
                    continue
                
                # Calculate time partitions
                day_start = datetime.combine(date, datetime.min.time()).replace(tzinfo=timezone.utc)
                partitions = []
                
                for i in range(0, 24 * 60, time_partition_minutes):
                    partition_start = day_start + timedelta(minutes=i)
                    partition_end = partition_start + timedelta(minutes=time_partition_minutes)
                    
                    # Find row groups that overlap with this partition
                    start_us = int(partition_start.timestamp() * 1_000_000)
                    end_us = int(partition_end.timestamp() * 1_000_000)
                    
                    relevant_row_groups = []
                    for j in range(metadata.num_row_groups):
                        row_group = metadata.row_group(j)
                        col_stats = row_group.column(0)
                        
                        if col_stats.statistics:
                            min_val = col_stats.statistics.min
                            max_val = col_stats.statistics.max
                            
                            if not (max_val < start_us or min_val > end_us):
                                relevant_row_groups.append(j)
                    
                    if relevant_row_groups:
                        # Calculate byte range for these row groups
                        byte_start = min(metadata.row_group(i).total_byte_size for i in relevant_row_groups)
                        byte_end = max(metadata.row_group(i).total_byte_size for i in relevant_row_groups)
                        
                        partitions.append((partition_start, partition_end, byte_start, byte_end))
                
                ranges[data_type] = partitions
                
            except Exception as e:
                logger.warning(f"Failed to analyze {data_type} data for {instrument_id} on {date_str}: {e}")
                continue
        
        return ranges
    
    def create_optimized_parquet(
        self,
        df: pd.DataFrame,
        output_path: str,
        partition_by_minutes: int = 1,
        row_group_size: int = 100000
    ) -> None:
        """
        Create optimized Parquet file with time-based partitioning and row groups
        
        Args:
            df: DataFrame to save
            output_path: Output file path
            partition_by_minutes: Partition size in minutes
            row_group_size: Target row group size
        """
        if 'timestamp' not in df.columns:
            raise ValueError("DataFrame must have 'timestamp' column for time-based partitioning")
        
        # Convert timestamp to datetime if needed
        if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='us')
        
        # Sort by timestamp for optimal row group distribution
        df = df.sort_values('timestamp')
        
        # Create PyArrow table
        table = pa.Table.from_pandas(df)
        
        # Configure Parquet writer with optimization
        pq.write_table(
            table,
            output_path,
            compression='snappy',
            use_dictionary=True,
            row_group_size=row_group_size,
            data_page_size=1024 * 1024,  # 1MB data pages
            write_statistics=True,  # Enable statistics for predicate pushdown
            use_deprecated_int96_timestamps=False
        )
        
        logger.info(f"Created optimized Parquet file: {output_path}")


class SparseDataAccessor:
    """High-level interface for sparse data access"""
    
    def __init__(self, parquet_optimizer: ParquetOptimizer):
        self.optimizer = parquet_optimizer
        self._range_cache = {}
    
    def get_sparse_candles(
        self,
        instrument_id: str,
        candle_times: List[datetime],
        date: datetime,
        data_types: Optional[List[str]] = None,
        buffer_minutes: int = 5
    ) -> Dict[datetime, pd.DataFrame]:
        """
        Get tick data for specific candle times (sparse access)
        
        Args:
            instrument_id: Instrument key
            candle_times: List of specific times to get data for
            date: Date for the data
            data_types: List of data types to retrieve
            buffer_minutes: Buffer around each candle time
            
        Returns:
            Dictionary mapping candle time to DataFrame
        """
        if data_types is None:
            data_types = ['trades']
        
        results = {}
        
        for candle_time in candle_times:
            # Calculate time range with buffer
            start_time = candle_time - timedelta(minutes=buffer_minutes)
            end_time = candle_time + timedelta(minutes=buffer_minutes)
            
            # Get data for this time range
            data_chunks = list(self.optimizer.get_optimized_tick_data(
                instrument_id=instrument_id,
                start_time=start_time,
                end_time=end_time,
                date=date,
                data_types=data_types,
                use_predicate_pushdown=True,
                chunk_size=10000
            ))
            
            if data_chunks:
                results[candle_time] = pd.concat(data_chunks, ignore_index=True)
            else:
                results[candle_time] = pd.DataFrame()
        
        return results
    
    def get_candle_data_efficiently(
        self,
        instrument_id: str,
        timeframe: str,
        start_date: datetime,
        end_date: datetime,
        sparse_candles: Optional[List[datetime]] = None
    ) -> pd.DataFrame:
        """
        Get candle data efficiently, only loading data for required candles
        
        Args:
            instrument_id: Instrument key
            timeframe: Candle timeframe (e.g., '1m', '5m')
            start_date: Start date
            end_date: End date
            sparse_candles: Optional list of specific candle times to load
            
        Returns:
            DataFrame with candle data
        """
        if sparse_candles:
            # Load only specific candles
            all_data = []
            for candle_time in sparse_candles:
                data = self.get_sparse_candles(
                    instrument_id=instrument_id,
                    candle_times=[candle_time],
                    date=candle_time.date(),
                    data_types=['trades']
                )
                if candle_time in data and not data[candle_time].empty:
                    all_data.append(data[candle_time])
            
            if all_data:
                return pd.concat(all_data, ignore_index=True)
            else:
                return pd.DataFrame()
        else:
            # Load all data in the range (fallback to regular method)
            all_data = []
            current_date = start_date.date()
            end_date_only = end_date.date()
            
            while current_date <= end_date_only:
                data_chunks = list(self.optimizer.get_optimized_tick_data(
                    instrument_id=instrument_id,
                    start_time=start_date,
                    end_time=end_date,
                    date=current_date,
                    data_types=['trades'],
                    use_predicate_pushdown=True,
                    chunk_size=10000
                ))
                
                if data_chunks:
                    all_data.extend(data_chunks)
                
                current_date += timedelta(days=1)
            
            if all_data:
                return pd.concat(all_data, ignore_index=True)
            else:
                return pd.DataFrame()
