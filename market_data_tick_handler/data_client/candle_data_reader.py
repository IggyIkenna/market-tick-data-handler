"""
Candle Data Reader

Reads processed candle data from GCS parquet files.
Supports all timeframes: 15s, 1m, 5m, 15m, 1h, 4h, 24h
"""

import logging
import pandas as pd
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from .data_client import DataClient

logger = logging.getLogger(__name__)

class CandleDataReader:
    """Reads candle data from GCS across all timeframes"""
    
    def __init__(self, data_client: DataClient):
        self.data_client = data_client
        self.client = data_client.client
        self.bucket = data_client.bucket
        
        # Supported timeframes
        self.timeframes = ['15s', '1m', '5m', '15m', '1h', '4h', '24h']
    
    def get_candles(
        self, 
        instrument_id: str, 
        timeframe: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> pd.DataFrame:
        """
        Get candle data for a specific instrument and timeframe using optimized Parquet queries
        
        Args:
            instrument_id: Instrument key (e.g., 'BINANCE:SPOT_PAIR:BTC-USDT')
            timeframe: Candle timeframe ('15s', '1m', '5m', '15m', '1h', '4h', '24h')
            start_date: Start date (UTC)
            end_date: End date (UTC)
            
        Returns:
            DataFrame with candle data
        """
        if timeframe not in self.timeframes:
            raise ValueError(f"Unsupported timeframe: {timeframe}. Supported: {self.timeframes}")
        
        # Use ParquetOptimizer for efficient timestamp-based queries
        from .parquet_optimizer import ParquetOptimizer
        optimizer = ParquetOptimizer(self.data_client.client, self.data_client.gcs_bucket)
        
        all_candles = []
        current_date = start_date
        
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            
            # Construct file path based on GCS structure
            # Pattern: processed_candles/by_date/day-{date}/timeframe-{timeframe}/{instrument_id}.parquet
            blob_name = f"processed_candles/by_date/day-{date_str}/timeframe-{timeframe}/{instrument_id}.parquet"
            
            try:
                # Use optimized Parquet reading with predicate pushdown
                df = self._read_candles_with_optimization(blob_name, start_date, end_date)
                
                if not df.empty:
                    all_candles.append(df)
                    
            except Exception as e:
                logger.warning(f"Failed to read {timeframe} candles for {instrument_id} on {date_str}: {e}")
                continue
            
            # Move to next day
            from datetime import timedelta
            current_date += timedelta(days=1)
        
        if not all_candles:
            return pd.DataFrame()
        
        # Combine all days
        result = pd.concat(all_candles, ignore_index=True)
        
        # Sort by timestamp
        if 'timestamp' in result.columns:
            result = result.sort_values('timestamp')
        
        return result
    
    def _read_candles_with_optimization(self, blob_name: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Read candles with Parquet optimization for efficient timestamp filtering"""
        import pyarrow.parquet as pq
        import pyarrow as pa
        
        try:
            # Download the Parquet file
            blob = self.bucket.blob(blob_name)
            parquet_data = blob.download_as_bytes()
            
            # Create PyArrow file from bytes
            parquet_file = pa.BufferReader(parquet_data)
            parquet_table = pq.ParquetFile(parquet_file)
            
            # Get metadata for row group statistics
            metadata = parquet_table.metadata
            
            # Convert timestamps to microseconds for comparison
            start_us = int(start_date.timestamp() * 1_000_000)
            end_us = int(end_date.timestamp() * 1_000_000)
            
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
                return pd.DataFrame()
            
            # Read only relevant row groups
            table = parquet_table.read_row_groups(relevant_row_groups)
            df = table.to_pandas()
            
            # Convert timestamp columns to datetime
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            if 'timestamp_out' in df.columns:
                df['timestamp_out'] = pd.to_datetime(df['timestamp_out'])
            
            # Final filtering by timestamp range
            if 'timestamp' in df.columns:
                df = df[(df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)]
            
            return df
            
        except Exception as e:
            logger.error(f"Error reading optimized candles from {blob_name}: {e}")
            return pd.DataFrame()
    
    def get_candles_for_timeframe(
        self, 
        instrument_id: str, 
        timeframe: str, 
        date: datetime
    ) -> pd.DataFrame:
        """
        Get candle data for a specific instrument, timeframe, and single date
        
        Args:
            instrument_id: Instrument key
            timeframe: Candle timeframe
            date: Specific date
            
        Returns:
            DataFrame with candle data for that date
        """
        date_str = date.strftime('%Y-%m-%d')
        blob_name = f"processed_candles/by_date/day-{date_str}/timeframe-{timeframe}/{instrument_id}.parquet"
        
        try:
            df = self.data_client.read_parquet_file(blob_name)
            
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            return df
            
        except Exception as e:
            logger.warning(f"Failed to read {timeframe} candles for {instrument_id} on {date_str}: {e}")
            return pd.DataFrame()
    
    def get_available_timeframes(self, instrument_id: str, date: datetime) -> List[str]:
        """
        Get list of available timeframes for a specific instrument and date
        
        Args:
            instrument_id: Instrument key
            date: Date to check
            
        Returns:
            List of available timeframes
        """
        date_str = date.strftime('%Y-%m-%d')
        available_timeframes = []
        
        for timeframe in self.timeframes:
            blob_name = f"processed_candles/by_date/day-{date_str}/timeframe-{timeframe}/{instrument_id}.parquet"
            blob = self.bucket.blob(blob_name)
            
            if blob.exists():
                available_timeframes.append(timeframe)
        
        return available_timeframes
    
    def get_candle_summary(self, instrument_id: str, date: datetime) -> Dict[str, Any]:
        """
        Get summary information about available candle data for an instrument
        
        Args:
            instrument_id: Instrument key
            date: Date to check
            
        Returns:
            Dictionary with candle data summary
        """
        date_str = date.strftime('%Y-%m-%d')
        summary = {
            'instrument_id': instrument_id,
            'date': date_str,
            'timeframes': {},
            'total_candles': 0
        }
        
        for timeframe in self.timeframes:
            blob_name = f"processed_candles/by_date/day-{date_str}/timeframe-{timeframe}/{instrument_id}.parquet"
            blob = self.bucket.blob(blob_name)
            
            if blob.exists():
                try:
                    df = self.data_client.read_parquet_file(blob_name)
                    summary['timeframes'][timeframe] = {
                        'available': True,
                        'candle_count': len(df),
                        'file_size': blob.size or 0,
                        'columns': list(df.columns),
                        'time_range': {
                            'start': df['timestamp'].min() if 'timestamp' in df.columns else None,
                            'end': df['timestamp'].max() if 'timestamp' in df.columns else None
                        }
                    }
                    summary['total_candles'] += len(df)
                except Exception as e:
                    summary['timeframes'][timeframe] = {
                        'available': True,
                        'error': str(e)
                    }
            else:
                summary['timeframes'][timeframe] = {
                    'available': False
                }
        
        return summary
    
    def get_multiple_timeframes(
        self, 
        instrument_id: str, 
        timeframes: List[str], 
        start_date: datetime, 
        end_date: datetime
    ) -> Dict[str, pd.DataFrame]:
        """
        Get candle data for multiple timeframes at once
        
        Args:
            instrument_id: Instrument key
            timeframes: List of timeframes to retrieve
            start_date: Start date
            end_date: End date
            
        Returns:
            Dictionary mapping timeframe to DataFrame
        """
        result = {}
        
        for timeframe in timeframes:
            try:
                df = self.get_candles(instrument_id, timeframe, start_date, end_date)
                result[timeframe] = df
            except Exception as e:
                logger.warning(f"Failed to get {timeframe} candles for {instrument_id}: {e}")
                result[timeframe] = pd.DataFrame()
        
        return result
