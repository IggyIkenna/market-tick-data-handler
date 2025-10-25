"""
Book Snapshot Processor

Records order book snapshots at candle intervals (15s to 24h) with both raw and derived features.
Supports both live and historical modes.
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

from ..data_client.data_client import DataClient
from ..streaming_service.tick_streamer.utc_timestamp_manager import UTCTimestampManager

logger = logging.getLogger(__name__)

@dataclass
class BookSnapshotConfig:
    """Configuration for book snapshot processing"""
    timeframes: List[str] = None
    levels: int = 5  # Number of order book levels to process
    
    def __post_init__(self):
        if self.timeframes is None:
            self.timeframes = ['15s', '1m', '5m', '15m', '1h', '4h', '24h']

class BookSnapshotProcessor:
    """Processes order book snapshots at candle intervals"""
    
    def __init__(self, data_client: DataClient, config: BookSnapshotConfig = None):
        self.data_client = data_client
        self.config = config or BookSnapshotConfig()
        self.timestamp_manager = UTCTimestampManager()
        
        # Supported timeframes
        self.supported_timeframes = ['15s', '1m', '5m', '15m', '1h', '4h', '24h']
        
        # Validate configuration
        for tf in self.config.timeframes:
            if tf not in self.supported_timeframes:
                raise ValueError(f"Book snapshot processor supports {self.supported_timeframes}, got: {tf}")
    
    async def process_day(
        self, 
        instrument_id: str, 
        date: datetime,
        output_bucket: str = None
    ) -> Dict[str, Any]:
        """
        Process a full day of book snapshots
        
        Args:
            instrument_id: Instrument key
            date: Date to process (UTC)
            output_bucket: GCS bucket for output
            
        Returns:
            Dictionary with processing results
        """
        logger.info(f"📖 Processing book snapshots for {instrument_id} on {date.strftime('%Y-%m-%d')}")
        
        output_bucket = output_bucket or self.data_client.gcs_bucket
        results = {
            'instrument_id': instrument_id,
            'date': date.strftime('%Y-%m-%d'),
            'timeframes': {},
            'errors': []
        }
        
        try:
            # Load book snapshot data for the day
            book_data = self._load_book_data(instrument_id, date)
            
            if book_data.empty:
                logger.warning(f"No book snapshot data found for {instrument_id} on {date.strftime('%Y-%m-%d')}")
                return results
            
            # Process each timeframe
            for timeframe in self.config.timeframes:
                try:
                    snapshots = self._process_timeframe(
                        book_data, instrument_id, timeframe, date
                    )
                    
                    if not snapshots.empty:
                        # Upload to GCS
                        self._upload_snapshots(snapshots, instrument_id, timeframe, date, output_bucket)
                        
                        results['timeframes'][timeframe] = {
                            'snapshot_count': len(snapshots),
                            'time_range': {
                                'start': snapshots['timestamp'].min().isoformat(),
                                'end': snapshots['timestamp'].max().isoformat()
                            }
                        }
                        
                        logger.info(f"✅ Processed {len(snapshots)} {timeframe} book snapshots for {instrument_id}")
                    else:
                        logger.warning(f"No {timeframe} book snapshots generated for {instrument_id}")
                        
                except Exception as e:
                    error_msg = f"Failed to process {timeframe} book snapshots: {e}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)
            
            return results
            
        except Exception as e:
            error_msg = f"Failed to process book snapshots for {instrument_id}: {e}"
            logger.error(error_msg)
            results['errors'].append(error_msg)
            return results
    
    async def _load_book_data(self, instrument_id: str, date: datetime) -> pd.DataFrame:
        """Load book snapshot data for a specific day"""
        date_str = date.strftime('%Y-%m-%d')
        blob_name = f"raw_tick_data/by_date/day-{date_str}/data_type-book_snapshot_5/{instrument_id}.parquet"
        
        try:
            df = self.data_client.read_parquet_file(blob_name)
            
            if not df.empty:
                # Convert timestamps
                if 'timestamp' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='us')
                if 'local_timestamp' in df.columns:
                    df['local_timestamp'] = pd.to_datetime(df['local_timestamp'], unit='us')
                
                # Sort by timestamp
                if 'timestamp' in df.columns:
                    df = df.sort_values('timestamp')
                elif 'local_timestamp' in df.columns:
                    df = df.sort_values('local_timestamp')
            
            return df
            
        except Exception as e:
            logger.warning(f"Failed to load book snapshot data for {instrument_id} on {date_str}: {e}")
            return pd.DataFrame()
    
    async def _process_timeframe(
        self, 
        book_data: pd.DataFrame, 
        instrument_id: str, 
        timeframe: str, 
        date: datetime
    ) -> pd.DataFrame:
        """Process book data into snapshots for a specific timeframe"""
        
        if book_data.empty:
            return pd.DataFrame()
        
        # Generate aligned timestamps for the day
        aligned_timestamps = self._generate_aligned_timestamps(date, timeframe)
        
        snapshots = []
        
        for i, timestamp in enumerate(aligned_timestamps):
            # Get next timestamp for filtering
            next_timestamp = aligned_timestamps[i + 1] if i + 1 < len(aligned_timestamps) else date + timedelta(days=1)
            
            # Find the closest book snapshot to this timestamp
            closest_snapshot = self._find_closest_snapshot(book_data, timestamp, next_timestamp)
            
            if closest_snapshot is not None:
                # Process the snapshot
                processed_snapshot = self._process_snapshot(
                    closest_snapshot, instrument_id, timeframe, timestamp
                )
                
                if processed_snapshot is not None:
                    snapshots.append(processed_snapshot)
            else:
                # Create empty snapshot with NaN values
                empty_snapshot = self._create_empty_snapshot(instrument_id, timeframe, timestamp)
                snapshots.append(empty_snapshot)
        
        if not snapshots:
            return pd.DataFrame()
        
        # Convert to DataFrame
        result_df = pd.DataFrame(snapshots)
        
        return result_df
    
    def _find_closest_snapshot(
        self, 
        book_data: pd.DataFrame, 
        timestamp: datetime, 
        next_timestamp: datetime
    ) -> Optional[pd.Series]:
        """Find the closest book snapshot to a timestamp"""
        
        # Filter snapshots within the interval
        interval_snapshots = book_data[
            (book_data['timestamp'] >= timestamp) & 
            (book_data['timestamp'] < next_timestamp)
        ]
        
        if interval_snapshots.empty:
            return None
        
        # Return the closest one to the target timestamp
        time_diffs = abs((interval_snapshots['timestamp'] - timestamp).dt.total_seconds())
        closest_idx = time_diffs.idxmin()
        
        return interval_snapshots.loc[closest_idx]
    
    def _process_snapshot(
        self, 
        snapshot: pd.Series, 
        instrument_id: str, 
        timeframe: str, 
        timestamp: datetime
    ) -> Optional[Dict[str, Any]]:
        """Process a single book snapshot into features"""
        
        # Extract symbol and exchange
        symbol, exchange = self._parse_instrument_id(instrument_id)
        
        # Calculate timestamp_out based on snapshot timestamp + 200ms
        snapshot_timestamp = snapshot['timestamp'] if 'timestamp' in snapshot else timestamp
        timestamp_out = self._calculate_timestamp_out(snapshot, timestamp)
        
        # Start with basic information
        processed = {
            'symbol': symbol,
            'exchange': exchange,
            'timeframe': timeframe,
            'timestamp': timestamp,
            'timestamp_out': timestamp_out,
            'snapshot_timestamp': snapshot_timestamp
        }
        
        # Extract raw features (bid/ask prices and volumes for each level)
        raw_features = self._extract_raw_features(snapshot)
        processed.update(raw_features)
        
        # Calculate derived features
        derived_features = self._calculate_derived_features(raw_features)
        processed.update(derived_features)
        
        return processed
    
    def _extract_raw_features(self, snapshot: pd.Series) -> Dict[str, float]:
        """Extract raw order book features"""
        
        features = {}
        
        # Extract bid and ask prices and volumes for each level
        for level in range(1, self.config.levels + 1):
            # Bid features
            bid_price_col = f'bid_price_{level}'
            bid_volume_col = f'bid_volume_{level}'
            
            features[bid_price_col] = snapshot.get(bid_price_col, np.nan)
            features[bid_volume_col] = snapshot.get(bid_volume_col, np.nan)
            
            # Ask features
            ask_price_col = f'ask_price_{level}'
            ask_volume_col = f'ask_volume_{level}'
            
            features[ask_price_col] = snapshot.get(ask_price_col, np.nan)
            features[ask_volume_col] = snapshot.get(ask_volume_col, np.nan)
        
        return features
    
    def _calculate_derived_features(self, raw_features: Dict[str, float]) -> Dict[str, float]:
        """Calculate derived order book features"""
        
        features = {}
        
        # Get level 1 prices and volumes
        bid_1 = raw_features.get('bid_price_1', np.nan)
        ask_1 = raw_features.get('ask_price_1', np.nan)
        bid_vol_1 = raw_features.get('bid_volume_1', np.nan)
        ask_vol_1 = raw_features.get('ask_volume_1', np.nan)
        
        # Basic price features
        if not np.isnan(bid_1) and not np.isnan(ask_1):
            features['mid_price'] = (bid_1 + ask_1) / 2
            features['spread_abs'] = ask_1 - bid_1
            features['spread_bps'] = (ask_1 - bid_1) / features['mid_price'] * 10000
            features['bid_ask_ratio'] = bid_1 / ask_1
        else:
            features['mid_price'] = np.nan
            features['spread_abs'] = np.nan
            features['spread_bps'] = np.nan
            features['bid_ask_ratio'] = np.nan
        
        # Level distance features (normalized)
        for level in range(1, self.config.levels + 1):
            bid_price = raw_features.get(f'bid_price_{level}', np.nan)
            ask_price = raw_features.get(f'ask_price_{level}', np.nan)
            
            if not np.isnan(bid_price) and not np.isnan(features['mid_price']):
                features[f'bid_distance_{level}'] = (bid_price - features['mid_price']) / features['mid_price'] * 10000
            else:
                features[f'bid_distance_{level}'] = np.nan
            
            if not np.isnan(ask_price) and not np.isnan(features['mid_price']):
                features[f'ask_distance_{level}'] = (ask_price - features['mid_price']) / features['mid_price'] * 10000
            else:
                features[f'ask_distance_{level}'] = np.nan
        
        # Volume imbalance features
        total_bid_volume = sum(raw_features.get(f'bid_volume_{level}', 0) for level in range(1, self.config.levels + 1))
        total_ask_volume = sum(raw_features.get(f'ask_volume_{level}', 0) for level in range(1, self.config.levels + 1))
        
        features['total_bid_volume'] = total_bid_volume
        features['total_ask_volume'] = total_ask_volume
        
        if total_bid_volume + total_ask_volume > 0:
            features['volume_imbalance'] = (total_bid_volume - total_ask_volume) / (total_bid_volume + total_ask_volume)
        else:
            features['volume_imbalance'] = np.nan
        
        # Volume ratios for each level
        for level in range(1, self.config.levels + 1):
            bid_volume = raw_features.get(f'bid_volume_{level}', 0)
            ask_volume = raw_features.get(f'ask_volume_{level}', 0)
            
            if total_bid_volume > 0:
                features[f'bid_volume_ratio_{level}'] = bid_volume / total_bid_volume
            else:
                features[f'bid_volume_ratio_{level}'] = np.nan
            
            if total_ask_volume > 0:
                features[f'ask_volume_ratio_{level}'] = ask_volume / total_ask_volume
            else:
                features[f'ask_volume_ratio_{level}'] = np.nan
        
        # Weighted price features
        if total_bid_volume > 0:
            bid_vwap = sum(raw_features.get(f'bid_price_{level}', 0) * raw_features.get(f'bid_volume_{level}', 0) 
                          for level in range(1, self.config.levels + 1)) / total_bid_volume
            features['bid_vwap'] = bid_vwap
        else:
            features['bid_vwap'] = np.nan
        
        if total_ask_volume > 0:
            ask_vwap = sum(raw_features.get(f'ask_price_{level}', 0) * raw_features.get(f'ask_volume_{level}', 0) 
                          for level in range(1, self.config.levels + 1)) / total_ask_volume
            features['ask_vwap'] = ask_vwap
        else:
            features['ask_vwap'] = np.nan
        
        if not np.isnan(features['bid_vwap']) and not np.isnan(features['ask_vwap']):
            features['vwap_spread'] = features['ask_vwap'] - features['bid_vwap']
        else:
            features['vwap_spread'] = np.nan
        
        # Shape features
        bid_levels_filled = sum(1 for level in range(1, self.config.levels + 1) 
                               if not np.isnan(raw_features.get(f'bid_price_{level}', np.nan)))
        ask_levels_filled = sum(1 for level in range(1, self.config.levels + 1) 
                               if not np.isnan(raw_features.get(f'ask_price_{level}', np.nan)))
        
        features['bid_levels_filled'] = bid_levels_filled
        features['ask_levels_filled'] = ask_levels_filled
        features['book_imbalance'] = (bid_levels_filled - ask_levels_filled) / self.config.levels
        
        return features
    
    def _calculate_timestamp_out(self, snapshot: pd.Series, fallback_timestamp: datetime) -> datetime:
        """Calculate timestamp_out based on snapshot timestamp + 200ms"""
        
        # Get timestamp from snapshot
        if 'timestamp' in snapshot and pd.notna(snapshot['timestamp']):
            snapshot_timestamp = snapshot['timestamp']
            
            # Convert microseconds to datetime if needed
            if isinstance(snapshot_timestamp, (int, float)):
                # Assume microseconds since epoch
                snapshot_dt = datetime.fromtimestamp(snapshot_timestamp / 1_000_000, tz=timezone.utc)
            elif isinstance(snapshot_timestamp, datetime):
                snapshot_dt = snapshot_timestamp
            else:
                # Try to convert
                snapshot_dt = pd.to_datetime(snapshot_timestamp)
            
            return snapshot_dt + timedelta(milliseconds=200)
        
        # Fallback to using fallback_timestamp + 200ms
        return fallback_timestamp + timedelta(milliseconds=200)
    
    def _create_empty_snapshot(self, instrument_id: str, timeframe: str, timestamp: datetime) -> Dict[str, Any]:
        """Create an empty snapshot with NaN values"""
        
        symbol, exchange = self._parse_instrument_id(instrument_id)
        
        # Calculate timestamp_out as timestamp + 200ms for empty snapshots
        timestamp_out = timestamp + timedelta(milliseconds=200)
        
        snapshot = {
            'symbol': symbol,
            'exchange': exchange,
            'timeframe': timeframe,
            'timestamp': timestamp,
            'timestamp_out': timestamp_out,
            'snapshot_timestamp': timestamp
        }
        
        # Add raw features with NaN values
        for level in range(1, self.config.levels + 1):
            snapshot[f'bid_price_{level}'] = np.nan
            snapshot[f'bid_volume_{level}'] = np.nan
            snapshot[f'ask_price_{level}'] = np.nan
            snapshot[f'ask_volume_{level}'] = np.nan
        
        # Add derived features with NaN values
        derived_columns = [
            'mid_price', 'spread_abs', 'spread_bps', 'bid_ask_ratio',
            'total_bid_volume', 'total_ask_volume', 'volume_imbalance',
            'bid_vwap', 'ask_vwap', 'vwap_spread',
            'bid_levels_filled', 'ask_levels_filled', 'book_imbalance'
        ]
        
        for level in range(1, self.config.levels + 1):
            derived_columns.extend([f'bid_distance_{level}', f'ask_distance_{level}'])
            derived_columns.extend([f'bid_volume_ratio_{level}', f'ask_volume_ratio_{level}'])
        
        for col in derived_columns:
            snapshot[col] = np.nan
        
        return snapshot
    
    def _parse_instrument_id(self, instrument_id: str) -> tuple[str, str]:
        """Parse instrument ID to extract symbol and exchange"""
        # Format: EXCHANGE:INSTRUMENT_TYPE:SYMBOL
        parts = instrument_id.split(':')
        if len(parts) >= 3:
            exchange = parts[0].lower()
            symbol = parts[2]
        else:
            exchange = 'unknown'
            symbol = instrument_id
        
        return symbol, exchange
    
    def _generate_aligned_timestamps(self, date: datetime, timeframe: str) -> List[datetime]:
        """Generate UTC-aligned timestamps for a day"""
        
        # Get interval in seconds
        interval_seconds = self.timestamp_manager.TIMEFRAMES.get(timeframe, 60)
        
        # Start from midnight UTC
        start_time = date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_time = start_time + timedelta(days=1)
        
        timestamps = []
        current = start_time
        
        while current < end_time:
            timestamps.append(current)
            current += timedelta(seconds=interval_seconds)
        
        return timestamps
    
    async def _upload_snapshots(
        self, 
        snapshots_df: pd.DataFrame, 
        instrument_id: str, 
        timeframe: str, 
        date: datetime,
        output_bucket: str
    ):
        """Upload book snapshots DataFrame to GCS with optimized Parquet for timestamp-based queries"""
        
        if snapshots_df.empty:
            logger.info(f"📤 No snapshots to upload for {instrument_id} {timeframe}")
            return
        
        date_str = date.strftime('%Y-%m-%d')
        
        # Sort by timestamp for optimal Parquet row group distribution
        snapshots_df = snapshots_df.sort_values('timestamp')
        
        # Create optimized Parquet file
        blob_name = f"processed_book_snapshots/by_date/day-{date_str}/timeframe-{timeframe}/{instrument_id}.parquet"
        
        # Create optimized Parquet with timestamp-based row groups
        parquet_buffer = self._create_optimized_parquet_buffer(snapshots_df)
        
        # Upload to GCS
        bucket = self.data_client.client.bucket(output_bucket)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(parquet_buffer)
        
        logger.info(f"📤 Uploaded {len(snapshots_df)} {timeframe} book snapshots to gs://{output_bucket}/{blob_name}")
    
    def _create_optimized_parquet_buffer(self, df: pd.DataFrame) -> bytes:
        """Create optimized Parquet buffer for efficient timestamp-based queries"""
        import pyarrow as pa
        import pyarrow.parquet as pq
        import io
        
        # Ensure timestamp columns are properly typed
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        if 'timestamp_out' in df.columns:
            df['timestamp_out'] = pd.to_datetime(df['timestamp_out'])
        if 'snapshot_timestamp' in df.columns:
            df['snapshot_timestamp'] = pd.to_datetime(df['snapshot_timestamp'])
        
        # Create PyArrow table
        table = pa.Table.from_pandas(df)
        
        # Create optimized Parquet buffer
        buffer = io.BytesIO()
        
        # Write with optimization settings for sparse data access
        pq.write_table(
            table,
            buffer,
            compression='snappy',
            use_dictionary=True,
            row_group_size=100000,  # ~1MB per row group for efficient filtering
            data_page_size=1024 * 1024,  # 1MB data pages
            write_statistics=True,  # Enable statistics for predicate pushdown
            use_deprecated_int96_timestamps=False,  # Use modern timestamp format
            column_encoding={
                'timestamp_out': 'DELTA_BINARY_PACKED',  # Primary query column
                'timestamp': 'DELTA_BINARY_PACKED',      # Secondary timestamp
                'snapshot_timestamp': 'DELTA_BINARY_PACKED'  # Book snapshot timestamp
            }
        )
        
        buffer.seek(0)
        return buffer.getvalue()
