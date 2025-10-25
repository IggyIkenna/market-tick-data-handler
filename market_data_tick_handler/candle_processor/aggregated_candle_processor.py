"""
Aggregated Candle Processor

Aggregates 1m candles into higher timeframes (5m, 15m, 1h, 4h, 24h).
Shares logic with historical processor for modular design.
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
class AggregationConfig:
    """Configuration for candle aggregation"""
    timeframes: List[str] = None
    enable_hft_features: bool = True
    
    def __post_init__(self):
        if self.timeframes is None:
            self.timeframes = ['5m', '15m', '1h', '4h', '24h']

class AggregatedCandleProcessor:
    """Aggregates 1m candles into higher timeframes"""
    
    def __init__(self, data_client: DataClient, config: AggregationConfig = None):
        self.data_client = data_client
        self.config = config or AggregationConfig()
        self.timestamp_manager = UTCTimestampManager()
        
        # Supported aggregation timeframes
        self.aggregation_timeframes = ['5m', '15m', '1h', '4h', '24h']
        
        # Validate configuration
        for tf in self.config.timeframes:
            if tf not in self.aggregation_timeframes:
                raise ValueError(f"Aggregation processor supports {self.aggregation_timeframes}, got: {tf}")
    
    async def process_day(
        self, 
        instrument_id: str, 
        date: datetime,
        output_bucket: str = None
    ) -> Dict[str, Any]:
        """
        Process a full day by aggregating 1m candles into higher timeframes
        
        Args:
            instrument_id: Instrument key
            date: Date to process (UTC)
            output_bucket: GCS bucket for output
            
        Returns:
            Dictionary with processing results
        """
        logger.info(f"📊 Aggregating candles for {instrument_id} on {date.strftime('%Y-%m-%d')}")
        
        output_bucket = output_bucket or self.data_client.gcs_bucket
        results = {
            'instrument_id': instrument_id,
            'date': date.strftime('%Y-%m-%d'),
            'timeframes': {},
            'errors': []
        }
        
        try:
            # Load 1m candles for the day
            one_minute_candles = await self._load_1m_candles(instrument_id, date)
            
            if one_minute_candles.empty:
                logger.warning(f"No 1m candles found for {instrument_id} on {date.strftime('%Y-%m-%d')}")
                return results
            
            # Process each aggregation timeframe
            for timeframe in self.config.timeframes:
                try:
                    aggregated_candles = await self._aggregate_timeframe(
                        one_minute_candles, instrument_id, timeframe, date
                    )
                    
                    if not aggregated_candles.empty:
                        # Upload to GCS
                        await self._upload_candles(aggregated_candles, instrument_id, timeframe, date, output_bucket)
                        
                        results['timeframes'][timeframe] = {
                            'candle_count': len(aggregated_candles),
                            'time_range': {
                                'start': aggregated_candles['timestamp'].min().isoformat(),
                                'end': aggregated_candles['timestamp'].max().isoformat()
                            }
                        }
                        
                        logger.info(f"✅ Aggregated {len(aggregated_candles)} {timeframe} candles for {instrument_id}")
                    else:
                        logger.warning(f"No {timeframe} candles aggregated for {instrument_id}")
                        
                except Exception as e:
                    error_msg = f"Failed to aggregate {timeframe} candles: {e}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)
            
            return results
            
        except Exception as e:
            error_msg = f"Failed to aggregate day for {instrument_id}: {e}"
            logger.error(error_msg)
            results['errors'].append(error_msg)
            return results
    
    async def _load_1m_candles(self, instrument_id: str, date: datetime) -> pd.DataFrame:
        """Load 1m candles for a specific day"""
        date_str = date.strftime('%Y-%m-%d')
        blob_name = f"processed_candles/by_date/day-{date_str}/timeframe-1m/{instrument_id}.parquet"
        
        try:
            df = self.data_client.read_parquet_file(blob_name)
            
            if not df.empty and 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df = df.sort_values('timestamp')
            
            return df
            
        except Exception as e:
            logger.warning(f"Failed to load 1m candles for {instrument_id} on {date_str}: {e}")
            return pd.DataFrame()
    
    async def _aggregate_timeframe(
        self, 
        one_minute_candles: pd.DataFrame, 
        instrument_id: str, 
        timeframe: str, 
        date: datetime
    ) -> pd.DataFrame:
        """Aggregate 1m candles into a higher timeframe"""
        
        if one_minute_candles.empty:
            return pd.DataFrame()
        
        # Generate aligned timestamps for the target timeframe
        aligned_timestamps = self._generate_aligned_timestamps(date, timeframe)
        
        aggregated_candles = []
        
        for i, timestamp in enumerate(aligned_timestamps):
            # Get next timestamp for filtering
            next_timestamp = aligned_timestamps[i + 1] if i + 1 < len(aligned_timestamps) else date + timedelta(days=1)
            
            # Filter 1m candles for this interval
            interval_candles = one_minute_candles[
                (one_minute_candles['timestamp'] >= timestamp) & 
                (one_minute_candles['timestamp'] < next_timestamp)
            ].copy()
            
            if not interval_candles.empty:
                # Aggregate the candles
                aggregated_candle = self._aggregate_candles(
                    interval_candles, instrument_id, timeframe, timestamp
                )
                
                if aggregated_candle is not None:
                    aggregated_candles.append(aggregated_candle)
            else:
                # Create empty candle with NaN values
                empty_candle = self._create_empty_candle(instrument_id, timeframe, timestamp)
                aggregated_candles.append(empty_candle)
        
        if not aggregated_candles:
            return pd.DataFrame()
        
        # Convert to DataFrame
        result_df = pd.DataFrame(aggregated_candles)
        
        # Add aggregated HFT features if enabled
        if self.config.enable_hft_features:
            result_df = self._add_aggregated_hft_features(result_df, one_minute_candles, timeframe)
        
        return result_df
    
    def _aggregate_candles(
        self, 
        candles: pd.DataFrame, 
        instrument_id: str, 
        timeframe: str, 
        timestamp: datetime
    ) -> Optional[Dict[str, Any]]:
        """Aggregate a group of 1m candles into a single higher timeframe candle"""
        
        if candles.empty:
            return None
        
        # Calculate timestamp_out based on latest timestamp_out from 1m candles + 200ms
        timestamp_out = self._calculate_timestamp_out(candles, timestamp)
        
        # Basic OHLCV aggregation
        aggregated = {
            'symbol': candles['symbol'].iloc[0] if 'symbol' in candles.columns else instrument_id.split(':')[-1],
            'exchange': candles['exchange'].iloc[0] if 'exchange' in candles.columns else 'unknown',
            'timeframe': timeframe,
            'timestamp': timestamp,
            'timestamp_out': timestamp_out,
            'open': candles['open'].iloc[0],
            'high': candles['high'].max(),
            'low': candles['low'].min(),
            'close': candles['close'].iloc[-1],
            'volume': candles['volume'].sum(),
            'trade_count': candles['trade_count'].sum(),
            'vwap': self._calculate_aggregated_vwap(candles)
        }
        
        return aggregated
    
    def _create_empty_candle(self, instrument_id: str, timeframe: str, timestamp: datetime) -> Dict[str, Any]:
        """Create an empty candle with NaN values"""
        
        symbol = instrument_id.split(':')[-1] if ':' in instrument_id else instrument_id
        
        # Calculate timestamp_out as timestamp + 200ms for empty candles
        timestamp_out = timestamp + timedelta(milliseconds=200)
        
        return {
            'symbol': symbol,
            'exchange': 'unknown',
            'timeframe': timeframe,
            'timestamp': timestamp,
            'timestamp_out': timestamp_out,
            'open': np.nan,
            'high': np.nan,
            'low': np.nan,
            'close': np.nan,
            'volume': 0.0,
            'trade_count': 0,
            'vwap': np.nan
        }
    
    def _calculate_timestamp_out(self, candles: pd.DataFrame, fallback_timestamp: datetime) -> datetime:
        """Calculate timestamp_out based on latest timestamp_out from 1m candles + 200ms"""
        
        if candles.empty:
            # Use fallback timestamp + 200ms
            return fallback_timestamp + timedelta(milliseconds=200)
        
        # Get the latest timestamp_out from candles
        if 'timestamp_out' in candles.columns:
            latest_timestamp_out = candles['timestamp_out'].max()
            if pd.notna(latest_timestamp_out):
                # timestamp_out is already a datetime, just add 200ms
                if isinstance(latest_timestamp_out, datetime):
                    return latest_timestamp_out + timedelta(milliseconds=200)
                else:
                    # Convert if it's not a datetime
                    return pd.to_datetime(latest_timestamp_out) + timedelta(milliseconds=200)
        
        # Fallback to using fallback_timestamp + 200ms
        return fallback_timestamp + timedelta(milliseconds=200)
    
    def _calculate_aggregated_vwap(self, candles: pd.DataFrame) -> float:
        """Calculate VWAP for aggregated candles"""
        
        if candles.empty or 'vwap' not in candles.columns:
            return np.nan
        
        # Use volume-weighted average of VWAPs
        total_volume = candles['volume'].sum()
        if total_volume == 0:
            return np.nan
        
        vwap_sum = (candles['vwap'] * candles['volume']).sum()
        return vwap_sum / total_volume
    
    def _add_aggregated_hft_features(
        self, 
        aggregated_df: pd.DataFrame, 
        one_minute_candles: pd.DataFrame, 
        timeframe: str
    ) -> pd.DataFrame:
        """Add aggregated HFT features to candles DataFrame"""
        
        # Initialize feature columns
        hft_columns = [
            'buy_volume_sum', 'sell_volume_sum', 'size_avg', 'price_vwap', 'trade_count',
            'delay_median', 'delay_max', 'delay_min', 'delay_mean',
            'liquidation_buy_volume', 'liquidation_sell_volume', 'liquidation_count',
            'funding_rate', 'index_price', 'mark_price', 'open_interest', 'predicted_funding_rate',
            'oi_change', 'liquidation_with_rising_oi', 'liquidation_with_falling_oi',
            'skew_25d_put_call_ratio', 'atm_mark_iv'
        ]
        
        for col in hft_columns:
            aggregated_df[col] = np.nan
        
        # Process each aggregated candle
        for i, candle in aggregated_df.iterrows():
            timestamp = candle['timestamp']
            
            # Get 1m candles for this interval
            interval_candles = one_minute_candles[
                (one_minute_candles['timestamp'] >= timestamp) & 
                (one_minute_candles['timestamp'] < timestamp + self._get_timeframe_delta(timeframe))
            ]
            
            if not interval_candles.empty:
                # Aggregate HFT features
                features = self._aggregate_hft_features(interval_candles, timeframe)
                
                # Update DataFrame
                for col, value in features.items():
                    aggregated_df.at[i, col] = value
        
        return aggregated_df
    
    def _aggregate_hft_features(self, candles: pd.DataFrame, timeframe: str) -> Dict[str, float]:
        """Aggregate HFT features from 1m candles"""
        
        features = {}
        
        # Features that should be summed
        sum_features = [
            'buy_volume_sum', 'sell_volume_sum', 'trade_count',
            'liquidation_buy_volume', 'liquidation_sell_volume', 'liquidation_count'
        ]
        
        for col in sum_features:
            if col in candles.columns:
                features[col] = candles[col].sum()
            else:
                features[col] = np.nan
        
        # Features that should be recalculated (averages, medians, etc.)
        if 'size_avg' in candles.columns:
            # Recalculate average trade size
            total_trades = candles['trade_count'].sum()
            if total_trades > 0:
                features['size_avg'] = (candles['size_avg'] * candles['trade_count']).sum() / total_trades
            else:
                features['size_avg'] = np.nan
        else:
            features['size_avg'] = np.nan
        
        # VWAP should be recalculated
        if 'price_vwap' in candles.columns and 'volume' in candles.columns:
            total_volume = candles['volume'].sum()
            if total_volume > 0:
                features['price_vwap'] = (candles['price_vwap'] * candles['volume']).sum() / total_volume
            else:
                features['price_vwap'] = np.nan
        else:
            features['price_vwap'] = np.nan
        
        # Delay features should be recalculated
        delay_features = ['delay_median', 'delay_max', 'delay_min', 'delay_mean']
        for col in delay_features:
            if col in candles.columns:
                # For aggregated timeframes, we need to recalculate from raw data
                # For now, use the median of medians as approximation
                features[col] = candles[col].median()
            else:
                features[col] = np.nan
        
        # Last value features (derivatives ticker)
        last_value_features = [
            'funding_rate', 'index_price', 'mark_price', 'open_interest', 'predicted_funding_rate'
        ]
        
        for col in last_value_features:
            if col in candles.columns:
                # Use last non-NaN value
                non_nan_values = candles[col].dropna()
                features[col] = non_nan_values.iloc[-1] if not non_nan_values.empty else np.nan
            else:
                features[col] = np.nan
        
        # Open interest change signals
        features['oi_change'] = np.nan  # Would need previous candle's OI
        features['liquidation_with_rising_oi'] = np.nan
        features['liquidation_with_falling_oi'] = np.nan
        
        # Options chain features
        features['skew_25d_put_call_ratio'] = np.nan
        features['atm_mark_iv'] = np.nan
        
        return features
    
    def _get_timeframe_delta(self, timeframe: str) -> timedelta:
        """Get timedelta for a timeframe"""
        seconds = self.timestamp_manager.TIMEFRAMES.get(timeframe, 60)
        return timedelta(seconds=seconds)
    
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
    
    async def _upload_candles(
        self, 
        candles_df: pd.DataFrame, 
        instrument_id: str, 
        timeframe: str, 
        date: datetime,
        output_bucket: str
    ):
        """Upload aggregated candles DataFrame to GCS with optimized Parquet for timestamp-based queries"""
        
        if candles_df.empty:
            logger.info(f"📤 No candles to upload for {instrument_id} {timeframe}")
            return
        
        date_str = date.strftime('%Y-%m-%d')
        
        # Sort by timestamp for optimal Parquet row group distribution
        candles_df = candles_df.sort_values('timestamp')
        
        # Create optimized Parquet file
        blob_name = f"processed_candles/by_date/day-{date_str}/timeframe-{timeframe}/{instrument_id}.parquet"
        
        # Create optimized Parquet with timestamp-based row groups
        parquet_buffer = self._create_optimized_parquet_buffer(candles_df)
        
        # Upload to GCS
        bucket = self.data_client.client.bucket(output_bucket)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(parquet_buffer)
        
        logger.info(f"📤 Uploaded {len(candles_df)} {timeframe} candles to gs://{output_bucket}/{blob_name}")
    
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
                'timestamp': 'DELTA_BINARY_PACKED'       # Secondary timestamp
            }
        )
        
        buffer.seek(0)
        return buffer.getvalue()
