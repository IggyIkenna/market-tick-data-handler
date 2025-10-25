"""
Historical Candle Processor

Processes tick data into 15s and 1m candles with HFT features for historical/batch processing.
Uses UTC-aligned boundaries and handles empty intervals with NaN values.
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
import asyncio

from ..data_client.data_client import DataClient
from ..streaming_service.candle_processor.candle_data import CandleBuilder, CandleData
from ..streaming_service.tick_streamer.utc_timestamp_manager import UTCTimestampManager

logger = logging.getLogger(__name__)

@dataclass
class ProcessingConfig:
    """Configuration for candle processing"""
    timeframes: List[str] = None
    enable_hft_features: bool = True
    enable_book_snapshots: bool = True
    enable_options_skew: bool = False
    data_types: List[str] = None
    
    def __post_init__(self):
        if self.timeframes is None:
            self.timeframes = ['15s', '1m']
        if self.data_types is None:
            self.data_types = ['trades', 'book_snapshot_5', 'derivative_ticker', 'liquidations', 'options_chain']

class HistoricalCandleProcessor:
    """Processes historical tick data into candles with HFT features"""
    
    def __init__(self, data_client: DataClient, config: ProcessingConfig = None):
        self.data_client = data_client
        self.config = config or ProcessingConfig()
        self.timestamp_manager = UTCTimestampManager()
        
        # Supported timeframes for base processing
        self.base_timeframes = ['15s', '1m']
        
        # Validate configuration
        for tf in self.config.timeframes:
            if tf not in self.base_timeframes:
                raise ValueError(f"Historical processor only supports {self.base_timeframes}, got: {tf}")
    
    async def process_day(
        self, 
        instrument_id: str, 
        date: datetime,
        output_bucket: str = None
    ) -> Dict[str, Any]:
        """
        Process a full day of tick data into candles
        
        Args:
            instrument_id: Instrument key (e.g., 'BINANCE:SPOT_PAIR:BTC-USDT')
            date: Date to process (UTC)
            output_bucket: GCS bucket for output (defaults to data_client bucket)
            
        Returns:
            Dictionary with processing results
        """
        logger.info(f"🕯️ Processing candles for {instrument_id} on {date.strftime('%Y-%m-%d')}")
        
        output_bucket = output_bucket or self.data_client.gcs_bucket
        results = {
            'instrument_id': instrument_id,
            'date': date.strftime('%Y-%m-%d'),
            'timeframes': {},
            'errors': []
        }
        
        try:
            # Load all required data for the day
            day_data = await self._load_day_data(instrument_id, date)
            
            if day_data['trades'].empty:
                logger.warning(f"No trade data found for {instrument_id} on {date.strftime('%Y-%m-%d')}")
                return results
            
            # Process each timeframe
            for timeframe in self.config.timeframes:
                try:
                    candles = await self._process_timeframe(
                        day_data, instrument_id, timeframe, date
                    )
                    
                    if not candles.empty:
                        # Upload to GCS
                        await self._upload_candles(candles, instrument_id, timeframe, date, output_bucket)
                        
                        results['timeframes'][timeframe] = {
                            'candle_count': len(candles),
                            'time_range': {
                                'start': candles['timestamp'].min().isoformat(),
                                'end': candles['timestamp'].max().isoformat()
                            }
                        }
                        
                        logger.info(f"✅ Processed {len(candles)} {timeframe} candles for {instrument_id}")
                    else:
                        logger.warning(f"No {timeframe} candles generated for {instrument_id}")
                        
                except Exception as e:
                    error_msg = f"Failed to process {timeframe} candles: {e}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)
            
            return results
            
        except Exception as e:
            error_msg = f"Failed to process day for {instrument_id}: {e}"
            logger.error(error_msg)
            results['errors'].append(error_msg)
            return results
    
    async def _load_day_data(self, instrument_id: str, date: datetime) -> Dict[str, pd.DataFrame]:
        """Load all required data types for a single day"""
        date_str = date.strftime('%Y-%m-%d')
        day_data = {}
        
        for data_type in self.config.data_types:
            try:
                blob_name = f"raw_tick_data/by_date/day-{date_str}/data_type-{data_type}/{instrument_id}.parquet"
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
                
                day_data[data_type] = df
                logger.info(f"📊 Loaded {len(df)} {data_type} records for {instrument_id}")
                
            except Exception as e:
                logger.warning(f"Failed to load {data_type} data for {instrument_id}: {e}")
                day_data[data_type] = pd.DataFrame()
        
        return day_data
    
    async def _process_timeframe(
        self, 
        day_data: Dict[str, pd.DataFrame], 
        instrument_id: str, 
        timeframe: str, 
        date: datetime
    ) -> pd.DataFrame:
        """Process data into candles for a specific timeframe"""
        
        # Get trades data
        trades_df = day_data.get('trades', pd.DataFrame())
        if trades_df.empty:
            return pd.DataFrame()
        
        # Extract symbol and exchange from instrument_id
        symbol, exchange = self._parse_instrument_id(instrument_id)
        
        # Generate aligned timestamps for the day
        aligned_timestamps = self._generate_aligned_timestamps(date, timeframe)
        
        # Process candles
        candles = []
        
        for i, timestamp in enumerate(aligned_timestamps):
            # Get next timestamp for filtering
            next_timestamp = aligned_timestamps[i + 1] if i + 1 < len(aligned_timestamps) else date + timedelta(days=1)
            
            # Filter trades for this interval
            interval_trades = trades_df[
                (trades_df['timestamp'] >= timestamp) & 
                (trades_df['timestamp'] < next_timestamp)
            ].copy()
            
            # Create candle
            candle = self._create_candle(
                symbol, exchange, timeframe, timestamp, 
                interval_trades, day_data
            )
            
            if candle is not None:
                candles.append(candle)
        
        if not candles:
            return pd.DataFrame()
        
        # Convert to DataFrame
        result_df = pd.DataFrame([candle.to_dict() for candle in candles])
        
        # Add HFT features if enabled
        if self.config.enable_hft_features:
            result_df = self._add_hft_features(result_df, day_data, timeframe)
        
        return result_df
    
    def _create_candle(
        self, 
        symbol: str, 
        exchange: str, 
        timeframe: str, 
        timestamp: datetime,
        trades: pd.DataFrame,
        day_data: Dict[str, pd.DataFrame]
    ) -> Optional[CandleData]:
        """Create a single candle from trades data"""
        
        # Calculate timestamp_out based on local_timestamp + 200ms
        timestamp_out = self._calculate_timestamp_out(trades, timestamp)
        
        if trades.empty:
            # Create empty candle with NaN values
            return CandleData(
                symbol=symbol,
                exchange=exchange,
                timeframe=timeframe,
                timestamp_in=timestamp,
                timestamp_out=timestamp_out,
                open=np.nan,
                high=np.nan,
                low=np.nan,
                close=np.nan,
                volume=0.0,
                trade_count=0,
                vwap=np.nan
            )
        
        # Use CandleBuilder to accumulate trades
        builder = CandleBuilder(
            symbol=symbol,
            exchange=exchange,
            timeframe=timeframe,
            timestamp_in=timestamp
        )
        
        # Add trades to builder
        for _, trade in trades.iterrows():
            price = trade.get('price', 0.0)
            amount = trade.get('amount', 0.0)
            builder.add_trade(price, amount)
        
        # Finalize candle with calculated timestamp_out
        return builder.finalize(timestamp_out)
    
    def _calculate_timestamp_out(self, trades: pd.DataFrame, fallback_timestamp: datetime) -> datetime:
        """Calculate timestamp_out as local_timestamp + 200ms"""
        
        if trades.empty:
            # Use fallback timestamp + 200ms
            return fallback_timestamp + timedelta(milliseconds=200)
        
        # Get the latest local_timestamp from trades
        if 'local_timestamp' in trades.columns:
            # Convert microseconds to datetime
            latest_local_timestamp = trades['local_timestamp'].max()
            if pd.notna(latest_local_timestamp):
                # Convert microseconds to datetime
                latest_dt = datetime.fromtimestamp(latest_local_timestamp / 1_000_000, tz=timezone.utc)
                return latest_dt + timedelta(milliseconds=200)
        
        # Fallback to using fallback_timestamp + 200ms
        return fallback_timestamp + timedelta(milliseconds=200)
    
    def _add_hft_features(
        self, 
        candles_df: pd.DataFrame, 
        day_data: Dict[str, pd.DataFrame], 
        timeframe: str
    ) -> pd.DataFrame:
        """Add HFT features to candles DataFrame"""
        
        # Initialize feature columns
        for col in self._get_hft_feature_columns():
            candles_df[col] = np.nan
        
        # Process each candle
        for i, candle in candles_df.iterrows():
            timestamp = candle['timestamp']
            
            # Get data for this timestamp
            candle_data = self._get_candle_data_at_timestamp(day_data, timestamp)
            
            # Calculate HFT features
            features = self._calculate_hft_features(candle_data, timeframe)
            
            # Update DataFrame
            for col, value in features.items():
                candles_df.at[i, col] = value
        
        return candles_df
    
    def _get_hft_feature_columns(self) -> List[str]:
        """Get list of HFT feature column names"""
        return [
            # Trade data features
            'buy_volume_sum', 'sell_volume_sum', 'size_avg', 'price_vwap', 'trade_count',
            'delay_median', 'delay_max', 'delay_min', 'delay_mean',
            
            # Liquidation features
            'liquidation_buy_volume', 'liquidation_sell_volume', 'liquidation_count',
            
            # Derivatives ticker features
            'funding_rate', 'index_price', 'mark_price', 'open_interest', 'predicted_funding_rate',
            
            # Open interest change signals
            'oi_change', 'liquidation_with_rising_oi', 'liquidation_with_falling_oi',
            
            # Options chain features (if enabled)
            'skew_25d_put_call_ratio', 'atm_mark_iv'
        ]
    
    def _get_candle_data_at_timestamp(
        self, 
        day_data: Dict[str, pd.DataFrame], 
        timestamp: datetime
    ) -> Dict[str, pd.DataFrame]:
        """Get relevant data for a specific timestamp"""
        
        # For now, return all data - more sophisticated filtering can be added
        return day_data
    
    def _calculate_hft_features(
        self, 
        candle_data: Dict[str, pd.DataFrame], 
        timeframe: str
    ) -> Dict[str, float]:
        """Calculate HFT features for a candle"""
        
        features = {}
        
        # Trade data features
        trades = candle_data.get('trades', pd.DataFrame())
        if not trades.empty:
            features.update(self._calculate_trade_features(trades))
        else:
            # Set NaN for all trade features
            for col in ['buy_volume_sum', 'sell_volume_sum', 'size_avg', 'price_vwap', 'trade_count',
                       'delay_median', 'delay_max', 'delay_min', 'delay_mean']:
                features[col] = np.nan
        
        # Liquidation features
        liquidations = candle_data.get('liquidations', pd.DataFrame())
        if not liquidations.empty:
            features.update(self._calculate_liquidation_features(liquidations))
        else:
            for col in ['liquidation_buy_volume', 'liquidation_sell_volume', 'liquidation_count']:
                features[col] = np.nan
        
        # Derivatives ticker features
        derivative_ticker = candle_data.get('derivative_ticker', pd.DataFrame())
        if not derivative_ticker.empty:
            features.update(self._calculate_derivative_features(derivative_ticker))
        else:
            for col in ['funding_rate', 'index_price', 'mark_price', 'open_interest', 'predicted_funding_rate']:
                features[col] = np.nan
        
        # Open interest change signals
        features.update(self._calculate_oi_change_signals(candle_data))
        
        # Options chain features (if enabled)
        if self.config.enable_options_skew:
            features.update(self._calculate_options_features(candle_data))
        else:
            features['skew_25d_put_call_ratio'] = np.nan
            features['atm_mark_iv'] = np.nan
        
        return features
    
    def _calculate_trade_features(self, trades: pd.DataFrame) -> Dict[str, float]:
        """Calculate trade-based HFT features"""
        features = {}
        
        if trades.empty:
            return features
        
        # Basic OHLCV features
        features['trade_count'] = len(trades)
        features['size_avg'] = trades['amount'].mean() if 'amount' in trades.columns else np.nan
        
        # Volume features
        total_volume = trades['amount'].sum() if 'amount' in trades.columns else 0
        features['price_vwap'] = (trades['price'] * trades['amount']).sum() / total_volume if total_volume > 0 else np.nan
        
        # Side-based volume (simplified - would need trade side information)
        features['buy_volume_sum'] = total_volume / 2  # Placeholder
        features['sell_volume_sum'] = total_volume / 2  # Placeholder
        
        # Delay features
        if 'local_timestamp' in trades.columns and 'timestamp' in trades.columns:
            delays = (trades['local_timestamp'] - trades['timestamp']).dt.total_seconds() * 1000
            features['delay_median'] = delays.median()
            features['delay_max'] = delays.max()
            features['delay_min'] = delays.min()
            features['delay_mean'] = delays.mean()
        else:
            for col in ['delay_median', 'delay_max', 'delay_min', 'delay_mean']:
                features[col] = np.nan
        
        return features
    
    def _calculate_liquidation_features(self, liquidations: pd.DataFrame) -> Dict[str, float]:
        """Calculate liquidation-based HFT features"""
        features = {}
        
        if liquidations.empty:
            return features
        
        features['liquidation_count'] = len(liquidations)
        
        # Side-based liquidation volume (simplified)
        if 'amount' in liquidations.columns:
            features['liquidation_buy_volume'] = liquidations['amount'].sum() / 2  # Placeholder
            features['liquidation_sell_volume'] = liquidations['amount'].sum() / 2  # Placeholder
        else:
            features['liquidation_buy_volume'] = 0.0
            features['liquidation_sell_volume'] = 0.0
        
        return features
    
    def _calculate_derivative_features(self, derivative_ticker: pd.DataFrame) -> Dict[str, float]:
        """Calculate derivative ticker features (last values)"""
        features = {}
        
        if derivative_ticker.empty:
            return features
        
        # Get last values
        last_row = derivative_ticker.iloc[-1]
        
        for col in ['funding_rate', 'index_price', 'mark_price', 'open_interest', 'predicted_funding_rate']:
            features[col] = last_row.get(col, np.nan)
        
        return features
    
    def _calculate_oi_change_signals(self, candle_data: Dict[str, pd.DataFrame]) -> Dict[str, float]:
        """Calculate open interest change signals"""
        features = {}
        
        # This would need previous candle's OI for comparison
        # For now, set to NaN
        features['oi_change'] = np.nan
        features['liquidation_with_rising_oi'] = np.nan
        features['liquidation_with_falling_oi'] = np.nan
        
        return features
    
    def _calculate_options_features(self, candle_data: Dict[str, pd.DataFrame]) -> Dict[str, float]:
        """Calculate options chain features (25-delta skew)"""
        features = {}
        
        # This would need complex options chain processing
        # For now, set to NaN
        features['skew_25d_put_call_ratio'] = np.nan
        features['atm_mark_iv'] = np.nan
        
        return features
    
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
    
    def _parse_instrument_id(self, instrument_id: str) -> Tuple[str, str]:
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
    
    async def _upload_candles(
        self, 
        candles_df: pd.DataFrame, 
        instrument_id: str, 
        timeframe: str, 
        date: datetime,
        output_bucket: str
    ):
        """Upload candles DataFrame to GCS with optimized Parquet for timestamp-based queries"""
        
        if candles_df.empty:
            logger.info(f"📤 No candles to upload for {instrument_id} {timeframe}")
            return
        
        date_str = date.strftime('%Y-%m-%d')
        
        # Sort by timestamp for optimal Parquet row group distribution
        candles_df = candles_df.sort_values('timestamp')
        
        # Create optimized Parquet file
        blob_name = f"processed_candles/by_date/day-{date_str}/timeframe-{timeframe}/{instrument_id}.parquet"
        
        # Use ParquetOptimizer for efficient timestamp-based queries
        from ..data_client.parquet_optimizer import ParquetOptimizer
        optimizer = ParquetOptimizer(self.data_client.client, output_bucket)
        
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
