"""
HFT Features Reader

Reads HFT (High-Frequency Trading) features from processed candle data.
HFT features are computed for 15s and 1m timeframes only.
"""

import logging
import pandas as pd
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from .data_client import DataClient

logger = logging.getLogger(__name__)

class HFTFeaturesReader:
    """Reads HFT features from processed candle data"""
    
    def __init__(self, data_client: DataClient):
        self.data_client = data_client
        self.client = data_client.client
        self.bucket = data_client.bucket
        
        # HFT features are only available for 15s and 1m timeframes
        self.supported_timeframes = ['15s', '1m']
        
        # Expected HFT feature columns
        self.hft_columns = [
            # Trade data features
            'buy_volume_sum', 'sell_volume_sum', 'size_avg', 'price_vwap', 'trade_count',
            'delay_median', 'delay_max', 'delay_min', 'delay_mean',
            
            # Liquidation features
            'liquidation_buy_volume', 'liquidation_sell_volume', 'liquidation_count',
            
            # Derivatives ticker features
            'funding_rate', 'index_price', 'mark_price', 'open_interest', 'predicted_funding_rate',
            
            # Open interest change signals
            'oi_change', 'liquidation_with_rising_oi', 'liquidation_with_falling_oi',
            
            # Options chain features (if available)
            'skew_25d_put_call_ratio', 'atm_mark_iv'
        ]
    
    def get_hft_features(
        self, 
        instrument_id: str, 
        timeframe: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> pd.DataFrame:
        """
        Get HFT features for a specific instrument and timeframe
        
        Args:
            instrument_id: Instrument key (e.g., 'BINANCE:SPOT_PAIR:BTC-USDT')
            timeframe: Timeframe ('15s' or '1m' only)
            start_date: Start date (UTC)
            end_date: End date (UTC)
            
        Returns:
            DataFrame with HFT features
        """
        if timeframe not in self.supported_timeframes:
            raise ValueError(f"HFT features only available for {self.supported_timeframes}, got: {timeframe}")
        
        all_features = []
        current_date = start_date
        
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            
            # Construct file path
            # Pattern: processed_candles/by_date/day-{date}/timeframe-{timeframe}/{instrument_id}.parquet
            blob_name = f"processed_candles/by_date/day-{date_str}/timeframe-{timeframe}/{instrument_id}.parquet"
            
            try:
                df = self.data_client.read_parquet_file(blob_name)
                
                if not df.empty:
                    # Ensure timestamp column is datetime
                    if 'timestamp' in df.columns:
                        df['timestamp'] = pd.to_datetime(df['timestamp'])
                    
                    # Filter by date range if needed
                    if 'timestamp' in df.columns:
                        df = df[(df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)]
                    
                    # Extract only HFT feature columns
                    hft_df = self._extract_hft_features(df)
                    
                    if not hft_df.empty:
                        all_features.append(hft_df)
                    
            except Exception as e:
                logger.warning(f"Failed to read HFT features for {instrument_id} {timeframe} on {date_str}: {e}")
                continue
            
            # Move to next day
            from datetime import timedelta
            current_date += timedelta(days=1)
        
        if not all_features:
            return pd.DataFrame()
        
        # Combine all days
        result = pd.concat(all_features, ignore_index=True)
        
        # Sort by timestamp
        if 'timestamp' in result.columns:
            result = result.sort_values('timestamp')
        
        return result
    
    def get_hft_features_for_date(
        self, 
        instrument_id: str, 
        timeframe: str, 
        date: datetime
    ) -> pd.DataFrame:
        """
        Get HFT features for a specific instrument, timeframe, and single date
        
        Args:
            instrument_id: Instrument key
            timeframe: Timeframe ('15s' or '1m')
            date: Specific date
            
        Returns:
            DataFrame with HFT features for that date
        """
        date_str = date.strftime('%Y-%m-%d')
        blob_name = f"processed_candles/by_date/day-{date_str}/timeframe-{timeframe}/{instrument_id}.parquet"
        
        try:
            df = self.data_client.read_parquet_file(blob_name)
            
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            return self._extract_hft_features(df)
            
        except Exception as e:
            logger.warning(f"Failed to read HFT features for {instrument_id} {timeframe} on {date_str}: {e}")
            return pd.DataFrame()
    
    def _extract_hft_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract HFT feature columns from a candle DataFrame
        
        Args:
            df: DataFrame with candle data
            
        Returns:
            DataFrame with only HFT feature columns
        """
        # Start with basic columns that should always be present
        base_columns = ['timestamp', 'symbol', 'exchange', 'timeframe']
        result_columns = []
        
        # Add base columns if they exist
        for col in base_columns:
            if col in df.columns:
                result_columns.append(col)
        
        # Add HFT feature columns that exist in the DataFrame
        for col in self.hft_columns:
            if col in df.columns:
                result_columns.append(col)
        
        if not result_columns:
            return pd.DataFrame()
        
        return df[result_columns].copy()
    
    def get_available_hft_features(self, instrument_id: str, date: datetime) -> Dict[str, List[str]]:
        """
        Get list of available HFT features for a specific instrument and date
        
        Args:
            instrument_id: Instrument key
            date: Date to check
            
        Returns:
            Dictionary mapping timeframe to list of available HFT features
        """
        date_str = date.strftime('%Y-%m-%d')
        available_features = {}
        
        for timeframe in self.supported_timeframes:
            blob_name = f"processed_candles/by_date/day-{date_str}/timeframe-{timeframe}/{instrument_id}.parquet"
            blob = self.bucket.blob(blob_name)
            
            if blob.exists():
                try:
                    df = self.data_client.read_parquet_file(blob_name)
                    available_features[timeframe] = [col for col in self.hft_columns if col in df.columns]
                except Exception as e:
                    logger.warning(f"Failed to check HFT features for {instrument_id} {timeframe} on {date_str}: {e}")
                    available_features[timeframe] = []
            else:
                available_features[timeframe] = []
        
        return available_features
    
    def get_hft_feature_summary(self, instrument_id: str, date: datetime) -> Dict[str, Any]:
        """
        Get summary information about available HFT features for an instrument
        
        Args:
            instrument_id: Instrument key
            date: Date to check
            
        Returns:
            Dictionary with HFT features summary
        """
        date_str = date.strftime('%Y-%m-%d')
        summary = {
            'instrument_id': instrument_id,
            'date': date_str,
            'timeframes': {},
            'total_features': 0
        }
        
        for timeframe in self.supported_timeframes:
            blob_name = f"processed_candles/by_date/day-{date_str}/timeframe-{timeframe}/{instrument_id}.parquet"
            blob = self.bucket.blob(blob_name)
            
            if blob.exists():
                try:
                    df = self.data_client.read_parquet_file(blob_name)
                    hft_df = self._extract_hft_features(df)
                    
                    summary['timeframes'][timeframe] = {
                        'available': True,
                        'feature_count': len(hft_df),
                        'available_features': [col for col in self.hft_columns if col in df.columns],
                        'file_size': blob.size or 0,
                        'time_range': {
                            'start': hft_df['timestamp'].min() if 'timestamp' in hft_df.columns else None,
                            'end': hft_df['timestamp'].max() if 'timestamp' in hft_df.columns else None
                        }
                    }
                    summary['total_features'] += len(hft_df)
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
