"""
MFT Features Reader

Reads MFT (Mid-Frequency Trading) features from processed candle data.
MFT features are the same as HFT features but available for all timeframes (1m and above).
"""

import logging
import pandas as pd
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from .data_client import DataClient

logger = logging.getLogger(__name__)

class MFTFeaturesReader:
    """Reads MFT features from processed candle data"""
    
    def __init__(self, data_client: DataClient):
        self.data_client = data_client
        self.client = data_client.client
        self.bucket = data_client.bucket
        
        # MFT features are available for 1m and above timeframes
        self.supported_timeframes = ['1m', '5m', '15m', '1h', '4h', '24h']
        
        # MFT features are the same as HFT features but for higher timeframes
        self.mft_columns = [
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
    
    def get_mft_features(
        self, 
        instrument_id: str, 
        timeframe: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> pd.DataFrame:
        """
        Get MFT features for a specific instrument and timeframe
        
        Args:
            instrument_id: Instrument key (e.g., 'BINANCE:SPOT_PAIR:BTC-USDT')
            timeframe: Timeframe ('1m', '5m', '15m', '1h', '4h', '24h')
            start_date: Start date (UTC)
            end_date: End date (UTC)
            
        Returns:
            DataFrame with MFT features
        """
        if timeframe not in self.supported_timeframes:
            raise ValueError(f"MFT features available for {self.supported_timeframes}, got: {timeframe}")
        
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
                    
                    # Extract only MFT feature columns
                    mft_df = self._extract_mft_features(df)
                    
                    if not mft_df.empty:
                        all_features.append(mft_df)
                    
            except Exception as e:
                logger.warning(f"Failed to read MFT features for {instrument_id} {timeframe} on {date_str}: {e}")
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
    
    def get_mft_features_for_date(
        self, 
        instrument_id: str, 
        timeframe: str, 
        date: datetime
    ) -> pd.DataFrame:
        """
        Get MFT features for a specific instrument, timeframe, and single date
        
        Args:
            instrument_id: Instrument key
            timeframe: Timeframe ('1m', '5m', '15m', '1h', '4h', '24h')
            date: Specific date
            
        Returns:
            DataFrame with MFT features for that date
        """
        date_str = date.strftime('%Y-%m-%d')
        blob_name = f"processed_candles/by_date/day-{date_str}/timeframe-{timeframe}/{instrument_id}.parquet"
        
        try:
            df = self.data_client.read_parquet_file(blob_name)
            
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            return self._extract_mft_features(df)
            
        except Exception as e:
            logger.warning(f"Failed to read MFT features for {instrument_id} {timeframe} on {date_str}: {e}")
            return pd.DataFrame()
    
    def _extract_mft_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract MFT feature columns from a candle DataFrame
        
        Args:
            df: DataFrame with candle data
            
        Returns:
            DataFrame with only MFT feature columns
        """
        # Start with basic columns that should always be present
        base_columns = ['timestamp', 'symbol', 'exchange', 'timeframe']
        result_columns = []
        
        # Add base columns if they exist
        for col in base_columns:
            if col in df.columns:
                result_columns.append(col)
        
        # Add MFT feature columns that exist in the DataFrame
        for col in self.mft_columns:
            if col in df.columns:
                result_columns.append(col)
        
        if not result_columns:
            return pd.DataFrame()
        
        return df[result_columns].copy()
    
    def get_available_mft_features(self, instrument_id: str, date: datetime) -> Dict[str, List[str]]:
        """
        Get list of available MFT features for a specific instrument and date
        
        Args:
            instrument_id: Instrument key
            date: Date to check
            
        Returns:
            Dictionary mapping timeframe to list of available MFT features
        """
        date_str = date.strftime('%Y-%m-%d')
        available_features = {}
        
        for timeframe in self.supported_timeframes:
            blob_name = f"processed_candles/by_date/day-{date_str}/timeframe-{timeframe}/{instrument_id}.parquet"
            blob = self.bucket.blob(blob_name)
            
            if blob.exists():
                try:
                    df = self.data_client.read_parquet_file(blob_name)
                    available_features[timeframe] = [col for col in self.mft_columns if col in df.columns]
                except Exception as e:
                    logger.warning(f"Failed to check MFT features for {instrument_id} {timeframe} on {date_str}: {e}")
                    available_features[timeframe] = []
            else:
                available_features[timeframe] = []
        
        return available_features
    
    def get_mft_feature_summary(self, instrument_id: str, date: datetime) -> Dict[str, Any]:
        """
        Get summary information about available MFT features for an instrument
        
        Args:
            instrument_id: Instrument key
            date: Date to check
            
        Returns:
            Dictionary with MFT features summary
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
                    mft_df = self._extract_mft_features(df)
                    
                    summary['timeframes'][timeframe] = {
                        'available': True,
                        'feature_count': len(mft_df),
                        'available_features': [col for col in self.mft_columns if col in df.columns],
                        'file_size': blob.size or 0,
                        'time_range': {
                            'start': mft_df['timestamp'].min() if 'timestamp' in mft_df.columns else None,
                            'end': mft_df['timestamp'].max() if 'timestamp' in mft_df.columns else None
                        }
                    }
                    summary['total_features'] += len(mft_df)
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
