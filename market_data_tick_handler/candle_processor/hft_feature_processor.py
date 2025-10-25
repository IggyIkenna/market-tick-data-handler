"""
HFT Feature Processor

Processes HFT (High-Frequency Trading) features for candles.
Handles both 15s base processing and aggregation to higher timeframes.
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

from ..data_client.data_client import DataClient

logger = logging.getLogger(__name__)

@dataclass
class HFTFeatureConfig:
    """Configuration for HFT feature processing"""
    timeframes: List[str] = None
    enable_options_skew: bool = False
    
    def __post_init__(self):
        if self.timeframes is None:
            self.timeframes = ['15s', '1m', '5m', '15m', '1h', '4h', '24h']

class HFTFeatureProcessor:
    """Processes HFT features for candles across all timeframes"""
    
    def __init__(self, data_client: DataClient, config: HFTFeatureConfig = None):
        self.data_client = data_client
        self.config = config or HFTFeatureConfig()
        
        # HFT feature columns
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
            
            # Options chain features (if enabled)
            'skew_25d_put_call_ratio', 'atm_mark_iv'
        ]
    
    def process_hft_features(
        self, 
        candles_df: pd.DataFrame, 
        day_data: Dict[str, pd.DataFrame], 
        timeframe: str
    ) -> pd.DataFrame:
        """
        Add HFT features to candles DataFrame
        
        Args:
            candles_df: DataFrame with candle data
            day_data: Dictionary with all data types for the day
            timeframe: Timeframe being processed
            
        Returns:
            DataFrame with HFT features added
        """
        
        if candles_df.empty:
            return candles_df
        
        # Initialize HFT feature columns
        for col in self.hft_columns:
            candles_df[col] = np.nan
        
        # Process each candle
        for i, candle in candles_df.iterrows():
            timestamp = candle['timestamp']
            
            # Get data for this timestamp
            candle_data = self._get_candle_data_at_timestamp(day_data, timestamp, timeframe)
            
            # Calculate HFT features
            features = self._calculate_hft_features(candle_data, timeframe)
            
            # Update DataFrame
            for col, value in features.items():
                candles_df.at[i, col] = value
        
        return candles_df
    
    def aggregate_hft_features(
        self, 
        candles_df: pd.DataFrame, 
        one_minute_candles: pd.DataFrame, 
        timeframe: str
    ) -> pd.DataFrame:
        """
        Aggregate HFT features from 1m candles to higher timeframes
        
        Args:
            candles_df: DataFrame with aggregated candles
            one_minute_candles: DataFrame with 1m candles
            timeframe: Target timeframe
            
        Returns:
            DataFrame with aggregated HFT features
        """
        
        if candles_df.empty or one_minute_candles.empty:
            return candles_df
        
        # Initialize HFT feature columns
        for col in self.hft_columns:
            candles_df[col] = np.nan
        
        # Process each aggregated candle
        for i, candle in candles_df.iterrows():
            timestamp = candle['timestamp']
            
            # Get 1m candles for this interval
            interval_candles = self._get_interval_candles(one_minute_candles, timestamp, timeframe)
            
            if not interval_candles.empty:
                # Aggregate HFT features
                features = self._aggregate_hft_features_for_interval(interval_candles, timeframe)
                
                # Update DataFrame
                for col, value in features.items():
                    candles_df.at[i, col] = value
        
        return candles_df
    
    def _get_candle_data_at_timestamp(
        self, 
        day_data: Dict[str, pd.DataFrame], 
        timestamp: datetime,
        timeframe: str
    ) -> Dict[str, pd.DataFrame]:
        """Get relevant data for a specific timestamp"""
        
        # For now, return all data - more sophisticated filtering can be added
        return day_data
    
    def _get_interval_candles(
        self, 
        one_minute_candles: pd.DataFrame, 
        timestamp: datetime, 
        timeframe: str
    ) -> pd.DataFrame:
        """Get 1m candles for a specific interval"""
        
        # Calculate interval duration
        interval_seconds = self._get_timeframe_seconds(timeframe)
        next_timestamp = timestamp + timedelta(seconds=interval_seconds)
        
        # Filter candles within the interval
        interval_candles = one_minute_candles[
            (one_minute_candles['timestamp'] >= timestamp) & 
            (one_minute_candles['timestamp'] < next_timestamp)
        ]
        
        return interval_candles
    
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
    
    def _aggregate_hft_features_for_interval(
        self, 
        interval_candles: pd.DataFrame, 
        timeframe: str
    ) -> Dict[str, float]:
        """Aggregate HFT features from 1m candles for a higher timeframe interval"""
        
        features = {}
        
        if interval_candles.empty:
            # Return NaN for all features
            for col in self.hft_columns:
                features[col] = np.nan
            return features
        
        # Features that should be summed
        sum_features = [
            'buy_volume_sum', 'sell_volume_sum', 'trade_count',
            'liquidation_buy_volume', 'liquidation_sell_volume', 'liquidation_count'
        ]
        
        for col in sum_features:
            if col in interval_candles.columns:
                features[col] = interval_candles[col].sum()
            else:
                features[col] = np.nan
        
        # Features that should be recalculated (averages, medians, etc.)
        if 'size_avg' in interval_candles.columns:
            # Recalculate average trade size
            total_trades = interval_candles['trade_count'].sum()
            if total_trades > 0:
                features['size_avg'] = (interval_candles['size_avg'] * interval_candles['trade_count']).sum() / total_trades
            else:
                features['size_avg'] = np.nan
        else:
            features['size_avg'] = np.nan
        
        # VWAP should be recalculated
        if 'price_vwap' in interval_candles.columns and 'volume' in interval_candles.columns:
            total_volume = interval_candles['volume'].sum()
            if total_volume > 0:
                features['price_vwap'] = (interval_candles['price_vwap'] * interval_candles['volume']).sum() / total_volume
            else:
                features['price_vwap'] = np.nan
        else:
            features['price_vwap'] = np.nan
        
        # Delay features should be recalculated
        delay_features = ['delay_median', 'delay_max', 'delay_min', 'delay_mean']
        for col in delay_features:
            if col in interval_candles.columns:
                # For aggregated timeframes, we need to recalculate from raw data
                # For now, use the median of medians as approximation
                features[col] = interval_candles[col].median()
            else:
                features[col] = np.nan
        
        # Last value features (derivatives ticker)
        last_value_features = [
            'funding_rate', 'index_price', 'mark_price', 'open_interest', 'predicted_funding_rate'
        ]
        
        for col in last_value_features:
            if col in interval_candles.columns:
                # Use last non-NaN value
                non_nan_values = interval_candles[col].dropna()
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
    
    def _get_timeframe_seconds(self, timeframe: str) -> int:
        """Get number of seconds for a timeframe"""
        timeframe_map = {
            '15s': 15,
            '1m': 60,
            '5m': 300,
            '15m': 900,
            '1h': 3600,
            '4h': 14400,
            '24h': 86400
        }
        return timeframe_map.get(timeframe, 60)
