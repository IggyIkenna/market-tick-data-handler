"""
Mock Data Client for Offline Development

This module provides a mock data client that can be used for offline development
when GCP access is not available or when testing without real data.
"""

import os
import pandas as pd
import logging
from datetime import datetime, date, timezone
from typing import Optional, List, Dict, Any
from pathlib import Path
import json

logger = logging.getLogger(__name__)


class MockDataClient:
    """Mock data client for offline development and testing"""
    
    def __init__(self, mock_data_path: Optional[str] = None):
        """
        Initialize the mock data client.
        
        Args:
            mock_data_path: Path to mock data directory (defaults to ./mock_data)
        """
        self.mock_data_path = Path(mock_data_path or os.getenv('MOCK_DATA_PATH', './mock_data'))
        self.mock_data_path.mkdir(exist_ok=True)
        
        # Initialize mock data if it doesn't exist
        self._initialize_mock_data()
    
    def _initialize_mock_data(self):
        """Initialize mock data files if they don't exist"""
        if not (self.mock_data_path / 'instruments.json').exists():
            self._create_mock_instruments()
        
        if not (self.mock_data_path / 'candles').exists():
            (self.mock_data_path / 'candles').mkdir(exist_ok=True)
            self._create_mock_candles()
        
        if not (self.mock_data_path / 'ticks').exists():
            (self.mock_data_path / 'ticks').mkdir(exist_ok=True)
            self._create_mock_ticks()
    
    def _create_mock_instruments(self):
        """Create mock instrument definitions"""
        mock_instruments = [
            {
                "instrument_id": "BINANCE:SPOT_PAIR:BTC-USDT",
                "venue": "BINANCE",
                "instrument_type": "SPOT_PAIR",
                "symbol": "BTC-USDT",
                "base_asset": "BTC",
                "quote_asset": "USDT",
                "is_active": True,
                "created_at": "2023-05-23T00:00:00Z"
            },
            {
                "instrument_id": "BINANCE:SPOT_PAIR:ETH-USDT",
                "venue": "BINANCE",
                "instrument_type": "SPOT_PAIR",
                "symbol": "ETH-USDT",
                "base_asset": "ETH",
                "quote_asset": "USDT",
                "is_active": True,
                "created_at": "2023-05-23T00:00:00Z"
            },
            {
                "instrument_id": "DERIBIT:PERP:BTC-USDT",
                "venue": "DERIBIT",
                "instrument_type": "PERP",
                "symbol": "BTC-USDT",
                "base_asset": "BTC",
                "quote_asset": "USDT",
                "is_active": True,
                "created_at": "2023-05-23T00:00:00Z"
            }
        ]
        
        with open(self.mock_data_path / 'instruments.json', 'w') as f:
            json.dump(mock_instruments, f, indent=2)
        
        logger.info(f"Created mock instruments file: {self.mock_data_path / 'instruments.json'}")
    
    def _create_mock_candles(self):
        """Create mock candle data for testing"""
        import numpy as np
        
        # Create mock 1m candles for BTC-USDT
        start_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
        
        # Generate 1440 1-minute candles
        timestamps = pd.date_range(start=start_time, end=end_time, freq='1T', inclusive='left')
        
        # Mock price data (random walk)
        np.random.seed(42)  # For reproducible results
        base_price = 50000.0
        price_changes = np.random.normal(0, 0.001, len(timestamps))
        prices = base_price * np.exp(np.cumsum(price_changes))
        
        candles = []
        for i, (ts, price) in enumerate(zip(timestamps, prices)):
            # Generate OHLCV data
            high = price * (1 + abs(np.random.normal(0, 0.0005)))
            low = price * (1 - abs(np.random.normal(0, 0.0005)))
            open_price = prices[i-1] if i > 0 else price
            close_price = price
            volume = np.random.uniform(0.1, 10.0)
            
            candles.append({
                'timestamp': int(ts.timestamp() * 1000000),  # microseconds
                'open': open_price,
                'high': high,
                'low': low,
                'close': close_price,
                'volume': volume,
                'trade_count': np.random.randint(1, 100),
                'buy_volume_sum': volume * np.random.uniform(0.4, 0.6),
                'sell_volume_sum': volume * np.random.uniform(0.4, 0.6),
                'size_avg': np.random.uniform(0.001, 0.1),
                'price_vwap': price,
                'delay_median': np.random.uniform(1, 10),
                'delay_mean': np.random.uniform(1, 10),
                'delay_max': np.random.uniform(10, 100),
                'delay_min': np.random.uniform(0.1, 1),
                'liquidation_buy_volume': np.random.uniform(0, 1),
                'liquidation_sell_volume': np.random.uniform(0, 1),
                'liquidation_count': np.random.randint(0, 5),
                'funding_rate': np.random.uniform(-0.0001, 0.0001),
                'index_price': price,
                'mark_price': price,
                'open_interest': np.random.uniform(1000, 10000),
                'predicted_funding_rate': np.random.uniform(-0.0001, 0.0001)
            })
        
        # Save as Parquet
        df = pd.DataFrame(candles)
        df.to_parquet(self.mock_data_path / 'candles' / 'BTC-USDT_1m_2024-01-01.parquet', index=False)
        
        logger.info(f"Created mock candles file: {self.mock_data_path / 'candles' / 'BTC-USDT_1m_2024-01-01.parquet'}")
    
    def _create_mock_ticks(self):
        """Create mock tick data for testing"""
        import numpy as np
        
        # Create mock tick data for BTC-USDT
        start_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2024, 1, 1, 12, 5, 0, tzinfo=timezone.utc)
        
        # Generate ticks every few seconds
        timestamps = pd.date_range(start=start_time, end=end_time, freq='2S')
        
        ticks = []
        base_price = 50000.0
        for i, ts in enumerate(timestamps):
            price = base_price + np.random.normal(0, 10)  # Random walk
            size = np.random.uniform(0.001, 0.1)
            side = np.random.choice(['buy', 'sell'])
            
            ticks.append({
                'timestamp': int(ts.timestamp() * 1000000),  # microseconds
                'local_timestamp': int(ts.timestamp() * 1000000) + np.random.randint(1, 1000),
                'id': f"mock_trade_{i}",
                'side': side,
                'price': price,
                'amount': size
            })
        
        # Save as Parquet
        df = pd.DataFrame(ticks)
        df.to_parquet(self.mock_data_path / 'ticks' / 'BTC-USDT_trades_2024-01-01.parquet', index=False)
        
        logger.info(f"Created mock ticks file: {self.mock_data_path / 'ticks' / 'BTC-USDT_trades_2024-01-01.parquet'}")
    
    def get_instruments(self, start_date: date, end_date: date) -> pd.DataFrame:
        """Get mock instrument definitions"""
        with open(self.mock_data_path / 'instruments.json', 'r') as f:
            instruments = json.load(f)
        
        df = pd.DataFrame(instruments)
        df['created_at'] = pd.to_datetime(df['created_at'])
        
        # Filter by date range
        mask = (df['created_at'].dt.date >= start_date) & (df['created_at'].dt.date <= end_date)
        return df[mask]
    
    def get_candles(self, instrument_id: str, timeframe: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Get mock candle data"""
        # For simplicity, return the same mock data regardless of parameters
        file_path = self.mock_data_path / 'candles' / 'BTC-USDT_1m_2024-01-01.parquet'
        
        if file_path.exists():
            df = pd.read_parquet(file_path)
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='us')
            
            # Filter by date range
            mask = (df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)
            return df[mask]
        else:
            logger.warning(f"Mock candles file not found: {file_path}")
            return pd.DataFrame()
    
    def get_tick_data(self, instrument_id: str, start_time: datetime, end_time: datetime, date: date) -> pd.DataFrame:
        """Get mock tick data"""
        # For simplicity, return the same mock data regardless of parameters
        file_path = self.mock_data_path / 'ticks' / 'BTC-USDT_trades_2024-01-01.parquet'
        
        if file_path.exists():
            df = pd.read_parquet(file_path)
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='us')
            df['local_timestamp'] = pd.to_datetime(df['local_timestamp'], unit='us')
            
            # Filter by time range
            mask = (df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)
            return df[mask]
        else:
            logger.warning(f"Mock ticks file not found: {file_path}")
            return pd.DataFrame()
    
    def get_hft_features(self, instrument_id: str, timeframe: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Get mock HFT features (same as candles for simplicity)"""
        return self.get_candles(instrument_id, timeframe, start_date, end_date)
    
    def get_mft_features(self, instrument_id: str, timeframe: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Get mock MFT features (same as candles for simplicity)"""
        return self.get_candles(instrument_id, timeframe, start_date, end_date)
    
    def upload_candles(self, candles: pd.DataFrame, instrument_id: str, timeframe: str, date: date) -> bool:
        """Mock upload (just logs the action)"""
        logger.info(f"Mock upload: {len(candles)} candles for {instrument_id} {timeframe} on {date}")
        return True
    
    def upload_ticks(self, ticks: pd.DataFrame, instrument_id: str, data_type: str, date: date) -> bool:
        """Mock upload (just logs the action)"""
        logger.info(f"Mock upload: {len(ticks)} ticks for {instrument_id} {data_type} on {date}")
        return True
