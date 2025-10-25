"""
Data Client Package

Provides clean data access methods for downstream services to read:
- Tick data from GCS with timestamp filtering
- Candle data (15s to 24h timeframes)
- HFT features (15s, 1m)
- MFT features (1m and above)
"""

from .data_client import DataClient
from .tick_data_reader import TickDataReader
from .candle_data_reader import CandleDataReader
from .hft_features_reader import HFTFeaturesReader
from .mft_features_reader import MFTFeaturesReader

__all__ = [
    'DataClient',
    'TickDataReader', 
    'CandleDataReader',
    'HFTFeaturesReader',
    'MFTFeaturesReader'
]
