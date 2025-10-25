"""
Market Data Tick Handler

A comprehensive package for downloading, processing, and storing cryptocurrency market data
from Tardis.dev with support for both batch processing and real-time streaming.

Key Features:
- Instrument definition generation and management
- Tick data download and upload to GCS
- Candle processing with HFT features
- Real-time streaming to BigQuery
- Optimized Parquet storage for efficient querying
- Multi-tier authentication support
- Package/library architecture for downstream services

Usage:
    from market_data_tick_handler import DataClient, CandleDataReader
    from market_data_tick_handler.config import get_config
    
    # Initialize
    config = get_config()
    data_client = DataClient(config.gcp.bucket, config)
    candle_reader = CandleDataReader(data_client)
    
    # Read candles
    candles = candle_reader.get_candles(
        instrument_id="BINANCE:SPOT_PAIR:BTC-USDT",
        timeframe="1m",
        start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end_date=datetime(2024, 1, 2, tzinfo=timezone.utc)
    )

    # Streaming
    python -m market_data_tick_handler.main --mode streaming-ticks --symbol BTC-USDT
"""

__version__ = "2.0.0"
__author__ = "Market Data Team"
__email__ = "market-data@yourcompany.com"
__description__ = "Comprehensive market data tick handler for cryptocurrency trading data"

# Core imports for easy access (safe imports that don't trigger config loading)
from .data_client.data_client import DataClient
from .data_client.candle_data_reader import CandleDataReader
from .data_client.tick_data_reader import TickDataReader

# Streaming components
from .streaming_service import CandleBuilder, CandleData, MultiTimeframeProcessor, UTCTimestampManager

# BigQuery components (safe to import)
from .bigquery_uploader.streaming_uploader import StreamingBigQueryUploader

# Lazy import functions for components that require config
def get_historical_candle_processor():
    """Get HistoricalCandleProcessor with lazy import"""
    from .candle_processor.historical_candle_processor import HistoricalCandleProcessor
    return HistoricalCandleProcessor

def get_download_orchestrator():
    """Get DownloadOrchestrator with lazy import"""
    from .data_downloader.download_orchestrator import DownloadOrchestrator
    return DownloadOrchestrator

def get_candle_uploader():
    """Get CandleUploader with lazy import"""
    from .bigquery_uploader.candle_uploader import CandleUploader
    return CandleUploader

# Additional lazy imports for components that may require config
def get_data_validator():
    """Get DataValidator with lazy import"""
    from .data_validator.data_validator import DataValidator
    return DataValidator
# Utils (safe to import)
from .utils.performance_monitor import performance_monitor

__all__ = [
    # Version info
    "__version__",
    "__author__",
    "__email__",
    "__description__",
    
    # Data clients
    "DataClient",
    "CandleDataReader", 
    "TickDataReader",
    "HFTFeaturesReader",
    "MFTFeaturesReader",
    
    # Processors
    "HistoricalCandleProcessor",
    "AggregatedCandleProcessor", 
    "HFTFeatureProcessor",
    
    # Uploaders
    "CandleUploader",
    "UploadOrchestrator",
    
    # Downloaders
    "DownloadOrchestrator",
    "TardisConnector",
    
    # Instrument processing
    "CanonicalInstrumentKeyGenerator",
    "InstrumentGCSUploader",
    
    # Validation
    "DataValidator",
    
    # Utilities
    "setup_structured_logging",
    "ErrorHandler",
    "performance_monitor",
]