"""
Market Data Tick Handler

A comprehensive system for downloading, processing, and serving high-frequency
tick data from Tardis.dev for cryptocurrency trading instruments.
"""

__version__ = "1.0.0"
__author__ = "Your Organization"
__email__ = "your-email@example.com"

# Import main components
from .models import (
    InstrumentKey, Venue, InstrumentType,
    TickData, TradeData, BookSnapshot, DerivativeTicker, Liquidation,
    GCSFileInfo, GapInfo, DownloadResult, AggregatedDownloadResult,
    QueryResult, ValidationResult, HealthStatus, ErrorResult,
    ErrorType, ErrorSeverity
)

from .config import get_config, ConfigManager, Config
from .instrument_registry import instrument_registry, InstrumentRegistry, InstrumentInfo, InstrumentCategory
from .tardis_connector import TardisConnector, create_tardis_connector
from .storage_manager import StorageManager, LocalStorage, ParquetSerializer
from .gcs_manager import GCSManager, create_gcs_manager
from .data_downloader import DataDownloader, create_data_downloader
from .query_service import QueryService, create_query_service
from .gap_detector import GapDetector, create_gap_detector, DataValidator, DataQualityMetrics, GapSeverity

# API and CLI
from .api import app
from .cli import main as cli_main

__all__ = [
    # Version info
    "__version__",
    "__author__",
    "__email__",
    
    # Models
    "InstrumentKey",
    "Venue", 
    "InstrumentType",
    "TickData",
    "TradeData",
    "BookSnapshot", 
    "DerivativeTicker",
    "Liquidation",
    "GCSFileInfo",
    "GapInfo",
    "DownloadResult",
    "AggregatedDownloadResult",
    "QueryResult",
    "ValidationResult",
    "HealthStatus",
    "ErrorResult",
    "ErrorType",
    "ErrorSeverity",
    
    # Configuration
    "get_config",
    "ConfigManager",
    "Config",
    
    # Instrument Registry
    "instrument_registry",
    "InstrumentRegistry",
    "InstrumentInfo",
    "InstrumentCategory",
    
    # Core Services
    "TardisConnector",
    "create_tardis_connector",
    "StorageManager",
    "LocalStorage",
    "ParquetSerializer",
    "GCSManager",
    "create_gcs_manager",
    "DataDownloader",
    "create_data_downloader",
    "QueryService",
    "create_query_service",
    "GapDetector",
    "create_gap_detector",
    "DataValidator",
    "DataQualityMetrics",
    "GapSeverity",
    
    # API and CLI
    "app",
    "cli_main",
]
