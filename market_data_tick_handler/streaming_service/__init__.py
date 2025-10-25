"""
Unified Real-Time Market Data Streaming Service

CONSOLIDATED architecture with clear separation of concerns:

1. **Node.js Ingestion Layer**: WebSocket streaming from Tardis.dev
2. **Python Processing Layer**: Tick processing, candle generation, HFT features
3. **Mode Separation**: 
   - Serve Mode: Features → importable by downstream services
   - Persist Mode: Data → BigQuery for analytics

Key Features:
- UNIFIED HFT features code (same for historical and live)
- Complete data type support with fallback strategies (Issue #003)
- Live CCXT instrument definitions (Issue #004)
- Optimized BigQuery partitioning and clustering
- Exchange symbol clustering for all tables
- UTC timestamp alignment and latency tracking
"""

# Core streaming components
from .tick_processor import TickHandler, DataTypeRouter
from .candle_processor.live_candle_processor import LiveCandleProcessor
from .hft_features.feature_calculator import HFTFeatureCalculator, HFTFeatures

# Instrument service (Issue #004)
from .instrument_service import LiveInstrumentProvider, CCXTAdapter, InstrumentMapper

# BigQuery streaming
from .bigquery_client.streaming_client import BigQueryStreamingClient, StreamingConfig

# Mode separation
from .modes import ServeMode, PersistMode
from .modes.serve_mode import LiveFeatureStream

# Legacy components (maintained for compatibility)
from .tick_streamer.live_tick_streamer import LiveTickStreamer
from .candle_processor.multi_timeframe_processor import MultiTimeframeProcessor

__version__ = "2.0.0"
__all__ = [
    # New unified components
    "TickHandler",
    "DataTypeRouter", 
    "LiveCandleProcessor",
    "HFTFeatureCalculator",
    "HFTFeatures",
    
    # Instrument service
    "LiveInstrumentProvider",
    "CCXTAdapter",
    "InstrumentMapper",
    
    # BigQuery streaming
    "BigQueryStreamingClient",
    "StreamingConfig",
    
    # Mode separation
    "ServeMode",
    "PersistMode", 
    "LiveFeatureStream",
    
    # Legacy components
    "LiveTickStreamer",
    "MultiTimeframeProcessor"
]
