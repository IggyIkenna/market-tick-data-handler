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
from .candle_processor.candle_data import CandleBuilder, CandleData
from .candle_processor.multi_timeframe_processor import MultiTimeframeProcessor
from .tick_streamer.utc_timestamp_manager import UTCTimestampManager

# Modes
from .modes.serve_mode import ServeMode, LiveFeatureStream, ServeConfig
from .modes.persist_mode import PersistMode, PersistConfig

# Processing components
from .candle_processor.live_candle_processor import LiveCandleProcessor
from .hft_features.feature_calculator import HFTFeatureCalculator, HFTFeatures
from .tick_processor.tick_handler import TickHandler

# Node.js integration
from .node_ingestion.python_websocket_server import PythonWebSocketServer

# Integrated streaming service - removed, using Node.js + Python WebSocket architecture

# Streaming service components (import lazily to avoid circular imports)
def get_live_tick_streamer():
    """Get LiveTickStreamer with lazy import to avoid circular dependencies"""
    from .tick_streamer.live_tick_streamer import LiveTickStreamer
    return LiveTickStreamer

__version__ = "2.0.0"
__all__ = [
    # Core components
    "CandleBuilder",
    "CandleData", 
    "MultiTimeframeProcessor",
    "UTCTimestampManager",
    
    # Modes
    "ServeMode",
    "LiveFeatureStream", 
    "ServeConfig",
    "PersistMode",
    "PersistConfig",
    
    # Processing
    "LiveCandleProcessor",
    "HFTFeatureCalculator",
    "HFTFeatures",
    "TickHandler",
    
    # Integration
    "PythonWebSocketServer",
    
    # Legacy
    "get_live_tick_streamer"
]
