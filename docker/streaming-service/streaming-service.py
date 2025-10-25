#!/usr/bin/env python3
"""
Docker entry point for Market Data Streaming Service

Supports both modes:
1. Raw tick streaming to BigQuery
2. Multi-timeframe candle processing with HFT features
"""

import asyncio
import os
import sys
import signal
import logging
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from src.utils.logger import setup_structured_logging
from src.streaming_service.tick_streamer.live_tick_streamer import LiveTickStreamer, StreamingConfig

logger = logging.getLogger(__name__)


def get_config_from_env() -> StreamingConfig:
    """Get streaming configuration from environment variables"""
    
    # Required environment variables
    mode = os.getenv('STREAMING_MODE', 'candles')
    symbol = os.getenv('STREAMING_SYMBOL', 'BTC-USDT')
    exchange = os.getenv('STREAMING_EXCHANGE', 'binance')
    
    # Optional configuration
    duration = None
    duration_str = os.getenv('STREAMING_DURATION')
    if duration_str and duration_str != '0':
        duration = int(duration_str)
    
    # BigQuery configuration (for ticks mode)
    bigquery_dataset = os.getenv('BIGQUERY_DATASET')
    bigquery_table = os.getenv('BIGQUERY_TABLE')
    
    # Timeframes configuration (for candles mode)
    timeframes_str = os.getenv('STREAMING_TIMEFRAMES', '15s,1m,5m,15m,4h,24h')
    timeframes = [tf.strip() for tf in timeframes_str.split(',')]
    
    # HFT features
    enable_hft_features = os.getenv('ENABLE_HFT_FEATURES', 'true').lower() == 'true'
    
    return StreamingConfig(
        mode=mode,
        symbol=symbol,
        exchange=exchange,
        duration=duration,
        bigquery_dataset=bigquery_dataset,
        bigquery_table=bigquery_table,
        timeframes=timeframes,
        enable_hft_features=enable_hft_features
    )


async def main():
    """Main entry point for Docker container"""
    
    # Setup logging
    log_level = os.getenv('LOG_LEVEL', 'INFO')
    setup_structured_logging(
        service_name="streaming-service",
        log_level=log_level
    )
    
    logger.info("🚀 Market Data Streaming Service starting...")
    
    # Validate required environment variables
    tardis_api_key = os.getenv('TARDIS_API_KEY')
    if not tardis_api_key:
        logger.error("❌ TARDIS_API_KEY environment variable is required")
        sys.exit(1)
    
    # Get configuration
    config = get_config_from_env()
    
    logger.info("📊 Streaming Configuration:")
    logger.info(f"   Mode: {config.mode}")
    logger.info(f"   Symbol: {config.symbol}")
    logger.info(f"   Exchange: {config.exchange}")
    logger.info(f"   Duration: {config.duration}s" if config.duration else "   Duration: Infinite")
    
    if config.mode == 'ticks':
        logger.info(f"   BigQuery Dataset: {config.bigquery_dataset}")
        logger.info(f"   BigQuery Table: {config.bigquery_table}")
    elif config.mode == 'candles':
        logger.info(f"   Timeframes: {config.timeframes}")
        logger.info(f"   HFT Features: {config.enable_hft_features}")
    
    # Create and start streamer
    try:
        streamer = LiveTickStreamer(config)
        
        # Setup signal handlers for graceful shutdown
        def signal_handler(signum, frame):
            logger.info(f"🛑 Received signal {signum}, stopping streamer...")
            asyncio.create_task(streamer.stop())
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Start streaming
        await streamer.start_streaming()
        
    except KeyboardInterrupt:
        logger.info("🛑 Streaming interrupted by user")
    except Exception as e:
        logger.error(f"❌ Streaming service error: {e}")
        sys.exit(1)
    
    logger.info("✅ Streaming service stopped")


if __name__ == "__main__":
    asyncio.run(main())
