"""
Integrated Streaming Service

Connects the live candle processor with the cost-optimized BigQuery batching uploader.
Provides unified streaming architecture for both ticks and candles with:
- Real-time processing using Python (faster than Node.js for computation)
- 1-minute BigQuery batching for cost optimization
- Proper integration with existing HFT features
"""

import asyncio
import logging
import pandas as pd
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

from .candle_processor.live_candle_processor import LiveCandleProcessor, LiveProcessingConfig
from .tick_processor.tick_handler import TickHandler
from ..bigquery_uploader.streaming_uploader import StreamingBigQueryUploader
from ..config import get_config

logger = logging.getLogger(__name__)

@dataclass
class IntegratedStreamingConfig:
    """Configuration for integrated streaming service"""
    symbol: str = 'BTC-USDT'
    exchange: str = 'binance' 
    timeframes: List[str] = None
    data_types: List[str] = None
    enable_hft_features: bool = True
    enable_bigquery_upload: bool = True
    batch_interval_seconds: int = 60  # 1-minute batching
    duration_seconds: int = 0  # 0 = infinite
    
    def __post_init__(self):
        if self.timeframes is None:
            self.timeframes = ['15s', '1m', '5m', '15m']
        if self.data_types is None:
            self.data_types = ['trades', 'book_snapshot_5', 'derivative_ticker', 'liquidations']

class IntegratedStreamingService:
    """
    Unified streaming service that integrates:
    - Live candle processing (Python for speed)
    - Cost-optimized BigQuery batching
    - Multi-timeframe support
    - HFT features computation
    """
    
    def __init__(self, config: IntegratedStreamingConfig = None):
        self.config = config or IntegratedStreamingConfig()
        self.app_config = get_config()
        
        # Initialize components
        self._init_processors()
        self._init_bigquery_uploader()
        
        # State management
        self.running = False
        self.stats = {
            'ticks_processed': 0,
            'candles_generated': 0,
            'batches_uploaded': 0,
            'start_time': None,
            'errors': []
        }
        
    def _init_processors(self):
        """Initialize live processors"""
        
        # Live candle processor config
        live_config = LiveProcessingConfig(
            timeframes=self.config.timeframes,
            enable_hft_features=self.config.enable_hft_features,
            max_history=100,
            buffer_size=1000
        )
        
        # Initialize processors
        self.candle_processor = LiveCandleProcessor(
            symbol=self.config.symbol,
            config=live_config
        )
        
        # Tick handler for raw tick processing
        self.tick_handler = TickHandler()
        
        logger.info(f"🔧 Initialized processors for {self.config.symbol}")
        logger.info(f"  Timeframes: {self.config.timeframes}")
        logger.info(f"  Data types: {self.config.data_types}")
    
    def _init_bigquery_uploader(self):
        """Initialize BigQuery uploader with batching"""
        
        if self.config.enable_bigquery_upload:
            self.bigquery_uploader = StreamingBigQueryUploader(
                project_id=self.app_config.gcp.project_id,
                dataset_id="market_data_streaming",
                batch_interval_seconds=self.config.batch_interval_seconds
            )
            logger.info(f"🔧 Initialized BigQuery uploader with {self.config.batch_interval_seconds}s batching")
        else:
            self.bigquery_uploader = None
            logger.info("🔧 BigQuery upload disabled")
    
    async def start_streaming(self):
        """Start the integrated streaming service"""
        
        logger.info(f"🚀 Starting integrated streaming for {self.config.exchange}:{self.config.symbol}")
        logger.info(f"⏱️ Duration: {'infinite' if self.config.duration_seconds == 0 else f'{self.config.duration_seconds}s'}")
        
        self.running = True
        self.stats['start_time'] = datetime.now(timezone.utc)
        
        try:
            # Start streaming from exchange (simulate with historical data for now)
            await self._stream_from_exchange()
            
        except Exception as e:
            logger.error(f"❌ Streaming service error: {e}")
            self.stats['errors'].append(str(e))
            raise
        finally:
            await self.shutdown()
    
    async def _stream_from_exchange(self):
        """Stream data from exchange (simulated with realistic data)"""
        
        start_time = datetime.now(timezone.utc)
        tick_counter = 0
        
        logger.info(f"📡 Starting data stream for {self.config.symbol}")
        
        while self.running:
            # Check duration limit
            if self.config.duration_seconds > 0:
                elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
                if elapsed >= self.config.duration_seconds:
                    logger.info(f"⏰ Duration limit reached ({self.config.duration_seconds}s)")
                    break
            
            # Generate realistic tick data
            tick_data = self._generate_realistic_tick(tick_counter)
            
            # Process tick through candle processor
            completed_candles = await self.candle_processor.process_tick(tick_data)
            
            # Add completed candles to BigQuery batches
            if completed_candles and self.bigquery_uploader:
                await self._add_candles_to_batches(completed_candles)
            
            # Process raw tick data
            if 'trades' in self.config.data_types:
                await self._add_tick_to_batch(tick_data)
            
            # Update stats
            self.stats['ticks_processed'] += 1
            self.stats['candles_generated'] += len(completed_candles)
            
            tick_counter += 1
            
            # Log progress every 100 ticks
            if tick_counter % 100 == 0:
                logger.info(f"📊 Processed {tick_counter} ticks, generated {self.stats['candles_generated']} candles")
                
                if self.bigquery_uploader:
                    batch_stats = self.bigquery_uploader.get_batch_stats()
                    logger.info(f"  📈 Batch queue status: {self._format_batch_stats(batch_stats)}")
            
            # Realistic tick frequency (simulate ~10 ticks/second)
            await asyncio.sleep(0.1)
    
    def _generate_realistic_tick(self, counter: int) -> Dict[str, Any]:
        """Generate realistic tick data for testing"""
        
        now = datetime.now(timezone.utc)
        
        # Realistic BTC price with small oscillations
        base_price = 45000.0
        price_offset = (counter % 100) * 5 + (counter % 10) * 0.5
        price = base_price + price_offset
        
        # Realistic volume
        volume = 0.01 + (counter % 20) * 0.005
        
        return {
            'symbol': self.config.symbol,
            'exchange': self.config.exchange,
            'timestamp': now,
            'local_timestamp': now - timedelta(milliseconds=50),  # 50ms latency
            'price': price,
            'amount': volume,
            'side': 'buy' if counter % 3 == 0 else 'sell',
            'trade_id': f'trade_{counter:08d}'
        }
    
    async def _add_candles_to_batches(self, completed_candles: List):
        """Add completed candles to BigQuery batch queues"""
        
        candles_by_timeframe = defaultdict(list)
        
        # Group candles by timeframe
        for candle in completed_candles:
            candle_dict = {
                'symbol': candle.symbol,
                'exchange': candle.exchange,
                'timeframe': candle.timeframe,
                'timestamp': candle.timestamp_in,
                'timestamp_out': candle.timestamp_out,
                'instrument_id': f"{candle.exchange.upper()}:SPOT_PAIR:{candle.symbol}",
                'open': candle.open,
                'high': candle.high,
                'low': candle.low,
                'close': candle.close,
                'volume': candle.volume,
                'trade_count': candle.trade_count,
                'vwap': candle.vwap
            }
            
            # Add HFT features if available
            if hasattr(candle, 'hft_features') and candle.hft_features:
                candle_dict.update(candle.hft_features.__dict__)
            
            candles_by_timeframe[candle.timeframe].append(candle_dict)
        
        # Add each timeframe to batch queue
        for timeframe, candle_list in candles_by_timeframe.items():
            if candle_list:
                candles_df = pd.DataFrame(candle_list)
                rows_added = self.bigquery_uploader.add_streaming_candles(candles_df, timeframe)
                logger.debug(f"Added {rows_added} {timeframe} candles to batch queue")
    
    async def _add_tick_to_batch(self, tick_data: Dict[str, Any]):
        """Add tick data to BigQuery batch queue"""
        
        # Convert tick to DataFrame format
        tick_dict = {
            'symbol': tick_data['symbol'],
            'exchange': tick_data['exchange'],
            'timestamp': tick_data['timestamp'],
            'timestamp_out': datetime.now(timezone.utc),  # Processing timestamp
            'local_timestamp': tick_data.get('local_timestamp', tick_data['timestamp']),
            'instrument_id': f"{tick_data['exchange'].upper()}:SPOT_PAIR:{tick_data['symbol']}",
            'data_type': 'trades',
            'price': tick_data['price'],
            'amount': tick_data['amount'],
            'side': tick_data['side'],
            'trade_id': tick_data.get('trade_id', '')
        }
        
        tick_df = pd.DataFrame([tick_dict])
        rows_added = self.bigquery_uploader.add_streaming_ticks(tick_df, 'trades')
        logger.debug(f"Added {rows_added} ticks to batch queue")
    
    def _format_batch_stats(self, stats: Dict[str, Any]) -> str:
        """Format batch statistics for logging"""
        
        parts = []
        
        if stats.get('queued_batches'):
            total_queued = sum(data['queued_rows'] for data in stats['queued_batches'].values())
            parts.append(f"{total_queued} rows queued")
        
        if stats.get('total_stats'):
            total_batches = sum(data['batches'] for data in stats['total_stats'].values())
            parts.append(f"{total_batches} batches uploaded")
        
        return " | ".join(parts) if parts else "No data"
    
    async def shutdown(self):
        """Gracefully shutdown the streaming service"""
        
        logger.info("🛑 Shutting down integrated streaming service...")
        
        self.running = False
        
        # Shutdown candle processor
        await self.candle_processor.shutdown()
        
        # Force flush all pending batches
        if self.bigquery_uploader:
            await self.bigquery_uploader.force_flush_all()
            self.bigquery_uploader.stop_batch_flusher()
        
        # Log final stats
        duration = (datetime.now(timezone.utc) - self.stats['start_time']).total_seconds()
        
        logger.info("📊 Final streaming statistics:")
        logger.info(f"  Duration: {duration:.1f} seconds")
        logger.info(f"  Ticks processed: {self.stats['ticks_processed']}")
        logger.info(f"  Candles generated: {self.stats['candles_generated']}")
        logger.info(f"  Errors: {len(self.stats['errors'])}")
        
        if self.bigquery_uploader:
            final_batch_stats = self.bigquery_uploader.get_batch_stats()
            logger.info(f"  Final batch stats: {self._format_batch_stats(final_batch_stats)}")
        
        logger.info("✅ Integrated streaming service shutdown complete")
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get comprehensive performance statistics"""
        
        stats = self.stats.copy()
        
        if self.stats['start_time']:
            duration = (datetime.now(timezone.utc) - self.stats['start_time']).total_seconds()
            stats['duration_seconds'] = duration
            stats['ticks_per_second'] = self.stats['ticks_processed'] / duration if duration > 0 else 0
            stats['candles_per_minute'] = self.stats['candles_generated'] / (duration / 60) if duration > 0 else 0
        
        # Add candle processor stats
        stats['candle_processor_stats'] = self.candle_processor.get_stats()
        
        # Add BigQuery batch stats
        if self.bigquery_uploader:
            stats['bigquery_batch_stats'] = self.bigquery_uploader.get_batch_stats()
        
        return stats

# Integration function for existing streaming handlers
async def create_integrated_streaming_service(
    symbol: str = 'BTC-USDT',
    exchange: str = 'binance',
    duration_seconds: int = 0,
    timeframes: List[str] = None,
    enable_bigquery: bool = True
) -> IntegratedStreamingService:
    """Factory function to create integrated streaming service"""
    
    config = IntegratedStreamingConfig(
        symbol=symbol,
        exchange=exchange,
        timeframes=timeframes or ['15s', '1m', '5m'],
        enable_bigquery_upload=enable_bigquery,
        duration_seconds=duration_seconds
    )
    
    return IntegratedStreamingService(config)

# Example usage
if __name__ == "__main__":
    async def demo_integrated_streaming():
        """Demonstrate integrated streaming with BigQuery batching"""
        
        logger.info("🚀 Starting integrated streaming demonstration")
        
        # Create service
        service = await create_integrated_streaming_service(
            symbol='BTC-USDT',
            exchange='binance',
            duration_seconds=120,  # 2 minutes
            timeframes=['1m', '5m'],
            enable_bigquery=True
        )
        
        # Start streaming
        await service.start_streaming()
        
        # Show final performance stats
        stats = service.get_performance_stats()
        logger.info(f"📊 Performance stats: {stats}")
    
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    # Run demo
    asyncio.run(demo_integrated_streaming())
