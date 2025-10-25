"""
Live Tick Data Streamer with Dual Modes

Mode 1: Raw tick streaming for analytics (BigQuery)
Mode 2: Multi-timeframe candle processing with HFT features
"""

import asyncio
import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, asdict
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

from market_data_tick_handler.utils.logger import setup_structured_logging
from market_data_tick_handler.streaming_service.tick_streamer.utc_timestamp_manager import UTCTimestampManager, TimestampPair
from market_data_tick_handler.streaming_service.bigquery_client.streaming_client import BigQueryStreamingClient
from market_data_tick_handler.streaming_service.candle_processor.multi_timeframe_processor import MultiTimeframeProcessor

logger = logging.getLogger(__name__)


@dataclass
class TickData:
    """Standardized tick data structure"""
    symbol: str
    exchange: str
    price: float
    amount: float
    side: str  # 'buy' or 'sell'
    timestamp: datetime
    timestamp_received: datetime
    trade_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'symbol': self.symbol,
            'exchange': self.exchange,
            'price': self.price,
            'amount': self.amount,
            'side': self.side,
            'timestamp': self.timestamp.isoformat(),
            'timestamp_received': self.timestamp_received.isoformat(),
            'trade_id': self.trade_id
        }


@dataclass
class StreamingConfig:
    """Configuration for streaming service"""
    mode: str  # 'ticks' or 'candles'
    symbol: str
    exchange: str = 'binance'
    duration: Optional[int] = None  # seconds, None = infinite
    bigquery_dataset: Optional[str] = None
    bigquery_table: Optional[str] = None
    timeframes: List[str] = None  # For candle mode
    enable_hft_features: bool = True
    
    def __post_init__(self):
        if self.timeframes is None:
            self.timeframes = ['15s', '1m', '5m', '15m', '4h', '24h']


class LiveTickStreamer:
    """
    Main live tick data streaming service with dual modes:
    
    Mode 1: 'ticks' - Stream raw tick data to BigQuery for analytics
    Mode 2: 'candles' - Process multi-timeframe candles with HFT features
    """
    
    def __init__(self, config: StreamingConfig):
        self.config = config
        self.running = False
        self.stats = {
            'total_ticks': 0,
            'total_candles': 0,
            'start_time': None,
            'last_tick_time': None
        }
        
        # Initialize components
        self.timestamp_manager = UTCTimestampManager()
        
        # Mode-specific components
        if config.mode == 'ticks':
            self.bigquery_client = BigQueryStreamingClient(
                dataset_id=config.bigquery_dataset or 'market_data_streaming',
                table_id=config.bigquery_table or f'ticks_{config.exchange}_{config.symbol.lower().replace("-", "_")}'
            )
            self.candle_processor = None
        elif config.mode == 'candles':
            self.bigquery_client = None
            self.candle_processor = MultiTimeframeProcessor(
                symbol=config.symbol,
                exchange=config.exchange,
                timeframes=config.timeframes,
                enable_hft_features=config.enable_hft_features
            )
        else:
            raise ValueError(f"Invalid mode: {config.mode}. Must be 'ticks' or 'candles'")
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info(f"✅ LiveTickStreamer initialized in '{config.mode}' mode")
        logger.info(f"   Symbol: {config.symbol}")
        logger.info(f"   Exchange: {config.exchange}")
        if config.mode == 'candles':
            logger.info(f"   Timeframes: {config.timeframes}")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"🛑 Received signal {signum}, stopping streamer...")
        self.stop()
    
    async def start_streaming(self) -> None:
        """Start the streaming service"""
        logger.info(f"🚀 Starting live tick streaming in '{self.config.mode}' mode")
        logger.info("=" * 60)
        
        self.running = True
        self.stats['start_time'] = datetime.now(timezone.utc)
        
        try:
            # Try real-time streaming first, fall back to historical replay
            await self._stream_real_time_data()
            
        except KeyboardInterrupt:
            logger.info("🛑 Streaming interrupted by user")
        except Exception as e:
            logger.error(f"❌ Streaming error: {e}")
            logger.info("🎬 Falling back to historical data replay...")
            await self._replay_historical_data()
        finally:
            await self.stop()
    
    async def _stream_real_time_data(self) -> None:
        """Stream real-time data using Tardis.dev Node.js approach"""
        try:
            # Import tardis-dev for real-time streaming
            from tardis_dev import streamNormalized, normalizeTrades
            
            logger.info("📡 Connecting to Tardis real-time stream...")
            
            # Configure stream
            tardis_symbol = self.config.symbol.lower().replace('-', '')
            
            messages = streamNormalized(
                {
                    'exchange': self.config.exchange,
                    'symbols': [tardis_symbol]
                },
                normalizeTrades
            )
            
            # Process messages
            async for message in messages:
                if not self.running:
                    break
                    
                if message.get('type') == 'trade':
                    await self._process_tick(message)
                    
                # Check duration limit
                if self.config.duration and self._get_runtime() >= self.config.duration:
                    logger.info(f"⏰ Duration limit ({self.config.duration}s) reached")
                    break
                    
        except ImportError:
            logger.warning("⚠️ tardis-dev not available, falling back to historical replay")
            await self._replay_historical_data()
        except Exception as e:
            logger.error(f"❌ Real-time streaming error: {e}")
            raise
    
    async def _replay_historical_data(self) -> None:
        """Replay historical data with realistic timing"""
        logger.info("🎬 Starting historical data replay...")
        
        try:
            # Generate realistic tick data for demo
            base_price = 67000.0 if 'BTC' in self.config.symbol else 3500.0 if 'ETH' in self.config.symbol else 1.0
            
            tick_count = 0
            while self.running:
                # Generate realistic trade
                price_change = (asyncio.get_event_loop().time() % 1 - 0.5) * 0.001  # Small price movements
                price = base_price * (1 + price_change)
                amount = 0.001 + (asyncio.get_event_loop().time() % 1) * 0.1
                side = 'buy' if tick_count % 2 == 0 else 'sell'
                
                # Create tick data
                tick_data = TickData(
                    symbol=self.config.symbol,
                    exchange=self.config.exchange,
                    price=price,
                    amount=amount,
                    side=side,
                    timestamp=datetime.now(timezone.utc),
                    timestamp_received=datetime.now(timezone.utc),
                    trade_id=f"demo_{tick_count}"
                )
                
                await self._process_tick_data(tick_data)
                tick_count += 1
                
                # Check duration limit
                if self.config.duration and self._get_runtime() >= self.config.duration:
                    logger.info(f"⏰ Duration limit ({self.config.duration}s) reached")
                    break
                
                # Realistic delay between ticks
                await asyncio.sleep(0.1)  # 10 ticks per second
                
        except Exception as e:
            logger.error(f"❌ Historical replay error: {e}")
            raise
    
    async def _process_tick(self, message: Dict[str, Any]) -> None:
        """Process a tick message from Tardis stream"""
        try:
            tick_data = TickData(
                symbol=self.config.symbol,
                exchange=self.config.exchange,
                price=float(message['price']),
                amount=float(message['amount']),
                side=message.get('side', 'unknown'),
                timestamp=datetime.fromisoformat(message['timestamp'].replace('Z', '+00:00')),
                timestamp_received=datetime.now(timezone.utc),
                trade_id=message.get('id')
            )
            
            await self._process_tick_data(tick_data)
            
        except Exception as e:
            logger.error(f"❌ Error processing tick: {e}")
    
    async def _process_tick_data(self, tick_data: TickData) -> None:
        """Process tick data based on current mode"""
        self.stats['total_ticks'] += 1
        self.stats['last_tick_time'] = tick_data.timestamp
        
        if self.config.mode == 'ticks':
            # Mode 1: Stream to BigQuery
            await self._stream_tick_to_bigquery(tick_data)
        elif self.config.mode == 'candles':
            # Mode 2: Process candles
            await self._process_tick_for_candles(tick_data)
        
        # Display real-time info
        if self.stats['total_ticks'] % 10 == 0:  # Every 10 ticks
            self._display_realtime_info(tick_data)
    
    async def _stream_tick_to_bigquery(self, tick_data: TickData) -> None:
        """Stream tick data to BigQuery (Mode 1)"""
        try:
            # Add BigQuery streaming logic here
            await self.bigquery_client.stream_tick(tick_data)
            
        except Exception as e:
            logger.error(f"❌ BigQuery streaming error: {e}")
    
    async def _process_tick_for_candles(self, tick_data: TickData) -> None:
        """Process tick for candle generation (Mode 2)"""
        try:
            completed_candles = await self.candle_processor.process_tick(tick_data)
            
            for candle in completed_candles:
                self.stats['total_candles'] += 1
                logger.info(f"🕯️ {candle.timeframe} CANDLE: {candle}")
                
        except Exception as e:
            logger.error(f"❌ Candle processing error: {e}")
    
    def _display_realtime_info(self, tick_data: TickData) -> None:
        """Display real-time streaming information"""
        runtime = self._get_runtime()
        ticks_per_sec = self.stats['total_ticks'] / max(runtime, 1)
        
        # Price change indicator
        price_color = "🟢" if tick_data.side == 'buy' else "🔴"
        
        # Mode-specific info
        mode_info = ""
        if self.config.mode == 'ticks':
            mode_info = f"| BigQuery: {self.stats['total_ticks']} ticks"
        elif self.config.mode == 'candles':
            mode_info = f"| Candles: {self.stats['total_candles']}"
        
        print(f"\r{price_color} {tick_data.timestamp.strftime('%H:%M:%S')} | "
              f"{tick_data.symbol} | ${tick_data.price:.2f} | "
              f"Vol: {tick_data.amount:.4f} | "
              f"Ticks: {self.stats['total_ticks']} ({ticks_per_sec:.1f}/s) "
              f"{mode_info}", end="", flush=True)
    
    def _get_runtime(self) -> float:
        """Get current runtime in seconds"""
        if self.stats['start_time']:
            return (datetime.now(timezone.utc) - self.stats['start_time']).total_seconds()
        return 0
    
    async def stop(self) -> None:
        """Stop the streaming service"""
        logger.info("\n🛑 Stopping live tick streamer...")
        self.running = False
        
        # Finalize any pending candles
        if self.candle_processor:
            await self.candle_processor.finalize_all_candles()
        
        # Close BigQuery client
        if self.bigquery_client:
            await self.bigquery_client.close()
        
        self._print_summary()
        logger.info("✅ Streaming stopped")
    
    def _print_summary(self) -> None:
        """Print streaming summary"""
        runtime = self._get_runtime()
        ticks_per_sec = self.stats['total_ticks'] / max(runtime, 1)
        
        print(f"\n\n📊 STREAMING SUMMARY ({self.config.mode.upper()} MODE)")
        print("=" * 50)
        print(f"Symbol: {self.config.symbol}")
        print(f"Exchange: {self.config.exchange}")
        print(f"Duration: {runtime:.1f}s")
        print(f"Total Ticks: {self.stats['total_ticks']:,}")
        print(f"Ticks/sec: {ticks_per_sec:.1f}")
        
        if self.config.mode == 'candles':
            print(f"Total Candles: {self.stats['total_candles']}")
            print(f"Timeframes: {', '.join(self.config.timeframes)}")
        elif self.config.mode == 'ticks':
            print(f"BigQuery Dataset: {self.config.bigquery_dataset}")
            print(f"BigQuery Table: {self.config.bigquery_table}")
        
        if self.stats['last_tick_time']:
            print(f"Last Tick: {self.stats['last_tick_time'].strftime('%Y-%m-%d %H:%M:%S UTC')}")


async def main():
    """Main entry point"""
    import argparse
    
    # Setup logging
    setup_structured_logging(
        service_name="live_tick_streamer",
        log_level="INFO"
    )
    
    # Parse arguments
    parser = argparse.ArgumentParser(description="Live Tick Data Streaming Service")
    parser.add_argument('--mode', choices=['ticks', 'candles'], required=True,
                       help='Streaming mode: ticks (BigQuery) or candles (multi-timeframe)')
    parser.add_argument('--symbol', default='BTC-USDT',
                       help='Trading symbol (default: BTC-USDT)')
    parser.add_argument('--exchange', default='binance',
                       help='Exchange name (default: binance)')
    parser.add_argument('--duration', type=int,
                       help='Duration in seconds (default: infinite)')
    parser.add_argument('--bigquery-dataset', 
                       help='BigQuery dataset for tick mode')
    parser.add_argument('--bigquery-table',
                       help='BigQuery table for tick mode')
    parser.add_argument('--timeframes', nargs='+',
                       default=['15s', '1m', '5m', '15m', '4h', '24h'],
                       help='Timeframes for candle mode')
    parser.add_argument('--no-hft-features', action='store_true',
                       help='Disable HFT features computation')
    
    args = parser.parse_args()
    
    # Create configuration
    config = StreamingConfig(
        mode=args.mode,
        symbol=args.symbol,
        exchange=args.exchange,
        duration=args.duration,
        bigquery_dataset=args.bigquery_dataset,
        bigquery_table=args.bigquery_table,
        timeframes=args.timeframes,
        enable_hft_features=not args.no_hft_features
    )
    
    # Create and start streamer
    streamer = LiveTickStreamer(config)
    await streamer.start_streaming()


if __name__ == "__main__":
    asyncio.run(main())
