#!/usr/bin/env python3
"""
Streaming BigQuery Batch Upload Example

Demonstrates the cost-optimized 1-minute batching system for streaming data:
- Add streaming data to queues (non-blocking)
- Automatic 1-minute batch flushing in background
- Manual flush control for shutdown
- Batch statistics monitoring
"""

import asyncio
import pandas as pd
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
import sys
import logging

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from market_data_tick_handler.bigquery_uploader.streaming_uploader import StreamingBigQueryUploader
from market_data_tick_handler.config import get_config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StreamingBatchDemo:
    """Demonstrates streaming batch upload with cost optimization"""
    
    def __init__(self):
        self.config = get_config()
        
        # Initialize streaming uploader with 1-minute batching
        self.uploader = StreamingBigQueryUploader(
            project_id=self.config.gcp.project_id,
            dataset_id="streaming_demo",
            batch_interval_seconds=60  # 1 minute batches
        )
        
    def generate_sample_candles(self, count: int = 10) -> pd.DataFrame:
        """Generate sample candle data"""
        
        now = datetime.now(timezone.utc)
        
        data = []
        for i in range(count):
            timestamp = now - timedelta(minutes=i)
            timestamp_out = timestamp + timedelta(milliseconds=200)
            
            data.append({
                'symbol': 'BTC-USDT',
                'exchange': 'binance',
                'timeframe': '1m',
                'timestamp': timestamp,
                'timestamp_out': timestamp_out,
                'instrument_id': 'BINANCE:SPOT_PAIR:BTC-USDT',
                'open': 45000.0 + i,
                'high': 45100.0 + i,
                'low': 44900.0 + i,
                'close': 45050.0 + i,
                'volume': 100.0 + i,
                'trade_count': 50 + i,
                'vwap': 45025.0 + i,
                'buy_volume_sum': 60.0 + i,
                'sell_volume_sum': 40.0 + i,
                'price_vwap': 45025.0 + i
            })
        
        return pd.DataFrame(data)
    
    def generate_sample_ticks(self, count: int = 100) -> pd.DataFrame:
        """Generate sample tick data"""
        
        now = datetime.now(timezone.utc)
        
        data = []
        for i in range(count):
            timestamp = now - timedelta(seconds=i)
            timestamp_out = timestamp + timedelta(milliseconds=200)
            
            data.append({
                'symbol': 'BTC-USDT',
                'exchange': 'binance',
                'timestamp': timestamp,
                'timestamp_out': timestamp_out,
                'local_timestamp': timestamp - timedelta(milliseconds=50),
                'instrument_id': 'BINANCE:SPOT_PAIR:BTC-USDT',
                'data_type': 'trades',
                'price': 45000.0 + (i % 100),
                'amount': 0.1 + (i % 10) * 0.01,
                'side': 'buy' if i % 2 == 0 else 'sell',
                'trade_id': f'trade_{i:06d}'
            })
        
        return pd.DataFrame(data)
    
    async def simulate_streaming_data(self):
        """Simulate streaming data with batched uploads"""
        
        logger.info("🚀 Starting streaming data simulation with 1-minute batching")
        logger.info("💡 Data will be queued and automatically flushed every 60 seconds")
        
        try:
            # Simulate 5 minutes of streaming data
            for minute in range(5):
                logger.info(f"\n📊 Minute {minute + 1}: Adding streaming data to batches")
                
                # Add candle data (every minute)
                candles_1m = self.generate_sample_candles(1)
                candles_5m = self.generate_sample_candles(1)
                
                rows_added = self.uploader.add_streaming_candles(candles_1m, '1m')
                logger.info(f"  ✅ Added {rows_added} 1m candles to batch queue")
                
                rows_added = self.uploader.add_streaming_candles(candles_5m, '5m')
                logger.info(f"  ✅ Added {rows_added} 5m candles to batch queue")
                
                # Add tick data (every minute, simulating multiple ticks)
                ticks_trades = self.generate_sample_ticks(50)
                ticks_book = self.generate_sample_ticks(20)
                
                rows_added = self.uploader.add_streaming_ticks(ticks_trades, 'trades')
                logger.info(f"  ✅ Added {rows_added} trade ticks to batch queue")
                
                rows_added = self.uploader.add_streaming_ticks(ticks_book, 'book_snapshot_5')
                logger.info(f"  ✅ Added {rows_added} book snapshot ticks to batch queue")
                
                # Show current batch stats
                stats = self.uploader.get_batch_stats()
                logger.info(f"  📈 Current batch stats: {self._format_stats(stats)}")
                
                # Wait 30 seconds before next batch (simulate real-time data)
                if minute < 4:  # Don't wait after last iteration
                    logger.info(f"  ⏳ Waiting 30 seconds before next data batch...")
                    await asyncio.sleep(30)
            
            # Wait for automatic batch flush (should happen within 60 seconds)
            logger.info(f"\n⏳ Waiting up to 90 seconds for automatic batch flush...")
            await asyncio.sleep(90)
            
            # Force flush any remaining batches
            logger.info(f"\n🚀 Force flushing any remaining batches...")
            await self.uploader.force_flush_all()
            
            # Final stats
            final_stats = self.uploader.get_batch_stats()
            logger.info(f"\n📊 Final batch statistics:")
            logger.info(f"  {self._format_stats(final_stats)}")
            
        except Exception as e:
            logger.error(f"❌ Streaming simulation failed: {e}")
            raise
        finally:
            # Stop the batch flusher
            self.uploader.stop_batch_flusher()
    
    def _format_stats(self, stats: dict) -> str:
        """Format batch statistics for logging"""
        
        parts = []
        
        # Queued data
        if stats['queued_batches']:
            queued_parts = []
            for key, data in stats['queued_batches'].items():
                queued_parts.append(f"{key}={data['queued_rows']} rows")
            parts.append(f"Queued: {', '.join(queued_parts)}")
        else:
            parts.append("Queued: None")
        
        # Total stats
        if stats['total_stats']:
            total_parts = []
            for key, data in stats['total_stats'].items():
                if data['batches'] > 0:
                    total_parts.append(f"{key}={data['batches']} batches")
            if total_parts:
                parts.append(f"Uploaded: {', '.join(total_parts)}")
        
        return " | ".join(parts)
    
    async def demonstrate_cost_optimization(self):
        """Demonstrate cost optimization compared to immediate uploads"""
        
        logger.info("\n" + "="*60)
        logger.info("💰 COST OPTIMIZATION DEMONSTRATION")
        logger.info("="*60)
        
        # Simulate high-frequency data
        logger.info("📈 Simulating high-frequency streaming data (1 candle every 5 seconds)")
        
        start_time = time.time()
        
        # Add data frequently (every 5 seconds for 2 minutes)
        for i in range(24):  # 2 minutes worth of 5-second intervals
            candles = self.generate_sample_candles(1)
            ticks = self.generate_sample_ticks(10)
            
            self.uploader.add_streaming_candles(candles, '1m')
            self.uploader.add_streaming_ticks(ticks, 'trades')
            
            logger.info(f"  📊 Added batch {i+1}/24 - {len(candles)} candles, {len(ticks)} ticks")
            
            if i < 23:  # Don't wait after last iteration
                await asyncio.sleep(5)
        
        elapsed_time = time.time() - start_time
        
        # Show what would happen with immediate uploads vs batching
        stats = self.uploader.get_batch_stats()
        
        logger.info(f"\n💡 Cost Analysis:")
        logger.info(f"  📊 Data added in {elapsed_time:.1f} seconds")
        logger.info(f"  🔄 Without batching: Would have made 48 BigQuery API calls (24 candles + 24 ticks)")
        logger.info(f"  💾 With 1-minute batching: Will make ~4 BigQuery API calls (2 candle batches + 2 tick batches)")
        logger.info(f"  💰 Cost reduction: ~92% fewer API calls")
        logger.info(f"  ⚡ Network efficiency: Larger, more efficient uploads")
        
        # Force flush to demonstrate final upload
        await self.uploader.force_flush_all()
        
        final_stats = self.uploader.get_batch_stats()
        logger.info(f"\n✅ Final upload completed with batching optimization")

async def main():
    """Run the streaming batch demonstration"""
    
    demo = StreamingBatchDemo()
    
    logger.info("🎯 Streaming BigQuery Batch Upload Demonstration")
    logger.info("="*60)
    
    # Run streaming simulation
    await demo.simulate_streaming_data()
    
    # Demonstrate cost optimization
    await demo.demonstrate_cost_optimization()
    
    logger.info("\n🎉 Demonstration completed successfully!")
    logger.info("\n📋 Key Benefits of 1-Minute Batching:")
    logger.info("  • 90%+ reduction in BigQuery API calls")
    logger.info("  • Lower costs due to fewer insert operations")
    logger.info("  • Better network efficiency with larger batches")
    logger.info("  • Automatic background flushing")
    logger.info("  • Thread-safe queuing system")
    logger.info("  • Graceful shutdown with force flush")

if __name__ == "__main__":
    asyncio.run(main())
