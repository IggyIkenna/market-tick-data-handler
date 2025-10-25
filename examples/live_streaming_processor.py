#!/usr/bin/env python3
"""
Live Streaming Candle Processor - Realistic Implementation

Simulates real live processing without pandas batch operations
"""

import time
import random
from datetime import datetime, timedelta
from collections import defaultdict
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LiveCandleProcessor:
    """Realistic live candle processor without pandas"""
    
    def __init__(self, interval_seconds=60):
        self.interval_seconds = interval_seconds
        self.current_candle = None
        self.candles = []
        self.trade_count = 0
        self.processing_times = []
        
    def _get_candle_time(self, timestamp):
        """Get candle time bucket for a timestamp"""
        # Floor to nearest interval
        seconds = int(timestamp.timestamp())
        bucket_seconds = (seconds // self.interval_seconds) * self.interval_seconds
        return datetime.fromtimestamp(bucket_seconds)
    
    def _process_trade(self, trade):
        """Process a single trade - realistic live processing"""
        start_time = time.time()
        
        # Get candle time for this trade
        candle_time = self._get_candle_time(trade['timestamp'])
        
        # If we're starting a new candle, finalize the previous one
        if self.current_candle is not None and candle_time != self.current_candle['candle_time']:
            self._finalize_candle()
        
        # Start new candle if needed
        if self.current_candle is None or candle_time != self.current_candle['candle_time']:
            self._start_new_candle(candle_time, trade)
        else:
            # Update existing candle
            self._update_candle(trade)
        
        # Record processing time
        processing_time = time.time() - start_time
        self.processing_times.append(processing_time)
        self.trade_count += 1
        
        return processing_time
    
    def _start_new_candle(self, candle_time, first_trade):
        """Start a new candle with first trade"""
        self.current_candle = {
            'candle_time': candle_time,
            'open': first_trade['price'],
            'high': first_trade['price'],
            'low': first_trade['price'],
            'close': first_trade['price'],
            'volume': first_trade['amount'],
            'trade_count': 1
        }
    
    def _update_candle(self, trade):
        """Update current candle with new trade"""
        self.current_candle['high'] = max(self.current_candle['high'], trade['price'])
        self.current_candle['low'] = min(self.current_candle['low'], trade['price'])
        self.current_candle['close'] = trade['price']
        self.current_candle['volume'] += trade['amount']
        self.current_candle['trade_count'] += 1
    
    def _finalize_candle(self):
        """Finalize current candle and add to completed candles"""
        if self.current_candle is not None:
            self.candles.append(self.current_candle.copy())
            self.current_candle = None
    
    def get_current_candle(self):
        """Get current incomplete candle"""
        return self.current_candle
    
    def get_completed_candles(self):
        """Get all completed candles"""
        return self.candles
    
    def get_processing_stats(self):
        """Get processing time statistics"""
        if not self.processing_times:
            return {}
        
        processing_times_ms = [t * 1000 for t in self.processing_times]
        
        return {
            'total_trades': self.trade_count,
            'total_candles': len(self.candles),
            'min_processing_time_ms': min(processing_times_ms),
            'max_processing_time_ms': max(processing_times_ms),
            'avg_processing_time_ms': sum(processing_times_ms) / len(processing_times_ms),
            'median_processing_time_ms': sorted(processing_times_ms)[len(processing_times_ms) // 2]
        }

def simulate_live_trade_stream():
    """Simulate realistic live trade stream"""
    
    logger.info("🔄 Simulating Live Trade Stream")
    logger.info("=" * 40)
    
    # Create processor
    processor = LiveCandleProcessor(interval_seconds=60)
    
    # Simulate realistic trade data
    base_time = datetime(2023, 5, 23, 0, 0, 0)
    base_price = 26849.28
    
    # Generate realistic trade stream
    trades = []
    current_time = base_time
    current_price = base_price
    
    # Simulate 1 hour of trades (3600 seconds)
    for second in range(3600):
        # Random number of trades per second (realistic for BTC-USDT)
        trades_per_second = random.randint(0, 10)
        
        for trade_num in range(trades_per_second):
            # Add random microseconds
            trade_time = current_time + timedelta(microseconds=random.randint(0, 999999))
            
            # Simulate price movement
            price_change = random.uniform(-0.01, 0.01)  # ±0.01% per trade
            current_price *= (1 + price_change)
            
            # Simulate trade size
            trade_size = random.uniform(0.001, 0.1)  # 0.001 to 0.1 BTC
            
            trade = {
                'timestamp': trade_time,
                'price': round(current_price, 2),
                'amount': round(trade_size, 4),
                'side': random.choice(['buy', 'sell'])
            }
            
            trades.append(trade)
        
        # Move to next second
        current_time += timedelta(seconds=1)
    
    logger.info(f"Generated {len(trades)} trades over 1 hour")
    
    # Process trades in order (simulating live stream)
    logger.info("\n📊 Processing Trades Live:")
    
    start_time = time.time()
    
    for i, trade in enumerate(trades):
        # Simulate realistic processing delay
        processing_time = processor._process_trade(trade)
        
        # Show progress every 1000 trades
        if (i + 1) % 1000 == 0:
            logger.info(f"  Processed {i+1:,} trades...")
        
        # Show candle completion
        if processor.current_candle and processor.current_candle['trade_count'] == 1:
            logger.info(f"  Started candle: {processor.current_candle['candle_time']}")
    
    # Finalize last candle
    processor._finalize_candle()
    
    total_time = time.time() - start_time
    
    # Get statistics
    stats = processor.get_processing_stats()
    
    logger.info(f"\n⏱️ Live Processing Results:")
    logger.info(f"  Total processing time: {total_time:.3f} seconds")
    logger.info(f"  Total trades: {stats['total_trades']:,}")
    logger.info(f"  Total candles: {stats['total_candles']}")
    logger.info(f"  Processing speed: {stats['total_trades']/total_time:,.0f} trades/second")
    
    logger.info(f"\n📊 Per-Trade Processing Time:")
    logger.info(f"  Min: {stats['min_processing_time_ms']:.3f}ms")
    logger.info(f"  Max: {stats['max_processing_time_ms']:.3f}ms")
    logger.info(f"  Average: {stats['avg_processing_time_ms']:.3f}ms")
    logger.info(f"  Median: {stats['median_processing_time_ms']:.3f}ms")
    
    # Show completed candles
    logger.info(f"\n🕯️ Completed Candles:")
    for i, candle in enumerate(processor.get_completed_candles()[:5]):
        logger.info(f"  {i+1}. {candle['candle_time']}: O=${candle['open']:.2f} H=${candle['high']:.2f} L=${candle['low']:.2f} C=${candle['close']:.2f} V={candle['volume']:.4f} Trades={candle['trade_count']}")
    
    return processor

def compare_with_batch_processing():
    """Compare live processing with batch processing"""
    
    logger.info("\n🔄 Live vs Batch Processing Comparison")
    logger.info("=" * 50)
    
    # Generate same trade data
    base_time = datetime(2023, 5, 23, 0, 0, 0)
    base_price = 26849.28
    
    trades = []
    current_time = base_time
    current_price = base_price
    
    # Generate 1000 trades
    for i in range(1000):
        trade_time = current_time + timedelta(microseconds=random.randint(0, 999999))
        price_change = random.uniform(-0.01, 0.01)
        current_price *= (1 + price_change)
        trade_size = random.uniform(0.001, 0.1)
        
        trade = {
            'timestamp': trade_time,
            'price': round(current_price, 2),
            'amount': round(trade_size, 4),
            'side': random.choice(['buy', 'sell'])
        }
        
        trades.append(trade)
        current_time += timedelta(seconds=1)
    
    # Live processing
    logger.info("\n📊 Live Processing:")
    live_processor = LiveCandleProcessor(interval_seconds=60)
    
    start_time = time.time()
    for trade in trades:
        live_processor._process_trade(trade)
    live_processor._finalize_candle()
    live_time = time.time() - start_time
    
    live_stats = live_processor.get_processing_stats()
    
    logger.info(f"  Processing time: {live_time:.3f} seconds")
    logger.info(f"  Trades processed: {live_stats['total_trades']}")
    logger.info(f"  Candles generated: {live_stats['total_candles']}")
    logger.info(f"  Avg per-trade time: {live_stats['avg_processing_time_ms']:.3f}ms")
    
    # Batch processing (pandas-like)
    logger.info("\n📊 Batch Processing (Pandas-like):")
    
    start_time = time.time()
    
    # Simulate pandas operations
    candle_times = []
    for trade in trades:
        seconds = int(trade['timestamp'].timestamp())
        bucket_seconds = (seconds // 60) * 60
        candle_times.append(datetime.fromtimestamp(bucket_seconds))
    
    # Group by candle time (simulated)
    candle_groups = defaultdict(list)
    for i, trade in enumerate(trades):
        candle_groups[candle_times[i]].append(trade)
    
    # Calculate OHLCV for each candle
    batch_candles = []
    for candle_time, candle_trades in candle_groups.items():
        if candle_trades:
            prices = [t['price'] for t in candle_trades]
            volumes = [t['amount'] for t in candle_trades]
            
            candle = {
                'candle_time': candle_time,
                'open': prices[0],
                'high': max(prices),
                'low': min(prices),
                'close': prices[-1],
                'volume': sum(volumes),
                'trade_count': len(candle_trades)
            }
            batch_candles.append(candle)
    
    batch_time = time.time() - start_time
    
    logger.info(f"  Processing time: {batch_time:.3f} seconds")
    logger.info(f"  Trades processed: {len(trades)}")
    logger.info(f"  Candles generated: {len(batch_candles)}")
    
    # Comparison
    logger.info(f"\n📈 Comparison:")
    logger.info(f"  Live processing: {live_time:.3f}s")
    logger.info(f"  Batch processing: {batch_time:.3f}s")
    logger.info(f"  Live is {batch_time/live_time:.1f}x faster")
    
    # Show candle differences
    logger.info(f"\n🕯️ Candle Comparison (First 3):")
    logger.info(f"{'Method':<10} {'Time':<20} {'O':<8} {'H':<8} {'L':<8} {'C':<8} {'V':<8} {'Trades':<8}")
    logger.info("-" * 80)
    
    for i in range(min(3, len(live_processor.get_completed_candles()), len(batch_candles))):
        live_candle = live_processor.get_completed_candles()[i]
        batch_candle = batch_candles[i]
        
        logger.info(f"{'Live':<10} {live_candle['candle_time']:<20} {live_candle['open']:<8.2f} {live_candle['high']:<8.2f} {live_candle['low']:<8.2f} {live_candle['close']:<8.2f} {live_candle['volume']:<8.4f} {live_candle['trade_count']:<8}")
        logger.info(f"{'Batch':<10} {batch_candle['candle_time']:<20} {batch_candle['open']:<8.2f} {batch_candle['high']:<8.2f} {batch_candle['low']:<8.2f} {batch_candle['close']:<8.2f} {batch_candle['volume']:<8.4f} {batch_candle['trade_count']:<8}")

def main():
    """Main function"""
    
    logger.info("🔄 Live Streaming Candle Processor")
    logger.info("=" * 50)
    
    # Simulate live trade stream
    processor = simulate_live_trade_stream()
    
    # Compare with batch processing
    compare_with_batch_processing()
    
    logger.info("\n✅ Live processing simulation completed!")

if __name__ == '__main__':
    main()
