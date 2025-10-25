#!/usr/bin/env python3
"""
Single Instrument Live Processor with Latency Statistics

Command-line tool for processing one Binance spot USDT instrument at a time
with threading and comprehensive latency statistics for 1-minute candle processing.

Usage:
    python single_instrument_processor.py --instrument BTC-USDT --duration 300
    python single_instrument_processor.py --list-instruments
    python single_instrument_processor.py --instrument ETH-USDT --threads 4 --stats-interval 10
"""

import asyncio
import argparse
import json
import logging
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any
from collections import defaultdict, deque
from dataclasses import dataclass, field
import statistics
import sys
from pathlib import Path
import signal
import queue
import random

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))
sys.path.append(str(Path(__file__).parent.parent))

from config import get_config
from market_data_tick_handler.utils.logger import setup_structured_logging
from market_data_tick_handler.data_downloader.instrument_reader import InstrumentReader
from market_data_tick_handler.models import InstrumentDefinition

# Configure logging
logger = logging.getLogger(__name__)
setup_structured_logging()

@dataclass
class LatencyStats:
    """Comprehensive latency statistics"""
    trade_processing_times: deque = field(default_factory=lambda: deque(maxlen=10000))
    candle_completion_times: deque = field(default_factory=lambda: deque(maxlen=1000))
    websocket_latency: deque = field(default_factory=lambda: deque(maxlen=1000))
    total_trades: int = 0
    total_candles: int = 0
    start_time: Optional[datetime] = None
    last_stats_time: Optional[datetime] = None
    
    def add_trade_processing_time(self, processing_time_ms: float):
        """Add trade processing time"""
        self.trade_processing_times.append(processing_time_ms)
        self.total_trades += 1
    
    def add_candle_completion_time(self, completion_time_ms: float):
        """Add candle completion time"""
        self.candle_completion_times.append(completion_time_ms)
        self.total_candles += 1
    
    def add_websocket_latency(self, latency_ms: float):
        """Add WebSocket latency"""
        self.websocket_latency.append(latency_ms)
    
    def get_summary(self) -> Dict[str, Any]:
        """Get comprehensive statistics summary"""
        now = datetime.now()
        
        # Trade processing statistics
        trade_stats = {}
        if self.trade_processing_times:
            trade_stats = {
                'count': len(self.trade_processing_times),
                'min_ms': min(self.trade_processing_times),
                'max_ms': max(self.trade_processing_times),
                'avg_ms': statistics.mean(self.trade_processing_times),
                'median_ms': statistics.median(self.trade_processing_times),
                'p95_ms': self._percentile(self.trade_processing_times, 95),
                'p99_ms': self._percentile(self.trade_processing_times, 99)
            }
        
        # Candle completion statistics
        candle_stats = {}
        if self.candle_completion_times:
            candle_stats = {
                'count': len(self.candle_completion_times),
                'min_ms': min(self.candle_completion_times),
                'max_ms': max(self.candle_completion_times),
                'avg_ms': statistics.mean(self.candle_completion_times),
                'median_ms': statistics.median(self.candle_completion_times),
                'p95_ms': self._percentile(self.candle_completion_times, 95),
                'p99_ms': self._percentile(self.candle_completion_times, 99)
            }
        
        # WebSocket latency statistics
        ws_stats = {}
        if self.websocket_latency:
            ws_stats = {
                'count': len(self.websocket_latency),
                'min_ms': min(self.websocket_latency),
                'max_ms': max(self.websocket_latency),
                'avg_ms': statistics.mean(self.websocket_latency),
                'median_ms': statistics.median(self.websocket_latency),
                'p95_ms': self._percentile(self.websocket_latency, 95),
                'p99_ms': self._percentile(self.websocket_latency, 99)
            }
        
        # Overall timing
        elapsed_seconds = 0
        trades_per_second = 0
        candles_per_minute = 0
        
        if self.start_time:
            elapsed_seconds = (now - self.start_time).total_seconds()
            trades_per_second = self.total_trades / elapsed_seconds if elapsed_seconds > 0 else 0
            candles_per_minute = (self.total_candles / elapsed_seconds) * 60 if elapsed_seconds > 0 else 0
        
        return {
            'elapsed_seconds': elapsed_seconds,
            'total_trades': self.total_trades,
            'total_candles': self.total_candles,
            'trades_per_second': trades_per_second,
            'candles_per_minute': candles_per_minute,
            'trade_processing': trade_stats,
            'candle_completion': candle_stats,
            'websocket_latency': ws_stats
        }
    
    def _percentile(self, data: deque, percentile: float) -> float:
        """Calculate percentile from deque data"""
        if not data:
            return 0.0
        sorted_data = sorted(data)
        index = int((percentile / 100) * len(sorted_data))
        return sorted_data[min(index, len(sorted_data) - 1)]

@dataclass
class LiveCandle:
    """Live candle with timing information"""
    instrument_key: str
    candle_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    trade_count: int
    start_time: datetime
    completion_time: Optional[datetime] = None
    
    def complete(self):
        """Mark candle as completed"""
        self.completion_time = datetime.now()
    
    def get_completion_time_ms(self) -> float:
        """Get completion time in milliseconds"""
        if self.completion_time:
            return (self.completion_time - self.start_time).total_seconds() * 1000
        return 0.0

class MockTradeGenerator:
    """Mock trade generator for testing without Tardis Machine"""
    
    def __init__(self, instrument_key: str, base_price: float = 50000):
        self.instrument_key = instrument_key
        self.base_price = base_price
        self.current_price = base_price
        self.running = False
        self.trade_queue = queue.Queue()
        
    def start(self):
        """Start generating trades"""
        self.running = True
        threading.Thread(target=self._generate_trades, daemon=True).start()
    
    def stop(self):
        """Stop generating trades"""
        self.running = False
    
    def get_trade(self, timeout: float = 1.0) -> Optional[Dict[str, Any]]:
        """Get next trade from queue"""
        try:
            return self.trade_queue.get(timeout=timeout)
        except queue.Empty:
            return None
    
    def _generate_trades(self):
        """Generate simulated trades in background thread"""
        while self.running:
            # Simulate realistic trade frequency (1-10 trades per second)
            trades_per_second = random.randint(1, 10)
            
            for _ in range(trades_per_second):
                if not self.running:
                    break
                
                # Generate trade
                price_change = random.uniform(-0.005, 0.005)  # ±0.5% per trade
                self.current_price *= (1 + price_change)
                
                trade = {
                    'timestamp': datetime.now(timezone.utc),
                    'price': round(self.current_price, 2),
                    'amount': round(random.uniform(0.001, 0.1), 4),
                    'side': random.choice(['buy', 'sell']),
                    'instrument_key': self.instrument_key
                }
                
                self.trade_queue.put(trade)
            
            # Wait for next second
            time.sleep(1.0)

class SingleInstrumentProcessor:
    """Single instrument processor with threading and latency tracking"""
    
    def __init__(self, config, instrument_key: str, num_threads: int = 1):
        self.config = config
        self.instrument_key = instrument_key
        self.num_threads = num_threads
        
        # Instrument information
        self.instrument: Optional[InstrumentDefinition] = None
        
        # Processing state
        self.current_candle: Optional[LiveCandle] = None
        self.completed_candles: List[LiveCandle] = []
        self.stats = LatencyStats()
        
        # Threading
        self.trade_queue = queue.Queue(maxsize=1000)
        self.running = False
        self.threads: List[threading.Thread] = []
        
        # Mock trade generator (for testing)
        self.trade_generator: Optional[MockTradeGenerator] = None
        
    async def initialize(self):
        """Initialize processor with instrument definition"""
        logger.info(f"🚀 Initializing processor for {self.instrument_key}")
        
        # Load instrument definitions
        reader = InstrumentReader(self.config.gcp.bucket)
        target_date = datetime(2023, 5, 23, tzinfo=timezone.utc)
        
        df = reader.get_instruments_for_date(
            date=target_date,
            venue='BINANCE',
            instrument_type='SPOT_PAIR'
        )
        
        # Find our instrument
        instrument_row = df[df['instrument_key'] == self.instrument_key]
        if instrument_row.empty:
            logger.error(f"❌ Instrument {self.instrument_key} not found")
            return False
        
        # Create instrument definition with proper data conversion
        row = instrument_row.iloc[0]
        
        # Convert timestamps to ISO strings
        available_from_str = row['available_from_datetime']
        if hasattr(available_from_str, 'isoformat'):
            available_from_str = available_from_str.isoformat()
        
        available_to_str = row['available_to_datetime']
        if hasattr(available_to_str, 'isoformat'):
            available_to_str = available_to_str.isoformat()
        
        # Convert numeric fields to strings where required
        tick_size_str = str(row.get('tick_size', '')) if row.get('tick_size') is not None else ''
        expiry_str = row.get('expiry')
        if expiry_str is not None and hasattr(expiry_str, 'isoformat'):
            expiry_str = expiry_str.isoformat()
        
        self.instrument = InstrumentDefinition(
            instrument_key=row['instrument_key'],
            venue=row['venue'],
            instrument_type=row['instrument_type'],
            available_from_datetime=str(available_from_str),
            available_to_datetime=str(available_to_str),
            data_types=row['data_types'],
            base_asset=row['base_asset'],
            quote_asset=row['quote_asset'],
            settle_asset=row['settle_asset'],
            exchange_raw_symbol=row['exchange_raw_symbol'],
            tardis_symbol=row['tardis_symbol'],
            tardis_exchange=row['tardis_exchange'],
            data_provider=row['data_provider'],
            venue_type=row['venue_type'],
            asset_class=row['asset_class'],
            inverse=row.get('inverse', False),
            symbol_type=row.get('symbol_type', ''),
            contract_type=row.get('contract_type', ''),
            strike=row.get('strike', ''),
            option_type=row.get('option_type', ''),
            expiry=expiry_str,
            contract_size=row.get('contract_size'),
            tick_size=tick_size_str,
            settlement_type=row.get('settlement_type', ''),
            underlying=row.get('underlying', ''),
            min_size=row.get('min_size', ''),
            ccxt_symbol=row.get('ccxt_symbol', ''),
            ccxt_exchange=row.get('ccxt_exchange', '')
        )
        
        logger.info(f"✅ Loaded instrument: {self.instrument.tardis_symbol}")
        
        # Initialize mock trade generator
        base_price = 50000 if 'BTC' in self.instrument_key else 3000 if 'ETH' in self.instrument_key else 1.0
        self.trade_generator = MockTradeGenerator(self.instrument_key, base_price)
        
        return True
    
    def start_processing(self):
        """Start processing with multiple threads"""
        logger.info(f"🔄 Starting processing with {self.num_threads} threads")
        
        self.running = True
        self.stats.start_time = datetime.now()
        
        # Start trade generator
        self.trade_generator.start()
        
        # Start processing threads
        for i in range(self.num_threads):
            thread = threading.Thread(
                target=self._processing_worker,
                name=f"Processor-{i+1}",
                daemon=True
            )
            thread.start()
            self.threads.append(thread)
        
        logger.info(f"✅ Started {self.num_threads} processing threads")
    
    def stop_processing(self):
        """Stop processing gracefully"""
        logger.info("🛑 Stopping processing...")
        
        self.running = False
        
        # Stop trade generator
        if self.trade_generator:
            self.trade_generator.stop()
        
        # Wait for threads to finish
        for thread in self.threads:
            thread.join(timeout=5.0)
        
        # Finalize current candle
        if self.current_candle:
            self._finalize_candle()
        
        logger.info("✅ Processing stopped")
    
    def _processing_worker(self):
        """Worker thread for processing trades"""
        thread_name = threading.current_thread().name
        
        while self.running:
            try:
                # Get trade from generator
                trade = self.trade_generator.get_trade(timeout=1.0)
                if trade is None:
                    continue
                
                # Process trade
                start_time = time.time()
                self._process_trade(trade)
                processing_time_ms = (time.time() - start_time) * 1000
                
                # Record latency
                self.stats.add_trade_processing_time(processing_time_ms)
                
            except Exception as e:
                logger.error(f"❌ Error in {thread_name}: {e}")
                continue
    
    def _process_trade(self, trade: Dict[str, Any]):
        """Process a single trade"""
        # Get current minute bucket
        candle_time = self._get_candle_time(trade['timestamp'])
        
        # Check if we need to finalize previous candle
        if (self.current_candle is not None and 
            self.current_candle.candle_time != candle_time):
            self._finalize_candle()
        
        # Start new candle or update existing
        if self.current_candle is None or self.current_candle.candle_time != candle_time:
            self._start_new_candle(trade, candle_time)
        else:
            self._update_candle(trade)
    
    def _get_candle_time(self, timestamp: datetime) -> datetime:
        """Get candle time bucket for a timestamp"""
        seconds = int(timestamp.timestamp())
        bucket_seconds = (seconds // 60) * 60
        return datetime.fromtimestamp(bucket_seconds, tz=timezone.utc)
    
    def _start_new_candle(self, trade: Dict[str, Any], candle_time: datetime):
        """Start a new candle"""
        self.current_candle = LiveCandle(
            instrument_key=trade['instrument_key'],
            candle_time=candle_time,
            open=trade['price'],
            high=trade['price'],
            low=trade['price'],
            close=trade['price'],
            volume=trade['amount'],
            trade_count=1,
            start_time=datetime.now()
        )
    
    def _update_candle(self, trade: Dict[str, Any]):
        """Update current candle with new trade"""
        self.current_candle.high = max(self.current_candle.high, trade['price'])
        self.current_candle.low = min(self.current_candle.low, trade['price'])
        self.current_candle.close = trade['price']
        self.current_candle.volume += trade['amount']
        self.current_candle.trade_count += 1
    
    def _finalize_candle(self):
        """Finalize current candle"""
        if self.current_candle:
            self.current_candle.complete()
            completion_time_ms = self.current_candle.get_completion_time_ms()
            
            # Record candle completion latency
            self.stats.add_candle_completion_time(completion_time_ms)
            
            # Add to completed candles
            self.completed_candles.append(self.current_candle)
            
            # Log candle completion
            candle = self.current_candle
            logger.info(
                f"🕯️ Completed candle {candle.instrument_key} "
                f"{candle.candle_time}: O=${candle.open:.2f} H=${candle.high:.2f} "
                f"L=${candle.low:.2f} C=${candle.close:.2f} V={candle.volume:.4f} "
                f"Trades={candle.trade_count} Completion={completion_time_ms:.2f}ms"
            )
            
            self.current_candle = None
    
    def get_current_stats(self) -> Dict[str, Any]:
        """Get current statistics"""
        return self.stats.get_summary()
    
    def print_stats(self):
        """Print formatted statistics"""
        stats = self.get_current_stats()
        
        print("\n" + "="*80)
        print(f"📊 LATENCY STATISTICS FOR {self.instrument_key}")
        print("="*80)
        
        # Overall statistics
        print(f"⏱️  Duration: {stats['elapsed_seconds']:.1f}s")
        print(f"📈 Total Trades: {stats['total_trades']:,}")
        print(f"🕯️  Total Candles: {stats['total_candles']}")
        print(f"🚀 Trades/sec: {stats['trades_per_second']:.1f}")
        print(f"⏰ Candles/min: {stats['candles_per_minute']:.1f}")
        
        # Trade processing latency
        if stats['trade_processing']:
            tp = stats['trade_processing']
            print(f"\n🔧 TRADE PROCESSING LATENCY:")
            print(f"   Count: {tp['count']:,}")
            print(f"   Min:   {tp['min_ms']:.3f}ms")
            print(f"   Max:   {tp['max_ms']:.3f}ms")
            print(f"   Avg:   {tp['avg_ms']:.3f}ms")
            print(f"   Median:{tp['median_ms']:.3f}ms")
            print(f"   P95:   {tp['p95_ms']:.3f}ms")
            print(f"   P99:   {tp['p99_ms']:.3f}ms")
        
        # Candle completion latency
        if stats['candle_completion']:
            cc = stats['candle_completion']
            print(f"\n🕯️  CANDLE COMPLETION LATENCY:")
            print(f"   Count: {cc['count']}")
            print(f"   Min:   {cc['min_ms']:.3f}ms")
            print(f"   Max:   {cc['max_ms']:.3f}ms")
            print(f"   Avg:   {cc['avg_ms']:.3f}ms")
            print(f"   Median:{cc['median_ms']:.3f}ms")
            print(f"   P95:   {cc['p95_ms']:.3f}ms")
            print(f"   P99:   {cc['p99_ms']:.3f}ms")
        
        print("="*80)

class CLIProcessor:
    """Command-line interface for single instrument processing"""
    
    def __init__(self):
        self.config = get_config()
        self.processor: Optional[SingleInstrumentProcessor] = None
        self.stats_thread: Optional[threading.Thread] = None
        self.running = False
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"🛑 Received signal {signum}, shutting down...")
        self.running = False
        if self.processor:
            self.processor.stop_processing()
    
    async def list_instruments(self):
        """List available Binance spot USDT instruments"""
        logger.info("📋 Loading available instruments...")
        
        try:
            reader = InstrumentReader(self.config.gcp.bucket)
            target_date = datetime(2023, 5, 23, tzinfo=timezone.utc)
            
            df = reader.get_instruments_for_date(
                date=target_date,
                venue='BINANCE',
                instrument_type='SPOT_PAIR'
            )
            
            # Filter for USDT pairs
            usdt_pairs = df[df['quote_asset'] == 'USDT']
            
            if usdt_pairs.empty:
                print("❌ No Binance spot USDT instruments found")
                return
            
            print(f"\n📋 Available Binance Spot USDT Instruments ({len(usdt_pairs)}):")
            print("-" * 80)
            print(f"{'#':<3} {'Instrument Key':<35} {'Tardis Symbol':<15} {'Base Asset':<10}")
            print("-" * 80)
            
            for i, (_, row) in enumerate(usdt_pairs.iterrows()):
                print(f"{i+1:<3} {row['instrument_key']:<35} {row['tardis_symbol']:<15} {row['base_asset']:<10}")
            
            print("-" * 80)
            
        except Exception as e:
            logger.error(f"❌ Failed to list instruments: {e}")
    
    async def process_instrument(self, instrument_key: str, duration: int, 
                                num_threads: int, stats_interval: int):
        """Process a single instrument with latency tracking"""
        logger.info(f"🚀 Starting processing for {instrument_key}")
        logger.info(f"   Duration: {duration}s")
        logger.info(f"   Threads: {num_threads}")
        logger.info(f"   Stats interval: {stats_interval}s")
        
        try:
            # Initialize processor
            self.processor = SingleInstrumentProcessor(
                self.config, instrument_key, num_threads
            )
            
            if not await self.processor.initialize():
                logger.error("❌ Failed to initialize processor")
                return
            
            # Start processing
            self.processor.start_processing()
            self.running = True
            
            # Start stats reporting thread
            self.stats_thread = threading.Thread(
                target=self._stats_reporter,
                args=(stats_interval,),
                daemon=True
            )
            self.stats_thread.start()
            
            # Run for specified duration
            await asyncio.sleep(duration)
            
        except Exception as e:
            logger.error(f"❌ Error during processing: {e}")
        finally:
            # Cleanup
            self.running = False
            if self.processor:
                self.processor.stop_processing()
            
            # Print final statistics
            if self.processor:
                self.processor.print_stats()
    
    def _stats_reporter(self, interval: int):
        """Report statistics at regular intervals"""
        while self.running and self.processor:
            time.sleep(interval)
            if self.running and self.processor:
                stats = self.processor.get_current_stats()
                logger.info(
                    f"📊 Stats: {stats['total_trades']:,} trades, "
                    f"{stats['trades_per_second']:.1f} trades/s, "
                    f"{stats['total_candles']} candles, "
                    f"Avg processing: {stats['trade_processing'].get('avg_ms', 0):.2f}ms"
                )

def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="Single Instrument Live Processor with Latency Statistics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python single_instrument_processor.py --list-instruments
  python single_instrument_processor.py --instrument BTC-USDT --duration 300
  python single_instrument_processor.py --instrument ETH-USDT --threads 4 --stats-interval 10
        """
    )
    
    parser.add_argument(
        '--instrument', '-i',
        type=str,
        help='Instrument key to process (e.g., BTC-USDT)'
    )
    
    parser.add_argument(
        '--duration', '-d',
        type=int,
        default=60,
        help='Processing duration in seconds (default: 60)'
    )
    
    parser.add_argument(
        '--threads', '-t',
        type=int,
        default=1,
        help='Number of processing threads (default: 1)'
    )
    
    parser.add_argument(
        '--stats-interval', '-s',
        type=int,
        default=10,
        help='Statistics reporting interval in seconds (default: 10)'
    )
    
    parser.add_argument(
        '--list-instruments', '-l',
        action='store_true',
        help='List available Binance spot USDT instruments'
    )
    
    args = parser.parse_args()
    
    # Create CLI processor
    cli = CLIProcessor()
    
    async def run():
        if args.list_instruments:
            await cli.list_instruments()
        elif args.instrument:
            # Convert instrument format if needed
            if '-' in args.instrument and not args.instrument.startswith('BINANCE:'):
                instrument_key = f"BINANCE:SPOT_PAIR:{args.instrument.upper()}"
            else:
                instrument_key = args.instrument
            
            await cli.process_instrument(
                instrument_key=instrument_key,
                duration=args.duration,
                num_threads=args.threads,
                stats_interval=args.stats_interval
            )
        else:
            parser.print_help()
    
    # Run the CLI
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("🛑 Interrupted by user")
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
