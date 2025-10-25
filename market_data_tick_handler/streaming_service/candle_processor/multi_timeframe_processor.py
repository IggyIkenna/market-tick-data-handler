"""
Multi-Timeframe Candle Processor

Processes tick data into multiple timeframes (15s, 1m, 5m, 15m, 4h, 24h)
with proper UTC timestamp alignment and latency tracking.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set
from collections import defaultdict

from .candle_data import CandleData, CandleBuilder
from ..tick_streamer.utc_timestamp_manager import UTCTimestampManager
from ..hft_features.feature_calculator import HFTFeatureCalculator

logger = logging.getLogger(__name__)


class MultiTimeframeProcessor:
    """
    Processes tick data into multiple timeframes simultaneously.
    
    Features:
    - UTC-aligned candle boundaries
    - Multiple timeframe support (15s, 1m, 5m, 15m, 4h, 24h)
    - Latency tracking (timestamp_in vs timestamp_out)
    - HFT feature computation
    - Efficient memory management
    """
    
    def __init__(self,
                 symbol: str,
                 exchange: str = 'binance',
                 timeframes: List[str] = None,
                 enable_hft_features: bool = True,
                 max_candles_history: int = 1000):
        """
        Initialize multi-timeframe processor.
        
        Args:
            symbol: Trading symbol (e.g., 'BTC-USDT')
            exchange: Exchange name
            timeframes: List of timeframes to process
            enable_hft_features: Whether to compute HFT features
            max_candles_history: Max candles to keep in memory per timeframe
        """
        self.symbol = symbol
        self.exchange = exchange
        self.timeframes = timeframes or ['15s', '1m', '5m', '15m', '4h', '24h']
        self.enable_hft_features = enable_hft_features
        self.max_candles_history = max_candles_history
        
        # Core components
        self.timestamp_manager = UTCTimestampManager()
        
        if enable_hft_features:
            self.hft_calculator = HFTFeatureCalculator(
                symbol=symbol,
                timeframes=['15s', '1m']  # Only compute HFT features for fast timeframes
            )
        else:
            self.hft_calculator = None
        
        # Candle builders for each timeframe
        self.candle_builders: Dict[str, Optional[CandleBuilder]] = {
            tf: None for tf in self.timeframes
        }
        
        # Completed candles history
        self.candles_history: Dict[str, List[CandleData]] = {
            tf: [] for tf in self.timeframes
        }
        
        # Statistics
        self.stats = {
            'total_ticks_processed': 0,
            'candles_completed': defaultdict(int),
            'last_tick_time': None,
            'start_time': datetime.now(timezone.utc)
        }
        
        logger.info(f"✅ MultiTimeframeProcessor initialized")
        logger.info(f"   Symbol: {symbol}")
        logger.info(f"   Exchange: {exchange}")
        logger.info(f"   Timeframes: {self.timeframes}")
        logger.info(f"   HFT Features: {enable_hft_features}")
    
    async def process_tick(self, tick_data) -> List[CandleData]:
        """
        Process a tick and return any completed candles.
        
        Args:
            tick_data: TickData object
            
        Returns:
            List of completed CandleData objects
        """
        self.stats['total_ticks_processed'] += 1
        self.stats['last_tick_time'] = tick_data.timestamp
        
        completed_candles = []
        
        # Process each timeframe
        for timeframe in self.timeframes:
            candle = await self._process_tick_for_timeframe(tick_data, timeframe)
            if candle:
                completed_candles.append(candle)
        
        # Compute HFT features for completed candles
        if self.hft_calculator and completed_candles:
            await self._compute_hft_features(completed_candles)
        
        return completed_candles
    
    async def _process_tick_for_timeframe(self, tick_data, timeframe: str) -> Optional[CandleData]:
        """Process tick for a specific timeframe"""
        try:
            # Check if we should finalize current candle
            should_finalize = self.timestamp_manager.should_finalize_candle(
                tick_data.timestamp, timeframe
            )
            
            completed_candle = None
            
            if should_finalize and self.candle_builders[timeframe] is not None:
                # Finalize current candle
                completed_candle = self.candle_builders[timeframe].finalize()
                self.stats['candles_completed'][timeframe] += 1
                
                # Add to history
                self._add_to_history(timeframe, completed_candle)
                
                # Reset builder
                self.candle_builders[timeframe] = None
            
            # Get aligned timestamp for this tick
            aligned_timestamp = self.timestamp_manager.get_aligned_timestamp(
                tick_data.timestamp, timeframe
            )
            
            # Create or get current candle builder
            if self.candle_builders[timeframe] is None:
                self.candle_builders[timeframe] = CandleBuilder(
                    symbol=self.symbol,
                    exchange=self.exchange,
                    timeframe=timeframe,
                    timestamp_in=aligned_timestamp
                )
            
            # Add trade to current candle
            self.candle_builders[timeframe].add_trade(
                price=tick_data.price,
                amount=tick_data.amount
            )
            
            return completed_candle
            
        except Exception as e:
            logger.error(f"❌ Error processing tick for {timeframe}: {e}")
            return None
    
    def _add_to_history(self, timeframe: str, candle: CandleData) -> None:
        """Add completed candle to history with memory management"""
        history = self.candles_history[timeframe]
        history.append(candle)
        
        # Trim history if too long
        if len(history) > self.max_candles_history:
            history.pop(0)  # Remove oldest
    
    async def _compute_hft_features(self, completed_candles: List[CandleData]) -> None:
        """Compute HFT features for completed candles"""
        if not self.hft_calculator:
            return
        
        try:
            for candle in completed_candles:
                if candle.timeframe in ['15s', '1m']:
                    features = await self.hft_calculator.compute_features(candle)
                    if features:
                        logger.debug(f"📊 HFT features for {candle.timeframe}: {len(features)} features")
                        
        except Exception as e:
            logger.error(f"❌ Error computing HFT features: {e}")
    
    async def finalize_all_candles(self) -> List[CandleData]:
        """Finalize all pending candles (called on shutdown)"""
        completed_candles = []
        
        for timeframe, builder in self.candle_builders.items():
            if builder and not builder.is_empty():
                candle = builder.finalize()
                completed_candles.append(candle)
                self.stats['candles_completed'][timeframe] += 1
                self._add_to_history(timeframe, candle)
                
                logger.info(f"🕯️ Finalized pending {timeframe} candle")
        
        # Clear builders
        self.candle_builders = {tf: None for tf in self.timeframes}
        
        return completed_candles
    
    def get_latest_candle(self, timeframe: str) -> Optional[CandleData]:
        """Get the most recent completed candle for a timeframe"""
        history = self.candles_history.get(timeframe, [])
        return history[-1] if history else None
    
    def get_candles_history(self, timeframe: str, limit: int = 100) -> List[CandleData]:
        """Get recent candles history for a timeframe"""
        history = self.candles_history.get(timeframe, [])
        return history[-limit:] if history else []
    
    def get_current_candle_info(self, timeframe: str) -> Optional[Dict]:
        """Get info about current (incomplete) candle"""
        builder = self.candle_builders.get(timeframe)
        if not builder:
            return None
        
        return {
            'timeframe': timeframe,
            'timestamp_in': builder.timestamp_in,
            'trade_count': builder.trade_count,
            'volume': builder.volume,
            'current_price': builder.close,
            'price_range': (builder.low, builder.high) if builder.low and builder.high else None
        }
    
    def get_stats(self) -> Dict:
        """Get processing statistics"""
        runtime = (datetime.now(timezone.utc) - self.stats['start_time']).total_seconds()
        ticks_per_sec = self.stats['total_ticks_processed'] / max(runtime, 1)
        
        return {
            'symbol': self.symbol,
            'exchange': self.exchange,
            'timeframes': self.timeframes,
            'runtime_seconds': runtime,
            'total_ticks_processed': self.stats['total_ticks_processed'],
            'ticks_per_second': ticks_per_sec,
            'candles_completed': dict(self.stats['candles_completed']),
            'last_tick_time': self.stats['last_tick_time'].isoformat() if self.stats['last_tick_time'] else None,
            'hft_features_enabled': self.enable_hft_features
        }
    
    def print_stats(self) -> None:
        """Print processing statistics"""
        stats = self.get_stats()
        
        print(f"\n📊 MULTI-TIMEFRAME PROCESSOR STATS")
        print(f"   Symbol: {stats['symbol']}")
        print(f"   Runtime: {stats['runtime_seconds']:.1f}s")
        print(f"   Ticks processed: {stats['total_ticks_processed']:,}")
        print(f"   Ticks/sec: {stats['ticks_per_second']:.1f}")
        print(f"   HFT features: {stats['hft_features_enabled']}")
        print(f"   Candles completed:")
        
        for timeframe in self.timeframes:
            count = stats['candles_completed'].get(timeframe, 0)
            latest = self.get_latest_candle(timeframe)
            latest_time = latest.timestamp_in.strftime('%H:%M:%S') if latest else 'None'
            print(f"     {timeframe:>4}: {count:>3} candles (latest: {latest_time})")
    
    async def get_market_summary(self) -> Dict:
        """Get current market summary across all timeframes"""
        summary = {
            'symbol': self.symbol,
            'exchange': self.exchange,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'timeframes': {}
        }
        
        for timeframe in self.timeframes:
            latest_candle = self.get_latest_candle(timeframe)
            current_info = self.get_current_candle_info(timeframe)
            
            tf_summary = {
                'latest_completed_candle': latest_candle.to_dict() if latest_candle else None,
                'current_candle': current_info,
                'total_candles': self.stats['candles_completed'].get(timeframe, 0)
            }
            
            summary['timeframes'][timeframe] = tf_summary
        
        return summary


# Example usage and testing
if __name__ == "__main__":
    import asyncio
    from dataclasses import dataclass
    from datetime import datetime, timezone
    
    @dataclass
    class MockTickData:
        symbol: str = "BTC-USDT"
        exchange: str = "binance"
        price: float = 67000.0
        amount: float = 0.1
        side: str = "buy"
        timestamp: datetime = datetime.now(timezone.utc)
        timestamp_received: datetime = datetime.now(timezone.utc)
        trade_id: str = "test_123"
    
    async def test_processor():
        # Create processor
        processor = MultiTimeframeProcessor(
            symbol="BTC-USDT",
            timeframes=['15s', '1m', '5m'],
            enable_hft_features=True
        )
        
        # Process some test ticks
        base_price = 67000.0
        for i in range(100):
            price = base_price + (i % 10 - 5) * 10  # Price variation
            tick = MockTickData(price=price, trade_id=f"test_{i}")
            
            completed_candles = await processor.process_tick(tick)
            
            for candle in completed_candles:
                print(f"Completed: {candle}")
            
            await asyncio.sleep(0.01)  # 100ms between ticks
        
        # Finalize and show stats
        final_candles = await processor.finalize_all_candles()
        for candle in final_candles:
            print(f"Final: {candle}")
        
        processor.print_stats()
    
    asyncio.run(test_processor())
