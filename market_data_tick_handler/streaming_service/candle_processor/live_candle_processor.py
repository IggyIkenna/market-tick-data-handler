"""
Live Candle Processor

UNIFIED processor that works for both live streaming and historical processing.
Uses the same HFT features code for consistency and testability.
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass
from collections import defaultdict, deque

from ..hft_features.feature_calculator import HFTFeatureCalculator, HFTFeatures
from .candle_data import CandleData, CandleBuilder
from ..tick_streamer.utc_timestamp_manager import UTCTimestampManager

logger = logging.getLogger(__name__)


@dataclass
class LiveProcessingConfig:
    """Configuration for live candle processing"""
    timeframes: List[str] = None
    enable_hft_features: bool = True
    max_history: int = 100
    buffer_size: int = 1000
    
    def __post_init__(self):
        if self.timeframes is None:
            self.timeframes = ['15s', '1m', '5m', '15m', '4h', '24h']


class LiveCandleProcessor:
    """
    Unified candle processor for both live streaming and historical processing.
    
    Uses the same HFT features code as historical processing for consistency.
    Handles UTC-aligned candle boundaries and multiple timeframes.
    """
    
    def __init__(self, 
                 symbol: str,
                 config: LiveProcessingConfig = None):
        """
        Initialize live candle processor.
        
        Args:
            symbol: Trading symbol (e.g., "BTC-USDT")
            config: Processing configuration
        """
        self.symbol = symbol
        self.config = config or LiveProcessingConfig()
        self.timestamp_manager = UTCTimestampManager()
        
        # Current candles for each timeframe
        self.current_candles: Dict[str, CandleBuilder] = {}
        self.completed_candles: Dict[str, deque] = {}
        
        # Initialize completed candles deques
        for tf in self.config.timeframes:
            self.completed_candles[tf] = deque(maxlen=self.config.buffer_size)
        
        # HFT features calculator (SAME CODE as historical)
        self.hft_calculator = None
        if self.config.enable_hft_features:
            self.hft_calculator = HFTFeatureCalculator(
                symbol=symbol,
                timeframes=['15s', '1m'],  # HFT features only for high-frequency timeframes
                max_history=self.config.max_history
            )
        
        # Statistics
        self.stats = {
            'total_ticks_processed': 0,
            'candles_completed': defaultdict(int),
            'start_time': datetime.now(timezone.utc),
            'last_tick_time': None
        }
        
        logger.info(f"✅ LiveCandleProcessor initialized for {symbol}")
        logger.info(f"   Timeframes: {self.config.timeframes}")
        logger.info(f"   HFT features: {self.config.enable_hft_features}")
    
    async def process_tick(self, tick_data: Dict[str, Any]) -> List[CandleData]:
        """
        Process a single tick into candles across all timeframes.
        
        Args:
            tick_data: Processed tick data
            
        Returns:
            List of completed candles (if any)
        """
        try:
            completed_candles = []
            
            # Extract tick information
            timestamp = tick_data['timestamp']
            price = float(tick_data['price'])
            amount = float(tick_data['amount'])
            
            # Update statistics
            self.stats['total_ticks_processed'] += 1
            self.stats['last_tick_time'] = timestamp
            
            # Process for each timeframe
            for timeframe in self.config.timeframes:
                candle = await self._process_tick_for_timeframe(
                    tick_data, timeframe, timestamp, price, amount
                )
                if candle:
                    completed_candles.append(candle)
            
            return completed_candles
            
        except Exception as e:
            logger.error(f"❌ Error processing tick: {e}")
            return []
    
    async def _process_tick_for_timeframe(self, 
                                        tick_data: Dict[str, Any],
                                        timeframe: str,
                                        timestamp: datetime,
                                        price: float,
                                        amount: float) -> Optional[CandleData]:
        """
        Process tick for a specific timeframe.
        
        Returns completed candle if timeframe boundary crossed.
        """
        # Get candle time boundary
        candle_time = self.timestamp_manager.get_candle_time(timestamp, timeframe)
        
        # Check if we need to finalize previous candle
        current_builder = self.current_candles.get(timeframe)
        if current_builder and current_builder.candle_time != candle_time:
            # Finalize previous candle
            completed_candle = await self._finalize_candle(timeframe, current_builder)
            if completed_candle:
                return completed_candle
        
        # Start new candle or update existing
        if timeframe not in self.current_candles or self.current_candles[timeframe].candle_time != candle_time:
            self.current_candles[timeframe] = CandleBuilder(
                symbol=self.symbol,
                timeframe=timeframe,
                candle_time=candle_time
            )
        
        # Update current candle
        self.current_candles[timeframe].add_tick(price, amount, timestamp)
        
        return None
    
    async def _finalize_candle(self, timeframe: str, builder: CandleBuilder) -> Optional[CandleData]:
        """
        Finalize a candle and compute HFT features.
        
        Args:
            timeframe: Candle timeframe
            builder: CandleBuilder instance
            
        Returns:
            Completed CandleData with features
        """
        try:
            # Build candle data
            candle_data = builder.build()
            
            # Compute HFT features using UNIFIED calculator
            hft_features = None
            if (self.hft_calculator and 
                timeframe in ['15s', '1m'] and 
                self.config.enable_hft_features):
                
                hft_features = await self.hft_calculator.compute_incremental(candle_data)
            
            # Add HFT features to candle
            if hft_features:
                candle_data.hft_features = hft_features.to_dict()
            
            # Store completed candle
            self.completed_candles[timeframe].append(candle_data)
            self.stats['candles_completed'][timeframe] += 1
            
            # Set timestamp_out after all processing
            candle_data.timestamp_out = datetime.now(timezone.utc)
            
            logger.debug(f"🕯️ Completed {timeframe} candle: {candle_data.symbol} @ {candle_data.timestamp_in}")
            
            return candle_data
            
        except Exception as e:
            logger.error(f"❌ Error finalizing {timeframe} candle: {e}")
            return None
    
    async def process_batch(self, 
                          ticks_df, 
                          timeframes: List[str] = None) -> Dict[str, List[CandleData]]:
        """
        Process historical batch of ticks (UNIFIED with historical processor).
        
        Args:
            ticks_df: DataFrame with tick data
            timeframes: Timeframes to process (default: all configured)
            
        Returns:
            Dictionary of timeframe -> list of candles
        """
        try:
            import pandas as pd
            
            if timeframes is None:
                timeframes = self.config.timeframes
            
            # Reset state for batch processing
            self.current_candles.clear()
            for tf in timeframes:
                self.completed_candles[tf].clear()
            
            # Sort ticks by timestamp
            ticks_df = ticks_df.sort_values('timestamp')
            
            all_completed_candles = defaultdict(list)
            
            # Process each tick
            for _, tick_row in ticks_df.iterrows():
                tick_data = {
                    'symbol': self.symbol,
                    'exchange': tick_row.get('exchange', 'unknown'),
                    'timestamp': pd.to_datetime(tick_row['timestamp']),
                    'price': float(tick_row['price']),
                    'amount': float(tick_row['amount']),
                    'side': tick_row.get('side', 'unknown')
                }
                
                completed_candles = await self.process_tick(tick_data)
                
                # Group by timeframe
                for candle in completed_candles:
                    all_completed_candles[candle.timeframe].append(candle)
            
            # Finalize any remaining candles
            for timeframe in timeframes:
                if timeframe in self.current_candles:
                    final_candle = await self._finalize_candle(
                        timeframe, self.current_candles[timeframe]
                    )
                    if final_candle:
                        all_completed_candles[timeframe].append(final_candle)
            
            logger.info(f"✅ Batch processed {len(ticks_df)} ticks into candles")
            for tf, candles in all_completed_candles.items():
                logger.info(f"   {tf}: {len(candles)} candles")
            
            return dict(all_completed_candles)
            
        except Exception as e:
            logger.error(f"❌ Error in batch processing: {e}")
            return {}
    
    async def get_current_candle(self, timeframe: str) -> Optional[CandleData]:
        """Get current incomplete candle for a timeframe"""
        builder = self.current_candles.get(timeframe)
        if builder:
            return builder.build()
        return None
    
    def get_completed_candles(self, 
                            timeframe: str, 
                            limit: int = None) -> List[CandleData]:
        """Get completed candles for a timeframe"""
        candles = list(self.completed_candles.get(timeframe, []))
        if limit:
            return candles[-limit:]
        return candles
    
    def get_latest_candle(self, timeframe: str) -> Optional[CandleData]:
        """Get the latest completed candle for a timeframe"""
        candles = self.completed_candles.get(timeframe, [])
        return candles[-1] if candles else None
    
    def get_stats(self) -> Dict[str, Any]:
        """Get processing statistics"""
        runtime = datetime.now(timezone.utc) - self.stats['start_time']
        
        return {
            'symbol': self.symbol,
            'total_ticks_processed': self.stats['total_ticks_processed'],
            'candles_completed': dict(self.stats['candles_completed']),
            'runtime_seconds': runtime.total_seconds(),
            'ticks_per_second': self.stats['total_ticks_processed'] / max(runtime.total_seconds(), 1),
            'last_tick_time': self.stats['last_tick_time'].isoformat() if self.stats['last_tick_time'] else None,
            'hft_features_enabled': self.config.enable_hft_features
        }
    
    async def shutdown(self) -> None:
        """Shutdown processor and finalize any remaining candles"""
        logger.info(f"🛑 Shutting down LiveCandleProcessor for {self.symbol}...")
        
        # Finalize any remaining candles
        final_candles = []
        for timeframe, builder in self.current_candles.items():
            if builder:
                candle = await self._finalize_candle(timeframe, builder)
                if candle:
                    final_candles.append(candle)
        
        if final_candles:
            logger.info(f"   Finalized {len(final_candles)} remaining candles")
        
        # Shutdown HFT calculator if exists
        if self.hft_calculator:
            # HFT calculator doesn't have shutdown method, just clear state
            pass
        
        logger.info("✅ LiveCandleProcessor shutdown complete")


# Example usage and testing
if __name__ == "__main__":
    import asyncio
    import pandas as pd
    from datetime import datetime, timezone, timedelta
    
    async def test_live_candle_processor():
        # Create processor
        config = LiveProcessingConfig(
            timeframes=['15s', '1m'],
            enable_hft_features=True
        )
        processor = LiveCandleProcessor("BTC-USDT", config)
        
        # Generate test ticks
        base_time = datetime.now(timezone.utc)
        base_price = 67000.0
        
        completed_candles = []
        
        # Process 100 ticks over 2 minutes
        for i in range(100):
            tick_time = base_time + timedelta(seconds=i)
            price = base_price + (i % 10) * 10  # Price oscillation
            amount = 0.1 + (i % 5) * 0.02
            
            tick_data = {
                'symbol': 'BTC-USDT',
                'exchange': 'binance',
                'timestamp': tick_time,
                'price': price,
                'amount': amount,
                'side': 'buy' if i % 2 == 0 else 'sell'
            }
            
            candles = await processor.process_tick(tick_data)
            completed_candles.extend(candles)
            
            if candles:
                for candle in candles:
                    print(f"Completed {candle.timeframe} candle: ${candle.close:.2f} "
                          f"V:{candle.volume:.3f} T:{candle.trade_count}")
                    if hasattr(candle, 'hft_features') and candle.hft_features:
                        features = candle.hft_features
                        print(f"  HFT: SMA5=${features.get('sma_5', 0):.2f} "
                              f"RSI={features.get('rsi_5', 0):.1f}")
        
        # Show statistics
        stats = processor.get_stats()
        print(f"\nStats: {stats}")
        
        await processor.shutdown()
    
    asyncio.run(test_live_candle_processor())
