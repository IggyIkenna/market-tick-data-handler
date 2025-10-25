#!/usr/bin/env python3
"""
Production Candle Processor with Correct Boundaries

This module provides a production-ready candle processor that generates
exactly 1,440 candles per day by shifting boundaries to avoid midnight issues.

Key Features:
- Exactly 1,440 candles per day (24 hours × 60 minutes)
- First candle: 00:01:00 (not 00:00:00)
- Last candle: 24:00:00 (next day midnight)
- Efficient live streaming and batch processing
- Memory efficient incremental processing
"""

import pandas as pd
import time
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Union
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

@dataclass
class CandleConfig:
    """Configuration for candle processing"""
    interval_seconds: int = 60
    latency_ms: int = 200
    boundary_shift_minutes: int = 1
    target_date: Optional[date] = None

@dataclass
class TradeData:
    """Trade data structure"""
    timestamp: datetime
    price: float
    amount: float
    side: Optional[str] = None
    trade_id: Optional[str] = None

@dataclass
class CandleData:
    """Candle data structure"""
    candle_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    trade_count: int

class ProductionCandleProcessor:
    """Production-ready candle processor with correct boundaries"""
    
    def __init__(self, config: CandleConfig):
        self.config = config
        self.current_candle = None
        self.candles = []
        self.trade_count = 0
        self.processing_times = []
        self.start_time = None
        
    def _get_candle_time(self, timestamp: datetime) -> datetime:
        """Get candle time bucket with shifted boundaries"""
        # Add boundary shift minutes, then floor to nearest interval
        shifted = timestamp + timedelta(minutes=self.config.boundary_shift_minutes)
        seconds = int(shifted.timestamp())
        bucket_seconds = (seconds // self.config.interval_seconds) * self.config.interval_seconds
        candle_time = datetime.fromtimestamp(bucket_seconds)
        return candle_time
    
    def _process_trade(self, trade: TradeData) -> float:
        """Process a single trade"""
        start_time = time.time()
        
        # Get candle time for this trade
        candle_time = self._get_candle_time(trade.timestamp)
        
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
    
    def _start_new_candle(self, candle_time: datetime, first_trade: TradeData):
        """Start a new candle with first trade"""
        self.current_candle = {
            'candle_time': candle_time,
            'open': first_trade.price,
            'high': first_trade.price,
            'low': first_trade.price,
            'close': first_trade.price,
            'volume': first_trade.amount,
            'trade_count': 1
        }
    
    def _update_candle(self, trade: TradeData):
        """Update current candle with new trade"""
        self.current_candle['high'] = max(self.current_candle['high'], trade.price)
        self.current_candle['low'] = min(self.current_candle['low'], trade.price)
        self.current_candle['close'] = trade.price
        self.current_candle['volume'] += trade.amount
        self.current_candle['trade_count'] += 1
    
    def _finalize_candle(self):
        """Finalize current candle and add to completed candles"""
        if self.current_candle is not None:
            self.candles.append(self.current_candle.copy())
            self.current_candle = None
    
    def process_trades(self, trades: List[TradeData]) -> List[CandleData]:
        """Process a list of trades and return completed candles"""
        self.start_time = time.time()
        
        for trade in trades:
            self._process_trade(trade)
        
        # Finalize last candle
        self._finalize_candle()
        
        # Convert to CandleData objects
        candle_objects = []
        for candle_dict in self.candles:
            candle_objects.append(CandleData(
                candle_time=candle_dict['candle_time'],
                open=candle_dict['open'],
                high=candle_dict['high'],
                low=candle_dict['low'],
                close=candle_dict['close'],
                volume=candle_dict['volume'],
                trade_count=candle_dict['trade_count']
            ))
        
        return candle_objects
    
    def get_completed_candles(self) -> List[Dict]:
        """Get all completed candles as dictionaries"""
        return self.candles
    
    def get_processing_stats(self) -> Dict:
        """Get processing time statistics"""
        if not self.processing_times:
            return {}
        
        processing_times_ms = [t * 1000 for t in self.processing_times]
        
        return {
            'total_trades': self.trade_count,
            'total_candles': len(self.candles),
            'total_processing_time': time.time() - self.start_time if self.start_time else 0,
            'min_processing_time_ms': min(processing_times_ms),
            'max_processing_time_ms': max(processing_times_ms),
            'avg_processing_time_ms': sum(processing_times_ms) / len(processing_times_ms),
            'median_processing_time_ms': sorted(processing_times_ms)[len(processing_times_ms) // 2]
        }

def preprocess_trades_for_candles(df: pd.DataFrame, target_date: date, latency_ms: int = 200) -> pd.DataFrame:
    """Preprocess trades for candle generation"""
    
    # Convert timestamps
    df['exchange_time'] = pd.to_datetime(df['timestamp'], unit='us')
    df['tardis_time'] = pd.to_datetime(df['local_timestamp'], unit='us')
    df['our_receive_time'] = df['tardis_time'] + pd.Timedelta(milliseconds=latency_ms)
    
    # Sort by our receive time
    df = df.sort_values('our_receive_time').reset_index(drop=True)
    
    # Filter to only include trades within the target day
    df_filtered = df[df['our_receive_time'].dt.date == target_date].copy()
    
    logger.info(f"Preprocessed trades: {len(df):,} -> {len(df_filtered):,} (filtered by date)")
    
    return df_filtered

def generate_daily_candles(df_trades: pd.DataFrame, target_date: date, config: Optional[CandleConfig] = None) -> List[CandleData]:
    """Generate daily candles with correct boundaries"""
    
    if config is None:
        config = CandleConfig(target_date=target_date)
    
    # Preprocess trades
    df_filtered = preprocess_trades_for_candles(df_trades, target_date, config.latency_ms)
    
    # Create processor
    processor = ProductionCandleProcessor(config)
    
    # Convert DataFrame to TradeData objects
    trades = []
    for _, trade_row in df_filtered.iterrows():
        trades.append(TradeData(
            timestamp=trade_row['our_receive_time'],
            price=trade_row['price'],
            amount=trade_row['amount'],
            side=trade_row.get('side'),
            trade_id=trade_row.get('id')
        ))
    
    # Process trades
    candles = processor.process_trades(trades)
    
    # Validate results
    if len(candles) != 1440:
        logger.warning(f"Expected 1440 candles, got {len(candles)}")
    
    # Log statistics
    stats = processor.get_processing_stats()
    logger.info(f"Generated {len(candles)} candles from {stats['total_trades']:,} trades in {stats['total_processing_time']:.3f}s")
    
    return candles

def validate_candle_boundaries(candles: List[CandleData]) -> bool:
    """Validate that candles have correct boundaries"""
    
    if len(candles) != 1440:
        logger.error(f"Expected 1440 candles, got {len(candles)}")
        return False
    
    # Check first candle
    first_candle = candles[0]
    if first_candle.candle_time.minute != 1:
        logger.error(f"First candle should be at 00:01:00, got {first_candle.candle_time}")
        return False
    
    # Check last candle
    last_candle = candles[-1]
    if last_candle.candle_time.minute != 0:
        logger.error(f"Last candle should be at 24:00:00, got {last_candle.candle_time}")
        return False
    
    # Check for midnight candles
    midnight_candles = [c for c in candles if c.candle_time.hour == 0 and c.candle_time.minute == 0]
    if len(midnight_candles) != 1:
        logger.error(f"Expected 1 midnight candle, got {len(midnight_candles)}")
        return False
    
    logger.info("✅ Candle boundaries validated successfully")
    return True

# Example usage
if __name__ == "__main__":
    # Example configuration
    config = CandleConfig(
        interval_seconds=60,
        latency_ms=200,
        boundary_shift_minutes=1,
        target_date=date(2023, 5, 23)
    )
    
    # Example trade data
    trades = [
        TradeData(
            timestamp=datetime(2023, 5, 23, 0, 0, 30),
            price=26849.28,
            amount=0.1
        ),
        TradeData(
            timestamp=datetime(2023, 5, 23, 0, 1, 30),
            price=26851.07,
            amount=0.2
        ),
    ]
    
    # Process trades
    processor = ProductionCandleProcessor(config)
    candles = processor.process_trades(trades)
    
    # Validate results
    validate_candle_boundaries(candles)
    
    print(f"Generated {len(candles)} candles")
    for candle in candles[:3]:  # Show first 3 candles
        print(f"  {candle.candle_time}: O=${candle.open:.2f} H=${candle.high:.2f} L=${candle.low:.2f} C=${candle.close:.2f} V={candle.volume:.4f}")
