"""
Candle data structures with timestamp tracking
"""

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import json


@dataclass
class CandleData:
    """
    OHLCV candle data with timestamp tracking for latency monitoring.
    
    timestamp_in: The aligned UTC timestamp (candle boundary time)
    timestamp_out: When the candle was processed/sent (for latency tracking)
    """
    symbol: str
    exchange: str
    timeframe: str
    timestamp_in: datetime  # Aligned candle time (UTC boundary)
    timestamp_out: datetime  # Processing time (when sent)
    open: float
    high: float
    low: float
    close: float
    volume: float
    trade_count: int
    vwap: Optional[float] = None  # Volume-weighted average price
    
    @property
    def latency_ms(self) -> float:
        """Calculate processing latency in milliseconds"""
        return (self.timestamp_out - self.timestamp_in).total_seconds() * 1000
    
    @property
    def price_change(self) -> float:
        """Calculate price change (close - open)"""
        return self.close - self.open
    
    @property
    def price_change_pct(self) -> float:
        """Calculate price change percentage"""
        if self.open == 0:
            return 0.0
        return (self.price_change / self.open) * 100
    
    @property
    def is_green(self) -> bool:
        """Check if candle is green (close >= open)"""
        return self.close >= self.open
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'symbol': self.symbol,
            'exchange': self.exchange,
            'timeframe': self.timeframe,
            'timestamp_in': self.timestamp_in.isoformat(),
            'timestamp_out': self.timestamp_out.isoformat(),
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume,
            'trade_count': self.trade_count,
            'vwap': self.vwap,
            'latency_ms': self.latency_ms,
            'price_change': self.price_change,
            'price_change_pct': self.price_change_pct,
            'is_green': self.is_green
        }
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict(), default=str)
    
    def __str__(self) -> str:
        """String representation for logging"""
        color = "🟢" if self.is_green else "🔴"
        return (f"{color} {self.timeframe} {self.timestamp_in.strftime('%H:%M:%S')} | "
                f"O=${self.open:.2f} H=${self.high:.2f} L=${self.low:.2f} C=${self.close:.2f} | "
                f"V={self.volume:.4f} T={self.trade_count} | "
                f"Δ{self.price_change:+.2f} ({self.price_change_pct:+.2f}%) | "
                f"Latency={self.latency_ms:.1f}ms")


@dataclass
class CandleBuilder:
    """
    Helper class for building candles from tick data.
    Accumulates trades within a time period.
    """
    symbol: str
    exchange: str
    timeframe: str
    timestamp_in: datetime
    
    # OHLCV data
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: float = 0.0
    trade_count: int = 0
    
    # For VWAP calculation
    volume_weighted_sum: float = 0.0
    
    def add_trade(self, price: float, amount: float) -> None:
        """Add a trade to this candle"""
        # Update OHLC
        if self.open is None:
            self.open = price
        
        if self.high is None or price > self.high:
            self.high = price
            
        if self.low is None or price < self.low:
            self.low = price
            
        self.close = price
        
        # Update volume and trade count
        self.volume += amount
        self.trade_count += 1
        
        # Update VWAP calculation
        self.volume_weighted_sum += price * amount
    
    @property
    def vwap(self) -> Optional[float]:
        """Calculate volume-weighted average price"""
        if self.volume == 0:
            return None
        return self.volume_weighted_sum / self.volume
    
    def finalize(self, timestamp_out: Optional[datetime] = None) -> CandleData:
        """
        Finalize the candle and return CandleData.
        
        Args:
            timestamp_out: Processing timestamp (defaults to current UTC time)
        """
        if timestamp_out is None:
            timestamp_out = datetime.now(timezone.utc)
        
        # Handle empty candle (no trades)
        if self.open is None:
            # Use previous close or 0
            self.open = self.high = self.low = self.close = 0.0
        
        return CandleData(
            symbol=self.symbol,
            exchange=self.exchange,
            timeframe=self.timeframe,
            timestamp_in=self.timestamp_in,
            timestamp_out=timestamp_out,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
            trade_count=self.trade_count,
            vwap=self.vwap
        )
    
    def is_empty(self) -> bool:
        """Check if candle has no trades"""
        return self.trade_count == 0
    
    def __str__(self) -> str:
        """String representation for debugging"""
        return (f"CandleBuilder({self.timeframe} {self.timestamp_in.strftime('%H:%M:%S')} | "
                f"T={self.trade_count} V={self.volume:.4f})")


# Example usage and testing
if __name__ == "__main__":
    from datetime import datetime, timezone, timedelta
    
    # Test candle building
    timestamp = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    
    builder = CandleBuilder(
        symbol="BTC-USDT",
        exchange="binance", 
        timeframe="1m",
        timestamp_in=timestamp
    )
    
    # Add some trades
    trades = [
        (67000.0, 0.1),
        (67050.0, 0.05),
        (66980.0, 0.15),
        (67020.0, 0.08)
    ]
    
    for price, amount in trades:
        builder.add_trade(price, amount)
    
    # Finalize candle
    candle = builder.finalize()
    
    print("Test Candle:")
    print(candle)
    print()
    print("JSON representation:")
    print(candle.to_json())
