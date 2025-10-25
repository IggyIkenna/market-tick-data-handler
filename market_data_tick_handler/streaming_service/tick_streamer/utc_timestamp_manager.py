"""
UTC Timestamp Management for Market Data Streaming

Handles proper UTC timestamp alignment to even intervals from midnight.
Ensures candles start at exact time boundaries (00:00, 00:01, 00:15, etc.)
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class TimestampPair:
    """Pair of timestamps for latency tracking"""
    timestamp_in: datetime  # Actual candle/interval time (aligned to UTC)
    timestamp_out: datetime  # Processing/output time (when sent)
    
    @property
    def latency_ms(self) -> float:
        """Calculate latency in milliseconds"""
        return (self.timestamp_out - self.timestamp_in).total_seconds() * 1000


class UTCTimestampManager:
    """
    Manages UTC timestamp alignment for market data streaming.
    
    Ensures all candles and intervals align to even UTC time boundaries:
    - 15s: :00, :15, :30, :45 of each minute
    - 1m: :00 of each minute  
    - 5m: :00, :05, :10, :15, :20, :25, :30, :35, :40, :45, :50, :55
    - 15m: :00, :15, :30, :45 of each hour
    - 4h: 00:00, 04:00, 08:00, 12:00, 16:00, 20:00
    - 24h: 00:00 UTC each day
    """
    
    # Supported timeframes in seconds
    TIMEFRAMES = {
        '15s': 15,
        '1m': 60,
        '5m': 300,
        '15m': 900,
        '4h': 14400,
        '24h': 86400
    }
    
    def __init__(self):
        self.current_candles: Dict[str, Optional[datetime]] = {}
        
    def get_aligned_timestamp(self, timestamp: datetime, timeframe: str) -> datetime:
        """
        Get UTC-aligned timestamp for the given timeframe.
        
        Args:
            timestamp: Input timestamp (any timezone)
            timeframe: Timeframe string ('15s', '1m', '5m', '15m', '4h', '24h')
            
        Returns:
            UTC-aligned timestamp for the timeframe boundary
        """
        if timeframe not in self.TIMEFRAMES:
            raise ValueError(f"Unsupported timeframe: {timeframe}. Supported: {list(self.TIMEFRAMES.keys())}")
        
        # Convert to UTC if not already
        if timestamp.tzinfo is None:
            utc_timestamp = timestamp.replace(tzinfo=timezone.utc)
        else:
            utc_timestamp = timestamp.astimezone(timezone.utc)
        
        interval_seconds = self.TIMEFRAMES[timeframe]
        
        # Get seconds since UTC midnight
        midnight_utc = utc_timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
        seconds_since_midnight = int((utc_timestamp - midnight_utc).total_seconds())
        
        # Align to interval boundary
        aligned_seconds = (seconds_since_midnight // interval_seconds) * interval_seconds
        
        # Create aligned timestamp
        aligned_timestamp = midnight_utc + timedelta(seconds=aligned_seconds)
        
        return aligned_timestamp
    
    def should_finalize_candle(self, timestamp: datetime, timeframe: str) -> bool:
        """
        Check if we should finalize the current candle for this timeframe.
        
        Args:
            timestamp: Current timestamp
            timeframe: Timeframe to check
            
        Returns:
            True if candle should be finalized
        """
        aligned_timestamp = self.get_aligned_timestamp(timestamp, timeframe)
        current_candle_time = self.current_candles.get(timeframe)
        
        if current_candle_time is None:
            # No current candle, start new one
            self.current_candles[timeframe] = aligned_timestamp
            return False
        
        if aligned_timestamp != current_candle_time:
            # Time boundary crossed, finalize current candle
            self.current_candles[timeframe] = aligned_timestamp
            return True
        
        return False
    
    def create_timestamp_pair(self, candle_time: datetime) -> TimestampPair:
        """
        Create a timestamp pair for latency tracking.
        
        Args:
            candle_time: The aligned candle timestamp (timestamp_in)
            
        Returns:
            TimestampPair with current time as timestamp_out
        """
        return TimestampPair(
            timestamp_in=candle_time,
            timestamp_out=datetime.now(timezone.utc)
        )
    
    def get_next_boundary(self, timestamp: datetime, timeframe: str) -> datetime:
        """
        Get the next time boundary for the given timeframe.
        
        Args:
            timestamp: Current timestamp
            timeframe: Timeframe string
            
        Returns:
            Next boundary timestamp
        """
        current_boundary = self.get_aligned_timestamp(timestamp, timeframe)
        interval_seconds = self.TIMEFRAMES[timeframe]
        
        return current_boundary + timedelta(seconds=interval_seconds)
    
    def get_time_until_next_boundary(self, timestamp: datetime, timeframe: str) -> float:
        """
        Get seconds until the next time boundary.
        
        Args:
            timestamp: Current timestamp
            timeframe: Timeframe string
            
        Returns:
            Seconds until next boundary
        """
        next_boundary = self.get_next_boundary(timestamp, timeframe)
        utc_timestamp = timestamp.astimezone(timezone.utc) if timestamp.tzinfo else timestamp.replace(tzinfo=timezone.utc)
        
        return (next_boundary - utc_timestamp).total_seconds()
    
    def format_timestamp(self, timestamp: datetime) -> str:
        """
        Format timestamp for logging/display.
        
        Args:
            timestamp: Timestamp to format
            
        Returns:
            Formatted timestamp string
        """
        return timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')
    
    def validate_alignment(self, timestamp: datetime, timeframe: str) -> bool:
        """
        Validate that a timestamp is properly aligned for the timeframe.
        
        Args:
            timestamp: Timestamp to validate
            timeframe: Expected timeframe
            
        Returns:
            True if properly aligned
        """
        aligned = self.get_aligned_timestamp(timestamp, timeframe)
        return timestamp == aligned
    
    def get_alignment_info(self, timestamp: datetime) -> Dict[str, datetime]:
        """
        Get alignment info for all timeframes.
        
        Args:
            timestamp: Input timestamp
            
        Returns:
            Dict mapping timeframes to aligned timestamps
        """
        return {
            tf: self.get_aligned_timestamp(timestamp, tf)
            for tf in self.TIMEFRAMES.keys()
        }


# Example usage and testing
if __name__ == "__main__":
    import json
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Create timestamp manager
    tm = UTCTimestampManager()
    
    # Test with current time
    now = datetime.now(timezone.utc)
    print(f"Current UTC time: {tm.format_timestamp(now)}")
    print()
    
    # Show alignments for all timeframes
    alignments = tm.get_alignment_info(now)
    print("Aligned timestamps:")
    for timeframe, aligned_ts in alignments.items():
        print(f"  {timeframe:>4}: {tm.format_timestamp(aligned_ts)}")
    print()
    
    # Test boundary detection
    print("Time until next boundaries:")
    for timeframe in tm.TIMEFRAMES.keys():
        seconds_until = tm.get_time_until_next_boundary(now, timeframe)
        print(f"  {timeframe:>4}: {seconds_until:.1f}s")
    
    # Test timestamp pair creation
    candle_time = tm.get_aligned_timestamp(now, '1m')
    ts_pair = tm.create_timestamp_pair(candle_time)
    print(f"\nTimestamp pair example:")
    print(f"  timestamp_in:  {tm.format_timestamp(ts_pair.timestamp_in)}")
    print(f"  timestamp_out: {tm.format_timestamp(ts_pair.timestamp_out)}")
    print(f"  latency:       {ts_pair.latency_ms:.1f}ms")
