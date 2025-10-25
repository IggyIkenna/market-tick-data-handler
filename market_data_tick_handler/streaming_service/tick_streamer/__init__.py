"""
Live tick data streaming module
"""

# Import working components
from .utc_timestamp_manager import UTCTimestampManager

# LiveTickStreamer available via lazy import to avoid circular dependencies
def get_live_tick_streamer():
    """Get LiveTickStreamer with lazy import"""
    from .live_tick_streamer import LiveTickStreamer
    return LiveTickStreamer

__all__ = ["UTCTimestampManager", "get_live_tick_streamer"]
