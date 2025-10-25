"""
Tick Handler

Receives and processes tick data from Node.js streamer.
Handles routing to appropriate processors based on data type.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

from .data_type_router import DataTypeRouter

logger = logging.getLogger(__name__)


@dataclass
class TickData:
    """Standardized tick data structure"""
    symbol: str
    exchange: str
    data_type: str
    timestamp: datetime
    local_timestamp: datetime
    data: Dict[str, Any]
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class TickHandler:
    """
    Handles incoming tick data from Node.js streamer.
    
    Routes ticks to appropriate processors based on data type.
    Supports all Tardis data types with fallback strategies.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize tick handler.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config or {}
        self.router = DataTypeRouter(config)
        self.processors = {}
        self.stats = {
            'total_ticks': 0,
            'ticks_by_type': {},
            'errors': 0,
            'start_time': datetime.now(timezone.utc)
        }
        
        logger.info("✅ TickHandler initialized")
    
    async def process_tick(self, raw_tick: Dict[str, Any]) -> Optional[TickData]:
        """
        Process a single tick from Node.js streamer.
        
        Args:
            raw_tick: Raw tick data from Node.js
            
        Returns:
            Processed TickData or None if processing failed
        """
        try:
            # Parse and validate tick data
            tick_data = self._parse_tick(raw_tick)
            if not tick_data:
                return None
            
            # Route to appropriate processor
            processor = await self.router.get_processor(tick_data.data_type)
            if not processor:
                logger.warning(f"No processor found for data type: {tick_data.data_type}")
                return None
            
            # Process the tick
            result = await processor.process(tick_data)
            
            # Update statistics
            self._update_stats(tick_data.data_type, success=True)
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Error processing tick: {e}")
            self._update_stats("unknown", success=False)
            return None
    
    def _parse_tick(self, raw_tick: Dict[str, Any]) -> Optional[TickData]:
        """
        Parse raw tick data into standardized format.
        
        Args:
            raw_tick: Raw tick from Node.js
            
        Returns:
            Parsed TickData or None if invalid
        """
        try:
            # Extract required fields
            symbol = raw_tick.get('symbol')
            exchange = raw_tick.get('exchange')
            data_type = raw_tick.get('type', 'trade')
            timestamp_str = raw_tick.get('timestamp')
            
            if not all([symbol, exchange, timestamp_str]):
                logger.warning(f"Missing required fields in tick: {raw_tick}")
                return None
            
            # Parse timestamp
            if isinstance(timestamp_str, str):
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            else:
                timestamp = datetime.fromtimestamp(timestamp_str / 1000, tz=timezone.utc)
            
            # Create standardized tick data
            tick_data = TickData(
                symbol=symbol,
                exchange=exchange,
                data_type=data_type,
                timestamp=timestamp,
                local_timestamp=datetime.now(timezone.utc),
                data=raw_tick,
                metadata={
                    'source': 'tardis_realtime',
                    'processing_time': datetime.now(timezone.utc).isoformat()
                }
            )
            
            return tick_data
            
        except Exception as e:
            logger.error(f"❌ Error parsing tick: {e}")
            return None
    
    def _update_stats(self, data_type: str, success: bool = True) -> None:
        """Update processing statistics"""
        self.stats['total_ticks'] += 1
        
        if data_type not in self.stats['ticks_by_type']:
            self.stats['ticks_by_type'][data_type] = 0
        self.stats['ticks_by_type'][data_type] += 1
        
        if not success:
            self.stats['errors'] += 1
    
    async def get_processor(self, data_type: str):
        """Get processor for specific data type"""
        return await self.router.get_processor(data_type)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get processing statistics"""
        runtime = datetime.now(timezone.utc) - self.stats['start_time']
        
        return {
            'total_ticks': self.stats['total_ticks'],
            'ticks_by_type': self.stats['ticks_by_type'],
            'errors': self.stats['errors'],
            'runtime_seconds': runtime.total_seconds(),
            'ticks_per_second': self.stats['total_ticks'] / max(runtime.total_seconds(), 1),
            'error_rate': self.stats['errors'] / max(self.stats['total_ticks'], 1)
        }
    
    async def shutdown(self) -> None:
        """Shutdown tick handler and cleanup resources"""
        logger.info("🛑 Shutting down TickHandler...")
        
        # Shutdown all processors
        for processor in self.processors.values():
            if hasattr(processor, 'shutdown'):
                await processor.shutdown()
        
        logger.info("✅ TickHandler shutdown complete")


# Example usage
if __name__ == "__main__":
    import asyncio
    from datetime import datetime, timezone
    
    async def test_tick_handler():
        handler = TickHandler()
        
        # Test tick data
        test_tick = {
            'symbol': 'BTC-USDT',
            'exchange': 'binance',
            'type': 'trade',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'price': 67000.0,
            'amount': 0.1,
            'side': 'buy'
        }
        
        result = await handler.process_tick(test_tick)
        print(f"Processed tick: {result}")
        
        stats = handler.get_stats()
        print(f"Stats: {stats}")
        
        await handler.shutdown()
    
    asyncio.run(test_tick_handler())
