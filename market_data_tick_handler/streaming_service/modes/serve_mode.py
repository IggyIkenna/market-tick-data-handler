"""
Serve Mode

Serves real-time candles and HFT features to downstream services.
Features published to Redis/in-memory queue for consumption by importers.
"""

import asyncio
import logging
import json
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Callable
from dataclasses import dataclass, asdict
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


@dataclass
class ServeConfig:
    """Configuration for serve mode"""
    transport: str = "inmemory"  # inmemory, redis, grpc
    redis_url: str = "redis://localhost:6379"
    redis_db: int = 0
    max_queue_size: int = 1000
    enable_persistence: bool = False  # Also save to GCS
    gcs_bucket: str = None


class FeatureTransport(ABC):
    """Abstract transport for feature serving"""
    
    @abstractmethod
    async def publish(self, channel: str, data: Dict[str, Any]) -> bool:
        """Publish data to channel"""
        pass
    
    @abstractmethod
    async def subscribe(self, channel: str, callback: Callable) -> None:
        """Subscribe to channel with callback"""
        pass
    
    @abstractmethod
    async def shutdown(self) -> None:
        """Shutdown transport"""
        pass


class InMemoryTransport(FeatureTransport):
    """In-memory transport for testing and single-process use"""
    
    def __init__(self, max_queue_size: int = 1000):
        self.subscribers = {}
        self.max_queue_size = max_queue_size
        
    async def publish(self, channel: str, data: Dict[str, Any]) -> bool:
        """Publish to in-memory subscribers"""
        if channel in self.subscribers:
            for callback in self.subscribers[channel]:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(data)
                    else:
                        callback(data)
                except Exception as e:
                    logger.error(f"❌ Error in subscriber callback: {e}")
        return True
    
    async def subscribe(self, channel: str, callback: Callable) -> None:
        """Subscribe to channel"""
        if channel not in self.subscribers:
            self.subscribers[channel] = []
        self.subscribers[channel].append(callback)
        logger.info(f"✅ Subscribed to channel: {channel}")
    
    async def shutdown(self) -> None:
        """Shutdown transport"""
        self.subscribers.clear()


class RedisTransport(FeatureTransport):
    """Redis transport for distributed serving"""
    
    def __init__(self, redis_url: str, redis_db: int = 0):
        self.redis_url = redis_url
        self.redis_db = redis_db
        self.redis = None
        self.pubsub = None
        
    async def _ensure_connection(self):
        """Ensure Redis connection is established"""
        if self.redis is None:
            try:
                import redis.asyncio as redis
                self.redis = redis.from_url(
                    self.redis_url, 
                    db=self.redis_db,
                    decode_responses=True
                )
                await self.redis.ping()
                logger.info(f"✅ Connected to Redis: {self.redis_url}")
            except ImportError:
                logger.error("❌ redis package not installed. Install with: pip install redis")
                raise
            except Exception as e:
                logger.error(f"❌ Failed to connect to Redis: {e}")
                raise
    
    async def publish(self, channel: str, data: Dict[str, Any]) -> bool:
        """Publish to Redis channel"""
        try:
            await self._ensure_connection()
            
            # Serialize data
            message = json.dumps(data, default=str)
            
            # Publish to Redis
            result = await self.redis.publish(channel, message)
            
            return result > 0  # Returns number of subscribers
            
        except Exception as e:
            logger.error(f"❌ Error publishing to Redis channel {channel}: {e}")
            return False
    
    async def subscribe(self, channel: str, callback: Callable) -> None:
        """Subscribe to Redis channel"""
        try:
            await self._ensure_connection()
            
            if self.pubsub is None:
                self.pubsub = self.redis.pubsub()
            
            await self.pubsub.subscribe(channel)
            
            # Start listening task
            asyncio.create_task(self._listen_to_channel(channel, callback))
            
            logger.info(f"✅ Subscribed to Redis channel: {channel}")
            
        except Exception as e:
            logger.error(f"❌ Error subscribing to Redis channel {channel}: {e}")
    
    async def _listen_to_channel(self, channel: str, callback: Callable):
        """Listen to Redis channel and call callback"""
        try:
            async for message in self.pubsub.listen():
                if message['type'] == 'message':
                    try:
                        data = json.loads(message['data'])
                        
                        if asyncio.iscoroutinefunction(callback):
                            await callback(data)
                        else:
                            callback(data)
                            
                    except Exception as e:
                        logger.error(f"❌ Error processing Redis message: {e}")
                        
        except Exception as e:
            logger.error(f"❌ Error listening to Redis channel {channel}: {e}")
    
    async def shutdown(self) -> None:
        """Shutdown Redis connection"""
        if self.pubsub:
            await self.pubsub.close()
        if self.redis:
            await self.redis.close()


class ServeMode:
    """
    Serve mode for streaming features to downstream services.
    
    Publishes real-time candles and HFT features to transport layer
    for consumption by execution systems, features services, etc.
    """
    
    def __init__(self, config: ServeConfig):
        """
        Initialize serve mode.
        
        Args:
            config: Serve mode configuration
        """
        self.config = config
        self.transport = self._create_transport()
        self.subscribers = {}
        
        # Statistics
        self.stats = {
            'features_served': 0,
            'candles_served': 0,
            'subscribers': 0,
            'errors': 0,
            'start_time': datetime.now(timezone.utc)
        }
        
        logger.info("✅ ServeMode initialized")
        logger.info(f"   Transport: {config.transport}")
    
    def _create_transport(self) -> FeatureTransport:
        """Create transport based on configuration"""
        if self.config.transport == "redis":
            return RedisTransport(self.config.redis_url, self.config.redis_db)
        elif self.config.transport == "inmemory":
            return InMemoryTransport(self.config.max_queue_size)
        else:
            raise ValueError(f"Unsupported transport: {self.config.transport}")
    
    async def serve_candle_with_features(self, candle_data, hft_features=None) -> bool:
        """
        Serve candle data with HFT features.
        
        Args:
            candle_data: CandleData object
            hft_features: HFTFeatures object (optional)
            
        Returns:
            True if successful
        """
        try:
            # Prepare data for serving
            serve_data = {
                'type': 'candle_with_features',
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'symbol': candle_data.symbol,
                'timeframe': candle_data.timeframe,
                'candle': {
                    'timestamp_in': candle_data.timestamp_in.isoformat(),
                    'timestamp_out': candle_data.timestamp_out.isoformat() if candle_data.timestamp_out else None,
                    'open': candle_data.open,
                    'high': candle_data.high,
                    'low': candle_data.low,
                    'close': candle_data.close,
                    'volume': candle_data.volume,
                    'trade_count': candle_data.trade_count,
                    'vwap': candle_data.vwap
                },
                'hft_features': hft_features.to_dict() if hft_features else None
            }
            
            # Publish to general candle channel
            channel = f"candles:{candle_data.symbol}:{candle_data.timeframe}"
            success = await self.transport.publish(channel, serve_data)
            
            # Also publish to symbol-specific channel
            symbol_channel = f"candles:{candle_data.symbol}"
            await self.transport.publish(symbol_channel, serve_data)
            
            # Update statistics
            self.stats['candles_served'] += 1
            if hft_features:
                self.stats['features_served'] += 1
            
            logger.debug(f"📡 Served candle: {candle_data.symbol} {candle_data.timeframe} @ {candle_data.close}")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Error serving candle with features: {e}")
            self.stats['errors'] += 1
            return False
    
    async def serve_tick_data(self, tick_data: Dict[str, Any]) -> bool:
        """
        Serve raw tick data.
        
        Args:
            tick_data: Processed tick data
            
        Returns:
            True if successful
        """
        try:
            # Prepare tick data for serving
            serve_data = {
                'type': 'tick',
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'symbol': tick_data['symbol'],
                'exchange': tick_data['exchange'],
                'data_type': tick_data['data_type'],
                'data': tick_data
            }
            
            # Publish to tick channel
            channel = f"ticks:{tick_data['symbol']}:{tick_data['data_type']}"
            success = await self.transport.publish(channel, serve_data)
            
            logger.debug(f"📡 Served tick: {tick_data['symbol']} {tick_data['data_type']}")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Error serving tick data: {e}")
            self.stats['errors'] += 1
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get serving statistics"""
        runtime = datetime.now(timezone.utc) - self.stats['start_time']
        
        return {
            'transport': self.config.transport,
            'features_served': self.stats['features_served'],
            'candles_served': self.stats['candles_served'],
            'subscribers': self.stats['subscribers'],
            'errors': self.stats['errors'],
            'runtime_seconds': runtime.total_seconds(),
            'candles_per_second': self.stats['candles_served'] / max(runtime.total_seconds(), 1),
            'error_rate': self.stats['errors'] / max(self.stats['candles_served'], 1)
        }
    
    async def shutdown(self) -> None:
        """Shutdown serve mode"""
        logger.info("🛑 Shutting down ServeMode...")
        
        await self.transport.shutdown()
        
        logger.info("✅ ServeMode shutdown complete")


class LiveFeatureStream:
    """
    Client interface for consuming live features from serve mode.
    
    Example usage:
    ```python
    stream = LiveFeatureStream(symbol="BTC-USDT", timeframe="1m")
    async for candle_with_features in stream:
        # Use features in execution system
        execute_strategy(candle_with_features)
    ```
    """
    
    def __init__(self, 
                 symbol: str,
                 timeframe: str = None,
                 transport_config: Dict[str, Any] = None):
        """
        Initialize live feature stream.
        
        Args:
            symbol: Trading symbol to subscribe to
            timeframe: Specific timeframe (optional, subscribes to all if None)
            transport_config: Transport configuration
        """
        self.symbol = symbol
        self.timeframe = timeframe
        self.transport_config = transport_config or {'transport': 'inmemory'}
        
        self.transport = None
        self.queue = asyncio.Queue()
        self.running = False
    
    async def __aenter__(self):
        """Async context manager entry"""
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.stop()
    
    async def start(self) -> None:
        """Start streaming"""
        config = ServeConfig(**self.transport_config)
        serve_mode = ServeMode(config)
        self.transport = serve_mode.transport
        
        # Subscribe to appropriate channel
        if self.timeframe:
            channel = f"candles:{self.symbol}:{self.timeframe}"
        else:
            channel = f"candles:{self.symbol}"
        
        await self.transport.subscribe(channel, self._on_message)
        self.running = True
        
        logger.info(f"✅ Started LiveFeatureStream for {self.symbol}")
    
    async def _on_message(self, data: Dict[str, Any]) -> None:
        """Handle incoming message"""
        await self.queue.put(data)
    
    async def __aiter__(self):
        """Async iterator"""
        return self
    
    async def __anext__(self):
        """Get next item"""
        if not self.running:
            raise StopAsyncIteration
        
        try:
            # Wait for next message with timeout
            data = await asyncio.wait_for(self.queue.get(), timeout=1.0)
            return data
        except asyncio.TimeoutError:
            if self.running:
                return await self.__anext__()
            else:
                raise StopAsyncIteration
    
    async def stop(self) -> None:
        """Stop streaming"""
        self.running = False
        if self.transport:
            await self.transport.shutdown()


# Example usage
if __name__ == "__main__":
    import asyncio
    from datetime import datetime, timezone
    
    async def test_serve_mode():
        # Test in-memory serving
        config = ServeConfig(transport="inmemory")
        serve_mode = ServeMode(config)
        
        # Mock candle data
        class MockCandle:
            def __init__(self):
                self.symbol = "BTC-USDT"
                self.timeframe = "1m"
                self.timestamp_in = datetime.now(timezone.utc)
                self.timestamp_out = datetime.now(timezone.utc)
                self.open = 67000.0
                self.high = 67100.0
                self.low = 66900.0
                self.close = 67050.0
                self.volume = 1.5
                self.trade_count = 45
                self.vwap = 67025.0
        
        # Mock HFT features
        class MockFeatures:
            def to_dict(self):
                return {
                    'sma_5': 67025.0,
                    'ema_5': 67030.0,
                    'rsi_5': 55.2
                }
        
        candle = MockCandle()
        features = MockFeatures()
        
        # Test serving
        success = await serve_mode.serve_candle_with_features(candle, features)
        print(f"Serve success: {success}")
        
        # Test consumer
        async def consumer(data):
            print(f"Received: {data['symbol']} @ ${data['candle']['close']}")
            if data.get('hft_features'):
                print(f"  SMA5: {data['hft_features']['sma_5']}")
        
        await serve_mode.transport.subscribe("candles:BTC-USDT:1m", consumer)
        
        # Serve another candle
        await serve_mode.serve_candle_with_features(candle, features)
        
        # Show stats
        stats = serve_mode.get_stats()
        print(f"Stats: {stats}")
        
        await serve_mode.shutdown()
    
    asyncio.run(test_serve_mode())
