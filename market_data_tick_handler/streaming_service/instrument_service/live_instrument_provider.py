"""
Live Instrument Provider

Provides live instrument definitions with in-memory cache and TTL.
Similar to canonical_key_generator.py but for live data without persistence.
Addresses Issue #004 - Live CCXT Instrument Definitions.
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List, Set
from dataclasses import dataclass
import json

from .ccxt_adapter import CCXTAdapter
from .instrument_mapper import InstrumentMapper
from ...models import InstrumentDefinition

logger = logging.getLogger(__name__)


@dataclass
class LiveInstrumentConfig:
    """Configuration for live instrument provider"""
    exchanges: List[str] = None
    refresh_interval: int = 300  # 5 minutes
    cache_ttl: int = 600  # 10 minutes
    base_currencies: List[str] = None
    quote_currencies: List[str] = None
    instrument_types: List[str] = None
    min_volume_24h: float = 0
    enable_monitoring: bool = True
    
    def __post_init__(self):
        if self.exchanges is None:
            self.exchanges = ["binance", "deribit", "coinbase"]
        if self.base_currencies is None:
            self.base_currencies = ["BTC", "ETH", "ADA", "SOL", "MATIC", "AVAX"]
        if self.quote_currencies is None:
            self.quote_currencies = ["USDT", "USD", "BTC", "ETH"]
        if self.instrument_types is None:
            self.instrument_types = ["spot", "swap", "future", "option"]


class LiveInstrumentProvider:
    """
    Provides live instrument definitions from CCXT with caching.
    
    Features:
    - In-memory cache with TTL (no persistence)
    - Automatic refresh based on intervals
    - Real-time instrument discovery
    - Integration with existing InstrumentDefinition format
    - Change monitoring and notifications
    """
    
    def __init__(self, config: LiveInstrumentConfig = None):
        """
        Initialize live instrument provider.
        
        Args:
            config: Provider configuration
        """
        self.config = config or LiveInstrumentConfig()
        self.ccxt_adapter = CCXTAdapter()
        self.mapper = InstrumentMapper()
        
        # In-memory cache
        self.cache: Dict[str, Dict[str, Any]] = {}  # exchange -> instruments
        self.cache_timestamps: Dict[str, datetime] = {}
        self.last_refresh = {}
        
        # Monitoring
        self.stats = {
            'total_instruments': 0,
            'instruments_by_exchange': {},
            'cache_hits': 0,
            'cache_misses': 0,
            'refresh_count': 0,
            'errors': 0,
            'start_time': datetime.now(timezone.utc)
        }
        
        # Change tracking
        self.previous_instruments: Dict[str, Set[str]] = {}
        self.change_callbacks = []
        
        # Background refresh task
        self.refresh_task = None
        self.running = False
        
        logger.info("✅ LiveInstrumentProvider initialized")
        logger.info(f"   Exchanges: {self.config.exchanges}")
        logger.info(f"   Refresh interval: {self.config.refresh_interval}s")
        logger.info(f"   Cache TTL: {self.config.cache_ttl}s")
    
    async def start(self) -> None:
        """Start the live instrument provider"""
        if self.running:
            return
        
        self.running = True
        
        # Initialize CCXT exchanges
        for exchange_id in self.config.exchanges:
            success = await self.ccxt_adapter.initialize_exchange(exchange_id)
            if success:
                logger.info(f"✅ Initialized exchange: {exchange_id}")
            else:
                logger.error(f"❌ Failed to initialize exchange: {exchange_id}")
        
        # Start background refresh task
        self.refresh_task = asyncio.create_task(self._background_refresh())
        
        # Initial refresh
        await self.refresh_all_instruments()
        
        logger.info("✅ LiveInstrumentProvider started")
    
    async def stop(self) -> None:
        """Stop the live instrument provider"""
        self.running = False
        
        if self.refresh_task:
            self.refresh_task.cancel()
            try:
                await self.refresh_task
            except asyncio.CancelledError:
                pass
        
        await self.ccxt_adapter.shutdown()
        
        logger.info("✅ LiveInstrumentProvider stopped")
    
    async def get_instrument(self, 
                           exchange: str,
                           symbol: str,
                           use_cache: bool = True) -> Optional[InstrumentDefinition]:
        """
        Get single instrument definition.
        
        Args:
            exchange: Exchange identifier (CCXT, VENUE, or Tardis)
            symbol: Symbol to look up
            use_cache: Whether to use cached data
            
        Returns:
            InstrumentDefinition or None if not found
        """
        try:
            # Map exchange identifier to CCXT format
            exchange_info = self.mapper.get_exchange_info(exchange)
            ccxt_exchange = exchange_info['ccxt_exchange']
            
            if not ccxt_exchange:
                logger.warning(f"Unknown exchange: {exchange}")
                return None
            
            # Check cache first
            if use_cache and self._is_cache_valid(ccxt_exchange):
                cached_instruments = self.cache.get(ccxt_exchange, {})
                
                # Try different symbol formats
                for cached_symbol, instrument_data in cached_instruments.items():
                    if (cached_symbol == symbol or 
                        instrument_data.get('exchange_raw_symbol') == symbol or
                        instrument_data.get('ccxt_symbol') == symbol):
                        
                        self.stats['cache_hits'] += 1
                        return InstrumentDefinition(**instrument_data)
                
                self.stats['cache_misses'] += 1
            
            # Fetch from CCXT
            instrument = await self.ccxt_adapter.get_instrument_definition(
                ccxt_exchange, symbol
            )
            
            if instrument:
                # Update cache
                if ccxt_exchange not in self.cache:
                    self.cache[ccxt_exchange] = {}
                
                self.cache[ccxt_exchange][symbol] = instrument.__dict__
                self.cache_timestamps[ccxt_exchange] = datetime.now(timezone.utc)
            
            return instrument
            
        except Exception as e:
            logger.error(f"❌ Error getting instrument {exchange}:{symbol}: {e}")
            self.stats['errors'] += 1
            return None
    
    async def get_instruments(self,
                            exchange: str = None,
                            filters: Dict[str, Any] = None,
                            use_cache: bool = True) -> List[InstrumentDefinition]:
        """
        Get multiple instrument definitions.
        
        Args:
            exchange: Specific exchange (optional, gets all if None)
            filters: Additional filters
            use_cache: Whether to use cached data
            
        Returns:
            List of InstrumentDefinition objects
        """
        try:
            if exchange:
                return await self._get_instruments_for_exchange(exchange, filters, use_cache)
            else:
                # Get instruments from all exchanges
                all_instruments = []
                for exchange_id in self.config.exchanges:
                    instruments = await self._get_instruments_for_exchange(
                        exchange_id, filters, use_cache
                    )
                    all_instruments.extend(instruments)
                return all_instruments
                
        except Exception as e:
            logger.error(f"❌ Error getting instruments: {e}")
            self.stats['errors'] += 1
            return []
    
    async def _get_instruments_for_exchange(self,
                                          exchange: str,
                                          filters: Dict[str, Any] = None,
                                          use_cache: bool = True) -> List[InstrumentDefinition]:
        """Get instruments for a specific exchange"""
        # Map exchange identifier
        exchange_info = self.mapper.get_exchange_info(exchange)
        ccxt_exchange = exchange_info['ccxt_exchange']
        
        if not ccxt_exchange:
            return []
        
        # Check cache
        if use_cache and self._is_cache_valid(ccxt_exchange):
            cached_instruments = self.cache.get(ccxt_exchange, {})
            instruments = [
                InstrumentDefinition(**data) 
                for data in cached_instruments.values()
            ]
            self.stats['cache_hits'] += len(instruments)
        else:
            # Fetch from CCXT
            combined_filters = self._combine_filters(filters)
            instruments = await self.ccxt_adapter.get_all_instruments(
                ccxt_exchange, combined_filters
            )
            
            # Update cache
            if instruments:
                self.cache[ccxt_exchange] = {
                    inst.ccxt_symbol: inst.__dict__ for inst in instruments
                }
                self.cache_timestamps[ccxt_exchange] = datetime.now(timezone.utc)
            
            self.stats['cache_misses'] += len(instruments)
        
        # Apply additional filters
        if filters:
            instruments = self._apply_filters(instruments, filters)
        
        return instruments
    
    def _combine_filters(self, additional_filters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Combine config filters with additional filters"""
        filters = {
            'base_currencies': self.config.base_currencies,
            'quote_currencies': self.config.quote_currencies,
            'types': self.config.instrument_types
        }
        
        if additional_filters:
            for key, value in additional_filters.items():
                if key in filters and value:
                    # Intersect lists if both exist
                    if isinstance(filters[key], list) and isinstance(value, list):
                        filters[key] = list(set(filters[key]) & set(value))
                    else:
                        filters[key] = value
                else:
                    filters[key] = value
        
        return filters
    
    def _apply_filters(self, 
                      instruments: List[InstrumentDefinition], 
                      filters: Dict[str, Any]) -> List[InstrumentDefinition]:
        """Apply additional filters to instruments"""
        filtered = instruments
        
        # Volume filter
        if filters.get('min_volume_24h') and filters['min_volume_24h'] > 0:
            # Note: This would require additional volume data from CCXT
            pass
        
        # Active filter
        if filters.get('active_only', True):
            filtered = [inst for inst in filtered if inst.active]
        
        # Custom filter function
        if 'filter_func' in filters:
            filter_func = filters['filter_func']
            filtered = [inst for inst in filtered if filter_func(inst)]
        
        return filtered
    
    def _is_cache_valid(self, exchange: str) -> bool:
        """Check if cache is still valid for exchange"""
        if exchange not in self.cache_timestamps:
            return False
        
        cache_age = datetime.now(timezone.utc) - self.cache_timestamps[exchange]
        return cache_age.total_seconds() < self.config.cache_ttl
    
    async def refresh_all_instruments(self) -> Dict[str, int]:
        """
        Refresh all instrument caches.
        
        Returns:
            Dictionary of exchange -> instrument count
        """
        logger.info("🔄 Refreshing all instruments...")
        
        results = {}
        
        for exchange_id in self.config.exchanges:
            try:
                # Clear cache to force refresh
                self.cache.pop(exchange_id, None)
                self.cache_timestamps.pop(exchange_id, None)
                
                # Fetch fresh data
                instruments = await self._get_instruments_for_exchange(
                    exchange_id, use_cache=False
                )
                
                results[exchange_id] = len(instruments)
                self.stats['instruments_by_exchange'][exchange_id] = len(instruments)
                
                # Track changes
                await self._track_changes(exchange_id, instruments)
                
                logger.info(f"✅ Refreshed {len(instruments)} instruments from {exchange_id}")
                
            except Exception as e:
                logger.error(f"❌ Error refreshing {exchange_id}: {e}")
                results[exchange_id] = 0
                self.stats['errors'] += 1
        
        self.stats['refresh_count'] += 1
        self.stats['total_instruments'] = sum(results.values())
        
        logger.info(f"✅ Refresh complete: {self.stats['total_instruments']} total instruments")
        
        return results
    
    async def _track_changes(self, exchange: str, instruments: List[InstrumentDefinition]):
        """Track changes in instruments and notify callbacks"""
        if not self.config.enable_monitoring:
            return
        
        current_symbols = {inst.ccxt_symbol for inst in instruments}
        previous_symbols = self.previous_instruments.get(exchange, set())
        
        added = current_symbols - previous_symbols
        removed = previous_symbols - current_symbols
        
        if added or removed:
            change_info = {
                'exchange': exchange,
                'timestamp': datetime.now(timezone.utc),
                'added': list(added),
                'removed': list(removed),
                'total_instruments': len(current_symbols)
            }
            
            logger.info(f"📊 Instrument changes for {exchange}:")
            if added:
                logger.info(f"   Added: {list(added)[:5]}{'...' if len(added) > 5 else ''}")
            if removed:
                logger.info(f"   Removed: {list(removed)[:5]}{'...' if len(removed) > 5 else ''}")
            
            # Notify callbacks
            for callback in self.change_callbacks:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(change_info)
                    else:
                        callback(change_info)
                except Exception as e:
                    logger.error(f"❌ Error in change callback: {e}")
        
        self.previous_instruments[exchange] = current_symbols
    
    def add_change_callback(self, callback):
        """Add callback for instrument changes"""
        self.change_callbacks.append(callback)
        logger.info("✅ Added instrument change callback")
    
    async def _background_refresh(self):
        """Background task for periodic refresh"""
        while self.running:
            try:
                await asyncio.sleep(self.config.refresh_interval)
                if self.running:
                    await self.refresh_all_instruments()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ Error in background refresh: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get provider statistics"""
        runtime = datetime.now(timezone.utc) - self.stats['start_time']
        
        return {
            'total_instruments': self.stats['total_instruments'],
            'instruments_by_exchange': self.stats['instruments_by_exchange'].copy(),
            'cache_hits': self.stats['cache_hits'],
            'cache_misses': self.stats['cache_misses'],
            'cache_hit_rate': self.stats['cache_hits'] / max(
                self.stats['cache_hits'] + self.stats['cache_misses'], 1
            ),
            'refresh_count': self.stats['refresh_count'],
            'errors': self.stats['errors'],
            'runtime_seconds': runtime.total_seconds(),
            'exchanges_configured': len(self.config.exchanges),
            'cache_status': {
                exchange: self._is_cache_valid(exchange)
                for exchange in self.config.exchanges
            }
        }
    
    def clear_cache(self, exchange: str = None) -> None:
        """Clear cache for specific exchange or all exchanges"""
        if exchange:
            self.cache.pop(exchange, None)
            self.cache_timestamps.pop(exchange, None)
            logger.info(f"🗑️ Cleared cache for {exchange}")
        else:
            self.cache.clear()
            self.cache_timestamps.clear()
            logger.info("🗑️ Cleared all caches")


# Example usage
if __name__ == "__main__":
    import asyncio
    
    async def test_live_instrument_provider():
        config = LiveInstrumentConfig(
            exchanges=["binance"],
            refresh_interval=60,  # 1 minute for testing
            base_currencies=["BTC", "ETH"],
            quote_currencies=["USDT"]
        )
        
        provider = LiveInstrumentProvider(config)
        
        # Add change callback
        def on_change(change_info):
            print(f"Instrument change: {change_info}")
        
        provider.add_change_callback(on_change)
        
        try:
            await provider.start()
            
            # Get specific instrument
            btc_usdt = await provider.get_instrument("binance", "BTC/USDT")
            if btc_usdt:
                print(f"BTC-USDT: {btc_usdt.instrument_key}")
                print(f"  Active: {btc_usdt.active}")
                print(f"  Tick size: {btc_usdt.tick_size}")
            
            # Get all USDT pairs
            usdt_pairs = await provider.get_instruments(
                "binance", 
                filters={"quote_currencies": ["USDT"]}
            )
            print(f"Found {len(usdt_pairs)} USDT pairs")
            
            # Show stats
            stats = provider.get_stats()
            print(f"Stats: {stats}")
            
            # Wait a bit to see background refresh
            await asyncio.sleep(5)
            
        finally:
            await provider.stop()
    
    # Uncomment to test (requires CCXT and exchange connectivity)
    # asyncio.run(test_live_instrument_provider())
