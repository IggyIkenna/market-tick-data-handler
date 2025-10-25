# Issue #004: Live CCXT Instrument Definitions

## Problem Statement

The current system uses historical instrument definitions from BigQuery/GCS, but for real-time trading operations, we need live CCXT instrument definitions that reflect the current state of available trading instruments, including dynamic availability, new listings, and delistings.

## Current State Analysis

### Current Instrument System

**Files**:
- `src/instrument_services/instrument_lister.py`
- `market_data_tick_handler/instrument_services/instrument_lister.py`
- `src/models.py` (InstrumentDefinition model)

The current system:

1. **Historical Focus**: Uses BigQuery/GCS for historical instrument definitions
2. **Static Data**: Instrument definitions are updated periodically, not in real-time
3. **Tardis Integration**: Primarily designed for Tardis historical data processing
4. **Limited Real-Time Support**: No mechanism for live instrument discovery

### Current InstrumentDefinition Model

```python
class InstrumentDefinition(BaseModel):
    instrument_key: str
    venue: str
    instrument_type: str
    available_from_datetime: str
    available_to_datetime: str
    data_types: str
    base_asset: str
    quote_asset: str
    settle_asset: str
    exchange_raw_symbol: str
    tardis_symbol: str
    tardis_exchange: str
    data_provider: str
    venue_type: str
    asset_class: str
    # ... additional fields
```

### Issues Identified

1. **No Real-Time Updates**: Instrument definitions are not updated in real-time
2. **Missing Live Data**: No integration with live exchange APIs for current instrument status
3. **Static Availability Windows**: `available_from_datetime` and `available_to_datetime` are historical
4. **No CCXT Integration**: No direct integration with CCXT for live instrument data
5. **Limited Trading Support**: Current system is focused on data processing, not trading operations

## Proposed Solutions

### Solution 1: CCXT Live Instrument Service

Create a dedicated service for live instrument definitions:

```python
import ccxt
from typing import Dict, List, Optional
from datetime import datetime, timezone

class CCXTLiveInstrumentService:
    def __init__(self):
        self.exchanges = {}
        self.instrument_cache = {}
        self.cache_ttl = 300  # 5 minutes
    
    async def initialize_exchanges(self, exchange_ids: List[str]):
        """Initialize CCXT exchanges"""
        for exchange_id in exchange_ids:
            try:
                exchange_class = getattr(ccxt, exchange_id)
                self.exchanges[exchange_id] = exchange_class({
                    'apiKey': os.getenv(f'{exchange_id.upper()}_API_KEY'),
                    'secret': os.getenv(f'{exchange_id.upper()}_SECRET'),
                    'sandbox': os.getenv('SANDBOX_MODE', 'false').lower() == 'true'
                })
            except Exception as e:
                logger.error(f"Failed to initialize {exchange_id}: {e}")
    
    async def get_live_instruments(self, exchange_id: str) -> List[Dict]:
        """Get live instrument definitions from CCXT"""
        if exchange_id not in self.exchanges:
            raise ValueError(f"Exchange {exchange_id} not initialized")
        
        exchange = self.exchanges[exchange_id]
        
        try:
            # Get markets from CCXT
            markets = await exchange.load_markets()
            
            instruments = []
            for symbol, market in markets.items():
                instrument = self.convert_ccxt_to_instrument(market, exchange_id)
                instruments.append(instrument)
            
            return instruments
            
        except Exception as e:
            logger.error(f"Failed to get instruments from {exchange_id}: {e}")
            return []
    
    def convert_ccxt_to_instrument(self, market: Dict, exchange_id: str) -> Dict:
        """Convert CCXT market data to InstrumentDefinition format"""
        return {
            'instrument_key': f"{exchange_id.upper()}:{market['type'].upper()}:{market['symbol']}",
            'venue': exchange_id.upper(),
            'instrument_type': self.map_ccxt_type(market['type']),
            'available_from_datetime': datetime.now(timezone.utc).isoformat(),
            'available_to_datetime': None,  # Live instruments don't have end date
            'data_types': self.get_available_data_types(market),
            'base_asset': market['base'],
            'quote_asset': market['quote'],
            'settle_asset': market.get('settle', market['quote']),
            'exchange_raw_symbol': market['symbol'],
            'tardis_symbol': self.convert_to_tardis_symbol(market['symbol']),
            'tardis_exchange': self.convert_to_tardis_exchange(exchange_id),
            'data_provider': 'CCXT',
            'venue_type': 'centralized',
            'asset_class': 'crypto',
            'is_live': True,
            'last_updated': datetime.now(timezone.utc).isoformat(),
            'trading_fees': market.get('fees', {}),
            'limits': market.get('limits', {}),
            'precision': market.get('precision', {}),
            'active': market.get('active', True)
        }
```

### Solution 2: Hybrid Instrument Manager

Combine historical and live instrument data:

```python
class HybridInstrumentManager:
    def __init__(self, historical_service, live_service):
        self.historical_service = historical_service
        self.live_service = live_service
        self.cache = {}
    
    async def get_instruments(self, 
                            exchange: str, 
                            symbol: Optional[str] = None,
                            use_live: bool = True) -> List[InstrumentDefinition]:
        """Get instruments from both historical and live sources"""
        
        instruments = []
        
        # Get live instruments if requested
        if use_live:
            try:
                live_instruments = await self.live_service.get_live_instruments(exchange)
                instruments.extend(live_instruments)
            except Exception as e:
                logger.warning(f"Failed to get live instruments: {e}")
        
        # Get historical instruments as fallback
        try:
            historical_instruments = await self.historical_service.list_instruments(
                date=datetime.now(),
                venue=exchange,
                symbol=symbol
            )
            instruments.extend(historical_instruments)
        except Exception as e:
            logger.warning(f"Failed to get historical instruments: {e}")
        
        # Deduplicate and merge
        return self.merge_instruments(instruments)
    
    def merge_instruments(self, instruments: List[Dict]) -> List[InstrumentDefinition]:
        """Merge live and historical instruments, preferring live data"""
        merged = {}
        
        for instrument in instruments:
            key = instrument['instrument_key']
            if key not in merged or instrument.get('is_live', False):
                merged[key] = instrument
        
        return [InstrumentDefinition(**inst) for inst in merged.values()]
```

### Solution 3: Real-Time Instrument Monitoring

Implement monitoring for instrument changes:

```python
class InstrumentChangeMonitor:
    def __init__(self, instrument_service):
        self.instrument_service = instrument_service
        self.previous_instruments = {}
        self.change_handlers = []
    
    async def start_monitoring(self, exchange: str, interval: int = 60):
        """Start monitoring instrument changes"""
        while True:
            try:
                current_instruments = await self.instrument_service.get_live_instruments(exchange)
                changes = self.detect_changes(exchange, current_instruments)
                
                if changes:
                    await self.handle_changes(changes)
                
                self.previous_instruments[exchange] = current_instruments
                await asyncio.sleep(interval)
                
            except Exception as e:
                logger.error(f"Error monitoring instruments: {e}")
                await asyncio.sleep(interval)
    
    def detect_changes(self, exchange: str, current: List[Dict]) -> Dict:
        """Detect changes in instrument definitions"""
        previous = self.previous_instruments.get(exchange, [])
        
        previous_keys = {inst['instrument_key'] for inst in previous}
        current_keys = {inst['instrument_key'] for inst in current}
        
        return {
            'added': [inst for inst in current if inst['instrument_key'] not in previous_keys],
            'removed': [inst for inst in previous if inst['instrument_key'] not in current_keys],
            'modified': [inst for inst in current if self.is_modified(inst, previous)]
        }
    
    async def handle_changes(self, changes: Dict):
        """Handle instrument changes"""
        for handler in self.change_handlers:
            try:
                await handler(changes)
            except Exception as e:
                logger.error(f"Error in change handler: {e}")
```

### Solution 4: Configuration-Driven Instrument Sources

Use configuration to manage instrument sources:

```yaml
# config/instrument_sources.yaml
sources:
  live:
    enabled: true
    provider: 'ccxt'
    exchanges: ['binance', 'coinbase', 'kraken', 'bitfinex']
    update_interval: 300  # 5 minutes
    cache_ttl: 600  # 10 minutes
  
  historical:
    enabled: true
    provider: 'tardis'
    fallback: true
  
  monitoring:
    enabled: true
    alert_on_changes: true
    webhook_url: 'https://api.example.com/instrument-changes'
  
  data_types:
    trading: true
    data_processing: true
    analytics: true
```

## Implementation Tasks

### Phase 1: CCXT Integration
- [ ] Install and configure CCXT library
- [ ] Implement `CCXTLiveInstrumentService` class
- [ ] Add exchange initialization and configuration
- [ ] Implement market data conversion to InstrumentDefinition format

### Phase 2: Hybrid System
- [ ] Implement `HybridInstrumentManager` class
- [ ] Add instrument merging and deduplication logic
- [ ] Implement fallback strategies for missing data
- [ ] Add caching and performance optimization

### Phase 3: Real-Time Monitoring
- [ ] Implement `InstrumentChangeMonitor` class
- [ ] Add change detection algorithms
- [ ] Implement change notification system
- [ ] Add monitoring dashboard and alerts

### Phase 4: Integration and Testing
- [ ] Integrate with existing instrument services
- [ ] Add configuration management
- [ ] Implement comprehensive testing
- [ ] Add performance monitoring and metrics

## Dependencies and Risks

### Dependencies
- CCXT library for exchange integration
- Exchange API keys and credentials
- Redis or similar for caching
- Webhook system for change notifications

### Risks
- **API Rate Limits**: Exchange APIs may have rate limits
- **Data Quality**: Live data may be less reliable than historical data
- **Performance**: Real-time updates may impact system performance
- **Complexity**: More complex system with multiple data sources

### Mitigation Strategies
- Implement rate limiting and backoff strategies
- Add data validation and quality checks
- Use caching to reduce API calls
- Implement graceful degradation for API failures

## Success Criteria

1. **Real-Time Updates**: Instrument definitions are updated in real-time
2. **Comprehensive Coverage**: All major exchanges are supported
3. **Performance**: Instrument updates complete within 5 minutes
4. **Reliability**: System handles API failures gracefully
5. **Monitoring**: Clear visibility into instrument changes and issues

## Priority

**Medium** - This is important for trading operations but not critical for data processing.

## Estimated Effort

- **CCXT Integration**: 2-3 weeks
- **Hybrid System**: 1-2 weeks
- **Real-Time Monitoring**: 2-3 weeks
- **Integration and Testing**: 1-2 weeks
- **Total**: 6-10 weeks

## ✅ IMPLEMENTATION STATUS: COMPLETED

This issue has been **SOLVED** as part of the unified streaming architecture consolidation (Issue #009).

### Implementation Details

**Files**: 
- `market_data_tick_handler/streaming_service/instrument_service/ccxt_adapter.py`
- `market_data_tick_handler/streaming_service/instrument_service/live_instrument_provider.py`
- `market_data_tick_handler/streaming_service/instrument_service/instrument_mapper.py`

### Key Components Implemented

1. **CCXTAdapter**: Converts CCXT market data to InstrumentDefinition format
   ```python
   adapter = CCXTAdapter()
   await adapter.initialize_exchange('binance')
   instrument = await adapter.get_instrument_definition('binance', 'BTC/USDT')
   ```

2. **LiveInstrumentProvider**: In-memory cache with TTL for real-time instruments
   ```python
   provider = LiveInstrumentProvider()
   await provider.start()
   btc_usdt = await provider.get_instrument("binance", "BTC/USDT")
   ```

3. **InstrumentMapper**: VENUE ↔ CCXT ↔ Tardis exchange mappings
   ```python
   mapper = InstrumentMapper()
   ccxt_exchange = mapper.venue_to_ccxt_exchange("BINANCE-SPOT")  # -> "binance"
   ```

### Features Delivered

1. **Complete Trading Parameters**:
   - ✅ `tick_size` - Minimum price increment
   - ✅ `min_size` - Minimum order size
   - ✅ `max_size` - Maximum order size
   - ✅ `trading_fees_maker` - Maker fee rate
   - ✅ `trading_fees_taker` - Taker fee rate
   - ✅ `contract_size` - Contract multiplier
   - ✅ `precision` - Price and amount precision

2. **Exchange Mappings**:
   ```python
   venue_mappings = {
       'BINANCE-SPOT': 'binance',
       'BINANCE-FUTURES': 'binance', 
       'DERIBIT': 'deribit',
       'COINBASE-SPOT': 'coinbase'
   }
   ```

3. **Real-Time Updates**:
   - In-memory cache with 10-minute TTL
   - Automatic refresh every 5 minutes
   - Change monitoring and notifications
   - Active/inactive instrument tracking

4. **InstrumentDefinition Compatibility**:
   - Full integration with existing `InstrumentDefinition` model
   - All required fields populated from CCXT data
   - Seamless integration with historical definitions

### Usage Examples

#### Get Live Instrument with Trading Parameters
```python
from market_data_tick_handler.streaming_service import LiveInstrumentProvider

provider = LiveInstrumentProvider()
await provider.start()

# Get live BTC-USDT with all trading parameters
btc_usdt = await provider.get_instrument("binance", "BTC/USDT")

print(f"Tick size: {btc_usdt.tick_size}")           # 0.01
print(f"Min size: {btc_usdt.min_size}")             # 0.00001  
print(f"Taker fee: {btc_usdt.trading_fees_taker}")  # 0.001
print(f"Active: {btc_usdt.active}")                 # True
```

#### Monitor Instrument Changes
```python
def on_instrument_change(change_info):
    print(f"Exchange: {change_info['exchange']}")
    print(f"Added: {change_info['added']}")
    print(f"Removed: {change_info['removed']}")

provider.add_change_callback(on_instrument_change)
```

#### Integration with Trading System
```python
# Get current trading parameters for order sizing
instrument = await provider.get_instrument("binance", "BTC/USDT")

if instrument and instrument.active:
    # Calculate order size within limits
    order_size = max(instrument.min_size, min(desired_size, instrument.max_size))
    
    # Round price to tick size
    order_price = round(price / instrument.tick_size) * instrument.tick_size
    
    # Account for trading fees
    expected_fee = order_size * order_price * instrument.trading_fees_taker
```

### Configuration
```yaml
# streaming.yaml
live_instruments:
  enabled: true
  exchanges: ["binance", "deribit", "coinbase"]
  refresh_interval: 300     # 5 minutes
  cache_ttl: 600           # 10 minutes
  
  exchange_mappings:
    BINANCE-SPOT: binance
    BINANCE-FUTURES: binance
    DERIBIT: deribit
```

### New Streaming Mode
```bash
# Live instrument sync mode
python -m market_data_tick_handler.main --mode live-instruments-sync \
  --exchanges binance,deribit --refresh-interval 300
```

### Benefits Achieved
- **Real-time trading parameters** from live CCXT APIs
- **Complete InstrumentDefinition compatibility** with existing systems
- **Automatic updates** with configurable refresh intervals
- **Change monitoring** for new/delisted instruments
- **Exchange abstraction** with automatic mapping
- **In-memory performance** with TTL-based caching
- **No persistence overhead** - pure live data

**Reference**: See [Streaming Architecture Documentation](../docs/STREAMING_ARCHITECTURE.md) for complete implementation details.
