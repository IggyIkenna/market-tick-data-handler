# Issue #003: Missing Data Types in Live Stream

## Problem Statement

The live streaming service is not receiving all expected data types from the Tardis real-time API, limiting the system's ability to provide comprehensive market data coverage and potentially causing gaps in data availability.

## Current State Analysis

**File**: `streaming/live_tick_streamer.js`

### Current Data Type Support

The live streaming service currently supports these data types:

```javascript
const tardisDataTypes = {
    'trades': 'trade',
    'book_snapshots': 'book_snapshot_5_100ms',
    'liquidations': 'trade', // Transformed from trades
    'derivative_ticker': 'derivative_ticker',
    'options_chain': 'trade' // Transformed from trades
};
```

### Issues Identified

1. **Limited Real-Time Data Types**: Only 5 data types are supported in live streaming
2. **API Mismatch**: Tardis real-time API may not support all historical data types
3. **Transformation Dependencies**: Some data types rely on trade data transformation
4. **Missing Critical Data**: Important data types like funding rates, gaps, candles may be missing
5. **No Fallback Strategy**: No mechanism to handle unavailable data types

### Data Type Availability Analysis

Based on Tardis documentation and historical data processing, the following data types should be available:

#### Currently Supported
- ✅ `trades` - Trade data
- ✅ `book_snapshots` - Order book snapshots
- ✅ `liquidations` - Liquidation data (transformed)
- ✅ `derivative_ticker` - Derivative ticker data
- ✅ `options_chain` - Options chain data (transformed)

#### Potentially Missing
- ❓ `funding_rates` - Funding rate data
- ❓ `gaps` - Data gap information
- ❓ `candles` - OHLCV candle data
- ❓ `spot_ticker` - Spot market ticker data
- ❓ `options_trades` - Options trade data
- ❓ `book_snapshots_1` - 1-level order book
- ❓ `book_snapshots_10` - 10-level order book
- ❓ `book_snapshots_25` - 25-level order book

## Proposed Solutions

### Solution 1: Comprehensive Data Type Audit

Create a systematic audit of available data types:

```javascript
class TardisDataTypeAuditor {
    async auditAvailableDataTypes(exchange, symbol) {
        const availableTypes = [];
        
        // Test each data type
        const dataTypes = [
            'trades', 'book_snapshots', 'liquidations', 'derivative_ticker',
            'options_chain', 'funding_rates', 'gaps', 'candles',
            'spot_ticker', 'options_trades', 'book_snapshots_1',
            'book_snapshots_10', 'book_snapshots_25'
        ];
        
        for (const dataType of dataTypes) {
            try {
                const isAvailable = await this.testDataTypeAvailability(exchange, symbol, dataType);
                if (isAvailable) {
                    availableTypes.push(dataType);
                }
            } catch (error) {
                console.warn(`Data type ${dataType} not available: ${error.message}`);
            }
        }
        
        return availableTypes;
    }
    
    async testDataTypeAvailability(exchange, symbol, dataType) {
        // Test if data type is available in real-time stream
        // Return true if available, false otherwise
    }
}
```

### Solution 2: Dynamic Data Type Discovery

Implement automatic discovery of available data types:

```javascript
class DynamicDataTypeManager {
    constructor() {
        this.availableTypes = new Map();
        this.fallbackStrategies = new Map();
    }
    
    async discoverAvailableTypes(exchange, symbol) {
        // Discover available data types at runtime
        const discovered = await this.scanTardisAPI(exchange, symbol);
        this.availableTypes.set(`${exchange}:${symbol}`, discovered);
        return discovered;
    }
    
    getSupportedDataTypes(exchange, symbol) {
        return this.availableTypes.get(`${exchange}:${symbol}`) || [];
    }
    
    hasFallbackStrategy(dataType) {
        return this.fallbackStrategies.has(dataType);
    }
    
    getFallbackStrategy(dataType) {
        return this.fallbackStrategies.get(dataType);
    }
}
```

### Solution 3: Fallback Strategy Implementation

Implement fallback strategies for missing data types:

```javascript
class DataTypeFallbackManager {
    constructor() {
        this.fallbackStrategies = {
            'funding_rates': this.fallbackFromDerivativeTicker,
            'gaps': this.fallbackFromTradeAnalysis,
            'candles': this.fallbackFromTradeAggregation,
            'spot_ticker': this.fallbackFromTrades,
            'options_trades': this.fallbackFromTrades,
            'book_snapshots_1': this.fallbackFromBookSnapshots,
            'book_snapshots_10': this.fallbackFromBookSnapshots,
            'book_snapshots_25': this.fallbackFromBookSnapshots
        };
    }
    
    async fallbackFromDerivativeTicker(tickerData) {
        // Extract funding rate from derivative ticker
        return {
            type: 'funding_rate',
            symbol: tickerData.symbol,
            rate: tickerData.fundingRate,
            timestamp: tickerData.timestamp
        };
    }
    
    async fallbackFromTradeAnalysis(trades) {
        // Analyze trades to detect gaps
        return this.detectGaps(trades);
    }
    
    async fallbackFromTradeAggregation(trades, interval) {
        // Aggregate trades into candles
        return this.aggregateToCandles(trades, interval);
    }
}
```

### Solution 4: Configuration-Driven Data Types

Use configuration to manage data type availability:

```yaml
# config/live_streaming_data_types.yaml
data_types:
  trades:
    enabled: true
    source: 'tardis_realtime'
    fallback: null
  
  book_snapshots:
    enabled: true
    source: 'tardis_realtime'
    fallback: 'trade_derived'
  
  liquidations:
    enabled: true
    source: 'trade_transformation'
    fallback: null
  
  funding_rates:
    enabled: true
    source: 'tardis_realtime'
    fallback: 'derivative_ticker'
  
  gaps:
    enabled: true
    source: 'trade_analysis'
    fallback: null
  
  candles:
    enabled: true
    source: 'trade_aggregation'
    fallback: null
```

## Implementation Tasks

### Phase 1: Data Type Audit
- [ ] Implement `TardisDataTypeAuditor` class
- [ ] Create test suite for data type availability
- [ ] Document available data types for each exchange
- [ ] Identify missing data types and their impact

### Phase 2: Dynamic Discovery
- [ ] Implement `DynamicDataTypeManager` class
- [ ] Add runtime data type discovery
- [ ] Implement caching for discovered data types
- [ ] Add monitoring for data type availability changes

### Phase 3: Fallback Strategies
- [ ] Implement `DataTypeFallbackManager` class
- [ ] Create fallback strategies for each missing data type
- [ ] Add validation for fallback data quality
- [ ] Implement fallback performance monitoring

### Phase 4: Configuration and Integration
- [ ] Create configuration system for data type management
- [ ] Integrate with live streaming service
- [ ] Add logging and monitoring for data type issues
- [ ] Implement graceful degradation for missing data types

## Dependencies and Risks

### Dependencies
- Tardis.dev API documentation
- Exchange-specific data type availability
- Historical data for fallback strategies
- Configuration management system

### Risks
- **API Limitations**: Tardis real-time API may not support all data types
- **Performance Impact**: Fallback strategies may add latency
- **Data Quality**: Fallback data may be less accurate than native data
- **Complexity**: More complex codebase with multiple data sources

### Mitigation Strategies
- Implement data quality metrics for fallback strategies
- Add performance monitoring for data type processing
- Create alerts for data type availability issues
- Implement graceful degradation for missing data types

## Success Criteria

1. **Data Type Coverage**: All critical data types are available (native or fallback)
2. **Performance**: Data type processing adds < 10ms latency
3. **Reliability**: Fallback strategies work consistently
4. **Monitoring**: Clear visibility into data type availability and quality
5. **Maintainability**: Easy to add new data types and fallback strategies

## Priority

**High** - This affects the completeness and reliability of the live streaming service.

## Estimated Effort

- **Data Type Audit**: 1 week
- **Dynamic Discovery**: 1-2 weeks
- **Fallback Strategies**: 2-3 weeks
- **Integration and Testing**: 1-2 weeks
- **Total**: 5-8 weeks

## ✅ IMPLEMENTATION STATUS: COMPLETED

This issue has been **SOLVED** as part of the unified streaming architecture consolidation (Issue #009).

### Implementation Details

**File**: `market_data_tick_handler/streaming_service/tick_processor/data_type_router.py`

The `DataTypeRouter` class now provides:

1. **Complete Data Type Support**:
   - ✅ Native: trades, book_snapshots, derivative_ticker, options_chain
   - ✅ Fallback: liquidations (from trades), funding_rates (from derivative_ticker)

2. **Fallback Strategies**:
   ```python
   fallback_strategies = {
       'liquidations': ['trades'],  # Derive from large trades
       'funding_rates': ['derivative_ticker'],  # Extract from ticker
       'gaps': ['trades'],  # Detect from trade analysis
       'candles': ['trades']  # Aggregate from trades
   }
   ```

3. **Configuration-Driven**:
   ```yaml
   # streaming.yaml
   data_types:
     trades:
       enabled: true
       source: tardis_realtime
       batch_timeout: 60000
     liquidations:
       enabled: true
       source: trade_transformation
       fallback: true
   ```

4. **Processor Classes**:
   - `TradeProcessor`
   - `BookSnapshotProcessor` 
   - `LiquidationProcessor` (with fallback from trades)
   - `DerivativeTickerProcessor`
   - `OptionsChainProcessor`
   - `FundingRateProcessor` (with fallback from derivative_ticker)

### Usage Example
```python
from market_data_tick_handler.streaming_service import DataTypeRouter

router = DataTypeRouter()
processor = await router.get_processor('liquidations')  # Gets fallback if needed
result = await processor.process(tick_data)
```

### Benefits Achieved
- **Complete coverage** of all critical data types
- **Automatic fallback** when native data unavailable
- **Configuration control** over enabled types and strategies
- **Extensible architecture** for adding new data types

**Reference**: See [Streaming Architecture Documentation](../docs/STREAMING_ARCHITECTURE.md) for complete implementation details.
