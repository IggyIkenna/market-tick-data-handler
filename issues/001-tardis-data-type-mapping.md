# Issue #001: Tardis Data Type Mapping for Replay

## Problem Statement

The current `tardisDataTypes` mapping in the live streaming service is limited and doesn't directly match historical Tardis data types, causing inconsistencies between live streaming and historical data replay functionality.

## Current State Analysis

**File**: `streaming/live_tick_streamer.js` (lines 143-149)

```javascript
const tardisDataTypes = {
    'trades': 'trade',
    'book_snapshots': 'book_snapshot_5_100ms', // 5 levels, 100ms intervals
    'liquidations': 'trade', // Use trades as base, transform to liquidations
    'derivative_ticker': 'derivative_ticker',
    'options_chain': 'trade' // Use trades as base, transform to options
};
```

### Issues Identified

1. **Limited Data Type Coverage**: Only 5 data types are mapped, missing many historical data types
2. **Inconsistent Mapping**: Some types use 'trade' as base with transformation, others use direct mapping
3. **No Historical Alignment**: Mapping doesn't align with what Tardis actually provides in historical data
4. **Replay Inconsistency**: Live streaming data structure differs from historical data structure
5. **Transformation Logic**: Current transformation is basic and may not preserve all necessary fields

### Impact

- **Replay Functionality**: Historical data replay may not work correctly with live streaming data
- **Data Consistency**: Inconsistencies between live and historical data processing pipelines
- **Feature Completeness**: Missing data types limit the capabilities of the streaming service
- **Debugging Difficulty**: Different data structures make troubleshooting complex

## Proposed Solutions

### Solution 1: Comprehensive Data Type Mapping
Create a complete mapping that covers all Tardis historical data types:

```javascript
const tardisDataTypes = {
    // Trade data
    'trades': 'trade',
    'liquidations': 'liquidation',
    
    // Order book data
    'book_snapshots': 'book_snapshot_5_100ms',
    'book_snapshots_1': 'book_snapshot_1_100ms',
    'book_snapshots_10': 'book_snapshot_10_100ms',
    'book_snapshots_25': 'book_snapshot_25_100ms',
    
    // Market data
    'derivative_ticker': 'derivative_ticker',
    'spot_ticker': 'ticker',
    
    // Options data
    'options_chain': 'options_chain',
    'options_trades': 'options_trade',
    
    // Funding data
    'funding_rates': 'funding_rate',
    
    // Other data types
    'candles': 'candle',
    'gaps': 'gap'
};
```

### Solution 2: Data Type Converter Service
Create a dedicated service to handle data type conversion:

```javascript
class TardisDataConverter {
    convertToHistoricalFormat(liveData, dataType) {
        // Convert live streaming data to match historical format
    }
    
    convertToLiveFormat(historicalData, dataType) {
        // Convert historical data to match live streaming format
    }
    
    validateDataConsistency(liveData, historicalData) {
        // Validate that data structures are consistent
    }
}
```

### Solution 3: Configuration-Driven Mapping
Use configuration files to define data type mappings:

```yaml
# config/tardis_data_types.yaml
data_types:
  trades:
    live: 'trade'
    historical: 'trade'
    transformation: 'none'
  liquidations:
    live: 'trade'
    historical: 'liquidation'
    transformation: 'liquidation_transform'
  book_snapshots:
    live: 'book_snapshot_5_100ms'
    historical: 'book_snapshot_5_100ms'
    transformation: 'none'
```

## Implementation Tasks

### Phase 1: Analysis and Mapping
- [ ] Audit all Tardis historical data types available
- [ ] Compare live streaming data structure with historical data structure
- [ ] Document differences and transformation requirements
- [ ] Create comprehensive data type mapping

### Phase 2: Converter Implementation
- [ ] Implement `TardisDataConverter` service
- [ ] Add transformation logic for each data type
- [ ] Implement validation methods
- [ ] Add unit tests for conversion logic

### Phase 3: Integration
- [ ] Update live streaming service to use converter
- [ ] Update historical replay service to use converter
- [ ] Add configuration management for data type mappings
- [ ] Implement fallback strategies for missing data types

### Phase 4: Testing and Validation
- [ ] Test data consistency between live and historical
- [ ] Validate replay functionality with converted data
- [ ] Performance testing for conversion overhead
- [ ] End-to-end testing with real data

## Dependencies and Risks

### Dependencies
- Tardis.dev API documentation for complete data type list
- Historical data samples for structure comparison
- Configuration management system
- Testing framework for data validation

### Risks
- **Performance Impact**: Data conversion may add latency
- **Data Loss**: Transformation might lose some information
- **API Changes**: Tardis API changes could break mappings
- **Complexity**: More complex codebase with conversion logic

### Mitigation Strategies
- Implement caching for frequently used conversions
- Add comprehensive logging for transformation issues
- Create automated tests for data consistency
- Version control for data type mappings

## Success Criteria

1. **Data Consistency**: Live and historical data have identical structure
2. **Complete Coverage**: All Tardis data types are supported
3. **Replay Functionality**: Historical replay works with live streaming data
4. **Performance**: Conversion overhead < 10ms per message
5. **Maintainability**: Easy to add new data types and transformations

## Priority

**High** - This affects core functionality and data consistency across the entire system.

## Estimated Effort

- **Analysis**: 2-3 days
- **Implementation**: 1-2 weeks
- **Testing**: 1 week
- **Total**: 3-4 weeks
