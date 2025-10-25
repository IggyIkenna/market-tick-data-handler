# Issue #005: Candle Buffering Strategy (15s to 24h)

## Problem Statement

The system needs to implement a comprehensive candle buffering strategy that can handle multiple timeframes (15s to 24h) with fallback mechanisms for data disconnections and missing candles, ensuring continuous availability of candle data for analytics and trading operations.

## Current State Analysis

### Current Candle Processing

**Files**:
- `streaming/live_tick_streamer.js` (candle processing logic)
- `src/streaming_service/hft_features/feature_calculator.py`
- `market_data_tick_handler/streaming_service/hft_features/feature_calculator.py`

The current system:

1. **Limited Timeframes**: Only supports basic candle intervals (15s, 1m, 5m, 15m, 4h, 24h)
2. **No Buffering**: No persistent buffering of candles across timeframes
3. **No Fallback**: No fallback strategy for missing candles or disconnections
4. **Single Source**: Relies only on live streaming data
5. **No Analytics Integration**: No integration with analytics service for historical data

### Current Candle Structure

```javascript
// Current candle structure in live_tick_streamer.js
this.currentCandle = {
    symbol: this.symbol,
    timestamp: candleTime,
    timestamp_in: candleTime,
    timestamp_out: null,
    open: price,
    high: price,
    low: price,
    close: price,
    volume: amount,
    tradeCount: 1,
    lastUpdate: new Date(),
    isFirstCandle: this.completedCandles.length === 0
};
```

### Issues Identified

1. **No Multi-Timeframe Buffering**: Candles are not buffered across multiple timeframes
2. **No Persistence**: Candles are lost on disconnection or restart
3. **No Fallback Strategy**: No mechanism to recover missing candles
4. **Limited Timeframe Support**: Missing intermediate timeframes (30s, 2m, 3m, 6m, 12m, 30m, 1h, 2h, 6h, 12h)
5. **No Analytics Integration**: No integration with analytics service for historical data
6. **No Replay Capability**: Cannot replay intraday data from Tardis or exchange

## Proposed Solutions

### Solution 1: Multi-Timeframe Candle Buffer

Implement a comprehensive candle buffering system:

```javascript
class MultiTimeframeCandleBuffer {
    constructor(symbol, exchange) {
        this.symbol = symbol;
        this.exchange = exchange;
        this.timeframes = [
            '15s', '30s', '1m', '2m', '3m', '5m', '6m', '10m', '12m', '15m', 
            '20m', '30m', '1h', '2h', '3h', '4h', '6h', '8h', '12h', '24h'
        ];
        this.buffers = {};
        this.initializeBuffers();
    }
    
    initializeBuffers() {
        for (const timeframe of this.timeframes) {
            this.buffers[timeframe] = {
                current: null,
                completed: [],
                maxBufferSize: this.getBufferSize(timeframe)
            };
        }
    }
    
    getBufferSize(timeframe) {
        // Larger timeframes need more buffer
        const sizes = {
            '15s': 1000,   // ~4 hours
            '30s': 500,    // ~4 hours
            '1m': 240,     // 4 hours
            '5m': 288,     // 24 hours
            '15m': 96,     // 24 hours
            '1h': 168,     // 1 week
            '4h': 42,      // 1 week
            '24h': 30      // 1 month
        };
        return sizes[timeframe] || 100;
    }
    
    async processTick(tick) {
        const price = parseFloat(tick.price);
        const amount = parseFloat(tick.amount);
        const timestamp = new Date(tick.timestamp);
        
        // Process tick for all timeframes
        for (const timeframe of this.timeframes) {
            await this.updateCandleForTimeframe(timeframe, price, amount, timestamp);
        }
    }
    
    async updateCandleForTimeframe(timeframe, price, amount, timestamp) {
        const intervalSeconds = this.getIntervalSeconds(timeframe);
        const candleTime = this.getCandleTime(timestamp, intervalSeconds);
        const buffer = this.buffers[timeframe];
        
        // Check if we need to finalize previous candle
        if (buffer.current && buffer.current.timestamp.getTime() !== candleTime.getTime()) {
            await this.finalizeCandle(timeframe);
        }
        
        // Start new candle or update existing
        if (!buffer.current || buffer.current.timestamp.getTime() !== candleTime.getTime()) {
            this.startNewCandle(timeframe, price, amount, candleTime);
        } else {
            this.updateCandle(timeframe, price, amount);
        }
    }
}
```

### Solution 2: Analytics Service Integration

Integrate with analytics service for historical data:

```javascript
class AnalyticsServiceIntegration {
    constructor(analyticsClient) {
        this.analyticsClient = analyticsClient;
        this.cache = new Map();
        this.cacheTTL = 300000; // 5 minutes
    }
    
    async getHistoricalCandles(symbol, timeframe, startTime, endTime) {
        const cacheKey = `${symbol}:${timeframe}:${startTime}:${endTime}`;
        
        // Check cache first
        if (this.cache.has(cacheKey)) {
            const cached = this.cache.get(cacheKey);
            if (Date.now() - cached.timestamp < this.cacheTTL) {
                return cached.data;
            }
        }
        
        try {
            // Fetch from analytics service
            const candles = await this.analyticsClient.getCandles({
                symbol,
                timeframe,
                startTime,
                endTime
            });
            
            // Cache the result
            this.cache.set(cacheKey, {
                data: candles,
                timestamp: Date.now()
            });
            
            return candles;
        } catch (error) {
            console.error('Failed to fetch historical candles:', error);
            return [];
        }
    }
    
    async fillMissingCandles(symbol, timeframe, missingPeriods) {
        const filledCandles = [];
        
        for (const period of missingPeriods) {
            try {
                const candles = await this.getHistoricalCandles(
                    symbol, 
                    timeframe, 
                    period.start, 
                    period.end
                );
                filledCandles.push(...candles);
            } catch (error) {
                console.error(`Failed to fill candles for period ${period.start}-${period.end}:`, error);
            }
        }
        
        return filledCandles;
    }
}
```

### Solution 3: Tardis Replay Integration

Implement Tardis replay for missing candles:

```javascript
class TardisReplayIntegration {
    constructor(tardisClient) {
        this.tardisClient = tardisClient;
    }
    
    async replayIntradayData(symbol, exchange, startTime, endTime) {
        try {
            const { replayNormalized } = require('tardis-dev');
            
            const messages = replayNormalized(
                {
                    exchange: exchange,
                    symbols: [symbol.toLowerCase().replace('-', '')],
                    from: startTime.toISOString(),
                    to: endTime.toISOString()
                },
                normalizeTrades
            );
            
            const trades = [];
            for await (const message of messages) {
                if (message.type === 'trade') {
                    trades.push(message);
                }
            }
            
            return trades;
        } catch (error) {
            console.error('Tardis replay failed:', error);
            return [];
        }
    }
    
    async generateCandlesFromTrades(trades, timeframe) {
        const intervalSeconds = this.getIntervalSeconds(timeframe);
        const candles = new Map();
        
        for (const trade of trades) {
            const timestamp = new Date(trade.timestamp);
            const candleTime = this.getCandleTime(timestamp, intervalSeconds);
            const key = candleTime.getTime();
            
            if (!candles.has(key)) {
                candles.set(key, {
                    symbol: trade.symbol,
                    timeframe,
                    timestamp: candleTime,
                    open: parseFloat(trade.price),
                    high: parseFloat(trade.price),
                    low: parseFloat(trade.price),
                    close: parseFloat(trade.price),
                    volume: parseFloat(trade.amount),
                    tradeCount: 1
                });
            } else {
                const candle = candles.get(key);
                candle.high = Math.max(candle.high, parseFloat(trade.price));
                candle.low = Math.min(candle.low, parseFloat(trade.price));
                candle.close = parseFloat(trade.price);
                candle.volume += parseFloat(trade.amount);
                candle.tradeCount++;
            }
        }
        
        return Array.from(candles.values());
    }
}
```

### Solution 4: Comprehensive Candle Manager

Combine all strategies into a unified system:

```javascript
class ComprehensiveCandleManager {
    constructor(symbol, exchange, options = {}) {
        this.symbol = symbol;
        this.exchange = exchange;
        this.buffer = new MultiTimeframeCandleBuffer(symbol, exchange);
        this.analytics = new AnalyticsServiceIntegration(options.analyticsClient);
        this.tardisReplay = new TardisReplayIntegration(options.tardisClient);
        this.fallbackStrategies = [
            'analytics',
            'tardis_replay',
            'exchange_api'
        ];
    }
    
    async processTick(tick) {
        // Process tick through buffer
        await this.buffer.processTick(tick);
        
        // Check for missing candles and fill them
        await this.checkAndFillMissingCandles();
    }
    
    async checkAndFillMissingCandles() {
        for (const timeframe of this.buffer.timeframes) {
            const missingPeriods = this.detectMissingCandles(timeframe);
            
            if (missingPeriods.length > 0) {
                await this.fillMissingCandles(timeframe, missingPeriods);
            }
        }
    }
    
    async fillMissingCandles(timeframe, missingPeriods) {
        for (const strategy of this.fallbackStrategies) {
            try {
                let candles = [];
                
                switch (strategy) {
                    case 'analytics':
                        candles = await this.analytics.fillMissingCandles(
                            this.symbol, timeframe, missingPeriods
                        );
                        break;
                    case 'tardis_replay':
                        const trades = await this.tardisReplay.replayIntradayData(
                            this.symbol, this.exchange, 
                            missingPeriods[0].start, missingPeriods[missingPeriods.length - 1].end
                        );
                        candles = await this.tardisReplay.generateCandlesFromTrades(trades, timeframe);
                        break;
                    case 'exchange_api':
                        // Implement exchange API fallback
                        break;
                }
                
                if (candles.length > 0) {
                    await this.buffer.addCandles(timeframe, candles);
                    break; // Success, no need to try other strategies
                }
            } catch (error) {
                console.error(`Fallback strategy ${strategy} failed:`, error);
            }
        }
    }
    
    async getCandles(timeframe, startTime, endTime) {
        return await this.buffer.getCandles(timeframe, startTime, endTime);
    }
}
```

## Implementation Tasks

### Phase 1: Multi-Timeframe Buffer
- [ ] Implement `MultiTimeframeCandleBuffer` class
- [ ] Add support for all timeframes (15s to 24h)
- [ ] Implement candle aggregation logic
- [ ] Add buffer management and cleanup

### Phase 2: Analytics Integration
- [ ] Implement `AnalyticsServiceIntegration` class
- [ ] Add caching for historical data
- [ ] Implement missing candle detection
- [ ] Add fallback strategies

### Phase 3: Tardis Replay
- [ ] Implement `TardisReplayIntegration` class
- [ ] Add trade-to-candle conversion
- [ ] Implement replay with realistic timing
- [ ] Add error handling and retry logic

### Phase 4: Unified System
- [ ] Implement `ComprehensiveCandleManager` class
- [ ] Integrate all components
- [ ] Add configuration management
- [ ] Implement monitoring and alerting

## Dependencies and Risks

### Dependencies
- Analytics service API
- Tardis.dev replay functionality
- Exchange APIs for fallback
- Redis or similar for caching
- Configuration management system

### Risks
- **Performance Impact**: Multiple timeframes may impact performance
- **Data Consistency**: Different sources may have inconsistent data
- **API Limitations**: Rate limits and availability issues
- **Complexity**: More complex system with multiple fallback strategies

### Mitigation Strategies
- Implement efficient data structures and algorithms
- Add data validation and consistency checks
- Implement rate limiting and backoff strategies
- Add comprehensive monitoring and alerting

## Success Criteria

1. **Complete Timeframe Coverage**: All timeframes from 15s to 24h are supported
2. **Data Continuity**: No missing candles during normal operation
3. **Fallback Reliability**: Fallback strategies work consistently
4. **Performance**: Candle processing completes within 100ms
5. **Monitoring**: Clear visibility into candle availability and quality

## Priority

**High** - This is critical for providing continuous candle data for analytics and trading.

## Estimated Effort

- **Multi-Timeframe Buffer**: 2-3 weeks
- **Analytics Integration**: 1-2 weeks
- **Tardis Replay**: 1-2 weeks
- **Unified System**: 2-3 weeks
- **Total**: 6-10 weeks
