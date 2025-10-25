# Issue #008: Historical Candle Buffer Startup for Downstream Usage

## Problem Statement

The package needs to automatically load 100 candles of historical data when starting up to provide immediate context for live data candles. This buffer is essential for downstream usage where users expect to have historical context available immediately when requesting live data.

## Current State Analysis

### Current Live Streaming System

**Files**:
- `streaming/live_tick_streamer.js` - Main live streaming service
- `src/streaming_service/hft_features/feature_calculator.py` - HFT feature calculator
- `market_data_tick_handler/streaming_service/hft_features/feature_calculator.py` - HFT feature calculator

### Current Behavior

The current system:

1. **No Historical Buffer**: Starts with empty candle history
2. **No Startup Data Loading**: Does not load historical data on startup
3. **Limited Context**: Users must wait for candles to accumulate
4. **No Downstream Support**: No mechanism for downstream services to get historical context
5. **No Buffer Management**: No systematic way to maintain historical buffer

### Issues Identified

1. **Cold Start Problem**: Users get no historical context when starting live streaming
2. **Feature Calculation Delay**: HFT features cannot be calculated without historical data
3. **Poor User Experience**: Downstream users must wait for data to accumulate
4. **No Buffer Persistence**: Historical buffer is lost on restart
5. **No Buffer Management**: No systematic way to maintain and update historical buffer

## Proposed Solutions

### Solution 1: Historical Candle Buffer Service

Create a dedicated service for managing historical candle buffers:

```python
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass
from collections import deque

@dataclass
class CandleBuffer:
    symbol: str
    timeframe: str
    candles: deque
    max_size: int
    last_updated: datetime
    
    def add_candle(self, candle: Dict):
        """Add a new candle to the buffer"""
        self.candles.append(candle)
        if len(self.candles) > self.max_size:
            self.candles.popleft()
        self.last_updated = datetime.now()
    
    def get_candles(self, count: int = None) -> List[Dict]:
        """Get candles from the buffer"""
        if count is None:
            return list(self.candles)
        return list(self.candles)[-count:]
    
    def get_latest_candle(self) -> Optional[Dict]:
        """Get the latest candle"""
        return self.candles[-1] if self.candles else None

class HistoricalCandleBufferService:
    def __init__(self, data_client, buffer_size: int = 100):
        self.data_client = data_client
        self.buffer_size = buffer_size
        self.buffers: Dict[str, CandleBuffer] = {}
        self.initialized = False
    
    async def initialize_buffers(self, symbols: List[str], timeframes: List[str]):
        """Initialize historical candle buffers for all symbols and timeframes"""
        print("🔄 Initializing historical candle buffers...")
        
        for symbol in symbols:
            for timeframe in timeframes:
                buffer_key = f"{symbol}:{timeframe}"
                
                # Load historical candles
                historical_candles = await self.load_historical_candles(symbol, timeframe)
                
                # Create buffer
                self.buffers[buffer_key] = CandleBuffer(
                    symbol=symbol,
                    timeframe=timeframe,
                    candles=deque(historical_candles, maxlen=self.buffer_size),
                    max_size=self.buffer_size,
                    last_updated=datetime.now()
                )
                
                print(f"✅ Loaded {len(historical_candles)} candles for {symbol} {timeframe}")
        
        self.initialized = True
        print("✅ Historical candle buffers initialized")
    
    async def load_historical_candles(self, symbol: str, timeframe: str) -> List[Dict]:
        """Load historical candles from data source"""
        try:
            # Calculate date range for last 100 candles
            end_date = datetime.now()
            start_date = self.calculate_start_date(end_date, timeframe, self.buffer_size)
            
            # Load candles from data client
            candles = await self.data_client.get_candles(
                symbol=symbol,
                timeframe=timeframe,
                start_date=start_date,
                end_date=end_date,
                limit=self.buffer_size
            )
            
            return candles
            
        except Exception as e:
            print(f"❌ Failed to load historical candles for {symbol} {timeframe}: {e}")
            return []
    
    def calculate_start_date(self, end_date: datetime, timeframe: str, candle_count: int) -> datetime:
        """Calculate start date based on timeframe and candle count"""
        timeframe_minutes = {
            '15s': 0.25,
            '30s': 0.5,
            '1m': 1,
            '5m': 5,
            '15m': 15,
            '1h': 60,
            '4h': 240,
            '24h': 1440
        }
        
        minutes = timeframe_minutes.get(timeframe, 1)
        total_minutes = minutes * candle_count
        
        return end_date - timedelta(minutes=total_minutes)
    
    def get_candles(self, symbol: str, timeframe: str, count: int = None) -> List[Dict]:
        """Get candles from buffer"""
        buffer_key = f"{symbol}:{timeframe}"
        buffer = self.buffers.get(buffer_key)
        
        if not buffer:
            return []
        
        return buffer.get_candles(count)
    
    def add_live_candle(self, symbol: str, timeframe: str, candle: Dict):
        """Add live candle to buffer"""
        buffer_key = f"{symbol}:{timeframe}"
        buffer = self.buffers.get(buffer_key)
        
        if buffer:
            buffer.add_candle(candle)
    
    def get_buffer_status(self) -> Dict:
        """Get status of all buffers"""
        status = {}
        for buffer_key, buffer in self.buffers.items():
            status[buffer_key] = {
                'candle_count': len(buffer.candles),
                'max_size': buffer.max_size,
                'last_updated': buffer.last_updated.isoformat(),
                'is_full': len(buffer.candles) == buffer.max_size
            }
        return status
```

### Solution 2: Enhanced Live Streaming Service

Integrate historical buffer into the live streaming service:

```javascript
class EnhancedLiveTickStreamer {
    constructor(symbol = 'BTC-USDT', exchange = 'binance', mode = 'candles', dataType = 'trades', candleInterval = '1m') {
        // ... existing constructor code ...
        
        // Initialize historical buffer service
        this.historicalBuffer = new HistoricalCandleBufferService(this.dataClient, 100);
        this.bufferInitialized = false;
    }
    
    async startStreaming() {
        console.log(`🚀 Starting enhanced live streaming for ${this.symbol}`);
        
        // Initialize historical buffers first
        if (!this.bufferInitialized) {
            console.log("🔄 Loading historical candle buffers...");
            await this.initializeHistoricalBuffers();
            this.bufferInitialized = true;
        }
        
        // Start live streaming
        await this.startLiveStreaming();
    }
    
    async initializeHistoricalBuffers() {
        try {
            // Load historical candles for all timeframes
            const timeframes = ['15s', '1m', '5m', '15m', '1h', '4h', '24h'];
            
            for (const timeframe of timeframes) {
                const candles = await this.loadHistoricalCandles(timeframe);
                
                // Store in buffer
                this.historicalBuffer.setCandles(this.symbol, timeframe, candles);
                
                console.log(`✅ Loaded ${candles.length} historical candles for ${timeframe}`);
            }
            
            console.log("✅ Historical buffers initialized successfully");
            
        } catch (error) {
            console.error("❌ Failed to initialize historical buffers:", error);
            // Continue with empty buffers
        }
    }
    
    async loadHistoricalCandles(timeframe) {
        try {
            // Calculate date range for last 100 candles
            const endDate = new Date();
            const startDate = this.calculateStartDate(endDate, timeframe, 100);
            
            // Load from data client
            const candles = await this.dataClient.getCandles({
                symbol: this.symbol,
                timeframe: timeframe,
                startDate: startDate,
                endDate: endDate,
                limit: 100
            });
            
            return candles;
            
        } catch (error) {
            console.error(`Failed to load historical candles for ${timeframe}:`, error);
            return [];
        }
    }
    
    calculateStartDate(endDate, timeframe, candleCount) {
        const timeframeMinutes = {
            '15s': 0.25,
            '30s': 0.5,
            '1m': 1,
            '5m': 5,
            '15m': 15,
            '1h': 60,
            '4h': 240,
            '24h': 1440
        };
        
        const minutes = timeframeMinutes[timeframe] || 1;
        const totalMinutes = minutes * candleCount;
        
        return new Date(endDate.getTime() - (totalMinutes * 60 * 1000));
    }
    
    async getCandlesWithHistory(timeframe, count = 100) {
        """Get candles with historical context"""
        // Get historical candles from buffer
        const historicalCandles = this.historicalBuffer.getCandles(this.symbol, timeframe, count);
        
        // Get current live candle
        const currentCandle = this.getCurrentCandle(timeframe);
        
        // Combine historical and current
        const allCandles = [...historicalCandles];
        if (currentCandle) {
            allCandles.push(currentCandle);
        }
        
        return allCandles;
    }
}
```

### Solution 3: Downstream API Enhancement

Enhance the downstream API to provide historical context:

```python
from fastapi import FastAPI, HTTPException
from typing import List, Optional
from datetime import datetime

app = FastAPI()

class CandleAPI:
    def __init__(self, buffer_service):
        self.buffer_service = buffer_service
    
    @app.get("/candles/{symbol}/{timeframe}")
    async def get_candles(
        symbol: str,
        timeframe: str,
        count: int = 100,
        include_live: bool = True
    ):
        """Get candles with historical context"""
        try:
            # Get historical candles from buffer
            historical_candles = self.buffer_service.get_candles(symbol, timeframe, count)
            
            if include_live:
                # Get current live candle
                live_candle = self.get_current_live_candle(symbol, timeframe)
                if live_candle:
                    historical_candles.append(live_candle)
            
            return {
                "symbol": symbol,
                "timeframe": timeframe,
                "candles": historical_candles,
                "count": len(historical_candles),
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/candles/{symbol}/{timeframe}/latest")
    async def get_latest_candle(symbol: str, timeframe: str):
        """Get latest candle with historical context"""
        try:
            # Get last 100 candles for context
            candles = self.buffer_service.get_candles(symbol, timeframe, 100)
            
            if not candles:
                raise HTTPException(status_code=404, detail="No candles available")
            
            latest_candle = candles[-1]
            
            return {
                "symbol": symbol,
                "timeframe": timeframe,
                "candle": latest_candle,
                "historical_context": candles[:-1],  # All except latest
                "context_count": len(candles) - 1
            }
            
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/candles/{symbol}/{timeframe}/buffer-status")
    async def get_buffer_status(symbol: str, timeframe: str):
        """Get buffer status for a symbol and timeframe"""
        try:
            status = self.buffer_service.get_buffer_status()
            buffer_key = f"{symbol}:{timeframe}"
            
            if buffer_key not in status:
                raise HTTPException(status_code=404, detail="Buffer not found")
            
            return status[buffer_key]
            
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
```

### Solution 4: Buffer Persistence and Recovery

Implement buffer persistence and recovery:

```python
import json
import os
from datetime import datetime, timedelta

class PersistentCandleBuffer:
    def __init__(self, buffer_service, persistence_dir: str = "data/buffers"):
        self.buffer_service = buffer_service
        self.persistence_dir = persistence_dir
        self.ensure_persistence_dir()
    
    def ensure_persistence_dir(self):
        """Ensure persistence directory exists"""
        os.makedirs(self.persistence_dir, exist_ok=True)
    
    async def save_buffers(self):
        """Save all buffers to disk"""
        try:
            for buffer_key, buffer in self.buffer_service.buffers.items():
                filename = f"{buffer_key.replace(':', '_')}.json"
                filepath = os.path.join(self.persistence_dir, filename)
                
                buffer_data = {
                    "symbol": buffer.symbol,
                    "timeframe": buffer.timeframe,
                    "candles": list(buffer.candles),
                    "max_size": buffer.max_size,
                    "last_updated": buffer.last_updated.isoformat(),
                    "saved_at": datetime.now().isoformat()
                }
                
                with open(filepath, 'w') as f:
                    json.dump(buffer_data, f, indent=2)
                
                print(f"✅ Saved buffer {buffer_key} to {filepath}")
                
        except Exception as e:
            print(f"❌ Failed to save buffers: {e}")
    
    async def load_buffers(self):
        """Load buffers from disk"""
        try:
            loaded_count = 0
            
            for filename in os.listdir(self.persistence_dir):
                if filename.endswith('.json'):
                    filepath = os.path.join(self.persistence_dir, filename)
                    
                    with open(filepath, 'r') as f:
                        buffer_data = json.load(f)
                    
                    # Check if buffer is recent (within last 24 hours)
                    saved_at = datetime.fromisoformat(buffer_data['saved_at'])
                    if datetime.now() - saved_at > timedelta(hours=24):
                        print(f"⚠️  Buffer {filename} is too old, skipping")
                        continue
                    
                    # Restore buffer
                    buffer_key = f"{buffer_data['symbol']}:{buffer_data['timeframe']}"
                    
                    self.buffer_service.buffers[buffer_key] = CandleBuffer(
                        symbol=buffer_data['symbol'],
                        timeframe=buffer_data['timeframe'],
                        candles=deque(buffer_data['candles'], maxlen=buffer_data['max_size']),
                        max_size=buffer_data['max_size'],
                        last_updated=datetime.fromisoformat(buffer_data['last_updated'])
                    )
                    
                    loaded_count += 1
                    print(f"✅ Loaded buffer {buffer_key} from {filename}")
            
            print(f"✅ Loaded {loaded_count} buffers from disk")
            
        except Exception as e:
            print(f"❌ Failed to load buffers: {e}")
    
    async def cleanup_old_buffers(self):
        """Clean up old buffer files"""
        try:
            for filename in os.listdir(self.persistence_dir):
                if filename.endswith('.json'):
                    filepath = os.path.join(self.persistence_dir, filename)
                    
                    # Check file age
                    file_time = datetime.fromtimestamp(os.path.getmtime(filepath))
                    if datetime.now() - file_time > timedelta(days=7):
                        os.remove(filepath)
                        print(f"🗑️  Removed old buffer file {filename}")
                        
        except Exception as e:
            print(f"❌ Failed to cleanup old buffers: {e}")
```

## Implementation Tasks

### Phase 1: Buffer Service Implementation
- [ ] Implement `HistoricalCandleBufferService` class
- [ ] Add historical data loading functionality
- [ ] Implement buffer management and updates
- [ ] Add buffer status monitoring

### Phase 2: Live Streaming Integration
- [ ] Integrate buffer service into live streaming service
- [ ] Add startup buffer initialization
- [ ] Implement live candle buffer updates
- [ ] Add buffer status reporting

### Phase 3: Downstream API Enhancement
- [ ] Enhance downstream API with historical context
- [ ] Add buffer status endpoints
- [ ] Implement candle retrieval with history
- [ ] Add API documentation and examples

### Phase 4: Persistence and Recovery
- [ ] Implement buffer persistence to disk
- [ ] Add buffer recovery on startup
- [ ] Implement buffer cleanup and maintenance
- [ ] Add buffer validation and integrity checks

## Dependencies and Risks

### Dependencies
- Data client for historical data access
- Persistent storage for buffer data
- Configuration management for buffer settings
- Monitoring system for buffer status

### Risks
- **Memory Usage**: Large buffers may consume significant memory
- **Data Freshness**: Historical data may become stale
- **Startup Time**: Loading historical data may delay startup
- **Storage Requirements**: Buffer persistence requires disk space

### Mitigation Strategies
- Implement configurable buffer sizes
- Add data freshness validation
- Implement async loading to reduce startup delay
- Add buffer compression and cleanup

## Success Criteria

1. **Immediate Context**: Historical candles available immediately on startup
2. **Buffer Completeness**: 100 candles loaded for each timeframe
3. **Performance**: Buffer loading completes within 30 seconds
4. **Reliability**: Buffers persist across restarts
5. **API Integration**: Downstream services can access historical context

## Priority

**High** - This is essential for providing a good user experience for downstream usage.

## Estimated Effort

- **Buffer Service Implementation**: 1-2 weeks
- **Live Streaming Integration**: 1 week
- **Downstream API Enhancement**: 1 week
- **Persistence and Recovery**: 1-2 weeks
- **Total**: 4-6 weeks
