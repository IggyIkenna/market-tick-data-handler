# Market Data Streaming Service Guide

## Overview

The Market Data Streaming Service provides two distinct modes for processing real-time cryptocurrency market data using Node.js for optimal WebSocket performance:

1. **Raw Tick Streaming** (`ticks` mode) - Streams raw tick data to BigQuery for analytics
2. **Multi-Timeframe Candle Processing** (`candles` mode) - Processes ticks into multiple timeframes with HFT features

> **Note**: This service is now integrated into the local development workflow and is not VM-deployed. Use `./deploy/local/run-main.sh streaming-ticks` or `./deploy/local/run-main.sh streaming-candles` for local development and testing.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Market Data Streaming Service                     │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─────────────────┐    ┌──────────────────────────────────────┐   │
│  │   Tardis.dev    │    │            Tick Streamer             │   │
│  │   Real-time     │───▶│  - UTC timestamp alignment           │   │
│  │   WebSocket     │    │  - Latency tracking                  │   │
│  │   (Node.js)     │    │  - Dual mode processing             │   │
│  └─────────────────┘    └──────────────────┬───────────────────┘   │
│                                            │                       │
│  ┌─────────────────┐    ┌──────────────────▼───────────────────┐   │
│  │   Historical    │    │              Mode Router             │   │
│  │   Data Replay   │───▶│                                      │   │
│  │   (Fallback)    │    └──────────────┬─────────────────┬─────┘   │
│  └─────────────────┘                   │                 │         │
│                                        │                 │         │
│           ┌────────────────────────────▼─┐             ┌─▼───────┐ │
│           │        TICKS MODE            │             │ CANDLES │ │
│           │                              │             │  MODE   │ │
│           │  ┌─────────────────────────┐ │             │         │ │
│           │  │    BigQuery Streaming   │ │             │ ┌─────┐ │ │
│           │  │  - Batch optimization   │ │             │ │15s  │ │ │
│           │  │  - Cost monitoring      │ │             │ │1m   │ │ │
│           │  │  - Schema management    │ │             │ │5m   │ │ │
│           │  │  - Grafana alternative  │ │             │ │15m  │ │ │
│           │  └─────────────────────────┘ │             │ │4h   │ │ │
│           │                              │             │ │24h  │ │ │
│           │  ┌─────────────────────────┐ │             │ └─────┘ │ │
│           │  │     Analytics Views     │ │             │         │ │
│           │  │  - Real-time prices     │ │             │ ┌─────┐ │ │
│           │  │  - Volume analysis      │ │             │ │ HFT │ │ │
│           │  │  - Latency monitoring   │ │             │ │FEAT │ │ │
│           │  └─────────────────────────┘ │             │ │URES │ │ │
│           └────────────────────────────▲─┘             │ └─────┘ │ │
│                                        │               └─────────┘ │
│                                        │                           │
│           ┌────────────────────────────┴─────────────────────────┐ │
│           │                  Monitoring & Logging                │ │
│           │  - Structured logging    - Performance metrics       │ │
│           │  - Health checks        - Error tracking             │ │
│           │  - Docker integration   - Grafana dashboards         │ │
│           └──────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

## Features

### UTC Timestamp Alignment
- All candles align to even UTC time boundaries from midnight
- Supports: 15s (:00, :15, :30, :45), 1m (:00), 5m (:00, :05, :10...), etc.
- Proper timezone handling and conversion

### Latency Tracking
- `timestamp_in`: Aligned candle boundary time
- `timestamp_out`: Processing completion time
- Real-time latency monitoring and reporting

### BigQuery Integration
- Cost-optimized batch streaming
- Auto-schema creation and management
- Pre-built analytics views
- Alternative to expensive Grafana setups

### HFT Features
Computed on 15s and 1m candles:
- **Moving Averages**: SMA, EMA, WMA (5, 10, 20 periods)
- **Momentum**: Price momentum, velocity, acceleration
- **Volume**: Volume ratios, VWAP, volume EMA
- **Volatility**: Rolling volatility, high-low ratios, returns
- **Microstructure**: Trade intensity, avg trade size, price impact
- **Technical**: RSI, Bollinger bands, MACD

## Quick Start

### 1. Environment Setup

```bash
# Set Tardis API key
export TARDIS_API_KEY="TD.your_api_key_here"

# Optional: Set GCP credentials for BigQuery
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
export GOOGLE_CLOUD_PROJECT="your-project-id"
```

### 2. Deploy Services

```bash
# Candle processing mode (default)
./deploy/streaming/deploy-streaming-service.sh --mode candles --symbol BTC-USDT

# Raw tick streaming to BigQuery
./deploy/streaming/deploy-streaming-service.sh --mode ticks --symbol ETH-USDT --duration 1800

# Node.js real-time streaming
./deploy/streaming/deploy-streaming-service.sh --nodejs --symbol BTC-USDT --duration 3600
```

### 3. Monitor Services

```bash
# View logs
docker-compose -f docker/streaming-service/docker-compose.yml logs -f candle-processor

# Check status
docker ps | grep market-

# Monitor dashboard (if enabled)
open http://localhost:8080
```

## Deployment Modes

### Mode 1: Raw Tick Streaming (`ticks`)

Streams raw tick data to BigQuery for analytics and visualization.

```bash
./deploy/streaming/deploy-streaming-service.sh \
  --mode ticks \
  --symbol BTC-USDT \
  --exchange binance \
  --duration 3600
```

**Features:**
- High-throughput tick ingestion
- BigQuery batch optimization
- Cost monitoring
- Pre-built analytics views
- Grafana alternative dashboards

**Output:** BigQuery table with schema:
```sql
CREATE TABLE `project.market_data_streaming.ticks_binance_btc_usdt` (
  symbol STRING,
  exchange STRING,
  price FLOAT64,
  amount FLOAT64,
  side STRING,
  timestamp TIMESTAMP,
  timestamp_received TIMESTAMP,
  trade_id STRING,
  latency_ms FLOAT64
);
```

### Mode 2: Multi-Timeframe Candles (`candles`)

Processes ticks into multiple timeframes with HFT features.

```bash
./deploy/streaming/deploy-streaming-service.sh \
  --mode candles \
  --symbol BTC-USDT \
  --duration 1800
```

**Features:**
- UTC-aligned candle boundaries
- Multiple timeframes: 15s, 1m, 5m, 15m, 4h, 24h
- HFT feature computation
- Real-time latency tracking
- Memory-efficient processing

**Output:** Structured candle data with features:
```json
{
  "symbol": "BTC-USDT",
  "timeframe": "1m",
  "timestamp_in": "2024-10-24T20:15:00Z",
  "timestamp_out": "2024-10-24T20:15:01.234Z",
  "ohlcv": {...},
  "hft_features": {...},
  "latency_ms": 1234.5
}
```

### Node.js Real-Time Streaming

Uses Tardis.dev Node.js client for true real-time streaming.

```bash
./deploy/streaming/deploy-streaming-service.sh \
  --nodejs \
  --symbol BTC-USDT \
  --duration 3600
```

**Features:**
- True real-time WebSocket streaming
- Minimal latency
- Direct exchange connection
- Live candle generation

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `TARDIS_API_KEY` | Tardis.dev API key | Required |
| `STREAMING_MODE` | Mode: `ticks` or `candles` | `candles` |
| `STREAMING_SYMBOL` | Trading symbol | `BTC-USDT` |
| `STREAMING_EXCHANGE` | Exchange name | `binance` |
| `STREAMING_DURATION` | Duration in seconds (0=infinite) | `3600` |
| `STREAMING_TIMEFRAMES` | Comma-separated timeframes | `15s,1m,5m,15m,4h,24h` |
| `ENABLE_HFT_FEATURES` | Enable HFT features | `true` |
| `BIGQUERY_DATASET` | BigQuery dataset | `market_data_streaming` |
| `BIGQUERY_TABLE` | BigQuery table | Auto-generated |
| `LOG_LEVEL` | Logging level | `INFO` |

### Docker Compose Profiles

- `ticks`: Raw tick streaming service
- `candles`: Multi-timeframe candle processor
- `nodejs`: Node.js real-time streamer
- `monitoring`: Optional monitoring dashboard

## BigQuery Analytics

### Cost Optimization

The service is designed as a cost-effective alternative to Grafana + InfluxDB:

- **Batch streaming**: Reduces BigQuery streaming costs
- **Partitioned tables**: Optimizes query performance
- **Clustered indexes**: Improves query speed
- **Pre-aggregated views**: Reduces query costs

### Pre-built Views

1. **Real-time Prices**
   ```sql
   SELECT symbol, timestamp, price, 
          price - LAG(price) OVER (ORDER BY timestamp) as change
   FROM ticks_table 
   WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
   ```

2. **Volume Analysis**
   ```sql
   SELECT symbol, TIMESTAMP_TRUNC(timestamp, MINUTE) as minute,
          SUM(amount) as volume, COUNT(*) as trades
   FROM ticks_table 
   GROUP BY symbol, minute
   ```

3. **Latency Monitoring**
   ```sql
   SELECT symbol, AVG(latency_ms) as avg_latency,
          PERCENTILE_CONT(latency_ms, 0.95) OVER() as p95_latency
   FROM ticks_table
   ```

## HFT Features Reference

### Price-Based Features
- `sma_5`, `sma_10`, `sma_20`: Simple moving averages
- `ema_5`, `ema_10`, `ema_20`: Exponential moving averages
- `wma_5`: Weighted moving average
- `price_momentum_3`, `price_momentum_5`: Price momentum
- `price_velocity`: Rate of price change
- `price_acceleration`: Rate of velocity change

### Volume Features
- `volume_sma_5`, `volume_ema_5`: Volume averages
- `volume_ratio`: Current vs average volume
- `vwap`: Volume-weighted average price
- `vwap_deviation`: Price deviation from VWAP

### Volatility Features
- `price_volatility_5`, `price_volatility_10`: Rolling volatility
- `high_low_ratio`: (High-Low)/Close ratio
- `close_to_close_return`: Logarithmic return

### Microstructure Features
- `trade_intensity`: Trades per unit time
- `avg_trade_size`: Average trade size
- `price_impact`: Price impact estimate
- `bid_ask_spread_proxy`: High-Low as spread proxy

### Technical Indicators
- `rsi_5`: 5-period RSI
- `bollinger_position`: Position within Bollinger bands
- `macd_signal`: MACD signal line

## Monitoring & Operations

### Health Checks

```bash
# Check service health
docker ps | grep market-
docker-compose -f docker/streaming-service/docker-compose.yml ps

# View service logs
docker-compose -f docker/streaming-service/docker-compose.yml logs -f candle-processor
```

### Performance Monitoring

The service tracks:
- Ticks processed per second
- Candles completed per timeframe
- Processing latency
- Memory usage
- Error rates

### Troubleshooting

Common issues and solutions:

1. **API Key Issues**
   ```bash
   # Check API key
   echo $TARDIS_API_KEY
   
   # Test API key
   curl -H "Authorization: Bearer $TARDIS_API_KEY" https://api.tardis.dev/v1/exchanges
   ```

2. **BigQuery Permissions**
   ```bash
   # Check GCP credentials
   gcloud auth list
   
   # Test BigQuery access
   bq ls
   ```

3. **Memory Issues**
   ```bash
   # Monitor memory usage
   docker stats
   
   # Adjust container resources
   docker-compose -f docker/streaming-service/docker-compose.yml up -d --scale candle-processor=1
   ```

## Advanced Usage

### Custom Timeframes

```bash
# Custom timeframes
export STREAMING_TIMEFRAMES="30s,2m,10m,1h"
./deploy/streaming/deploy-streaming-service.sh --mode candles
```

### Multiple Symbols

```bash
# Deploy multiple instances
for symbol in BTC-USDT ETH-USDT ADA-USDT; do
  ./deploy/streaming/deploy-streaming-service.sh --mode candles --symbol $symbol &
done
```

### Production Deployment

```bash
# Production configuration
export STREAMING_DURATION=0  # Infinite
export LOG_LEVEL=WARNING
export ENABLE_HFT_FEATURES=true

./deploy/streaming/deploy-streaming-service.sh \
  --mode candles \
  --symbol BTC-USDT \
  --env production
```

## API Integration

The service can be extended with REST/WebSocket APIs for real-time data access:

```python
# Example API client
import asyncio
import websockets
import json

async def connect_to_stream():
    uri = "ws://localhost:8081/stream"
    async with websockets.connect(uri) as websocket:
        async for message in websocket:
            data = json.loads(message)
            print(f"Candle: {data['symbol']} {data['timeframe']} ${data['close']}")

asyncio.run(connect_to_stream())
```

## Cost Analysis

### BigQuery vs Grafana + InfluxDB

| Solution | Monthly Cost (1M ticks/day) | Features |
|----------|----------------------------|----------|
| **BigQuery** | ~$5-15 | Serverless, analytics, ML |
| **Grafana + InfluxDB** | ~$50-200 | Self-hosted, dashboards |
| **Commercial** | ~$500-2000 | Enterprise features |

The BigQuery solution provides 80% of the functionality at 10% of the cost.

## Support

For issues and questions:

1. Check the logs: `docker-compose logs`
2. Review this documentation
3. Check Tardis.dev API status
4. Verify BigQuery quotas and permissions

## Changelog

- **v1.0.0**: Initial release with dual modes
- **v1.1.0**: Added HFT features and BigQuery integration
- **v1.2.0**: Node.js real-time streaming support
