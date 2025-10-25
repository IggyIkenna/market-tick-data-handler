# Node.js Ingestion Layer

Real-time market data streaming using Tardis.dev Node.js client integrated with Python processing pipeline.

## Architecture

```
Tardis.dev Live Stream → Node.js Ingestion → WebSocket → Python Processing → Modes
     (WebSocket)           (tardis-dev)      (ws://)      (HFT Features)    (Serve/Persist)
```

## Features

- **Live Streaming**: Real-time data from Tardis.dev using [Node.js client](https://docs.tardis.dev/api/node-js)
- **Data Types**: Trades, book snapshots, derivative tickers, liquidations
- **WebSocket Integration**: Seamless communication with Python processing layer
- **BigQuery Persistence**: Optional direct persistence to BigQuery
- **Multi-timeframe**: Support for 15s, 1m, 5m, 15m candles
- **HFT Features**: Real-time feature calculation in Python

## Quick Start

### 1. Install Dependencies

```bash
# Install Node.js dependencies
npm install

# Install Python dependencies
pip install websockets asyncio
```

### 2. Set Environment Variables

```bash
# Required
export TARDIS_API_KEY="your_tardis_api_key"
export GCP_PROJECT_ID="your_project_id"
export GOOGLE_APPLICATION_CREDENTIALS="path/to/credentials.json"

# Optional
export BIGQUERY_DATASET="market_data_streaming"
export BIGQUERY_BATCH_SIZE="1000"
export BIGQUERY_BATCH_TIMEOUT="60000"
```

### 3. Start Streaming

```bash
# Start both Node.js and Python components
./start_streaming.sh

# Or with custom parameters
SYMBOL="ETH-USDT" EXCHANGE="binance" ./start_streaming.sh
```

### 4. Manual Start (for development)

```bash
# Terminal 1: Start Python WebSocket server
python3 python_websocket_server.py --symbol BTC-USDT --exchange binance

# Terminal 2: Start Node.js streamer
node live_tick_streamer.js --symbol BTC-USDT --exchange binance --dataTypes trades,book_snapshot_5
```

## Configuration

### Node.js Streamer Options

```bash
node live_tick_streamer.js [options]

Options:
  --symbol <symbol>           Trading symbol (default: BTC-USDT)
  --exchange <exchange>       Exchange name (default: binance)
  --mode <mode>              Mode: ticks or candles (default: candles)
  --dataTypes <types>        Comma-separated data types (default: trades,book_snapshot_5,derivative_ticker,liquidations)
  --timeframes <timeframes>  Comma-separated timeframes (default: 15s,1m,5m,15m)
  --pythonWebSocketUrl <url> Python WebSocket URL (default: ws://localhost:8765)
  --bigqueryEnabled <bool>   Enable BigQuery persistence (default: false)
```

### Python Server Options

```bash
python3 python_websocket_server.py [options]

Options:
  --host <host>              WebSocket host (default: localhost)
  --port <port>              WebSocket port (default: 8765)
  --symbol <symbol>          Trading symbol (default: BTC-USDT)
  --exchange <exchange>      Exchange name (default: binance)
  --timeframes <timeframes>  Space-separated timeframes (default: 15s 1m 5m 15m)
  --no-serve                 Disable serve mode
  --no-persist               Disable persist mode
```

## Data Flow

### 1. Tardis.dev Streaming
- Node.js connects to Tardis.dev WebSocket API
- Receives real-time market data (trades, book changes, etc.)
- Normalizes data using Tardis.dev normalizers

### 2. WebSocket Communication
- Node.js sends normalized data to Python via WebSocket
- Python receives and processes data in real-time
- Bidirectional communication for status and control

### 3. Python Processing
- **Tick Handler**: Processes raw tick data
- **Candle Processor**: Generates multi-timeframe candles
- **HFT Calculator**: Computes high-frequency trading features
- **Modes**: Serve (Redis/memory) and Persist (BigQuery)

## Supported Exchanges

- Binance
- Binance Futures
- BitMEX
- Deribit
- FTX
- OKEx
- Huobi Global
- Huobi DM
- Bybit
- And more...

## Data Types

### Trades
- Real-time trade data
- Price, amount, side, timestamp
- Trade ID and exchange info

### Book Snapshots
- Order book depth (5 or 25 levels)
- Bid/ask prices and sizes
- Real-time updates

### Derivative Tickers
- Funding rates
- Open interest
- Mark prices
- Index prices

### Liquidations
- Liquidation events
- Size and side
- Timestamp and exchange

## Monitoring

### Logs
- `logs/python_server.log` - Python WebSocket server logs
- `logs/node_streamer.log` - Node.js streamer logs

### Statistics
- Messages per second
- Total trades processed
- Total candles generated
- Uptime and connection status

### Health Checks
```bash
# Check if processes are running
ps aux | grep -E "(node|python)" | grep -v grep

# Check WebSocket connection
curl -i -N -H "Connection: Upgrade" -H "Upgrade: websocket" -H "Sec-WebSocket-Key: test" -H "Sec-WebSocket-Version: 13" http://localhost:8765/
```

## Development

### Testing
```bash
# Test Node.js streamer
node test_streaming.js

# Test Python server
python3 -m pytest test_python_server.py
```

### Debugging
```bash
# Enable debug logging
export DEBUG=tardis-dev
node live_tick_streamer.js --symbol BTC-USDT

# Python debug mode
python3 -u python_websocket_server.py --symbol BTC-USDT
```

## Troubleshooting

### Common Issues

1. **Tardis API Key Error**
   ```bash
   export TARDIS_API_KEY="your_actual_api_key"
   ```

2. **WebSocket Connection Failed**
   - Check if Python server is running
   - Verify port 8765 is available
   - Check firewall settings

3. **BigQuery Permission Error**
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="path/to/credentials.json"
   gcloud auth application-default login
   ```

4. **Node.js Dependencies Missing**
   ```bash
   npm install
   ```

5. **Python Dependencies Missing**
   ```bash
   pip install websockets asyncio
   ```

### Performance Tuning

1. **Batch Size**: Adjust `BIGQUERY_BATCH_SIZE` for BigQuery performance
2. **Timeout**: Adjust `BIGQUERY_BATCH_TIMEOUT` for latency vs throughput
3. **Memory**: Monitor memory usage for large symbol sets
4. **CPU**: Use multiple processes for different symbols

## Integration with Python Package

The Node.js ingestion layer integrates seamlessly with the existing Python package:

```python
# In your Python code
from market_data_tick_handler.streaming_service.modes.serve_mode import LiveFeatureStream

# Consume live features
async with LiveFeatureStream(symbol="BTC-USDT") as stream:
    async for candle_with_features in stream:
        # Use features in your strategy
        execute_strategy(candle_with_features)
```

## License

MIT License - see main project LICENSE file.
