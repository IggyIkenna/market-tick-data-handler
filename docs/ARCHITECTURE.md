# Market Data Tick Handler - Architecture

## Overview

The Market Data Tick Handler is a comprehensive system for downloading, processing, and storing cryptocurrency market data from Tardis.dev. It has been refactored into a clean package/library architecture that supports both batch processing and real-time streaming use cases.

## System Architecture

### Package Architecture

The system is organized as a clean package/library with three main deployment modes:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Package/Library Architecture                 │
│                                                                 │
│  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐   │
│  │  Data Client    │ │  Candle         │ │  BigQuery       │   │
│  │  Package        │ │  Processor      │ │  Uploader       │   │
│  │  (src/data_     │ │  Package        │ │  Package        │   │
│  │   client/)      │ │  (src/candle_   │ │  (src/bigquery_ │   │
│  │                 │ │   processor/)   │ │   uploader/)    │   │
│  └─────────────────┘ └─────────────────┘ └─────────────────┘   │
│                                                                 │
│  ┌─────────────────┐ ┌─────────────────┐                       │
│  │  Streaming      │ │  Main Entry     │                       │
│  │  Service        │ │  Point          │                       │
│  │  (Node.js)      │ │  (src/main.py)  │                       │
│  └─────────────────┘ └─────────────────┘                       │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Deployment Modes                             │
│                                                                 │
│  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐   │
│  │  VM Deployments │ │  Package Usage  │ │  Live Streaming │   │
│  │  (Batch Jobs)   │ │  (Library)      │ │  (Node.js)      │   │
│  │                 │ │                 │ │                 │   │
│  │  • Instruments  │ │  • Features     │ │  • Tick         │   │
│  │  • Downloads    │ │    Service      │ │    Streaming    │   │
│  │  • Candles      │ │  • Execution    │ │  • Candle       │   │
│  │  • BigQuery     │ │    Deployment   │ │    Processing   │   │
│  │    Uploads      │ │  • Analytics    │ │                 │   │
│  │                 │ │  • Backtesting  │ │                 │   │
│  └─────────────────┘ └─────────────────┘ └─────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Deployment Options                           │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐               │
│  │   Docker    │ │     VM      │ │   Local     │               │
│  │  (updated)  │ │ (enhanced)  │ │  (direct)   │               │
│  └─────────────┘ └─────────────┘ └─────────────┘               │
└─────────────────────────────────────────────────────────────────┘
```

## Operation Modes

The system supports multiple operation modes across different deployment types:

### Batch Processing Modes (VM Deployments)

#### 1. `check-gaps` - Gap Detection
- **Purpose**: Light check for missing instrument definitions in date range
- **Input**: Date range
- **Output**: List of missing instrument definition periods
- **Use Case**: Quick validation before instrument generation

#### 2. `instruments` - Instrument Generation
- **Purpose**: Generate canonical instrument definitions and upload to GCS
- **Input**: Date range, optional exchange filters
- **Output**: Instrument definitions in GCS
- **Use Case**: Create instrument registry for data downloads

#### 3. `missing-tick-reports` - Missing Data Analysis
- **Purpose**: Identify which instrument-level and data-type-level Tardis downloads are missing
- **Input**: Date range, optional venue/data-type filters
- **Output**: Missing data reports in GCS
- **Use Case**: Determine what data needs to be downloaded

#### 4. `download` - Smart Data Download
- **Purpose**: Download missing data (default) or force download all data
- **Input**: Date range, missing data reports from GCS, optional `--force` flag
- **Output**: Downloaded tick data in GCS with optimized Parquet format
- **Use Cases**: 
  - **Missing Data Mode (default)**: Efficient backfill of missing data
  - **Force Download Mode (`--force`)**: Re-download all data regardless of existing files
- **Architecture**: Uses `download_missing_data()` or `download_and_upload_data()` based on mode

#### 5. `validate` - Data Validation
- **Purpose**: Check for missing data and validate completeness
- **Input**: Date range, optional filters
- **Output**: Validation reports
- **Use Case**: Verify data completeness after downloads

#### 6. `candle-processing` - Historical Candle Generation
- **Purpose**: Process historical tick data into candles with HFT features
- **Input**: Date range, instrument filters
- **Output**: Processed candles in GCS (15s, 1m, 5m, 15m, 1h, 4h, 24h)
- **Use Case**: Generate candles for analytics and backtesting

#### 7. `bigquery-upload` - BigQuery Batch Upload
- **Purpose**: Upload processed candles to BigQuery for analytics
- **Input**: Date range, processed candles from GCS
- **Output**: Candles in BigQuery tables
- **Use Case**: Enable analytics and reporting

#### 8. `full-pipeline-ticks` - Complete Workflow
- **Purpose**: Run complete pipeline (check-gaps → instruments → missing-tick-reports → download → validate)
- **Input**: Date range, optional filters
- **Output**: Complete data pipeline execution
- **Use Case**: End-to-end data processing

### Live Streaming Modes (Node.js Services)

#### 9. `streaming-ticks` - Real-time Tick Streaming
- **Purpose**: Stream raw tick data to BigQuery for analytics
- **Input**: Symbol list, real-time WebSocket data
- **Output**: Tick data in BigQuery
- **Use Case**: Real-time analytics and monitoring

#### 10. `streaming-candles` - Real-time Candle Processing
- **Purpose**: Process real-time candles with HFT features
- **Input**: Symbol list, real-time WebSocket data
- **Output**: Candles with HFT features in BigQuery
- **Use Case**: Real-time trading and execution systems

## Data Architecture

### Single Partition Strategy

The system uses an optimized single partition strategy for maximum efficiency:

#### Instrument Definitions
```
gs://market-data-tick/
└── instrument_availability/
    └── by_date/
        └── day-{date}/
            └── instruments.parquet
```

#### Tick Data
```
gs://market-data-tick/
└── raw_tick_data/
    └── by_date/
        └── day-{date}/
            ├── data_type-trades/
            │   ├── BINANCE:SPOT_PAIR:BTC-USDT.parquet
            │   └── DERIBIT:PERP:BTC-USDT.parquet
            ├── data_type-book_snapshot_5/
            │   ├── BINANCE:SPOT_PAIR:BTC-USDT.parquet
            │   └── DERIBIT:PERP:BTC-USDT.parquet
            └── data_type-options_chain/
                └── DERIBIT:OPTION:BTC-USD-50000-241225-CALL.parquet
```

#### Processed Candles
```
gs://market-data-tick/
└── processed_candles/
    └── by_date/
        └── day-{date}/
            ├── timeframe-15s/
            │   └── BINANCE:SPOT_PAIR:BTC-USDT.parquet
            ├── timeframe-1m/
            │   └── BINANCE:SPOT_PAIR:BTC-USDT.parquet
            ├── timeframe-5m/
            │   └── BINANCE:SPOT_PAIR:BTC-USDT.parquet
            ├── timeframe-15m/
            │   └── BINANCE:SPOT_PAIR:BTC-USDT.parquet
            ├── timeframe-1h/
            │   └── BINANCE:SPOT_PAIR:BTC-USDT.parquet
            ├── timeframe-4h/
            │   └── BINANCE:SPOT_PAIR:BTC-USDT.parquet
            └── timeframe-24h/
                └── BINANCE:SPOT_PAIR:BTC-USDT.parquet
```

#### Book Snapshots
```
gs://market-data-tick/
└── processed_book_snapshots/
    └── by_date/
        └── day-{date}/
            ├── timeframe-15s/
            │   └── BINANCE:SPOT_PAIR:BTC-USDT.parquet
            ├── timeframe-1m/
            │   └── BINANCE:SPOT_PAIR:BTC-USDT.parquet
            └── ... (all timeframes)
```

### Data Types by Instrument Type

| Instrument Type | Available Data Types |
|----------------|---------------------|
| SPOT_PAIR | trades, book_snapshot_5 |
| PERP/PERPETUAL | trades, book_snapshot_5, derivative_ticker, liquidations |
| FUTURE | trades, book_snapshot_5, derivative_ticker, liquidations |
| OPTION | trades, book_snapshot_5, options_chain |

## Package Components

### Data Client Package (`src/data_client/`)

Clean data access interfaces for downstream services:

- **`DataClient`**: Core GCS data access and authentication
- **`TickDataReader`**: Read tick data with Parquet timestamp filtering
- **`CandleDataReader`**: Read processed candles across all timeframes
- **`HFTFeaturesReader`**: Read HFT features (15s, 1m timeframes)
- **`MFTFeaturesReader`**: Read MFT features (1m+ timeframes)

### Candle Processor Package (`src/candle_processor/`)

Historical and real-time candle processing:

- **`HistoricalCandleProcessor`**: Process tick data → 15s/1m candles with HFT features
- **`AggregatedCandleProcessor`**: Aggregate 1m → 5m/15m/4h/24h candles
- **`BookSnapshotProcessor`**: Process order book snapshots with derived features
- **`HFTFeatureProcessor`**: Calculate and aggregate HFT features

> **HFT Features**: See [HFT Features Specification](HFT_FEATURES_SPECIFICATION.md) for detailed technical specifications of the high-frequency trading features calculated from raw tick data.

### BigQuery Uploader Package (`src/bigquery_uploader/`)

Batch upload functionality for analytics:

- **`CandleUploader`**: Upload processed candles to BigQuery
- **`UploadOrchestrator`**: Orchestrate batch uploads with concurrency control

## Features Service Workflow

The features service uses the market data package in historical mode to process HFT features:

### Complete Data Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│                    VM Deployment Pipeline                       │
│                                                                 │
│  1. Instrument Definitions → GCS                                │
│  2. Missing Data Reports → GCS                                  │
│  3. Tick Data Download → GCS (optimized Parquet)                │
│  4. Candle Processing → GCS (15s-24h with HFT features)        │
│  5. BigQuery Upload → BigQuery (candles with HFT features)      │
│  6. MFT Features → GCS (1m+ timeframes)                         │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Features Service Package Usage               │
│                                                                 │
│  1. Import market data package                                  │
│  2. Query instruments from GCS                                  │
│  3. Get candle data from BigQuery (with HFT features)           │
│  4. Process additional MFT features                             │
│  5. Push features to GCS for backtesting                        │
└─────────────────────────────────────────────────────────────────┘
```

### Data Storage Architecture

The system maintains data in multiple formats for different use cases:

#### GCS Storage
- **Raw Tick Data**: Optimized Parquet with timestamp partitioning for intra-day queries
- **Processed Candles**: 15s, 1m, 5m, 15m, 1h, 4h, 24h timeframes with HFT features
- **MFT Features**: Medium-frequency features for 1m+ timeframes
- **Instrument Definitions**: Canonical instrument registry

#### BigQuery Storage
- **Candles with HFT Features**: One table per timeframe for analytics
- **Real-time Streaming Data**: Live tick and candle data for monitoring

### Package Usage Patterns

#### 1. Features Service (Historical Mode)
```python
from src.data_client import DataClient, CandleDataReader
from src.bigquery_uploader import BigQueryClient
from config import get_config

# Initialize clients
config = get_config()
data_client = DataClient(config.gcp.bucket, config)
candle_reader = CandleDataReader(data_client)
bq_client = BigQueryClient(config.gcp.project_id)

# Query instruments
instruments = data_client.get_instrument_definitions(
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 2)
)

# Get candle data from BigQuery (with HFT features)
candles = bq_client.get_candles(
    instrument_id="BINANCE:SPOT_PAIR:BTC-USDT",
    timeframe="1m",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 2)
)

# Process additional MFT features
# ... feature calculation logic ...

# Push features to GCS for backtesting
data_client.upload_features(features, "features/btc-usdt/1m/2024-01-01")
```

#### 2. Execution Service (Real-time + Historical)
```python
# Real-time: Get 15s candles with HFT features
candles_15s = candle_reader.get_candles(
    instrument_id="BINANCE:SPOT_PAIR:BTC-USDT",
    timeframe="15s",
    start_date=datetime.now() - timedelta(hours=1),
    end_date=datetime.now()
)

# Historical: Get tick data for backtesting
tick_data = data_client.get_tick_data(
    instrument_id="BINANCE:SPOT_PAIR:BTC-USDT",
    start_time=datetime(2024, 1, 1, 9, 0),
    end_time=datetime(2024, 1, 1, 9, 5)
)
```

#### 3. Analytics Service (BigQuery Queries)

**Historical Data (Daily Partitioned, No TTL):**
```python
# Query processed candles from BigQuery (historical/batch)
query = """
SELECT timestamp, timestamp_out, open, high, low, close, volume, 
       buy_volume_sum, sell_volume_sum, price_vwap, trade_count
FROM `project.dataset.candles_1m`
WHERE instrument_id = 'BINANCE:SPOT_PAIR:BTC-USDT'
  AND timestamp_out BETWEEN '2024-01-01' AND '2024-01-02'
  AND exchange = 'binance'
  AND symbol = 'BTC-USDT'
ORDER BY timestamp_out
"""
results = bq_client.query(query)
```

**Streaming Data (Hourly Partitioned, 30-day TTL):**
```python
# Query live streaming candles (last hour)
query = """
SELECT timestamp_out, symbol, exchange, open, high, low, close, volume,
       buy_volume_sum, sell_volume_sum, price_vwap
FROM `project.dataset.streaming_candles_1m`
WHERE timestamp_out >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
  AND exchange = 'binance'
  AND symbol = 'BTC-USDT'
ORDER BY timestamp_out DESC
"""
results = bq_client.query(query)
```

### Streaming Service (Python-First Architecture)

Real-time data processing with cost-optimized batching using **Python for speed**:

**Why Python over Node.js:**
- **Computational Performance**: HFT feature calculations are faster in Python
- **Unified Codebase**: Same feature logic for historical and live processing  
- **Memory Efficiency**: Better handling of large data structures
- **Integration**: Seamless integration with existing BigQuery batching

**Legacy Node.js Support:**
- **`streaming/live_tick_streamer.js`**: Available for WebSocket-only scenarios
- **Recommendation**: Use Python integrated service for production

**Python Integrated Streaming:**
```python
from market_data_tick_handler.streaming_service.integrated_streaming_service import create_integrated_streaming_service

# Create integrated streaming service
service = await create_integrated_streaming_service(
    symbol='BTC-USDT',
    exchange='binance', 
    timeframes=['15s', '1m', '5m', '15m'],
    enable_bigquery=True  # Automatic 1-minute batching
)

# Start streaming (handles everything automatically)
await service.start_streaming()

# Benefits:
# - Python speed for HFT calculations
# - 1-minute BigQuery batching (90% cost reduction)
# - Unified codebase with historical processing
# - Automatic batch flushing and error handling
```

**Cost Optimization:**
- **1-minute batching**: Reduces BigQuery API calls by 90%+
- **Background flushing**: Automatic uploads every minute
- **Thread-safe queuing**: High-frequency data handling
- **Graceful shutdown**: Force flush on termination

## Performance Architecture

### Optimal Configuration

The system is designed with performance best practices:

#### 1. Download Mode - Parallel Processing
- **Parallel Downloads**: Multiple instruments downloaded concurrently using `max_workers`
- **Connection Reuse**: Single TardisConnector shared across workers
- **Async I/O**: Non-blocking network operations
- **Performance**: 4.83x speedup over sequential downloads

#### 2. Processing Mode - Sequential (Optimal)
- **Sequential Processing**: Decompression, CSV parsing, and validation
- **Reason**: CPU-bound operations with GIL limitations
- **Performance**: Parallelization would hurt performance (0.91x with parallel)

#### 3. Upload Mode - Batched Parallel
- **Batched Uploads**: Memory-aware batching (90% threshold)
- **Parallel Uploads**: Multiple files uploaded concurrently
- **Client Reuse**: Singleton GCS client pattern
- **Performance**: 1.81x speedup over individual uploads

### Memory Management

- **Memory Threshold**: 90% RAM usage triggers upload batching
- **Batch Size**: Configurable via `BATCH_SIZE` environment variable (default: 1000)
- **Monitoring**: Real-time memory usage tracking
- **Safety**: Prevents out-of-memory errors during large operations

### Connection Optimization

- **Shared GCS Client**: All components use `src/utils/gcs_client.py`
- **Connection Pooling**: HTTP connections reused across operations
- **Authentication**: Single authentication shared across components
- **Performance**: Reduced connection overhead and faster operations

## Configuration Management

### Environment Variables

#### Required
```bash
TARDIS_API_KEY=TD.your_api_key_here
GCP_PROJECT_ID=your-gcp-project-id
GCP_CREDENTIALS_PATH=/path/to/service-account-key.json
GCS_BUCKET=your-gcs-bucket-name
```

#### Optional Performance Tuning
```bash
DOWNLOAD_MAX_WORKERS=8          # Parallel download workers (1-16)
MAX_CONCURRENT_REQUESTS=50      # Max concurrent Tardis requests
MAX_PARALLEL_UPLOADS=20         # Max parallel GCS uploads
BATCH_SIZE=1000                 # Instruments per batch
MEMORY_THRESHOLD_PERCENT=90     # Memory threshold for batching
```

### Configuration Validation

The system includes comprehensive configuration validation:
- **API Key Format**: Validates Tardis API key format
- **GCP Credentials**: Checks file existence and format
- **Performance Limits**: Validates reasonable ranges for all parameters
- **Error Messages**: Clear, actionable error messages for misconfiguration

## Component Overview

### Core Components

#### 1. CanonicalInstrumentKeyGenerator
- **Purpose**: Generate standardized instrument keys
- **Input**: Exchange symbol information
- **Output**: Canonical instrument keys (VENUE:TYPE:SYMBOL:EXPIRY:OPTION_TYPE)
- **Features**: Type normalization, validation, deduplication

#### 2. DownloadOrchestrator
- **Purpose**: Coordinate data downloads and uploads
- **Features**: Parallel processing, memory monitoring, batch management
- **Performance**: 4.83x download speedup, 1.81x upload speedup

#### 3. InstrumentGCSUploader
- **Purpose**: Upload instrument definitions to GCS
- **Features**: Pydantic validation, single partition strategy
- **Validation**: Comprehensive instrument definition validation

#### 4. DataValidator
- **Purpose**: Validate data completeness and quality
- **Features**: Missing data detection, schema validation
- **Output**: Detailed validation reports

#### 5. InstrumentReader
- **Purpose**: Read and filter instrument definitions from GCS
- **Features**: Date-based filtering, venue/type filtering
- **Performance**: Optimized for single partition queries

### Utility Components

#### 1. Shared GCS Client (`src/utils/gcs_client.py`)
- **Purpose**: Centralized GCS client management
- **Features**: Singleton pattern, connection pooling
- **Benefits**: Reduced authentication overhead, better performance

#### 2. Memory Monitor (`src/utils/memory_monitor.py`)
- **Purpose**: Cross-platform memory monitoring
- **Features**: Threshold detection, usage tracking
- **Platforms**: macOS, Linux support

#### 3. Performance Monitor (`src/utils/performance_monitor.py`)
- **Purpose**: Performance metrics collection
- **Features**: Timing, throughput calculation, export
- **Output**: JSON metrics for analysis

## Deployment Architecture

### Local Development
- **Entry Point**: `python -m market_data_tick_handler.main. --mode <mode>`
- **Configuration**: `.env` files
- **Dependencies**: `requirements.txt`

### Docker Deployment
- **Images**: Separate images for different operations
- **Configuration**: Environment variables
- **Volumes**: Data and logs persistence

### VM Deployment
- **Scripts**: Automated VM creation and management
- **Sharding**: Multiple VMs for parallel processing
- **Monitoring**: Built-in progress tracking and logging

## File Structure

```
src/
├── main.py                    # Centralized entry point
├── models.py                  # Data models and validation
├── instrument_processor/
│   ├── canonical_key_generator.py
│   └── gcs_uploader.py
├── data_downloader/
│   ├── download_orchestrator.py
│   ├── instrument_reader.py
│   └── tardis_connector.py
├── data_validator/
│   └── data_validator.py
└── utils/
    ├── gcs_client.py          # Shared GCS client
    ├── memory_monitor.py      # Memory monitoring
    ├── performance_monitor.py # Performance tracking
    ├── logger.py              # Structured logging
    └── error_handler.py       # Error handling

deploy/
├── local/
│   └── run-main.sh           # Convenience script
└── vm/
    ├── deploy-instruments.sh
    ├── deploy-tardis.sh
    ├── shard-deploy.sh
    └── build-images.sh

docker/
├── instrument-generation/
├── tardis-download/
└── shared/
```

## Key Benefits

1. **Centralized Entry Point**: Single command for all operations
2. **Environment-Based Configuration**: Easy configuration management
3. **Performance Optimized**: 4.83x download, 1.81x upload speedup
4. **Memory Safe**: 90% threshold prevents OOM errors
5. **Connection Efficient**: Shared clients reduce overhead
6. **Docker-Friendly**: Clean containerization
7. **Scalable**: VM deployment with sharding support
8. **Validated**: Comprehensive configuration and data validation

## Performance Metrics

### Real-World Performance
- **End-to-End Pipeline**: 1.31x overall speedup
- **Download Operations**: 4.83x speedup (main bottleneck addressed)
- **Upload Operations**: 1.81x speedup
- **Memory Usage**: 90% threshold prevents crashes
- **Connection Overhead**: Significantly reduced with shared clients

### Scalability
- **Local**: Single machine processing
- **Docker**: Containerized deployment
- **VM**: Multi-VM sharding for large datasets
- **GCS**: Optimized single partition strategy

This architecture provides enterprise-grade performance and reliability for cryptocurrency market data processing at scale.
