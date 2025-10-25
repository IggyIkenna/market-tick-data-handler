# Package Usage Guide

This guide shows how to use the Market Data Handler as a package/library for different downstream use cases.

## Overview

The Market Data Handler is a comprehensive cryptocurrency market data infrastructure designed to serve as a clean internal package/library for downstream services. It provides:

- **Historical Data Backfill**: Complete tick data from Tardis.dev with optimized storage
- **Real-Time Streaming**: Node.js-based live data processing with WebSocket connections
- **Processed Candles**: 15s to 24h timeframes with high-frequency trading (HFT) features
- **Order Book Snapshots**: 5-level bid/ask data with 40+ derived features
- **Clean Data Access**: Library interfaces for efficient data retrieval and filtering

### Key Benefits

- **High Performance**: 4.83x download speedup, 1.81x upload speedup
- **Memory Efficient**: 90% threshold prevents OOM errors during large operations
- **Parquet Optimized**: Row group filtering for fast timestamp-based queries
- **Scalable**: VM deployments for batch processing, Node.js for real-time streaming
- **Production Ready**: Comprehensive validation, error handling, and monitoring

## Service Tiers & Architecture

The system provides two distinct service tiers:

### Tier 1: Analytics Services (Batch Historical)
- **Scope**: All instruments
- **TTL**: 1 month data retention
- **Mode**: VM-deployed batch jobs
- **Use Cases**: Analytics, research, comprehensive market analysis
- **Data**: Complete historical datasets for all instruments

### Tier 2: Live/Historical Query & Stream (Package/Library)
- **Scope**: Instrument(s) and underlying filtered info
- **Mode**: Package/library for downstream services
- **Use Cases**: Features service, execution service, backtesting, order book analytics
- **Data**: Filtered, real-time, and historical data for specific instruments
- **Special Features**: Options chain features, underlying instrument filtering
- **Instrument Filtering**: All services filter for specific instruments they need (streaming or historical)

## Complete Data Pipeline & Package Usage

### VM Deployment Pipeline (Batch Processing)

The system runs scheduled VM deployments to maintain data infrastructure:

```bash
# 1. Generate instrument definitions (daily at 8 AM UTC)
python -m market_data_tick_handler.main. --mode instruments --start-date 2023-05-23 --end-date 2023-05-23

# 2. Generate missing data reports (daily at 8 AM UTC)
python -m market_data_tick_handler.main. --mode missing-tick-reports --start-date 2023-05-23 --end-date 2023-05-23

# 3. Download missing tick data (daily at 8 AM UTC)
python -m market_data_tick_handler.main. --mode download --start-date 2023-05-23 --end-date 2023-05-23 --venues binance

# 4. Process candles with HFT features (daily at 8 AM UTC)
python -m market_data_tick_handler.main. --mode candle-processing --start-date 2023-05-23 --end-date 2023-05-23

# 5. Upload candles to BigQuery (daily at 8 AM UTC)
python -m market_data_tick_handler.main. --mode bigquery-upload --start-date 2023-05-23 --end-date 2023-05-23

# 6. Process MFT features (daily at 8 AM UTC)
python -m market_data_tick_handler.main. --mode mft-processing --start-date 2023-05-23 --end-date 2023-05-23
```

### Features Service Package Usage

The features service imports the market data package and uses it in historical mode:

```python
# features_service/main.py
from src.data_client import DataClient, CandleDataReader
from src.bigquery_uploader import BigQueryClient
from config import get_config
from datetime import datetime, timedelta

class FeaturesService:
    def __init__(self):
        # Initialize market data package clients
        self.config = get_config()
        self.data_client = DataClient(self.config.gcp.bucket, self.config)
        self.candle_reader = CandleDataReader(self.data_client)
        self.bq_client = BigQueryClient(self.config.gcp.project_id)
    
    def process_hft_features(self, instrument_id: str, start_date: datetime, end_date: datetime):
        """Process HFT features using market data package"""
        
        # 1. Query instruments from GCS
        instruments = self.data_client.get_instrument_definitions(
            start_date=start_date,
            end_date=end_date
        )
        
        # 2. Get candle data from BigQuery (already has HFT features)
        candles = self.bq_client.get_candles(
            instrument_id=instrument_id,
            timeframe="1m",
            start_date=start_date,
            end_date=end_date
        )
        
        # 3. Process additional MFT features
        mft_features = self._calculate_mft_features(candles)
        
        # 4. Push features to GCS for backtesting
        self.data_client.upload_features(
            features=mft_features,
            path=f"features/{instrument_id.replace(':', '_')}/1m/{start_date.strftime('%Y-%m-%d')}"
        )
        
        return mft_features
    
    def _calculate_mft_features(self, candles):
        """Calculate medium-frequency trading features"""
        # Implementation for MFT feature calculation
        pass
```

### Authentication Setup for Package Usage

#### 1. Service Account Setup

```bash
# Create service account for features service
gcloud iam service-accounts create features-service \
  --display-name="Features Service" \
  --description="Service account for features service package usage"

# Assign required IAM roles
gcloud projects add-iam-policy-binding your-project-id \
  --member="serviceAccount:features-service@your-project-id.iam.gserviceaccount.com" \
  --role="roles/storage.objectViewer"

gcloud projects add-iam-policy-binding your-project-id \
  --member="serviceAccount:features-service@your-project-id.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataViewer"

# Create and download key
gcloud iam service-accounts keys create features-service-key.json \
  --iam-account=features-service@your-project-id.iam.gserviceaccount.com
```

#### 2. Environment Configuration

```bash
# Set environment variables
export GOOGLE_APPLICATION_CREDENTIALS=./features-service-key.json
export GCP_PROJECT_ID=your-project-id
export GCS_BUCKET=market-data-tick
export USE_SECRET_MANAGER=true
# TARDIS_API_KEY is now stored in Secret Manager - no need to set it manually
```

#### 3. Package Integration

```python
# In your features service requirements.txt
market-data-tick-handler>=2.0.0

# In your features service code
from market_data_tick_handler import DataClient, CandleDataReader, BigQueryClient
from market_data_tick_handler.config import get_config
```

### Streaming Service Setup

#### 1. Node.js Dependencies

The streaming service requires Node.js dependencies that are automatically installed:

```bash
# Navigate to streaming directory
cd streaming

# Install dependencies (automatic on first run)
npm install

# Or install manually
npm install tardis-dev @google-cloud/bigquery dotenv
```

#### 2. Environment Configuration

```bash
# Copy environment template
cp streaming.env.example streaming/.env

# Edit streaming/.env with your values
GCP_PROJECT_ID=your-project-id
GOOGLE_APPLICATION_CREDENTIALS=path/to/your/service-account-key.json
BIGQUERY_DATASET=market_data_streaming
# TARDIS_API_KEY is now stored in Secret Manager - no need to set it manually
```

#### 3. Streaming Modes

**Tick Streaming Mode:**
- Streams raw tick data to BigQuery
- Creates table: `market_data_streaming.ticks_binance_btc_usdt`
- Schema: timestamp, local_timestamp, symbol, exchange, price, amount, side, id

**Candle Streaming Mode:**
- Streams real-time candles with HFT features
- Generates 1-minute candles with HFT features
- Displays real-time candle information

#### 4. Usage Examples

```bash
# Stream raw ticks to BigQuery (5 minutes)
python -m market_data_tick_handler.main. --mode streaming-ticks --symbol BTC-USDT --duration 300

# Stream candles with HFT features (infinite)
python -m market_data_tick_handler.main. --mode streaming-candles --symbol BTC-USDT --duration 0

# Multiple symbols (run in separate terminals)
python -m market_data_tick_handler.main. --mode streaming-ticks --symbol ETH-USDT --duration 600
python -m market_data_tick_handler.main. --mode streaming-candles --symbol BTC-USDT --duration 0
```

## Download Modes

The system supports two download modes for different use cases:

### Missing Data Mode (Default)
Only downloads data that is marked as missing in the missing data reports. This is the default behavior and is most efficient for production use:

```bash
# Download only missing data
python -m market_data_tick_handler.main. --mode download --start-date 2023-05-23 --end-date 2023-05-23 --venues binance --instrument-types SPOT_PAIR --data-types trades
```

**Use Cases**:
- Production data backfill
- Daily scheduled jobs
- Efficient resource usage
- Incremental data updates

### Force Download Mode
Downloads all data regardless of existing files. Useful for testing, re-downloading, or when you need to overwrite existing data:

```bash
# Force download all data
python -m market_data_tick_handler.main. --mode download --start-date 2023-05-23 --end-date 2023-05-23 --venues binance --instrument-types SPOT_PAIR --data-types trades --force
```

**Use Cases**:
- Testing and development
- Data quality issues requiring re-download
- Overwriting corrupted files
- Performance testing with known datasets

## Target Audiences & Use Cases

### 1. Features Service
**Needs**: 1m+ candles for feature calculation and model training
- **Data**: Historical candles (1m, 5m, 15m, 1h, 4h, 24h) with OHLCV
- **Features**: Volume-weighted prices, trade counts, basic aggregations
- **Access Pattern**: Batch loading by date range and instrument
- **Scope**: Instrument(s) and underlying filtered info (especially for options features)
- **Mode**: Live and historical query & stream package/library

### 2. Execution Service
**Needs**: 15s candles and HFT features for high-frequency trading
- **Data**: 15s candles with HFT features, order book snapshots
- **Features**: Buy/sell volume, delays, liquidations, derivatives ticker
- **Access Pattern**: Real-time streaming and historical backfill
- **Scope**: Specific trading instruments
- **Mode**: Live and historical query & stream package/library

### 3. Analytics Service
**Needs**: Raw tick data for comprehensive market analysis
- **Data**: All tick data types (trades, book snapshots, liquidations, etc.)
- **Features**: Complete market microstructure data
- **Access Pattern**: Streaming to BigQuery for analytics
- **Scope**: All instruments for comprehensive analysis
- **Mode**: Live streaming to BigQuery

### 4. Backtesting Services
**Needs**: Efficient data retrieval for backtesting scenarios
- **Data**: Tick data, candles, and features based on backtest requirements
- **Features**: Timestamp filtering, memory-efficient loading
- **Access Pattern**: Date range queries with Parquet filtering
- **Scope**: Specific instruments and time periods
- **Mode**: Package/library for data access

## Installation

```bash
# Install the package in development mode
pip install -e .

# Or add to your requirements.txt
git+https://github.com/your-org/market-tick-data-handler.git
```

## HFT Features

The system provides comprehensive High-Frequency Trading (HFT) features calculated from raw tick data. These features capture market microstructure patterns, trading behavior, and execution quality essential for:

- **Execution Algorithms**: Optimizing trade execution timing and strategy
- **Market Making**: Understanding bid-ask dynamics and order flow  
- **Risk Management**: Detecting unusual trading patterns and liquidity conditions
- **Alpha Generation**: Identifying short-term market inefficiencies

For detailed technical specifications, see [HFT Features Specification](HFT_FEATURES_SPECIFICATION.md).

## Quick Start

```python
from src.data_client import DataClient, CandleDataReader
from config import get_config

# Initialize
config = get_config()
data_client = DataClient(config.gcp.bucket, config)
candle_reader = CandleDataReader(data_client)

# Read 1m candles
candles = candle_reader.get_candles(
    instrument_id="BINANCE:SPOT_PAIR:BTC-USDT",
    timeframe="1m",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    end_date=datetime(2024, 1, 2, tzinfo=timezone.utc)
)
```

## Use Cases

### 1. Features Service (1m Candles)

For services that need 1-minute candles for feature calculation:

```python
from src.data_client import DataClient, CandleDataReader
from datetime import datetime, timezone

# Initialize
data_client = DataClient(bucket_name, config)
candle_reader = CandleDataReader(data_client)

# Get 1m candles
candles = candle_reader.get_candles(
    instrument_id="BINANCE:SPOT_PAIR:BTC-USDT",
    timeframe="1m",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    end_date=datetime(2024, 1, 2, tzinfo=timezone.utc)
)

# Calculate features
candles['price_change_pct'] = (candles['close'] - candles['open']) / candles['open'] * 100
candles['volume_ma'] = candles['volume'].rolling(window=20).mean()
```

### 2. Execution Deployment (15s Candles + HFT Features)

For high-frequency trading systems that need 15-second candles with HFT features:

```python
from src.data_client import DataClient, CandleDataReader, HFTFeaturesReader

# Initialize
data_client = DataClient(bucket_name, config)
candle_reader = CandleDataReader(data_client)
hft_reader = HFTFeaturesReader(data_client)

# Get 15s candles
candles_15s = candle_reader.get_candles(
    instrument_id="BINANCE:SPOT_PAIR:BTC-USDT",
    timeframe="15s",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    end_date=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
)

# Get HFT features
hft_features = hft_reader.get_hft_features(
    instrument_id="BINANCE:SPOT_PAIR:BTC-USDT",
    timeframe="15s",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    end_date=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
)

# Merge data
merged_data = pd.merge(candles_15s, hft_features, on=['timestamp', 'symbol', 'exchange'], how='left')

# Use for execution
print(f"Average delay: {merged_data['delay_median'].mean():.2f}ms")
print(f"Total liquidations: {merged_data['liquidation_count'].sum()}")
```

### 3. Analytics (Tick Data Streaming)

For analytics services that need to stream tick data to BigQuery:

#### Option A: Python Package Streaming Mode
```bash
# Stream raw tick data to BigQuery
python -m market_data_tick_handler.main. --mode streaming-ticks --symbol BTC-USDT --duration 300

# Stream real-time candles with HFT features
python -m market_data_tick_handler.main. --mode streaming-candles --symbol BTC-USDT --duration 300
```

#### Option B: Direct Package Usage
```python
from src.data_client import DataClient, TickDataReader

# Initialize
data_client = DataClient(bucket_name, config)
tick_reader = TickDataReader(data_client)

# Stream tick data
for chunk in tick_reader.get_tick_data_streaming(
    instrument_id="BINANCE:SPOT_PAIR:BTC-USDT",
    start_time=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
    end_time=datetime(2024, 1, 1, 12, 5, 0, tzinfo=timezone.utc),
    date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    chunk_size=1000
):
    # Process chunk and send to BigQuery
    process_and_upload_to_bigquery(chunk)
```

### 4. Backtesting - Tick Data Only

For backtesting systems that need efficient tick data retrieval:

```python
from src.data_client import DataClient, TickDataReader

# Initialize
data_client = DataClient(bucket_name, config)
tick_reader = TickDataReader(data_client)

# Define trading windows
trading_windows = [
    (datetime(2024, 1, 1, 8, 0, 0, tzinfo=timezone.utc), datetime(2024, 1, 1, 8, 5, 0, tzinfo=timezone.utc)),
    (datetime(2024, 1, 1, 14, 0, 0, tzinfo=timezone.utc), datetime(2024, 1, 1, 14, 5, 0, tzinfo=timezone.utc))
]

# Load tick data for backtesting
all_tick_data = []
for start_time, end_time in trading_windows:
    for chunk in tick_reader.get_tick_data_streaming(
        instrument_id="BINANCE:SPOT_PAIR:BTC-USDT",
        start_time=start_time,
        end_time=end_time,
        date=datetime(2024, 1, 1, tzinfo=timezone.utc)
    ):
        all_tick_data.append(chunk)

# Combine and use for backtesting
tick_data = pd.concat(all_tick_data, ignore_index=True)
run_backtest(tick_data)
```

### 5. Backtesting - HFT Features Only

For execution deployment backtesting with HFT features:

```python
from src.data_client import DataClient, HFTFeaturesReader

# Initialize
data_client = DataClient(bucket_name, config)
hft_reader = HFTFeaturesReader(data_client)

# Get HFT features for backtesting period
hft_15s = hft_reader.get_hft_features(
    instrument_id="BINANCE:SPOT_PAIR:BTC-USDT",
    timeframe="15s",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    end_date=datetime(2024, 1, 7, tzinfo=timezone.utc)
)

hft_1m = hft_reader.get_hft_features(
    instrument_id="BINANCE:SPOT_PAIR:BTC-USDT",
    timeframe="1m",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    end_date=datetime(2024, 1, 7, tzinfo=timezone.utc)
)

# Use for backtesting
run_execution_backtest(hft_15s, hft_1m)
```

### 6. Backtesting - MFT Features Only

For features deployment backtesting with MFT features:

```python
from src.data_client import DataClient, MFTFeaturesReader

# Initialize
data_client = DataClient(bucket_name, config)
mft_reader = MFTFeaturesReader(data_client)

# Get MFT features for backtesting
mft_features = mft_reader.get_mft_features(
    instrument_id="BINANCE:SPOT_PAIR:BTC-USDT",
    timeframe="1h",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    end_date=datetime(2024, 1, 31, tzinfo=timezone.utc)
)

# Use for backtesting
run_features_backtest(mft_features)
```

## Data Processing

### Historical Candle Processing

Process historical tick data into candles:

```python
from src.candle_processor import HistoricalCandleProcessor, ProcessingConfig

# Configure processor
config = ProcessingConfig(
    timeframes=['15s', '1m'],
    enable_hft_features=True,
    enable_book_snapshots=True,
    data_types=['trades', 'book_snapshot_5', 'derivative_ticker', 'liquidations']
)

processor = HistoricalCandleProcessor(data_client, config)

# Process a day
result = await processor.process_day(
    instrument_id="BINANCE:SPOT_PAIR:BTC-USDT",
    date=datetime(2024, 1, 1, tzinfo=timezone.utc)
)
```

### Candle Aggregation

Aggregate 1m candles into higher timeframes:

```python
from src.candle_processor import AggregatedCandleProcessor, AggregationConfig

# Configure aggregator
config = AggregationConfig(
    timeframes=['5m', '15m', '1h', '4h', '24h'],
    enable_hft_features=True
)

aggregator = AggregatedCandleProcessor(data_client, config)

# Process a day
result = await aggregator.process_day(
    instrument_id="BINANCE:SPOT_PAIR:BTC-USDT",
    date=datetime(2024, 1, 1, tzinfo=timezone.utc)
)
```

### BigQuery Upload

Upload processed candles to BigQuery:

```python
from src.bigquery_uploader import CandleUploader, UploadConfig

# Configure upload
config = UploadConfig(
    project_id="your-project",
    dataset_id="market_data_candles",
    timeframes=['15s', '1m', '5m', '15m', '1h', '4h', '24h']
)

uploader = CandleUploader(data_client, config)

# Upload a day
result = await uploader.upload_day(
    date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    overwrite=False
)
```

## Available Data Types

### Candles
- **Timeframes**: 15s, 1m, 5m, 15m, 1h, 4h, 24h
- **Features**: OHLCV, VWAP, trade count
- **HFT Features**: Buy/sell volume, size avg, delays, liquidations, derivatives ticker
- **Book Snapshots**: 5 levels of bid/ask prices and volumes with derived features

### Tick Data
- **Types**: trades, book_snapshot_5, derivative_ticker, liquidations, options_chain
- **Filtering**: Timestamp-based filtering with Parquet predicate pushdown
- **Streaming**: Memory-efficient chunked reading for large datasets

### Features
- **HFT Features**: Available for 15s and 1m timeframes
- **MFT Features**: Available for 1m and higher timeframes
- **Book Features**: Raw and derived order book features

## Performance Considerations

### Parquet Filtering
The package leverages Parquet's built-in row group statistics for efficient timestamp filtering:

```python
# Basic filtering (loads entire file, then filters)
tick_data = tick_reader.get_tick_data(
    instrument_id="BINANCE:SPOT_PAIR:BTC-USDT",
    start_time=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
    end_time=datetime(2024, 1, 1, 12, 5, 0, tzinfo=timezone.utc),
    date=datetime(2024, 1, 1, tzinfo=timezone.utc)
)

# Optimized filtering (uses predicate pushdown - recommended for sparse data)
tick_data = tick_reader.get_tick_data_optimized(
    instrument_id="BINANCE:SPOT_PAIR:BTC-USDT",
    start_time=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
    end_time=datetime(2024, 1, 1, 12, 5, 0, tzinfo=timezone.utc),
    date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    use_predicate_pushdown=True
)
```

### Sparse Data Access for Backtesting
For backtesting scenarios where you only need data at specific times:

```python
# Get data for specific candle times only (e.g., every 5-20 candles)
candle_times = [
    datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
    datetime(2024, 1, 1, 12, 15, 0, tzinfo=timezone.utc),
    datetime(2024, 1, 1, 12, 30, 0, tzinfo=timezone.utc),
    # ... more specific times
]

sparse_data = tick_reader.get_sparse_candles(
    instrument_id="BINANCE:SPOT_PAIR:BTC-USDT",
    candle_times=candle_times,
    date=datetime(2024, 1, 1, tzinfo=timezone.utc).date(),
    data_types=['trades'],
    buffer_minutes=2  # 2-minute buffer around each candle
)

# Result: Dictionary mapping candle_time -> DataFrame
for candle_time, df in sparse_data.items():
    print(f"Candle {candle_time}: {len(df)} records")
```

### Byte Range Analysis
Analyze data distribution for optimization:

```python
# Get byte ranges for efficient data access
ranges = tick_reader.get_sparse_data_ranges(
    instrument_id="BINANCE:SPOT_PAIR:BTC-USDT",
    date=datetime(2024, 1, 1, tzinfo=timezone.utc).date(),
    data_types=['trades'],
    time_partition_minutes=1  # 1-minute partitions
)

# Result: Dictionary mapping data_type -> list of (start_time, end_time, byte_start, byte_end)
for data_type, partitions in ranges.items():
    print(f"{data_type}: {len(partitions)} partitions")
    for start_time, end_time, byte_start, byte_end in partitions[:3]:
        print(f"  {start_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')}: {byte_end - byte_start:,} bytes")
```

### Memory Efficiency
Use streaming readers for large datasets:

```python
# Stream data in chunks to avoid memory issues
for chunk in tick_reader.get_tick_data_streaming(
    instrument_id="BINANCE:SPOT_PAIR:BTC-USDT",
    start_time=start_time,
    end_time=end_time,
    date=date,
    chunk_size=10000
):
    process_chunk(chunk)
```

### Concurrent Processing
The BigQuery uploader supports concurrent processing:

```python
orchestration_config = OrchestrationConfig(
    max_concurrent_days=5,
    max_concurrent_timeframes=3,
    retry_failed_days=True
)
```

## Error Handling

All components include comprehensive error handling:

```python
try:
    candles = candle_reader.get_candles(instrument_id, timeframe, start_date, end_date)
except Exception as e:
    logger.error(f"Failed to read candles: {e}")
    # Handle error appropriately
```

## Configuration

The package uses the same configuration system as the main application:

```python
from config import get_config

config = get_config()
data_client = DataClient(config.gcp.bucket, config)
```

## Examples

See `examples/package_usage_examples.py` for complete working examples of all use cases.

## Support

For questions or issues, please refer to the main documentation or contact the development team.
