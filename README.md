# Market Tick Data Handler

<<<<<<< Current (Your changes)
A high-performance system for downloading, processing, and storing cryptocurrency tick data from Tardis.dev into Google Cloud Storage. Now refactored as a clean internal package/library for downstream services.
=======
A high-performance system for downloading, processing, and storing cryptocurrency tick data from Tardis.dev into Google Cloud Storage, with comprehensive validation framework and real-time streaming integration.
>>>>>>> Incoming (Background Agent changes)

## 🚀 Quick Installation

### For External Users (Recommended)
```bash
# One-command installation with all dependencies
curl -sSL https://raw.githubusercontent.com/iggyikenna/market-tick-data-handler/main/install.sh | bash

# Or clone and install locally
git clone https://github.com/iggyikenna/market-tick-data-handler.git
cd market-tick-data-handler
./install.sh --local
```

### For Developers (No GCP Required)
```bash
# 1. Clone and setup
git clone https://github.com/iggyikenna/market-tick-data-handler.git
cd market-tick-data-handler
pip install -e .

# 2. Choose "Mock Data Mode" for testing without GCP
# 3. Run performance test
python examples/standalone_performance_test.py
```

### For Production (With GCP Access)
```bash
# 1. Clone and setup
git clone https://github.com/iggyikenna/market-tick-data-handler.git
cd market-tick-data-handler
pip install -e .

# 2. Choose "Restore Credentials" or use your own
# 3. Run real data test
python examples/performance_comparison_test.py
```

### Package Installation (For Downstream Services)
```bash
# Install as Python package from GitHub (recommended)
pip install git+https://github.com/iggyikenna/market-tick-data-handler.git

# Or install in development mode
git clone https://github.com/iggyikenna/market-tick-data-handler.git
cd market-tick-data-handler
pip install -e .

# Install with development dependencies
pip install -e .[dev]

# Install with streaming capabilities
pip install -e .[streaming]

# Install everything
pip install -e .[dev,streaming]
```

See [Package Installation Guide](PACKAGE_INSTALLATION.md) for detailed instructions.

### Prerequisites
- Python 3.8+ (3.11+ recommended)
- Google Cloud SDK (for production)
- Docker (optional)
- Tardis.dev API key (stored in Google Cloud Secret Manager)
- Git access to this private repository

### Local Development Installation
```bash
# Clone the repository
git clone <repository-url>
cd market-tick-data-handler

# Run automated setup
./scripts/setup-dev.sh

# Or manual setup
pip install -r requirements.txt
cp env.example .env
# Edit .env with your API keys and configuration
```

### Quick Test
```bash
# Test with mock data (no GCP required)
python examples/standalone_performance_test.py

# Test with real data (requires GCP)
python examples/performance_comparison_test.py

# Test with data download
python -m market_data_tick_handler.main. --mode download --start-date 2023-05-23 --end-date 2023-05-23 --venues deribit --max-instruments 5

# Inspect instrument definitions
python examples/inspect_instrument.py BINANCE-FUTURES:PERPETUAL:SOL-USDT
```

### Authentication Setup
The package supports multiple authentication modes for different use cases:

```bash
# Production (Secret Manager + GCS)
./scripts/setup-auth.sh production your-project market-data-prod

# Development (Environment variables + GCS)
./scripts/setup-auth.sh development your-dev-project market-data-dev

# Read-only (GCS only)
./scripts/setup-auth.sh readonly your-project market-data-readonly

# Mock/Offline (No GCP access)
./scripts/setup-auth.sh mock
```

See [Authentication Strategy](docs/AUTHENTICATION_STRATEGY.md) for detailed setup instructions.

### Sparse Data Access for Backtesting
The package includes optimized Parquet reading for sparse data access:

```python
# Get data for specific candle times only (e.g., every 5-20 candles)
candle_times = [
    datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
    datetime(2024, 1, 1, 12, 15, 0, tzinfo=timezone.utc),
    # ... more specific times
]

sparse_data = tick_reader.get_sparse_candles(
    instrument_id="BINANCE:SPOT_PAIR:BTC-USDT",
    candle_times=candle_times,
    date=datetime(2024, 1, 1, tzinfo=timezone.utc).date(),
    buffer_minutes=2
)
```

**Benefits**:
- **95-98% reduction** in data transfer for sparse queries
- **10x faster** loading for backtesting scenarios
- **90% less memory** usage for large datasets

See [Parquet Optimization Strategy](docs/PARQUET_OPTIMIZATION_STRATEGY.md) for technical details.

## 🎯 Core Operations

The system provides multiple operations through a centralized entry point:

### Package/Library Usage
The system can be used as a package for downstream services:

#### Historical Data Access
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

#### Real-Time Streaming (Unified v2.0.0)
```bash
# NEW: Unified Streaming Architecture

# Stream raw tick data to BigQuery (8 data types with fallbacks) 
python -m market_data_tick_handler.main --mode streaming-ticks-bigquery --symbol BTC-USDT --duration 300

# Stream candles + HFT features for downstream services (importable)
python -m market_data_tick_handler.main --mode streaming-candles-serve --symbol BTC-USDT --duration 0

# Stream candles + HFT features to BigQuery for analytics
python -m market_data_tick_handler.main --mode streaming-candles-bigquery --symbol BTC-USDT --duration 0

# Sync live instrument definitions from CCXT (8 exchanges)
python -m market_data_tick_handler.main --mode live-instruments-sync --exchanges binance,deribit
```

#### Package Integration (NEW)
```python
# Import unified streaming components
from market_data_tick_handler.streaming_service import (
    LiveFeatureStream,      # Consumer interface
    LiveInstrumentProvider, # Live CCXT instruments  
    HFTFeatureCalculator   # Unified features (historical + live)
)

# Consume live features in downstream service
async with LiveFeatureStream(symbol="BTC-USDT", timeframe="1m") as stream:
    async for candle_with_features in stream:
        execute_strategy(candle_with_features)
```

See [Package Usage Guide](docs/PACKAGE_USAGE.md) for detailed examples.

## 🏗️ Architecture

The system has been refactored into a clean package/library architecture:

### Package Components
- **`src/data_client/`**: Clean data access interfaces for reading processed data
- **`src/candle_processor/`**: Historical and aggregated candle processing
- **`src/bigquery_uploader/`**: BigQuery batch upload functionality
- **`src/streaming_service/`**: Live streaming components (Node.js primary)

### Use Cases
1. **Features Service**: Gets 1m candles for feature calculation
2. **Execution Deployment**: Gets 15s candles and HFT features for high-frequency trading
3. **Analytics**: Streams tick data to BigQuery for analytics
4. **Backtesting**: Efficient data retrieval for backtesting scenarios

### Deployment Modes
- **VM Deployments**: Batch processing jobs (instruments, downloads, candles, BigQuery uploads)
- **Package Usage**: Direct import by downstream services
- **Live Streaming**: Node.js services for real-time data processing

### Batch Processing Operations

#### 1. Instrument Generation
Generate canonical instrument definitions and upload to GCS:
```bash
python -m market_data_tick_handler.main. --mode instruments --start-date 2023-05-23 --end-date 2023-05-25
```

#### 2. Data Download
Download tick data from Tardis and upload to GCS:
```bash
python -m market_data_tick_handler.main. --mode download --start-date 2023-05-23 --end-date 2023-05-25 --venues deribit
```

#### 3. Candle Processing
Process historical tick data into candles with HFT features:
```bash
python -m market_data_tick_handler.main. --mode candle-processing --start-date 2024-01-01 --end-date 2024-01-01
```

#### 4. BigQuery Upload
Upload processed candles to BigQuery for analytics:
```bash
python -m market_data_tick_handler.main. --mode bigquery-upload --start-date 2024-01-01 --end-date 2024-01-01
```

#### 5. Data Validation
Validate data completeness and check for missing data:
```bash
python -m market_data_tick_handler.main. --mode validate --start-date 2023-05-23 --end-date 2023-05-25
```

### 4. Full Pipeline
Run complete workflow (instruments → download → validate):
```bash
python -m market_data_tick_handler.main. --mode full-pipeline-ticks --start-date 2023-05-23 --end-date 2023-05-25
```

## 🚀 Deployment Options

### Local Development
```bash
# Use convenience script
./deploy/local/run-main.sh instruments --start-date 2023-05-23 --end-date 2023-05-25
./deploy/local/run-main.sh download --start-date 2023-05-23 --end-date 2023-05-25
./deploy/local/run-main.sh validate --start-date 2023-05-23 --end-date 2023-05-25
./deploy/local/run-main.sh full-pipeline-ticks --start-date 2023-05-23 --end-date 2023-05-25
```

### VM Deployment
```bash
# Single VM for development/testing
./deploy/vm/deploy-instruments.sh deploy
./deploy/vm/deploy-tardis.sh deploy

# Multiple VMs for production
./deploy/vm/shard-deploy.sh instruments --start-date 2023-05-23 --end-date 2023-05-25 --shards 10
./deploy/vm/shard-deploy.sh tardis --start-date 2023-05-23 --end-date 2023-05-25 --shards 20
```

## 📊 Data Organization

The system uses a single partition strategy for optimal performance:

```
gs://market-data-tick/
├── instrument_availability/
│   └── by_date/
│       └── day-{date}/
│           └── instruments.parquet
└── raw_tick_data/
    └── by_date/
        └── day-{date}/
            ├── data_type-trades/
            │   └── {instrument_key}.parquet
            └── data_type-book_snapshot_5/
                └── {instrument_key}.parquet
```

## 📚 Documentation

- [Downstream Usage Guide](docs/DOWNSTREAM_USAGE.md) - **Primary guide for teams consuming this as a package/library**
- [Secret Manager Setup](docs/SECRET_MANAGER_SETUP.md) - **Secure API key management with Google Cloud Secret Manager**
- [Architecture Overview](docs/ARCHITECTURE.md) - System architecture details
- [Authentication Strategy](docs/AUTHENTICATION_STRATEGY.md) - **Multi-tier authentication for different use cases**
- [Setup Guide](docs/SETUP_GUIDE.md) - Complete setup instructions
- [Deployment Guide](docs/DEPLOYMENT_GUIDE.md) - VM deployment instructions
- [Package Usage](docs/PACKAGE_USAGE.md) - Detailed API reference
- [HFT Features Specification](docs/HFT_FEATURES_SPECIFICATION.md) - **Technical specification of high-frequency trading features**
- [Parquet Optimization Strategy](docs/PARQUET_OPTIMIZATION_STRATEGY.md) - **Efficient sparse data access for backtesting**
- [Instrument Key Specification](docs/INSTRUMENT_KEY.md) - Canonical instrument key format

## 🔧 Configuration

Create a `.env` file with your configuration:
```bash
# Copy environment template
cp env.example .env

# Edit .env with your actual values
# Required: TARDIS_API_KEY, GCP_PROJECT_ID, GCS_BUCKET
```

### Method 1: Local Python Execution (No Docker)
**Best for**: Development, debugging, quick testing

```bash
# Activate virtual environment (if using one)
source venv_local/bin/activate

# Run instrument generation directly
python run_fixed_local_instrument_generation.py
```

**Output**: 
- ✅ Processes 3 days of data (2023-05-23 to 2023-05-25)
- ✅ Generates ~4,066 instruments
- ✅ Uploads to GCS with proper partitioning
- ✅ Creates aggregate files

### Method 2: Docker Script (Recommended)
**Best for**: Consistent environment, easy execution

```bash
# Build Docker image (first time only)
./deploylocal/build-all.sh

# Run instrument generation in Docker
./deploylocal/run-instrument-generation.sh
```

**Features**:
- ✅ Automatic Docker image building
- ✅ Environment file validation
- ✅ Volume mounting for data persistence
- ✅ Error handling and logging

### Method 3: Docker Compose
**Best for**: Development with docker-compose, service orchestration

```bash
# Navigate to Docker directory
cd docker/instrument-generation

# Run with docker-compose
docker compose up --build instrument-generator
```

**Features**:
- ✅ Service definition in docker-compose.yml
- ✅ Environment variable management
- ✅ Network isolation
- ✅ Volume management

### Method 4: Direct Docker Run
**Best for**: Advanced users, custom configurations

```bash
# Build image
docker build -f docker/instrument-generation/Dockerfile -t market-tick-instrument-generator:latest .

# Run container
docker run --env-file .env \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/logs:/app/logs \
  -v $(pwd)/temp:/app/temp \
  market-tick-instrument-generator:latest
```

### 📊 Output Verification

All methods produce **identical results**:

| **Metric** | **Value** |
|------------|-----------|
| **Days Processed** | 3 (2023-05-23 to 2023-05-25) |
| **Total Instruments** | ~4,066 |
| **Exchanges** | Deribit |
| **Instrument Types** | SPOT_PAIR, FUTURE, OPTION |
| **GCS Partitions** | 6 per day (by_date, by_venue, by_type, aggregated) |
| **Errors** | 0 |

### 🔍 Monitoring and Debugging

#### Check Output Files
```bash
# View generated data
ls -la data/

# Check logs
ls -la logs/

# View temporary files
ls -la temp/
```

#### Docker-Specific Debugging
```bash
# Check Docker images
docker images | grep market-tick

# Check running containers
docker ps

# View container logs
docker logs market-tick-instrument-generator

# Run container interactively
docker run -it --env-file .env market-tick-instrument-generator:latest /bin/bash
```

#### GCS Verification
```bash
# Check uploaded files
gsutil ls gs://market-data-tick/instrument_availability/

# Download a sample file
gsutil cp gs://market-data-tick/instrument_availability/instruments_20230523.parquet ./
```

### ⚡ Performance Comparison

| **Method** | **Setup Time** | **Execution Time** | **Best For** |
|------------|----------------|-------------------|--------------|
| **Local Python** | 0s | ~2-3 minutes | Development, debugging |
| **Docker Script** | 30s (first build) | ~2-3 minutes | Production-like testing |
| **Docker Compose** | 30s (first build) | ~2-3 minutes | Service development |
| **Direct Docker** | 30s (first build) | ~2-3 minutes | Advanced users |

### 🛠️ Troubleshooting

#### Common Issues

1. **Missing .env file**
   ```bash
   cp env.example .env
   # Edit .env with your values
   ```

2. **Docker not running**
   ```bash
   # Start Docker Desktop
   # Check with: docker info
   ```

3. **Permission denied**
   ```bash
   chmod +x deploylocal/*.sh
   ```

4. **Missing GCP credentials**
   ```bash
   # Ensure central-element-323112-e35fb0ddafe2.json exists
   ls -la central-element-323112-e35fb0ddafe2.json
   ```

5. **Build failures**
   ```bash
   # Clean Docker cache
   docker system prune -a
   
   # Rebuild
   ./deploylocal/build-all.sh
   ```

#### Debug Commands
```bash
# Check environment variables
cat .env

# Test Tardis API connection
python -c "from config import get_config; print(get_config().tardis.api_key[:10] + '...')"

# Test GCP connection
python -c "from google.cloud import storage; print('GCP OK')"

# Check Docker build context
docker build --no-cache -f docker/instrument-generation/Dockerfile -t test-build .
```

## 📁 Repository Structure

```
market-tick-data-handler/
├── 📁 deploy/                    # Deployment scripts and configurations
│   ├── orchestration/           # VM orchestration scripts
│   ├── docker-build.sh         # Docker build and push script
│   └── setup-iam.sh            # IAM permissions setup
├── 📁 test/                     # Test suite
│   ├── test_timestamp_consistency.py
│   ├── test_timestamp_validation.py
│   └── README.md
├── 📁 deploy                  # Utility and test scripts
│   ├── vm_data_downloader.py   # Main VM download script
│   ├── test_*.py               # Various test scripts
│   └── download_*.py           # Sample download scripts
├── 📁 docs/                     # Documentation
│   ├── PROJECT_SPECIFICATION.md
│   ├── SETUP_GUIDE.md
│   ├── DATA_RETRIEVAL_GUIDE.md
│   └── ...
├── 📄 Core Python modules
│   ├── src/
│   │   ├── models.py           # Data models and schemas
│   ├── config.py              # Configuration management
│   ├── instrument_registry.py # Instrument definitions
│   ├── tardis_connector.py    # Tardis.dev API client
│   ├── gcs_upload_service.py  # GCS upload service
│   └── ...
└── 📄 Configuration files
    ├── requirements.txt
    ├── Dockerfile
    └── .env.example
```

## 🏗️ Architecture

### Data Flow
1. **Download**: Fetch tick data from Tardis.dev API
2. **Process**: Convert to Parquet format with proper typing
3. **Store**: Upload to GCS with optimized single partition structure
4. **Query**: Retrieve data for analysis and backtesting

### Optimized Single Partition Strategy
- **Instrument Definitions**: Single aggregated file per day (`by_date/day-{date}/instruments.parquet`)
- **Tick Data**: One file per instrument per day (`by_date/day-{date}/data_type-{type}/{instrument_key}.parquet`)
- **Performance**: 10-30x faster queries for common use cases
- **Storage**: 66% reduction in total storage vs triple partition

### Data Types
- **Trades**: Individual trade executions
- **Book Snapshots**: Order book snapshots (top 5 levels)
- **Derivative Ticker**: Funding rates, mark prices, open interest
- **Liquidations**: Liquidation events

## 🚀 Deployment

### 1. Build and Push Docker Image
```bash
# Build and push to GCR
./deploy/docker-build.sh latest
```

### 2. Deploy VMs for Data Download
```bash
# Single day download (60 VMs)
./deploy/orchestration/orchestrate-tick-download.sh \
  --start-date 2023-05-23 \
  --end-date 2023-05-23

# Multi-day download
./deploy/orchestration/orchestrate-tick-download.sh \
  --start-date 2023-05-23 \
  --end-date 2023-05-25 \
  --instances 3
```

### 3. Monitor Progress
```bash
# Check VM status
./deploy/orchestration/monitor-tick-download.sh

# Check GCS uploads
gsutil ls gs://market-data-tick/data/2023-05-23/
```

### 4. Cleanup
```bash
# Clean up VMs
./deploy/orchestration/cleanup-tick-vms.sh
```

## 📊 Data Retrieval

### Optimized GCS Structure
```
gs://market-data-tick/
├── instrument_availability/
│   └── by_date/
│       └── day-2023-05-23/
│           └── instruments.parquet (3MB)
└── raw_tick_data/
    └── by_date/
        └── day-2023-05-23/
            ├── data_type-trades/
            │   ├── BINANCE:SPOT_PAIR:BTC-USDT.parquet (2GB)
            │   ├── BINANCE:SPOT_PAIR:ETH-USDT.parquet (1.5GB)
            │   └── DERIBIT:PERP:BTC-USDT.parquet (1.8GB)
            └── data_type-book_snapshot_5/
                ├── BINANCE:SPOT_PAIR:BTC-USDT.parquet (500MB)
                ├── BINANCE:SPOT_PAIR:ETH-USDT.parquet (400MB)
                └── DERIBIT:PERP:BTC-USDT.parquet (450MB)
```

### Example Queries
```bash
# List available data
gsutil ls gs://market-data-tick/raw_tick_data/by_date/day-2023-05-23/

# Download specific instrument trades
gsutil cp gs://market-data-tick/raw_tick_data/by_date/day-2023-05-23/data_type-trades/BINANCE:SPOT_PAIR:BTC-USDT.parquet ./

# Download all trades for a date
gsutil -m cp -r gs://market-data-tick/raw_tick_data/by_date/day-2023-05-23/data_type-trades/ ./

# Download instrument definitions
gsutil cp gs://market-data-tick/instrument_availability/by_date/day-2023-05-23/instruments.parquet ./
```

### Python Integration
```python
from google.cloud import storage
import pandas as pd

# Download and load tick data
client = storage.Client()
bucket = client.bucket('market-data-tick')

# Get BTC-USDT trades for 2023-05-23
blob = bucket.blob('raw_tick_data/by_date/day-2023-05-23/data_type-trades/BINANCE:SPOT_PAIR:BTC-USDT.parquet')
blob.download_to_filename('btc_trades.parquet')
df = pd.read_parquet('btc_trades.parquet')

# Get instrument definitions for 2023-05-23
blob = bucket.blob('instrument_availability/by_date/day-2023-05-23/instruments.parquet')
blob.download_to_filename('instruments.parquet')
instruments = pd.read_parquet('instruments.parquet')
```

## 🔍 Validation Framework

### Comprehensive Data Validation

The system includes a robust validation framework for ensuring data quality and consistency:

- **Cross-Source Validation**: Compare Binance vs Tardis data
- **Timestamp Stability**: Validate timestamp consistency and ordering
- **Aggregation Consistency**: Verify candle aggregation correctness
- **Real-time Streaming Validation**: Validate streaming data in real-time

### Quick Validation Test

```bash
# Run all validation tests
python run_validation_tests.py --test-type all

# Run specific validation types
python run_validation_tests.py --test-type cross-source --symbol BTC-USDT --timeframe 1m
python run_validation_tests.py --test-type timestamp --data-file data.parquet
python run_validation_tests.py --test-type aggregation --base-file 1m.parquet --agg-file 5m.parquet
```

### Streaming Integration

Real-time validation integration with the unified streaming architecture:

```python
from src.validation.streaming_integration import StreamingServiceValidator

# Create streaming validator
validator = StreamingServiceValidator(
    cross_source_validator=cross_source_validator,
    timestamp_validator=timestamp_validator,
    aggregation_validator=aggregation_validator
)

# Start validator
await validator.start()

# Validate streaming candle
result = await validator.validate_candle(candle, "BTC-USDT", "1m")
```

### Validation Documentation

- [Validation Framework Guide](docs/VALIDATION_FRAMEWORK_README.md)
- [Streaming Integration Guide](docs/STREAMING_VALIDATION_INTEGRATION.md)
- [Testing Analysis](docs/TESTING_ANALYSIS.md)

## 🧪 Testing

### Run Tests
```bash
# Run all tests
pytest tests/

# Run specific test categories
pytest tests/unit/ -v
pytest tests/integration/ -v
pytest tests/performance/ -v

# Run with coverage
pytest --cov=src tests/
```

### Quality Gates
```bash
# Run quality gates (used in deployment)
python tests/run_quality_gates.py

# Deploy with quality gates (blocks if tests fail)
./deploy/local/run-main.sh [MODE] --run-quality-gates [OPTIONS]

# Examples
./deploy/local/run-main.sh check-gaps --run-quality-gates --start-date 2023-05-23 --end-date 2023-05-25
./deploy/local/run-main.sh instruments --run-quality-gates --start-date 2023-05-23 --end-date 2023-05-25
```

### Test Scripts
```bash
# Test Tardis connection
python deploytest_tardis_simple.py

# Test data download
python deploytest_basic.py

# Test comprehensive pipeline
python deploytest_comprehensive.py
```

### Quality Gates Integration
The system includes comprehensive quality gates that can be integrated into deployment:

- **Optional**: Quality gates only run when `--run-quality-gates` flag is used
- **Blocking**: Deployment is completely blocked if quality gates fail
- **Comprehensive**: Includes dependency checks, environment validation, GCS connectivity, Tardis API validation, and test execution
- **Clear Feedback**: Detailed output showing what's being checked and why deployment failed

For detailed testing documentation, see [tests/README.md](tests/README.md).

## 📚 Documentation

- **[Project Specification](docs/PROJECT_SPECIFICATION.md)**: Detailed project overview
- **[Setup Guide](docs/SETUP_GUIDE.md)**: Installation and configuration
- **[Data Retrieval Guide](docs/DATA_RETRIEVAL_GUIDE.md)**: How to retrieve and work with data
- **[Deployment Guide](docs/DEPLOYMENT_GUIDE.md)**: VM deployment and orchestration
- **[API Reference](docs/API_REFERENCE.md)**: REST API documentation
- **[Technical Reference](docs/TECHNICAL_REFERENCE.md)**: Architecture and implementation details

## 🛠️ Examples

The `examples/` directory contains utility scripts and examples:

- **[inspect_instrument.py](examples/inspect_instrument.py)**: Command-line tool to inspect instrument definitions from GCS
- **[examples/README.md](examples/README.md)**: Detailed documentation for example scripts

### Quick Instrument Inspection
```bash
# Inspect any instrument by ID
python examples/inspect_instrument.py BINANCE-FUTURES:PERPETUAL:SOL-USDT

# Show summary of all instruments for a date
python examples/inspect_instrument.py BINANCE:SPOT_PAIR:BTC-USDT --summary

# Use different date
python examples/inspect_instrument.py DERIBIT:OPTION:BTC-USD-241225-50000-CALL --date 2023-05-24
```

## 🔧 Configuration

### Environment Variables
```bash
# Required
TARDIS_API_KEY=your_tardis_api_key
GCP_PROJECT_ID=your_project_id
GCS_BUCKET=market-data-tick

# Optional
GCP_CREDENTIALS_PATH=path/to/credentials.json
LOG_LEVEL=INFO
DOWNLOAD_MAX_WORKERS=2        # 2 workers for Tardis download (leaves 2 vCPUs for system)
```

### VM Configuration
For production VM deployments:
- **Machine Type**: `e2-highmem-8` (8 vCPU, 64GB RAM)
- **Workers**: 2 (optimal for parallel downloads while reserving system resources)
- **Memory**: 64GB allows parallel processing of large parquet files

### Instrument Configuration
The system supports 60 instruments:
- 20 USDT spot pairs (BTC-USDT, ETH-USDT, etc.)
- 20 USD spot pairs (BTC-USD, ETH-USD, etc.)
- 20 perpetual futures (BTCUSDT, ETHUSDT, etc.)

## 🚨 Troubleshooting

### Common Issues
1. **VM not downloading data**: Check IAM permissions and API keys
2. **Docker build fails**: Ensure all dependencies are installed
3. **GCS upload fails**: Verify bucket permissions and credentials
4. **Memory issues**: Use smaller date ranges or more VMs

### Monitoring
- Check VM logs: `gcloud compute instances get-serial-port-output tick-data-vm-0 --zone=asia-northeast1-a`
- Monitor GCS uploads: `gsutil ls gs://market-data-tick/data/`
- Check Docker logs: `docker logs <container_id>`

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For issues and questions:
1. Check the troubleshooting guide
2. Review the documentation
3. Open an issue on GitHub
4. Contact the development team

---

**Note**: This system is designed for high-frequency trading data processing. Ensure you have appropriate API rate limits and storage costs in mind when scaling up.
