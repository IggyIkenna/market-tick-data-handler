# Market Tick Data Handler

A high-performance system for downloading, processing, and storing cryptocurrency tick data from Tardis.dev into Google Cloud Storage, with comprehensive validation framework and real-time streaming integration.

## 🚀 Quick Start

### Prerequisites
- Python 3.11+
- Google Cloud SDK
- Docker
- Tardis.dev API key

### Installation
```bash
# Clone the repository
git clone <repository-url>
cd market-tick-data-handler

# Install dependencies
pip install -r requirements.txt

# Set up environment
cp env.example .env
# Edit .env with your API keys and configuration
```

### Quick Test
```bash
# Test with instrument generation
python -m src.main --mode instruments --start-date 2023-05-23 --end-date 2023-05-23

# Test with data download
python -m src.main --mode download --start-date 2023-05-23 --end-date 2023-05-23 --venues deribit --max-instruments 5
```

## 🎯 Core Operations

The system provides three main operations through a centralized entry point:

### 1. Instrument Generation
Generate canonical instrument definitions and upload to GCS:
```bash
python -m src.main --mode instruments --start-date 2023-05-23 --end-date 2023-05-25
```

### 2. Data Download
Download tick data from Tardis and upload to GCS:
```bash
python -m src.main --mode download --start-date 2023-05-23 --end-date 2023-05-25 --venues deribit
```

### 3. Data Validation
Validate data completeness and check for missing data:
```bash
python -m src.main --mode validate --start-date 2023-05-23 --end-date 2023-05-25
```

### 4. Full Pipeline
Run complete workflow (instruments → download → validate):
```bash
python -m src.main --mode full-pipeline --start-date 2023-05-23 --end-date 2023-05-25
```

## 🚀 Deployment Options

### Local Development
```bash
# Use convenience script
./deploy/local/run-main.sh instruments --start-date 2023-05-23 --end-date 2023-05-25
./deploy/local/run-main.sh download --start-date 2023-05-23 --end-date 2023-05-25
./deploy/local/run-main.sh validate --start-date 2023-05-23 --end-date 2023-05-25
./deploy/local/run-main.sh full-pipeline --start-date 2023-05-23 --end-date 2023-05-25
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

- [Architecture Overview](docs/ARCHITECTURE_OVERVIEW.md) - Current system architecture
- [Main Usage](docs/MAIN_USAGE.md) - Detailed usage instructions
- [Quick Reference](docs/QUICK_REFERENCE.md) - Quick commands and structure
- [Setup Guide](docs/SETUP_GUIDE.md) - Complete setup instructions
- [Deployment Guide](docs/DEPLOYMENT_GUIDE.md) - VM deployment instructions
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
pytest test/

# Run specific test
pytest test/test_timestamp_consistency.py

# Run with coverage
pytest --cov=. test/
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

## 📚 Documentation

- **[Project Specification](docs/PROJECT_SPECIFICATION.md)**: Detailed project overview
- **[Setup Guide](docs/SETUP_GUIDE.md)**: Installation and configuration
- **[Data Retrieval Guide](docs/DATA_RETRIEVAL_GUIDE.md)**: How to retrieve and work with data
- **[Deployment Guide](docs/DEPLOYMENT_GUIDE.md)**: VM deployment and orchestration
- **[API Reference](docs/API_REFERENCE.md)**: REST API documentation
- **[Technical Reference](docs/TECHNICAL_REFERENCE.md)**: Architecture and implementation details

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
```

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
