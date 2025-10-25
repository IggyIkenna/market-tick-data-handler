# Market Data Tick Handler - Setup Guide

**Version**: 2.0.0 (Refactored Package Architecture)  
**Last Updated**: December 2024

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [Configuration](#configuration)
4. [Local Development](#local-development)
5. [Package Usage](#package-usage)
6. [Live Streaming Setup](#live-streaming-setup)
7. [Docker Setup](#docker-setup)
8. [VM Deployment](#vm-deployment)
9. [Verification](#verification)
10. [Troubleshooting](#troubleshooting)

## Prerequisites

### System Requirements
- **Python**: 3.9 or higher
- **Memory**: 8GB RAM minimum (50GB for production VMs)
- **Storage**: 100GB disk space (for production VMs)
- **Network**: Stable internet connection with good bandwidth

### Required Accounts
- **Tardis.dev**: API key for data access
- **Google Cloud Platform**: Service account with GCS access
- **Git**: For cloning the repository

### Required Tools
- **Git**: For version control
- **Docker**: For containerization (optional)
- **gcloud CLI**: For Google Cloud operations
- **Python pip**: For package management

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/your-org/market-data-tick-handler.git
cd market-data-tick-handler
```

### 2. Create Virtual Environment

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Linux/Mac:
source venv/bin/activate
# On Windows:
venv\Scripts\activate
```

### 3. Install Dependencies

```bash
# Install from requirements.txt
pip install -r requirements.txt

# Or install in development mode
pip install -e .
```

### 4. Verify Installation

```bash
# Check if package is installed
python -c "import market_data_tick_handler; print('Installation successful')"

# Run basic tests
python -m pytest tests/test_basic.py -v
```

## Configuration

### 1. Environment Variables

Create a `.env` file in the project root:

```bash
# Tardis API Configuration
TARDIS_API_KEY=TD.l6pTDHIcc9fwJZEz.Y7cp7lBSu-pkPEv.55-ZZYvZqtQL7hY.C2-

# Google Cloud Configuration
GCP_PROJECT_ID=central-element-323112
GCP_CREDENTIALS_PATH=central-element-323112-e35fb0ddafe2.json
GCS_BUCKET=market-data-tick
GCS_REGION=asia-northeast1-c

# Service Configuration
LOG_LEVEL=INFO
LOG_DESTINATION=local
MAX_CONCURRENT_REQUESTS=2
RATE_LIMIT_PER_VM=1000000

# Sharding Configuration (for VM deployment)
SHARD_INDEX=0
TOTAL_SHARDS=30
```

### 2. Google Cloud Setup

#### Create Service Account
```bash
# Create service account
gcloud iam service-accounts create tick-data-handler \
    --description="Service account for tick data handler" \
    --display-name="Tick Data Handler"

# Grant necessary permissions
gcloud projects add-iam-policy-binding central-element-323112 \
    --member="serviceAccount:tick-data-handler@central-element-323112.iam.gserviceaccount.com" \
    --role="roles/storage.admin"

# Create and download key
gcloud iam service-accounts keys create central-element-323112-e35fb0ddafe2.json \
    --iam-account=tick-data-handler@central-element-323112.iam.gserviceaccount.com
```

#### Create GCS Bucket
```bash
# Create bucket
gsutil mb -p central-element-323112 -c STANDARD -l asia-northeast1 gs://market-data-tick

# Set bucket permissions
gsutil iam ch serviceAccount:tick-data-handler@central-element-323112.iam.gserviceaccount.com:objectAdmin gs://market-data-tick
```

### 3. Tardis API Setup

1. Sign up for Tardis.dev account
2. Generate API key from dashboard
3. Add API key to `.env` file
4. Test connection:

```bash
python deploytest_tardis_connection.py
```

## Local Development

### 1. Project Structure

```
market-tick-data-handler/
├── src/                               # Main source code
│   ├── main.py                        # Centralized entry point
│   ├── models.py                      # Data models
│   ├── instrument_processor/          # Instrument definition generation
│   │   ├── canonical_key_generator.py
│   │   └── gcs_uploader.py
│   ├── data_downloader/               # Data download and upload
│   │   ├── download_orchestrator.py
│   │   ├── instrument_reader.py
│   │   └── tardis_connector.py
│   └── data_validator/                # Data validation
│       └── data_validator.py
├── deploy/                            # Deployment scripts
│   ├── local/                         # Local execution
│   │   └── run-main.sh
│   └── vm/                            # VM deployment
│       ├── deploy-instruments.sh
│       ├── deploy-tardis.sh
│       ├── build-images.sh
│       └── shard-deploy.sh
├── docker/                            # Docker configurations
│   ├── instrument-generation/
│   ├── tardis-download/
│   └── shared/
├── docs/                              # Documentation
├── config.py                          # Configuration management
├── requirements.txt                   # Dependencies
└── .env                              # Environment variables
```

### 2. Complete Data Pipeline

The system follows a comprehensive data pipeline from raw tick data to processed features:

#### VM Deployment Pipeline (Scheduled Daily at 8 AM UTC)

```bash
# 1. Generate instrument definitions
python -m market_data_tick_handler.main. --mode instruments --start-date 2023-05-23 --end-date 2023-05-23

# 2. Generate missing data reports
python -m market_data_tick_handler.main. --mode missing-tick-reports --start-date 2023-05-23 --end-date 2023-05-23

# 3. Download missing tick data (default mode)
python -m market_data_tick_handler.main. --mode download --start-date 2023-05-23 --end-date 2023-05-23 --venues binance

# 4. Process candles with HFT features
python -m market_data_tick_handler.main. --mode candle-processing --start-date 2023-05-23 --end-date 2023-05-23

# 5. Upload candles to BigQuery
python -m market_data_tick_handler.main. --mode bigquery-upload --start-date 2023-05-23 --end-date 2023-05-23

# 6. Process MFT features
python -m market_data_tick_handler.main. --mode mft-processing --start-date 2023-05-23 --end-date 2023-05-23
```

#### Data Storage Architecture

- **GCS Storage**:
  - Raw tick data (optimized Parquet with timestamp partitioning)
  - Processed candles (15s, 1m, 5m, 15m, 1h, 4h, 24h timeframes)
  - MFT features (1m+ timeframes)
  - Instrument definitions

- **BigQuery Storage**:
  - Candles with HFT features (one table per timeframe)
  - Real-time streaming data

- **Package Usage**:
  - Features service imports package to query BigQuery
  - Gets candle data with HFT features
  - Processes additional MFT features
  - Pushes features to GCS for backtesting

### 3. Running the Service

#### Using the Main Entry Point
```bash
# Check for missing instrument definitions
python -m market_data_tick_handler.main. --mode check-gaps --start-date 2023-05-23 --end-date 2023-05-25

# Generate instrument definitions
python -m market_data_tick_handler.main. --mode instruments --start-date 2023-05-23 --end-date 2023-05-25

# Generate missing data reports
python -m market_data_tick_handler.main. --mode missing-tick-reports --start-date 2023-05-23 --end-date 2023-05-25

# Download only missing data (default mode)
python -m market_data_tick_handler.main. --mode download --start-date 2023-05-23 --end-date 2023-05-25 --venues deribit

# Force download all data (overrides missing data check)
python -m market_data_tick_handler.main. --mode download --start-date 2023-05-23 --end-date 2023-05-25 --venues deribit --force

# Validate data completeness
python -m market_data_tick_handler.main. --mode validate --start-date 2023-05-23 --end-date 2023-05-25

# Run full pipeline (check-gaps → instruments → missing-tick-reports → download → validate)
python -m market_data_tick_handler.main. --mode full-pipeline-ticks --start-date 2023-05-23 --end-date 2023-05-25
```

#### Using Convenience Scripts
```bash
# Batch processing operations
./deploy/local/run-main.sh check-gaps --start-date 2023-05-23 --end-date 2023-05-25
./deploy/local/run-main.sh instruments --start-date 2023-05-23 --end-date 2023-05-25
./deploy/local/run-main.sh missing-tick-reports --start-date 2023-05-23 --end-date 2023-05-25
./deploy/local/run-main.sh download --start-date 2023-05-23 --end-date 2023-05-25
./deploy/local/run-main.sh download --start-date 2023-05-23 --end-date 2023-05-25 --force
./deploy/local/run-main.sh candle-processing --start-date 2024-01-01 --end-date 2024-01-01
./deploy/local/run-main.sh bigquery-upload --start-date 2024-01-01 --end-date 2024-01-01
./deploy/local/run-main.sh validate --start-date 2023-05-23 --end-date 2023-05-25
./deploy/local/run-main.sh full-pipeline-ticks --start-date 2023-05-23 --end-date 2023-05-25

# Live streaming operations
./deploy/local/run-main.sh streaming-ticks --symbol BTC-USDT
./deploy/local/run-main.sh streaming-candles --symbol BTC-USDT,ETH-USDT
```

### 3. Development Tools

#### Code Formatting
```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Format code
black market_data_tick_handler/
isort market_data_tick_handler/

# Type checking
mypy market_data_tick_handler/
```

#### Testing
```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_tardis_connector.py

# Run with coverage
pytest --cov=market_data_tick_handler --cov-report=html
```

## Package Usage

The Market Data Handler can be used as a package/library by downstream services:

### 1. Install as Package

```bash
# Install in development mode
pip install -e .

# Or add to requirements.txt
git+https://github.com/your-org/market-tick-data-handler.git
```

### 2. Basic Usage

```python
from src.data_client import DataClient, CandleDataReader, TickDataReader
from config import get_config
from datetime import datetime, timezone

# Initialize
config = get_config()
data_client = DataClient(config.gcp.bucket, config)

# Read candles
candle_reader = CandleDataReader(data_client)
candles = candle_reader.get_candles(
    instrument_id="BINANCE:SPOT_PAIR:BTC-USDT",
    timeframe="1m",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    end_date=datetime(2024, 1, 2, tzinfo=timezone.utc)
)

# Read tick data with filtering
tick_reader = TickDataReader(data_client)
ticks = tick_reader.get_tick_data(
    instrument_id="BINANCE:SPOT_PAIR:BTC-USDT",
    start_time=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
    end_time=datetime(2024, 1, 1, 12, 5, 0, tzinfo=timezone.utc),
    date=datetime(2024, 1, 1).date()
)
```

### 3. Use Cases

#### Features Service
```python
# Get 1m candles for feature calculation
candles = candle_reader.get_candles(
    instrument_id="BINANCE:SPOT_PAIR:BTC-USDT",
    timeframe="1m",
    start_date=start_date,
    end_date=end_date
)
```

#### Execution Service
```python
# Get 15s candles with HFT features
candles = candle_reader.get_candles(
    instrument_id="BINANCE:SPOT_PAIR:BTC-USDT",
    timeframe="15s",
    start_date=start_date,
    end_date=end_date
)
```

#### Backtesting
```python
# Get tick data for specific time range
ticks = tick_reader.get_tick_data(
    instrument_id="BINANCE:SPOT_PAIR:BTC-USDT",
    start_time=start_time,
    end_time=end_time,
    date=date
)
```

## Live Streaming Setup

### 1. Node.js Requirements

```bash
# Install Node.js (version 18 or higher)
curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
sudo apt-get install -y nodejs

# Verify installation
node --version
npm --version
```

### 2. Install Dependencies

```bash
# Navigate to Node.js streaming directory
cd live_streaming/nodejs

# Install dependencies
npm install
```

### 3. Configure Environment

```bash
# Set your Tardis API key
export TARDIS_API_KEY="TD.your_api_key_here"

# Set GCP credentials
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
export GCP_PROJECT_ID="your-gcp-project-id"
```

### 4. Run Streaming Services

```bash
# Stream raw ticks to BigQuery
node live_tick_streamer.js --mode ticks --symbol BTC-USDT

# Stream real-time candles with HFT features
node live_tick_streamer.js --mode candles --symbol BTC-USDT,ETH-USDT

# Or use the convenience script
./deploy/local/run-main.sh streaming-ticks --symbol BTC-USDT
./deploy/local/run-main.sh streaming-candles --symbol BTC-USDT,ETH-USDT
```

## Docker Setup

### 1. Build Docker Image

```bash
# Build image
docker build -t market-data-tick-handler:latest .

# Build with specific tag
docker build -t market-data-tick-handler:v1.0.0 .
```

### 2. Run Container

```bash
# Run with environment variables
docker run -d \
  --name tick-data-handler \
  -e TARDIS_API_KEY=your_api_key \
  -e GCP_PROJECT_ID=central-element-323112 \
  -e GCS_BUCKET=market-data-tick \
  -v /path/to/credentials:/app/credentials \
  -p 8000:8000 \
  market-data-tick-handler:latest

# Run with .env file
docker run -d \
  --name tick-data-handler \
  --env-file .env \
  -v /path/to/credentials:/app/credentials \
  -p 8000:8000 \
  market-data-tick-handler:latest
```

### 3. Docker Compose

Create `docker-compose.yml`:

```yaml
version: '3.8'

services:
  tick-data-handler:
    build: .
    ports:
      - "8000:8000"
    environment:
      - TARDIS_API_KEY=${TARDIS_API_KEY}
      - GCP_PROJECT_ID=${GCP_PROJECT_ID}
      - GCS_BUCKET=${GCS_BUCKET}
    volumes:
      - ./credentials:/app/credentials
      - ./logs:/app/logs
    restart: unless-stopped
```

Run with Docker Compose:

```bash
# Start services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

## VM Deployment

### 1. VM Deployment

#### Single VM for Development/Testing
```bash
# Deploy single VM for instrument generation
./deploy/vm/deploy-instruments.sh deploy

# Deploy single VM for data download
./deploy/vm/deploy-tardis.sh deploy
```

#### Multiple VMs for Production
```bash
# Deploy multiple VMs with sharding
./deploy/vm/shard-deploy.sh instruments --start-date 2023-05-23 --end-date 2023-05-25 --shards 10
./deploy/vm/shard-deploy.sh tardis --start-date 2023-05-23 --end-date 2023-05-25 --shards 20
```

### 2. VM Configuration

The VM deployment scripts handle all configuration automatically. They will:

1. **Install Dependencies**: Python, pip, git, gcloud CLI
2. **Clone Repository**: Download the latest code
3. **Setup Environment**: Create virtual environment and install dependencies
4. **Configure Credentials**: Set up GCP service account authentication
5. **Deploy Application**: Start the appropriate service based on deployment type

#### Manual Configuration (if needed)
```bash
# SSH into VM
gcloud compute ssh VM_NAME --zone=asia-northeast1-c

# Check application status
cd /opt/market-tick-data-handler
tail -f logs/*.log

# Restart service if needed
sudo systemctl restart market-tick-handler
```

### 3. Data Organization

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

## Verification

### 1. Test Tardis Connection

```bash
python deploytest_tardis_connection.py
```

Expected output:
```
✅ Tardis API connection successful
✅ API key valid
✅ Rate limits: 30M calls/day
```

### 2. Test GCS Access

```bash
python deploytest_gcs_access.py
```

Expected output:
```
✅ GCS bucket accessible: market-data-tick
✅ Upload test successful
✅ Download test successful
```

### 3. Test Data Download

```bash
# Download sample data (missing data mode)
python -m market_data_tick_handler.main. --mode download --start-date 2023-05-23 --end-date 2023-05-23 --venues deribit --max-instruments 5

# Force download sample data
python -m market_data_tick_handler.main. --mode download --start-date 2023-05-23 --end-date 2023-05-23 --venues deribit --max-instruments 5 --force

# Verify data in GCS
gsutil ls gs://market-data-tick/raw_tick_data/by_date/day-2023-05-23/
```

### 4. Test Full Pipeline

```bash
# Run validation
python -m market_data_tick_handler.main. --mode validate --start-date 2023-05-23 --end-date 2023-05-23

# Run complete pipeline
python -m market_data_tick_handler.main. --mode full-pipeline-ticks --start-date 2023-05-23 --end-date 2023-05-23 --max-instruments 5

# Verify all data types
gsutil ls gs://market-data-tick/instrument_availability/by_date/day-2023-05-23/
gsutil ls gs://market-data-tick/raw_tick_data/by_date/day-2023-05-23/
```

## Troubleshooting

### Common Issues

#### 1. Tardis API Connection Failed
```bash
# Check API key
echo $TARDIS_API_KEY

# Test connection manually
curl -H "Authorization: Bearer $TARDIS_API_KEY" \
  "https://datasets.tardis.dev/v1/binance/trades/2024/10/20/BTCUSDT.csv.gz"
```

#### 2. GCS Access Denied
```bash
# Check credentials
gcloud auth list

# Test GCS access
gsutil ls gs://market-data-tick/

# Check service account permissions
gcloud projects get-iam-policy central-element-323112
```

#### 3. Memory Issues
```bash
# Check memory usage
free -h

# Monitor during download
htop

# Increase swap if needed
sudo fallocate -l 8G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
```

#### 4. Network Issues
```bash
# Test network connectivity
ping datasets.tardis.dev
curl -I https://datasets.tardis.dev

# Check DNS resolution
nslookup datasets.tardis.dev
```

### Debug Mode

Enable debug logging:

```bash
export LOG_LEVEL=DEBUG
export LOG_DESTINATION=both
python -m market_data_tick_handler.cli download --date 2024-10-20
```

### Log Files

- **Local logs**: `logs/market_data_tick_handler.log`
- **GCP logs**: Google Cloud Logging console
- **VM logs**: `/var/log/syslog`

### Getting Help

1. Check the [Troubleshooting Guide](TROUBLESHOOTING.md)
2. Review logs for error messages
3. Test individual components
4. Check network and credentials
5. Contact support team

---

**Next Steps**: After successful setup, proceed to [Deployment Guide](DEPLOYMENT_GUIDE.md) for production deployment.