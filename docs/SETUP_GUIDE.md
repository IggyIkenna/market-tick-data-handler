# Market Data Tick Handler - Setup Guide

**Version**: 1.0.0  
**Last Updated**: January 15, 2025

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [Configuration](#configuration)
4. [Local Development](#local-development)
5. [Docker Setup](#docker-setup)
6. [VM Deployment](#vm-deployment)
7. [Verification](#verification)
8. [Troubleshooting](#troubleshooting)

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
python scripts/test_tardis_connection.py
```

## Local Development

### 1. Project Structure

```
market_data_tick_handler/
├── market_data_tick_handler/          # Main package
│   ├── __init__.py
│   ├── config.py                      # Configuration management
│   ├── src/
│   │   ├── models.py                  # Data models
│   ├── instrument_registry.py         # Instrument definitions
│   ├── tardis_connector.py            # Tardis API client
│   ├── gcs_manager.py                 # GCS operations
│   ├── storage_manager.py             # Local storage
│   ├── data_downloader.py             # Main download service
│   ├── query_service.py               # Data query service
│   ├── gap_detector.py                # Gap detection
│   ├── api.py                         # FastAPI service
│   ├── cli.py                         # CLI interface
│   └── utils/                         # Utility modules
├── tests/                             # Test suite
├── scripts/                           # Utility scripts
├── docs/                              # Documentation
├── requirements.txt                   # Dependencies
├── Dockerfile                         # Container config
└── .env                              # Environment variables
```

### 2. Running the Service

#### Start FastAPI Service
```bash
# Development mode with auto-reload
uvicorn market_data_tick_handler.api:app --reload --host 0.0.0.0 --port 8000

# Production mode
uvicorn market_data_tick_handler.api:app --host 0.0.0.0 --port 8000
```

#### Using CLI
```bash
# Download data for specific date
python -m market_data_tick_handler.cli download --date 2024-10-20

# Query data
python -m market_data_tick_handler.cli query --instrument binance-spot:SPOT_ASSET:BTC-USDT --start-date 2024-10-20 --end-date 2024-10-21

# Check for gaps
python -m market_data_tick_handler.cli check-gaps --start-date 2024-10-20 --end-date 2024-10-21
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

### 1. VM Creation

```bash
# Create 30 VMs for sharding
for i in {0..29}; do
  gcloud compute instances create tick-data-downloader-$i \
    --zone=asia-northeast1-c \
    --machine-type=e2-highmem-8 \
    --boot-disk-size=100GB \
    --boot-disk-type=pd-standard \
    --image-family=ubuntu-2004-lts \
    --image-project=ubuntu-os-cloud \
    --tags=tick-data-downloader \
    --metadata=shard-index=$i
done
```

### 2. VM Configuration

#### Install Dependencies
```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install Python and dependencies
sudo apt install -y python3.9 python3.9-venv python3-pip git

# Install Google Cloud SDK
curl https://sdk.cloud.google.com | bash
exec -l $SHELL
gcloud init
```

#### Deploy Application
```bash
# Clone repository
git clone https://github.com/your-org/market-data-tick-handler.git
cd market-data-tick-handler

# Create virtual environment
python3.9 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Copy credentials
cp /path/to/credentials.json central-element-323112-e35fb0ddafe2.json

# Configure environment
cp .env.example .env
# Edit .env with VM-specific settings
```

### 3. Startup Script

Create `vm_startup_script.sh`:

```bash
#!/bin/bash

# Get shard index from metadata
SHARD_INDEX=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/shard-index)

# Set environment variables
export SHARD_INDEX=$SHARD_INDEX
export TOTAL_SHARDS=30
export LOG_LEVEL=INFO
export LOG_DESTINATION=gcp

# Start the service
cd /home/ubuntu/market-data-tick-handler
source venv/bin/activate
python -m market_data_tick_handler.data_downloader --shard-index $SHARD_INDEX
```

## Verification

### 1. Test Tardis Connection

```bash
python scripts/test_tardis_connection.py
```

Expected output:
```
✅ Tardis API connection successful
✅ API key valid
✅ Rate limits: 30M calls/day
```

### 2. Test GCS Access

```bash
python scripts/test_gcs_access.py
```

Expected output:
```
✅ GCS bucket accessible: market-data-tick
✅ Upload test successful
✅ Download test successful
```

### 3. Test Data Download

```bash
# Download sample data
python -m market_data_tick_handler.cli download --date 2024-10-20 --test-mode

# Verify data in GCS
gsutil ls gs://market-data-tick/data/2024-10-20/
```

### 4. Test API Service

```bash
# Start service
uvicorn market_data_tick_handler.api:app --host 0.0.0.0 --port 8000 &

# Test health endpoint
curl http://localhost:8000/health

# Test data query
curl "http://localhost:8000/api/v1/tick-data/binance-spot:SPOT_ASSET:BTC-USDT?start_date=2024-10-20&end_date=2024-10-21"
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