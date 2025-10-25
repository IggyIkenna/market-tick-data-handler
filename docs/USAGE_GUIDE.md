# Market Data Tick Handler - Usage Guide

## Quick Start

### 1. Setup Environment
```bash
# Copy environment template
cp env.example .env

# Edit with your actual values
nano .env
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Complete Data Pipeline

The system follows a complete data pipeline from raw tick data to processed features:

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

- **GCS**: Raw tick data (optimized Parquet), processed candles, MFT features
- **BigQuery**: Candles with HFT features (one table per timeframe)
- **Package Usage**: Features service imports package to query BigQuery and push features to GCS

### 4. Run Operations
 
#### Batch Processing (VM Deployments)
```bash
# Generate instrument definitions
python -m market_data_tick_handler.main. --mode instruments --start-date 2023-05-23 --end-date 2023-05-25

# Download missing tick data (default mode)
python -m market_data_tick_handler.main. --mode download --start-date 2023-05-23 --end-date 2023-05-25 --venues deribit

# Force download all data (overrides missing data check)
python -m market_data_tick_handler.main. --mode download --start-date 2023-05-23 --end-date 2023-05-25 --venues deribit --force

# Process candles with HFT features
python -m market_data_tick_handler.main. --mode candle-processing --start-date 2024-01-01 --end-date 2024-01-01

# Upload candles to BigQuery
python -m market_data_tick_handler.main. --mode bigquery-upload --start-date 2024-01-01 --end-date 2024-01-01

# Validate data
python -m market_data_tick_handler.main. --mode validate --start-date 2023-05-23 --end-date 2023-05-25

# Run full pipeline
python -m market_data_tick_handler.main. --mode full-pipeline-ticks --start-date 2023-05-23 --end-date 2023-05-25
```

#### Live Streaming (Local Development)
```bash
# Stream raw ticks to BigQuery
./deploy/local/run-main.sh streaming-ticks --symbol BTC-USDT

# Stream real-time candles with HFT features
./deploy/local/run-main.sh streaming-candles --symbol BTC-USDT,ETH-USDT
```

#### Package Usage (Library)
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

## Operation Modes

The system supports multiple operation modes across different deployment types:

### Batch Processing Modes (VM Deployments)

| Mode | Description | Purpose |
|------|-------------|---------|
| `check-gaps` | Light check for missing instrument definitions | Quick validation before instrument generation |
| `instruments` | Generate instrument definitions and upload to GCS | Create instrument registry for data downloads |
| `missing-tick-reports` | Generate missing data reports for date range | Identify what data needs to be downloaded |
| `download` | Download only missing data based on reports | Backfill missing data efficiently |
| `candle-processing` | Process historical tick data into candles with HFT features | Generate candles for analytics and backtesting |
| `bigquery-upload` | Upload processed candles to BigQuery for analytics | Enable analytics and reporting |
| `validate` | Check for missing data and validate completeness | Verify data completeness after downloads |
| `full-pipeline-ticks` | Run complete workflow (all modes in sequence) | End-to-end data processing |

### Live Streaming Modes (Node.js Services)

| Mode | Description | Purpose |
|------|-------------|---------|
| `streaming-ticks` | Stream raw tick data to BigQuery for analytics | Real-time analytics and monitoring |
| `streaming-candles` | Process real-time candles with HFT features | Real-time trading and execution systems |

## Command Line Interface

### Basic Usage
```bash
python -m market_data_tick_handler.main. --mode <MODE> --start-date <DATE> --end-date <DATE> [OPTIONS]
```

### Required Arguments
- `--mode`: Operation mode (check-gaps, instruments, missing-tick-reports, download, validate, full-pipeline-ticks)
- `--start-date`: Start date in YYYY-MM-DD format
- `--end-date`: End date in YYYY-MM-DD format

### Optional Arguments

#### Configuration
- `--env-file`: Path to environment file (.env)
- `--config-file`: Path to configuration file (YAML)

#### Filtering Options
- `--exchanges`: Exchanges to process (for instruments mode)
- `--venues`: Venues to process (for download mode)
- `--instrument-types`: Instrument types to process
- `--data-types`: Data types to download (e.g., trades book_snapshot_5)
- `--max-instruments`: Maximum number of instruments to process

#### Performance
- `--max-workers`: Maximum number of worker threads (default: 4)

#### Logging
- `--log-level`: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- `--verbose`, `-v`: Enable verbose logging

## Usage Examples

### Gap Detection
```bash
# Check for missing instrument definitions
python -m market_data_tick_handler.main. --mode check-gaps --start-date 2023-05-23 --end-date 2023-05-25

# Check specific date range
python -m market_data_tick_handler.main. --mode check-gaps --start-date 2023-05-24 --end-date 2023-05-26
```

### Instrument Generation
```bash
# Basic instrument generation
python -m market_data_tick_handler.main. --mode instruments --start-date 2023-05-23 --end-date 2023-05-25

# With specific exchanges
python -m market_data_tick_handler.main. --mode instruments \
  --start-date 2023-05-23 --end-date 2023-05-25 \
  --exchanges deribit binance-futures

# With custom environment file
python -m market_data_tick_handler.main. --mode instruments \
  --start-date 2023-05-23 --end-date 2023-05-25 \
  --env-file .env.production
```

### Missing Data Reports
```bash
# Generate missing data reports
python -m market_data_tick_handler.main. --mode missing-tick-reports --start-date 2023-05-23 --end-date 2023-05-25

# Generate reports for specific venues and data types
python -m market_data_tick_handler.main. --mode missing-tick-reports \
  --start-date 2023-05-23 --end-date 2023-05-25 \
  --venues deribit binance-futures \
  --data-types trades book_snapshot_5
```

### Tick Data Download
```bash
# Basic download
python -m market_data_tick_handler.main. --mode download \
  --start-date 2023-05-23 --end-date 2023-05-25

# With specific venues and data types
python -m market_data_tick_handler.main. --mode download \
  --start-date 2023-05-23 --end-date 2023-05-25 \
  --venues deribit binance-futures \
  --data-types trades book_snapshot_5

# With instrument type filtering
python -m market_data_tick_handler.main. --mode download \
  --start-date 2023-05-23 --end-date 2023-05-25 \
  --venues deribit \
  --instrument-types option perpetual \
  --max-instruments 5
```

### Data Validation
```bash
# Basic validation
python -m market_data_tick_handler.main. --mode validate \
  --start-date 2023-05-23 --end-date 2023-05-25

# Validate specific venues and data types
python -m market_data_tick_handler.main. --mode validate \
  --start-date 2023-05-23 --end-date 2023-05-25 \
  --venues deribit binance-futures \
  --data-types trades book_snapshot_5
```

### Full Pipeline
```bash
# Complete pipeline (check-gaps → instruments → missing-tick-reports → download → validate)
python -m market_data_tick_handler.main. --mode full-pipeline-ticks \
  --start-date 2023-05-23 --end-date 2023-05-25

# With custom configuration
python -m market_data_tick_handler.main. --mode full-pipeline-ticks \
  --start-date 2023-05-23 --end-date 2023-05-25 \
  --exchanges deribit binance-futures \
  --venues deribit \
  --instrument-types option perpetual \
  --data-types trades book_snapshot_5
```

## Convenience Scripts

### Local Execution
```bash
# Use the convenience script for all modes
./deploy/local/run-main.sh check-gaps --start-date 2023-05-23 --end-date 2023-05-25
./deploy/local/run-main.sh instruments --start-date 2023-05-23 --end-date 2023-05-25
./deploy/local/run-main.sh missing-tick-reports --start-date 2023-05-23 --end-date 2023-05-25
./deploy/local/run-main.sh download --start-date 2023-05-23 --end-date 2023-05-25
./deploy/local/run-main.sh validate --start-date 2023-05-23 --end-date 2023-05-25
./deploy/local/run-main.sh full-pipeline-ticks --start-date 2023-05-23 --end-date 2023-05-25
```

### VM Deployment - Development/Testing
```bash
# Deploy single VM for instrument generation
./deploy/vm/deploy-instruments.sh

# Deploy single VM for Tardis download
./deploy/vm/deploy-tardis.sh

# Deploy multiple VMs with sharding
./deploy/vm/shard-deploy.sh instruments --start-date 2023-05-23 --end-date 2023-05-25 --shards 5
```

### VM Deployment - Production Orchestration
```bash
# Deploy multiple VMs for massive data processing
./deploy/vm/shard-deploy.sh tardis --start-date 2023-05-01 --end-date 2023-07-31 --shards 60

# Build and push Docker images
./deploy/vm/build-images.sh build-and-push --tag v1.0.0

# Clean up VMs
./deploy/vm/shard-deploy.sh cleanup
```

## Docker Usage

### Using Docker Compose
```bash
# Instrument generation
cd docker/instrument-generation
docker-compose up --build

# Tick data download
cd docker/tardis-download
docker-compose up --build
```

### Using Docker Run
```bash
# Build image
docker build -f docker/instrument-generation/Dockerfile -t market-tick-handler .

# Run instrument generation
docker run --env-file .env \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/logs:/app/logs \
  market-tick-handler \
  python -m market_data_tick_handler.main. --mode instruments --start-date 2023-05-23 --end-date 2023-05-25

# Run tick data download
docker run --env-file .env \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/logs:/app/logs \
  market-tick-handler \
  python -m market_data_tick_handler.main. --mode download --start-date 2023-05-23 --end-date 2023-05-25
```

## Environment Configuration

### Required Environment Variables
```bash
# Tardis API
TARDIS_API_KEY=TD.your_api_key_here

# Google Cloud Platform
GCP_PROJECT_ID=your-gcp-project-id
GCP_CREDENTIALS_PATH=/path/to/service-account-key.json
GCS_BUCKET=your-gcs-bucket-name
```

### Optional Environment Variables
```bash
# Service Configuration
LOG_LEVEL=INFO
MAX_CONCURRENT_REQUESTS=50
BATCH_SIZE=1000

# Performance Tuning
DOWNLOAD_MAX_WORKERS=2        # Optimal for VM deployment (leaves 2 vCPUs for system)
MAX_PARALLEL_UPLOADS=20
MEMORY_THRESHOLD_PERCENT=90

# Mode-specific defaults
INSTRUMENT_EXCHANGES=binance,binance-futures,deribit,bybit,bybit-spot,okex,okex-futures,okex-swap
DOWNLOAD_VENUES=deribit,binance-futures
DOWNLOAD_DATA_TYPES=trades,book_snapshot_5
```

## File Structure Overview

```
src/
├── main.py                  # Centralized entry point
├── instrument_processor/    # Instrument definition generation
├── data_downloader/         # Data download and upload
└── data_validator/          # Data validation

deploy/
├── local/                   # Local execution scripts
│   └── run-main.sh         # Single convenience script
└── vm/                      # VM deployment scripts
    ├── deploy-instruments.sh
    ├── deploy-tardis.sh
    ├── build-images.sh
    └── shard-deploy.sh

docker/
├── instrument-generation/   # Docker for instrument generation
├── tardis-download/         # Docker for Tardis download
└── shared/                  # Shared Docker utilities
```

## Deployment Modes

| Mode | Purpose | Script | Scale | Docker |
|------|---------|--------|-------|--------|
| Local Python | Run operations locally (no Docker) | `python -m market_data_tick_handler.main. --mode <mode>` | Local | None |
| Local Docker | Run operations in Docker locally | `./deploy/local/run-main.sh <mode>` | Local | `docker/` |
| VM - Development | Deploy single VM for testing | `./deploy/vm/deploy-*.sh` | 1 VM | Local builds |
| VM - Production | Deploy multiple VMs for massive processing | `./deploy/vm/shard-deploy.sh` | Multiple VMs | Artifact Registry |

## When to Use Each Mode

### **Local Python** (No Docker)
- ✅ Quick development and debugging
- ✅ Test code changes instantly
- ✅ No Docker overhead
- ✅ Direct Python environment

### **Local Docker** (Development & Testing)
- ✅ Test new features
- ✅ Debug issues
- ✅ Learn the system
- ✅ Process small datasets
- ✅ Quick iterations

### **VM Development** (`deploy/vm/`)
- ✅ Test VM deployment
- ✅ Process 1-7 days of data
- ✅ Validate before production
- ✅ Single VM testing

### **VM Production** (`deploy/vm/shard-deploy.sh`)
- ✅ Process months/years of data
- ✅ Production workloads
- ✅ Maximum parallelism
- ✅ Cost-optimized processing

## Output Structure

### Generated Files
```
data/
├── instrument_availability/          # Instrument definitions
│   └── by_date/                     # Single partition by date
│       └── day-{date}/              # Daily partitions
│           └── instruments.parquet  # Daily instrument definitions
└── raw_tick_data/                   # Tick data
    └── by_date/                     # Single partition by date
        └── day-{date}/              # Daily partitions
            └── data_type-{type}/    # Data type subdirectories
                └── {instrument_key}.parquet  # Individual instrument files

logs/                                # Execution logs
temp/                                # Temporary files
downloads/                           # Downloaded files (if applicable)
```

## Monitoring

### Local
```bash
# Check logs
tail -f logs/*.log

# Check containers
docker ps
docker logs CONTAINER_NAME
```

### VM
```bash
# SSH into VM
gcloud compute ssh VM_NAME --zone=asia-northeast1-c

# Check logs
sudo journalctl -u google-startup-scripts.service -f
```

## Troubleshooting

### Common Issues

1. **Missing Environment Variables**
   ```
   ValueError: TARDIS_API_KEY environment variable is required
   ```
   Solution: Set required environment variables in `.env` file

2. **Invalid Date Format**
   ```
   ValueError: Invalid date format '2023/05/23'. Use YYYY-MM-DD format.
   ```
   Solution: Use YYYY-MM-DD format for dates

3. **GCS Authentication**
   ```
   google.auth.exceptions.DefaultCredentialsError
   ```
   Solution: Set `GCP_CREDENTIALS_PATH` and ensure service account has proper permissions

4. **Docker not running**: Start Docker Desktop
5. **Permission denied**: `chmod +x deploy**/*.sh`
6. **Missing .env**: `cp env.example .env`
7. **VM issues**: Check gcloud auth and project settings

### Debug Mode
```bash
# Enable verbose logging
python -m market_data_tick_handler.main. --mode instruments --start-date 2023-05-23 --end-date 2023-05-25 --verbose

# Or set log level
python -m market_data_tick_handler.main. --mode instruments --start-date 2023-05-23 --end-date 2023-05-25 --log-level DEBUG
```

## Migration from Old Scripts

### Before (Old Way)
```bash
# Instrument generation
python run_fixed_local_instrument_generation.py

# Tick data download
python deployvm_data_downloader.py
```

### After (New Way)
```bash
# Instrument generation
python -m market_data_tick_handler.main. --mode instruments --start-date 2023-05-23 --end-date 2023-05-25

# Tick data download
python -m market_data_tick_handler.main. --mode download --start-date 2023-05-23 --end-date 2023-05-25 --venues deribit
```

## Advanced Usage

### Custom Configuration Files
```bash
# Use custom environment file
python -m market_data_tick_handler.main. --mode instruments \
  --start-date 2023-05-23 --end-date 2023-05-25 \
  --env-file .env.production

# Use YAML configuration file
python -m market_data_tick_handler.main. --mode instruments \
  --start-date 2023-05-23 --end-date 2023-05-25 \
  --config-file config/production.yaml
```

### Batch Processing
```bash
# Process multiple date ranges
for date in 2023-05-23 2023-05-24 2023-05-25; do
  python -m market_data_tick_handler.main. --mode instruments --start-date $date --end-date $date
done
```

### Monitoring and Logging
```bash
# Enable monitoring
export ENABLE_MONITORING=true
export METRICS_ENDPOINT=http://localhost:8080/metrics

python -m market_data_tick_handler.main. --mode full-pipeline-ticks --start-date 2023-05-23 --end-date 2023-05-25
```

## Benefits of New Architecture

1. **Centralized Entry Point**: Single command for all operations
2. **Environment-Based Configuration**: Easy configuration management with `.env` files
3. **Docker-Friendly**: Clean containerization with proper argument passing
4. **Flexible Filtering**: Granular control over what data to process
5. **Better Error Handling**: Comprehensive error messages and validation
6. **Consistent Interface**: Same command structure for all modes
7. **Easy Deployment**: Simple to deploy on VMs with different configurations
8. **Performance Optimized**: 4.83x download, 1.81x upload speedup
9. **Memory Safe**: 90% threshold prevents OOM errors
10. **Connection Efficient**: Shared clients reduce overhead
