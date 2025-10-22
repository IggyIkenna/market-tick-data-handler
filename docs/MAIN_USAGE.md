# Market Data Tick Handler - Main Entry Point Usage

This document explains how to use the new centralized `src/main.py` entry point for all market data operations.

## Overview

The new `src/main.py` provides a unified interface for:
- **Instrument Generation**: Generate canonical instrument definitions and upload to GCS
- **Tick Data Download**: Download market data from Tardis and upload to GCS
- **Data Validation**: Validate data completeness and check for missing data
- **Full Pipeline**: Run the complete workflow (instruments → download → validate)

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

### 3. Run Operations

```bash
# Generate instrument definitions
python -m src.main --mode instruments --start-date 2023-05-23 --end-date 2023-05-25

# Download tick data
python -m src.main --mode download --start-date 2023-05-23 --end-date 2023-05-25 --venues deribit

# Validate data
python -m src.main --mode validate --start-date 2023-05-23 --end-date 2023-05-25

# Run full pipeline
python -m src.main --mode full-pipeline --start-date 2023-05-23 --end-date 2023-05-25
```

## Command Line Interface

### Basic Usage

```bash
python -m src.main --mode <MODE> --start-date <DATE> --end-date <DATE> [OPTIONS]
```

### Modes

| Mode | Description |
|------|-------------|
| `instruments` | Generate instrument definitions and upload to GCS |
| `download` | Download tick data and upload to GCS |
| `validate` | Validate data completeness and check for missing data |
| `full-pipeline` | Run complete pipeline (instruments + download + validate) |

### Required Arguments

- `--mode`: Operation mode (instruments, download, validate, full-pipeline)
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

#### Logging
- `--log-level`: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- `--verbose`, `-v`: Enable verbose logging

## Examples

### Instrument Generation

```bash
# Basic instrument generation
python -m src.main --mode instruments --start-date 2023-05-23 --end-date 2023-05-25

# With specific exchanges
python -m src.main --mode instruments \
  --start-date 2023-05-23 --end-date 2023-05-25 \
  --exchanges deribit binance-futures

# With custom environment file
python -m src.main --mode instruments \
  --start-date 2023-05-23 --end-date 2023-05-25 \
  --env-file .env.production
```

### Tick Data Download

```bash
# Basic download
python -m src.main --mode download \
  --start-date 2023-05-23 --end-date 2023-05-25

# With specific venues and data types
python -m src.main --mode download \
  --start-date 2023-05-23 --end-date 2023-05-25 \
  --venues deribit binance-futures \
  --data-types trades book_snapshot_5

# With instrument type filtering
python -m src.main --mode download \
  --start-date 2023-05-23 --end-date 2023-05-25 \
  --venues deribit \
  --instrument-types option perpetual \
  --max-instruments 5
```

### Data Validation

```bash
# Basic validation
python -m src.main --mode validate \
  --start-date 2023-05-23 --end-date 2023-05-25

# Validate specific venues and data types
python -m src.main --mode validate \
  --start-date 2023-05-23 --end-date 2023-05-25 \
  --venues deribit binance-futures \
  --data-types trades book_snapshot_5
```

### Full Pipeline

```bash
# Complete pipeline
python -m src.main --mode full-pipeline \
  --start-date 2023-05-23 --end-date 2023-05-25

# With custom configuration
python -m src.main --mode full-pipeline \
  --start-date 2023-05-23 --end-date 2023-05-25 \
  --exchanges deribit binance-futures \
  --venues deribit \
  --instrument-types option perpetual \
  --data-types trades book_snapshot_5
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
MAX_CONCURRENT_REQUESTS=2
BATCH_SIZE=1000

# Mode-specific defaults
INSTRUMENT_EXCHANGES=binance,binance-futures,deribit,bybit,bybit-spot,okex,okex-futures,okex-swap
DOWNLOAD_VENUES=deribit,binance-futures
DOWNLOAD_DATA_TYPES=trades,book_snapshot_5
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
  python -m src.main --mode instruments --start-date 2023-05-23 --end-date 2023-05-25

# Run tick data download
docker run --env-file .env \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/logs:/app/logs \
  market-tick-handler \
  python -m src.main --mode download --start-date 2023-05-23 --end-date 2023-05-25
```

## Convenience Scripts

### Local Execution

```bash
# Use the convenience script
./deploy/local/run-main.sh instruments --start-date 2023-05-23 --end-date 2023-05-25
./deploy/local/run-main.sh download --start-date 2023-05-23 --end-date 2023-05-25
./deploy/local/run-main.sh validate --start-date 2023-05-23 --end-date 2023-05-25
./deploy/local/run-main.sh full-pipeline --start-date 2023-05-23 --end-date 2023-05-25
```

### Docker Execution

```bash
# Use Docker Compose directly
cd docker/instrument-generation && docker-compose up --build
cd docker/tardis-download && docker-compose up --build
```

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

## Error Handling

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

### Debug Mode

```bash
# Enable verbose logging
python -m src.main --mode instruments --start-date 2023-05-23 --end-date 2023-05-25 --verbose

# Or set log level
python -m src.main --mode instruments --start-date 2023-05-23 --end-date 2023-05-25 --log-level DEBUG
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
python -m src.main --mode instruments --start-date 2023-05-23 --end-date 2023-05-25

# Tick data download
python -m src.main --mode download --start-date 2023-05-23 --end-date 2023-05-25 --venues deribit
```

## Benefits of New Architecture

1. **Centralized Entry Point**: Single command for all operations
2. **Environment-Based Configuration**: Easy configuration management with `.env` files
3. **Docker-Friendly**: Clean containerization with proper argument passing
4. **Flexible Filtering**: Granular control over what data to process
5. **Better Error Handling**: Comprehensive error messages and validation
6. **Consistent Interface**: Same command structure for all modes
7. **Easy Deployment**: Simple to deploy on VMs with different configurations

## Advanced Usage

### Custom Configuration Files

```bash
# Use custom environment file
python -m src.main --mode instruments \
  --start-date 2023-05-23 --end-date 2023-05-25 \
  --env-file .env.production

# Use YAML configuration file
python -m src.main --mode instruments \
  --start-date 2023-05-23 --end-date 2023-05-25 \
  --config-file config/production.yaml
```

### Batch Processing

```bash
# Process multiple date ranges
for date in 2023-05-23 2023-05-24 2023-05-25; do
  python -m src.main --mode instruments --start-date $date --end-date $date
done
```

### Monitoring and Logging

```bash
# Enable monitoring
export ENABLE_MONITORING=true
export METRICS_ENDPOINT=http://localhost:8080/metrics

python -m src.main --mode full-pipeline --start-date 2023-05-23 --end-date 2023-05-25
```
