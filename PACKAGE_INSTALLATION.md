# Market Data Tick Handler - Installation Guide

## 🚀 Quick Installation

### Option 1: Install from GitHub (Recommended)
```bash
# Install directly from GitHub with all dependencies
pip install git+https://github.com/iggyikenna/market-tick-data-handler.git

# Or install with development dependencies
pip install git+https://github.com/iggyikenna/market-tick-data-handler.git[dev]
```

### Option 2: Clone and Install Locally
```bash
# Clone the repository
git clone https://github.com/iggyikenna/market-tick-data-handler.git
cd market-tick-data-handler

# Install in development mode (recommended for contributors)
pip install -e .

# Or install with development dependencies
pip install -e .[dev]
```

## 📦 What Gets Installed

The package automatically installs all required dependencies:

### Core Dependencies (Always Installed)
- `pandas>=1.5.0` - Data manipulation
- `numpy>=1.21.0` - Numerical computing
- `google-cloud-storage>=2.7.0` - GCS integration
- `google-cloud-bigquery>=3.11.0` - BigQuery integration
- `google-cloud-secret-manager>=2.16.0` - Secret management
- `aiohttp>=3.8.0` - Async HTTP client
- `asyncio-throttle>=1.0.0` - Rate limiting
- `python-dotenv>=0.19.0` - Environment variables
- `pyarrow>=10.0.0` - Parquet support
- `fastparquet>=0.8.0` - Parquet reading
- `pydantic>=1.10.0` - Data validation
- `structlog>=22.0.0` - Structured logging
- `tenacity>=8.0.0` - Retry logic
- `tardis-dev>=0.1.0` - Tardis API client

### Optional Dependencies
```bash
# Development tools (testing, linting, formatting)
pip install market-data-tick-handler[dev]

# Streaming capabilities (Node.js integration)
pip install market-data-tick-handler[streaming]

# Everything
pip install market-data-tick-handler[dev,streaming]
```

## 🔧 Prerequisites

### Required
- **Python 3.8+** (3.11+ recommended)
- **Git** (for cloning)

### For Production Use
- **Google Cloud SDK** - For GCP authentication
- **Tardis.dev API key** - Stored in Google Cloud Secret Manager
- **GCP Project** with proper IAM permissions

### For Development
- **pytest** - For running tests
- **black** - For code formatting
- **mypy** - For type checking

## 🎯 Usage After Installation

### As a Command Line Tool
```bash
# Set up authentication
export USE_SECRET_MANAGER=true
export GCP_PROJECT_ID=your-project-id

# Run any mode
python -m market_data_tick_handler --mode instruments --start-date 2023-05-23 --end-date 2023-05-25
python -m market_data_tick_handler --mode download --start-date 2023-05-23 --end-date 2023-05-25
python -m market_data_tick_handler --mode missing-tick-reports --start-date 2023-05-23 --end-date 2023-05-25
```

### As a Python Package
```python
from market_data_tick_handler import DataClient, CandleDataReader
from market_data_tick_handler.config import get_config

# Initialize
config = get_config()
data_client = DataClient(config.gcp.bucket, config)
candle_reader = CandleDataReader(data_client)

# Use the package
candles = candle_reader.get_candles(
    instrument_id="BINANCE:SPOT_PAIR:BTC-USDT",
    timeframe="1m",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    end_date=datetime(2024, 1, 2, tzinfo=timezone.utc)
)
```

## 🔐 Authentication Setup

### For Production (Secret Manager)
```bash
# Set environment variables
export USE_SECRET_MANAGER=true
export GCP_PROJECT_ID=your-project-id
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# Store Tardis API key in Secret Manager
echo "TD.your_tardis_api_key" | gcloud secrets create tardis-api-key --data-file=-
```

### For Development (Environment Variables)
```bash
# Set environment variables
export USE_SECRET_MANAGER=false
export TARDIS_API_KEY=TD.your_test_key
export GCP_PROJECT_ID=your-dev-project
export GCS_BUCKET=your-dev-bucket
```

## 🐛 Troubleshooting

### Common Issues

1. **Import Error**: Make sure you installed the package
   ```bash
   pip install -e .
   ```

2. **Authentication Error**: Check your environment variables
   ```bash
   echo $USE_SECRET_MANAGER
   echo $GCP_PROJECT_ID
   ```

3. **Missing Dependencies**: Reinstall with all dependencies
   ```bash
   pip install -e .[dev]
   ```

4. **Permission Denied**: Check GCP IAM roles
   ```bash
   gcloud projects get-iam-policy your-project-id
   ```

### Getting Help

- **Documentation**: [docs/](docs/)
- **Issues**: [GitHub Issues](https://github.com/iggyikenna/market-tick-data-handler/issues)
- **Examples**: [examples/](examples/)

## 📋 System Requirements

### Minimum Requirements
- Python 3.8+
- 4GB RAM
- 10GB disk space

### Recommended for Production
- Python 3.11+
- 8GB+ RAM
- SSD storage
- Google Cloud VM (for optimal performance)

## 🔄 Updating

```bash
# Update from GitHub
pip install --upgrade git+https://github.com/iggyikenna/market-tick-data-handler.git

# Or if installed locally
cd market-tick-data-handler
git pull
pip install -e .
```

## 📝 License

MIT License - see [LICENSE](LICENSE) file for details.