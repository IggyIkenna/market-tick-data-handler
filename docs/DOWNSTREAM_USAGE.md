# Downstream Usage Guide

This guide demonstrates how to use the market-tick-data-handler package for downstream services and applications.

## Secret Manager Quick Start

For secure API key management, the package supports Google Cloud Secret Manager:

### Quick Setup (Tardis API Key)
```bash
# 1. Set up Tardis API key in Secret Manager
./scripts/setup-secret-manager.sh --project-id YOUR_PROJECT_ID --api-key TD.your_key

# 2. Configure environment
export USE_SECRET_MANAGER=true
export GCP_PROJECT_ID=YOUR_PROJECT_ID
export TARDIS_SECRET_NAME=tardis-api-key

# 3. Use in code (automatic)
from config import get_config
config = get_config()  # Automatically retrieves from Secret Manager
```

### Quick Setup (Other Secrets)
```python
from src.utils.secret_manager_utils import SecretManagerUtils

# Initialize
sm = SecretManagerUtils("YOUR_PROJECT_ID")

# Upload secrets
sm.upload_api_key("binance", "your-key", "Binance API key")
sm.upload_trading_keys("binance", {"api_key": "key", "secret_key": "secret"})

# Retrieve secrets
api_key = sm.get_api_key("binance")
trading_keys = sm.get_trading_keys("binance")
```

**📚 For detailed setup instructions, see [Secret Manager Setup Guide](SECRET_MANAGER_SETUP.md)**

## Table of Contents

1. [Package Overview](#package-overview)
2. [Installation and Setup](#installation-and-setup)
3. [Instrument Services](#instrument-services)
4. [Secret Manager Utilities](#secret-manager-utilities)
5. [Command Line Tools](#command-line-tools)
6. [API Integration Examples](#api-integration-examples)
7. [Deployment Integration](#deployment-integration)
8. [Best Practices](#best-practices)

## Package Overview

The market-tick-data-handler package provides comprehensive functionality for:

- **Instrument Definition Management**: Query, filter, and analyze instrument definitions
- **Secret Management**: Secure storage and retrieval of API keys and trading credentials
- **Data Access**: Clean interfaces for accessing GCS-stored market data
- **Tardis Integration**: Formatted data for Tardis API queries

## Installation and Setup

### Prerequisites

```bash
# Install required packages
pip install -r requirements.txt

# Set up Google Cloud credentials
export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/service-account-key.json"

# Set up environment variables
export GCP_PROJECT_ID="your-project-id"
export GCP_BUCKET="your-bucket-name"
```

### Basic Setup

```python
from config import get_config
from src.instrument_services import InstrumentInspector, InstrumentLister
from src.utils.secret_manager_utils import SecretManagerUtils

# Initialize services
config = get_config()
inspector = InstrumentInspector(config.gcp.bucket)
lister = InstrumentLister(config.gcp.bucket)
secret_manager = SecretManagerUtils(config.gcp.project_id)
```

## Instrument Services

### Instrument Inspector

The `InstrumentInspector` service provides detailed inspection of individual instruments.

#### Basic Usage

```python
from datetime import datetime, timezone
from src.instrument_services import InstrumentInspector

inspector = InstrumentInspector("your-bucket-name")

# Inspect a specific instrument
result = inspector.inspect_instrument(
    instrument_id="DERIBIT:OPTION:BTC-USD-241225-50000-CALL",
    date=datetime(2023, 8, 29, tzinfo=timezone.utc)
)

if result['success']:
    print(result['formatted_attributes'])
    instrument = result['instrument']
    print(f"Tardis Symbol: {instrument.tardis_symbol}")
    print(f"Data Types: {instrument.data_types}")
else:
    print(f"Error: {result['error']}")
    if 'similar_instruments' in result:
        print(f"Similar instruments: {result['similar_instruments']}")
```

#### Advanced Usage

```python
# Inspect with summary
result = inspector.inspect_instrument(
    instrument_id="BINANCE:SPOT_PAIR:BTC-USDT",
    date=datetime(2023, 8, 29, tzinfo=timezone.utc),
    show_summary=True
)

if result['success']:
    print("Instrument found!")
    print(result['formatted_attributes'])
    
    # Access summary information
    summary = result['summary']
    print(f"Total instruments available: {summary['total_instruments']}")
    print(f"Venue breakdown: {summary['by_venue_type']}")
else:
    print(f"Error: {result['error']}")

# Get similar instruments
similar = inspector.get_similar_instruments(
    instrument_id="DERIBIT:OPTION:BTC-USD",
    date=datetime(2023, 8, 29, tzinfo=timezone.utc),
    limit=5
)
print(f"Similar instruments: {similar}")
```

### Instrument Lister

The `InstrumentLister` service provides filtering and listing capabilities for instrument definitions.

#### Basic Listing

```python
from src.instrument_services import InstrumentLister

lister = InstrumentLister("your-bucket-name")

# List all instruments for a date
result = lister.list_instruments(
    date=datetime(2023, 8, 29, tzinfo=timezone.utc)
)

print(f"Found {result['total_instruments']} instruments")
for instrument in result['instruments']:
    print(f"  {instrument['instrument_key']}")
```

#### Filtered Listing

```python
# Filter by venue and instrument type
result = lister.list_instruments(
    date=datetime(2023, 8, 29, tzinfo=timezone.utc),
    venue="deribit",
    instrument_type="option"
)

# Filter by underlying asset and expiry
result = lister.list_instruments(
    date=datetime(2023, 8, 29, tzinfo=timezone.utc),
    underlying="BTC-USD",
    expiry="2024-06-28"
)

# Get options chain for specific underlying and expiry
result = lister.list_instruments(
    date=datetime(2023, 8, 29, tzinfo=timezone.utc),
    instrument_type="option",
    underlying="BTC-USD",
    expiry="2024-06-28",
    limit=10
)
```

#### Tardis Integration

```python
# Get instruments formatted for Tardis API
result = lister.list_instruments(
    date=datetime(2023, 8, 29, tzinfo=timezone.utc),
    instrument_type="option",
    underlying="BTC-USD",
    format_type="tardis"
)

for instrument in result['instruments']:
    print(f"Tardis Symbol: {instrument['tardis_symbol']}")
    print(f"Tardis Exchange: {instrument['tardis_exchange']}")
    print(f"Data Types: {instrument['data_types']}")
    print(f"Available From: {instrument['available_from']}")
    print(f"Available To: {instrument['available_to']}")
    print()
```

#### Statistics

```python
# Get instrument statistics
stats = lister.get_statistics(datetime(2023, 8, 29, tzinfo=timezone.utc))

print(f"Total instruments: {stats['total_instruments']}")
print(f"By venue: {stats['by_venue']}")
print(f"By type: {stats['by_instrument_type']}")
print(f"Top base assets: {stats['top_base_assets']}")
```

## Secret Manager Utilities

The `SecretManagerUtils` class provides secure management of API keys and trading credentials.

### API Key Management

```python
from src.utils.secret_manager_utils import SecretManagerUtils

secret_manager = SecretManagerUtils("your-project-id")

# Upload API keys
success = secret_manager.upload_api_key(
    service_name="tardis",
    api_key="your-tardis-api-key",
    description="Tardis API key for market data access"
)

# Retrieve API keys
api_key = secret_manager.get_api_key("tardis")
if api_key:
    print(f"Tardis API key: {api_key[:8]}...{api_key[-4:]}")
```

### Trading Keys Management

```python
# Upload trading keys for an exchange
trading_keys = {
    "api_key": "your-api-key",
    "secret_key": "your-secret-key",
    "testnet": True
}

success = secret_manager.upload_trading_keys(
    exchange="binance",
    keys=trading_keys,
    description="Binance trading keys for automated trading"
)

# Retrieve trading keys
keys = secret_manager.get_trading_keys("binance")
if keys:
    print(f"Binance API key: {keys['api_key'][:8]}...")
    print(f"Testnet mode: {keys['testnet']}")
```

### Configuration Management

```python
# Upload configuration
config_data = {
    "tardis": {
        "base_url": "https://api.tardis.dev",
        "timeout": 30
    },
    "exchanges": {
        "binance": {"rate_limit": 1200},
        "deribit": {"rate_limit": 1000}
    }
}

success = secret_manager.upload_config(
    config_name="trading",
    config_data=config_data
)

# Retrieve configuration
config = secret_manager.get_config("trading")
if config:
    print(f"Tardis URL: {config['tardis']['base_url']}")
```

### Secret Discovery

```python
# List all secrets
api_keys = secret_manager.list_api_keys()
trading_keys = secret_manager.list_trading_keys()
configs = secret_manager.list_configs()

print(f"Available API keys: {api_keys}")
print(f"Available trading keys: {trading_keys}")
print(f"Available configs: {configs}")
```

## Command Line Tools

The package includes command-line tools for easy interaction.

### Instrument Inspection

```bash
# Inspect a specific instrument
python examples/inspect_instrument.py DERIBIT:OPTION:BTC-USD-241225-50000-CALL --date 2023-08-29

# Inspect with summary
python examples/inspect_instrument.py BINANCE:SPOT_PAIR:BTC-USDT --date 2023-08-29 --summary

# Verbose output
python examples/inspect_instrument.py DERIBIT:FUTURE:BTC-USD-240628 --date 2023-08-29 --verbose
```

### Instrument Listing

```bash
# List all instruments
python examples/list_instruments.py --date 2023-08-29

# Filter by venue
python examples/list_instruments.py --date 2023-08-29 --venue deribit

# Filter by instrument type
python examples/list_instruments.py --date 2023-08-29 --instrument-type option

# Get options chain
python examples/list_instruments.py --date 2023-08-29 --instrument-type option --underlying BTC-USD --expiry 2024-06-28

# Tardis format
python examples/list_instruments.py --date 2023-08-29 --format tardis --limit 10

# JSON output
python examples/list_instruments.py --date 2023-08-29 --format json --limit 5

# Statistics
python examples/list_instruments.py --date 2023-08-29 --stats
```

### Examples

```bash
# Run Tardis lookup example
python examples/tardis_lookup_example.py

# Run secret manager example
python examples/secret_manager_example.py

# Run package usage example
python examples/package_usage_example.py
```

## API Integration Examples

### Tardis API Integration

```python
import requests
from src.instrument_services import InstrumentLister

# Get instruments formatted for Tardis
lister = InstrumentLister("your-bucket-name")
result = lister.list_instruments(
    date=datetime(2023, 8, 29, tzinfo=timezone.utc),
    instrument_type="option",
    underlying="BTC-USD",
    format_type="tardis"
)

# Query Tardis API
for instrument in result['instruments']:
    tardis_symbol = instrument['tardis_symbol']
    tardis_exchange = instrument['tardis_exchange']
    data_types = instrument['data_types'].split(',')
    
    # Make Tardis API request
    url = f"https://api.tardis.dev/v1/data/{tardis_exchange}/{tardis_symbol}"
    params = {
        'data_types': data_types,
        'start_date': '2023-08-29',
        'end_date': '2023-08-29'
    }
    
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        print(f"Retrieved data for {tardis_symbol}: {len(data)} records")
```

### Trading System Integration

```python
from src.utils.secret_manager_utils import SecretManagerUtils
from src.instrument_services import InstrumentLister

# Get trading keys
secret_manager = SecretManagerUtils("your-project-id")
trading_keys = secret_manager.get_trading_keys("binance")

if trading_keys:
    # Initialize trading client
    from binance.client import Client
    client = Client(
        api_key=trading_keys['api_key'],
        api_secret=trading_keys['secret_key'],
        testnet=trading_keys['testnet']
    )
    
    # Get instruments to trade
    lister = InstrumentLister("your-bucket-name")
    result = lister.list_instruments(
        date=datetime.now(timezone.utc),
        venue="binance",
        instrument_type="perpetual",
        format_type="tardis"
    )
    
    # Execute trades
    for instrument in result['instruments']:
        symbol = instrument['tardis_symbol']
        # ... trading logic here
```

### Data Pipeline Integration

```python
from src.instrument_services import InstrumentLister
from src.data_client.data_client import DataClient

# Get instruments for data processing
lister = InstrumentLister("your-bucket-name")
result = lister.list_instruments(
    date=datetime(2023, 8, 29, tzinfo=timezone.utc),
    venue="deribit",
    instrument_type="option",
    format_type="tardis"
)

# Process each instrument
data_client = DataClient("your-bucket-name", config)
for instrument in result['instruments']:
    # Get tick data paths
    file_paths = await data_client.get_tick_data_paths(
        instrument_ids=[instrument['instrument_key']],
        start_date=datetime(2023, 8, 29, tzinfo=timezone.utc),
        end_date=datetime(2023, 8, 29, tzinfo=timezone.utc),
        data_types=instrument['data_types'].split(',')
    )
    
    # Process files
    for file_path in file_paths:
        # ... data processing logic here
        pass
```

## Deployment Integration

### VM Deployment

For VM-based batch execution, the package can be integrated into deployment scripts:

```bash
#!/bin/bash
# deploy-trading-service.sh

# Install package
pip install -r requirements.txt

# Set up secrets
python examples/secret_manager_example.py

# Run batch processing
python -c "
from src.instrument_services import InstrumentLister
from datetime import datetime, timezone

lister = InstrumentLister('your-bucket-name')
result = lister.list_instruments(
    date=datetime.now(timezone.utc),
    instrument_type='option',
    format_type='tardis'
)

# Process instruments
for instrument in result['instruments']:
    print(f'Processing {instrument[\"instrument_key\"]}')
    # ... batch processing logic
"
```

### Docker Integration

```dockerfile
FROM python:3.9-slim

# Install package
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy source code
COPY src/ /app/src/
COPY config.py /app/
COPY examples/ /app/examples/

# Set up environment
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/credentials.json
ENV GCP_PROJECT_ID=your-project-id
ENV GCP_BUCKET=your-bucket-name

# Run service
CMD ["python", "examples/package_usage_example.py"]
```

### Kubernetes Integration

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: market-data-processor
spec:
  replicas: 3
  selector:
    matchLabels:
      app: market-data-processor
  template:
    metadata:
      labels:
        app: market-data-processor
    spec:
      containers:
      - name: processor
        image: your-registry/market-data-processor:latest
        env:
        - name: GOOGLE_APPLICATION_CREDENTIALS
          value: "/app/credentials.json"
        - name: GCP_PROJECT_ID
          value: "your-project-id"
        - name: GCP_BUCKET
          value: "your-bucket-name"
        volumeMounts:
        - name: credentials
          mountPath: /app/credentials.json
          subPath: credentials.json
      volumes:
      - name: credentials
        secret:
          secretName: gcp-credentials
```

## Best Practices

### Error Handling

```python
from src.instrument_services import InstrumentLister
import logging

logger = logging.getLogger(__name__)

try:
    lister = InstrumentLister("your-bucket-name")
    result = lister.list_instruments(date=datetime.now(timezone.utc))
    
    if 'error' in result:
        logger.error(f"Error listing instruments: {result['error']}")
        # Handle error appropriately
    else:
        # Process instruments
        for instrument in result['instruments']:
            # ... processing logic
            pass
            
except Exception as e:
    logger.error(f"Failed to initialize instrument lister: {e}")
    # Handle initialization error
```

### Caching

```python
from functools import lru_cache
from src.instrument_services import InstrumentLister

class CachedInstrumentLister:
    def __init__(self, bucket_name):
        self.lister = InstrumentLister(bucket_name)
    
    @lru_cache(maxsize=128)
    def get_instruments_cached(self, date_str, venue=None, instrument_type=None):
        date = datetime.fromisoformat(date_str)
        return self.lister.list_instruments(
            date=date,
            venue=venue,
            instrument_type=instrument_type
        )

# Usage
cached_lister = CachedInstrumentLister("your-bucket-name")
result = cached_lister.get_instruments_cached(
    "2023-08-29T00:00:00+00:00",
    venue="deribit",
    instrument_type="option"
)
```

### Configuration Management

```python
from src.utils.secret_manager_utils import SecretManagerUtils
import os

class ConfigManager:
    def __init__(self, project_id):
        self.secret_manager = SecretManagerUtils(project_id)
        self._config_cache = {}
    
    def get_config(self, config_name):
        if config_name not in self._config_cache:
            config = self.secret_manager.get_config(config_name)
            if config:
                self._config_cache[config_name] = config
            else:
                # Fallback to environment variables
                self._config_cache[config_name] = self._get_env_config(config_name)
        
        return self._config_cache[config_name]
    
    def _get_env_config(self, config_name):
        # Fallback configuration from environment variables
        return {
            "tardis": {
                "base_url": os.getenv("TARDIS_BASE_URL", "https://api.tardis.dev"),
                "timeout": int(os.getenv("TARDIS_TIMEOUT", "30"))
            }
        }

# Usage
config_manager = ConfigManager("your-project-id")
config = config_manager.get_config("trading")
```

### Monitoring and Logging

```python
import logging
from src.instrument_services import InstrumentLister

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

class MonitoredInstrumentLister(InstrumentLister):
    def list_instruments(self, *args, **kwargs):
        start_time = time.time()
        try:
            result = super().list_instruments(*args, **kwargs)
            duration = time.time() - start_time
            logger.info(f"Listed {result['total_instruments']} instruments in {duration:.2f}s")
            return result
        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Failed to list instruments after {duration:.2f}s: {e}")
            raise

# Usage
lister = MonitoredInstrumentLister("your-bucket-name")
```

This comprehensive guide provides everything needed to integrate the market-tick-data-handler package into downstream services and applications.
