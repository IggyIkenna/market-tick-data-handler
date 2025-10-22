# Config.py Usage Analysis - 4 Deployment Modes

## Overview

`config.py` is the **core configuration module** used across all 4 deployment modes for both instrument definitions and tick download operations. It provides centralized configuration management through environment variables and file-based settings.

## 4 Deployment Modes

### 1. **Local Python Execution** (No Docker)
- **Purpose**: Direct Python execution for development/testing
- **Script**: `./scripts/local/run-main.sh`
- **Entry Point**: `python -m src.main`

### 2. **Local Docker Development**
- **Purpose**: Local development with Docker containers
- **Scripts**: 
  - `./scripts/local/run-main.sh` (calls Docker)
  - `docker/instrument-generation/Dockerfile`
  - `docker/tardis-download/Dockerfile`

### 3. **VM Deployment - Development/Testing**
- **Purpose**: Single VM deployment for testing
- **Script**: `scripts/vm_data_downloader.py`
- **Scale**: 1 VM at a time

### 4. **VM Deployment - Production Orchestration**
- **Purpose**: Massive scale production processing (60+ VMs)
- **Script**: `scripts/vm_data_downloader.py` (in containers)
- **Docker**: `deploy/orchestration/Dockerfile`
- **Scale**: 60+ VMs simultaneously with sharding

## Config.py Usage by Mode

### **Mode 1: Local Python Execution**

#### **Instrument Definitions**
```python
# Entry Point: src/main.py
from config import get_config

# Used by:
- src/main.py (ModeHandler base class)
- src/instrument_processor/canonical_key_generator.py
- src/instrument_processor/gcs_uploader.py

# Configuration accessed:
- config.tardis.api_key
- config.gcp.bucket
- config.gcp.project_id
- config.gcp.credentials_path
```

#### **Tick Download**
```python
# Entry Point: src/main.py
from config import get_config

# Used by:
- src/main.py (ModeHandler base class)
- src/data_downloader/download_orchestrator.py
- src/data_downloader/tardis_connector.py

# Configuration accessed:
- config.tardis.api_key
- config.tardis.base_url
- config.tardis.timeout
- config.tardis.max_concurrent
- config.tardis.rate_limit_per_vm
- config.gcp.bucket
```

### **Mode 2: Local Docker Development**

#### **Instrument Definitions**
```dockerfile
# docker/instrument-generation/Dockerfile
CMD ["python", "-m", "src.main", "--mode", "instruments", ...]

# Uses same config.py as Mode 1
# Environment variables passed via docker-compose.yml:
- TARDIS_API_KEY
- GCP_PROJECT_ID
- GCS_BUCKET
- GCS_REGION
```

#### **Tick Download**
```dockerfile
# docker/tardis-download/Dockerfile
CMD ["python", "-m", "src.main", "--mode", "download", ...]

# Uses same config.py as Mode 1
# Environment variables passed via docker-compose.yml:
- TARDIS_API_KEY
- GCP_PROJECT_ID
- GCS_BUCKET
- GCS_REGION
- DOWNLOAD_VENUES
- DOWNLOAD_INSTRUMENT_TYPES
- DOWNLOAD_DATA_TYPES
```

### **Mode 3: VM Deployment - Development/Testing**

#### **Instrument Definitions**
```python
# scripts/vm_data_downloader.py (when used for instruments)
from config import get_config

# Used by:
- scripts/vm_data_downloader.py
- src/instrument_processor/canonical_key_generator.py
- src/instrument_processor/gcs_uploader.py

# Configuration accessed:
- config.tardis.api_key
- config.gcp.bucket
- config.gcp.project_id
- config.gcp.credentials_path
- config.sharding.shard_index
- config.sharding.total_shards
```

#### **Tick Download**
```python
# scripts/vm_data_downloader.py
from config import get_config

# Used by:
- scripts/vm_data_downloader.py
- src/data_downloader/download_orchestrator.py
- src/data_downloader/tardis_connector.py

# Configuration accessed:
- config.tardis.api_key
- config.tardis.base_url
- config.tardis.timeout
- config.tardis.max_concurrent
- config.tardis.rate_limit_per_vm
- config.gcp.bucket
- config.sharding.shard_index
- config.sharding.total_shards
- config.sharding.instruments_per_shard
```

### **Mode 4: VM Deployment - Production Orchestration**

#### **Instrument Definitions**
```python
# deploy/orchestration/Dockerfile -> scripts/vm_data_downloader.py
from config import get_config

# Used by:
- scripts/vm_data_downloader.py (in container)
- src/instrument_processor/canonical_key_generator.py
- src/instrument_processor/gcs_uploader.py

# Configuration accessed:
- config.tardis.api_key
- config.gcp.bucket
- config.gcp.project_id
- config.gcp.credentials_path
- config.sharding.shard_index
- config.sharding.total_shards
- config.sharding.instruments_per_shard
- config.service.log_level
- config.service.max_concurrent_requests
```

#### **Tick Download**
```python
# deploy/orchestration/Dockerfile -> scripts/vm_data_downloader.py
from config import get_config

# Used by:
- scripts/vm_data_downloader.py (in container)
- src/data_downloader/download_orchestrator.py
- src/data_downloader/tardis_connector.py

# Configuration accessed:
- config.tardis.api_key
- config.tardis.base_url
- config.tardis.timeout
- config.tardis.max_concurrent
- config.tardis.rate_limit_per_vm
- config.gcp.bucket
- config.sharding.shard_index
- config.sharding.total_shards
- config.sharding.instruments_per_shard
- config.service.log_level
- config.service.max_concurrent_requests
- config.service.batch_size
- config.service.memory_efficient
```

## Core Modules Using Config.py

### **1. src/main.py**
```python
from config import get_config

# Used for:
- ModeHandler base class initialization
- GCS bucket configuration
- Tardis API key configuration
- All 4 deployment modes
```

### **2. src/instrument_processor/canonical_key_generator.py**
```python
from config import get_config

# Used for:
- Tardis API key for instrument data fetching
- All 4 deployment modes
```

### **3. src/data_downloader/tardis_connector.py**
```python
from config import get_config

# Used for:
- Tardis API configuration (API key, base URL, timeout, etc.)
- Rate limiting configuration
- All 4 deployment modes
```

### **4. src/data_downloader/download_orchestrator.py**
```python
# Indirectly uses config.py through tardis_connector.py
# Used for:
- GCS bucket configuration
- All 4 deployment modes
```

### **5. scripts/vm_data_downloader.py**
```python
from config import get_config

# Used for:
- VM-specific configuration (sharding, etc.)
- Modes 3 and 4 only
```

## Configuration Sources by Mode

### **Mode 1: Local Python**
- **Primary**: `.env` file
- **Fallback**: System environment variables
- **File**: `config.py` loads from `.env`

### **Mode 2: Local Docker**
- **Primary**: `docker-compose.yml` environment variables
- **Fallback**: `.env` file (if mounted)
- **File**: `config.py` loads from environment

### **Mode 3: VM Development**
- **Primary**: VM environment variables
- **Fallback**: `.env` file (if present)
- **File**: `config.py` loads from environment

### **Mode 4: VM Production**
- **Primary**: Container environment variables
- **Fallback**: System environment variables
- **File**: `config.py` loads from environment

## Environment Variables Used

### **Core Configuration**
```bash
# Tardis API
TARDIS_API_KEY=TD.xxx...
TARDIS_BASE_URL=https://datasets.tardis.dev
TARDIS_TIMEOUT=30
TARDIS_MAX_RETRIES=3
MAX_CONCURRENT_REQUESTS=2
RATE_LIMIT_PER_VM=1000000

# GCP Configuration
GCP_PROJECT_ID=central-element-323112
GCS_BUCKET=market-data-tick
GCS_REGION=asia-northeast1-c
GCP_CREDENTIALS_PATH=/path/to/credentials.json

# Service Configuration
LOG_LEVEL=INFO
LOG_DESTINATION=local
BATCH_SIZE=1000
MEMORY_EFFICIENT=false
ENABLE_CACHING=true
CACHE_TTL=3600
```

### **Mode-Specific Configuration**
```bash
# Instrument Generation
INSTRUMENT_EXCHANGES=binance,deribit,bybit,okex
INSTRUMENT_START_DATE=2023-05-23
INSTRUMENT_END_DATE=2023-05-25

# Tick Download
DOWNLOAD_VENUES=deribit,binance-futures
DOWNLOAD_INSTRUMENT_TYPES=option,perpetual,spot
DOWNLOAD_DATA_TYPES=trades,book_snapshot_5
DOWNLOAD_MAX_INSTRUMENTS=10

# Sharding (Modes 3 & 4)
SHARD_INDEX=0
TOTAL_SHARDS=60
INSTRUMENTS_PER_SHARD=2
```

## Summary

**`config.py` is used by ALL 4 deployment modes** for both instrument definitions and tick download:

1. **Mode 1 (Local Python)**: Direct import in `src/main.py` and related modules
2. **Mode 2 (Local Docker)**: Same as Mode 1, but in Docker containers
3. **Mode 3 (VM Dev)**: Used in `scripts/vm_data_downloader.py` for single VM
4. **Mode 4 (VM Prod)**: Used in `scripts/vm_data_downloader.py` for 60+ VMs

**Key modules using config.py**:
- `src/main.py` (all modes)
- `src/instrument_processor/canonical_key_generator.py` (all modes)
- `src/data_downloader/tardis_connector.py` (all modes)
- `scripts/vm_data_downloader.py` (modes 3 & 4)

**Configuration sources**:
- Environment variables (primary)
- `.env` files (fallback)
- YAML config files (optional)

The centralized `config.py` ensures consistent configuration across all deployment modes while allowing mode-specific overrides through environment variables.
