# Market Data Handler - VM Deployment Guide

## Overview
This guide covers VM deployment for the Market Tick Data Handler with support for batch processing modes including instrument generation, data downloads, candle processing, and BigQuery uploads.

## Current Architecture (Refactored December 2024)
- **Package/Library Architecture**: Clean internal package for downstream services
- **VM Deployments**: Batch processing jobs only (no live streaming services)
- **VM Type**: e2-standard-8 (8 vCPUs, 32GB RAM, 16 Gbps network)
- **Performance**: 8 workers, 500 concurrent downloads, 200 parallel uploads
- **Data Storage**: GCS bucket with organized partition strategy
- **Entry Point**: `src/main.py` with multiple batch processing modes
- **Live Streaming**: Node.js services (not VM-deployed)

## Key Files
- `deploy/vm/deploy-download.sh` - Single VM for download mode
- `deploy/vm/shard-deploy-download.sh` - Multiple VMs with date-based sharding for download
- `deploy/vm/deploy-candle-processing.sh` - Candle processing deployment
- `deploy/vm/deploy-bigquery-upload.sh` - BigQuery upload deployment
- `deploy/vm/logs-pull.sh` - Log aggregation and monitoring from Cloud Logging
- `deploy/vm/check-gcs.sh` - GCS data validation and completeness checking
- `deploy/vm/build-images.sh` - Docker image build and push
- `docs/BUDGET_ANALYSIS.md` - Cost analysis and budget planning

## VM Naming Convention (Updated)
- **Single VM**: `market-data-single-YYYYMMDD` (e.g., `market-data-single-20230602`)
- **Sharded VMs**: `market-data-shard-N` (e.g., `market-data-shard-0`, `market-data-shard-1`)
- **Monitoring Pattern**: `market-data-*` for all VMs

## Quick Start Commands (Refactored Architecture)

### Batch Processing Deployments

#### 1. Data Download (Single VM)
```bash
./deploy/vm/deploy-download.sh deploy \
    --start-date 2023-06-02 \
    --end-date 2023-06-02 \
    --venues binance,binance-futures,deribit,bybit,bybit-spot,okex,okex-futures,okex-swap \
    --data-types trades,book_snapshot_5,derivative_ticker,liquidations,options_chain
```

#### 2. Candle Processing (Single VM)
```bash
./deploy/vm/deploy-candle-processing.sh deploy \
    --start-date 2024-01-01 \
    --end-date 2024-01-01
```

#### 3. BigQuery Upload (Single VM)
```bash
./deploy/vm/deploy-bigquery-upload.sh deploy \
    --start-date 2024-01-01 \
    --end-date 2024-01-01
```

#### 4. Sharded Data Download (Production)
```bash
./deploy/vm/shard-deploy-download.sh deploy \
    --start-date 2023-05-23 \
    --end-date 2024-10-22 \
    --shards 100 \
    --venues binance,binance-futures,deribit,bybit,bybit-spot,okex,okex-futures,okex-swap \
    --data-types trades,book_snapshot_5,derivative_ticker,liquidations,options_chain
```

### Live Streaming (Local Development)
```bash
# Stream raw ticks to BigQuery
./deploy/local/run-main.sh streaming-ticks --symbol BTC-USDT

# Stream real-time candles with HFT features
./deploy/local/run-main.sh streaming-candles --symbol BTC-USDT,ETH-USDT
```

### Monitoring & Observability
```bash
# Monitor all VMs
./deploy/vm/logs-pull.sh --pattern "market-data-*" --follow

# Check specific shard
./deploy/vm/logs-pull.sh market-data-shard-0 --lines 100

# Validate GCS data
./deploy/vm/check-gcs.sh --start-date 2023-05-23 --end-date 2024-10-22 --venue binance

# Fast cleanup
./deploy/vm/shard-deploy-download.sh cleanup
```

## Performance Specifications
- **VM Type**: e2-standard-8 (8 vCPUs, 32GB RAM, 16 Gbps network)
- **Workers**: 8 parallel workers per VM
- **Concurrent Downloads**: 500 (Tardis API)
- **Parallel Uploads**: 200 (GCS)
- **Batch Size**: 5000 instruments
- **Memory Threshold**: 85%
- **Network Utilization**: <1% of available bandwidth (optimization opportunity)

## Critical Pitfalls & Solutions

### 1. Google Cloud SDK Installation ❌➡️✅
**Problem**: Startup script failed with "gcloud command not found"
**Root Cause**: Google Cloud SDK not installed on VM
**Solution**: Added proper SDK installation to startup script:
```bash
# Install Google Cloud SDK
echo "Installing Google Cloud SDK..."
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
apt-get update -y
apt-get install -y google-cloud-cli
```

### 2. Docker Authentication ❌➡️✅
**Problem**: Docker pull failed with "Unauthenticated request"
**Root Cause**: VM couldn't authenticate with Artifact Registry
**Solution**: 
1. Install Google Cloud SDK (see above)
2. Configure Docker authentication:
```bash
gcloud auth configure-docker asia-northeast1-docker.pkg.dev --quiet
```
3. Add IAM permissions:
```bash
gcloud projects add-iam-policy-binding central-element-323112 \
  --member="serviceAccount:1060025368044-compute@developer.gserviceaccount.com" \
  --role="roles/artifactregistry.reader"
```

### 3. GCS Upload Permissions ❌➡️✅
**Problem**: 403 errors during GCS uploads
**Root Cause**: Service account missing delete permissions for resumable uploads
**Solution**: Added comprehensive storage permissions:
```bash
gcloud projects add-iam-policy-binding central-element-323112 \
  --member="serviceAccount:1060025368044-compute@developer.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"
```

### 4. Shard Index Extraction ❌➡️✅
**Problem**: VMs couldn't extract shard index from instance name
**Root Cause**: Incorrect regex pattern for VM name parsing
**Solution**: Fixed regex pattern:
```bash
if [[ $INSTANCE_NAME =~ market-data-vm-([0-9]+)$ ]]; then
    SHARD_INDEX=${BASH_REMATCH[1]}
else
    # Fallback: try to extract any number at the end
    SHARD_INDEX=${INSTANCE_NAME##*-}
    if ! [[ $SHARD_INDEX =~ ^[0-9]+$ ]]; then
        SHARD_INDEX=0
    fi
fi
```

### 5. Credential Mounting Issues ❌➡️✅
**Problem**: `IsADirectoryError` when mounting credentials
**Root Cause**: Attempting to mount file into existing directory
**Solution**: Use VM's default service account instead of explicit credential mounting

### 6. Unused Reserved IPs 💰
**Problem**: 40 reserved IPs costing money unnecessarily
**Solution**: Deleted all unused reserved IPs:
```bash
gcloud compute addresses list --filter="name~market-data-ip" --format="value(name)" | xargs -I {} gcloud compute addresses delete {} --region=asia-northeast1 --quiet
```

## Working Configuration

### VM Configuration
- **Machine Type**: e2-highmem-8 (8 vCPUs, 64GB RAM) for Tardis download
- **Workers**: 2 (leaving 2 vCPUs for system operations)
- **Disk Size**: 100GB (sufficient for large parquet files)
- **Disk Type**: pd-standard
- **Image**: ubuntu-2204-lts
- **Scopes**: https://www.googleapis.com/auth/cloud-platform
- **Preemptible**: Yes (for cost savings)
- **Memory Requirements**: 64GB allows parallel processing of large parquet files

### Environment Variables
```bash
START_DATE="2020-01-01"
END_DATE="2025-10-19"
BATCH_SIZE="7"
MAX_CONCURRENT="4"
INSTRUMENT_TYPE="all"
SHARD_INDEX="${SHARD_INDEX}"
TOTAL_SHARDS="40"
```

### Docker Image
- **Repository**: asia-northeast1-docker.pkg.dev/central-element-323112/market-data-handler/market-data-downloader
- **Tag**: v4
- **Status**: ✅ Working with all fixes

## Usage

### Single VM Deployment (Development/Testing)
```bash
# Deploy single VM for download mode
./deploy/vm/deploy-download.sh deploy --start-date 2023-05-23 --end-date 2023-05-23 --venues binance

# Check VM status
./deploy/vm/deploy-download.sh status --vm-name download-vm-binance-20230523

# View logs
./deploy/vm/logs-pull.sh download-vm-binance-20230523 --lines 50

# Run download on existing VM
./deploy/vm/deploy-download.sh run --start-date 2023-05-23 --end-date 2023-05-23 --venues binance --vm-name download-vm-binance-20230523

# Clean up
./deploy/vm/deploy-download.sh delete --vm-name download-vm-binance-20230523
```

### Sharded VM Deployment (Production)
```bash
# Deploy 4 VMs for 4 days of data (each VM processes one complete day)
./deploy/vm/shard-deploy-download.sh deploy --start-date 2023-05-23 --end-date 2023-05-26 --shards 4 --venues binance

# Check status of all shard VMs
./deploy/vm/shard-deploy-download.sh status

# View logs from all shard VMs
./deploy/vm/logs-pull.sh --pattern "download-shard-binance-*" --follow

# Validate data completeness in GCS
./deploy/vm/check-gcs.sh --start-date 2023-05-23 --end-date 2023-05-26 --venue binance --detailed

# Clean up all shard VMs
./deploy/vm/shard-deploy-download.sh cleanup
```

### Docker Image Management
```bash
# Build and push Docker images
./deploy/vm/build-images.sh build-and-push --tag v1.0.0

# Clean up local images
./deploy/vm/build-images.sh clean
```

## Monitoring

### Check VM Status
```bash
gcloud compute instances list --filter="name~market-data-vm" --format="table(name,status,machineType,zone)"
```

### Check VM Logs
```bash
gcloud compute instances get-serial-port-output market-data-vm-0 --zone=asia-northeast1-a | tail -20
```

### Check GCS Uploads
```bash
# Check instrument definitions
gsutil ls gs://market-data-tick/instrument_availability/by_date/day-2023-05-23/

# Check tick data
gsutil ls gs://market-data-tick/raw_tick_data/by_date/day-2023-05-23/data_type-trades/

# Validate data completeness using the check script
./deploy/vm/check-gcs.sh --start-date 2023-05-23 --end-date 2023-05-26 --venue binance --detailed
```

## Download Mode Architecture

### Sharding Strategy
- **Date-based Sharding**: Each VM processes one complete day of data
- **Parallel Processing**: Multiple VMs run simultaneously for different days
- **VM Naming**: `download-shard-{venue}-{shard_index}` (e.g., `download-shard-binance-0`)
- **Data Organization**: Data stored in GCS with structure `raw_tick_data/by_date/day-{date}/data_type-{type}/`

### VM Configuration
- **Machine Type**: `e2-highmem-8` (8 vCPUs, 64GB RAM)
- **Disk Size**: 100GB (sufficient for large parquet files)
- **Image**: Ubuntu 22.04 LTS
- **Docker Image**: `market-tick-tardis-downloader:latest`
- **Scopes**: Full cloud platform access for GCS uploads

### Data Flow
1. **VM Creation**: Each VM is created with metadata specifying its shard index and date range
2. **Startup Script**: VM pulls Docker image and runs download for its assigned date
3. **Data Processing**: Downloads tick data for specified venues and data types
4. **GCS Upload**: Uploads processed data to organized GCS structure
5. **Logging**: All operations logged to Cloud Logging with proper tagging

### Monitoring & Validation
- **Log Aggregation**: `logs-pull.sh` pulls logs from Cloud Logging
- **Data Validation**: `check-gcs.sh` verifies data completeness and file counts
- **Status Monitoring**: Real-time VM status and process monitoring
- **Error Handling**: Comprehensive error checking and user feedback

## Cost Optimization
- ✅ **Preemptible VMs**: ~80% cost savings
- ✅ **No reserved IPs**: Using ephemeral IPs
- ✅ **Right-sized disks**: 100GB sufficient for data volume
- ✅ **Efficient sharding**: 1 VM per day for optimal parallelization

## Next Steps for 4-Tier Plan

### Tier 1: Current (40 instruments)
- ✅ 20 Spot USDT pairs
- ✅ 20 Perpetual USDT pairs
- **Status**: Working perfectly

### Tier 2: Add 20 Spot USD pairs
```bash
# Deploy additional 20 VMs for spot USD pairs
./orchestrate-market-data-download.sh --instances 20 --start-shard 40 --end-shard 59
```

### Tier 3: Cloud Run Daily Jobs
```bash
# Setup daily Cloud Run jobs for all 60 instruments
./orchestrate-market-data-download.sh --type cloudrun --instances 60
./setup-daily-scheduler.sh
```

### Tier 4: Query Service
- Deploy query service for real-time data access
- Setup monitoring and alerting
- Implement data validation

## ⚠️ CRITICAL PITFALLS & SOLUTIONS

### 🔴 DATE PARSING ISSUE - CRITICAL ⚠️
**Problem**: Date strings from environment variables (`START_DATE`, `END_DATE`) are parsed using `datetime.strptime()` which creates **naive datetime objects** interpreted as **local timezone** (JST in Tokyo region).

**Symptoms**:
- Data starts at correct UTC time but ends 9 hours later (Tokyo timezone)
- Date boundaries are in JST but candle timestamps are UTC
- Creates misalignment between date partitions and actual data timestamps

**Root Cause**:
- `datetime.strptime("2020-01-01", "%Y-%m-%d")` creates naive datetime (no timezone)
- In Tokyo VMs, naive datetimes are interpreted as JST (UTC+9)
- But candle timestamps use `datetime.utcfromtimestamp()` (UTC)
- This creates a 9-hour offset between date boundaries and candle data

**Solution**:
1. **Set VM timezone to UTC** (already done):
   ```bash
   timedatectl set-timezone UTC
   ```

2. **Use UTC-aware date parsing**:
   ```python
   # ❌ WRONG - creates naive datetime (interpreted as local timezone)
   start_dt = datetime.strptime(start_date, "%Y-%m-%d")
   
   # ✅ CORRECT - explicitly UTC
   start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=None)  # Naive UTC
   ```

3. **Fixed in**: `deploycloud_run_downloader.py` lines 113-114

**Verification**:
```bash
# Check if date boundaries align with candle timestamps
gcloud storage cat gs://market-data-candles/data/2020-01-01/1m/binance-spot/BTC_USDT/2020-01-01-1m.csv | head -5
# Should show timestamps starting at 00:00:00 UTC, not 09:00:00
```

**Impact**: This bug causes data misalignment and makes backtesting unreliable!

---

### 🔴 COMPREHENSIVE TIMEZONE FIXES - CRITICAL ⚠️

**Problem**: Multiple timezone-related issues affect data consistency in non-UTC regions (e.g., Tokyo `asia-northeast1-a`).

**Issues Fixed**:

1. **Candle Timestamp Conversion**:
   - **Before**: `datetime.fromtimestamp(candle[0] / 1000)` used local timezone (JST)
   - **After**: `datetime.utcfromtimestamp(candle[0] / 1000)` uses UTC
   - **Files**: `binance_connector.py`

2. **Date String Parsing**:
   - **Before**: `datetime.strptime("2020-01-01", "%Y-%m-%d")` created naive datetime interpreted as local time
   - **After**: `datetime.strptime("2020-01-01", "%Y-%m-%d").replace(tzinfo=None)` creates naive UTC datetime
   - **Files**: All scripts with date parsing (see comprehensive list below)

3. **VM System Timezone**:
   - **Before**: VM timezone was set to local region (JST for Tokyo)
   - **After**: VM timezone set to UTC in startup script
   - **Files**: `vm-startup-script-fixed.sh`

**Files Updated with Timezone Fixes**:
- `binance_connector.py` - Candle timestamp conversion
- `deploycloud_run_downloader.py` - Date parsing
- `market_data_query_service.py` - Date parsing
- `deploydownload_and_aggregate.py` - Date parsing
- `market_data_service.py` - Date parsing
- `gcs_manager.py` - Date parsing
- `cli.py` - Date parsing
- `api.py` - Date parsing
- `vm-startup-script-fixed.sh` - VM timezone setting

**Testing**: Run `python -m pytest tests/test_timezone_handling.py -v` to verify all timezone fixes.

**Verification**:
```bash
# Check if timestamps are correct (should show 00:00:00 to 23:55:00 for a day)
gcloud storage cat gs://market-data-candles/data/2020-01-01/5m/binance-spot/BTC_USDT/2020-01-01-5m.csv | head -20
```

**Impact**: These fixes ensure ALL downloaded data is consistent and usable for backtesting!

---

## Troubleshooting

### Common Issues
1. **VM startup fails**: Check serial port logs for authentication errors
2. **GCS upload fails**: Verify IAM permissions
3. **Docker pull fails**: Check Artifact Registry permissions
4. **Shard mismatch**: Verify VM naming convention

### Debug Commands
```bash
# Check VM logs
gcloud compute instances get-serial-port-output VM_NAME --zone=asia-northeast1-a

# Check IAM permissions
gcloud projects get-iam-policy central-element-323112

# Check Cloud Run job logs
gcloud run jobs executions list --job=market-data-job-0 --region=asia-northeast1
```

## Lessons Learned
1. **Always test with 1-2 VMs first** before deploying all
2. **Google Cloud SDK installation is critical** for Docker authentication
3. **IAM permissions must be comprehensive** (not just basic storage)
4. **VM naming convention matters** for shard extraction
5. **Reserved IPs cost money** - use ephemeral IPs when possible
6. **Preemptible VMs provide massive cost savings** with minimal risk

## Success Metrics
- ✅ **40 VMs running successfully**
- ✅ **GCS uploads working**
- ✅ **No authentication errors**
- ✅ **Proper sharding (1:1 VM:instrument)**
- ✅ **Cost optimized**
- ✅ **Clean, maintainable scripts**
