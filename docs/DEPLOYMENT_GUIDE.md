# Market Tick Data Handler - Deployment Guide

## Overview

This guide covers the complete deployment process for downloading Tardis tick data for all 60 instruments across 2 dates using VM-based infrastructure.

## Prerequisites

1. **Google Cloud Project**: `central-element-323112`
2. **Service Account**: `1060025368044-compute@developer.gserviceaccount.com`
3. **GCS Bucket**: `market-data-tick`
4. **Artifact Registry**: `asia-northeast1-docker.pkg.dev/central-element-323112/market-tick-handler`
5. **Tardis API Key**: Set in environment variables

## Quick Start

### 1. Setup IAM Permissions

```bash
# Run the IAM setup script
./orchestration/setup-iam.sh
```

This will add the following permissions to the service account:
- `roles/storage.objectAdmin`: GCS upload/download/delete
- `roles/artifactregistry.reader`: Docker image pull
- `roles/compute.instanceAdmin`: VM management

### 2. Build and Push Docker Image

```bash
# Build and push the Docker image
gcloud builds submit --config orchestration/cloudbuild.yaml
```

### 3. Deploy VMs for Data Download

```bash
# Download 2023-05-23 data
./orchestration/orchestrate-tick-download.sh --date 2023-05-23

# Download 2025-10-20 data
./orchestration/orchestrate-tick-download.sh --date 2025-10-20
```

### 4. Monitor Progress

```bash
# Interactive monitoring
./orchestration/monitor-tick-download.sh

# Command line monitoring
./orchestration/monitor-tick-download.sh all 2023-05-23
```

### 5. Cleanup

```bash
# Clean up VMs after completion
./orchestration/cleanup-tick-vms.sh
```

## Detailed Setup

### Phase 1: Environment Setup

1. **Set Environment Variables**:
   ```bash
   export TARDIS_API_KEY="your_tardis_api_key"
   export GCP_PROJECT_ID="central-element-323112"
   export GCP_CREDENTIALS_PATH="central-element-323112-e35fb0ddafe2.json"
   export GCS_BUCKET="market-data-tick"
   ```

2. **Create GCS Bucket** (if not exists):
   ```bash
   gsutil mb gs://market-data-tick
   ```

3. **Enable Required APIs**:
   ```bash
   gcloud services enable compute.googleapis.com
   gcloud services enable storage.googleapis.com
   gcloud services enable artifactregistry.googleapis.com
   gcloud services enable cloudbuild.googleapis.com
   ```

### Phase 2: Docker Image Build

1. **Create Artifact Registry Repository**:
   ```bash
   gcloud artifacts repositories create market-tick-handler \
     --repository-format=docker \
     --location=asia-northeast1 \
     --description="Market Tick Data Handler Docker Images"
   ```

2. **Build and Push Image**:
   ```bash
   gcloud builds submit --config orchestration/cloudbuild.yaml
   ```

### Phase 3: VM Deployment

1. **Deploy for 2023-05-23**:
   ```bash
   ./orchestration/orchestrate-tick-download.sh --date 2023-05-23 --instances 60
   ```

2. **Deploy for 2025-10-20**:
   ```bash
   ./orchestration/orchestrate-tick-download.sh --date 2025-10-20 --instances 60
   ```

## Architecture

### VM Configuration
- **Machine Type**: `e2-highmem-2` (2 vCPU, 16GB RAM)
- **Disk Size**: 100GB pd-standard
- **Image**: ubuntu-2204-lts
- **Preemptible**: Yes (for cost savings)
- **Zone**: asia-northeast1-a

### Sharding Strategy
- **60 VMs**: 1 instrument per VM
- **Shard Assignment**: Round-robin distribution
- **Data Types**: All available per instrument type

### Data Organization
```
gs://market-data-tick/
├── 2023-05-23/
│   ├── trades/
│   │   ├── binance-spot/
│   │   │   ├── BTCUSDT/
│   │   │   └── ETHUSDT/
│   │   └── binance-futures/
│   ├── book_snapshot_5/
│   ├── quotes/
│   ├── derivative_ticker/
│   └── liquidations/
└── 2025-10-20/
    └── ...
```

## Monitoring and Troubleshooting

### VM Status Monitoring

```bash
# Check VM status
gcloud compute instances list --filter="name~tick-data-vm"

# Check specific VM logs
gcloud compute instances get-serial-port-output tick-data-vm-0 --zone=asia-northeast1-a

# Monitor GCS uploads
gsutil ls gs://market-data-tick/2023-05-23/
```

### Common Issues

1. **VM Startup Failures**:
   - Check serial port logs for authentication errors
   - Verify IAM permissions
   - Check Docker image availability

2. **GCS Upload Failures**:
   - Verify storage permissions
   - Check network connectivity
   - Review quota limits

3. **Data Download Failures**:
   - Check Tardis API key validity
   - Verify instrument registry
   - Review rate limiting

### Debug Commands

```bash
# Check IAM permissions
gcloud projects get-iam-policy central-element-323112

# Check Artifact Registry
gcloud artifacts repositories list

# Check GCS bucket
gsutil ls gs://market-data-tick/

# Check VM logs
gcloud compute instances get-serial-port-output VM_NAME --zone=asia-northeast1-a
```

## Cost Estimation

### VM Costs
- **60 preemptible VMs**: ~$0.60/hour
- **Expected runtime**: 1-2 hours per date
- **Total VM cost**: ~$1.20 for both dates

### Storage Costs
- **Data volume**: 600GB-1.2TB
- **GCS storage**: ~$20/month
- **Network egress**: Minimal (first 1TB free in region)

### Total Estimated Cost
- **VM deployment**: ~$1.20
- **Storage (monthly)**: ~$20
- **Total**: ~$21.20

## Data Validation

### Expected Data Types
- **Spot pairs (40)**: trades, book_snapshot_5, quotes
- **Perpetuals (20)**: trades, book_snapshot_5, derivative_ticker, liquidations, quotes

### Data Quality Checks
1. **Timestamp consistency**: All data starts at UTC midnight
2. **Full day coverage**: 24-hour data for each instrument
3. **Parquet format**: Compressed, typed data files
4. **GCS organization**: Proper path structure

### Validation Commands

```bash
# Check data completeness
gsutil ls gs://market-data-tick/2023-05-23/trades/binance-spot/ | wc -l

# Verify Parquet files
gsutil cp gs://market-data-tick/2023-05-23/trades/binance-spot/BTCUSDT/trades_2023-05-23_BTCUSDT_spot.parquet /tmp/
python -c "import pandas as pd; df = pd.read_parquet('/tmp/trades_2023-05-23_BTCUSDT_spot.parquet'); print(f'Records: {len(df)}, Columns: {list(df.columns)}')"
```

## Next Steps

1. **Data Analysis**: Use downloaded data for backtesting and analysis
2. **Query Service**: Implement data querying capabilities
3. **Daily Updates**: Set up automated daily data downloads
4. **Monitoring**: Add alerting for data quality issues

## Support

For issues or questions:
1. Check VM logs using monitoring scripts
2. Review GCS upload status
3. Verify IAM permissions
4. Check Tardis API status

## Files Reference

- `orchestration/orchestrate-tick-download.sh`: Main deployment script
- `orchestration/vm-startup-script.sh`: VM startup configuration
- `orchestration/monitor-tick-download.sh`: Monitoring and debugging
- `orchestration/cleanup-tick-vms.sh`: Cleanup script
- `orchestration/setup-iam.sh`: IAM permissions setup
- `scripts/vm_data_downloader.py`: Main data download logic
- `gcs_upload_service.py`: GCS upload service