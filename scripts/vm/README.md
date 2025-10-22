# VM Deployment Scripts

This directory contains scripts for deploying and managing VMs for the Market Tick Data Handler.

## Overview

The VM deployment is organized into three main scripts:

1. **`deploy-instruments.sh`** - Deploy single VM for instrument definitions
2. **`deploy-tardis.sh`** - Deploy single VM for Tardis tick data download
3. **`shard-deploy.sh`** - Deploy multiple VMs with workload sharding
4. **`build-images.sh`** - Build and push Docker images

## Quick Start

### 1. Build Docker Images

```bash
# Build images locally
./build-images.sh build

# Build and push to Google Container Registry
./build-images.sh build-and-push --tag v1.0.0
```

### 2. Deploy Single VMs

#### Instrument Definitions
```bash
# Deploy VM
./deploy-instruments.sh deploy

# Run instrument generation
./deploy-instruments.sh run --start-date 2023-05-23 --end-date 2023-05-25

# Check status
./deploy-instruments.sh status

# SSH into VM
./deploy-instruments.sh ssh
```

#### Tardis Data Download
```bash
# Deploy VM
./deploy-tardis.sh deploy

# Run data download
./deploy-tardis.sh run --start-date 2023-05-23 --end-date 2023-05-25 --venues deribit,binance

# Check status
./deploy-tardis.sh status

# SSH into VM
./deploy-tardis.sh ssh
```

### 3. Deploy Sharded VMs

#### Instrument Definitions (Multiple VMs)
```bash
# Deploy 5 VMs for 3 days of data
./shard-deploy.sh instruments --start-date 2023-05-23 --end-date 2023-05-25 --shards 5

# Deploy with preemptible instances (cheaper)
./shard-deploy.sh instruments --start-date 2023-05-23 --end-date 2023-05-25 --shards 10 --preemptible
```

#### Tardis Data Download (Multiple VMs)
```bash
# Deploy 10 VMs for 3 days of data
./shard-deploy.sh tardis --start-date 2023-05-23 --end-date 2023-05-25 --shards 10

# Deploy with specific venues and data types
./shard-deploy.sh tardis --start-date 2023-05-23 --end-date 2023-05-25 --shards 10 --venues deribit,binance --data-types trades,book_snapshot_5
```

## Script Details

### deploy-instruments.sh

Manages a single VM for instrument definition generation.

**Commands:**
- `deploy` - Create and configure VM
- `start` - Start the VM
- `stop` - Stop the VM
- `status` - Show VM and process status
- `ssh` - SSH into the VM
- `logs` - View logs
- `run` - Run instrument generation
- `delete` - Delete the VM

**Usage:**
```bash
./deploy-instruments.sh <command> [options]

Options for 'run' command:
  --start-date DATE    Start date (YYYY-MM-DD)
  --end-date DATE      End date (YYYY-MM-DD)
  --venues VENUES      Comma-separated list of venues
```

### deploy-tardis.sh

Manages a single VM for Tardis tick data download.

**Commands:**
- `deploy` - Create and configure VM
- `start` - Start the VM
- `stop` - Stop the VM
- `status` - Show VM and process status
- `ssh` - SSH into the VM
- `logs` - View logs
- `run` - Run data download
- `delete` - Delete the VM

**Usage:**
```bash
./deploy-tardis.sh <command> [options]

Options for 'run' command:
  --start-date DATE    Start date (YYYY-MM-DD)
  --end-date DATE      End date (YYYY-MM-DD)
  --venues VENUES      Comma-separated list of venues
  --data-types TYPES   Comma-separated list of data types
```

### shard-deploy.sh

Deploys multiple VMs with workload distribution.

**Modes:**
- `instruments` - Deploy VMs for instrument definitions
- `tardis` - Deploy VMs for Tardis data download
- `cleanup` - Clean up all sharded VMs

**Options:**
- `--start-date DATE` - Start date (YYYY-MM-DD)
- `--end-date DATE` - End date (YYYY-MM-DD)
- `--shards NUM` - Number of shards/VMs (default: 10)
- `--start-shard NUM` - Starting shard index (default: 0)
- `--end-shard NUM` - Ending shard index (default: shards-1)
- `--venues VENUES` - Comma-separated list of venues (tardis only)
- `--data-types TYPES` - Comma-separated list of data types (tardis only)
- `--preemptible` - Use preemptible instances (cheaper but can be terminated)

**Usage:**
```bash
./shard-deploy.sh <mode> [options]

Examples:
  ./shard-deploy.sh instruments --start-date 2023-05-23 --end-date 2023-05-25 --shards 5
  ./shard-deploy.sh tardis --start-date 2023-05-23 --end-date 2023-05-25 --shards 10 --venues deribit,binance
  ./shard-deploy.sh cleanup
```

### build-images.sh

Builds and pushes Docker images for VM deployment.

**Commands:**
- `build` - Build Docker images locally
- `push` - Push images to Google Container Registry
- `build-and-push` - Build and push images
- `clean` - Clean up local images

**Options:**
- `--tag TAG` - Custom tag for images (default: latest)
- `--no-cache` - Build without using cache

**Usage:**
```bash
./build-images.sh <command> [options]

Examples:
  ./build-images.sh build
  ./build-images.sh build-and-push --tag v1.0.0
  ./build-images.sh clean
```

## Workload Distribution

### Instrument Definitions
- Each VM processes **one full day** of instrument definitions
- Data is uploaded to GCS **daily** (small files, aggregated)
- VMs are named: `instrument-shard-0`, `instrument-shard-1`, etc.

### Tardis Data Download
- Each VM processes **one full day** of tick data per symbol per exchange
- Data is uploaded to GCS **after processing each day**
- VMs are named: `tardis-shard-0`, `tardis-shard-1`, etc.

## Monitoring

### Check VM Status
```bash
# List all VMs
gcloud compute instances list --filter="name~shard"

# Check specific VM
gcloud compute instances describe instrument-shard-0 --zone=asia-northeast1-c
```

### View Logs
```bash
# SSH into VM and check logs
gcloud compute ssh instrument-shard-0 --zone=asia-northeast1-c
cd /opt/market-tick-data-handler
tail -f logs/instrument_generation.log
```

### Check GCS Uploads
```bash
# Check instrument definitions
gsutil ls gs://market-data-tick/instruments/daily/

# Check tick data
gsutil ls gs://market-data-tick/daily/by_date/
```

## Configuration

### Environment Variables
The VMs are configured with the following environment variables:
- `TARDIS_API_KEY` - Tardis API key
- `GCP_PROJECT_ID` - Google Cloud project ID
- `GCS_BUCKET` - GCS bucket name
- `LOG_LEVEL` - Logging level
- And more...

### Machine Types
- **Instrument VMs**: `e2-standard-4` (4 vCPU, 16GB RAM)
- **Tardis VMs**: `e2-highmem-8` (8 vCPU, 64GB RAM)

### Zones
- Default zone: `asia-northeast1-c`
- Can be configured in the scripts

## Cleanup

### Clean up single VMs
```bash
./deploy-instruments.sh delete
./deploy-tardis.sh delete
```

### Clean up all sharded VMs
```bash
./shard-deploy.sh cleanup
```

### Clean up Docker images
```bash
./build-images.sh clean
```

## Troubleshooting

### Common Issues

1. **VM not starting**: Check if credentials file exists and is properly uploaded
2. **Process not running**: SSH into VM and check logs
3. **Permission denied**: Ensure proper GCS permissions
4. **Out of disk space**: Increase disk size in VM configuration

### Debug Commands

```bash
# Check VM startup logs
gcloud compute instances get-serial-port-output VM_NAME --zone=ZONE

# Check system logs
gcloud compute ssh VM_NAME --zone=ZONE --command="sudo journalctl -u google-startup-scripts.service -f"

# Check application logs
gcloud compute ssh VM_NAME --zone=ZONE --command="cd /opt/market-tick-data-handler && tail -f logs/*.log"
```

## Best Practices

1. **Use preemptible instances** for cost savings on non-critical workloads
2. **Monitor disk usage** and increase disk size if needed
3. **Check logs regularly** to ensure processes are running correctly
4. **Clean up VMs** when not needed to avoid costs
5. **Use appropriate machine types** based on workload requirements
6. **Test with small date ranges** before running large workloads
