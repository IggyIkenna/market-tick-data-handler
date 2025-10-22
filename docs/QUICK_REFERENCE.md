# Quick Reference - Market Tick Data Handler

## 🚀 Quick Commands

### Local Python Execution (No Docker)
```bash
# Activate virtual environment (if using one)
source venv_local/bin/activate

# Run instrument generation directly
python run_fixed_local_instrument_generation.py
```

### Local Docker Development
```bash
# Run instrument generation
./deploy/local/run-main.sh instruments --start-date 2023-05-23 --end-date 2023-05-25

# Run Tardis download
./deploy/local/run-main.sh download --start-date 2023-05-23 --end-date 2023-05-25

# Run validation
./deploy/local/run-main.sh validate --start-date 2023-05-23 --end-date 2023-05-25

# Run full pipeline
./deploy/local/run-main.sh full-pipeline --start-date 2023-05-23 --end-date 2023-05-25
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

## 📁 File Structure Overview

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

## 🔧 Setup

1. **Prerequisites**
   - Docker Desktop running
   - Google Cloud SDK (for VM deployment)
   - GCP service account key present

2. **Environment**
   ```bash
   cp env.example .env
   # Edit .env with your values
   ```

3. **Run Operations**
   ```bash
   # Use the convenience script
   ./deploy/local/run-main.sh instruments --start-date 2023-05-23 --end-date 2023-05-25
   ```

## 📊 4 Deployment Modes

| Mode | Purpose | Script | Scale | Docker |
|------|---------|--------|-------|--------|
| Local Python | Run operations locally (no Docker) | `python -m src.main --mode <mode>` | Local | None |
| Local Docker | Run operations in Docker locally | `./deploy/local/run-main.sh <mode>` | Local | `docker/` |
| VM - Development | Deploy single VM for testing | `./deploy/vm/deploy-*.sh` | 1 VM | Local builds |
| VM - Production | Deploy multiple VMs for massive processing | `./deploy/vm/shard-deploy.sh` | Multiple VMs | Artifact Registry |

## 🎯 When to Use Each Mode

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

## 🔍 Monitoring

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

## 🛠️ Troubleshooting

- **Docker not running**: Start Docker Desktop
- **Permission denied**: `chmod +x deploy**/*.sh`
- **Missing .env**: `cp env.example .env`
- **VM issues**: Check gcloud auth and project settings
