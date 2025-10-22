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
# Build all images
./scripts/local/build-all.sh

# Run instrument generation
./scripts/local/run-instrument-generation.sh

# Run Tardis download
./scripts/local/run-tardis-download.sh
```

### VM Deployment - Development/Testing
```bash
# Deploy single VM for instrument generation
./scripts/vm/deploy-instrument-generation.sh

# Deploy single VM for Tardis download
./scripts/vm/deploy-tardis-download.sh

# Manage development VMs
./scripts/vm/vm-manager.sh
```

### VM Deployment - Production Orchestration
```bash
# Deploy 60+ VMs for massive data processing
./deploy/orchestration/orchestrate-tick-download.sh \
  --start-date 2023-05-01 \
  --end-date 2023-07-31 \
  --instances 60

# Monitor production VMs
./deploy/orchestration/monitor-tick-download.sh

# Clean up production VMs
./deploy/orchestration/cleanup-tick-vms.sh
```

## 📁 File Structure Overview

```
docker/
├── instrument-generation/    # Docker for instrument generation
├── tardis-download/         # Docker for Tardis download
└── shared/                  # Shared Docker utilities

scripts/
├── local/                   # Local Docker execution
└── vm/                      # Single VM deployment (development/testing)

deploy/                      # Production orchestration (60+ VMs)
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

3. **Build Images**
   ```bash
   ./scripts/local/build-all.sh
   ```

## 📊 4 Deployment Modes

| Mode | Purpose | Script | Scale | Docker |
|------|---------|--------|-------|--------|
| Local Python - Instrument | Run instrument generation locally (no Docker) | `python run_fixed_local_instrument_generation.py` | Local | None |
| Local Docker - Instrument | Run instrument generation locally | `./scripts/local/run-instrument-generation.sh` | Local | `docker/instrument-generation/` |
| Local Docker - Tardis | Run Tardis download locally | `./scripts/local/run-tardis-download.sh` | Local | `docker/tardis-download/` |
| VM - Development | Deploy single VM for testing | `./scripts/vm/deploy-*.sh` | 1 VM | Local builds |
| VM - Production | Deploy 60+ VMs for massive processing | `./deploy/orchestration/orchestrate-*.sh` | 60+ VMs | Artifact Registry |

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

### **VM Development** (`scripts/vm/`)
- ✅ Test VM deployment
- ✅ Process 1-7 days of data
- ✅ Validate before production
- ✅ Single VM testing

### **VM Production** (`deploy/orchestration/`)
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
- **Permission denied**: `chmod +x scripts/**/*.sh`
- **Missing .env**: `cp env.example .env`
- **VM issues**: Check gcloud auth and project settings
