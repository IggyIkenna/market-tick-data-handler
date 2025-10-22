# Market Tick Data Handler - Organized Structure

This document explains the new organized file structure for the Market Tick Data Handler project, designed to support 4 different deployment modes clearly and without confusion.

## 📁 New File Structure

```
market-tick-data-handler/
├── docker/                          # All Docker-related files
│   ├── instrument-generation/      # Docker for instrument generation
│   │   ├── Dockerfile
│   │   ├── docker-compose.yml
│   │   └── .dockerignore
│   ├── tardis-download/            # Docker for Tardis tick download
│   │   ├── Dockerfile
│   │   ├── docker-compose.yml
│   │   └── .dockerignore
│   └── shared/                      # Shared Docker utilities
│       ├── requirements.txt
│       └── common.dockerignore
├── deploy                         # All deployment scripts
│   ├── local/                       # Local Docker execution
│   │   ├── run-instrument-generation.sh
│   │   ├── run-tardis-download.sh
│   │   └── build-all.sh
│   └── vm/                          # VM deployment
│       ├── deploy-instrument-generation.sh
│       ├── deploy-tardis-download.sh
│       └── vm-manager.sh
├── deploy/                          # Existing VM orchestration (kept as-is)
│   └── orchestration/
├── src/                             # Source code
├── data/                            # Data files
├── logs/                            # Log files
├── temp/                            # Temporary files
├── downloads/                        # Download files
├── env.example                      # Environment template
└── README.md                        # Main documentation
```

## 🎯 Four Deployment Modes

### 1. Local Docker - Instrument Generation
**Purpose**: Run instrument generation locally in Docker
**Script**: `./deploylocal/run-instrument-generation.sh`
**Docker**: `docker/instrument-generation/`

### 2. Local Docker - Tardis Download
**Purpose**: Run Tardis data download locally in Docker
**Script**: `./deploylocal/run-tardis-download.sh`
**Docker**: `docker/tardis-download/`

### 3. VM Deployment - Development/Testing
**Purpose**: Deploy single VM for development and testing
**Scripts**: 
- `./deployvm/deploy-instrument-generation.sh` (single VM)
- `./deployvm/deploy-tardis-download.sh` (single VM)
**Docker**: Uses local Docker images
**Scale**: 1 VM at a time

### 4. VM Deployment - Production Orchestration
**Purpose**: Deploy massive scale production processing (60+ VMs)
**Scripts**: 
- `./deploy/orchestration/orchestrate-tick-download.sh` (60+ VMs)
- `./deploy/orchestration/monitor-tick-download.sh` (monitoring)
**Docker**: Uses Artifact Registry images
**Scale**: 60+ VMs simultaneously with sharding

## 🚀 Quick Start Guide

### Prerequisites
1. **Docker Desktop** running
2. **Google Cloud SDK** installed (for VM deployment)
3. **GCP Service Account Key** (`central-element-323112-e35fb0ddafe2.json`)
4. **Environment Variables** (copy `env.example` to `.env`)

### Local Development

#### Build All Images
```bash
./deploylocal/build-all.sh
```

#### Run Instrument Generation Locally
```bash
./deploylocal/run-instrument-generation.sh
```

#### Run Tardis Download Locally
```bash
./deploylocal/run-tardis-download.sh
```

### VM Deployment - Development/Testing

#### Deploy Single VM for Instrument Generation
```bash
./deployvm/deploy-instrument-generation.sh
```

#### Deploy Single VM for Tardis Download
```bash
./deployvm/deploy-tardis-download.sh
```

#### Manage Development VMs
```bash
./deployvm/vm-manager.sh
```

### VM Deployment - Production Orchestration

#### Deploy Massive Scale Processing (60+ VMs)
```bash
# Process 3 months of data with 60 VMs
./deploy/orchestration/orchestrate-tick-download.sh \
  --start-date 2023-05-01 \
  --end-date 2023-07-31 \
  --instances 60
```

#### Monitor Production VMs
```bash
./deploy/orchestration/monitor-tick-download.sh
```

#### Clean Up Production VMs
```bash
./deploy/orchestration/cleanup-tick-vms.sh
```

## 📋 Script Descriptions

### Local Scripts (`deploylocal/`)

- **`build-all.sh`**: Builds all Docker images for local development
- **`run-instrument-generation.sh`**: Runs instrument generation in Docker locally
- **`run-tardis-download.sh`**: Runs Tardis data download in Docker locally

### VM Scripts - Development (`deployvm/`)

- **`deploy-instrument-generation.sh`**: Creates single VM for instrument generation testing
- **`deploy-tardis-download.sh`**: Creates single VM for Tardis download testing
- **`vm-manager.sh`**: Manages development VMs (start, stop, delete, monitor)

### VM Scripts - Production (`deploy/orchestration/`)

- **`orchestrate-tick-download.sh`**: Deploys 60+ VMs for massive scale data processing
- **`monitor-tick-download.sh`**: Monitors production VM fleet
- **`cleanup-tick-vms.sh`**: Cleans up production VMs after completion
- **`vm-startup-script.sh`**: Startup script for production VMs
- **`cloudbuild.yaml`**: CI/CD pipeline for building production Docker images

## 🐳 Docker Structure

### Instrument Generation (`docker/instrument-generation/`)
- **Dockerfile**: Optimized for instrument generation
- **docker-compose.yml**: Local development with volume mounts
- **.dockerignore**: Excludes unnecessary files

### Tardis Download (`docker/tardis-download/`)
- **Dockerfile**: Optimized for Tardis data download
- **docker-compose.yml**: Local development with volume mounts
- **.dockerignore**: Excludes unnecessary files

### Shared (`docker/shared/`)
- **requirements.txt**: Common Python dependencies
- **common.dockerignore**: Common ignore patterns

## 🎯 **Key Distinction: Development vs Production VM Deployment**

### **Development VMs (`deployvm/`)**
- **Scale**: 1 VM at a time
- **Purpose**: Testing, development, small-scale processing
- **Docker Source**: Local builds
- **VM Type**: Standard VMs
- **Control**: Manual start/stop
- **Cost**: Low (single VM)
- **Use Case**: Test new features, learn, experiment

### **Production Orchestration (`deploy/orchestration/`)**
- **Scale**: 60+ VMs simultaneously
- **Purpose**: Massive production data processing
- **Docker Source**: Artifact Registry (pre-built)
- **VM Type**: Preemptible VMs (cost-optimized)
- **Control**: Automated with sharding
- **Cost**: High (many VMs)
- **Use Case**: Process months/years of data

### **When to Use Each**

| **Scenario** | **Use** | **Why** |
|--------------|---------|---------|
| Testing new features | `deployvm/` | Single VM, easy to debug |
| Processing 1 day of data | `deployvm/` | Small scale, cost-effective |
| Processing 3 months of data | `deploy/orchestration/` | Massive parallelism needed |
| Learning the system | `deployvm/` | Simple, controlled environment |
| Production workload | `deploy/orchestration/` | Battle-tested, optimized |

## 🔧 Environment Configuration

### Required Environment Variables
```bash
# Copy template
cp env.example .env

# Required variables
TARDIS_API_KEY=your_tardis_api_key
GCP_PROJECT_ID=central-element-323112
GCS_BUCKET=market-data-tick
```

### Optional Variables
- `LOG_LEVEL`: Logging level (default: INFO)
- `MAX_CONCURRENT_REQUESTS`: Concurrent requests (default: 2)
- `BATCH_SIZE`: Processing batch size (default: 1000)
- `DEBUG`: Debug mode (default: false)

## 📊 Directory Mounts

### Local Docker
- `./data` → `/app/data` - Output data files
- `./logs` → `/app/logs` - Log files
- `./temp` → `/app/temp` - Temporary files
- `./downloads` → `/app/downloads` - Download files (Tardis only)

### VM Deployment
- VM storage → `/opt/market-tick-data-handler/data`
- VM storage → `/opt/market-tick-data-handler/logs`
- VM storage → `/opt/market-tick-data-handler/temp`
- VM storage → `/opt/market-tick-data-handler/downloads`

## 🔍 Monitoring and Debugging

### Local Development
```bash
# Check container logs
docker logs market-tick-instrument-generator
docker logs market-tick-tardis-downloader

# Run container interactively
docker run -it --env-file .env market-tick-instrument-generator:latest /bin/bash
```

### VM Deployment
```bash
# SSH into VM
gcloud compute ssh instrument-generator-vm --zone=asia-northeast1-c

# Check startup logs
sudo journalctl -u google-startup-scripts.service -f

# Check application logs
tail -f /opt/market-tick-data-handler/logs/*.log
```

## 🛠️ Troubleshooting

### Common Issues

1. **Docker not running**
   - Start Docker Desktop
   - Check with `docker info`

2. **Missing environment variables**
   - Copy `env.example` to `.env`
   - Update with actual values

3. **Permission denied**
   - Make scripts executable: `chmod +x deploy**/*.sh`

4. **VM deployment fails**
   - Check gcloud authentication: `gcloud auth list`
   - Verify project: `gcloud config get-value project`
   - Check quotas and permissions

### Debug Commands

```bash
# Check Docker images
docker images | grep market-tick

# Check running containers
docker ps

# Check VM status
gcloud compute instances list

# Check VM logs
gcloud compute instances get-serial-port-output VM_NAME --zone=ZONE
```

## 📈 Performance Considerations

- **Docker Layer Caching**: Optimized Dockerfiles for faster rebuilds
- **Volume Mounts**: Persistent data across container restarts
- **Resource Limits**: Appropriate VM sizes for each workload
- **Parallel Processing**: Configurable concurrent requests

## 🔒 Security Notes

- Containers run as non-root user
- GCP service account key included in images (consider secrets management for production)
- Environment variables should not contain sensitive data in plain text
- VM instances have appropriate IAM scopes

## 📚 Additional Documentation

- **VM Deployment Guide**: `docs/VM_DEPLOYMENT.md`
- **Docker Setup Guide**: `docs/DOCKER_SETUP.md`
- **API Documentation**: `docs/API_DOCUMENTATION.md`
- **Troubleshooting Guide**: `docs/TROUBLESHOOTING.md`

## 🤝 Contributing

When adding new deployment modes:

1. Create new directory under `docker/` for Docker files
2. Create new scripts under `deploylocal/` or `deployvm/`
3. Update this documentation
4. Test all deployment modes
5. Update environment templates if needed
