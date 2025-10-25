# Docker Build Instructions

## Overview
This directory contains Docker configurations for building optimized images for the Market Data Tick Handler.

## Files

### Dockerfiles
- **`tardis-download/Dockerfile`**: Original Dockerfile (requires service account file locally)
- **`tardis-download/Dockerfile.amd64`**: Optimized for Cloud Build (AMD64, no service account file required)

### Build Configurations
- **`tardis-download/cloudbuild.yaml`**: Google Cloud Build configuration for automated builds

## Building Images

### Local Build (Mac/ARM64)
```bash
# Build for local testing (ARM64)
docker build -f docker/tardis-download/Dockerfile -t market-tick-tardis-downloader:local .
```

### Cloud Build (Production AMD64)
```bash
# Build and push optimized AMD64 image for production
gcloud beta builds submit --config docker/tardis-download/cloudbuild.yaml .
```

This will create:
- `asia-northeast1-docker.pkg.dev/central-element-323112/market-data-tick-handler/market-tick-tardis-downloader:latest`
- `asia-northeast1-docker.pkg.dev/central-element-323112/market-data-tick-handler/market-tick-tardis-downloader:optimized`

## Image Features

### Optimized Configuration
- **Platform**: linux/amd64 (compatible with Google Cloud VMs)
- **Base**: python:3.11-slim (lightweight)
- **User**: Non-root user (tickdata) for security
- **Dependencies**: All required Python packages pre-installed
- **Service Account**: Dummy file (VMs use default service account)

### Performance Settings
- **Default Mode**: full-pipeline-ticks (instruments → missing-tick-reports → download)
- **Workers**: 8 parallel workers per VM
- **Concurrent Downloads**: 500 (Tardis API)
- **Parallel Uploads**: 200 (GCS)
- **Memory Threshold**: 85%

## Usage in Deployment

The images are automatically used by:
- `deploy/vm/deploy-download.sh` - Single VM deployment
- `deploy/vm/shard-deploy-download.sh` - Multi-VM sharded deployment

## Troubleshooting

### Common Issues
1. **Service Account Error**: Use `Dockerfile.amd64` for Cloud Build
2. **Platform Mismatch**: Build with `--platform=linux/amd64` for cloud deployment
3. **Build Failures**: Check Cloud Build logs at the provided URL

### Build Monitoring
Enable live streaming with:
```bash
export CLOUDSDK_PYTHON_SITEPACKAGES=1
gcloud beta builds submit --config docker/tardis-download/cloudbuild.yaml .
```

---
*Last Updated: October 24, 2025*
