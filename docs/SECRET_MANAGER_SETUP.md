# Secret Manager Setup Guide

## Overview

This guide explains how to set up and use Google Cloud Secret Manager with the Market Data Handler. The system supports two complementary approaches:

- **config.py integration**: Automatic Tardis API key retrieval (fixed, production config)
- **SecretManagerUtils**: Manual management of dynamic API keys and trading credentials

## How It Works: Two-Layer Authentication

Understanding the relationship between GCP authentication and Secret Manager is crucial:

### Layer 1: GCP Authentication
**Purpose**: Authenticate with Google Cloud services
**Environment Variables**: 
- `GOOGLE_APPLICATION_CREDENTIALS` (standard Google Cloud)
- `GCP_CREDENTIALS_PATH` (custom config variable)

**What it does**:
- Authenticates your application with Google Cloud
- Required to access GCS, BigQuery, Secret Manager, etc.
- Uses service account keys or instance metadata

### Layer 2: Secret Manager
**Purpose**: Securely store and retrieve application secrets
**Configuration**: `USE_SECRET_MANAGER=true`

**What it does**:
- Stores API keys, trading credentials, configurations
- Requires GCP authentication (Layer 1) to function
- Provides encrypted, auditable secret storage

### Key Point: No Fallback Relationship
- **Secret Manager is NOT a fallback for GCP credentials**
- **GCP credentials are NOT stored in Secret Manager**
- **Secret Manager requires GCP authentication to work**

### Authentication Flow
```
1. GCP Authentication (GOOGLE_APPLICATION_CREDENTIALS)
   ↓
2. Access Secret Manager (authenticated Google Cloud service)
   ↓
3. Retrieve application secrets (API keys, trading keys, etc.)
```

## Prerequisites

### 1. Google Cloud Setup

```bash
# Enable Secret Manager API
gcloud services enable secretmanager.googleapis.com --project=YOUR_PROJECT_ID

# Verify API is enabled
gcloud services list --enabled --filter="name:secretmanager.googleapis.com"
```

### 2. Service Account Permissions

Create a service account with the following roles:

```bash
# Create service account (if not exists)
gcloud iam service-accounts create market-data-handler \
  --display-name="Market Data Handler" \
  --description="Service account for market data processing"

# Grant Secret Manager permissions
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:market-data-handler@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:market-data-handler@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretVersionManager"

# Create and download key
gcloud iam service-accounts keys create credentials.json \
  --iam-account=market-data-handler@YOUR_PROJECT_ID.iam.gserviceaccount.com
```

### 3. Install Dependencies

```bash
# Install required package
pip install google-cloud-secret-manager==2.16.4

# Or install all requirements
pip install -r requirements.txt
```

## Setup for Tardis API Key (Fixed Configuration)

The Tardis API key is automatically retrieved by `config.py` when `USE_SECRET_MANAGER=true`.

### Option A: Using the Setup Script

```bash
# Run the provided setup script
./scripts/setup-secret-manager.sh \
  --project-id YOUR_PROJECT_ID \
  --api-key TD.your_tardis_api_key_here

# With custom secret name
./scripts/setup-secret-manager.sh \
  --project-id YOUR_PROJECT_ID \
  --api-key TD.your_tardis_api_key_here \
  --secret-name custom-tardis-key
```

### Option B: Manual Setup

```bash
# Create the secret
gcloud secrets create tardis-api-key \
  --project=YOUR_PROJECT_ID \
  --replication-policy=automatic

# Add the API key
echo -n "TD.your_tardis_api_key_here" | gcloud secrets versions add tardis-api-key \
  --project=YOUR_PROJECT_ID \
  --data-file=-

# Grant service account access
gcloud secrets add-iam-policy-binding tardis-api-key \
  --member="serviceAccount:market-data-handler@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

### Configuration

Set these environment variables:

```bash
# In .env file or environment
USE_SECRET_MANAGER=true
TARDIS_SECRET_NAME=tardis-api-key
GCP_PROJECT_ID=YOUR_PROJECT_ID
GCP_CREDENTIALS_PATH=/path/to/credentials.json
```

## Setup for Dynamic Secrets (Trading Keys, Other APIs)

Use `SecretManagerUtils` for secrets that change frequently or need manual management.

### Basic Usage

```python
from src.utils.secret_manager_utils import SecretManagerUtils

# Initialize
sm = SecretManagerUtils("YOUR_PROJECT_ID")

# Upload API keys
sm.upload_api_key("binance", "your-binance-api-key", "Binance API key")
sm.upload_api_key("deribit", "your-deribit-api-key", "Deribit API key")

# Upload trading keys
sm.upload_trading_keys("binance", {
    "api_key": "your-binance-api-key",
    "secret_key": "your-binance-secret-key",
    "testnet": True
})

# Upload configuration
sm.upload_config("trading", {
    "exchanges": {
        "binance": {"rate_limit": 1200},
        "deribit": {"rate_limit": 1000}
    }
})
```

### Retrieving Secrets

```python
# Get API keys
binance_key = sm.get_api_key("binance")
deribit_key = sm.get_api_key("deribit")

# Get trading keys
binance_trading = sm.get_trading_keys("binance")
if binance_trading:
    print(f"API Key: {binance_trading['api_key']}")
    print(f"Testnet: {binance_trading['testnet']}")

# Get configuration
trading_config = sm.get_config("trading")
```

### List Available Secrets

```python
# List all secrets by type
api_keys = sm.list_api_keys()
trading_keys = sm.list_trading_keys()
configs = sm.list_configs()

print(f"Available API keys: {api_keys}")
print(f"Available trading keys: {trading_keys}")
print(f"Available configs: {configs}")
```

## Environment-Specific Instructions

### Local Development

#### Option 1: Using gcloud authentication

```bash
# Authenticate with your user account
gcloud auth application-default login

# Set environment variables
export USE_SECRET_MANAGER=true
export GCP_PROJECT_ID=YOUR_PROJECT_ID
export TARDIS_SECRET_NAME=tardis-api-key
```

#### Option 2: Using service account

```bash
# Set credentials path
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
export USE_SECRET_MANAGER=true
export GCP_PROJECT_ID=YOUR_PROJECT_ID
export TARDIS_SECRET_NAME=tardis-api-key
```

#### Option 3: Fallback to environment variables

```bash
# Disable Secret Manager for local development
export USE_SECRET_MANAGER=false
export TARDIS_API_KEY=TD.your_tardis_api_key_here
export GCP_PROJECT_ID=YOUR_PROJECT_ID
```

### VM Deployment

For VM deployments, the service account is automatically authenticated via instance metadata:

```bash
# Update your .env file on the VM
USE_SECRET_MANAGER=true
TARDIS_SECRET_NAME=tardis-api-key
GCP_PROJECT_ID=YOUR_PROJECT_ID
# GCP_CREDENTIALS_PATH not needed - uses instance metadata
```

Ensure the VM's service account has the required Secret Manager permissions:

```bash
# Grant permissions to VM service account
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:VM_SERVICE_ACCOUNT@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

### Docker Deployment

```dockerfile
# Dockerfile
FROM python:3.9-slim

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy application code
COPY src/ /app/src/
COPY config.py /app/

# Set environment variables
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/credentials.json
ENV USE_SECRET_MANAGER=true
ENV GCP_PROJECT_ID=YOUR_PROJECT_ID
ENV TARDIS_SECRET_NAME=tardis-api-key

# Mount credentials
COPY credentials.json /app/credentials.json

# Run application
CMD ["python", "src/main.py"]
```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: market-data-handler
spec:
  replicas: 3
  selector:
    matchLabels:
      app: market-data-handler
  template:
    metadata:
      labels:
        app: market-data-handler
    spec:
      serviceAccountName: market-data-handler
      containers:
      - name: handler
        image: your-registry/market-data-handler:latest
        env:
        - name: USE_SECRET_MANAGER
          value: "true"
        - name: GCP_PROJECT_ID
          value: "YOUR_PROJECT_ID"
        - name: TARDIS_SECRET_NAME
          value: "tardis-api-key"
        volumeMounts:
        - name: credentials
          mountPath: /app/credentials.json
          subPath: credentials.json
      volumes:
      - name: credentials
        secret:
          secretName: gcp-credentials
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: market-data-handler
  annotations:
    iam.gke.io/gcp-service-account: market-data-handler@YOUR_PROJECT_ID.iam.gserviceaccount.com
```

## Best Practices

### 1. Naming Conventions

Use consistent naming patterns for secrets:

- **Tardis API**: `tardis-api-key` (fixed)
- **Other API keys**: `api-key-{service}` (e.g., `api-key-binance`)
- **Trading keys**: `trading-keys-{exchange}` (e.g., `trading-keys-binance`)
- **Configurations**: `config-{name}` (e.g., `config-trading`)

### 2. Environment Separation

Use different secrets for different environments:

```bash
# Development
tardis-api-key-dev
api-key-binance-dev

# Staging
tardis-api-key-staging
api-key-binance-staging

# Production
tardis-api-key-prod
api-key-binance-prod
```

### 3. Security Best Practices

- **Never commit secrets to version control**
- **Use least-privilege IAM roles**
- **Enable audit logging for secret access**
- **Rotate secrets regularly**
- **Use different service accounts for different environments**

### 4. Error Handling

Always implement fallback behavior:

```python
from src.utils.secret_manager_utils import SecretManagerUtils
import os

def get_api_key_with_fallback(service_name):
    """Get API key with fallback to environment variable"""
    try:
        sm = SecretManagerUtils(os.getenv('GCP_PROJECT_ID'))
        return sm.get_api_key(service_name)
    except Exception as e:
        print(f"Secret Manager failed: {e}")
        # Fallback to environment variable
        return os.getenv(f'{service_name.upper()}_API_KEY')
```

## Troubleshooting

### Common Issues

#### 1. Authentication Failures

**Error**: `DefaultCredentialsError: Could not automatically determine credentials`

**Cause**: Missing or invalid GCP authentication (Layer 1)

**Solutions**:
```bash
# For local development
gcloud auth application-default login

# For service account
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json

# Verify authentication
gcloud auth list
```

**Note**: This error occurs when trying to access Secret Manager without proper GCP authentication. Secret Manager requires GCP credentials to function.

#### 2. Permission Denied

**Error**: `PermissionDenied: 403 Permission denied on resource`

**Cause**: Service account lacks Secret Manager permissions (Layer 2)

**Solutions**:
```bash
# Check current permissions
gcloud secrets get-iam-policy tardis-api-key --project=YOUR_PROJECT_ID

# Grant required permissions
gcloud secrets add-iam-policy-binding tardis-api-key \
  --member="serviceAccount:YOUR_SA@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

**Note**: This error occurs when GCP authentication works but the service account doesn't have Secret Manager permissions.

#### 3. Secret Not Found

**Error**: `NotFound: 404 Secret not found`

**Solutions**:
```bash
# List all secrets
gcloud secrets list --project=YOUR_PROJECT_ID

# Check secret exists
gcloud secrets describe tardis-api-key --project=YOUR_PROJECT_ID

# Create secret if missing
gcloud secrets create tardis-api-key --project=YOUR_PROJECT_ID
```

#### 4. Fallback Behavior

The system automatically falls back to environment variables if:
- Secret Manager is not available
- GCP authentication fails (Layer 1)
- Secret doesn't exist (Layer 2)
- `USE_SECRET_MANAGER=false`

**Important**: The fallback is for **application secrets** (API keys), not GCP credentials.

Check the logs for fallback messages:
```
WARNING: Failed to retrieve API key from Secret Manager: ...
INFO: Retrieved Tardis API key from environment variable
```

### Debugging Commands

```bash
# Test Secret Manager access
gcloud secrets versions access latest --secret=tardis-api-key --project=YOUR_PROJECT_ID

# List all secrets
gcloud secrets list --project=YOUR_PROJECT_ID

# Check IAM permissions
gcloud projects get-iam-policy YOUR_PROJECT_ID

# Test service account authentication
gcloud auth activate-service-account --key-file=credentials.json
```

## Examples

### Complete Setup Example

```python
#!/usr/bin/env python3
"""Complete Secret Manager setup example"""

import os
from src.utils.secret_manager_utils import SecretManagerUtils
from config import get_config

def setup_secrets():
    """Set up all required secrets"""
    project_id = os.getenv('GCP_PROJECT_ID')
    sm = SecretManagerUtils(project_id)
    
    # Upload Tardis API key
    tardis_key = os.getenv('TARDIS_API_KEY')
    if tardis_key:
        sm.upload_api_key('tardis', tardis_key, 'Tardis API key for market data')
    
    # Upload trading keys
    binance_keys = {
        'api_key': os.getenv('BINANCE_API_KEY'),
        'secret_key': os.getenv('BINANCE_SECRET_KEY'),
        'testnet': True
    }
    if binance_keys['api_key']:
        sm.upload_trading_keys('binance', binance_keys, 'Binance trading keys')
    
    print("Secrets uploaded successfully!")

def test_config():
    """Test configuration loading"""
    config = get_config()
    print(f"Project ID: {config.gcp.project_id}")
    print(f"Tardis API Key: {config.tardis.api_key[:10]}...")

if __name__ == "__main__":
    setup_secrets()
    test_config()
```

## Next Steps

1. **Set up your first secret** using the Tardis API key
2. **Test the configuration** with `python examples/secret_manager_example.py`
3. **Explore dynamic secrets** for your trading keys
4. **Implement in your services** using the provided patterns

For more information, see:
- [Downstream Usage Guide](DOWNSTREAM_USAGE.md)
- [Package Usage Examples](PACKAGE_USAGE.md)
- [Deployment Guide](DEPLOYMENT_GUIDE.md)
