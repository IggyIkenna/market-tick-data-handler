# Authentication Strategy for Package Distribution

## Overview

The Market Data Handler package supports multiple authentication strategies to accommodate different use cases:

1. **Production Teams**: Full GCP access with Secret Manager
2. **Development Teams**: Local testing with mock data or limited GCP access
3. **External Teams**: Read-only access to processed data
4. **CI/CD**: Automated testing with service accounts

## Authentication Architecture

The system uses a two-layer authentication approach:

### Layer 1: Google Cloud Authentication
**Purpose**: Authenticate with Google Cloud services
**Required for**: GCS, BigQuery, Secret Manager, and other GCP services
**Environment Variables**:
- `GOOGLE_APPLICATION_CREDENTIALS` - Standard Google Cloud authentication
- `GCP_CREDENTIALS_PATH` - Custom configuration variable

### Layer 2: Application Secret Management
**Purpose**: Securely store and retrieve application secrets
**Options**:
- **Secret Manager**: Encrypted, auditable secret storage (recommended for production)
- **Environment Variables**: Simple key-value storage (suitable for development)

### Important Distinction
- **GCP credentials** authenticate your application with Google Cloud
- **Secret Manager** stores your application's API keys and secrets
- **Secret Manager requires GCP authentication** to function
- **No fallback relationship** - these are separate layers

## Required IAM Permissions

### Minimum Required Roles

**For Data Download (Production)**:
- `roles/storage.objectAdmin` - Upload/download data to/from GCS
- `roles/secretmanager.secretAccessor` - Access Tardis API key
- `roles/bigquery.dataEditor` - Upload processed data to BigQuery

**For Data Reading Only (Development)**:
- `roles/storage.objectViewer` - Read data from GCS
- `roles/secretmanager.secretAccessor` - Access Tardis API key (optional)

**For Mock Data (No GCP Access)**:
- No GCP permissions required
- Uses local mock data files

### Service Account Security Best Practices

1. **Principle of Least Privilege**: Only assign required roles
2. **Key Rotation**: Rotate service account keys regularly
3. **Conditional Access**: Use IAM conditions for time/network restrictions
4. **Audit Logging**: Enable Cloud Audit Logs for monitoring
5. **Key Management**: Store keys securely, never commit to git

## Authentication Tiers

### Tier 1: Full Production Access
**Use Case**: Production teams with full GCP project access

**Requirements**:
- GCP project with Secret Manager enabled
- Service account with proper IAM roles
- Tardis API key stored in Secret Manager

**Service Account Setup**:
```bash
# 1. Create service account
gcloud iam service-accounts create market-data-prod \
  --display-name="Market Data Production" \
  --description="Service account for production market data operations"

# 2. Assign required IAM roles
gcloud projects add-iam-policy-binding your-project-id \
  --member="serviceAccount:market-data-prod@your-project-id.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

gcloud projects add-iam-policy-binding your-project-id \
  --member="serviceAccount:market-data-prod@your-project-id.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

gcloud projects add-iam-policy-binding your-project-id \
  --member="serviceAccount:market-data-prod@your-project-id.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

# 3. Create and download key
gcloud iam service-accounts keys create market-data-prod-key.json \
  --iam-account=market-data-prod@your-project-id.iam.gserviceaccount.com

# 4. Store Tardis API key in Secret Manager
echo "TD.your_tardis_api_key_here" | gcloud secrets create tardis-api-key --data-file=-
```

**Configuration**:
```bash
# Environment variables
export USE_SECRET_MANAGER=true
export GCP_PROJECT_ID=your-production-project
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/market-data-prod-key.json
export TARDIS_SECRET_NAME=tardis-api-key
```

**Usage**:
```python
from src.data_client import DataClient
from config import get_config

# Automatically uses Secret Manager
config = get_config()
data_client = DataClient(config.gcp.bucket, config)
```

### Tier 2: Development/Local Testing
**Use Case**: Developers testing locally without full GCP access

**Requirements**:
- Local environment variables
- Optional: Limited GCS access for testing
- Mock data support for offline development

**Service Account Setup (Optional)**:
```bash
# 1. Create development service account
gcloud iam service-accounts create market-data-dev \
  --display-name="Market Data Development" \
  --description="Service account for development testing"

# 2. Assign minimal IAM roles for testing
gcloud projects add-iam-policy-binding your-project-id \
  --member="serviceAccount:market-data-dev@your-project-id.iam.gserviceaccount.com" \
  --role="roles/storage.objectViewer"

# 3. Create and download key
gcloud iam service-accounts keys create market-data-dev-key.json \
  --iam-account=market-data-dev@your-project-id.iam.gserviceaccount.com
```

**Configuration**:
```bash
# Environment variables (no Secret Manager)
export USE_SECRET_MANAGER=false
export TARDIS_API_KEY=TD.your_test_key
export GCP_PROJECT_ID=your-dev-project
export GCP_CREDENTIALS_PATH=/path/to/dev-service-account.json
export GCS_BUCKET=your-dev-bucket
```

**Usage**:
```python
from src.data_client import DataClient
from config import get_config

# Uses environment variables
config = get_config()
data_client = DataClient(config.gcp.bucket, config)
```

### Tier 3: Read-Only Data Access
**Use Case**: Teams that only need to read processed data

**Requirements**:
- Read-only GCS access
- No Tardis API key needed
- No Secret Manager access

**Configuration**:
```bash
# Read-only configuration
export USE_SECRET_MANAGER=false
export GCP_PROJECT_ID=your-project
export GCP_CREDENTIALS_PATH=/path/to/readonly-service-account.json
export GCS_BUCKET=your-data-bucket
# No TARDIS_API_KEY needed
```

**Usage**:
```python
from src.data_client import CandleDataReader, TickDataReader
from config import get_config

# Only data reading capabilities
config = get_config()
candle_reader = CandleDataReader(data_client)
```

### Tier 4: Mock/Offline Mode
**Use Case**: Development without any GCP access

**Requirements**:
- No GCP credentials
- Mock data files
- Offline development

**Configuration**:
```bash
# Mock mode
export USE_MOCK_DATA=true
export MOCK_DATA_PATH=/path/to/mock/data
```

**Usage**:
```python
from src.data_client import MockDataClient
from config import get_config

# Uses mock data
config = get_config()
data_client = MockDataClient()
```

## Implementation Strategy

### 1. Enhanced Configuration Management

**Update `config.py`** to support multiple authentication modes:

```python
@dataclass
class AuthenticationConfig:
    """Authentication configuration"""
    mode: str = "auto"  # auto, secret_manager, env_vars, mock
    use_secret_manager: bool = False
    use_mock_data: bool = False
    mock_data_path: Optional[str] = None
    
    def __post_init__(self):
        if self.mode == "auto":
            self.use_secret_manager = os.getenv('USE_SECRET_MANAGER', 'false').lower() == 'true'
            self.use_mock_data = os.getenv('USE_MOCK_DATA', 'false').lower() == 'true'
```

### 2. Mock Data Client

**Create `src/data_client/mock_data_client.py`**:

```python
class MockDataClient:
    """Mock data client for offline development"""
    
    def __init__(self, mock_data_path: str = None):
        self.mock_data_path = mock_data_path or os.getenv('MOCK_DATA_PATH', './mock_data')
    
    def get_tick_data(self, instrument_id: str, start_time: datetime, end_time: datetime, date: date):
        """Return mock tick data"""
        # Load from mock data files
        pass
    
    def get_candles(self, instrument_id: str, timeframe: str, start_date: datetime, end_date: datetime):
        """Return mock candle data"""
        # Load from mock data files
        pass
```

### 3. Graceful Degradation

**Update `DataClient`** to handle missing credentials gracefully:

```python
class DataClient:
    def __init__(self, bucket: str, config: Config):
        self.bucket = bucket
        self.config = config
        self._client = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Initialize GCS client with graceful fallback"""
        try:
            self._client = get_shared_gcs_client(self.config)
        except Exception as e:
            if self.config.auth.use_mock_data:
                logger.info("GCS unavailable, using mock data")
                self._client = MockDataClient()
            else:
                raise AuthenticationError(f"GCS authentication failed: {e}")
```

### 4. Service Account Templates

**Create service account templates** for different use cases:

#### Production Service Account
```json
{
  "type": "service_account",
  "project_id": "your-project",
  "private_key_id": "...",
  "private_key": "...",
  "client_email": "market-data-handler@your-project.iam.gserviceaccount.com",
  "client_id": "...",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs"
}
```

**Required IAM Roles**:
- `roles/secretmanager.secretAccessor`
- `roles/storage.objectViewer`
- `roles/storage.objectCreator` (for uploads)

#### Development Service Account
```json
{
  "type": "service_account",
  "project_id": "your-dev-project",
  "private_key_id": "...",
  "private_key": "...",
  "client_email": "market-data-dev@your-dev-project.iam.gserviceaccount.com",
  "client_id": "...",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs"
}
```

**Required IAM Roles**:
- `roles/storage.objectViewer`
- `roles/storage.objectCreator` (for testing)

#### Read-Only Service Account
```json
{
  "type": "service_account",
  "project_id": "your-project",
  "private_key_id": "...",
  "private_key": "...",
  "client_email": "market-data-readonly@your-project.iam.gserviceaccount.com",
  "client_id": "...",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs"
}
```

**Required IAM Roles**:
- `roles/storage.objectViewer`

### 5. Setup Scripts

**Create `scripts/setup-auth.sh`**:

```bash
#!/bin/bash
# Setup authentication for different tiers

TIER=$1
PROJECT_ID=$2
SERVICE_ACCOUNT_NAME=$3

case $TIER in
  "production")
    echo "Setting up production authentication..."
    gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME \
      --display-name="Market Data Handler Production" \
      --description="Service account for production market data operations"
    
    gcloud projects add-iam-policy-binding $PROJECT_ID \
      --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
      --role="roles/secretmanager.secretAccessor"
    
    gcloud projects add-iam-policy-binding $PROJECT_ID \
      --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
      --role="roles/storage.objectViewer"
    
    gcloud projects add-iam-policy-binding $PROJECT_ID \
      --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
      --role="roles/storage.objectCreator"
    ;;
    
  "development")
    echo "Setting up development authentication..."
    gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME \
      --display-name="Market Data Handler Development" \
      --description="Service account for development market data operations"
    
    gcloud projects add-iam-policy-binding $PROJECT_ID \
      --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
      --role="roles/storage.objectViewer"
    
    gcloud projects add-iam-policy-binding $PROJECT_ID \
      --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
      --role="roles/storage.objectCreator"
    ;;
    
  "readonly")
    echo "Setting up read-only authentication..."
    gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME \
      --display-name="Market Data Handler Read-Only" \
      --description="Service account for read-only market data access"
    
    gcloud projects add-iam-policy-binding $PROJECT_ID \
      --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
      --role="roles/storage.objectViewer"
    ;;
esac

# Generate and download key
gcloud iam service-accounts keys create $SERVICE_ACCOUNT_NAME-key.json \
  --iam-account=$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com

echo "Service account key saved to: $SERVICE_ACCOUNT_NAME-key.json"
echo "Set GCP_CREDENTIALS_PATH to this file path"
```

### 6. Documentation Updates

**Update `docs/SETUP_GUIDE.md`** with authentication tiers:

```markdown
## Authentication Setup

### For Production Teams
1. Run setup script: `./scripts/setup-auth.sh production your-project market-data-prod`
2. Set environment variables:
   ```bash
   export USE_SECRET_MANAGER=true
   export GCP_PROJECT_ID=your-project
   export GCP_CREDENTIALS_PATH=./market-data-prod-key.json
   ```

### For Development Teams
1. Run setup script: `./scripts/setup-auth.sh development your-dev-project market-data-dev`
2. Set environment variables:
   ```bash
   export USE_SECRET_MANAGER=false
   export TARDIS_API_KEY=TD.your_test_key
   export GCP_PROJECT_ID=your-dev-project
   export GCP_CREDENTIALS_PATH=./market-data-dev-key.json
   ```

### For Read-Only Access
1. Run setup script: `./scripts/setup-auth.sh readonly your-project market-data-readonly`
2. Set environment variables:
   ```bash
   export USE_SECRET_MANAGER=false
   export GCP_PROJECT_ID=your-project
   export GCP_CREDENTIALS_PATH=./market-data-readonly-key.json
   ```

### For Offline Development
1. Set environment variables:
   ```bash
   export USE_MOCK_DATA=true
   export MOCK_DATA_PATH=./mock_data
   ```
```

## Benefits

1. **Flexible Authentication**: Supports multiple use cases
2. **Security**: Proper IAM roles and Secret Manager integration
3. **Development Friendly**: Mock data and offline modes
4. **Production Ready**: Full GCP integration with proper credentials
5. **Easy Setup**: Automated service account creation and configuration
6. **Graceful Degradation**: Falls back to mock data when GCP unavailable

## Migration Path

1. **Phase 1**: Implement enhanced configuration management
2. **Phase 2**: Add mock data client and graceful degradation
3. **Phase 3**: Create service account templates and setup scripts
4. **Phase 4**: Update documentation and examples
5. **Phase 5**: Test with different authentication tiers

This strategy ensures that the package can be used by teams with different levels of GCP access while maintaining security and ease of use.
