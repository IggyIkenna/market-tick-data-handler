#!/bin/bash
# Setup authentication for different tiers

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to show usage
show_usage() {
    echo -e "${BLUE}Market Data Handler - Authentication Setup${NC}"
    echo "============================================="
    echo ""
    echo "Usage: $0 [TIER] [PROJECT_ID] [SERVICE_ACCOUNT_NAME]"
    echo ""
    echo "Tiers:"
    echo "  production  - Full GCP access with Secret Manager"
    echo "  development - Limited GCP access for testing"
    echo "  readonly    - Read-only access to processed data"
    echo "  mock        - Offline development with mock data"
    echo ""
    echo "Examples:"
    echo "  $0 production your-production-project market-data-prod"
    echo "  $0 development your-dev-project market-data-dev"
    echo "  $0 readonly your-project market-data-readonly"
    echo "  $0 mock"
    echo ""
}

# Check if required arguments are provided
if [ $# -lt 1 ]; then
    show_usage
    exit 1
fi

TIER=$1
PROJECT_ID=$2
SERVICE_ACCOUNT_NAME=$3

# Validate arguments
if [ "$TIER" != "mock" ] && [ -z "$PROJECT_ID" ]; then
    echo -e "${RED}❌ PROJECT_ID is required for all tiers except 'mock'${NC}"
    show_usage
    exit 1
fi

if [ "$TIER" != "mock" ] && [ -z "$SERVICE_ACCOUNT_NAME" ]; then
    echo -e "${RED}❌ SERVICE_ACCOUNT_NAME is required for all tiers except 'mock'${NC}"
    show_usage
    exit 1
fi

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo -e "${RED}❌ gcloud CLI is not installed. Please install it first.${NC}"
    echo "Visit: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Check if user is authenticated
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
    echo -e "${YELLOW}⚠️  No active gcloud authentication found. Please run 'gcloud auth login' first.${NC}"
    exit 1
fi

echo -e "${BLUE}🔐 Setting up authentication for tier: ${TIER}${NC}"
echo "============================================="

case $TIER in
    "production")
        echo -e "${YELLOW}Setting up production authentication...${NC}"
        
        # Create service account
        echo "Creating service account: $SERVICE_ACCOUNT_NAME"
        gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME \
            --display-name="Market Data Handler Production" \
            --description="Service account for production market data operations" \
            --project=$PROJECT_ID || echo "Service account may already exist"
        
        # Assign roles
        echo "Assigning IAM roles..."
        
        # Secret Manager access
        gcloud projects add-iam-policy-binding $PROJECT_ID \
            --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
            --role="roles/secretmanager.secretAccessor" \
            --quiet
        
        # GCS access
        gcloud projects add-iam-policy-binding $PROJECT_ID \
            --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
            --role="roles/storage.objectViewer" \
            --quiet
        
        gcloud projects add-iam-policy-binding $PROJECT_ID \
            --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
            --role="roles/storage.objectCreator" \
            --quiet
        
        # Generate and download key
        echo "Generating service account key..."
        gcloud iam service-accounts keys create $SERVICE_ACCOUNT_NAME-key.json \
            --iam-account=$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com \
            --project=$PROJECT_ID
        
        echo -e "${GREEN}✅ Production authentication setup complete!${NC}"
        echo ""
        echo "Environment variables to set:"
        echo "export USE_SECRET_MANAGER=true"
        echo "export GCP_PROJECT_ID=$PROJECT_ID"
        echo "export GCP_CREDENTIALS_PATH=$(pwd)/$SERVICE_ACCOUNT_NAME-key.json"
        echo "export TARDIS_SECRET_NAME=tardis-api-key"
        echo ""
        echo "Next steps:"
        echo "1. Upload your Tardis API key to Secret Manager:"
        echo "   gcloud secrets create tardis-api-key --data-file=- <<< 'TD.your_api_key_here'"
        echo "2. Set the environment variables above"
        echo "3. Test with: python -c \"from src.data_client import DataClient; print('Success!')\""
        ;;
        
    "development")
        echo -e "${YELLOW}Setting up development authentication...${NC}"
        
        # Create service account
        echo "Creating service account: $SERVICE_ACCOUNT_NAME"
        gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME \
            --display-name="Market Data Handler Development" \
            --description="Service account for development market data operations" \
            --project=$PROJECT_ID || echo "Service account may already exist"
        
        # Assign roles (no Secret Manager access)
        echo "Assigning IAM roles..."
        
        gcloud projects add-iam-policy-binding $PROJECT_ID \
            --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
            --role="roles/storage.objectViewer" \
            --quiet
        
        gcloud projects add-iam-policy-binding $PROJECT_ID \
            --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
            --role="roles/storage.objectCreator" \
            --quiet
        
        # Generate and download key
        echo "Generating service account key..."
        gcloud iam service-accounts keys create $SERVICE_ACCOUNT_NAME-key.json \
            --iam-account=$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com \
            --project=$PROJECT_ID
        
        echo -e "${GREEN}✅ Development authentication setup complete!${NC}"
        echo ""
        echo "Environment variables to set:"
        echo "export USE_SECRET_MANAGER=false"
        echo "export TARDIS_API_KEY=TD.your_test_key"
        echo "export GCP_PROJECT_ID=$PROJECT_ID"
        echo "export GCP_CREDENTIALS_PATH=$(pwd)/$SERVICE_ACCOUNT_NAME-key.json"
        echo "export GCS_BUCKET=your-dev-bucket"
        echo ""
        echo "Next steps:"
        echo "1. Set your Tardis API key in TARDIS_API_KEY"
        echo "2. Set your GCS bucket name in GCS_BUCKET"
        echo "3. Set the environment variables above"
        echo "4. Test with: python -c \"from src.data_client import DataClient; print('Success!')\""
        ;;
        
    "readonly")
        echo -e "${YELLOW}Setting up read-only authentication...${NC}"
        
        # Create service account
        echo "Creating service account: $SERVICE_ACCOUNT_NAME"
        gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME \
            --display-name="Market Data Handler Read-Only" \
            --description="Service account for read-only market data access" \
            --project=$PROJECT_ID || echo "Service account may already exist"
        
        # Assign read-only role
        echo "Assigning IAM roles..."
        
        gcloud projects add-iam-policy-binding $PROJECT_ID \
            --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
            --role="roles/storage.objectViewer" \
            --quiet
        
        # Generate and download key
        echo "Generating service account key..."
        gcloud iam service-accounts keys create $SERVICE_ACCOUNT_NAME-key.json \
            --iam-account=$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com \
            --project=$PROJECT_ID
        
        echo -e "${GREEN}✅ Read-only authentication setup complete!${NC}"
        echo ""
        echo "Environment variables to set:"
        echo "export USE_SECRET_MANAGER=false"
        echo "export GCP_PROJECT_ID=$PROJECT_ID"
        echo "export GCP_CREDENTIALS_PATH=$(pwd)/$SERVICE_ACCOUNT_NAME-key.json"
        echo "export GCS_BUCKET=your-data-bucket"
        echo ""
        echo "Next steps:"
        echo "1. Set your GCS bucket name in GCS_BUCKET"
        echo "2. Set the environment variables above"
        echo "3. Test with: python -c \"from src.data_client import DataClient; print('Success!')\""
        ;;
        
    "mock")
        echo -e "${YELLOW}Setting up mock/offline authentication...${NC}"
        
        # Create mock data directory
        echo "Creating mock data directory..."
        mkdir -p mock_data
        
        echo -e "${GREEN}✅ Mock authentication setup complete!${NC}"
        echo ""
        echo "Environment variables to set:"
        echo "export USE_MOCK_DATA=true"
        echo "export MOCK_DATA_PATH=$(pwd)/mock_data"
        echo ""
        echo "Next steps:"
        echo "1. Set the environment variables above"
        echo "2. Test with: python -c \"from src.data_client import DataClient; print('Success!')\""
        echo ""
        echo "Note: Mock data will be automatically generated when you first use the client."
        ;;
        
    *)
        echo -e "${RED}❌ Invalid tier: $TIER${NC}"
        show_usage
        exit 1
        ;;
esac

echo ""
echo -e "${BLUE}🔧 Additional Configuration${NC}"
echo "=============================="
echo ""
echo "You can also create a .env file with these variables:"
echo ""

case $TIER in
    "production")
        echo "USE_SECRET_MANAGER=true"
        echo "GCP_PROJECT_ID=$PROJECT_ID"
        echo "GCP_CREDENTIALS_PATH=$(pwd)/$SERVICE_ACCOUNT_NAME-key.json"
        echo "TARDIS_SECRET_NAME=tardis-api-key"
        echo "GCS_BUCKET=your-production-bucket"
        ;;
    "development")
        echo "USE_SECRET_MANAGER=false"
        echo "TARDIS_API_KEY=TD.your_test_key"
        echo "GCP_PROJECT_ID=$PROJECT_ID"
        echo "GCP_CREDENTIALS_PATH=$(pwd)/$SERVICE_ACCOUNT_NAME-key.json"
        echo "GCS_BUCKET=your-dev-bucket"
        ;;
    "readonly")
        echo "USE_SECRET_MANAGER=false"
        echo "GCP_PROJECT_ID=$PROJECT_ID"
        echo "GCP_CREDENTIALS_PATH=$(pwd)/$SERVICE_ACCOUNT_NAME-key.json"
        echo "GCS_BUCKET=your-data-bucket"
        ;;
    "mock")
        echo "USE_MOCK_DATA=true"
        echo "MOCK_DATA_PATH=$(pwd)/mock_data"
        ;;
esac

echo ""
echo -e "${GREEN}🎉 Setup complete! You can now use the Market Data Handler package.${NC}"
