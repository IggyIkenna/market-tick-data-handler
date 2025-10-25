#!/bin/bash

# Setup Google Cloud Secret Manager for Market Data Handler
# This script helps set up the Tardis API key in Secret Manager

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
SECRET_NAME="tardis-api-key"
PROJECT_ID=""
API_KEY=""

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -p, --project-id PROJECT_ID    GCP Project ID (required)"
    echo "  -k, --api-key API_KEY         Tardis API key (required)"
    echo "  -s, --secret-name NAME        Secret name (default: tardis-api-key)"
    echo "  -h, --help                    Show this help message"
    echo ""
    echo "Example:"
    echo "  $0 --project-id my-project --api-key TD.your_api_key_here"
    echo "  $0 -p my-project -k TD.your_api_key_here -s custom-secret-name"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -p|--project-id)
            PROJECT_ID="$2"
            shift 2
            ;;
        -k|--api-key)
            API_KEY="$2"
            shift 2
            ;;
        -s|--secret-name)
            SECRET_NAME="$2"
            shift 2
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Validate required parameters
if [[ -z "$PROJECT_ID" ]]; then
    print_error "Project ID is required. Use -p or --project-id"
    show_usage
    exit 1
fi

if [[ -z "$API_KEY" ]]; then
    print_error "API key is required. Use -k or --api-key"
    show_usage
    exit 1
fi

# Validate API key format
if [[ ! "$API_KEY" =~ ^TD\. ]]; then
    print_warning "API key doesn't start with 'TD.' - please verify it's correct"
fi

print_status "Setting up Secret Manager for project: $PROJECT_ID"
print_status "Secret name: $SECRET_NAME"

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    print_error "gcloud CLI is not installed. Please install it first:"
    echo "  https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Check if user is authenticated
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
    print_error "No active gcloud authentication found. Please run:"
    echo "  gcloud auth login"
    exit 1
fi

# Set the project
print_status "Setting project to: $PROJECT_ID"
gcloud config set project "$PROJECT_ID"

# Enable Secret Manager API
print_status "Enabling Secret Manager API..."
gcloud services enable secretmanager.googleapis.com

# Check if secret already exists
if gcloud secrets describe "$SECRET_NAME" --project="$PROJECT_ID" &>/dev/null; then
    print_warning "Secret '$SECRET_NAME' already exists."
    read -p "Do you want to add a new version? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_status "Exiting without changes."
        exit 0
    fi
else
    # Create the secret
    print_status "Creating secret: $SECRET_NAME"
    echo -n | gcloud secrets create "$SECRET_NAME" --data-file=- --project="$PROJECT_ID"
fi

# Add the secret version
print_status "Adding API key to secret..."
echo -n "$API_KEY" | gcloud secrets versions add "$SECRET_NAME" --data-file=- --project="$PROJECT_ID"

# Get the current service account
SERVICE_ACCOUNT=$(gcloud config get-value account)
if [[ -n "$SERVICE_ACCOUNT" ]]; then
    print_status "Granting access to current user: $SERVICE_ACCOUNT"
    gcloud secrets add-iam-policy-binding "$SECRET_NAME" \
        --member="user:$SERVICE_ACCOUNT" \
        --role="roles/secretmanager.secretAccessor" \
        --project="$PROJECT_ID"
fi

# Test the secret
print_status "Testing secret retrieval..."
RETRIEVED_KEY=$(gcloud secrets versions access latest --secret="$SECRET_NAME" --project="$PROJECT_ID")

if [[ "$RETRIEVED_KEY" == "$API_KEY" ]]; then
    print_status "✅ Secret setup successful!"
    echo ""
    echo "Next steps:"
    echo "1. Update your .env file with:"
    echo "   USE_SECRET_MANAGER=true"
    echo "   TARDIS_SECRET_NAME=$SECRET_NAME"
    echo "   GCP_PROJECT_ID=$PROJECT_ID"
    echo ""
    echo "2. For service accounts, grant access with:"
    echo "   gcloud secrets add-iam-policy-binding $SECRET_NAME \\"
    echo "     --member=\"serviceAccount:YOUR_SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com\" \\"
    echo "     --role=\"roles/secretmanager.secretAccessor\""
    echo ""
    echo "3. Remove or comment out the TARDIS_API_KEY from your .env file for security"
else
    print_error "❌ Secret retrieval test failed. The retrieved key doesn't match the input."
    exit 1
fi
