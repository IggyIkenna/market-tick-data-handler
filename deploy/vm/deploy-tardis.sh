#!/bin/bash

# VM Deployment script for Tardis Tick Data Download
# This script deploys and manages VMs for downloading Tardis tick data

set -e

# Configuration
PROJECT_ID="central-element-323112"
ZONE="asia-northeast1-c"
MACHINE_TYPE="e2-highmem-8"
IMAGE_FAMILY="ubuntu-2004-lts"
IMAGE_PROJECT="ubuntu-os-cloud"
VM_NAME="tardis-downloader"
DOCKER_IMAGE="market-tick-tardis-downloader:latest"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to show usage
show_usage() {
    echo -e "${BLUE}VM Deployment for Tardis Tick Data Download${NC}"
    echo "==============================================="
    echo ""
    echo "Usage: $0 <command> [options]"
    echo ""
    echo "Commands:"
    echo "  deploy          - Deploy a new VM for Tardis data download"
    echo "  start           - Start the VM"
    echo "  stop            - Stop the VM"
    echo "  status          - Show VM status"
    echo "  ssh             - SSH into the VM"
    echo "  logs            - View logs"
    echo "  run             - Run Tardis data download on the VM"
    echo "  delete          - Delete the VM"
    echo "  help            - Show this help"
    echo ""
    echo "Options for 'run' command:"
    echo "  --start-date DATE    Start date (YYYY-MM-DD)"
    echo "  --end-date DATE      End date (YYYY-MM-DD)"
    echo "  --venues VENUES      Comma-separated list of venues"
    echo "  --data-types TYPES   Comma-separated list of data types"
    echo ""
    echo "Examples:"
    echo "  $0 deploy"
    echo "  $0 run --start-date 2023-05-23 --end-date 2023-05-25"
    echo "  $0 status"
}

# Function to check if VM exists
check_vm_exists() {
    gcloud compute instances describe "$VM_NAME" --zone="$ZONE" --project="$PROJECT_ID" >/dev/null 2>&1
}

# Function to get VM status
get_vm_status() {
    if check_vm_exists; then
        gcloud compute instances describe "$VM_NAME" --zone="$ZONE" --project="$PROJECT_ID" --format="value(status)"
    else
        echo "NOT_FOUND"
    fi
}

# Function to deploy VM
deploy_vm() {
    echo -e "${BLUE}☁️  Deploying Tardis Data Download VM${NC}"
    echo "============================================="
    
    # Check if gcloud is installed and authenticated
    if ! command -v gcloud > /dev/null 2>&1; then
        echo -e "${RED}❌ gcloud CLI not found. Please install Google Cloud SDK.${NC}"
        exit 1
    fi

    if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
        echo -e "${RED}❌ Not authenticated with gcloud. Please run 'gcloud auth login'.${NC}"
        exit 1
    fi

    # Set the project
    echo -e "${YELLOW}🔧 Setting project to $PROJECT_ID...${NC}"
    gcloud config set project "$PROJECT_ID"

    # Check if VM already exists
    if check_vm_exists; then
        echo -e "${YELLOW}⚠️  VM $VM_NAME already exists. Deleting...${NC}"
        gcloud compute instances stop "$VM_NAME" --zone="$ZONE" --quiet
        gcloud compute instances delete "$VM_NAME" --zone="$ZONE" --quiet
    fi

    # Create startup script
    echo -e "${YELLOW}📝 Creating startup script...${NC}"
    cat > /tmp/tardis-startup.sh << 'EOF'
#!/bin/bash

# Update system
apt-get update
apt-get install -y docker.io docker-compose git python3 python3-pip

# Start Docker service
systemctl start docker
systemctl enable docker

# Add user to docker group
usermod -aG docker $USER

# Create application directory
mkdir -p /opt/market-tick-data-handler
cd /opt/market-tick-data-handler

# Clone repository (in production, use proper authentication)
# git clone <your-repo-url> .

# For now, we'll assume the code is already there
# In production, you'd want to:
# 1. Use Cloud Build to build and push Docker images
# 2. Pull the image from Container Registry
# 3. Run the container

# Set up environment
cat > .env << 'ENVEOF'
# Tardis API Configuration
TARDIS_API_KEY=TD.l6pTDHIcc9fwJZEz.Y7cp7lBSu-pkPEv.55-ZZYvZqtQL7hY.C2-pXYQ6yebRF7M.DwzJ7MFPry-C7Yp.xe1j
TARDIS_BASE_URL=https://datasets.tardis.dev
TARDIS_TIMEOUT=60
TARDIS_MAX_RETRIES=3
TARDIS_MAX_CONCURRENT=50
MAX_CONCURRENT_REQUESTS=50
MAX_PARALLEL_UPLOADS=20
RATE_LIMIT_PER_VM=1000000

# GCP Configuration
GCP_PROJECT_ID=central-element-323112
GCP_CREDENTIALS_PATH=/opt/market-tick-data-handler/central-element-323112-e35fb0ddafe2.json
GCS_BUCKET=market-data-tick
GCS_REGION=asia-northeast1-c
GOOGLE_APPLICATION_CREDENTIALS=/opt/market-tick-data-handler/central-element-323112-e35fb0ddafe2.json

# Service Configuration
LOG_LEVEL=INFO
LOG_DESTINATION=local
BATCH_SIZE=1000
MEMORY_EFFICIENT=false
ENABLE_CACHING=true
CACHE_TTL=3600

# Data Directories
DATA_DIRECTORY=/opt/market-tick-data-handler/data
PARQUET_DIRECTORY=/opt/market-tick-data-handler/data/parquet
DOWNLOADS_DIRECTORY=/opt/market-tick-data-handler/downloads

# Sharding Configuration (for VM mode)
SHARD_INDEX=0
TOTAL_SHARDS=1
INSTRUMENTS_PER_SHARD=2

# Output Configuration
OUTPUT_FORMAT=json
DEFAULT_LIMIT=10000
INCLUDE_METADATA=true
COMPRESSION=snappy

# Runtime Configuration
DEBUG=false
TEST_MODE=false
ENVEOF

# Create necessary directories
mkdir -p data downloads logs temp parquet

# Copy credentials file
if [ -f /tmp/credentials.json ]; then
    cp /tmp/credentials.json central-element-323112-e35fb0ddafe2.json
    chmod 600 central-element-323112-e35fb0ddafe2.json
fi

# Set up Google Cloud authentication
export GOOGLE_APPLICATION_CREDENTIALS=/opt/market-tick-data-handler/central-element-323112-e35fb0ddafe2.json

# Test GCP authentication
echo "Testing GCP authentication..."
if gcloud auth activate-service-account --key-file=/opt/market-tick-data-handler/central-element-323112-e35fb0ddafe2.json; then
    echo "✅ GCP authentication successful"
else
    echo "❌ GCP authentication failed"
fi

# Set project
gcloud config set project central-element-323112

echo "VM setup completed!"
EOF

    # Create VM
    echo -e "${YELLOW}🚀 Creating VM: $VM_NAME...${NC}"
    gcloud compute instances create "$VM_NAME" \
        --zone="$ZONE" \
        --machine-type="$MACHINE_TYPE" \
        --image-family="$IMAGE_FAMILY" \
        --image-project="$IMAGE_PROJECT" \
        --boot-disk-size=100GB \
        --boot-disk-type=pd-standard \
        --metadata-from-file startup-script=/tmp/tardis-startup.sh \
        --scopes=https://www.googleapis.com/auth/cloud-platform \
        --tags=tardis-downloader

    # Upload credentials if they exist
    if [ -f "central-element-323112-e35fb0ddafe2.json" ]; then
        echo -e "${YELLOW}📤 Uploading credentials...${NC}"
        gcloud compute scp central-element-323112-e35fb0ddafe2.json "$VM_NAME:/tmp/credentials.json" --zone="$ZONE"
    fi

    echo -e "${GREEN}✅ VM created successfully!${NC}"
    echo ""
    echo -e "${BLUE}📋 VM Details:${NC}"
    echo "  Name: $VM_NAME"
    echo "  Zone: $ZONE"
    echo "  Machine Type: $MACHINE_TYPE"
    echo "  Project: $PROJECT_ID"
    echo ""
    echo -e "${BLUE}📋 Next steps:${NC}"
    echo "1. Wait for VM to start up (2-3 minutes)"
    echo "2. SSH into the VM: $0 ssh"
    echo "3. Run data download: $0 run --start-date 2023-05-23 --end-date 2023-05-25"

    # Clean up
    rm -f /tmp/tardis-startup.sh
}

# Function to start VM
start_vm() {
    echo -e "${YELLOW}🚀 Starting VM...${NC}"
    
    if check_vm_exists; then
        gcloud compute instances start "$VM_NAME" --zone="$ZONE" --project="$PROJECT_ID"
        echo -e "${GREEN}✅ VM started${NC}"
        
        # Wait for VM to be ready
        echo -e "${YELLOW}⏳ Waiting for VM to be ready...${NC}"
        sleep 30
    else
        echo -e "${RED}❌ VM does not exist. Run '$0 deploy' to create it.${NC}"
        exit 1
    fi
}

# Function to stop VM
stop_vm() {
    echo -e "${YELLOW}🛑 Stopping VM...${NC}"
    
    if check_vm_exists; then
        gcloud compute instances stop "$VM_NAME" --zone="$ZONE" --project="$PROJECT_ID"
        echo -e "${GREEN}✅ VM stopped${NC}"
    else
        echo -e "${RED}❌ VM does not exist${NC}"
        exit 1
    fi
}

# Function to show VM status
show_status() {
    echo -e "${BLUE}📊 VM Status${NC}"
    echo "============="
    
    if check_vm_exists; then
        VM_STATUS=$(get_vm_status)
        echo -e "VM Status: ${GREEN}$VM_STATUS${NC}"
        
        if [ "$VM_STATUS" = "RUNNING" ]; then
            # Get external IP
            EXTERNAL_IP=$(gcloud compute instances describe "$VM_NAME" --zone="$ZONE" --project="$PROJECT_ID" --format="value(networkInterfaces[0].accessConfigs[0].natIP)")
            echo "External IP: $EXTERNAL_IP"
            
            # Check if data download is running
            echo -e "\n${YELLOW}Checking data download process...${NC}"
            gcloud compute ssh "$VM_NAME" --zone="$ZONE" --project="$PROJECT_ID" --command="
                if pgrep -f 'python.*main.*download' > /dev/null; then
                    echo '✅ Data download is running'
                    ps aux | grep 'python.*main.*download' | grep -v grep
                else
                    echo '❌ Data download is not running'
                fi
            " 2>/dev/null || echo "Could not check process status"
        fi
    else
        echo -e "${RED}❌ VM does not exist${NC}"
    fi
}

# Function to SSH into VM
ssh_vm() {
    echo -e "${YELLOW}🔗 Connecting to VM...${NC}"
    
    if check_vm_exists; then
        VM_STATUS=$(get_vm_status)
        
        if [ "$VM_STATUS" = "RUNNING" ]; then
            gcloud compute ssh "$VM_NAME" --zone="$ZONE" --project="$PROJECT_ID"
        else
            echo -e "${RED}❌ VM is not running. Start it first with: $0 start${NC}"
            exit 1
        fi
    else
        echo -e "${RED}❌ VM does not exist. Run '$0 deploy' to create it.${NC}"
        exit 1
    fi
}

# Function to view logs
view_logs() {
    echo -e "${YELLOW}📋 Viewing logs...${NC}"
    
    if check_vm_exists; then
        VM_STATUS=$(get_vm_status)
        
        if [ "$VM_STATUS" = "RUNNING" ]; then
            gcloud compute ssh "$VM_NAME" --zone="$ZONE" --project="$PROJECT_ID" --command="
                cd /opt/market-tick-data-handler
                if [ -f logs/tardis_download.log ]; then
                    tail -50 logs/tardis_download.log
                else
                    echo 'No logs found'
                fi
            "
        else
            echo -e "${RED}❌ VM is not running${NC}"
            exit 1
        fi
    else
        echo -e "${RED}❌ VM does not exist${NC}"
        exit 1
    fi
}

# Function to run Tardis data download
run_download() {
    local start_date=""
    local end_date=""
    local venues=""
    local data_types=""
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --start-date)
                start_date="$2"
                shift 2
                ;;
            --end-date)
                end_date="$2"
                shift 2
                ;;
            --venues)
                venues="$2"
                shift 2
                ;;
            --data-types)
                data_types="$2"
                shift 2
                ;;
            *)
                echo "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
    done
    
    # Validate required parameters
    if [ -z "$start_date" ] || [ -z "$end_date" ]; then
        echo -e "${RED}❌ --start-date and --end-date are required${NC}"
        show_usage
        exit 1
    fi
    
    echo -e "${BLUE}🏃 Running Tardis data download...${NC}"
    echo "Start Date: $start_date"
    echo "End Date: $end_date"
    echo "Venues: ${venues:-all}"
    echo "Data Types: ${data_types:-all}"
    
    if check_vm_exists; then
        VM_STATUS=$(get_vm_status)
        
        if [ "$VM_STATUS" = "RUNNING" ]; then
            # Build command
            cmd="cd /opt/market-tick-data-handler && python3 -m src.main --mode download --start-date $start_date --end-date $end_date"
            if [ -n "$venues" ]; then
                cmd="$cmd --venues $venues"
            fi
            if [ -n "$data_types" ]; then
                cmd="$cmd --data-types $data_types"
            fi
            
            echo -e "${YELLOW}Executing: $cmd${NC}"
            
            gcloud compute ssh "$VM_NAME" --zone="$ZONE" --project="$PROJECT_ID" --command="$cmd"
        else
            echo -e "${RED}❌ VM is not running. Start it first with: $0 start${NC}"
            exit 1
        fi
    else
        echo -e "${RED}❌ VM does not exist. Run '$0 deploy' to create it.${NC}"
        exit 1
    fi
}

# Function to delete VM
delete_vm() {
    echo -e "${YELLOW}⚠️  This will permanently delete the VM and all its data!${NC}"
    read -p "Are you sure? (y/N): " -n 1 -r
    echo
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${YELLOW}🗑️  Deleting VM...${NC}"
        
        if check_vm_exists; then
            gcloud compute instances delete "$VM_NAME" --zone="$ZONE" --project="$PROJECT_ID" --quiet
            echo -e "${GREEN}✅ VM deleted${NC}"
        else
            echo -e "${RED}❌ VM does not exist${NC}"
        fi
    else
        echo -e "${YELLOW}Deletion cancelled${NC}"
    fi
}

# Main execution
case "${1:-help}" in
    deploy)
        deploy_vm
        ;;
    start)
        start_vm
        ;;
    stop)
        stop_vm
        ;;
    status)
        show_status
        ;;
    ssh)
        ssh_vm
        ;;
    logs)
        view_logs
        ;;
    run)
        shift
        run_download "$@"
        ;;
    delete)
        delete_vm
        ;;
    help|--help|-h)
        show_usage
        ;;
    *)
        echo -e "${RED}❌ Unknown command: $1${NC}"
        show_usage
        exit 1
        ;;
esac
