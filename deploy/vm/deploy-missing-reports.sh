#!/bin/bash

# VM Deployment script for Missing Data Reports
# This script deploys and manages VMs for generating missing data reports

set -e

# Configuration
PROJECT_ID="central-element-323112"
ZONE="asia-northeast1-c"
MACHINE_TYPE="e2-standard-4"
IMAGE_FAMILY="ubuntu-2204-lts"
IMAGE_PROJECT="ubuntu-os-cloud"
VM_NAME="missing-reports-vm"
DOCKER_IMAGE="asia-northeast1-docker.pkg.dev/central-element-323112/market-data-tick-handler/market-tick-tardis-downloader:latest"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to show usage
show_usage() {
    echo -e "${BLUE}VM Deployment for Missing Data Reports${NC}"
    echo "============================================="
    echo ""
    echo "Usage: $0 <command> [options]"
    echo ""
    echo "Commands:"
    echo "  deploy          - Deploy a new VM for missing reports generation"
    echo "  start           - Start the VM"
    echo "  stop            - Stop the VM"
    echo "  status          - Show VM status"
    echo "  health          - Check VM health and integrity events"
    echo "  ssh             - SSH into the VM"
    echo "  logs            - View logs"
    echo "  run             - Run missing reports generation on the VM"
    echo "  delete          - Delete the VM"
    echo "  help            - Show this help"
    echo ""
    echo "Options for 'run' command:"
    echo "  --start-date DATE    Start date (YYYY-MM-DD)"
    echo "  --end-date DATE      End date (YYYY-MM-DD)"
    echo "  --venues VENUES      Comma-separated list of venues"
    echo "  --instrument-types TYPES  Comma-separated list of instrument types"
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
    echo -e "${BLUE}☁️  Deploying Missing Reports VM${NC}"
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
    
    # Run quality gates before deployment
    echo -e "${YELLOW}🧪 Running quality gates...${NC}"
    if [ -f "scripts/pre_deploy_check.sh" ]; then
        if ./scripts/pre_deploy_check.sh; then
            echo -e "${GREEN}✅ Quality gates passed${NC}"
        else
            echo -e "${RED}❌ Quality gates failed. Deployment aborted.${NC}"
            exit 1
        fi
    else
        echo -e "${YELLOW}⚠️  Warning: Pre-deploy check script not found, skipping quality gates${NC}"
    fi

    # Check if VM already exists
    if check_vm_exists; then
        echo -e "${YELLOW}⚠️  VM $VM_NAME already exists. Use 'delete' command first to remove it.${NC}"
        exit 1
    fi

    # Create startup script
    echo -e "${YELLOW}📝 Creating startup script...${NC}"
    cat > /tmp/missing-reports-startup.sh << 'EOF'
#!/bin/bash
set -e

# Update system
apt-get update
apt-get install -y docker.io

# Start Docker service
systemctl start docker
systemctl enable docker

# Add user to docker group
usermod -aG docker $USER

# Pull the Docker image
docker pull asia-northeast1-docker.pkg.dev/central-element-323112/market-data-tick-handler/market-tick-tardis-downloader:latest

# Create log directory
mkdir -p /var/log/market-data

# Set up log rotation
cat > /etc/logrotate.d/market-data << 'LOGROTATE_EOF'
/var/log/market-data/*.log {
    daily
    missingok
    rotate 7
    compress
    delaycompress
    notifempty
    create 644 root root
}
LOGROTATE_EOF

echo "Startup script completed successfully"
EOF

    chmod +x /tmp/missing-reports-startup.sh

    # Create the VM
    echo -e "${YELLOW}🚀 Creating VM instance...${NC}"
    gcloud compute instances create "$VM_NAME" \
        --zone="$ZONE" \
        --machine-type="$MACHINE_TYPE" \
        --image-family="$IMAGE_FAMILY" \
        --image-project="$IMAGE_PROJECT" \
        --boot-disk-size=20GB \
        --boot-disk-type=pd-standard \
        --tags=market-data-missing-reports \
        --metadata-from-file startup-script=/tmp/missing-reports-startup.sh \
        --scopes=https://www.googleapis.com/auth/cloud-platform \
        --preemptible

    # Wait for VM to be ready
    echo -e "${YELLOW}⏳ Waiting for VM to be ready...${NC}"
    sleep 30

    # Get VM IP
    VM_IP=$(gcloud compute instances describe "$VM_NAME" --zone="$ZONE" --format="value(networkInterfaces[0].accessConfigs[0].natIP)")
    echo -e "${GREEN}✅ VM deployed successfully!${NC}"
    echo -e "${BLUE}📋 VM Details:${NC}"
    echo "  Name: $VM_NAME"
    echo "  Zone: $ZONE"
    echo "  IP: $VM_IP"
    echo "  Machine Type: $MACHINE_TYPE"
    echo ""
    echo -e "${YELLOW}💡 Next steps:${NC}"
    echo "  - Wait for startup script to complete (2-3 minutes)"
    echo "  - Check status: $0 status"
    echo "  - Run missing reports: $0 run --start-date 2023-05-23 --end-date 2023-05-25"
    echo "  - View logs: $0 logs"
    
    # Clean up
    rm -f /tmp/missing-reports-startup.sh
}

# Function to start VM
start_vm() {
    echo -e "${YELLOW}🚀 Starting VM...${NC}"
    
    if ! check_vm_exists; then
        echo -e "${RED}❌ VM does not exist. Please deploy first.${NC}"
        exit 1
    fi
    
    gcloud compute instances start "$VM_NAME" --zone="$ZONE"
    echo -e "${GREEN}✅ VM started successfully${NC}"
}

# Function to stop VM
stop_vm() {
    echo -e "${YELLOW}🛑 Stopping VM...${NC}"
    
    if ! check_vm_exists; then
        echo -e "${RED}❌ VM does not exist${NC}"
        exit 1
    fi
    
    gcloud compute instances stop "$VM_NAME" --zone="$ZONE"
    echo -e "${GREEN}✅ VM stopped successfully${NC}"
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
            
            # Check if missing reports generation is running
            echo -e "\n${YELLOW}Checking missing reports generation process...${NC}"
            gcloud compute ssh "$VM_NAME" --zone="$ZONE" --project="$PROJECT_ID" --command="
                if pgrep -f 'python.*main.*missing-reports' > /dev/null; then
                    echo '✅ Missing reports generation is running'
                    ps aux | grep 'python.*main.*missing-reports' | grep -v grep
                else
                    echo '❌ Missing reports generation is not running'
                fi
            " 2>/dev/null || echo "Could not check process status"
        fi
    else
        echo -e "${RED}❌ VM does not exist${NC}"
    fi
}

# Function to check VM health
check_vm_health() {
    echo -e "${YELLOW}🔍 Checking VM health for $VM_NAME...${NC}"
    
    # Check if VM is running
    local status=$(gcloud compute instances describe $VM_NAME --zone=$ZONE --format="value(status)" 2>/dev/null || echo "NOT_FOUND")
    
    if [ "$status" = "NOT_FOUND" ]; then
        echo -e "${RED}❌ VM $VM_NAME not found${NC}"
        return 1
    elif [ "$status" = "RUNNING" ]; then
        echo -e "${GREEN}✅ VM $VM_NAME is running${NC}"
        
        # Check for integrity events
        echo -e "${YELLOW}🔍 Checking for integrity events...${NC}"
        local integrity_events=$(gcloud logging read "resource.type=gce_instance AND resource.labels.instance_id=$VM_NAME AND jsonPayload.@type=type.googleapis.com/cloud_integrity.IntegrityEvent" --limit=5 --format="value(timestamp)" 2>/dev/null | wc -l)
        
        if [ "$integrity_events" -gt 0 ]; then
            echo -e "${YELLOW}⚠️  Found $integrity_events integrity events (this is normal for Shielded VMs)${NC}"
        else
            echo -e "${GREEN}✅ No recent integrity events${NC}"
        fi
        
        return 0
    else
        echo -e "${RED}❌ VM $VM_NAME status: $status${NC}"
        return 1
    fi
}

# Function to SSH into VM
ssh_vm() {
    echo -e "${YELLOW}🔗 Connecting to VM...${NC}"
    
    if check_vm_exists; then
        VM_STATUS=$(get_vm_status)
        if [ "$VM_STATUS" = "RUNNING" ]; then
            gcloud compute ssh "$VM_NAME" --zone="$ZONE"
        else
            echo -e "${RED}❌ VM is not running. Current status: $VM_STATUS${NC}"
            exit 1
        fi
    else
        echo -e "${RED}❌ VM does not exist${NC}"
        exit 1
    fi
}

# Function to view logs
view_logs() {
    echo -e "${YELLOW}📋 Viewing VM logs...${NC}"
    
    if check_vm_exists; then
        VM_STATUS=$(get_vm_status)
        if [ "$VM_STATUS" = "RUNNING" ]; then
            echo -e "${BLUE}Recent startup logs:${NC}"
            gcloud compute instances get-serial-port-output "$VM_NAME" --zone="$ZONE" --port=1 | tail -50
            
            echo -e "\n${BLUE}Application logs:${NC}"
            gcloud compute ssh "$VM_NAME" --zone="$ZONE" --command="
                if [ -f /var/log/market-data/missing-reports.log ]; then
                    tail -50 /var/log/market-data/missing-reports.log
                else
                    echo 'No application logs found'
                fi
            " 2>/dev/null || echo "Could not retrieve application logs"
        else
            echo -e "${RED}❌ VM is not running. Current status: $VM_STATUS${NC}"
            exit 1
        fi
    else
        echo -e "${RED}❌ VM does not exist${NC}"
        exit 1
    fi
}

# Function to run missing reports generation
run_missing_reports() {
    local start_date=""
    local end_date=""
    local venues=""
    local instrument_types=""
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
            --instrument-types)
                instrument_types="$2"
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
    
    echo -e "${BLUE}🏃 Running missing reports generation...${NC}"
    echo "Start Date: $start_date"
    echo "End Date: $end_date"
    echo "Venues: ${venues:-all}"
    echo "Instrument Types: ${instrument_types:-all}"
    echo "Data Types: ${data_types:-all}"
    
    if check_vm_exists; then
        VM_STATUS=$(get_vm_status)
        if [ "$VM_STATUS" = "RUNNING" ]; then
            # Build command
            local cmd="docker run --rm -v /var/log/market-data:/app/logs"
            cmd="$cmd asia-northeast1-docker.pkg.dev/central-element-323112/market-data-tick-handler/market-tick-tardis-downloader:latest"
            cmd="$cmd python -m market_data_tick_handler.main --mode missing-reports"
            cmd="$cmd --start-date $start_date --end-date $end_date"
            
            if [ -n "$venues" ]; then
                cmd="$cmd --venues $venues"
            fi
            if [ -n "$instrument_types" ]; then
                cmd="$cmd --instrument-types $instrument_types"
            fi
            if [ -n "$data_types" ]; then
                cmd="$cmd --data-types $data_types"
            fi
            
            # Run the command
            echo -e "${YELLOW}🚀 Executing command on VM...${NC}"
            gcloud compute ssh "$VM_NAME" --zone="$ZONE" --command="$cmd"
            
            echo -e "${GREEN}✅ Missing reports generation completed${NC}"
        else
            echo -e "${RED}❌ VM is not running. Current status: $VM_STATUS${NC}"
            exit 1
        fi
    else
        echo -e "${RED}❌ VM does not exist${NC}"
        exit 1
    fi
}

# Function to delete VM
delete_vm() {
    echo -e "${YELLOW}🗑️  Deleting VM...${NC}"
    
    if ! check_vm_exists; then
        echo -e "${RED}❌ VM does not exist${NC}"
        exit 1
    fi
    
    gcloud compute instances delete "$VM_NAME" --zone="$ZONE" --quiet
    echo -e "${GREEN}✅ VM deleted successfully${NC}"
}

# Main execution
case "$1" in
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
    health)
        check_vm_health
        ;;
    ssh)
        ssh_vm
        ;;
    logs)
        view_logs
        ;;
    run)
        shift
        run_missing_reports "$@"
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
