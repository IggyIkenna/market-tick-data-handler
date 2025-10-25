#!/bin/bash

# Sharded VM Deployment Script
# This script creates multiple VMs to distribute workload across shards
# Each VM processes one full day at a time

set -e

# Configuration
PROJECT_ID="central-element-323112"
ZONE="asia-northeast1-c"
MACHINE_TYPE_INSTRUMENTS="e2-standard-4"
MACHINE_TYPE_TARDIS="e2-highmem-8"  # 8 vCPU, 64GB RAM - optimal for Tardis download with 2 workers
IMAGE_FAMILY="ubuntu-2004-lts"
IMAGE_PROJECT="ubuntu-os-cloud"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to show usage
show_usage() {
    echo -e "${BLUE}Sharded VM Deployment Script${NC}"
    echo "=============================="
    echo ""
    echo "Usage: $0 <mode> [options]"
    echo ""
    echo "Modes:"
    echo "  instruments     - Deploy VMs for instrument definitions"
    echo "  tardis          - Deploy VMs for Tardis data download"
    echo "  cleanup         - Clean up all sharded VMs"
    echo ""
    echo "Options:"
    echo "  --start-date DATE    Start date (YYYY-MM-DD)"
    echo "  --end-date DATE      End date (YYYY-MM-DD)"
    echo "  --shards NUM         Number of shards/VMs (default: 10)"
    echo "  --start-shard NUM    Starting shard index (default: 0)"
    echo "  --end-shard NUM      Ending shard index (default: shards-1)"
    echo "  --venues VENUES      Comma-separated list of venues (tardis only)"
    echo "  --data-types TYPES   Comma-separated list of data types (tardis only)"
    echo "  --preemptible        Use preemptible instances (cheaper but can be terminated)"
    echo "  --help              Show this help"
    echo ""
    echo "Examples:"
    echo "  $0 instruments --start-date 2023-05-23 --end-date 2023-05-25 --shards 5"
    echo "  $0 tardis --start-date 2023-05-23 --end-date 2023-05-25 --shards 10 --venues deribit,binance"
    echo "  $0 cleanup"
}

# Function to calculate date range
calculate_date_range() {
    local start_date="$1"
    local end_date="$2"
    
    # Calculate total days (macOS compatible)
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        START_EPOCH=$(date -j -f "%Y-%m-%d" "$start_date" +%s)
        END_EPOCH=$(date -j -f "%Y-%m-%d" "$end_date" +%s)
    else
        # Linux
        START_EPOCH=$(date -d "$start_date" +%s)
        END_EPOCH=$(date -d "$end_date" +%s)
    fi
    TOTAL_DAYS=$(( (END_EPOCH - START_EPOCH) / 86400 + 1 ))
    
    echo "$TOTAL_DAYS"
}

# Function to get date for shard
get_shard_date() {
    local shard_index="$1"
    local start_date="$2"
    local total_days="$3"
    
    # Calculate which day this shard should process
    local day_index=$((shard_index % total_days))
    
    # Calculate the date (macOS compatible)
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        local start_epoch=$(date -j -f "%Y-%m-%d" "$start_date" +%s)
        local shard_epoch=$((start_epoch + day_index * 86400))
        date -r "$shard_epoch" +%Y-%m-%d
    else
        # Linux
        date -d "$start_date + $day_index days" +%Y-%m-%d
    fi
}

# Function to deploy instrument VMs
deploy_instrument_vms() {
    local start_date="$1"
    local end_date="$2"
    local num_shards="$3"
    local start_shard="$4"
    local end_shard="$5"
    local preemptible="$6"
    
    echo -e "${BLUE}🏗️  Deploying Instrument Definition VMs${NC}"
    echo "============================================="
    echo "Date Range: $start_date to $end_date"
    echo "Shards: $start_shard to $end_shard ($num_shards total)"
    echo "Machine Type: $MACHINE_TYPE_INSTRUMENTS"
    echo "Preemptible: $preemptible"
    echo ""
    
    local total_days=$(calculate_date_range "$start_date" "$end_date")
    echo "Total days to process: $total_days"
    echo ""
    
    # Create startup script for instruments
    cat > /tmp/instrument-shard-startup.sh << 'EOF'
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

# Set up environment
cat > .env << 'ENVEOF'
# Tardis API Configuration
# For production, use Secret Manager instead of hardcoded API key
USE_SECRET_MANAGER=true
TARDIS_SECRET_NAME=tardis-api-key
# Fallback API key (remove in production)
TARDIS_API_KEY=TD.l6pTDHIcc9fwJZEz.Y7cp7lBSu-pkPEv.55-ZZYvZqtQL7hY.C2-pXYQ6yebRF7M.DwzJ7MFPry-C7Yp.xe1j
TARDIS_BASE_URL=https://datasets.tardis.dev
TARDIS_TIMEOUT=60
TARDIS_MAX_RETRIES=3
TARDIS_MAX_CONCURRENT=50
MAX_CONCURRENT_REQUESTS=50
MAX_PARALLEL_UPLOADS=20
DOWNLOAD_MAX_WORKERS=2
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

# Sharding Configuration (will be overridden by metadata)
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
mkdir -p data logs temp

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

# Get shard information
SHARD_INDEX=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/shard-index 2>/dev/null || echo "0")
START_DATE=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/start-date 2>/dev/null || echo "2023-05-23")
END_DATE=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/end-date 2>/dev/null || echo "2023-05-23")
TOTAL_DAYS=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/total-days 2>/dev/null || echo "1")

# Calculate date for this shard
if [[ "$OSTYPE" == "darwin"* ]]; then
    START_EPOCH=$(date -j -f "%Y-%m-%d" "$START_DATE" +%s)
    DAY_INDEX=$((SHARD_INDEX % TOTAL_DAYS))
    SHARD_EPOCH=$((START_EPOCH + DAY_INDEX * 86400))
    SHARD_DATE=$(date -r "$SHARD_EPOCH" +%Y-%m-%d)
else
    DAY_INDEX=$((SHARD_INDEX % TOTAL_DAYS))
    SHARD_DATE=$(date -d "$START_DATE + $DAY_INDEX days" +%Y-%m-%d)
fi

echo "Shard $SHARD_INDEX processing date: $SHARD_DATE"

# Run instrument generation for this shard's date
echo "Starting instrument generation for $SHARD_DATE..."
python3 -m market_data_tick_handler.main --mode instruments --start-date "$SHARD_DATE" --end-date "$SHARD_DATE"

echo "Instrument generation completed for shard $SHARD_INDEX"
EOF

    # Deploy VMs
    for i in $(seq "$start_shard" "$end_shard"); do
        local vm_name="instrument-shard-$i"
        local shard_date=$(get_shard_date "$i" "$start_date" "$total_days")
        
        echo -e "${YELLOW}Creating VM $vm_name for date $shard_date...${NC}"
        
        # Build gcloud command
        local gcloud_cmd="gcloud compute instances create $vm_name"
        gcloud_cmd="$gcloud_cmd --zone=$ZONE"
        gcloud_cmd="$gcloud_cmd --machine-type=$MACHINE_TYPE_INSTRUMENTS"
        gcloud_cmd="$gcloud_cmd --image-family=$IMAGE_FAMILY"
        gcloud_cmd="$gcloud_cmd --image-project=$IMAGE_PROJECT"
        gcloud_cmd="$gcloud_cmd --boot-disk-size=50GB"
        gcloud_cmd="$gcloud_cmd --boot-disk-type=pd-standard"
        gcloud_cmd="$gcloud_cmd --metadata-from-file startup-script=/tmp/instrument-shard-startup.sh"
        gcloud_cmd="$gcloud_cmd --metadata shard-index=$i,start-date=$start_date,end-date=$end_date,total-days=$total_days"
        gcloud_cmd="$gcloud_cmd --scopes=https://www.googleapis.com/auth/cloud-platform"
        gcloud_cmd="$gcloud_cmd --tags=instrument-shard"
        
        if [ "$preemptible" = "true" ]; then
            gcloud_cmd="$gcloud_cmd --preemptible"
        fi
        
        # Create VM
        eval "$gcloud_cmd"
        
        # Upload credentials if they exist
        if [ -f "central-element-323112-e35fb0ddafe2.json" ]; then
            gcloud compute scp central-element-323112-e35fb0ddafe2.json "$vm_name:/tmp/credentials.json" --zone="$ZONE"
        fi
        
        echo -e "${GREEN}✅ Created $vm_name${NC}"
    done
    
    echo ""
    echo -e "${GREEN}🎉 Successfully deployed $num_shards instrument VMs!${NC}"
    echo ""
    echo -e "${BLUE}📊 Summary:${NC}"
    echo "  - VM names: instrument-shard-$start_shard to instrument-shard-$end_shard"
    echo "  - Each VM processes one day of instrument definitions"
    echo "  - Data is uploaded to GCS daily (small files, aggregated)"
    echo ""
    echo -e "${BLUE}🔍 Monitor with:${NC}"
    echo "  gcloud compute instances list --filter='name~instrument-shard'"
    echo "  gsutil ls gs://market-data-tick/instruments/daily/"
    
    # Clean up
    rm -f /tmp/instrument-shard-startup.sh
}

# Function to deploy Tardis VMs
deploy_tardis_vms() {
    local start_date="$1"
    local end_date="$2"
    local num_shards="$3"
    local start_shard="$4"
    local end_shard="$5"
    local venues="$6"
    local data_types="$7"
    local preemptible="$8"
    
    echo -e "${BLUE}🏗️  Deploying Tardis Data Download VMs${NC}"
    echo "============================================="
    echo "Date Range: $start_date to $end_date"
    echo "Shards: $start_shard to $end_shard ($num_shards total)"
    echo "Machine Type: $MACHINE_TYPE_TARDIS"
    echo "Venues: ${venues:-all}"
    echo "Data Types: ${data_types:-all}"
    echo "Preemptible: $preemptible"
    echo ""
    
    local total_days=$(calculate_date_range "$start_date" "$end_date")
    echo "Total days to process: $total_days"
    echo ""
    
    # Create startup script for Tardis
    cat > /tmp/tardis-shard-startup.sh << 'EOF'
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

# Set up environment
cat > .env << 'ENVEOF'
# Tardis API Configuration
# For production, use Secret Manager instead of hardcoded API key
USE_SECRET_MANAGER=true
TARDIS_SECRET_NAME=tardis-api-key
# Fallback API key (remove in production)
TARDIS_API_KEY=TD.l6pTDHIcc9fwJZEz.Y7cp7lBSu-pkPEv.55-ZZYvZqtQL7hY.C2-pXYQ6yebRF7M.DwzJ7MFPry-C7Yp.xe1j
TARDIS_BASE_URL=https://datasets.tardis.dev
TARDIS_TIMEOUT=60
TARDIS_MAX_RETRIES=3
TARDIS_MAX_CONCURRENT=50
MAX_CONCURRENT_REQUESTS=50
MAX_PARALLEL_UPLOADS=20
DOWNLOAD_MAX_WORKERS=2
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

# Sharding Configuration (will be overridden by metadata)
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

# Get shard information
SHARD_INDEX=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/shard-index 2>/dev/null || echo "0")
START_DATE=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/start-date 2>/dev/null || echo "2023-05-23")
END_DATE=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/end-date 2>/dev/null || echo "2023-05-23")
TOTAL_DAYS=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/total-days 2>/dev/null || echo "1")
VENUES=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/venues 2>/dev/null || echo "")
DATA_TYPES=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/data-types 2>/dev/null || echo "")

# Calculate date for this shard
if [[ "$OSTYPE" == "darwin"* ]]; then
    START_EPOCH=$(date -j -f "%Y-%m-%d" "$START_DATE" +%s)
    DAY_INDEX=$((SHARD_INDEX % TOTAL_DAYS))
    SHARD_EPOCH=$((START_EPOCH + DAY_INDEX * 86400))
    SHARD_DATE=$(date -r "$SHARD_EPOCH" +%Y-%m-%d)
else
    DAY_INDEX=$((SHARD_INDEX % TOTAL_DAYS))
    SHARD_DATE=$(date -d "$START_DATE + $DAY_INDEX days" +%Y-%m-%d)
fi

echo "Shard $SHARD_INDEX processing date: $SHARD_DATE"

# Run data download for this shard's date with sharding parameters
echo "Starting data download for $SHARD_DATE..."
cmd="python3 -m market_data_tick_handler.main --mode download --start-date $SHARD_DATE --end-date $SHARD_DATE --shard-index $SHARD_INDEX --total-shards $TOTAL_SHARDS"
if [ -n "$VENUES" ]; then
    cmd="$cmd --venues $VENUES"
fi
if [ -n "$DATA_TYPES" ]; then
    cmd="$cmd --data-types $DATA_TYPES"
fi

eval "$cmd"

echo "Data download completed for shard $SHARD_INDEX"
EOF

    # Deploy VMs
    for i in $(seq "$start_shard" "$end_shard"); do
        local vm_name="tardis-shard-$i"
        local shard_date=$(get_shard_date "$i" "$start_date" "$total_days")
        
        echo -e "${YELLOW}Creating VM $vm_name for date $shard_date...${NC}"
        
        # Build gcloud command
        local gcloud_cmd="gcloud compute instances create $vm_name"
        gcloud_cmd="$gcloud_cmd --zone=$ZONE"
        gcloud_cmd="$gcloud_cmd --machine-type=$MACHINE_TYPE_TARDIS"
        gcloud_cmd="$gcloud_cmd --image-family=$IMAGE_FAMILY"
        gcloud_cmd="$gcloud_cmd --image-project=$IMAGE_PROJECT"
        gcloud_cmd="$gcloud_cmd --boot-disk-size=100GB"
        gcloud_cmd="$gcloud_cmd --boot-disk-type=pd-standard"
        gcloud_cmd="$gcloud_cmd --metadata-from-file startup-script=/tmp/tardis-shard-startup.sh"
        gcloud_cmd="$gcloud_cmd --metadata shard-index=$i,start-date=$start_date,end-date=$end_date,total-days=$total_days,venues=$venues,data-types=$data_types"
        gcloud_cmd="$gcloud_cmd --scopes=https://www.googleapis.com/auth/cloud-platform"
        gcloud_cmd="$gcloud_cmd --tags=tardis-shard"
        
        if [ "$preemptible" = "true" ]; then
            gcloud_cmd="$gcloud_cmd --preemptible"
        fi
        
        # Create VM
        eval "$gcloud_cmd"
        
        # Upload credentials if they exist
        if [ -f "central-element-323112-e35fb0ddafe2.json" ]; then
            gcloud compute scp central-element-323112-e35fb0ddafe2.json "$vm_name:/tmp/credentials.json" --zone="$ZONE"
        fi
        
        echo -e "${GREEN}✅ Created $vm_name${NC}"
    done
    
    echo ""
    echo -e "${GREEN}🎉 Successfully deployed $num_shards Tardis VMs!${NC}"
    echo ""
    echo -e "${BLUE}📊 Summary:${NC}"
    echo "  - VM names: tardis-shard-$start_shard to tardis-shard-$end_shard"
    echo "  - Each VM processes one day of tick data per symbol per exchange"
    echo "  - Data is uploaded to GCS after processing each day"
    echo ""
    echo -e "${BLUE}🔍 Monitor with:${NC}"
    echo "  gcloud compute instances list --filter='name~tardis-shard'"
    echo "  gsutil ls gs://market-data-tick/daily/by_date/"
    
    # Clean up
    rm -f /tmp/tardis-shard-startup.sh
}

# Function to cleanup all sharded VMs
cleanup_vms() {
    echo -e "${YELLOW}🧹 Cleaning up all sharded VMs...${NC}"
    
    # List and delete instrument shard VMs
    local instrument_vms=$(gcloud compute instances list --filter="name~instrument-shard" --format="value(name)" --zones="$ZONE" 2>/dev/null || true)
    if [ -n "$instrument_vms" ]; then
        echo "Found instrument shard VMs:"
        echo "$instrument_vms"
        echo "$instrument_vms" | xargs -I {} gcloud compute instances delete {} --zone="$ZONE" --quiet
        echo -e "${GREEN}✅ Deleted instrument shard VMs${NC}"
    else
        echo "No instrument shard VMs found"
    fi
    
    # List and delete Tardis shard VMs
    local tardis_vms=$(gcloud compute instances list --filter="name~tardis-shard" --format="value(name)" --zones="$ZONE" 2>/dev/null || true)
    if [ -n "$tardis_vms" ]; then
        echo "Found Tardis shard VMs:"
        echo "$tardis_vms"
        echo "$tardis_vms" | xargs -I {} gcloud compute instances delete {} --zone="$ZONE" --quiet
        echo -e "${GREEN}✅ Deleted Tardis shard VMs${NC}"
    else
        echo "No Tardis shard VMs found"
    fi
    
    echo -e "${GREEN}🎉 Cleanup completed!${NC}"
}

# Main execution
case "${1:-help}" in
    instruments)
        shift
        # Parse arguments
        start_date=""
        end_date=""
        num_shards=10
        start_shard=0
        end_shard=9
        preemptible="false"
        
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
                --shards)
                    num_shards="$2"
                    end_shard=$((num_shards - 1))
                    shift 2
                    ;;
                --start-shard)
                    start_shard="$2"
                    shift 2
                    ;;
                --end-shard)
                    end_shard="$2"
                    shift 2
                    ;;
                --preemptible)
                    preemptible="true"
                    shift
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
        
        deploy_instrument_vms "$start_date" "$end_date" "$num_shards" "$start_shard" "$end_shard" "$preemptible"
        ;;
    tardis)
        shift
        # Parse arguments
        start_date=""
        end_date=""
        num_shards=10
        start_shard=0
        end_shard=9
        venues=""
        data_types=""
        preemptible="false"
        
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
                --shards)
                    num_shards="$2"
                    end_shard=$((num_shards - 1))
                    shift 2
                    ;;
                --start-shard)
                    start_shard="$2"
                    shift 2
                    ;;
                --end-shard)
                    end_shard="$2"
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
                --preemptible)
                    preemptible="true"
                    shift
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
        
        deploy_tardis_vms "$start_date" "$end_date" "$num_shards" "$start_shard" "$end_shard" "$venues" "$data_types" "$preemptible"
        ;;
    cleanup)
        cleanup_vms
        ;;
    help|--help|-h)
        show_usage
        ;;
    *)
        echo -e "${RED}❌ Unknown mode: $1${NC}"
        show_usage
        exit 1
        ;;
esac
