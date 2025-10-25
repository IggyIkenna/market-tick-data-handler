#!/bin/bash

# Log Pulling Script for VM Instances
# This script pulls and displays logs from Cloud Logging for specific VM instances

set -e

# Configuration
PROJECT_ID="central-element-323112"
ZONE="asia-northeast1-c"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to show usage
show_usage() {
    echo -e "${BLUE}VM Log Pulling Script${NC}"
    echo "======================="
    echo ""
    echo "Usage: $0 [options] [VM_NAME]"
    echo ""
    echo "Options:"
    echo "  --pattern PATTERN    Filter VMs by name pattern (e.g., 'market-data-shard-*')"
    echo "  --follow, -f         Follow logs in real-time"
    echo "  --lines N            Number of lines to show (default: 50)"
    echo "  --level LEVEL        Filter by log level (DEBUG, INFO, WARNING, ERROR)"
    echo "  --since TIME         Show logs since time (e.g., '1h', '30m', '2023-05-23T10:00:00Z')"
    echo "  --until TIME         Show logs until time (e.g., '1h', '30m', '2023-05-23T10:00:00Z')"
    echo "  --grep PATTERN       Grep pattern to filter log content"
    echo "  --json               Output logs in JSON format"
    echo "  --help               Show this help"
    echo ""
    echo "Examples:"
    echo "  $0 download-vm-binance-20230523"
    echo "  $0 --pattern 'market-data-shard-*' --follow"
    echo "  $0 --pattern 'download-shard-binance-*' --lines 100 --level ERROR"
    echo "  $0 --pattern 'download-*' --since '1h' --grep 'ERROR'"
}

# Function to check if gcloud is available
check_gcloud() {
    if ! command -v gcloud > /dev/null 2>&1; then
        echo -e "${RED}❌ gcloud CLI not found. Please install Google Cloud SDK.${NC}"
        exit 1
    fi

    if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
        echo -e "${RED}❌ Not authenticated with gcloud. Please run 'gcloud auth login'.${NC}"
        exit 1
    fi
}

# Function to get VM instances by pattern
get_vm_instances() {
    local pattern="$1"
    
    if [ -n "$pattern" ]; then
        gcloud compute instances list --filter="name~$pattern" --format="value(name)" --zones="$ZONE" 2>/dev/null || true
    else
        echo ""
    fi
}

# Function to build gcloud logging query
build_logging_query() {
    local vm_name="$1"
    local level="$2"
    local since="$3"
    local until="$4"
    local grep_pattern="$5"
    
    local query="resource.type=\"gce_instance\" AND resource.labels.instance_name=\"$vm_name\""
    
    if [ -n "$level" ]; then
        query="$query AND severity>=\"$level\""
    fi
    
    if [ -n "$since" ]; then
        query="$query AND timestamp>=\"$since\""
    fi
    
    if [ -n "$until" ]; then
        query="$query AND timestamp<=\"$until\""
    fi
    
    if [ -n "$grep_pattern" ]; then
        query="$query AND textPayload=~\"$grep_pattern\""
    fi
    
    echo "$query"
}

# Function to pull logs for a single VM
pull_vm_logs() {
    local vm_name="$1"
    local follow="$2"
    local lines="$3"
    local level="$4"
    local since="$5"
    local until="$6"
    local grep_pattern="$7"
    local json_output="$8"
    
    echo -e "${BLUE}📋 Pulling logs for $vm_name${NC}"
    echo "============================================="
    
    # Check if VM exists
    if ! gcloud compute instances describe "$vm_name" --zone="$ZONE" --project="$PROJECT_ID" >/dev/null 2>&1; then
        echo -e "${RED}❌ VM $vm_name not found${NC}"
        return 1
    fi
    
    # Build query
    local query=$(build_logging_query "$vm_name" "$level" "$since" "$until" "$grep_pattern")
    
    # Build gcloud logging command
    local cmd="gcloud logging read \"$query\" --project=$PROJECT_ID --limit=$lines --format=\"value(timestamp,severity,textPayload)\""
    
    if [ "$json_output" = "true" ]; then
        cmd="gcloud logging read \"$query\" --project=$PROJECT_ID --limit=$lines --format=json"
    fi
    
    if [ "$follow" = "true" ]; then
        cmd="$cmd --freshness=1m"
        echo -e "${YELLOW}Following logs for $vm_name (press Ctrl+C to stop)...${NC}"
        echo ""
        
        # Follow logs with a loop
        while true; do
            eval "$cmd" | while IFS=$'\t' read -r timestamp severity message; do
                if [ -n "$timestamp" ] && [ -n "$severity" ] && [ -n "$message" ]; then
                    local color=""
                    case "$severity" in
                        "ERROR") color="${RED}" ;;
                        "WARNING") color="${YELLOW}" ;;
                        "INFO") color="${GREEN}" ;;
                        "DEBUG") color="${BLUE}" ;;
                        *) color="${NC}" ;;
                    esac
                    echo -e "[$timestamp] ${color}[$severity]${NC} $message"
                fi
            done
            sleep 5
        done
    else
        echo -e "${YELLOW}Recent logs for $vm_name:${NC}"
        echo ""
        
        if [ "$json_output" = "true" ]; then
            eval "$cmd"
        else
            eval "$cmd" | while IFS=$'\t' read -r timestamp severity message; do
                if [ -n "$timestamp" ] && [ -n "$severity" ] && [ -n "$message" ]; then
                    local color=""
                    case "$severity" in
                        "ERROR") color="${RED}" ;;
                        "WARNING") color="${YELLOW}" ;;
                        "INFO") color="${GREEN}" ;;
                        "DEBUG") color="${BLUE}" ;;
                        *) color="${NC}" ;;
                    esac
                    echo -e "[$timestamp] ${color}[$severity]${NC} $message"
                fi
            done
        fi
    fi
}

# Function to pull logs for multiple VMs
pull_multiple_vm_logs() {
    local pattern="$1"
    local follow="$2"
    local lines="$3"
    local level="$4"
    local since="$5"
    local until="$6"
    local grep_pattern="$7"
    local json_output="$8"
    
    local vms=$(get_vm_instances "$pattern")
    
    if [ -z "$vms" ]; then
        echo -e "${YELLOW}No VMs found matching pattern: $pattern${NC}"
        return 1
    fi
    
    echo -e "${BLUE}📋 Found VMs matching pattern: $pattern${NC}"
    echo "$vms"
    echo ""
    
    if [ "$follow" = "true" ]; then
        echo -e "${YELLOW}Following logs from all VMs (press Ctrl+C to stop)...${NC}"
        echo ""
        
        # Follow logs from all VMs
        while true; do
            echo "$vms" | while read -r vm_name; do
                if [ -n "$vm_name" ]; then
                    local query=$(build_logging_query "$vm_name" "$level" "$since" "$until" "$grep_pattern")
                    local cmd="gcloud logging read \"$query\" --project=$PROJECT_ID --limit=10 --format=\"value(timestamp,severity,textPayload)\" --freshness=1m"
                    
                    eval "$cmd" 2>/dev/null | while IFS=$'\t' read -r timestamp severity message; do
                        if [ -n "$timestamp" ] && [ -n "$severity" ] && [ -n "$message" ]; then
                            local color=""
                            case "$severity" in
                                "ERROR") color="${RED}" ;;
                                "WARNING") color="${YELLOW}" ;;
                                "INFO") color="${GREEN}" ;;
                                "DEBUG") color="${BLUE}" ;;
                                *) color="${NC}" ;;
                            esac
                            echo -e "[$vm_name] [$timestamp] ${color}[$severity]${NC} $message"
                        fi
                    done
                fi
            done
            sleep 5
        done
    else
        # Show recent logs from all VMs
        echo "$vms" | while read -r vm_name; do
            if [ -n "$vm_name" ]; then
                pull_vm_logs "$vm_name" "false" "$lines" "$level" "$since" "$until" "$grep_pattern" "$json_output"
                echo ""
            fi
        done
    fi
}

# Main execution
main() {
    # Parse arguments
    local vm_name=""
    local pattern=""
    local follow="false"
    local lines=50
    local level=""
    local since=""
    local until=""
    local grep_pattern=""
    local json_output="false"
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --pattern)
                pattern="$2"
                shift 2
                ;;
            --follow|-f)
                follow="true"
                shift
                ;;
            --lines)
                lines="$2"
                shift 2
                ;;
            --level)
                level="$2"
                shift 2
                ;;
            --since)
                since="$2"
                shift 2
                ;;
            --until)
                until="$2"
                shift 2
                ;;
            --grep)
                grep_pattern="$2"
                shift 2
                ;;
            --json)
                json_output="true"
                shift
                ;;
            --help)
                show_usage
                exit 0
                ;;
            -*)
                echo "Unknown option: $1"
                show_usage
                exit 1
                ;;
            *)
                if [ -z "$vm_name" ]; then
                    vm_name="$1"
                else
                    echo "Multiple VM names provided. Use --pattern for multiple VMs."
                    show_usage
                    exit 1
                fi
                shift
                ;;
        esac
    done
    
    # Check prerequisites
    check_gcloud
    
    # Set project
    gcloud config set project "$PROJECT_ID"
    
    # Determine if we're pulling from single VM or multiple VMs
    if [ -n "$vm_name" ]; then
        # Single VM
        pull_vm_logs "$vm_name" "$follow" "$lines" "$level" "$since" "$until" "$grep_pattern" "$json_output"
    elif [ -n "$pattern" ]; then
        # Multiple VMs by pattern
        pull_multiple_vm_logs "$pattern" "$follow" "$lines" "$level" "$since" "$until" "$grep_pattern" "$json_output"
    else
        echo -e "${RED}❌ Either VM name or --pattern must be provided${NC}"
        show_usage
        exit 1
    fi
}

# Run main function
main "$@"
