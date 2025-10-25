#!/bin/bash

# GCS Data Validation Script
# This script validates data completeness in GCS bucket for download operations

set -e

# Configuration
PROJECT_ID="central-element-323112"
BUCKET_NAME="market-data-tick"
REGION="asia-northeast1"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to show usage
show_usage() {
    echo -e "${BLUE}GCS Data Validation Script${NC}"
    echo "============================="
    echo ""
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --start-date DATE    Start date (YYYY-MM-DD)"
    echo "  --end-date DATE      End date (YYYY-MM-DD)"
    echo "  --venue VENUE        Venue to check (default: binance)"
    echo "  --data-types TYPES   Comma-separated list of data types (default: trades,book_snapshot_5)"
    echo "  --bucket BUCKET      GCS bucket name (default: market-data-tick)"
    echo "  --detailed           Show detailed file listing"
    echo "  --summary-only       Show only summary statistics"
    echo "  --help               Show this help"
    echo ""
    echo "Examples:"
    echo "  $0 --start-date 2023-05-23 --end-date 2023-05-23 --venue binance"
    echo "  $0 --start-date 2023-05-23 --end-date 2023-05-26 --venue binance --detailed"
    echo "  $0 --start-date 2023-05-23 --end-date 2023-05-26 --summary-only"
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

# Function to check if gsutil is available
check_gsutil() {
    if ! command -v gsutil > /dev/null 2>&1; then
        echo -e "${RED}❌ gsutil not found. Please install Google Cloud SDK.${NC}"
        exit 1
    fi
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

# Function to get all dates in range
get_date_range() {
    local start_date="$1"
    local end_date="$2"
    
    local dates=()
    local total_days=$(calculate_date_range "$start_date" "$end_date")
    
    # Generate dates using epoch arithmetic
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        local start_epoch=$(date -j -f "%Y-%m-%d" "$start_date" +%s)
        for ((i=0; i<total_days; i++)); do
            local current_epoch=$((start_epoch + i * 86400))
            local current_date=$(date -r "$current_epoch" +%Y-%m-%d)
            dates+=("$current_date")
        done
    else
        # Linux
        for ((i=0; i<total_days; i++)); do
            local current_date=$(date -d "$start_date + $i days" +%Y-%m-%d)
            dates+=("$current_date")
        done
    fi
    
    printf '%s\n' "${dates[@]}"
}

# Function to check data for a specific date
check_date_data() {
    local date="$1"
    local venue="$2"
    local data_types="$3"
    local detailed="$4"
    
    local date_clean=$(echo "$date" | sed 's/-//g')
    local base_path="gs://$BUCKET_NAME/raw_tick_data/by_date/day-$date"
    
    echo -e "${BLUE}📅 Checking data for $date${NC}"
    echo "============================================="
    
    local total_files=0
    local total_size=0
    local missing_data_types=()
    local found_data_types=()
    
    # Check each data type
    IFS=',' read -ra DATA_TYPES_ARRAY <<< "$data_types"
    for data_type in "${DATA_TYPES_ARRAY[@]}"; do
        data_type=$(echo "$data_type" | xargs) # trim whitespace
        local data_path="$base_path/data_type-$data_type"
        
        echo -e "\n${YELLOW}Checking $data_type data...${NC}"
        
        # List files in the data type directory
        local files=$(gsutil ls "$data_path/**" 2>/dev/null || true)
        
        if [ -n "$files" ]; then
            local file_count=$(echo "$files" | wc -l)
            local size_info=$(gsutil du -s "$data_path" 2>/dev/null || echo "0")
            local size=$(echo "$size_info" | cut -d' ' -f1)
            
            total_files=$((total_files + file_count))
            total_size=$((total_size + size))
            found_data_types+=("$data_type")
            
            echo -e "  ✅ Found $file_count files ($(echo $size | awk '{printf "%.1f %s", $1/1024/1024, "MB"}'))"
            
            if [ "$detailed" = "true" ]; then
                echo "$files" | head -10 | while read -r file; do
                    if [ -n "$file" ]; then
                        local file_size=$(gsutil du "$file" 2>/dev/null | cut -d' ' -f1)
                        local file_name=$(basename "$file")
                        echo -e "    - $file_name ($(echo $file_size | awk '{printf "%.1f %s", $1/1024/1024, "MB"}'))"
                    fi
                done
                if [ $file_count -gt 10 ]; then
                    echo -e "    ... and $((file_count - 10)) more files"
                fi
            fi
        else
            missing_data_types+=("$data_type")
            echo -e "  ❌ No files found for $data_type"
        fi
    done
    
    # Summary for this date
    echo -e "\n${BLUE}Summary for $date:${NC}"
    echo "  Total files: $total_files"
    echo "  Total size: $(echo $total_size | awk '{printf "%.1f %s", $1/1024/1024, "MB"}')"
    echo "  Found data types: ${found_data_types[*]}"
    
    if [ ${#missing_data_types[@]} -gt 0 ]; then
        echo -e "  ${RED}Missing data types: ${missing_data_types[*]}${NC}"
        return 1
    else
        echo -e "  ${GREEN}✅ All data types found${NC}"
        return 0
    fi
}

# Function to check instrument definitions
check_instrument_definitions() {
    local start_date="$1"
    local end_date="$2"
    local detailed="$3"
    
    echo -e "${BLUE}🔧 Checking instrument definitions${NC}"
    echo "============================================="
    
    local total_instrument_files=0
    local total_instrument_size=0
    local missing_dates=()
    
    # Get all dates in range
    local dates=($(get_date_range "$start_date" "$end_date"))
    
    for date in "${dates[@]}"; do
        local instrument_path="gs://$BUCKET_NAME/instrument_availability/by_date/day-$date/instruments.parquet"
        
        # Check if instrument file exists
        if gsutil ls "$instrument_path" >/dev/null 2>&1; then
            local size=$(gsutil du "$instrument_path" 2>/dev/null | cut -d' ' -f1)
            total_instrument_files=$((total_instrument_files + 1))
            total_instrument_size=$((total_instrument_size + size))
            
            if [ "$detailed" = "true" ]; then
                echo -e "  ✅ $date: $(echo $size | awk '{printf "%.1f %s", $1/1024/1024, "MB"}')"
            fi
        else
            missing_dates+=("$date")
            echo -e "  ❌ $date: Missing instrument definitions"
        fi
    done
    
    echo -e "\n${BLUE}Instrument definitions summary:${NC}"
    echo "  Total files: $total_instrument_files"
    echo "  Total size: $(echo $total_instrument_size | awk '{printf "%.1f %s", $1/1024/1024, "MB"}')"
    
    if [ ${#missing_dates[@]} -gt 0 ]; then
        echo -e "  ${RED}Missing dates: ${missing_dates[*]}${NC}"
        return 1
    else
        echo -e "  ${GREEN}✅ All instrument definitions found${NC}"
        return 0
    fi
}

# Function to show overall summary
show_summary() {
    local start_date="$1"
    local end_date="$2"
    local venue="$3"
    local data_types="$4"
    local total_days="$5"
    local successful_days="$6"
    local failed_days="$7"
    
    echo -e "\n${BLUE}📊 Overall Summary${NC}"
    echo "=================="
    echo "Date Range: $start_date to $end_date ($total_days days)"
    echo "Venue: $venue"
    echo "Data Types: $data_types"
    echo "Successful days: $successful_days"
    echo "Failed days: $failed_days"
    
    local success_rate=$((successful_days * 100 / total_days))
    echo "Success rate: $success_rate%"
    
    if [ $success_rate -eq 100 ]; then
        echo -e "${GREEN}🎉 All data is complete!${NC}"
    elif [ $success_rate -ge 80 ]; then
        echo -e "${YELLOW}⚠️  Most data is complete, but some days are missing${NC}"
    else
        echo -e "${RED}❌ Significant data is missing${NC}"
    fi
}

# Main execution
main() {
    # Parse arguments
    local start_date=""
    local end_date=""
    local venue="binance"
    local data_types="trades,book_snapshot_5"
    local detailed="false"
    local summary_only="false"
    
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
            --venue)
                venue="$2"
                shift 2
                ;;
            --data-types)
                data_types="$2"
                shift 2
                ;;
            --bucket)
                BUCKET_NAME="$2"
                shift 2
                ;;
            --detailed)
                detailed="true"
                shift
                ;;
            --summary-only)
                summary_only="true"
                shift
                ;;
            --help)
                show_usage
                exit 0
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
    
    # Check prerequisites
    check_gcloud
    check_gsutil
    
    # Set project
    gcloud config set project "$PROJECT_ID"
    
    # Calculate total days
    local total_days=$(calculate_date_range "$start_date" "$end_date")
    local successful_days=0
    local failed_days=0
    
    echo -e "${BLUE}🔍 GCS Data Validation${NC}"
    echo "======================="
    echo "Bucket: $BUCKET_NAME"
    echo "Date Range: $start_date to $end_date ($total_days days)"
    echo "Venue: $venue"
    echo "Data Types: $data_types"
    echo ""
    
    # Check instrument definitions first
    if [ "$summary_only" = "false" ]; then
        if check_instrument_definitions "$start_date" "$end_date" "$detailed"; then
            echo -e "${GREEN}✅ Instrument definitions are complete${NC}"
        else
            echo -e "${YELLOW}⚠️  Some instrument definitions are missing${NC}"
        fi
        echo ""
    fi
    
    # Check data for each date
    local dates=($(get_date_range "$start_date" "$end_date"))
    
    for date in "${dates[@]}"; do
        if check_date_data "$date" "$venue" "$data_types" "$detailed"; then
            successful_days=$((successful_days + 1))
        else
            failed_days=$((failed_days + 1))
        fi
        echo ""
    done
    
    # Show overall summary
    show_summary "$start_date" "$end_date" "$venue" "$data_types" "$total_days" "$successful_days" "$failed_days"
    
    # Exit with appropriate code
    if [ $failed_days -eq 0 ]; then
        exit 0
    else
        exit 1
    fi
}

# Run main function
main "$@"
