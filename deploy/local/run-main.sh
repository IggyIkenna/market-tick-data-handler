#!/bin/bash

# Local script for running the new main.py entry point
# This script demonstrates how to use the centralized main.py with different modes

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Market Data Tick Handler - Main Entry Point${NC}"
echo "============================================="

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo -e "${YELLOW}⚠️  .env file not found. Creating from template...${NC}"
    if [ -f "env.example" ]; then
        cp env.example .env
        echo -e "${GREEN}✅ Created .env file from env.example${NC}"
        echo -e "${YELLOW}💡 Please review and update .env file with your actual values${NC}"
    else
        echo -e "${RED}❌ No env.example file found. Please create .env file manually.${NC}"
        exit 1
    fi
fi

# Create necessary directories
echo -e "${YELLOW}📁 Creating necessary directories...${NC}"
mkdir -p data logs temp downloads

# Function to show usage
show_usage() {
    echo -e "${BLUE}Usage: $0 [MODE] [OPTIONS]${NC}"
    echo ""
        echo "Modes:"
        echo "  instruments     - Generate instrument definitions and upload to GCS"
        echo "  missing-reports - Generate missing data reports for date range and upload to GCS"
        echo "  download        - Download only missing data (requires instrument definitions and missing data reports in GCS)"
        echo "  validate        - Check for missing data and validate completeness"
        echo "  check-gaps      - Check for file existence gaps in date range (simple file checker)"
        echo "  full-pipeline   - Run complete pipeline (instruments + missing-reports + download + validate)"
    echo ""
    echo "Examples:"
    echo "  $0 instruments --start-date 2023-05-23 --end-date 2023-05-25"
    echo "  $0 instruments --start-date 2023-05-23 --end-date 2023-05-25 --max-workers 8"
    echo "  $0 missing-reports --start-date 2023-05-23 --end-date 2023-05-25"
    echo "  $0 download --start-date 2023-05-23 --end-date 2023-05-25 --venues deribit"
    echo "  $0 validate --start-date 2023-05-23 --end-date 2023-05-25 --venues deribit --data-types trades book_snapshot_5"
    echo "  $0 check-gaps --start-date 2023-05-23 --end-date 2023-05-25"
    echo "  $0 full-pipeline --start-date 2023-05-23 --end-date 2023-05-25"
    echo ""
    echo "Performance Tips:"
    echo "  --max-workers 4    # Default: 4 parallel workers for instrument generation"
    echo "  --max-workers 8    # Use more workers for faster processing (if CPU allows)"
    echo ""
    echo "For more options, run: python -m src.main --help"
}

# Check if mode is provided
if [ $# -eq 0 ]; then
    show_usage
    exit 1
fi

MODE=$1
shift  # Remove mode from arguments

# Validate mode
        case $MODE in
            instruments|missing-reports|download|validate|check-gaps|full-pipeline)
                echo -e "${GREEN}✅ Mode: $MODE${NC}"
                ;;
    *)
        echo -e "${RED}❌ Invalid mode: $MODE${NC}"
        show_usage
        exit 1
        ;;
esac

# Check if Python is available
if ! command -v python3 > /dev/null 2>&1; then
    echo -e "${RED}❌ Python 3 is not installed or not in PATH${NC}"
    exit 1
fi

# Check if required Python packages are installed
echo -e "${YELLOW}🔍 Checking Python dependencies...${NC}"
if ! python3 -c "import pandas, google.cloud, requests" > /dev/null 2>&1; then
    echo -e "${YELLOW}⚠️  Some required Python packages are missing. Installing...${NC}"
    pip3 install -r requirements.txt
fi

# Run the appropriate script based on mode
echo -e "${YELLOW}🏃 Running: python -m src.main --mode $MODE $@${NC}"
echo ""

# Handle special modes that use different scripts
case $MODE in
    missing-reports)
        echo -e "${YELLOW}📊 Running missing data report generation...${NC}"
        python3 run_missing_data_report.py "$@"
        ;;
    full-pipeline)
        echo -e "${YELLOW}🔄 Running full pipeline: instruments + missing-reports + download + validate${NC}"
        echo ""
        
        # Step 1: Generate instruments
        echo -e "${BLUE}Step 1: Generating instrument definitions...${NC}"
        python3 -m src.main --mode instruments "$@"
        if [ $? -ne 0 ]; then
            echo -e "${RED}❌ Instrument generation failed${NC}"
            exit 1
        fi
        
        # Step 2: Generate missing data reports
        echo -e "${BLUE}Step 2: Generating missing data reports...${NC}"
        python3 run_missing_data_report.py "$@"
        if [ $? -ne 0 ]; then
            echo -e "${RED}❌ Missing data report generation failed${NC}"
            exit 1
        fi
        
        # Step 3: Download missing data
        echo -e "${BLUE}Step 3: Downloading missing data...${NC}"
        python3 -m src.main --mode download "$@"
        if [ $? -ne 0 ]; then
            echo -e "${RED}❌ Download failed${NC}"
            exit 1
        fi
        
        # Step 4: Validate
        echo -e "${BLUE}Step 4: Validating data completeness...${NC}"
        python3 -m src.main --mode validate "$@"
        if [ $? -ne 0 ]; then
            echo -e "${RED}❌ Validation failed${NC}"
            exit 1
        fi
        
        echo -e "${GREEN}🎉 Full pipeline completed successfully!${NC}"
        ;;
    *)
        # Default: use src.main for all other modes
        python3 -m src.main --mode "$MODE" "$@"
        ;;
esac

# Check exit status
if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}✅ Operation completed successfully!${NC}"
    echo ""
    echo -e "${BLUE}📋 Output files:${NC}"
    echo "  - Data: ./data/"
    echo "  - Logs: ./logs/"
    echo "  - Temp: ./temp/"
    if [ "$MODE" = "download" ] || [ "$MODE" = "full-pipeline" ]; then
        echo "  - Downloads: ./downloads/"
    fi
    if [ "$MODE" = "validate" ] || [ "$MODE" = "full-pipeline" ]; then
        echo "  - Validation reports: Check console output for missing data details"
    fi
    echo ""
    echo -e "${YELLOW}💡 Tip: Check the logs directory for detailed execution logs${NC}"
else
    echo -e "${RED}❌ Operation failed!${NC}"
    echo -e "${YELLOW}💡 Check the logs directory for error details${NC}"
    exit 1
fi
