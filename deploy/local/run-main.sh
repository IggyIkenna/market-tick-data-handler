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
        echo "  available-tick-reports - Generate available data catalog (inverse of missing data)"
        echo "  download        - Download missing data (default) or force download with --force flag"
        echo "  validate        - Check for missing data and validate completeness"
        echo "  check-gaps      - Check for file existence gaps in date range (simple file checker)"
        echo "  full-pipeline   - Run complete pipeline (instruments + missing-reports + download + validate)"
        echo "  candle-processing - Process historical tick data into candles with HFT features"
        echo "  run-full-pipeline-candles - Full candle processing pipeline with optional BigQuery upload"
        echo "  bigquery-upload - Upload processed candles to BigQuery for analytics"
        echo "  streaming-ticks - Stream raw tick data to BigQuery (Node.js)"
        echo "  streaming-candles - Stream real-time candles with HFT features (Node.js)"
        echo "  streaming-trades - Stream trade data (1min batching)"
        echo "  streaming-liquidations - Stream liquidation data (15min batching)"
        echo "  streaming-book-snapshots - Stream order book snapshots (1min batching)"
        echo "  streaming-derivative-ticker - Stream derivative ticker data (15min batching)"
        echo "  streaming-options-chain - Stream options chain data (15min batching)"
    echo ""
    echo "Options:"
    echo "  --run-quality-gates  - Run quality gates before deployment (blocks deployment if tests fail)"
    echo ""
    echo "Examples:"
    echo "  $0 instruments --start-date 2023-05-23 --end-date 2023-05-25"
    echo "  $0 instruments --start-date 2023-05-23 --end-date 2023-05-25 --max-workers 8"
    echo "  $0 missing-reports --start-date 2023-05-23 --end-date 2023-05-25"
    echo "  $0 download --start-date 2023-05-23 --end-date 2023-05-25 --venues deribit"
    echo "  $0 download --start-date 2023-05-23 --end-date 2023-05-25 --venues binance --force"
    echo "  $0 validate --start-date 2023-05-23 --end-date 2023-05-25 --venues deribit --data-types trades book_snapshot_5"
    echo "  $0 check-gaps --start-date 2023-05-23 --end-date 2023-05-25"
    echo "  $0 full-pipeline --start-date 2023-05-23 --end-date 2023-05-25"
    echo "  $0 available-tick-reports --start-date 2023-05-23 --end-date 2023-05-25"
    echo "  $0 candle-processing --start-date 2024-01-01 --end-date 2024-01-01"
    echo "  $0 run-full-pipeline-candles --start-date 2024-01-01 --end-date 2024-01-01"
    echo "  $0 run-full-pipeline-candles --start-date 2024-01-01 --end-date 2024-01-01 --upload-to-bigquery"
    echo "  $0 bigquery-upload --start-date 2024-01-01 --end-date 2024-01-01"
  echo "  $0 streaming-ticks --symbol BTC-USDT --duration 300"
  echo "  $0 streaming-candles --symbol BTC-USDT --duration 0"
  echo "  $0 streaming-trades --symbol BTC-USDT --duration 60"
  echo "  $0 streaming-liquidations --symbol BTC-USDT --duration 300"
  echo "  $0 streaming-book-snapshots --symbol BTC-USDT --duration 60"
  echo "  $0 streaming-derivative-ticker --symbol BTC-USDT --duration 300"
  echo "  $0 streaming-options-chain --symbol BTC-USDT --duration 300"
    echo "  $0 instruments --start-date 2023-05-23 --end-date 2023-05-25 --run-quality-gates"
    echo ""
    echo "Performance Tips:"
    echo "  --max-workers 4    # Default: 4 parallel workers for instrument generation"
    echo "  --max-workers 8    # Use more workers for faster processing (if CPU allows)"
    echo ""
    echo "For more options, run: python -m market_data_tick_handler.main --help"
}

# Check if mode is provided
if [ $# -eq 0 ]; then
    show_usage
    exit 1
fi

MODE=$1
shift  # Remove mode from arguments

# Check for quality gates flag
RUN_QUALITY_GATES=false
if [[ " $* " =~ " --run-quality-gates " ]]; then
    RUN_QUALITY_GATES=true
    # Remove the flag from arguments
    set -- "${@/--run-quality-gates/}"
fi

# Validate mode
        case $MODE in
            instruments|missing-reports|available-tick-reports|download|validate|check-gaps|candle-processing|run-full-pipeline-candles|bigquery-upload|streaming-ticks|streaming-candles|streaming-trades|streaming-liquidations|streaming-book-snapshots|streaming-derivative-ticker|streaming-options-chain|full-pipeline)
                echo -e "${GREEN}✅ Mode: $MODE${NC}"
                ;;
    *)
        echo -e "${RED}❌ Invalid mode: $MODE${NC}"
        show_usage
        exit 1
        ;;
esac

# Handle streaming modes first (no Python dependencies needed)
case $MODE in
    streaming-ticks|streaming-candles|streaming-trades|streaming-liquidations|streaming-book-snapshots|streaming-derivative-ticker|streaming-options-chain)
        # These modes use Node.js, skip Python dependency checks
        ;;
    *)
        # Check if Python is available for other modes
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

        # Run quality gates if requested
        if [ "$RUN_QUALITY_GATES" = true ]; then
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
        fi
        ;;
esac

# Handle special modes that use different scripts
case $MODE in
    missing-reports)
        echo -e "${YELLOW}📊 Running missing data report generation...${NC}"
        python3 run_missing_data_report.py "$@"
        ;;
    available-tick-reports)
        echo -e "${YELLOW}📊 Running available data catalog generation...${NC}"
        python3 -m market_data_tick_handler.main --mode available-tick-reports "$@"
        ;;
    run-full-pipeline-candles)
        echo -e "${YELLOW}🚀 Running full candle processing pipeline...${NC}"
        python3 -m market_data_tick_handler.main --mode run-full-pipeline-candles "$@"
        ;;
    streaming-ticks)
        echo -e "${YELLOW}📡 Starting Node.js tick streaming to BigQuery...${NC}"
        if [ ! -d "./streaming" ]; then
            echo -e "${RED}❌ Node.js streaming directory not found${NC}"
            exit 1
        fi
        cd ./streaming
        if [ ! -f "package.json" ]; then
            echo -e "${YELLOW}⚠️  Installing Node.js dependencies...${NC}"
            npm install
        fi
        node live_tick_streamer.js --mode ticks --interval 1m "$@"
        ;;
    streaming-candles)
        echo -e "${YELLOW}🕯️ Starting Node.js candle streaming with HFT features...${NC}"
        if [ ! -d "./streaming" ]; then
            echo -e "${RED}❌ Node.js streaming directory not found${NC}"
            exit 1
        fi
        cd ./streaming
        if [ ! -f "package.json" ]; then
            echo -e "${YELLOW}⚠️  Installing Node.js dependencies...${NC}"
            npm install
        fi
        node live_tick_streamer.js --mode candles --interval 1m "$@"
        ;;
    streaming-trades)
        echo -e "${YELLOW}📈 Starting trade data streaming (1min batching)...${NC}"
        if [ ! -d "./streaming" ]; then
            echo -e "${RED}❌ Node.js streaming directory not found${NC}"
            exit 1
        fi
        cd ./streaming
        if [ ! -f "package.json" ]; then
            echo -e "${YELLOW}⚠️  Installing Node.js dependencies...${NC}"
            npm install
        fi
        node live_tick_streamer.js --mode ticks --data-type trades --interval 1m "$@"
        ;;
    streaming-liquidations)
        echo -e "${YELLOW}💥 Starting liquidation data streaming (15min batching)...${NC}"
        if [ ! -d "./streaming" ]; then
            echo -e "${RED}❌ Node.js streaming directory not found${NC}"
            exit 1
        fi
        cd ./streaming
        if [ ! -f "package.json" ]; then
            echo -e "${YELLOW}⚠️  Installing Node.js dependencies...${NC}"
            npm install
        fi
        node live_tick_streamer.js --mode ticks --data-type liquidations --interval 1m "$@"
        ;;
    streaming-book-snapshots)
        echo -e "${YELLOW}📊 Starting order book snapshots streaming (1min batching)...${NC}"
        if [ ! -d "./streaming" ]; then
            echo -e "${RED}❌ Node.js streaming directory not found${NC}"
            exit 1
        fi
        cd ./streaming
        if [ ! -f "package.json" ]; then
            echo -e "${YELLOW}⚠️  Installing Node.js dependencies...${NC}"
            npm install
        fi
        node live_tick_streamer.js --mode ticks --data-type book_snapshots --interval 1m "$@"
        ;;
    streaming-derivative-ticker)
        echo -e "${YELLOW}📈 Starting derivative ticker streaming (15min batching)...${NC}"
        if [ ! -d "./streaming" ]; then
            echo -e "${RED}❌ Node.js streaming directory not found${NC}"
            exit 1
        fi
        cd ./streaming
        if [ ! -f "package.json" ]; then
            echo -e "${YELLOW}⚠️  Installing Node.js dependencies...${NC}"
            npm install
        fi
        node live_tick_streamer.js --mode ticks --data-type derivative_ticker --interval 1m "$@"
        ;;
    streaming-options-chain)
        echo -e "${YELLOW}🔗 Starting options chain streaming (15min batching)...${NC}"
        if [ ! -d "./streaming" ]; then
            echo -e "${RED}❌ Node.js streaming directory not found${NC}"
            exit 1
        fi
        cd ./streaming
        if [ ! -f "package.json" ]; then
            echo -e "${YELLOW}⚠️  Installing Node.js dependencies...${NC}"
            npm install
        fi
        node live_tick_streamer.js --mode ticks --data-type options_chain --interval 1m "$@"
        ;;
esac

# Handle full-pipeline separately as it needs special logic
if [ "$MODE" = "full-pipeline" ]; then
        echo -e "${YELLOW}🔄 Running full pipeline: instruments + missing-reports + download + validate${NC}"
        echo ""
        
        # Step 1: Generate instruments
        echo -e "${BLUE}Step 1: Generating instrument definitions...${NC}"
        python3 -m market_data_tick_handler.main --mode instruments "$@"
        if [ $? -ne 0 ]; then
            echo -e "${RED}❌ Instrument generation failed${NC}"
            exit 1
        fi
        
        # Step 2: Generate missing data reports
        echo -e "${BLUE}Step 2: Generating missing data reports...${NC}"
        python3 -m market_data_tick_handler.main --mode missing-reports "$@"
        if [ $? -ne 0 ]; then
            echo -e "${RED}❌ Missing data report generation failed${NC}"
            exit 1
        fi
        
        # Step 3: Download missing data
        echo -e "${BLUE}Step 3: Downloading missing data...${NC}"
        python3 -m market_data_tick_handler.main --mode download "$@"
        if [ $? -ne 0 ]; then
            echo -e "${RED}❌ Download failed${NC}"
            exit 1
        fi
        
        # Step 4: Validate
        echo -e "${BLUE}Step 4: Validating data completeness...${NC}"
        python3 -m market_data_tick_handler.main --mode validate "$@"
        if [ $? -ne 0 ]; then
            echo -e "${RED}❌ Validation failed${NC}"
            exit 1
        fi
        
        echo -e "${GREEN}🎉 Full pipeline completed successfully!${NC}"
fi

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
