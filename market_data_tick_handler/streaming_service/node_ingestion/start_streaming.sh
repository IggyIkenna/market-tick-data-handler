#!/bin/bash
"""
Start Live Streaming Service

Starts both Node.js ingestion layer and Python processing layer
for real-time market data streaming from Tardis.dev
"""

set -e

# Configuration
SYMBOL=${SYMBOL:-"BTC-USDT"}
EXCHANGE=${EXCHANGE:-"binance"}
MODE=${MODE:-"candles"}
DATA_TYPES=${DATA_TYPES:-"trades,book_snapshot_5,derivative_ticker,liquidations"}
TIMEFRAMES=${TIMEFRAMES:-"15s,1m,5m,15m"}
PYTHON_HOST=${PYTHON_HOST:-"localhost"}
PYTHON_PORT=${PYTHON_PORT:-"8765"}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Starting Live Streaming Service${NC}"
echo -e "${BLUE}================================${NC}"
echo -e "Symbol: ${GREEN}${SYMBOL}${NC}"
echo -e "Exchange: ${GREEN}${EXCHANGE}${NC}"
echo -e "Mode: ${GREEN}${MODE}${NC}"
echo -e "Data Types: ${GREEN}${DATA_TYPES}${NC}"
echo -e "Timeframes: ${GREEN}${TIMEFRAMES}${NC}"
echo -e "Python Server: ${GREEN}${PYTHON_HOST}:${PYTHON_PORT}${NC}"
echo ""

# Check if Node.js is installed
if ! command -v node &> /dev/null; then
    echo -e "${RED}❌ Node.js is not installed${NC}"
    echo "Please install Node.js 16+ from https://nodejs.org/"
    exit 1
fi

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}❌ Python 3 is not installed${NC}"
    echo "Please install Python 3.8+ from https://python.org/"
    exit 1
fi

# Check if package.json exists
if [ ! -f "package.json" ]; then
    echo -e "${YELLOW}📦 Installing Node.js dependencies...${NC}"
    npm install
fi

# Check if node_modules exists
if [ ! -d "node_modules" ]; then
    echo -e "${YELLOW}📦 Installing Node.js dependencies...${NC}"
    npm install
fi

# Check if tardis-dev is installed
if ! npm list tardis-dev &> /dev/null; then
    echo -e "${YELLOW}📦 Installing tardis-dev...${NC}"
    npm install tardis-dev
fi

# Check if Python dependencies are available
echo -e "${YELLOW}🔍 Checking Python dependencies...${NC}"
python3 -c "
import sys
try:
    import websockets
    import asyncio
    print('✅ Python dependencies available')
except ImportError as e:
    print(f'❌ Missing Python dependency: {e}')
    print('Please install: pip install websockets')
    sys.exit(1)
"

# Create logs directory
mkdir -p logs

# Function to cleanup background processes
cleanup() {
    echo -e "\n${YELLOW}🛑 Shutting down streaming service...${NC}"
    if [ ! -z "$PYTHON_PID" ]; then
        kill $PYTHON_PID 2>/dev/null || true
    fi
    if [ ! -z "$NODE_PID" ]; then
        kill $NODE_PID 2>/dev/null || true
    fi
    exit 0
}

# Set up signal handlers
trap cleanup SIGINT SIGTERM

# Start Python WebSocket server in background
echo -e "${BLUE}🐍 Starting Python WebSocket server...${NC}"
python3 python_websocket_server.py \
    --host $PYTHON_HOST \
    --port $PYTHON_PORT \
    --symbol $SYMBOL \
    --exchange $EXCHANGE \
    --timeframes $TIMEFRAMES \
    > logs/python_server.log 2>&1 &
PYTHON_PID=$!

# Wait for Python server to start
echo -e "${YELLOW}⏳ Waiting for Python server to start...${NC}"
sleep 3

# Check if Python server is running
if ! kill -0 $PYTHON_PID 2>/dev/null; then
    echo -e "${RED}❌ Python server failed to start${NC}"
    echo "Check logs/python_server.log for details"
    exit 1
fi

echo -e "${GREEN}✅ Python server started (PID: $PYTHON_PID)${NC}"

# Start Node.js streamer
echo -e "${BLUE}📡 Starting Node.js Tardis streamer...${NC}"
node live_tick_streamer.js \
    --symbol $SYMBOL \
    --exchange $EXCHANGE \
    --mode $MODE \
    --dataTypes $DATA_TYPES \
    --timeframes $TIMEFRAMES \
    --pythonWebSocketUrl ws://$PYTHON_HOST:$PYTHON_PORT \
    > logs/node_streamer.log 2>&1 &
NODE_PID=$!

# Wait for Node.js to start
sleep 2

# Check if Node.js is running
if ! kill -0 $NODE_PID 2>/dev/null; then
    echo -e "${RED}❌ Node.js streamer failed to start${NC}"
    echo "Check logs/node_streamer.log for details"
    cleanup
    exit 1
fi

echo -e "${GREEN}✅ Node.js streamer started (PID: $NODE_PID)${NC}"
echo ""
echo -e "${GREEN}🎉 Live streaming service is running!${NC}"
echo -e "${BLUE}================================${NC}"
echo -e "Python Server: ${GREEN}ws://$PYTHON_HOST:$PYTHON_PORT${NC}"
echo -e "Logs: ${GREEN}logs/python_server.log${NC} and ${GREEN}logs/node_streamer.log${NC}"
echo -e "Press ${YELLOW}Ctrl+C${NC} to stop"
echo ""

# Monitor processes
while true; do
    if ! kill -0 $PYTHON_PID 2>/dev/null; then
        echo -e "${RED}❌ Python server died${NC}"
        cleanup
        exit 1
    fi
    
    if ! kill -0 $NODE_PID 2>/dev/null; then
        echo -e "${RED}❌ Node.js streamer died${NC}"
        cleanup
        exit 1
    fi
    
    sleep 5
done
