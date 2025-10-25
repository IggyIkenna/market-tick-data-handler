#!/bin/bash
# Entrypoint script for Market Data Streaming Service

set -e

echo "🚀 Starting Market Data Streaming Service"
echo "=========================================="

# Check environment variables
if [ -z "$TARDIS_API_KEY" ]; then
    echo "❌ TARDIS_API_KEY environment variable is required"
    exit 1
fi

echo "✅ Tardis API Key: ${TARDIS_API_KEY:0:10}..."

# Set defaults
STREAMING_MODE=${STREAMING_MODE:-"candles"}
STREAMING_SYMBOL=${STREAMING_SYMBOL:-"BTC-USDT"}
STREAMING_EXCHANGE=${STREAMING_EXCHANGE:-"binance"}
STREAMING_DURATION=${STREAMING_DURATION:-"3600"}

echo "📊 Configuration:"
echo "   Mode: $STREAMING_MODE"
echo "   Symbol: $STREAMING_SYMBOL"
echo "   Exchange: $STREAMING_EXCHANGE"
echo "   Duration: ${STREAMING_DURATION}s"

# Check if we should use Node.js real-time streaming
if [ "$USE_NODEJS_STREAMING" = "true" ]; then
    echo "🌟 Using Node.js real-time streaming"
    cd /app/nodejs
    exec node live_tick_streamer.js \
        --symbol="$STREAMING_SYMBOL" \
        --duration="$STREAMING_DURATION"
else
    echo "🐍 Using Python streaming service"
    
    # Set BigQuery configuration if in ticks mode
    if [ "$STREAMING_MODE" = "ticks" ]; then
        export BIGQUERY_DATASET=${BIGQUERY_DATASET:-"market_data_streaming"}
        export BIGQUERY_TABLE=${BIGQUERY_TABLE:-"ticks_${STREAMING_EXCHANGE}_$(echo $STREAMING_SYMBOL | tr '[:upper:]' '[:lower:]' | tr '-' '_')"}
        echo "   BigQuery Dataset: $BIGQUERY_DATASET"
        echo "   BigQuery Table: $BIGQUERY_TABLE"
    fi
    
    # Execute the command
    exec "$@"
fi
