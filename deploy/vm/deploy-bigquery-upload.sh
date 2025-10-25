#!/bin/bash

# Deploy BigQuery Upload Service
# Uploads processed candle data to BigQuery for analytics

set -e

# Configuration
SERVICE_NAME="bigquery-upload"
SERVICE_USER="marketdata"
SERVICE_DIR="/opt/market-data-handler"
LOG_DIR="/var/log/market-data-handler"
ENV_FILE="/etc/market-data-handler/.env"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 Deploying BigQuery Upload Service${NC}"
echo "============================================="

# Check if running as root
if [[ $EUID -eq 0 ]]; then
   echo -e "${RED}❌ This script should not be run as root${NC}"
   exit 1
fi

# Check if service user exists
if ! id "$SERVICE_USER" &>/dev/null; then
    echo -e "${YELLOW}⚠️  Creating service user: $SERVICE_USER${NC}"
    sudo useradd -r -s /bin/false -d "$SERVICE_DIR" "$SERVICE_USER"
fi

# Create directories
echo -e "${YELLOW}📁 Creating directories${NC}"
sudo mkdir -p "$SERVICE_DIR"
sudo mkdir -p "$LOG_DIR"
sudo mkdir -p "$(dirname "$ENV_FILE")"

# Set ownership
sudo chown -R "$SERVICE_USER:$SERVICE_USER" "$SERVICE_DIR"
sudo chown -R "$SERVICE_USER:$SERVICE_USER" "$LOG_DIR"

# Copy application files
echo -e "${YELLOW}📦 Copying application files${NC}"
sudo cp -r . "$SERVICE_DIR/"
sudo chown -R "$SERVICE_USER:$SERVICE_USER" "$SERVICE_DIR"

# Create environment file if it doesn't exist
if [[ ! -f "$ENV_FILE" ]]; then
    echo -e "${YELLOW}⚙️  Creating environment file${NC}"
    sudo tee "$ENV_FILE" > /dev/null << EOF
# Market Data Handler Configuration
TARDIS_API_KEY=your_api_key_here
GCP_PROJECT_ID=your_project_id
GCP_BUCKET=your_bucket_name
GCP_CREDENTIALS_PATH=/opt/market-data-handler/credentials.json

# BigQuery Upload Configuration
BIGQUERY_DATASET=market_data_candles
BIGQUERY_BATCH_SIZE=1000
MAX_CONCURRENT_DAYS=5
MAX_CONCURRENT_TIMEFRAMES=3
RETRY_FAILED_DAYS=true
EOF
    sudo chown "$SERVICE_USER:$SERVICE_USER" "$ENV_FILE"
    sudo chmod 600 "$ENV_FILE"
fi

# Install Python dependencies
echo -e "${YELLOW}🐍 Installing Python dependencies${NC}"
cd "$SERVICE_DIR"
sudo -u "$SERVICE_USER" python3 -m pip install --user -r requirements.txt

# Create systemd service file
echo -e "${YELLOW}⚙️  Creating systemd service${NC}"
sudo tee /etc/systemd/system/bigquery-upload.service > /dev/null << EOF
[Unit]
Description=Market Data BigQuery Upload Service
After=network.target

[Service]
Type=oneshot
User=$SERVICE_USER
Group=$SERVICE_USER
WorkingDirectory=$SERVICE_DIR
Environment=PATH=/home/$SERVICE_USER/.local/bin:/usr/local/bin:/usr/bin:/bin
EnvironmentFile=$ENV_FILE
ExecStart=/home/$SERVICE_USER/.local/bin/python src/main.py --mode bigquery-upload --start-date %i --end-date %i
StandardOutput=journal
StandardError=journal
SyslogIdentifier=bigquery-upload

[Install]
WantedBy=multi-user.target
EOF

# Create cron job for daily upload
echo -e "${YELLOW}⏰ Setting up cron job for daily upload${NC}"
sudo tee /etc/cron.d/bigquery-upload > /dev/null << EOF
# Market Data BigQuery Upload - Daily at 9 AM UTC (after candle processing)
0 9 * * * $SERVICE_USER /usr/bin/systemctl start bigquery-upload@\$(date -d 'yesterday' +\%Y-\%m-\%d)@\$(date -d 'yesterday' +\%Y-\%m-\%d) >/dev/null 2>&1
EOF

# Create monitoring script
echo -e "${YELLOW}📊 Creating monitoring script${NC}"
sudo tee "$SERVICE_DIR/monitor-bigquery-upload.sh" > /dev/null << 'EOF'
#!/bin/bash

# Monitor BigQuery upload service
SERVICE_NAME="bigquery-upload"
LOG_FILE="/var/log/market-data-handler/bigquery-upload.log"

echo "🔍 BigQuery Upload Service Status"
echo "================================="

# Check service status
if systemctl is-active --quiet "$SERVICE_NAME@*"; then
    echo "✅ Service is running"
else
    echo "❌ Service is not running"
fi

# Check recent logs
echo ""
echo "📋 Recent logs:"
journalctl -u "$SERVICE_NAME@*" --since "1 hour ago" --no-pager | tail -20

# Check disk usage
echo ""
echo "💾 Disk usage:"
df -h /opt/market-data-handler

# Check memory usage
echo ""
echo "🧠 Memory usage:"
free -h

# Check BigQuery dataset status
echo ""
echo "📊 BigQuery dataset status:"
if command -v bq &> /dev/null; then
    bq ls market_data_candles 2>/dev/null || echo "Dataset not found or not accessible"
else
    echo "BigQuery CLI not installed"
fi
EOF

sudo chmod +x "$SERVICE_DIR/monitor-bigquery-upload.sh"
sudo chown "$SERVICE_USER:$SERVICE_USER" "$SERVICE_DIR/monitor-bigquery-upload.sh"

# Create log rotation configuration
echo -e "${YELLOW}📝 Setting up log rotation${NC}"
sudo tee /etc/logrotate.d/bigquery-upload > /dev/null << EOF
$LOG_DIR/*.log {
    daily
    missingok
    rotate 30
    compress
    delaycompress
    notifempty
    create 644 $SERVICE_USER $SERVICE_USER
    postrotate
        systemctl reload bigquery-upload@* || true
    endscript
}
EOF

# Create BigQuery dataset setup script
echo -e "${YELLOW}🗄️  Creating BigQuery dataset setup script${NC}"
sudo tee "$SERVICE_DIR/setup-bigquery-dataset.sh" > /dev/null << 'EOF'
#!/bin/bash

# Setup BigQuery dataset for market data candles
PROJECT_ID=${GCP_PROJECT_ID:-"your-project-id"}
DATASET_ID=${BIGQUERY_DATASET:-"market_data_candles"}

echo "🗄️  Setting up BigQuery dataset: $DATASET_ID"
echo "Project: $PROJECT_ID"

# Create dataset
bq mk --dataset \
    --description "Market data candles and features" \
    --location US \
    "$PROJECT_ID:$DATASET_ID"

echo "✅ Dataset created successfully"

# Create tables for each timeframe
TIMEFRAMES=("15s" "1m" "5m" "15m" "1h" "4h" "24h")

for timeframe in "${TIMEFRAMES[@]}"; do
    table_name="candles_${timeframe//m/min//h/hour//s/sec}"
    echo "Creating table: $table_name"
    
    bq mk --table \
        --description "Candles for $timeframe timeframe" \
        --time_partitioning_field timestamp \
        --time_partitioning_type DAY \
        "$PROJECT_ID:$DATASET_ID.$table_name" \
        timestamp:TIMESTAMP,symbol:STRING,exchange:STRING,timeframe:STRING,date:DATE,instrument_id:STRING,open:FLOAT,high:FLOAT,low:FLOAT,close:FLOAT,volume:FLOAT,trade_count:INTEGER,vwap:FLOAT
done

echo "✅ All tables created successfully"
EOF

sudo chmod +x "$SERVICE_DIR/setup-bigquery-dataset.sh"
sudo chown "$SERVICE_USER:$SERVICE_USER" "$SERVICE_DIR/setup-bigquery-dataset.sh"

# Reload systemd
echo -e "${YELLOW}🔄 Reloading systemd${NC}"
sudo systemctl daemon-reload

# Enable service
echo -e "${YELLOW}✅ Enabling service${NC}"
sudo systemctl enable bigquery-upload@*.service

# Create test script
echo -e "${YELLOW}🧪 Creating test script${NC}"
sudo tee "$SERVICE_DIR/test-bigquery-upload.sh" > /dev/null << 'EOF'
#!/bin/bash

# Test BigQuery upload service
SERVICE_NAME="bigquery-upload"
TEST_DATE=$(date -d 'yesterday' +%Y-%m-%d)

echo "🧪 Testing BigQuery Upload Service"
echo "=================================="
echo "Test date: $TEST_DATE"

# Test with a single day
echo "Starting test..."
sudo systemctl start "$SERVICE_NAME@$TEST_DATE@$TEST_DATE"

# Wait for completion
echo "Waiting for completion..."
sleep 60

# Check status
if systemctl is-failed --quiet "$SERVICE_NAME@$TEST_DATE@$TEST_DATE"; then
    echo "❌ Test failed"
    journalctl -u "$SERVICE_NAME@$TEST_DATE@$TEST_DATE" --no-pager | tail -20
    exit 1
else
    echo "✅ Test completed successfully"
fi

# Check BigQuery data
echo "Checking BigQuery data..."
if command -v bq &> /dev/null; then
    bq query --use_legacy_sql=false "
        SELECT 
            timeframe,
            COUNT(*) as row_count,
            MIN(timestamp) as min_timestamp,
            MAX(timestamp) as max_timestamp
        FROM \`$GCP_PROJECT_ID.market_data_candles.candles_*\`
        WHERE date = '$TEST_DATE'
        GROUP BY timeframe
        ORDER BY timeframe
    "
else
    echo "BigQuery CLI not available for verification"
fi
EOF

sudo chmod +x "$SERVICE_DIR/test-bigquery-upload.sh"
sudo chown "$SERVICE_USER:$SERVICE_USER" "$SERVICE_DIR/test-bigquery-upload.sh"

echo -e "${GREEN}✅ BigQuery Upload Service deployed successfully!${NC}"
echo ""
echo "📋 Service Information:"
echo "  Service: bigquery-upload@<start-date>@<end-date>"
echo "  User: $SERVICE_USER"
echo "  Directory: $SERVICE_DIR"
echo "  Logs: $LOG_DIR"
echo "  Config: $ENV_FILE"
echo ""
echo "🚀 Usage:"
echo "  # Upload a single day"
echo "  sudo systemctl start bigquery-upload@2024-01-01@2024-01-01"
echo ""
echo "  # Upload a date range"
echo "  for date in {2024-01-01..2024-01-07}; do"
echo "    sudo systemctl start bigquery-upload@\$date@\$date"
echo "  done"
echo ""
echo "  # Setup BigQuery dataset"
echo "  $SERVICE_DIR/setup-bigquery-dataset.sh"
echo ""
echo "  # Monitor service"
echo "  $SERVICE_DIR/monitor-bigquery-upload.sh"
echo ""
echo "  # Test service"
echo "  $SERVICE_DIR/test-bigquery-upload.sh"
echo ""
echo "📊 Monitoring:"
echo "  # View logs"
echo "  journalctl -u bigquery-upload@* -f"
echo ""
echo "  # Check status"
echo "  systemctl status bigquery-upload@*"
echo ""
echo "🗄️  BigQuery:"
echo "  # Check dataset"
echo "  bq ls market_data_candles"
echo ""
echo "  # Query data"
echo "  bq query --use_legacy_sql=false 'SELECT * FROM \`your-project.market_data_candles.candles_1m\` LIMIT 10'"
