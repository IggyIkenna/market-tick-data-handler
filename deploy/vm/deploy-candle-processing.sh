#!/bin/bash

# Deploy Candle Processing Service
# Processes historical tick data into candles with HFT features

set -e

# Configuration
SERVICE_NAME="candle-processing"
SERVICE_USER="marketdata"
SERVICE_DIR="/opt/market-data-handler"
LOG_DIR="/var/log/market-data-handler"
ENV_FILE="/etc/market-data-handler/.env"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 Deploying Candle Processing Service${NC}"
echo "================================================"

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

# Candle Processing Configuration
ENABLE_HFT_FEATURES=true
ENABLE_BOOK_SNAPSHOTS=true
ENABLE_OPTIONS_SKEW=false
BATCH_SIZE=1000
MAX_CONCURRENT_DAYS=5
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
sudo tee /etc/systemd/system/candle-processing.service > /dev/null << EOF
[Unit]
Description=Market Data Candle Processing Service
After=network.target

[Service]
Type=oneshot
User=$SERVICE_USER
Group=$SERVICE_USER
WorkingDirectory=$SERVICE_DIR
Environment=PATH=/home/$SERVICE_USER/.local/bin:/usr/local/bin:/usr/bin:/bin
EnvironmentFile=$ENV_FILE
ExecStart=/home/$SERVICE_USER/.local/bin/python src/main.py --mode candle-processing --start-date %i --end-date %i
StandardOutput=journal
StandardError=journal
SyslogIdentifier=candle-processing

[Install]
WantedBy=multi-user.target
EOF

# Create cron job for daily processing
echo -e "${YELLOW}⏰ Setting up cron job for daily processing${NC}"
sudo tee /etc/cron.d/candle-processing > /dev/null << EOF
# Market Data Candle Processing - Daily at 8 AM UTC
0 8 * * * $SERVICE_USER /usr/bin/systemctl start candle-processing@\$(date -d 'yesterday' +\%Y-\%m-\%d)@\$(date -d 'yesterday' +\%Y-\%m-\%d) >/dev/null 2>&1
EOF

# Create monitoring script
echo -e "${YELLOW}📊 Creating monitoring script${NC}"
sudo tee "$SERVICE_DIR/monitor-candle-processing.sh" > /dev/null << 'EOF'
#!/bin/bash

# Monitor candle processing service
SERVICE_NAME="candle-processing"
LOG_FILE="/var/log/market-data-handler/candle-processing.log"

echo "🔍 Candle Processing Service Status"
echo "=================================="

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
EOF

sudo chmod +x "$SERVICE_DIR/monitor-candle-processing.sh"
sudo chown "$SERVICE_USER:$SERVICE_USER" "$SERVICE_DIR/monitor-candle-processing.sh"

# Create log rotation configuration
echo -e "${YELLOW}📝 Setting up log rotation${NC}"
sudo tee /etc/logrotate.d/candle-processing > /dev/null << EOF
$LOG_DIR/*.log {
    daily
    missingok
    rotate 30
    compress
    delaycompress
    notifempty
    create 644 $SERVICE_USER $SERVICE_USER
    postrotate
        systemctl reload candle-processing@* || true
    endscript
}
EOF

# Reload systemd
echo -e "${YELLOW}🔄 Reloading systemd${NC}"
sudo systemctl daemon-reload

# Enable service
echo -e "${YELLOW}✅ Enabling service${NC}"
sudo systemctl enable candle-processing@*.service

# Create test script
echo -e "${YELLOW}🧪 Creating test script${NC}"
sudo tee "$SERVICE_DIR/test-candle-processing.sh" > /dev/null << 'EOF'
#!/bin/bash

# Test candle processing service
SERVICE_NAME="candle-processing"
TEST_DATE=$(date -d 'yesterday' +%Y-%m-%d)

echo "🧪 Testing Candle Processing Service"
echo "===================================="
echo "Test date: $TEST_DATE"

# Test with a single day
echo "Starting test..."
sudo systemctl start "$SERVICE_NAME@$TEST_DATE@$TEST_DATE"

# Wait for completion
echo "Waiting for completion..."
sleep 30

# Check status
if systemctl is-failed --quiet "$SERVICE_NAME@$TEST_DATE@$TEST_DATE"; then
    echo "❌ Test failed"
    journalctl -u "$SERVICE_NAME@$TEST_DATE@$TEST_DATE" --no-pager | tail -20
    exit 1
else
    echo "✅ Test completed successfully"
fi
EOF

sudo chmod +x "$SERVICE_DIR/test-candle-processing.sh"
sudo chown "$SERVICE_USER:$SERVICE_USER" "$SERVICE_DIR/test-candle-processing.sh"

echo -e "${GREEN}✅ Candle Processing Service deployed successfully!${NC}"
echo ""
echo "📋 Service Information:"
echo "  Service: candle-processing@<start-date>@<end-date>"
echo "  User: $SERVICE_USER"
echo "  Directory: $SERVICE_DIR"
echo "  Logs: $LOG_DIR"
echo "  Config: $ENV_FILE"
echo ""
echo "🚀 Usage:"
echo "  # Process a single day"
echo "  sudo systemctl start candle-processing@2024-01-01@2024-01-01"
echo ""
echo "  # Process a date range (multiple days)"
echo "  for date in {2024-01-01..2024-01-07}; do"
echo "    sudo systemctl start candle-processing@\$date@\$date"
echo "  done"
echo ""
echo "  # Monitor service"
echo "  $SERVICE_DIR/monitor-candle-processing.sh"
echo ""
echo "  # Test service"
echo "  $SERVICE_DIR/test-candle-processing.sh"
echo ""
echo "📊 Monitoring:"
echo "  # View logs"
echo "  journalctl -u candle-processing@* -f"
echo ""
echo "  # Check status"
echo "  systemctl status candle-processing@*"
