#!/bin/bash

# Market Tick Data Handler - Setup Script
# Handles architecture-specific Python dependencies and environment setup

set -e

echo "🚀 Setting up Market Tick Data Handler..."

# Detect architecture
ARCH=$(uname -m)
OS=$(uname -s)

echo "📋 Detected: $OS $ARCH"

# Check Python version
PYTHON_VERSION=$(python3 --version 2>&1 | cut -d' ' -f2 | cut -d'.' -f1,2)
echo "🐍 Python version: $PYTHON_VERSION"

# Install Python dependencies with architecture-specific handling
echo "📦 Installing Python dependencies..."

if [[ "$ARCH" == "arm64" && "$OS" == "Darwin" ]]; then
    echo "🍎 Detected Apple Silicon (ARM64) - installing ARM64-compatible packages..."
    
    # Force reinstall numpy and pandas for ARM64 compatibility
    python3 -m pip install --upgrade --force-reinstall --no-cache-dir \
        numpy>=2.0.0 \
        pandas>=2.3.0 \
        scipy>=1.11.4 \
        scikit-learn>=1.3.2
    
    # Install other dependencies
    python3 -m pip install --upgrade --no-cache-dir -r requirements.txt
    
elif [[ "$ARCH" == "x86_64" ]]; then
    echo "💻 Detected x86_64 - installing standard packages..."
    python3 -m pip install --upgrade --no-cache-dir -r requirements.txt
    
else
    echo "⚠️  Unknown architecture $ARCH - attempting standard installation..."
    python3 -m pip install --upgrade --no-cache-dir -r requirements.txt
fi

# Install Node.js dependencies for streaming
echo "📦 Installing Node.js dependencies..."
if command -v npm &> /dev/null; then
    cd streaming
    npm install
    cd ..
    echo "✅ Node.js dependencies installed"
else
    echo "⚠️  npm not found - Node.js streaming will not work"
    echo "   Install Node.js from https://nodejs.org/"
fi

# Verify installation
echo "🔍 Verifying installation..."

# Test Python imports
python3 -c "
import numpy as np
import pandas as pd
import sys
print(f'✅ NumPy {np.__version__} - {np.show_config()}')
print(f'✅ Pandas {pd.__version__}')
print(f'✅ Python {sys.version}')
print('✅ All Python dependencies working!')
"

# Test Node.js streaming dependencies
if command -v node &> /dev/null; then
    cd streaming
    node -e "
    try {
        const tardis = require('tardis-dev');
        const { BigQuery } = require('@google-cloud/bigquery');
        console.log('✅ Node.js dependencies working!');
    } catch (e) {
        console.log('❌ Node.js dependencies error:', e.message);
        process.exit(1);
    }
    "
    cd ..
fi

echo ""
echo "🎉 Setup complete!"
echo ""
echo "📋 Next steps:"
echo "1. Set up environment variables:"
echo "   export TARDIS_API_KEY=your_tardis_key"
echo "   export GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account.json"
echo "   export GCP_PROJECT_ID=your_project_id"
echo ""
echo "2. Test the system:"
echo "   python3 -m market_data_tick_handler.main. --mode missing-tick-reports --start-date 2023-05-23 --end-date 2023-05-23"
echo "   node streaming/live_tick_streamer.js --mode candles --symbol BTC-USDT --duration 10"
echo ""
echo "3. For production deployment, see docs/DEPLOYMENT.md"
