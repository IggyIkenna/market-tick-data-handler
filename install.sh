#!/bin/bash
# Market Data Tick Handler - Easy Installation Script
# This script handles the complete installation process for external users

set -e  # Exit on any error

echo "🚀 Market Data Tick Handler - Installation Script"
echo "=================================================="

# Check Python version
echo "📋 Checking Python version..."
python_version=$(python3 --version 2>&1 | cut -d' ' -f2 | cut -d'.' -f1,2)
required_version="3.8"

if [ "$(printf '%s\n' "$required_version" "$python_version" | sort -V | head -n1)" != "$required_version" ]; then
    echo "❌ Error: Python 3.8+ is required. Found: Python $python_version"
    echo "Please install Python 3.8 or higher and try again."
    exit 1
fi

echo "✅ Python $python_version detected"

# Check if pip is available
if ! command -v pip3 &> /dev/null; then
    echo "❌ Error: pip3 is not installed"
    echo "Please install pip3 and try again."
    exit 1
fi

echo "✅ pip3 is available"

# Install the package
echo "📦 Installing Market Data Tick Handler..."
echo "This will install all required dependencies automatically."

# Install from GitHub (recommended for external users)
if [ "$1" = "--local" ]; then
    echo "Installing from local source..."
    pip3 install -e .
else
    echo "Installing from GitHub repository..."
    pip3 install git+https://github.com/iggyikenna/market-tick-data-handler.git
fi

echo "✅ Installation completed successfully!"

# Test the installation
echo "🧪 Testing installation..."
python3 -c "from market_data_tick_handler import DataClient; print('✅ Package imports work correctly')"

echo ""
echo "🎉 Installation Complete!"
echo "========================"
echo ""
echo "Next steps:"
echo "1. Set up authentication:"
echo "   export USE_SECRET_MANAGER=true"
echo "   export GCP_PROJECT_ID=your-project-id"
echo ""
echo "2. Run the package:"
echo "   python3 -m market_data_tick_handler --mode instruments --start-date 2023-05-23 --end-date 2023-05-25"
echo ""
echo "3. Or use as a Python library:"
echo "   from market_data_tick_handler import DataClient"
echo ""
echo "📚 For more information, see:"
echo "   - PACKAGE_INSTALLATION.md"
echo "   - docs/"
echo "   - examples/"
echo ""
echo "🐛 Need help? Check:"
echo "   - GitHub Issues: https://github.com/iggyikenna/market-tick-data-handler/issues"
echo "   - Documentation: https://github.com/iggyikenna/market-tick-data-handler/tree/main/docs"
