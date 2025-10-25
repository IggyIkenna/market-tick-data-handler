#!/bin/bash
# Quick setup script for developers

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "🚀 Market Data Handler - Developer Setup"
echo "========================================"
echo ""

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is required but not installed"
    exit 1
fi

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "📦 Creating Python virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo "📥 Installing dependencies..."
pip install -r requirements.txt

echo ""
echo "🎯 Choose your setup method:"
echo ""
echo "1. Mock Data Mode (No GCP required)"
echo "2. Restore Credentials from Git History"
echo "3. Use Your Own Credentials"
echo "4. Skip Setup (Manual configuration)"
echo ""

read -p "Enter your choice (1-4): " choice

case $choice in
    1)
        echo ""
        echo "🎭 Setting up Mock Data Mode..."
        export USE_MOCK_DATA=true
        export MOCK_DATA_PATH=./mock_data
        export GCS_BUCKET=mock-bucket
        
        # Create mock data directory
        mkdir -p mock_data
        
        echo "✅ Mock data mode configured"
        echo ""
        echo "To test:"
        echo "  python examples/standalone_performance_test.py"
        ;;
    2)
        echo ""
        echo "📁 Restoring credentials from git history..."
        ./scripts/restore-credentials.sh
        
        if [ $? -eq 0 ]; then
            echo ""
            echo "✅ Credentials restored successfully"
            echo ""
            echo "To test:"
            echo "  export GOOGLE_APPLICATION_CREDENTIALS=./central-element-323112-e35fb0ddafe2.json"
            echo "  python examples/performance_comparison_test.py"
        fi
        ;;
    3)
        echo ""
        echo "🔑 Manual credentials setup..."
        echo ""
        echo "Please:"
        echo "1. Get credentials file from team member"
        echo "2. Place it in the project root"
        echo "3. Set environment variable:"
        echo "   export GOOGLE_APPLICATION_CREDENTIALS=./your-credentials.json"
        echo "4. Run: python examples/performance_comparison_test.py"
        ;;
    4)
        echo ""
        echo "⏭️  Skipping automatic setup"
        echo ""
        echo "See docs/DEVELOPER_SETUP.md for manual setup instructions"
        ;;
    *)
        echo "❌ Invalid choice"
        exit 1
        ;;
esac

echo ""
echo "🎉 Setup complete!"
echo ""
echo "📚 Next steps:"
echo "1. Read docs/DEVELOPER_SETUP.md for detailed instructions"
echo "2. Run performance tests to verify setup"
echo "3. Explore examples/ folder for usage examples"
echo ""
echo "Happy coding! 🚀"
