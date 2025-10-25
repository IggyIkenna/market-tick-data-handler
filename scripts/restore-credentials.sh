#!/bin/bash
# Restore credentials file from previous commit

set -e

CREDENTIALS_FILE="central-element-323112-e35fb0ddafe2.json"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "🔍 Looking for credentials file in git history..."

# Check if file exists in current working directory
if [ -f "$PROJECT_ROOT/$CREDENTIALS_FILE" ]; then
    echo "✅ Credentials file already exists: $CREDENTIALS_FILE"
    exit 0
fi

# Find the last commit where the file existed
LAST_COMMIT=$(git log --oneline --follow -- "$CREDENTIALS_FILE" | head -1 | cut -d' ' -f1)

if [ -z "$LAST_COMMIT" ]; then
    echo "❌ Credentials file not found in git history"
    echo ""
    echo "Please get the credentials file from a team member or create your own:"
    echo "1. Ask a team member for the file"
    echo "2. Create your own service account key"
    echo "3. Use mock data mode for testing"
    echo ""
    echo "See docs/DEVELOPER_SETUP.md for detailed instructions"
    exit 1
fi

echo "📁 Found credentials file in commit: $LAST_COMMIT"
echo "🔄 Restoring credentials file..."

# Restore the file from the last commit
git checkout "$LAST_COMMIT" -- "$CREDENTIALS_FILE"

if [ -f "$PROJECT_ROOT/$CREDENTIALS_FILE" ]; then
    echo "✅ Successfully restored credentials file: $CREDENTIALS_FILE"
    echo ""
    echo "Next steps:"
    echo "1. Set environment variable:"
    echo "   export GOOGLE_APPLICATION_CREDENTIALS=./$CREDENTIALS_FILE"
    echo ""
    echo "2. Run the performance test:"
    echo "   python examples/performance_comparison_test.py"
else
    echo "❌ Failed to restore credentials file"
    exit 1
fi
