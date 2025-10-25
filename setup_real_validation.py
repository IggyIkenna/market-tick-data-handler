#!/usr/bin/env python3
"""
Setup Real Validation Environment

This script helps set up the environment for real validation testing
with actual Tardis API, Google Cloud Storage, and Binance data.
"""

import os
import sys
from pathlib import Path

def setup_environment():
    """Set up environment variables for real validation"""
    
    print("🔧 Setting up Real Validation Environment")
    print("=" * 50)
    
    # Check if credentials file exists
    creds_file = "/workspace/central-element-323112-e35fb0ddafe2.json"
    if not os.path.exists(creds_file):
        print(f"❌ Credentials file not found: {creds_file}")
        return False
    
    print(f"✅ Credentials file found: {creds_file}")
    
    # Set up environment variables
    env_vars = {
        'TARDIS_API_KEY': 'TD.your_tardis_api_key',  # User needs to replace
        'GCP_PROJECT_ID': 'central-element-323112',
        'GCS_BUCKET': 'your-gcs-bucket',  # User needs to replace
        'GCP_CREDENTIALS_PATH': creds_file,
        'USE_SECRET_MANAGER': 'false',
        'BINANCE_API_KEY': 'your_binance_api_key',  # User needs to replace
        'BINANCE_SECRET_KEY': 'your_binance_secret_key'  # User needs to replace
    }
    
    print("\n📝 Environment variables to set:")
    for key, value in env_vars.items():
        if 'your_' in value:
            print(f"   {key}={value}  # ⚠️  REPLACE WITH REAL VALUE")
        else:
            print(f"   {key}={value}")
            os.environ[key] = value
    
    print("\n🔑 Required API Keys:")
    print("1. Tardis API Key: Get from https://tardis.dev/")
    print("2. Binance API Key: Get from https://www.binance.com/en/my/settings/api-management")
    print("3. GCS Bucket: Your Google Cloud Storage bucket name")
    
    print("\n📋 Setup Commands:")
    print("export TARDIS_API_KEY='TD.your_actual_tardis_key'")
    print("export GCS_BUCKET='your_actual_bucket_name'")
    print("export BINANCE_API_KEY='your_actual_binance_key'")
    print("export BINANCE_SECRET_KEY='your_actual_binance_secret'")
    
    return True

def check_dependencies():
    """Check if all required dependencies are installed"""
    
    print("\n🔍 Checking Dependencies...")
    
    required_packages = [
        'ccxt',
        'pandas',
        'numpy',
        'google-cloud-storage',
        'google-cloud-bigquery',
        'google-auth',
        'pydantic',
        'python-dotenv',
        'python-dateutil',
        'pytz'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
            print(f"   ✅ {package}")
        except ImportError:
            print(f"   ❌ {package}")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\n📦 Install missing packages:")
        print(f"pip3 install {' '.join(missing_packages)}")
        return False
    
    print("\n✅ All dependencies are installed!")
    return True

def create_test_config():
    """Create a test configuration file"""
    
    config_content = '''# Real Validation Test Configuration
# Copy this to .env and fill in your actual values

# Tardis API Configuration
TARDIS_API_KEY=TD.your_tardis_api_key_here

# Google Cloud Configuration
GCP_PROJECT_ID=central-element-323112
GCS_BUCKET=your_gcs_bucket_name_here
GCP_CREDENTIALS_PATH=/workspace/central-element-323112-e35fb0ddafe2.json
USE_SECRET_MANAGER=false

# Binance API Configuration
BINANCE_API_KEY=your_binance_api_key_here
BINANCE_SECRET_KEY=your_binance_secret_key_here

# Validation Configuration
VALIDATION_TIMEOUT_SECONDS=300
VALIDATION_MAX_CANDLES=1000
VALIDATION_TOLERANCE_PERCENT=0.1
'''
    
    with open('/workspace/.env.example', 'w') as f:
        f.write(config_content)
    
    print("\n📄 Created .env.example file with configuration template")
    print("   Copy .env.example to .env and fill in your actual values")

def main():
    """Main setup function"""
    
    print("🚀 Real Validation Setup")
    print("=" * 50)
    
    # Setup environment
    if not setup_environment():
        return 1
    
    # Check dependencies
    if not check_dependencies():
        return 1
    
    # Create test config
    create_test_config()
    
    print("\n✅ Setup completed!")
    print("\n📋 Next Steps:")
    print("1. Set your actual API keys in environment variables")
    print("2. Update GCS_BUCKET with your actual bucket name")
    print("3. Run: python3 test_real_validation.py")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())