#!/usr/bin/env python3
"""
Authentication Examples for Market Data Handler Package

This example demonstrates how to use the package with different authentication modes:
1. Production (Secret Manager + GCS)
2. Development (Environment variables + GCS)
3. Read-only (GCS only)
4. Mock/Offline (No GCP access)
"""

import sys
import os
from datetime import datetime, timezone, date
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from market_data_tick_handler.data_client import DataClient
from config import get_config

def example_production():
    """Example: Production authentication with Secret Manager"""
    print("🔐 Production Authentication Example")
    print("=" * 50)
    
    # Set up production environment
    os.environ['USE_SECRET_MANAGER'] = 'true'
    os.environ['GCP_PROJECT_ID'] = 'your-production-project'
    os.environ['GCP_CREDENTIALS_PATH'] = './market-data-prod-key.json'
    os.environ['TARDIS_SECRET_NAME'] = 'tardis-api-key'
    os.environ['GCS_BUCKET'] = 'your-production-bucket'
    
    try:
        # Initialize with production config
        config = get_config()
        data_client = DataClient(config.gcp.bucket, config)
        
        print(f"✅ DataClient initialized (mock: {data_client.is_mock})")
        print(f"   - Project: {config.gcp.project_id}")
        print(f"   - Bucket: {config.gcp.bucket}")
        print(f"   - Secret Manager: {config.auth.use_secret_manager}")
        
        # Example: Get instrument definitions
        # instruments = await data_client.get_instrument_definitions()
        # print(f"   - Found {instruments['count']} instruments")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("   Make sure you have:")
        print("   1. Valid GCP credentials")
        print("   2. Secret Manager access")
        print("   3. Tardis API key in Secret Manager")

def example_development():
    """Example: Development authentication with environment variables"""
    print("\n🔧 Development Authentication Example")
    print("=" * 50)
    
    # Set up development environment
    os.environ['USE_SECRET_MANAGER'] = 'false'
    os.environ['TARDIS_API_KEY'] = 'TD.your_test_key'
    os.environ['GCP_PROJECT_ID'] = 'your-dev-project'
    os.environ['GCP_CREDENTIALS_PATH'] = './market-data-dev-key.json'
    os.environ['GCS_BUCKET'] = 'your-dev-bucket'
    
    try:
        # Initialize with development config
        config = get_config()
        data_client = DataClient(config.gcp.bucket, config)
        
        print(f"✅ DataClient initialized (mock: {data_client.is_mock})")
        print(f"   - Project: {config.gcp.project_id}")
        print(f"   - Bucket: {config.gcp.bucket}")
        print(f"   - Tardis API Key: {config.tardis.api_key[:10]}...")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("   Make sure you have:")
        print("   1. Valid GCP credentials")
        print("   2. Tardis API key in environment")

def example_readonly():
    """Example: Read-only authentication"""
    print("\n👁️  Read-Only Authentication Example")
    print("=" * 50)
    
    # Set up read-only environment
    os.environ['USE_SECRET_MANAGER'] = 'false'
    os.environ['GCP_PROJECT_ID'] = 'your-project'
    os.environ['GCP_CREDENTIALS_PATH'] = './market-data-readonly-key.json'
    os.environ['GCS_BUCKET'] = 'your-data-bucket'
    
    try:
        # Initialize with read-only config
        config = get_config()
        data_client = DataClient(config.gcp.bucket, config)
        
        print(f"✅ DataClient initialized (mock: {data_client.is_mock})")
        print(f"   - Project: {config.gcp.project_id}")
        print(f"   - Bucket: {config.gcp.bucket}")
        print(f"   - Read-only access")
        
        # Example: Read candles (read-only operation)
        # candles = data_client.candle_reader.get_candles(
        #     instrument_id="BINANCE:SPOT_PAIR:BTC-USDT",
        #     timeframe="1m",
        #     start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        #     end_date=datetime(2024, 1, 2, tzinfo=timezone.utc)
        # )
        # print(f"   - Found {len(candles)} candles")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("   Make sure you have:")
        print("   1. Valid GCP credentials")
        print("   2. Read-only GCS access")

def example_mock():
    """Example: Mock/offline authentication"""
    print("\n🎭 Mock/Offline Authentication Example")
    print("=" * 50)
    
    # Set up mock environment
    os.environ['USE_MOCK_DATA'] = 'true'
    os.environ['MOCK_DATA_PATH'] = './mock_data'
    os.environ['GCS_BUCKET'] = 'mock-bucket'  # Not used in mock mode
    
    try:
        # Initialize with mock config
        config = get_config()
        data_client = DataClient(config.gcp.bucket, config)
        
        print(f"✅ DataClient initialized (mock: {data_client.is_mock})")
        print(f"   - Mock data path: {config.auth.mock_data_path}")
        print(f"   - Offline development mode")
        
        # Example: Get mock instruments
        # instruments = data_client._mock_client.get_instruments(
        #     start_date=date(2024, 1, 1),
        #     end_date=date(2024, 1, 2)
        # )
        # print(f"   - Found {len(instruments)} mock instruments")
        
        # Example: Get mock candles
        # candles = data_client._mock_client.get_candles(
        #     instrument_id="BINANCE:SPOT_PAIR:BTC-USDT",
        #     timeframe="1m",
        #     start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        #     end_date=datetime(2024, 1, 2, tzinfo=timezone.utc)
        # )
        # print(f"   - Found {len(candles)} mock candles")
        
    except Exception as e:
        print(f"❌ Error: {e}")

def example_graceful_degradation():
    """Example: Graceful degradation when GCP is unavailable"""
    print("\n🔄 Graceful Degradation Example")
    print("=" * 50)
    
    # Set up environment that will fail GCP authentication
    os.environ['USE_SECRET_MANAGER'] = 'false'
    os.environ['USE_MOCK_DATA'] = 'true'  # Enable fallback
    os.environ['TARDIS_API_KEY'] = 'TD.invalid_key'
    os.environ['GCP_PROJECT_ID'] = 'invalid-project'
    os.environ['GCP_CREDENTIALS_PATH'] = './invalid-credentials.json'
    os.environ['GCS_BUCKET'] = 'invalid-bucket'
    
    try:
        # This should fall back to mock data
        config = get_config()
        data_client = DataClient(config.gcp.bucket, config)
        
        print(f"✅ DataClient initialized (mock: {data_client.is_mock})")
        print(f"   - Gracefully fell back to mock data")
        print(f"   - No GCP access required")
        
    except Exception as e:
        print(f"❌ Error: {e}")

def main():
    """Run all authentication examples"""
    print("🚀 Market Data Handler - Authentication Examples")
    print("=" * 60)
    print()
    print("This example demonstrates different authentication modes")
    print("for using the Market Data Handler package.")
    print()
    
    # Run examples
    example_production()
    example_development()
    example_readonly()
    example_mock()
    example_graceful_degradation()
    
    print("\n" + "=" * 60)
    print("📚 Next Steps:")
    print("1. Choose the authentication mode that fits your use case")
    print("2. Run the setup script: ./scripts/setup-auth.sh [tier] [project] [service-account]")
    print("3. Set the appropriate environment variables")
    print("4. Start using the package in your application")
    print()
    print("For more information, see:")
    print("- docs/AUTHENTICATION_STRATEGY.md")
    print("- docs/PACKAGE_USAGE.md")

if __name__ == "__main__":
    main()
