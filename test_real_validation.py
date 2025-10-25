#!/usr/bin/env python3
"""
Test Real Validation Framework

Simple test to verify the real validation framework works with actual services.
"""

import sys
import os
import asyncio
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

# Set up environment variables for real services
os.environ['GCP_CREDENTIALS_PATH'] = '/workspace/central-element-323112-e35fb0ddafe2.json'

import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def test_environment_setup():
    """Test environment setup"""
    print("🔧 Testing Environment Setup...")
    
    # Check environment variables
    required_vars = [
        'GCP_PROJECT_ID', 
        'GCS_BUCKET',
        'GCP_CREDENTIALS_PATH'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    # Check if Tardis API key is available (either from env or secret manager)
    if not os.getenv('TARDIS_API_KEY') and not os.getenv('USE_SECRET_MANAGER'):
        missing_vars.append('TARDIS_API_KEY (or USE_SECRET_MANAGER=true)')
    
    if missing_vars:
        print(f"❌ Missing environment variables: {missing_vars}")
        print("   Please set your actual API keys and configuration")
        return False
    
    # Check credentials file
    creds_file = os.getenv('GCP_CREDENTIALS_PATH')
    if not os.path.exists(creds_file):
        print(f"❌ Credentials file not found: {creds_file}")
        return False
    
    print("✅ Environment setup validated")
    return True


async def test_imports():
    """Test that all imports work"""
    print("📦 Testing Imports...")
    
    try:
        from src.validation.cross_source_validator import CrossSourceValidator
        from src.validation.timestamp_validator import TimestampValidator
        from src.validation.aggregation_validator import AggregationValidator
        from src.data_downloader.data_client import DataClient
        from src.data_downloader.tardis_connector import TardisConnector
        from config import get_config
        
        print("✅ All imports successful")
        return True
        
    except Exception as e:
        print(f"❌ Import failed: {e}")
        return False


async def test_config_loading():
    """Test configuration loading"""
    print("⚙️ Testing Configuration Loading...")
    
    try:
        from config import get_config
        
        config = get_config()
        print(f"✅ Configuration loaded for project: {config.gcp.project_id}")
        return True
        
    except Exception as e:
        print(f"❌ Configuration loading failed: {e}")
        return False


async def test_services_initialization():
    """Test services initialization"""
    print("🔌 Testing Services Initialization...")
    
    try:
        from src.data_downloader.data_client import DataClient
        from src.data_downloader.tardis_connector import TardisConnector
        from src.validation.cross_source_validator import CrossSourceValidator
        from config import get_config
        
        # Load configuration
        config = get_config()
        
        # Initialize services
        data_client = DataClient(config.gcp.bucket, config)
        tardis_connector = TardisConnector()
        cross_source_validator = CrossSourceValidator(data_client, tardis_connector)
        
        print("✅ Services initialized successfully")
        return True
        
    except Exception as e:
        print(f"❌ Services initialization failed: {e}")
        return False


async def test_binance_data_retrieval():
    """Test Binance data retrieval"""
    print("🔄 Testing Binance Data Retrieval...")
    
    try:
        from src.data_downloader.data_client import DataClient
        from src.data_downloader.tardis_connector import TardisConnector
        from src.validation.cross_source_validator import CrossSourceValidator
        from config import get_config
        
        # Initialize services
        config = get_config()
        data_client = DataClient(config.gcp.bucket, config)
        tardis_connector = TardisConnector()
        cross_source_validator = CrossSourceValidator(data_client, tardis_connector)
        
        # Test parameters
        symbol = "BTC-USDT"
        timeframe = "1m"
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=1)
        
        # Get Binance candles
        binance_candles = await cross_source_validator._get_binance_candles(
            symbol=symbol,
            timeframe=timeframe,
            start_date=start_time,
            end_date=end_time,
            max_candles=100
        )
        
        if binance_candles:
            print(f"✅ Retrieved {len(binance_candles)} Binance candles")
            print(f"   First candle: {binance_candles[0].timestamp} - O:{binance_candles[0].open} H:{binance_candles[0].high} L:{binance_candles[0].low} C:{binance_candles[0].close}")
            return True
        else:
            print("⚠️ No Binance candles retrieved")
            return False
            
    except Exception as e:
        print(f"❌ Binance data retrieval failed: {e}")
        return False


async def main():
    """Main test function"""
    
    print("🧪 Real Validation Framework Test")
    print("=" * 50)
    print("This test verifies the real validation framework works with actual services")
    print("=" * 50)
    
    tests = [
        ("Environment Setup", test_environment_setup),
        ("Imports", test_imports),
        ("Configuration Loading", test_config_loading),
        ("Services Initialization", test_services_initialization),
        ("Binance Data Retrieval", test_binance_data_retrieval),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        try:
            result = await test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} failed with exception: {e}")
            results.append((test_name, False))
    
    # Print summary
    print("\n" + "=" * 50)
    print("📊 Test Results Summary")
    print("=" * 50)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed! The real validation framework is ready to use.")
        print("\nNext steps:")
        print("1. Set your actual API keys in environment variables")
        print("2. Run: python3 real_validation.py")
        return 0
    else:
        print(f"\n⚠️ {total - passed} tests failed. Please check the configuration.")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)