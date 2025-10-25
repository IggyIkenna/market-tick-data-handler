#!/usr/bin/env python3
"""
Real Validation Tests

Tests using actual Tardis API, Google Cloud Storage, and Binance data
as specified in the documentation.
"""

import sys
import os
import asyncio
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

# Set up environment variables for real services
os.environ['TARDIS_API_KEY'] = 'TD.your_tardis_api_key'  # Replace with real key
os.environ['GCP_PROJECT_ID'] = 'central-element-323112'
os.environ['GCS_BUCKET'] = 'your-gcs-bucket'  # Replace with real bucket
os.environ['GCP_CREDENTIALS_PATH'] = '/workspace/central-element-323112-e35fb0ddafe2.json'
os.environ['USE_SECRET_MANAGER'] = 'false'

# Binance API credentials (replace with real keys)
os.environ['BINANCE_API_KEY'] = 'your_binance_api_key'
os.environ['BINANCE_SECRET_KEY'] = 'your_binance_secret_key'

import logging
from src.validation.cross_source_validator import CrossSourceValidator
from src.validation.timestamp_validator import TimestampValidator
from src.validation.aggregation_validator import AggregationValidator
from src.validation.validation_results import ValidationStatus, ValidationResult, ValidationReport
from src.data_downloader.data_client import DataClient
from src.data_downloader.tardis_connector import TardisConnector
from config import get_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RealValidationTester:
    """Test validation framework with real services"""
    
    def __init__(self):
        self.config = None
        self.data_client = None
        self.tardis_connector = None
        self.cross_source_validator = None
        self.timestamp_validator = None
        self.aggregation_validator = None
    
    async def setup(self):
        """Set up real services"""
        try:
            logger.info("🔧 Setting up real services...")
            
            # Load configuration
            self.config = get_config()
            logger.info(f"✅ Configuration loaded for project: {self.config.gcp.project_id}")
            
            # Initialize data client
            self.data_client = DataClient(self.config.gcp.bucket, self.config)
            logger.info(f"✅ Data client initialized for bucket: {self.config.gcp.bucket}")
            
            # Initialize Tardis connector
            self.tardis_connector = TardisConnector()
            logger.info("✅ Tardis connector initialized")
            
            # Initialize validators
            self.cross_source_validator = CrossSourceValidator(
                self.data_client, 
                self.tardis_connector
            )
            self.timestamp_validator = TimestampValidator()
            self.aggregation_validator = AggregationValidator()
            
            logger.info("✅ All validators initialized")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to setup services: {e}")
            return False
    
    async def test_binance_data_retrieval(self):
        """Test retrieving real Binance data"""
        try:
            logger.info("🔄 Testing Binance data retrieval...")
            
            # Test parameters
            symbol = "BTC-USDT"
            timeframe = "1m"
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(hours=1)
            
            # Get Binance candles
            binance_candles = await self.cross_source_validator._get_binance_candles(
                symbol=symbol,
                timeframe=timeframe,
                start_date=start_time,
                end_date=end_time,
                max_candles=100
            )
            
            if binance_candles:
                logger.info(f"✅ Retrieved {len(binance_candles)} Binance candles")
                logger.info(f"   First candle: {binance_candles[0].timestamp} - O:{binance_candles[0].open} H:{binance_candles[0].high} L:{binance_candles[0].low} C:{binance_candles[0].close}")
                logger.info(f"   Last candle: {binance_candles[-1].timestamp} - O:{binance_candles[-1].open} H:{binance_candles[-1].high} L:{binance_candles[-1].low} C:{binance_candles[-1].close}")
                return True
            else:
                logger.warning("⚠️ No Binance candles retrieved")
                return False
                
        except Exception as e:
            logger.error(f"❌ Binance data retrieval failed: {e}")
            return False
    
    async def test_tardis_data_retrieval(self):
        """Test retrieving real Tardis data from GCS"""
        try:
            logger.info("🔄 Testing Tardis data retrieval from GCS...")
            
            # Test parameters
            symbol = "BTC-USDT"
            timeframe = "1m"
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(hours=1)
            
            # Get Tardis candles
            tardis_candles = await self.cross_source_validator._get_tardis_candles(
                symbol=symbol,
                timeframe=timeframe,
                start_date=start_time,
                end_date=end_time,
                max_candles=100
            )
            
            if tardis_candles:
                logger.info(f"✅ Retrieved {len(tardis_candles)} Tardis candles")
                logger.info(f"   First candle: {tardis_candles[0].timestamp} - O:{tardis_candles[0].open} H:{tardis_candles[0].high} L:{tardis_candles[0].low} C:{tardis_candles[0].close}")
                logger.info(f"   Last candle: {tardis_candles[-1].timestamp} - O:{tardis_candles[-1].open} H:{tardis_candles[-1].high} L:{tardis_candles[-1].low} C:{tardis_candles[-1].close}")
                return True
            else:
                logger.warning("⚠️ No Tardis candles retrieved (this is expected if no data is available)")
                return False
                
        except Exception as e:
            logger.error(f"❌ Tardis data retrieval failed: {e}")
            return False
    
    async def test_cross_source_validation(self):
        """Test cross-source validation with real data"""
        try:
            logger.info("🔄 Testing cross-source validation...")
            
            # Test parameters
            symbol = "BTC-USDT"
            timeframe = "1m"
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(hours=1)
            
            # Run cross-source validation
            result = await self.cross_source_validator.validate_timeframe_consistency(
                symbol=symbol,
                timeframe=timeframe,
                start_date=start_time,
                end_date=end_time,
                max_candles=100
            )
            
            logger.info(f"✅ Cross-source validation completed")
            logger.info(f"   Status: {result.status.value}")
            logger.info(f"   Message: {result.message}")
            if result.details:
                logger.info(f"   Details: {result.details}")
            
            return result.status == ValidationStatus.PASS
            
        except Exception as e:
            logger.error(f"❌ Cross-source validation failed: {e}")
            return False
    
    async def test_timestamp_validation(self):
        """Test timestamp validation with real data"""
        try:
            logger.info("🔄 Testing timestamp validation...")
            
            # Get some real data for testing
            symbol = "BTC-USDT"
            timeframe = "1m"
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(hours=1)
            
            # Get Binance candles for timestamp validation
            binance_candles = await self.cross_source_validator._get_binance_candles(
                symbol=symbol,
                timeframe=timeframe,
                start_date=start_time,
                end_date=end_time,
                max_candles=100
            )
            
            if not binance_candles:
                logger.warning("⚠️ No Binance candles available for timestamp validation")
                return False
            
            # Extract timestamps
            timestamps = [candle.timestamp for candle in binance_candles]
            
            # Run timestamp validation
            result = self.timestamp_validator.validate_timestamp_stability(
                timestamps=timestamps,
                expected_interval_seconds=60.0,
                test_name="real_timestamp_validation"
            )
            
            logger.info(f"✅ Timestamp validation completed")
            logger.info(f"   Status: {result.status.value}")
            logger.info(f"   Message: {result.message}")
            if result.details:
                logger.info(f"   Details: {result.details}")
            
            return result.status == ValidationStatus.PASS
            
        except Exception as e:
            logger.error(f"❌ Timestamp validation failed: {e}")
            return False
    
    async def test_aggregation_validation(self):
        """Test aggregation validation with real data"""
        try:
            logger.info("🔄 Testing aggregation validation...")
            
            # Get real data for testing
            symbol = "BTC-USDT"
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(hours=2)
            
            # Get 1m candles
            candles_1m = await self.cross_source_validator._get_binance_candles(
                symbol=symbol,
                timeframe="1m",
                start_date=start_time,
                end_date=end_time,
                max_candles=120
            )
            
            if not candles_1m or len(candles_1m) < 10:
                logger.warning("⚠️ Insufficient 1m candles for aggregation validation")
                return False
            
            # Get 5m candles
            candles_5m = await self.cross_source_validator._get_binance_candles(
                symbol=symbol,
                timeframe="5m",
                start_date=start_time,
                end_date=end_time,
                max_candles=24
            )
            
            if not candles_5m or len(candles_5m) < 5:
                logger.warning("⚠️ Insufficient 5m candles for aggregation validation")
                return False
            
            # Run aggregation validation
            result = self.aggregation_validator.validate_aggregation_consistency(
                source_candles=candles_1m,
                target_candles=candles_5m,
                test_name="real_aggregation_validation"
            )
            
            logger.info(f"✅ Aggregation validation completed")
            logger.info(f"   Status: {result.status.value}")
            logger.info(f"   Message: {result.message}")
            if result.details:
                logger.info(f"   Details: {result.details}")
            
            return result.status == ValidationStatus.PASS
            
        except Exception as e:
            logger.error(f"❌ Aggregation validation failed: {e}")
            return False
    
    async def run_comprehensive_test(self):
        """Run comprehensive validation test with real data"""
        try:
            logger.info("🚀 Starting comprehensive real validation test...")
            
            # Create validation report
            report = ValidationReport(
                report_id=f"real_validation_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                start_time=datetime.now(timezone.utc),
                end_time=datetime.now(timezone.utc)
            )
            
            # Test Binance data retrieval
            binance_result = await self.test_binance_data_retrieval()
            report.add_result(ValidationResult(
                test_name="binance_data_retrieval",
                status=ValidationStatus.PASS if binance_result else ValidationStatus.FAIL,
                message="Binance data retrieval test",
                details={"success": binance_result}
            ))
            
            # Test Tardis data retrieval
            tardis_result = await self.test_tardis_data_retrieval()
            report.add_result(ValidationResult(
                test_name="tardis_data_retrieval",
                status=ValidationStatus.PASS if tardis_result else ValidationStatus.WARNING,
                message="Tardis data retrieval test",
                details={"success": tardis_result}
            ))
            
            # Test cross-source validation
            if binance_result:
                cross_source_result = await self.test_cross_source_validation()
                report.add_result(ValidationResult(
                    test_name="cross_source_validation",
                    status=ValidationStatus.PASS if cross_source_result else ValidationStatus.FAIL,
                    message="Cross-source validation test",
                    details={"success": cross_source_result}
                ))
            
            # Test timestamp validation
            if binance_result:
                timestamp_result = await self.test_timestamp_validation()
                report.add_result(ValidationResult(
                    test_name="timestamp_validation",
                    status=ValidationStatus.PASS if timestamp_result else ValidationStatus.FAIL,
                    message="Timestamp validation test",
                    details={"success": timestamp_result}
                ))
            
            # Test aggregation validation
            if binance_result:
                aggregation_result = await self.test_aggregation_validation()
                report.add_result(ValidationResult(
                    test_name="aggregation_validation",
                    status=ValidationStatus.PASS if aggregation_result else ValidationStatus.FAIL,
                    message="Aggregation validation test",
                    details={"success": aggregation_result}
                ))
            
            report.end_time = datetime.now(timezone.utc)
            
            # Print results
            logger.info("📊 Real Validation Test Results:")
            logger.info(f"   Total Tests: {report.total_tests}")
            logger.info(f"   Passed: {report.passed_tests}")
            logger.info(f"   Failed: {report.failed_tests}")
            logger.info(f"   Warnings: {report.warning_tests}")
            logger.info(f"   Success Rate: {report.get_success_rate():.1f}%")
            
            return report
            
        except Exception as e:
            logger.error(f"❌ Comprehensive test failed: {e}")
            return None


async def main():
    """Main test runner"""
    print("🧪 Real Validation Framework Test")
    print("=" * 50)
    print("This test uses actual Tardis API, Google Cloud Storage, and Binance data")
    print("Make sure you have:")
    print("1. Valid Tardis API key in TARDIS_API_KEY environment variable")
    print("2. Valid Binance API keys in BINANCE_API_KEY and BINANCE_SECRET_KEY")
    print("3. Valid GCS bucket name in GCS_BUCKET environment variable")
    print("4. Valid Google Cloud credentials file")
    print("=" * 50)
    
    tester = RealValidationTester()
    
    # Setup services
    if not await tester.setup():
        print("❌ Failed to setup services. Please check your configuration.")
        return 1
    
    # Run comprehensive test
    report = await tester.run_comprehensive_test()
    
    if report:
        if report.get_success_rate() >= 80:
            print("🎉 Real validation test completed successfully!")
            return 0
        else:
            print("⚠️ Real validation test completed with some issues.")
            return 1
    else:
        print("❌ Real validation test failed!")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)