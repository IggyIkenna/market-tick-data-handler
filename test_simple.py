#!/usr/bin/env python3
"""
Simple test to verify basic functionality
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Set environment variables
os.environ['TARDIS_API_KEY'] = 'TD.dummy_key'
os.environ['GCP_PROJECT_ID'] = 'dummy_project'
os.environ['GCS_BUCKET'] = 'dummy_bucket'
os.environ['GCP_CREDENTIALS_PATH'] = '/tmp/dummy.json'
os.environ['USE_SECRET_MANAGER'] = 'false'

def test_validation_results():
    """Test validation results import and basic functionality"""
    try:
        from src.validation.validation_results import ValidationStatus, ValidationResult
        print("✅ ValidationStatus and ValidationResult imported successfully")
        
        # Test ValidationStatus enum
        assert ValidationStatus.PASS.value == "PASS"
        assert ValidationStatus.FAIL.value == "FAIL"
        assert ValidationStatus.WARNING.value == "WARNING"
        assert ValidationStatus.SKIP.value == "SKIP"
        print("✅ ValidationStatus enum values are correct")
        
        # Test ValidationResult creation
        result = ValidationResult(
            test_name="test_validation",
            status=ValidationStatus.PASS,
            message="Test passed",
            details={"test": "value"}
        )
        assert result.test_name == "test_validation"
        assert result.status == ValidationStatus.PASS
        assert result.message == "Test passed"
        print("✅ ValidationResult creation works correctly")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

def test_timestamp_validator():
    """Test timestamp validator import and basic functionality"""
    try:
        from src.validation.timestamp_validator import TimestampValidator, TimestampValidationConfig
        print("✅ TimestampValidator imported successfully")
        
        # Test TimestampValidationConfig
        config = TimestampValidationConfig()
        assert config.min_data_freshness_seconds == 300.0
        assert config.max_timestamp_drift_seconds == 1.0
        print("✅ TimestampValidationConfig works correctly")
        
        # Test TimestampValidator creation
        validator = TimestampValidator()
        assert validator is not None
        print("✅ TimestampValidator creation works correctly")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

def test_aggregation_validator():
    """Test aggregation validator import and basic functionality"""
    try:
        from src.validation.aggregation_validator import AggregationValidator
        print("✅ AggregationValidator imported successfully")
        
        # Test AggregationValidator creation
        validator = AggregationValidator()
        assert validator is not None
        print("✅ AggregationValidator creation works correctly")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

def test_cross_source_validator():
    """Test cross source validator import and basic functionality"""
    try:
        from src.validation.cross_source_validator import CrossSourceValidator, OHLCV
        print("✅ CrossSourceValidator imported successfully")
        
        # Test OHLCV dataclass
        from datetime import datetime, timezone
        candle = OHLCV(
            timestamp=datetime.now(timezone.utc),
            open=50000.0,
            high=51000.0,
            low=49000.0,
            close=50500.0,
            volume=100.0,
            symbol="BTC-USDT",
            timeframe="1m",
            source="test"
        )
        assert candle.symbol == "BTC-USDT"
        assert candle.timeframe == "1m"
        print("✅ OHLCV dataclass works correctly")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

def main():
    """Run all tests"""
    print("🧪 Running simple validation tests...")
    print("=" * 50)
    
    tests = [
        test_validation_results,
        test_timestamp_validator,
        test_aggregation_validator,
        test_cross_source_validator
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        print(f"\n🔍 Running {test.__name__}...")
        if test():
            passed += 1
            print(f"✅ {test.__name__} passed")
        else:
            print(f"❌ {test.__name__} failed")
    
    print("\n" + "=" * 50)
    print(f"📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed!")
        return 0
    else:
        print("💥 Some tests failed!")
        return 1

if __name__ == "__main__":
    exit(main())