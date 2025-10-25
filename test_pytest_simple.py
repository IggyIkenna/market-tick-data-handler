#!/usr/bin/env python3
"""
Simple pytest tests for validation framework
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

import pytest
from datetime import datetime, timezone, timedelta

def test_validation_status_enum():
    """Test ValidationStatus enum values"""
    from src.validation.validation_results import ValidationStatus
    
    assert ValidationStatus.PASS.value == "PASS"
    assert ValidationStatus.FAIL.value == "FAIL"
    assert ValidationStatus.WARNING.value == "WARNING"
    assert ValidationStatus.SKIP.value == "SKIP"

def test_validation_result_creation():
    """Test ValidationResult creation"""
    from src.validation.validation_results import ValidationResult, ValidationStatus
    
    result = ValidationResult(
        test_name="test_validation",
        status=ValidationStatus.PASS,
        message="Test passed",
        details={"test": "value"}
    )
    
    assert result.test_name == "test_validation"
    assert result.status == ValidationStatus.PASS
    assert result.message == "Test passed"
    assert result.details == {"test": "value"}

def test_validation_report():
    """Test ValidationReport functionality"""
    from src.validation.validation_results import ValidationReport, ValidationResult, ValidationStatus
    
    report = ValidationReport(
        report_id="test_report",
        start_time=datetime.now(timezone.utc),
        end_time=datetime.now(timezone.utc)
    )
    
    result = ValidationResult(
        test_name="test_validation",
        status=ValidationStatus.PASS,
        message="Test passed"
    )
    
    report.add_result(result)
    
    assert report.total_tests == 1
    assert report.passed_tests == 1
    assert report.failed_tests == 0
    assert report.get_success_rate() == 100.0

def test_timestamp_validator_config():
    """Test TimestampValidationConfig"""
    from src.validation.timestamp_validator import TimestampValidationConfig
    
    config = TimestampValidationConfig()
    
    assert config.min_data_freshness_seconds == 300.0
    assert config.max_timestamp_drift_seconds == 1.0
    assert config.duplicate_timestamp_tolerance == 0.001

def test_timestamp_validator_creation():
    """Test TimestampValidator creation"""
    from src.validation.timestamp_validator import TimestampValidator
    
    validator = TimestampValidator()
    assert validator is not None

def test_ohlcv_dataclass():
    """Test OHLCV dataclass"""
    from src.validation.cross_source_validator import OHLCV
    
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
    assert candle.open == 50000.0
    assert candle.high == 51000.0
    assert candle.low == 49000.0
    assert candle.close == 50500.0
    assert candle.volume == 100.0

def test_aggregation_validator_creation():
    """Test AggregationValidator creation"""
    from src.validation.aggregation_validator import AggregationValidator
    
    validator = AggregationValidator()
    assert validator is not None

def test_streaming_validator_config():
    """Test StreamingValidationConfig"""
    from src.validation.streaming_validator import StreamingValidationConfig
    
    config = StreamingValidationConfig()
    
    assert config.validation_interval_seconds == 60
    assert config.max_candles_per_validation == 100
    assert config.price_tolerance_pct == 0.01
    assert config.volume_tolerance_pct == 0.05

def test_streaming_validator_creation():
    """Test StreamingValidator creation"""
    from src.validation.streaming_validator import StreamingValidator, StreamingValidationConfig
    from src.validation.cross_source_validator import CrossSourceValidator
    from src.validation.timestamp_validator import TimestampValidator
    from src.validation.aggregation_validator import AggregationValidator
    from src.data_downloader.data_client import DataClient
    from src.data_downloader.tardis_connector import TardisConnector
    from unittest.mock import Mock
    
    # Create mock validators
    data_client = Mock(spec=DataClient)
    tardis_connector = Mock(spec=TardisConnector)
    cross_source_validator = CrossSourceValidator(data_client, tardis_connector)
    timestamp_validator = TimestampValidator()
    aggregation_validator = AggregationValidator()
    
    # Create streaming validator
    config = StreamingValidationConfig()
    validator = StreamingValidator(
        cross_source_validator=cross_source_validator,
        timestamp_validator=timestamp_validator,
        aggregation_validator=aggregation_validator,
        config=config
    )
    
    assert validator is not None
    assert validator.config.validation_interval_seconds == 60