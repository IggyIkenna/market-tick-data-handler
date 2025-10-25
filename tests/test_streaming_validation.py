"""
Tests for streaming validation components.

Tests the integration between the validation framework and the unified streaming architecture.
"""

import pytest
import asyncio
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock, AsyncMock, patch
import pandas as pd

from market_data_tick_handler.validation.streaming_validator import StreamingValidator, StreamingValidationConfig
from market_data_tick_handler.validation.streaming_integration import StreamingServiceIntegration, StreamingIntegrationConfig
from market_data_tick_handler.validation.validation_results import ValidationResult, ValidationStatus, ValidationReport
from market_data_tick_handler.validation.cross_source_validator import OHLCV


class TestStreamingValidator:
    """Test the StreamingValidator class"""
    
    @pytest.fixture
    def mock_validators(self):
        """Create mock validators"""
        cross_source = Mock()
        timestamp = Mock()
        aggregation = Mock()
        return cross_source, timestamp, aggregation
    
    @pytest.fixture
    def streaming_validator(self, mock_validators):
        """Create StreamingValidator instance"""
        cross_source, timestamp, aggregation = mock_validators
        config = StreamingValidationConfig(
            validation_interval_seconds=1,  # Short interval for testing
            max_candles_per_validation=10
        )
        return StreamingValidator(cross_source, timestamp, aggregation, config)
    
    @pytest.fixture
    def sample_candle(self):
        """Create a sample OHLCV candle"""
        return OHLCV(
            timestamp=datetime.utcnow(),
            open=50000.0,
            high=51000.0,
            low=49000.0,
            close=50500.0,
            volume=100.0,
            symbol="BTC-USDT",
            timeframe="1m",
            source="streaming"
        )
    
    @pytest.mark.asyncio
    async def test_validate_streaming_candle_success(self, streaming_validator, sample_candle):
        """Test successful validation of a streaming candle"""
        result = await streaming_validator.validate_streaming_candle(
            sample_candle, "BTC-USDT", "1m"
        )
        
        assert result.status == ValidationStatus.PASS
        assert result.test_name == "streaming_candle_validation"
        assert "Streaming candle validation passed" in result.message
    
    @pytest.mark.asyncio
    async def test_validate_streaming_candle_timestamp_freshness(self, streaming_validator):
        """Test validation with old timestamp"""
        old_candle = OHLCV(
            timestamp=datetime.utcnow() - timedelta(hours=2),
            open=50000.0,
            high=51000.0,
            low=49000.0,
            close=50500.0,
            volume=100.0,
            symbol="BTC-USDT",
            timeframe="1m",
            source="streaming"
        )
        
        result = await streaming_validator.validate_streaming_candle(
            old_candle, "BTC-USDT", "1m"
        )
        
        assert result.status == ValidationStatus.WARNING
        assert "Timestamp is" in result.message
        assert "seconds old" in result.message
    
    @pytest.mark.asyncio
    async def test_validate_streaming_candle_timezone_error(self, streaming_validator):
        """Test validation with non-UTC timezone"""
        non_utc_candle = OHLCV(
            timestamp=datetime.now(),  # No timezone info
            open=50000.0,
            high=51000.0,
            low=49000.0,
            close=50500.0,
            volume=100.0,
            symbol="BTC-USDT",
            timeframe="1m",
            source="streaming"
        )
        
        result = await streaming_validator.validate_streaming_candle(
            non_utc_candle, "BTC-USDT", "1m"
        )
        
        assert result.status == ValidationStatus.FAIL
        assert "not in UTC timezone" in result.message
    
    @pytest.mark.asyncio
    async def test_validate_streaming_candle_exception(self, streaming_validator):
        """Test validation with exception"""
        # Create invalid candle to trigger exception
        invalid_candle = Mock()
        invalid_candle.timestamp = "invalid_timestamp"  # This will cause an error
        
        result = await streaming_validator.validate_streaming_candle(
            invalid_candle, "BTC-USDT", "1m"
        )
        
        assert result.status == ValidationStatus.FAIL
        assert "Streaming candle validation failed" in result.message
    
    @pytest.mark.asyncio
    async def test_validate_streaming_service_output(self, streaming_validator, sample_candle):
        """Test validation of streaming service output"""
        candles = [sample_candle]
        
        result = await streaming_validator.validate_streaming_service_output(
            candles, "BTC-USDT", "1m"
        )
        
        assert isinstance(result, ValidationReport)
        assert result.total_tests > 0
    
    @pytest.mark.asyncio
    async def test_validate_streaming_service_output_empty(self, streaming_validator):
        """Test validation with empty candle list"""
        result = await streaming_validator.validate_streaming_service_output(
            [], "BTC-USDT", "1m"
        )
        
        assert isinstance(result, ValidationReport)
        assert result.failed_tests > 0
    
    def test_get_interval_seconds(self, streaming_validator):
        """Test interval seconds calculation"""
        assert streaming_validator._get_interval_seconds("1m") == 60.0
        assert streaming_validator._get_interval_seconds("5m") == 300.0
        assert streaming_validator._get_interval_seconds("1h") == 3600.0
        assert streaming_validator._get_interval_seconds("unknown") == 60.0
    
    def test_clear_candle_buffer(self, streaming_validator, sample_candle):
        """Test clearing candle buffer"""
        # Add some candles
        streaming_validator.candle_buffer["BTC-USDT_1m"] = [sample_candle]
        
        # Clear specific symbol/timeframe
        streaming_validator.clear_candle_buffer("BTC-USDT", "1m")
        assert "BTC-USDT_1m" not in streaming_validator.candle_buffer
        
        # Add candles again
        streaming_validator.candle_buffer["BTC-USDT_1m"] = [sample_candle]
        streaming_validator.candle_buffer["ETH-USDT_1m"] = [sample_candle]
        
        # Clear all
        streaming_validator.clear_candle_buffer()
        assert len(streaming_validator.candle_buffer) == 0
    
    def test_get_validation_stats(self, streaming_validator):
        """Test getting validation statistics"""
        stats = streaming_validator.get_validation_stats()
        
        assert "total_validations" in stats
        assert "passed_validations" in stats
        assert "failed_validations" in stats
        assert "success_rate" in stats
        assert stats["success_rate"] == 0.0  # No validations yet


class TestStreamingServiceIntegration:
    """Test the StreamingServiceIntegration class"""
    
    @pytest.fixture
    def mock_streaming_validator(self):
        """Create mock streaming validator"""
        validator = Mock()
        validator.validate_streaming_candle = AsyncMock()
        validator.validate_streaming_service_output = AsyncMock()
        return validator
    
    @pytest.fixture
    def integration(self, mock_streaming_validator):
        """Create StreamingServiceIntegration instance"""
        config = StreamingIntegrationConfig(
            enable_async_validation=False,  # Disable for testing
            validation_interval_seconds=1
        )
        return StreamingServiceIntegration(mock_streaming_validator, config)
    
    @pytest.fixture
    def sample_candle(self):
        """Create a sample OHLCV candle"""
        return OHLCV(
            timestamp=datetime.utcnow(),
            open=50000.0,
            high=51000.0,
            low=49000.0,
            close=50500.0,
            volume=100.0,
            symbol="BTC-USDT",
            timeframe="1m",
            source="streaming"
        )
    
    @pytest.mark.asyncio
    async def test_start_stop(self, integration):
        """Test starting and stopping the integration"""
        assert not integration.is_running
        
        await integration.start()
        assert integration.is_running
        
        await integration.stop()
        assert not integration.is_running
    
    @pytest.mark.asyncio
    async def test_validate_candle_success(self, integration, sample_candle):
        """Test successful candle validation"""
        # Mock successful validation
        integration.validator.validate_streaming_candle.return_value = ValidationResult(
            test_name="test",
            status=ValidationStatus.PASS,
            message="Success"
        )
        
        result = await integration.validate_candle(sample_candle, "BTC-USDT", "1m")
        
        assert result.status == ValidationStatus.PASS
        assert integration.stats["total_candles_validated"] == 1
        assert integration.stats["successful_validations"] == 1
    
    @pytest.mark.asyncio
    async def test_validate_candle_failure(self, integration, sample_candle):
        """Test candle validation failure"""
        # Mock validation failure
        integration.validator.validate_streaming_candle.return_value = ValidationResult(
            test_name="test",
            status=ValidationStatus.FAIL,
            message="Failure"
        )
        
        result = await integration.validate_candle(sample_candle, "BTC-USDT", "1m")
        
        assert result.status == ValidationStatus.FAIL
        assert integration.stats["total_candles_validated"] == 1
        assert integration.stats["failed_validations"] == 1
    
    @pytest.mark.asyncio
    async def test_validate_candle_exception(self, integration, sample_candle):
        """Test candle validation with exception"""
        # Mock exception
        integration.validator.validate_streaming_candle.side_effect = Exception("Test error")
        
        result = await integration.validate_candle(sample_candle, "BTC-USDT", "1m")
        
        assert result.status == ValidationStatus.FAIL
        assert "Test error" in result.message
        assert integration.consecutive_failures == 1
    
    @pytest.mark.asyncio
    async def test_validate_candle_batch(self, integration, sample_candle):
        """Test batch validation"""
        candles = [sample_candle]
        
        # Mock successful validation
        mock_report = ValidationReport(
            report_id="test",
            start_time=datetime.utcnow(),
            end_time=datetime.utcnow()
        )
        integration.validator.validate_streaming_service_output.return_value = mock_report
        
        result = await integration.validate_candle_batch(candles, "BTC-USDT", "1m")
        
        assert isinstance(result, ValidationReport)
        integration.validator.validate_streaming_service_output.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_skip_validation_after_failures(self, integration, sample_candle):
        """Test that validation is skipped after consecutive failures"""
        # Set up failure state
        integration.consecutive_failures = 10
        integration.last_failure_time = datetime.utcnow()
        
        result = await integration.validate_candle(sample_candle, "BTC-USDT", "1m")
        
        assert result.status == ValidationStatus.SKIP
        assert "skipped due to recent failures" in result.message
        integration.validator.validate_streaming_candle.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_validation_callback(self, integration, sample_candle):
        """Test validation callback"""
        callback_called = False
        
        def callback(result):
            nonlocal callback_called
            callback_called = True
        
        integration.add_validation_callback(callback)
        
        # Mock successful validation
        integration.validator.validate_streaming_candle.return_value = ValidationResult(
            test_name="test",
            status=ValidationStatus.PASS,
            message="Success"
        )
        
        await integration.validate_candle(sample_candle, "BTC-USDT", "1m")
        
        assert callback_called
    
    @pytest.mark.asyncio
    async def test_error_callback(self, integration, sample_candle):
        """Test error callback"""
        callback_called = False
        error_received = None
        failure_count = 0
        
        def error_callback(error, count):
            nonlocal callback_called, error_received, failure_count
            callback_called = True
            error_received = error
            failure_count = count
        
        integration.add_error_callback(error_callback)
        
        # Mock exception
        integration.validator.validate_streaming_candle.side_effect = Exception("Test error")
        
        await integration.validate_candle(sample_candle, "BTC-USDT", "1m")
        
        assert callback_called
        assert str(error_received) == "Test error"
        assert failure_count == 1
    
    def test_get_stats(self, integration):
        """Test getting integration statistics"""
        stats = integration.get_stats()
        
        assert "total_candles_validated" in stats
        assert "successful_validations" in stats
        assert "failed_validations" in stats
        assert "success_rate" in stats
        assert "consecutive_failures" in stats
        assert "is_running" in stats
    
    def test_reset_stats(self, integration):
        """Test resetting statistics"""
        # Set some stats
        integration.stats["total_candles_validated"] = 100
        integration.consecutive_failures = 5
        
        integration.reset_stats()
        
        assert integration.stats["total_candles_validated"] == 0
        assert integration.consecutive_failures == 0


class TestStreamingValidationIntegration:
    """Integration tests for streaming validation"""
    
    @pytest.mark.asyncio
    async def test_end_to_end_validation(self):
        """Test end-to-end validation flow"""
        # This would be a more comprehensive integration test
        # that tests the full flow from streaming service to validation
        pass
    
    @pytest.mark.asyncio
    async def test_performance_under_load(self):
        """Test validation performance under load"""
        # This would test validation performance with many concurrent candles
        pass
    
    @pytest.mark.asyncio
    async def test_error_recovery(self):
        """Test error recovery mechanisms"""
        # This would test how the system recovers from various error conditions
        pass


@pytest.mark.integration
class TestStreamingValidationIntegrationReal:
    """Real integration tests (marked as integration)"""
    
    @pytest.mark.asyncio
    async def test_real_streaming_validation(self):
        """Test with real streaming data"""
        # This would test with actual streaming data
        # Requires real streaming service to be running
        pass
    
    @pytest.mark.asyncio
    async def test_real_cross_source_validation(self):
        """Test real cross-source validation"""
        # This would test with real Binance data
        # Requires API keys and network access
        pass
