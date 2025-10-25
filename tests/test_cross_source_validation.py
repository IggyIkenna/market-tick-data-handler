"""
Cross-Source Validation Tests

Tests for validating data consistency between Binance CCXT and Tardis-derived candles.
Implements comprehensive validation using the three rules system.
"""

import pytest
import asyncio
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock, AsyncMock, patch
import pandas as pd

from market_data_tick_handler.validation.cross_source_validator import CrossSourceValidator, OHLCV
from market_data_tick_handler.validation.validation_results import ValidationStatus
from market_data_tick_handler.data_client.data_client import DataClient
from market_data_tick_handler.data_downloader.tardis_connector import TardisConnector


class TestCrossSourceValidation:
    """Test cross-source data validation"""
    
    @pytest.fixture
    def mock_data_client(self):
        """Mock data client"""
        client = Mock(spec=DataClient)
        client.gcs_bucket = "test-bucket"
        return client
    
    @pytest.fixture
    def mock_tardis_connector(self):
        """Mock Tardis connector"""
        connector = Mock(spec=TardisConnector)
        return connector
    
    @pytest.fixture
    def validator(self, mock_data_client, mock_tardis_connector):
        """Create validator instance"""
        return CrossSourceValidator(mock_data_client, mock_tardis_connector)
    
    @pytest.fixture
    def sample_binance_candles(self):
        """Sample Binance candles for testing"""
        base_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        candles = []
        
        for i in range(10):
            timestamp = base_time + timedelta(minutes=i)
            candles.append(OHLCV(
                timestamp=timestamp,
                open=100.0 + i,
                high=105.0 + i,
                low=95.0 + i,
                close=102.0 + i,
                volume=1000.0 + i * 100,
                symbol="BTC-USDT",
                timeframe="1m",
                source="binance"
            ))
        
        return candles
    
    @pytest.fixture
    def sample_tardis_candles(self):
        """Sample Tardis candles for testing"""
        base_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        candles = []
        
        for i in range(10):
            timestamp = base_time + timedelta(minutes=i)
            candles.append(OHLCV(
                timestamp=timestamp,
                open=100.0 + i,
                high=105.0 + i,
                low=95.0 + i,
                close=102.0 + i,
                volume=1000.0 + i * 100,
                symbol="BTC-USDT",
                timeframe="1m",
                source="tardis"
            ))
        
        return candles
    
    @pytest.mark.asyncio
    async def test_timeframe_consistency_success(self, validator, sample_binance_candles, sample_tardis_candles):
        """Test successful timeframe consistency validation"""
        # Mock the data retrieval methods
        validator._get_binance_candles = AsyncMock(return_value=sample_binance_candles)
        validator._get_tardis_candles = AsyncMock(return_value=sample_tardis_candles)
        
        start_date = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 15, 12, 10, 0, tzinfo=timezone.utc)
        
        result = await validator.validate_timeframe_consistency(
            symbol="BTC-USDT",
            timeframe="1m",
            start_date=start_date,
            end_date=end_date
        )
        
        assert result.status == ValidationStatus.PASS
        assert "All three rules passed" in result.message
        assert result.details['aligned_candles'] == 10
        assert result.details['binance_candles'] == 10
        assert result.details['tardis_candles'] == 10
    
    @pytest.mark.asyncio
    async def test_timeframe_consistency_ohlc_mismatch(self, validator):
        """Test timeframe consistency with OHLC mismatch"""
        # Create candles with OHLC mismatch
        base_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        
        binance_candles = [OHLCV(
            timestamp=base_time,
            open=100.0,
            high=105.0,
            low=95.0,
            close=102.0,
            volume=1000.0,
            symbol="BTC-USDT",
            timeframe="1m",
            source="binance"
        )]
        
        tardis_candles = [OHLCV(
            timestamp=base_time,
            open=200.0,  # Different open price
            high=205.0,  # Different high price
            low=195.0,   # Different low price
            close=202.0, # Different close price
            volume=1000.0,
            symbol="BTC-USDT",
            timeframe="1m",
            source="tardis"
        )]
        
        validator._get_binance_candles = AsyncMock(return_value=binance_candles)
        validator._get_tardis_candles = AsyncMock(return_value=tardis_candles)
        
        start_date = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 15, 12, 1, 0, tzinfo=timezone.utc)
        
        result = await validator.validate_timeframe_consistency(
            symbol="BTC-USDT",
            timeframe="1m",
            start_date=start_date,
            end_date=end_date
        )
        
        assert result.status == ValidationStatus.FAIL
        assert "Rule 1 passed, Rules 2&3 failed" in result.message
        assert result.details['validation_results']['rule_1_passed'] == True
        assert result.details['validation_results']['rule_2_passed'] == False
        assert result.details['validation_results']['rule_3_passed'] == True
    
    @pytest.mark.asyncio
    async def test_timeframe_consistency_no_data(self, validator):
        """Test timeframe consistency with no data"""
        validator._get_binance_candles = AsyncMock(return_value=[])
        validator._get_tardis_candles = AsyncMock(return_value=[])
        
        start_date = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 15, 12, 1, 0, tzinfo=timezone.utc)
        
        result = await validator.validate_timeframe_consistency(
            symbol="BTC-USDT",
            timeframe="1m",
            start_date=start_date,
            end_date=end_date
        )
        
        assert result.status == ValidationStatus.FAIL
        assert "No data available from one or both sources" in result.message
    
    @pytest.mark.asyncio
    async def test_aggregation_consistency_success(self, validator):
        """Test successful aggregation consistency validation"""
        # Create 1m candles
        base_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        minute_candles = []
        
        for i in range(5):
            timestamp = base_time + timedelta(minutes=i)
            minute_candles.append(OHLCV(
                timestamp=timestamp,
                open=100.0 + i,
                high=105.0 + i,
                low=95.0 + i,
                close=102.0 + i,
                volume=1000.0 + i * 100,
                symbol="BTC-USDT",
                timeframe="1m",
                source="binance"
            ))
        
        # Create 5m aggregated candle
        five_min_candle = OHLCV(
            timestamp=base_time,
            open=100.0,
            high=109.0,  # Max of all highs
            low=95.0,    # Min of all lows
            close=104.0, # Last close
            volume=6000.0, # Sum of all volumes
            symbol="BTC-USDT",
            timeframe="5m",
            source="binance"
        )
        
        validator._get_binance_candles = AsyncMock(side_effect=[minute_candles, [five_min_candle]])
        validator._get_tardis_candles = AsyncMock(side_effect=[minute_candles, [five_min_candle]])
        
        start_date = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 15, 12, 5, 0, tzinfo=timezone.utc)
        
        result = await validator.validate_aggregation_consistency(
            symbol="BTC-USDT",
            base_timeframe="1m",
            aggregated_timeframe="5m",
            start_date=start_date,
            end_date=end_date
        )
        
        assert result.status == ValidationStatus.PASS
        assert "Aggregation consistency validated" in result.message
    
    @pytest.mark.asyncio
    async def test_timestamp_alignment_success(self, validator, sample_binance_candles, sample_tardis_candles):
        """Test successful timestamp alignment validation"""
        validator._get_binance_candles = AsyncMock(return_value=sample_binance_candles)
        validator._get_tardis_candles = AsyncMock(return_value=sample_tardis_candles)
        
        start_date = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 15, 12, 10, 0, tzinfo=timezone.utc)
        
        result = await validator.validate_timestamp_alignment(
            symbol="BTC-USDT",
            timeframe="1m",
            start_date=start_date,
            end_date=end_date
        )
        
        assert result.status == ValidationStatus.PASS
        assert "Timestamps properly aligned" in result.message
        assert result.details['aligned'] == True
        assert result.details['overlap_ratio'] >= 0.8
    
    def test_align_candles(self, validator, sample_binance_candles, sample_tardis_candles):
        """Test candle alignment functionality"""
        aligned = validator._align_candles(sample_binance_candles, sample_tardis_candles)
        
        assert len(aligned) == 10
        assert all('timestamp' in item for item in aligned)
        assert all('binance' in item for item in aligned)
        assert all('tardis' in item for item in aligned)
    
    def test_apply_three_rules_success(self, validator, sample_binance_candles, sample_tardis_candles):
        """Test three rules validation with matching data"""
        aligned = validator._align_candles(sample_binance_candles, sample_tardis_candles)
        results = validator._apply_three_rules(aligned)
        
        assert results['rule_1_passed'] == True
        assert results['rule_2_passed'] == True
        assert results['rule_3_passed'] == True
        assert results['rule_1_details']['aligned_candles'] == 10
    
    def test_apply_three_rules_ohlc_failure(self, validator):
        """Test three rules validation with OHLC mismatch"""
        base_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        
        binance_candle = OHLCV(
            timestamp=base_time,
            open=100.0,
            high=105.0,
            low=95.0,
            close=102.0,
            volume=1000.0,
            symbol="BTC-USDT",
            timeframe="1m",
            source="binance"
        )
        
        tardis_candle = OHLCV(
            timestamp=base_time,
            open=200.0,  # 100% difference - should fail
            high=205.0,
            low=195.0,
            close=202.0,
            volume=1000.0,
            symbol="BTC-USDT",
            timeframe="1m",
            source="tardis"
        )
        
        aligned = [{'timestamp': base_time, 'binance': binance_candle, 'tardis': tardis_candle}]
        results = validator._apply_three_rules(aligned)
        
        assert results['rule_1_passed'] == True
        assert results['rule_2_passed'] == False
        assert results['rule_3_passed'] == True
        assert len(results['rule_2_details']['ohlc_errors']) > 0
    
    def test_aggregate_candles(self, validator):
        """Test candle aggregation functionality"""
        base_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        minute_candles = []
        
        for i in range(5):
            timestamp = base_time + timedelta(minutes=i)
            minute_candles.append(OHLCV(
                timestamp=timestamp,
                open=100.0 + i,
                high=105.0 + i,
                low=95.0 + i,
                close=102.0 + i,
                volume=1000.0 + i * 100,
                symbol="BTC-USDT",
                timeframe="1m",
                source="binance"
            ))
        
        aggregated = validator._aggregate_candles(minute_candles, "5m")
        
        assert len(aggregated) == 1
        assert aggregated[0].open == 100.0  # First candle's open
        assert aggregated[0].high == 109.0  # Max of all highs
        assert aggregated[0].low == 95.0    # Min of all lows
        assert aggregated[0].close == 104.0 # Last candle's close
        assert aggregated[0].volume == 6000.0  # Sum of all volumes
        assert aggregated[0].timeframe == "5m"
    
    def test_get_timeframe_boundary(self, validator):
        """Test timeframe boundary calculation"""
        timestamp = datetime(2024, 1, 15, 12, 3, 45, tzinfo=timezone.utc)
        
        # Test 1m boundary
        boundary_1m = validator._get_timeframe_boundary(timestamp, "1m")
        expected_1m = datetime(2024, 1, 15, 12, 3, 0, tzinfo=timezone.utc)
        assert boundary_1m == expected_1m
        
        # Test 5m boundary
        boundary_5m = validator._get_timeframe_boundary(timestamp, "5m")
        expected_5m = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        assert boundary_5m == expected_5m
        
        # Test 15s boundary
        boundary_15s = validator._get_timeframe_boundary(timestamp, "15s")
        expected_15s = datetime(2024, 1, 15, 12, 3, 30, tzinfo=timezone.utc)
        assert boundary_15s == expected_15s


@pytest.mark.integration
class TestCrossSourceValidationIntegration:
    """Integration tests for cross-source validation"""
    
    @pytest.mark.asyncio
    async def test_full_validation_workflow(self):
        """Test complete validation workflow"""
        # This would test the full integration with real data sources
        # For now, it's marked as integration test and skipped in unit tests
        pytest.skip("Integration test - requires real data sources")
    
    @pytest.mark.asyncio
    async def test_validation_with_real_binance_data(self):
        """Test validation with real Binance data"""
        # This would test with actual Binance API calls
        pytest.skip("Integration test - requires real API access")
