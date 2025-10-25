"""
Aggregation Validation Tests

Tests for aggregation consistency and correctness validation.
"""

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock

from market_data_tick_handler.validation.aggregation_validator import AggregationValidator, AggregationValidationConfig
from market_data_tick_handler.validation.cross_source_validator import OHLCV
from market_data_tick_handler.validation.validation_results import ValidationStatus


class TestAggregationValidator:
    """Test aggregation validation functionality"""
    
    @pytest.fixture
    def validator(self):
        """Create validator instance"""
        config = AggregationValidationConfig(
            price_tolerance=0.0001,
            volume_tolerance=0.01,
            timestamp_tolerance_seconds=1.0
        )
        return AggregationValidator(config)
    
    @pytest.fixture
    def minute_candles(self):
        """Sample 1-minute candles for aggregation testing"""
        base_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        candles = []
        
        for i in range(5):
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
    def five_minute_candle(self):
        """Correctly aggregated 5-minute candle"""
        base_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        return OHLCV(
            timestamp=base_time,
            open=100.0,  # First candle's open
            high=109.0,  # Max of all highs (105 + 4)
            low=95.0,    # Min of all lows
            close=104.0, # Last candle's close (102 + 4)
            volume=6000.0,  # Sum of all volumes
            symbol="BTC-USDT",
            timeframe="5m",
            source="binance"
        )
    
    @pytest.fixture
    def incorrect_five_minute_candle(self):
        """Incorrectly aggregated 5-minute candle"""
        base_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        return OHLCV(
            timestamp=base_time,
            open=200.0,  # Wrong open
            high=300.0,  # Wrong high
            low=50.0,    # Wrong low
            close=250.0, # Wrong close
            volume=10000.0,  # Wrong volume
            symbol="BTC-USDT",
            timeframe="5m",
            source="binance"
        )
    
    def test_ohlc_preservation_success(self, validator, minute_candles, five_minute_candle):
        """Test successful OHLC preservation validation"""
        result = validator.validate_ohlc_preservation(
            base_candles=minute_candles,
            aggregated_candles=[five_minute_candle],
            test_name="ohlc_preservation_success"
        )
        
        assert result.status == ValidationStatus.PASS
        assert "OHLC preservation validated" in result.message
        assert result.details['total_groups'] == 1
        assert result.details['error_count'] == 0
    
    def test_ohlc_preservation_failure(self, validator, minute_candles, incorrect_five_minute_candle):
        """Test OHLC preservation validation with incorrect aggregation"""
        result = validator.validate_ohlc_preservation(
            base_candles=minute_candles,
            aggregated_candles=[incorrect_five_minute_candle],
            test_name="ohlc_preservation_failure"
        )
        
        assert result.status == ValidationStatus.FAIL
        assert "OHLC preservation errors detected" in result.message
        assert result.details['error_count'] > 0
        assert len(result.details['validation_errors']) > 0
    
    def test_ohlc_preservation_empty_data(self, validator):
        """Test OHLC preservation with empty data"""
        result = validator.validate_ohlc_preservation(
            base_candles=[],
            aggregated_candles=[],
            test_name="empty_data"
        )
        
        assert result.status == ValidationStatus.FAIL
        assert "Missing base or aggregated candles" in result.message
    
    def test_volume_aggregation_success(self, validator, minute_candles, five_minute_candle):
        """Test successful volume aggregation validation"""
        result = validator.validate_volume_aggregation(
            base_candles=minute_candles,
            aggregated_candles=[five_minute_candle],
            test_name="volume_aggregation_success"
        )
        
        assert result.status == ValidationStatus.PASS
        assert "Volume aggregation validated" in result.message
        assert result.details['error_count'] == 0
    
    def test_volume_aggregation_failure(self, validator, minute_candles, incorrect_five_minute_candle):
        """Test volume aggregation validation with incorrect volume"""
        result = validator.validate_volume_aggregation(
            base_candles=minute_candles,
            aggregated_candles=[incorrect_five_minute_candle],
            test_name="volume_aggregation_failure"
        )
        
        assert result.status == ValidationStatus.FAIL
        assert "Volume aggregation errors detected" in result.message
        assert result.details['error_count'] > 0
        assert len(result.details['volume_errors']) > 0
    
    def test_timeframe_boundaries_success(self, validator, five_minute_candle):
        """Test successful timeframe boundary validation"""
        result = validator.validate_timeframe_boundaries(
            candles=[five_minute_candle],
            expected_timeframe="5m",
            test_name="timeframe_boundaries_success"
        )
        
        assert result.status == ValidationStatus.PASS
        assert "Timeframe boundaries validated" in result.message
        assert result.details['error_count'] == 0
    
    def test_timeframe_boundaries_failure(self, validator):
        """Test timeframe boundary validation with misaligned timestamps"""
        # Create candle with misaligned timestamp
        misaligned_candle = OHLCV(
            timestamp=datetime(2024, 1, 15, 12, 0, 30, tzinfo=timezone.utc),  # 30 seconds offset
            open=100.0,
            high=105.0,
            low=95.0,
            close=102.0,
            volume=1000.0,
            symbol="BTC-USDT",
            timeframe="5m",
            source="binance"
        )
        
        result = validator.validate_timeframe_boundaries(
            candles=[misaligned_candle],
            expected_timeframe="5m",
            test_name="timeframe_boundaries_failure"
        )
        
        assert result.status == ValidationStatus.FAIL
        assert "Timeframe boundary misalignment detected" in result.message
        assert result.details['error_count'] > 0
    
    def test_aggregation_consistency_success(self, validator, minute_candles, five_minute_candle):
        """Test successful aggregation consistency validation"""
        result = validator.validate_aggregation_consistency(
            source_candles=minute_candles,
            target_candles=[five_minute_candle],
            test_name="aggregation_consistency_success"
        )
        
        assert result.status == ValidationStatus.PASS
        assert "Aggregation consistency validated" in result.message
        assert result.details['comparison']['passed'] == True
    
    def test_aggregation_consistency_failure(self, validator, minute_candles, incorrect_five_minute_candle):
        """Test aggregation consistency validation with incorrect aggregation"""
        result = validator.validate_aggregation_consistency(
            source_candles=minute_candles,
            target_candles=[incorrect_five_minute_candle],
            test_name="aggregation_consistency_failure"
        )
        
        assert result.status == ValidationStatus.FAIL
        assert "Aggregation inconsistency detected" in result.message
        assert result.details['comparison']['passed'] == False
    
    def test_group_candles_by_timeframe(self, validator, minute_candles):
        """Test candle grouping by timeframe"""
        # Create 5-minute aggregated candles for grouping
        five_min_candles = [validator._aggregate_candles(minute_candles, "5m")[0]]
        
        grouped = validator._group_candles_by_timeframe(minute_candles, five_min_candles)
        
        assert len(grouped) == 1
        assert len(grouped[list(grouped.keys())[0]]) == 5  # All 5 minute candles in one group
    
    def test_find_aggregated_candle(self, validator, five_minute_candle):
        """Test finding aggregated candle by boundary"""
        boundary = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        
        found = validator._find_aggregated_candle([five_minute_candle], boundary)
        
        assert found is not None
        assert found.timestamp == five_minute_candle.timestamp
    
    def test_find_aggregated_candle_not_found(self, validator):
        """Test finding aggregated candle when not found"""
        boundary = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        different_candle = OHLCV(
            timestamp=datetime(2024, 1, 15, 12, 5, 0, tzinfo=timezone.utc),
            open=100.0,
            high=105.0,
            low=95.0,
            close=102.0,
            volume=1000.0,
            symbol="BTC-USDT",
            timeframe="5m",
            source="binance"
        )
        
        found = validator._find_aggregated_candle([different_candle], boundary)
        
        assert found is None
    
    def test_validate_ohlc_group_success(self, validator, minute_candles, five_minute_candle):
        """Test OHLC group validation with correct data"""
        errors = validator._validate_ohlc_group(minute_candles, five_minute_candle)
        
        assert len(errors) == 0
    
    def test_validate_ohlc_group_failure(self, validator, minute_candles, incorrect_five_minute_candle):
        """Test OHLC group validation with incorrect data"""
        errors = validator._validate_ohlc_group(minute_candles, incorrect_five_minute_candle)
        
        assert len(errors) > 0
        # Should have errors for all OHLC fields
        assert any(error['field'] == 'open' for error in errors)
        assert any(error['field'] == 'high' for error in errors)
        assert any(error['field'] == 'low' for error in errors)
        assert any(error['field'] == 'close' for error in errors)
    
    def test_get_timeframe_boundary(self, validator):
        """Test timeframe boundary calculation"""
        timestamp = datetime(2024, 1, 15, 12, 3, 45, tzinfo=timezone.utc)
        
        # Test different timeframes
        boundary_1m = validator._get_timeframe_boundary(timestamp, "1m")
        assert boundary_1m == datetime(2024, 1, 15, 12, 3, 0, tzinfo=timezone.utc)
        
        boundary_5m = validator._get_timeframe_boundary(timestamp, "5m")
        assert boundary_5m == datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        
        boundary_15m = validator._get_timeframe_boundary(timestamp, "15m")
        assert boundary_5m == datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        
        boundary_1h = validator._get_timeframe_boundary(timestamp, "1h")
        assert boundary_1h == datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    
    def test_aggregate_candles(self, validator, minute_candles):
        """Test candle aggregation functionality"""
        aggregated = validator._aggregate_candles(minute_candles, "5m")
        
        assert len(aggregated) == 1
        agg_candle = aggregated[0]
        
        # Check OHLC values
        assert agg_candle.open == 100.0  # First candle's open
        assert agg_candle.high == 109.0  # Max of all highs
        assert agg_candle.low == 95.0    # Min of all lows
        assert agg_candle.close == 104.0 # Last candle's close
        assert agg_candle.volume == 6000.0  # Sum of all volumes
        assert agg_candle.timeframe == "5m"
    
    def test_aggregate_candles_empty(self, validator):
        """Test candle aggregation with empty list"""
        aggregated = validator._aggregate_candles([], "5m")
        
        assert len(aggregated) == 0
    
    def test_compare_candle_sets_success(self, validator, five_minute_candle):
        """Test candle set comparison with matching data"""
        candles1 = [five_minute_candle]
        candles2 = [five_minute_candle]
        
        comparison = validator._compare_candle_sets(candles1, candles2)
        
        assert comparison['passed'] == True
        assert comparison['error_count'] == 0
    
    def test_compare_candle_sets_failure(self, validator, five_minute_candle, incorrect_five_minute_candle):
        """Test candle set comparison with different data"""
        candles1 = [five_minute_candle]
        candles2 = [incorrect_five_minute_candle]
        
        comparison = validator._compare_candle_sets(candles1, candles2)
        
        assert comparison['passed'] == False
        assert comparison['error_count'] > 0
        assert len(comparison['errors']) > 0
    
    def test_compare_candle_sets_empty(self, validator):
        """Test candle set comparison with empty data"""
        comparison = validator._compare_candle_sets([], [])
        
        assert comparison['passed'] == False
        assert "One or both candle sets empty" in comparison['message']
    
    def test_compare_candle_sets_no_common_timestamps(self, validator):
        """Test candle set comparison with no common timestamps"""
        candle1 = OHLCV(
            timestamp=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
            open=100.0,
            high=105.0,
            low=95.0,
            close=102.0,
            volume=1000.0,
            symbol="BTC-USDT",
            timeframe="1m",
            source="binance"
        )
        
        candle2 = OHLCV(
            timestamp=datetime(2024, 1, 15, 12, 5, 0, tzinfo=timezone.utc),  # Different time
            open=100.0,
            high=105.0,
            low=95.0,
            close=102.0,
            volume=1000.0,
            symbol="BTC-USDT",
            timeframe="1m",
            source="binance"
        )
        
        comparison = validator._compare_candle_sets([candle1], [candle2])
        
        assert comparison['passed'] == False
        assert "No common timestamps found" in comparison['message']


@pytest.mark.integration
class TestAggregationValidationIntegration:
    """Integration tests for aggregation validation"""
    
    def test_validation_with_real_market_data(self):
        """Test validation with real market data"""
        pytest.skip("Integration test - requires real market data")
    
    def test_validation_with_high_frequency_aggregation(self):
        """Test validation with high-frequency data aggregation"""
        pytest.skip("Integration test - requires high-frequency data")
