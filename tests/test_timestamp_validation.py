"""
Timestamp Validation Tests

Tests for timestamp stability, alignment, and consistency validation.
"""

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock

from src.validation.timestamp_validator import TimestampValidator, TimestampValidationConfig
from src.validation.validation_results import ValidationStatus


class TestTimestampValidator:
    """Test timestamp validation functionality"""
    
    @pytest.fixture
    def validator(self):
        """Create validator instance"""
        config = TimestampValidationConfig(
            max_timestamp_drift_seconds=1.0,
            max_clock_skew_seconds=5.0,
            min_data_freshness_seconds=300.0
        )
        return TimestampValidator(config)
    
    @pytest.fixture
    def perfect_timestamps(self):
        """Perfect 1-minute interval timestamps"""
        base_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        return [base_time + timedelta(minutes=i) for i in range(10)]
    
    @pytest.fixture
    def irregular_timestamps(self):
        """Irregular interval timestamps"""
        base_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        return [
            base_time,
            base_time + timedelta(minutes=1),
            base_time + timedelta(minutes=3),  # Gap
            base_time + timedelta(minutes=4),
            base_time + timedelta(minutes=5),
        ]
    
    @pytest.fixture
    def duplicate_timestamps(self):
        """Timestamps with duplicates"""
        base_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        return [
            base_time,
            base_time + timedelta(minutes=1),
            base_time + timedelta(minutes=1),  # Duplicate
            base_time + timedelta(minutes=2),
        ]
    
    def test_timestamp_stability_success(self, validator, perfect_timestamps):
        """Test successful timestamp stability validation"""
        result = validator.validate_timestamp_stability(
            timestamps=perfect_timestamps,
            expected_interval_seconds=60.0,
            test_name="perfect_timestamps"
        )
        
        assert result.status == ValidationStatus.PASS
        assert "Timestamp stability validated" in result.message
        assert result.details['total_timestamps'] == 10
        assert result.details['duplicate_check']['passed'] == True
        assert result.details['ordering_check']['passed'] == True
        assert result.details['interval_check']['passed'] == True
        assert result.details['gap_check']['passed'] == True
    
    def test_timestamp_stability_duplicates(self, validator, duplicate_timestamps):
        """Test timestamp stability with duplicates"""
        result = validator.validate_timestamp_stability(
            timestamps=duplicate_timestamps,
            expected_interval_seconds=60.0,
            test_name="duplicate_timestamps"
        )
        
        assert result.status == ValidationStatus.FAIL
        assert "Timestamp stability issues detected" in result.message
        assert result.details['duplicate_check']['passed'] == False
        assert result.details['duplicate_check']['duplicate_count'] == 1
    
    def test_timestamp_stability_irregular_intervals(self, validator, irregular_timestamps):
        """Test timestamp stability with irregular intervals"""
        result = validator.validate_timestamp_stability(
            timestamps=irregular_timestamps,
            expected_interval_seconds=60.0,
            test_name="irregular_timestamps"
        )
        
        assert result.status == ValidationStatus.FAIL
        assert "Timestamp stability issues detected" in result.message
        assert result.details['interval_check']['passed'] == False
        assert result.details['gap_check']['passed'] == False
    
    def test_timestamp_stability_empty_list(self, validator):
        """Test timestamp stability with empty list"""
        result = validator.validate_timestamp_stability(
            timestamps=[],
            expected_interval_seconds=60.0,
            test_name="empty_timestamps"
        )
        
        assert result.status == ValidationStatus.FAIL
        assert "No timestamps provided" in result.message
    
    def test_timezone_consistency_success(self, validator, perfect_timestamps):
        """Test successful timezone consistency validation"""
        result = validator.validate_timezone_consistency(
            timestamps=perfect_timestamps,
            test_name="timezone_consistency"
        )
        
        assert result.status == ValidationStatus.PASS
        assert "Timezone consistency validated" in result.message
        assert result.details['timezone_check']['passed'] == True
        assert result.details['timezone_check']['timezone_count'] == 1
    
    def test_timezone_consistency_mixed(self, validator):
        """Test timezone consistency with mixed timezones"""
        mixed_timestamps = [
            datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 15, 12, 1, 0),  # Naive timestamp
        ]
        
        result = validator.validate_timezone_consistency(
            timestamps=mixed_timestamps,
            test_name="mixed_timezones"
        )
        
        assert result.status == ValidationStatus.FAIL
        assert "Timezone consistency issues detected" in result.message
        assert result.details['timezone_check']['passed'] == False
        assert result.details['timezone_check']['timezone_count'] == 2
    
    def test_data_freshness_success(self, validator):
        """Test successful data freshness validation"""
        # Create recent timestamps
        now = datetime.now(timezone.utc)
        recent_timestamps = [
            now - timedelta(minutes=1),
            now - timedelta(minutes=2),
            now - timedelta(minutes=3),
        ]
        
        result = validator.validate_data_freshness(
            timestamps=recent_timestamps,
            test_name="fresh_data"
        )
        
        assert result.status == ValidationStatus.PASS
        assert "Data freshness validated" in result.message
        assert result.details['passed'] == True
    
    def test_data_freshness_stale(self, validator):
        """Test data freshness validation with stale data"""
        # Create stale timestamps
        now = datetime.now(timezone.utc)
        stale_timestamps = [
            now - timedelta(hours=1),  # 1 hour old
            now - timedelta(hours=2),  # 2 hours old
        ]
        
        result = validator.validate_data_freshness(
            timestamps=stale_timestamps,
            test_name="stale_data"
        )
        
        assert result.status == ValidationStatus.WARNING
        assert "Data freshness warning" in result.message
        assert result.details['passed'] == False
        assert result.details['stale_count'] == 2
    
    def test_clock_synchronization_success(self, validator):
        """Test successful clock synchronization validation"""
        base_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        
        local_timestamps = [
            base_time,
            base_time + timedelta(seconds=1),
            base_time + timedelta(seconds=2),
        ]
        
        server_timestamps = [
            base_time + timedelta(milliseconds=100),  # 100ms ahead
            base_time + timedelta(seconds=1, milliseconds=100),
            base_time + timedelta(seconds=2, milliseconds=100),
        ]
        
        result = validator.validate_clock_synchronization(
            local_timestamps=local_timestamps,
            server_timestamps=server_timestamps,
            test_name="clock_sync"
        )
        
        assert result.status == ValidationStatus.PASS
        assert "Clock synchronization validated" in result.message
        assert result.details['passed'] == True
    
    def test_clock_synchronization_skew(self, validator):
        """Test clock synchronization with excessive skew"""
        base_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        
        local_timestamps = [
            base_time,
            base_time + timedelta(seconds=1),
        ]
        
        server_timestamps = [
            base_time + timedelta(seconds=10),  # 10 seconds ahead - excessive skew
            base_time + timedelta(seconds=11),
        ]
        
        result = validator.validate_clock_synchronization(
            local_timestamps=local_timestamps,
            server_timestamps=server_timestamps,
            test_name="clock_skew"
        )
        
        assert result.status == ValidationStatus.WARNING
        assert "Clock synchronization issues detected" in result.message
        assert result.details['passed'] == False
        assert result.details['skew_errors'] == 2
    
    def test_clock_synchronization_mismatched_counts(self, validator):
        """Test clock synchronization with mismatched timestamp counts"""
        base_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        
        local_timestamps = [base_time, base_time + timedelta(seconds=1)]
        server_timestamps = [base_time]  # Different count
        
        result = validator.validate_clock_synchronization(
            local_timestamps=local_timestamps,
            server_timestamps=server_timestamps,
            test_name="mismatched_counts"
        )
        
        assert result.status == ValidationStatus.FAIL
        assert "Missing local or server timestamps" in result.message
        assert result.details['passed'] == False
    
    def test_check_duplicate_timestamps(self, validator):
        """Test duplicate timestamp checking"""
        timestamps = [
            datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 15, 12, 1, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 15, 12, 1, 0, tzinfo=timezone.utc),  # Duplicate
        ]
        
        result = validator._check_duplicate_timestamps(timestamps)
        
        assert result['passed'] == False
        assert result['duplicate_count'] == 1
        assert len(result['duplicates']) == 1
    
    def test_check_timestamp_ordering(self, validator):
        """Test timestamp ordering check"""
        timestamps = [
            datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 15, 12, 2, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 15, 12, 1, 0, tzinfo=timezone.utc),  # Out of order
        ]
        
        result = validator._check_timestamp_ordering(timestamps)
        
        assert result['passed'] == False
        assert result['out_of_order_count'] == 1
        assert len(result['out_of_order']) == 1
    
    def test_check_interval_consistency(self, validator):
        """Test interval consistency checking"""
        timestamps = [
            datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 15, 12, 1, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 15, 12, 2, 0, tzinfo=timezone.utc),
        ]
        
        result = validator._check_interval_consistency(timestamps, 60.0)
        
        assert result['passed'] == True
        assert result['avg_interval'] == 60.0
        assert result['std_interval'] == 0.0
    
    def test_check_interval_consistency_irregular(self, validator):
        """Test interval consistency with irregular intervals"""
        timestamps = [
            datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 15, 12, 1, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 15, 12, 3, 0, tzinfo=timezone.utc),  # 2-minute gap
        ]
        
        result = validator._check_interval_consistency(timestamps, 60.0)
        
        assert result['passed'] == False
        assert len(result['interval_errors']) > 0
    
    def test_check_timestamp_gaps(self, validator):
        """Test timestamp gap checking"""
        timestamps = [
            datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 15, 12, 1, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 15, 12, 3, 0, tzinfo=timezone.utc),  # Gap
        ]
        
        result = validator._check_timestamp_gaps(timestamps, 60.0)
        
        assert result['passed'] == False
        assert result['gap_count'] == 1
        assert len(result['gaps']) == 1
    
    def test_check_timezone_consistency(self, validator):
        """Test timezone consistency checking"""
        utc_timestamps = [
            datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 15, 12, 1, 0, tzinfo=timezone.utc),
        ]
        
        result = validator._check_timezone_consistency(utc_timestamps)
        
        assert result['passed'] == True
        assert result['timezone_count'] == 1
    
    def test_check_data_freshness(self, validator):
        """Test data freshness checking"""
        now = datetime.now(timezone.utc)
        fresh_timestamps = [
            now - timedelta(minutes=1),
            now - timedelta(minutes=2),
        ]
        
        result = validator._check_data_freshness(fresh_timestamps)
        
        assert result['passed'] == True
        assert result['stale_count'] == 0
    
    def test_check_data_freshness_stale(self, validator):
        """Test data freshness checking with stale data"""
        now = datetime.now(timezone.utc)
        stale_timestamps = [
            now - timedelta(hours=1),
            now - timedelta(hours=2),
        ]
        
        result = validator._check_data_freshness(stale_timestamps)
        
        assert result['passed'] == False
        assert result['stale_count'] == 2
        assert result['max_age_seconds'] > 3600  # More than 1 hour


@pytest.mark.integration
class TestTimestampValidationIntegration:
    """Integration tests for timestamp validation"""
    
    def test_validation_with_real_market_data(self):
        """Test validation with real market data timestamps"""
        # This would test with actual market data
        pytest.skip("Integration test - requires real market data")
    
    def test_validation_with_high_frequency_data(self):
        """Test validation with high-frequency data"""
        # This would test with high-frequency tick data
        pytest.skip("Integration test - requires high-frequency data")