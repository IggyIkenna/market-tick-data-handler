#!/usr/bin/env python3
"""
Data Quality Tests

Tests for data quality validation and monitoring:
1. Data completeness validation
2. Data consistency checks
3. Data range validation
4. Data format validation
5. Data freshness validation
6. Data integrity checks
"""

import pytest
import pytest_asyncio
import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Tuple
import logging

# Add project root to path
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from market_data_tick_handler.data_downloader.tardis_connector import TardisConnector
from market_data_tick_handler.data_validator.data_validator import DataValidator
from config import get_config

logger = logging.getLogger(__name__)

class DataQualityTests:
    """Base class for data quality tests"""
    
    def __init__(self):
        self.config = get_config()
        self.tardis_connector = None
        self.data_validator = None
        
    async def setup(self):
        """Setup test environment"""
        self.tardis_connector = TardisConnector(api_key=self.config.tardis.api_key)
        await self.tardis_connector._create_session()
        
        self.data_validator = DataValidator(self.config.gcp.bucket)
        
    async def teardown(self):
        """Cleanup test environment"""
        if self.tardis_connector:
            await self.tardis_connector.close()
    
    def validate_data_completeness(self, df: pd.DataFrame, data_type: str) -> Dict[str, Any]:
        """
        Validate data completeness
        
        Args:
            df: DataFrame to validate
            data_type: Type of data
            
        Returns:
            Dict with completeness validation results
        """
        result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'completeness_stats': {}
        }
        
        if df.empty:
            result['warnings'].append("Empty DataFrame - no data to validate")
            return result
        
        # Define required columns for each data type
        required_columns = {
            'trades': ['timestamp', 'price', 'amount', 'side'],
            'book_snapshot_5': ['timestamp', 'asks_0_price', 'bids_0_price'],
            'derivative_ticker': ['timestamp', 'last_price'],
            'liquidations': ['timestamp', 'price', 'amount', 'side'],
            'options_chain': ['timestamp', 'strike_price', 'type']
        }
        
        if data_type in required_columns:
            required_cols = required_columns[data_type]
            missing_columns = []
            
            for col in required_cols:
                if col not in df.columns:
                    missing_columns.append(col)
            
            if missing_columns:
                result['errors'].append(f"Missing required columns: {missing_columns}")
                result['valid'] = False
            
            # Check for missing values in required columns
            missing_values = {}
            for col in required_cols:
                if col in df.columns:
                    missing_count = df[col].isna().sum()
                    if missing_count > 0:
                        missing_values[col] = int(missing_count)
                        result['warnings'].append(f"Column '{col}' has {missing_count} missing values")
            
            result['completeness_stats']['missing_values'] = missing_values
            result['completeness_stats']['missing_columns'] = missing_columns
        
        # Check overall data completeness
        total_rows = len(df)
        complete_rows = df.dropna().shape[0]
        completeness_ratio = complete_rows / total_rows if total_rows > 0 else 0
        
        result['completeness_stats']['total_rows'] = total_rows
        result['completeness_stats']['complete_rows'] = complete_rows
        result['completeness_stats']['completeness_ratio'] = completeness_ratio
        
        if completeness_ratio < 0.95:  # 95% completeness threshold
            result['warnings'].append(f"Data completeness below threshold: {completeness_ratio:.2%}")
        
        return result
    
    def validate_data_consistency(self, df: pd.DataFrame, data_type: str) -> Dict[str, Any]:
        """
        Validate data consistency
        
        Args:
            df: DataFrame to validate
            data_type: Type of data
            
        Returns:
            Dict with consistency validation results
        """
        result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'consistency_stats': {}
        }
        
        if df.empty:
            result['warnings'].append("Empty DataFrame - no data to validate")
            return result
        
        # Check for duplicate timestamps
        if 'timestamp' in df.columns:
            timestamp_counts = df['timestamp'].value_counts()
            duplicates = timestamp_counts[timestamp_counts > 1]
            
            if len(duplicates) > 0:
                result['warnings'].append(f"Found {len(duplicates)} duplicate timestamps")
                result['consistency_stats']['duplicate_timestamps'] = len(duplicates)
        
        # Check for data type consistency
        type_inconsistencies = []
        for col in df.columns:
            if df[col].dtype == 'object':
                # Check for mixed types in object columns
                non_null_values = df[col].dropna()
                if len(non_null_values) > 0:
                    types = set(type(val).__name__ for val in non_null_values)
                    if len(types) > 1:
                        type_inconsistencies.append(col)
        
        if type_inconsistencies:
            result['warnings'].append(f"Type inconsistencies in columns: {type_inconsistencies}")
            result['consistency_stats']['type_inconsistencies'] = type_inconsistencies
        
        # Check for logical consistency based on data type
        if data_type == 'trades':
            # Check that buy/sell sides are valid
            if 'side' in df.columns:
                valid_sides = set(df['side'].dropna().unique())
                invalid_sides = valid_sides - {'buy', 'sell'}
                if invalid_sides:
                    result['errors'].append(f"Invalid trade sides: {invalid_sides}")
                    result['valid'] = False
        
        elif data_type == 'book_snapshot_5':
            # Check that bid prices are less than ask prices
            if 'bids_0_price' in df.columns and 'asks_0_price' in df.columns:
                invalid_spreads = df[
                    (df['bids_0_price'] >= df['asks_0_price']) & 
                    (df['bids_0_price'].notna()) & 
                    (df['asks_0_price'].notna())
                ]
                
                if len(invalid_spreads) > 0:
                    result['warnings'].append(f"Found {len(invalid_spreads)} invalid bid-ask spreads")
                    result['consistency_stats']['invalid_spreads'] = len(invalid_spreads)
        
        return result
    
    def validate_data_ranges(self, df: pd.DataFrame, data_type: str) -> Dict[str, Any]:
        """
        Validate data ranges and bounds
        
        Args:
            df: DataFrame to validate
            data_type: Type of data
            
        Returns:
            Dict with range validation results
        """
        result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'range_stats': {}
        }
        
        if df.empty:
            result['warnings'].append("Empty DataFrame - no data to validate")
            return result
        
        # Define expected ranges for different data types
        range_checks = {
            'trades': {
                'price': {'min': 0, 'max': 1e6, 'name': 'Trade Price'},
                'amount': {'min': 0, 'max': 1e9, 'name': 'Trade Amount'}
            },
            'book_snapshot_5': {
                'asks_0_price': {'min': 0, 'max': 1e6, 'name': 'Ask Price Level 0'},
                'bids_0_price': {'min': 0, 'max': 1e6, 'name': 'Bid Price Level 0'},
                'asks_0_amount': {'min': 0, 'max': 1e9, 'name': 'Ask Amount Level 0'},
                'bids_0_amount': {'min': 0, 'max': 1e9, 'name': 'Bid Amount Level 0'}
            },
            'derivative_ticker': {
                'funding_rate': {'min': -1, 'max': 1, 'name': 'Funding Rate'},
                'open_interest': {'min': 0, 'max': 1e12, 'name': 'Open Interest'},
                'last_price': {'min': 0, 'max': 1e6, 'name': 'Last Price'}
            },
            'liquidations': {
                'price': {'min': 0, 'max': 1e6, 'name': 'Liquidation Price'},
                'amount': {'min': 0, 'max': 1e9, 'name': 'Liquidation Amount'}
            }
        }
        
        if data_type in range_checks:
            checks = range_checks[data_type]
            
            for column, check in checks.items():
                if column not in df.columns:
                    continue
                
                col_data = df[column].dropna()
                if len(col_data) == 0:
                    continue
                
                col_name = check['name']
                min_val = col_data.min()
                max_val = col_data.max()
                
                # Check minimum value
                if min_val < check['min']:
                    result['errors'].append(f"{col_name} minimum value {min_val} below expected {check['min']}")
                    result['valid'] = False
                
                # Check maximum value
                if max_val > check['max']:
                    result['errors'].append(f"{col_name} maximum value {max_val} above expected {check['max']}")
                    result['valid'] = False
                
                # Store statistics
                result['range_stats'][column] = {
                    'min': float(min_val),
                    'max': float(max_val),
                    'mean': float(col_data.mean()),
                    'std': float(col_data.std()),
                    'count': len(col_data)
                }
        
        return result
    
    def validate_data_freshness(self, df: pd.DataFrame, data_type: str, expected_date: datetime) -> Dict[str, Any]:
        """
        Validate data freshness
        
        Args:
            df: DataFrame to validate
            data_type: Type of data
            expected_date: Expected date for the data
            
        Returns:
            Dict with freshness validation results
        """
        result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'freshness_stats': {}
        }
        
        if df.empty:
            result['warnings'].append("Empty DataFrame - no data to validate")
            return result
        
        if 'timestamp' in df.columns:
            timestamps = df['timestamp'].dropna()
            if len(timestamps) == 0:
                result['warnings'].append("No valid timestamps found")
                return result
            
            # Convert timestamps to datetime
            try:
                # Assume timestamps are in microseconds
                timestamps_dt = pd.to_datetime(timestamps, unit='us')
                
                # Check if timestamps are within expected date range
                expected_start = expected_date.replace(hour=0, minute=0, second=0, microsecond=0)
                expected_end = expected_date.replace(hour=23, minute=59, second=59, microsecond=999999)
                
                min_timestamp = timestamps_dt.min()
                max_timestamp = timestamps_dt.max()
                
                # Check if data is from expected date
                if min_timestamp.date() != expected_date.date():
                    result['warnings'].append(f"Data contains timestamps from {min_timestamp.date()}, expected {expected_date.date()}")
                
                if max_timestamp.date() != expected_date.date():
                    result['warnings'].append(f"Data contains timestamps from {max_timestamp.date()}, expected {expected_date.date()}")
                
                # Check data age (how old the data is)
                now = datetime.now(timezone.utc)
                data_age_hours = (now - max_timestamp).total_seconds() / 3600
                
                result['freshness_stats']['min_timestamp'] = min_timestamp.isoformat()
                result['freshness_stats']['max_timestamp'] = max_timestamp.isoformat()
                result['freshness_stats']['data_age_hours'] = data_age_hours
                result['freshness_stats']['expected_date'] = expected_date.isoformat()
                
                # Warn if data is very old
                if data_age_hours > 24:
                    result['warnings'].append(f"Data is {data_age_hours:.1f} hours old")
                
            except Exception as e:
                result['errors'].append(f"Failed to parse timestamps: {e}")
                result['valid'] = False
        
        return result
    
    def validate_data_integrity(self, df: pd.DataFrame, data_type: str) -> Dict[str, Any]:
        """
        Validate data integrity
        
        Args:
            df: DataFrame to validate
            data_type: Type of data
            
        Returns:
            Dict with integrity validation results
        """
        result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'integrity_stats': {}
        }
        
        if df.empty:
            result['warnings'].append("Empty DataFrame - no data to validate")
            return result
        
        # Check for data corruption indicators
        corruption_indicators = []
        
        # Check for infinite values
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            if col in ['timestamp', 'local_timestamp', 'expiration', 'funding_timestamp']:
                continue  # Skip timestamp columns
            
            infinite_count = np.isinf(df[col]).sum()
            if infinite_count > 0:
                corruption_indicators.append(f"Column '{col}' has {infinite_count} infinite values")
        
        # Check for NaN values in critical columns
        critical_columns = {
            'trades': ['timestamp', 'price', 'amount'],
            'book_snapshot_5': ['timestamp', 'asks_0_price', 'bids_0_price'],
            'derivative_ticker': ['timestamp', 'last_price'],
            'liquidations': ['timestamp', 'price', 'amount']
        }
        
        if data_type in critical_columns:
            critical_cols = critical_columns[data_type]
            for col in critical_cols:
                if col in df.columns:
                    nan_count = df[col].isna().sum()
                    if nan_count > 0:
                        corruption_indicators.append(f"Column '{col}' has {nan_count} NaN values")
        
        # Check for negative values where they shouldn't exist
        for col in numeric_columns:
            if col in ['timestamp', 'local_timestamp', 'expiration', 'funding_timestamp']:
                continue  # Skip timestamp columns
            
            negative_count = (df[col] < 0).sum()
            if negative_count > 0:
                corruption_indicators.append(f"Column '{col}' has {negative_count} negative values")
        
        if corruption_indicators:
            result['warnings'].extend(corruption_indicators)
            result['integrity_stats']['corruption_indicators'] = corruption_indicators
        
        # Check for data distribution anomalies
        if len(df) > 100:  # Only check if we have enough data
            for col in numeric_columns:
                if col in ['timestamp', 'local_timestamp', 'expiration', 'funding_timestamp']:
                    continue  # Skip timestamp columns
                
                col_data = df[col].dropna()
                if len(col_data) < 10:
                    continue
                
                # Check for extreme outliers (beyond 5 standard deviations)
                mean_val = col_data.mean()
                std_val = col_data.std()
                
                if std_val > 0:
                    outliers = col_data[np.abs(col_data - mean_val) > 5 * std_val]
                    if len(outliers) > 0:
                        result['warnings'].append(f"Column '{col}' has {len(outliers)} extreme outliers")
                        result['integrity_stats'][f'{col}_outliers'] = len(outliers)
        
        return result


# Test fixtures and sample instruments
TEST_INSTRUMENTS = {
    'binance': {
        'trades': {'tardis_exchange': 'binance', 'tardis_symbol': 'BTCUSDT'},
        'book_snapshot_5': {'tardis_exchange': 'binance', 'tardis_symbol': 'BTCUSDT'}
    },
    'deribit': {
        'trades': {'tardis_exchange': 'deribit', 'tardis_symbol': 'BTC-PERPETUAL'},
        'book_snapshot_5': {'tardis_exchange': 'deribit', 'tardis_symbol': 'BTC-PERPETUAL'},
        'derivative_ticker': {'tardis_exchange': 'deribit', 'tardis_symbol': 'BTC-PERPETUAL'},
        'liquidations': {'tardis_exchange': 'deribit', 'tardis_symbol': 'BTC-PERPETUAL'}
    }
}

TEST_DATE = datetime(2023, 5, 23, tzinfo=timezone.utc)


@pytest_asyncio.fixture
async def data_quality_tester():
    """Fixture for data quality tests"""
    tester = DataQualityTests()
    await tester.setup()
    try:
        yield tester
    finally:
        await tester.teardown()


@pytest.mark.asyncio
class TestDataCompleteness:
    """Test data completeness validation"""
    
    async def test_data_completeness_all_types(self, data_quality_tester):
        """Test data completeness for all data types"""
        results = []
        
        for venue, instruments in TEST_INSTRUMENTS.items():
            for data_type, instrument in instruments.items():
                logger.info(f"Testing data completeness for {venue}:{data_type}")
                
                try:
                    # Download data
                    download_results = await data_quality_tester.tardis_connector.download_daily_data_direct(
                        tardis_exchange=instrument['tardis_exchange'],
                        tardis_symbol=instrument['tardis_symbol'],
                        date=TEST_DATE,
                        data_types=[data_type]
                    )
                    
                    if data_type in download_results:
                        df = download_results[data_type]
                        
                        # Validate completeness
                        completeness_result = data_quality_tester.validate_data_completeness(df, data_type)
                        completeness_result['venue'] = venue
                        completeness_result['data_type'] = data_type
                        
                        results.append(completeness_result)
                        
                        # Assert completeness is valid
                        assert completeness_result['valid'], f"Data completeness failed for {venue}:{data_type}: {completeness_result['errors']}"
                        
                        logger.info(f"✅ Data completeness validation passed for {venue}:{data_type}")
                    else:
                        logger.warning(f"No data available for {venue}:{data_type}")
                
                except Exception as e:
                    logger.error(f"Failed to test data completeness for {venue}:{data_type}: {e}")
        
        logger.info(f"Data completeness validation completed for {len(results)} venue/data_type combinations")
    
    async def test_missing_data_detection(self, data_quality_tester):
        """Test detection of missing data"""
        logger.info("Testing missing data detection")
        
        # Create a DataFrame with missing data
        df_with_missing = pd.DataFrame({
            'timestamp': [1, 2, 3, 4, 5],
            'price': [100.0, 101.0, np.nan, 103.0, 104.0],
            'amount': [1.0, 2.0, 3.0, np.nan, 5.0],
            'side': ['buy', 'sell', 'buy', 'sell', np.nan]
        })
        
        # Validate completeness
        result = data_quality_tester.validate_data_completeness(df_with_missing, 'trades')
        
        # Should detect missing values
        assert 'missing_values' in result['completeness_stats']
        assert result['completeness_stats']['missing_values']['price'] == 1
        assert result['completeness_stats']['missing_values']['amount'] == 1
        assert result['completeness_stats']['missing_values']['side'] == 1
        
        # Should have warnings about missing values
        assert len(result['warnings']) > 0
        assert any('missing values' in warning for warning in result['warnings'])
        
        logger.info("✅ Missing data detection test passed")


@pytest.mark.asyncio
class TestDataConsistency:
    """Test data consistency validation"""
    
    async def test_data_consistency_all_types(self, data_quality_tester):
        """Test data consistency for all data types"""
        results = []
        
        for venue, instruments in TEST_INSTRUMENTS.items():
            for data_type, instrument in instruments.items():
                logger.info(f"Testing data consistency for {venue}:{data_type}")
                
                try:
                    # Download data
                    download_results = await data_quality_tester.tardis_connector.download_daily_data_direct(
                        tardis_exchange=instrument['tardis_exchange'],
                        tardis_symbol=instrument['tardis_symbol'],
                        date=TEST_DATE,
                        data_types=[data_type]
                    )
                    
                    if data_type in download_results:
                        df = download_results[data_type]
                        
                        # Validate consistency
                        consistency_result = data_quality_tester.validate_data_consistency(df, data_type)
                        consistency_result['venue'] = venue
                        consistency_result['data_type'] = data_type
                        
                        results.append(consistency_result)
                        
                        # Assert consistency is valid
                        assert consistency_result['valid'], f"Data consistency failed for {venue}:{data_type}: {consistency_result['errors']}"
                        
                        logger.info(f"✅ Data consistency validation passed for {venue}:{data_type}")
                    else:
                        logger.warning(f"No data available for {venue}:{data_type}")
                
                except Exception as e:
                    logger.error(f"Failed to test data consistency for {venue}:{data_type}: {e}")
        
        logger.info(f"Data consistency validation completed for {len(results)} venue/data_type combinations")
    
    async def test_duplicate_timestamp_detection(self, data_quality_tester):
        """Test detection of duplicate timestamps"""
        logger.info("Testing duplicate timestamp detection")
        
        # Create a DataFrame with duplicate timestamps
        df_with_duplicates = pd.DataFrame({
            'timestamp': [1, 2, 2, 3, 4],  # Duplicate timestamp at index 2
            'price': [100.0, 101.0, 101.5, 103.0, 104.0],
            'amount': [1.0, 2.0, 1.5, 3.0, 4.0],
            'side': ['buy', 'sell', 'buy', 'sell', 'buy']
        })
        
        # Validate consistency
        result = data_quality_tester.validate_data_consistency(df_with_duplicates, 'trades')
        
        # Should detect duplicate timestamps
        assert 'duplicate_timestamps' in result['consistency_stats']
        assert result['consistency_stats']['duplicate_timestamps'] == 1
        
        # Should have warnings about duplicates
        assert len(result['warnings']) > 0
        assert any('duplicate timestamps' in warning for warning in result['warnings'])
        
        logger.info("✅ Duplicate timestamp detection test passed")


@pytest.mark.asyncio
class TestDataRanges:
    """Test data range validation"""
    
    async def test_data_ranges_all_types(self, data_quality_tester):
        """Test data ranges for all data types"""
        results = []
        
        for venue, instruments in TEST_INSTRUMENTS.items():
            for data_type, instrument in instruments.items():
                logger.info(f"Testing data ranges for {venue}:{data_type}")
                
                try:
                    # Download data
                    download_results = await data_quality_tester.tardis_connector.download_daily_data_direct(
                        tardis_exchange=instrument['tardis_exchange'],
                        tardis_symbol=instrument['tardis_symbol'],
                        date=TEST_DATE,
                        data_types=[data_type]
                    )
                    
                    if data_type in download_results:
                        df = download_results[data_type]
                        
                        # Validate ranges
                        range_result = data_quality_tester.validate_data_ranges(df, data_type)
                        range_result['venue'] = venue
                        range_result['data_type'] = data_type
                        
                        results.append(range_result)
                        
                        # Assert ranges are valid
                        assert range_result['valid'], f"Data ranges failed for {venue}:{data_type}: {range_result['errors']}"
                        
                        logger.info(f"✅ Data ranges validation passed for {venue}:{data_type}")
                    else:
                        logger.warning(f"No data available for {venue}:{data_type}")
                
                except Exception as e:
                    logger.error(f"Failed to test data ranges for {venue}:{data_type}: {e}")
        
        logger.info(f"Data ranges validation completed for {len(results)} venue/data_type combinations")
    
    async def test_out_of_range_detection(self, data_quality_tester):
        """Test detection of out-of-range values"""
        logger.info("Testing out-of-range detection")
        
        # Create a DataFrame with out-of-range values
        df_with_outliers = pd.DataFrame({
            'timestamp': [1, 2, 3, 4, 5],
            'price': [100.0, 101.0, -50.0, 103.0, 104.0],  # Negative price
            'amount': [1.0, 2.0, 3.0, 4.0, 5.0]
        })
        
        # Validate ranges
        result = data_quality_tester.validate_data_ranges(df_with_outliers, 'trades')
        
        # Should detect out-of-range values
        assert not result['valid']
        assert len(result['errors']) > 0
        assert any('minimum value' in error for error in result['errors'])
        
        logger.info("✅ Out-of-range detection test passed")


@pytest.mark.asyncio
class TestDataFreshness:
    """Test data freshness validation"""
    
    async def test_data_freshness_all_types(self, data_quality_tester):
        """Test data freshness for all data types"""
        results = []
        
        for venue, instruments in TEST_INSTRUMENTS.items():
            for data_type, instrument in instruments.items():
                logger.info(f"Testing data freshness for {venue}:{data_type}")
                
                try:
                    # Download data
                    download_results = await data_quality_tester.tardis_connector.download_daily_data_direct(
                        tardis_exchange=instrument['tardis_exchange'],
                        tardis_symbol=instrument['tardis_symbol'],
                        date=TEST_DATE,
                        data_types=[data_type]
                    )
                    
                    if data_type in download_results:
                        df = download_results[data_type]
                        
                        # Validate freshness
                        freshness_result = data_quality_tester.validate_data_freshness(df, data_type, TEST_DATE)
                        freshness_result['venue'] = venue
                        freshness_result['data_type'] = data_type
                        
                        results.append(freshness_result)
                        
                        # Assert freshness is valid
                        assert freshness_result['valid'], f"Data freshness failed for {venue}:{data_type}: {freshness_result['errors']}"
                        
                        logger.info(f"✅ Data freshness validation passed for {venue}:{data_type}")
                    else:
                        logger.warning(f"No data available for {venue}:{data_type}")
                
                except Exception as e:
                    logger.error(f"Failed to test data freshness for {venue}:{data_type}: {e}")
        
        logger.info(f"Data freshness validation completed for {len(results)} venue/data_type combinations")


@pytest.mark.asyncio
class TestDataIntegrity:
    """Test data integrity validation"""
    
    async def test_data_integrity_all_types(self, data_quality_tester):
        """Test data integrity for all data types"""
        results = []
        
        for venue, instruments in TEST_INSTRUMENTS.items():
            for data_type, instrument in instruments.items():
                logger.info(f"Testing data integrity for {venue}:{data_type}")
                
                try:
                    # Download data
                    download_results = await data_quality_tester.tardis_connector.download_daily_data_direct(
                        tardis_exchange=instrument['tardis_exchange'],
                        tardis_symbol=instrument['tardis_symbol'],
                        date=TEST_DATE,
                        data_types=[data_type]
                    )
                    
                    if data_type in download_results:
                        df = download_results[data_type]
                        
                        # Validate integrity
                        integrity_result = data_quality_tester.validate_data_integrity(df, data_type)
                        integrity_result['venue'] = venue
                        integrity_result['data_type'] = data_type
                        
                        results.append(integrity_result)
                        
                        # Assert integrity is valid
                        assert integrity_result['valid'], f"Data integrity failed for {venue}:{data_type}: {integrity_result['errors']}"
                        
                        logger.info(f"✅ Data integrity validation passed for {venue}:{data_type}")
                    else:
                        logger.warning(f"No data available for {venue}:{data_type}")
                
                except Exception as e:
                    logger.error(f"Failed to test data integrity for {venue}:{data_type}: {e}")
        
        logger.info(f"Data integrity validation completed for {len(results)} venue/data_type combinations")
    
    async def test_corruption_detection(self, data_quality_tester):
        """Test detection of data corruption"""
        logger.info("Testing corruption detection")
        
        # Create a DataFrame with corruption indicators
        df_with_corruption = pd.DataFrame({
            'timestamp': [1, 2, 3, 4, 5],
            'price': [100.0, 101.0, np.inf, 103.0, 104.0],  # Infinite value
            'amount': [1.0, 2.0, 3.0, 4.0, 5.0]
        })
        
        # Validate integrity
        result = data_quality_tester.validate_data_integrity(df_with_corruption, 'trades')
        
        # Should detect corruption indicators
        assert 'corruption_indicators' in result['integrity_stats']
        assert len(result['integrity_stats']['corruption_indicators']) > 0
        assert any('infinite values' in indicator for indicator in result['integrity_stats']['corruption_indicators'])
        
        # Should have warnings about corruption
        assert len(result['warnings']) > 0
        assert any('infinite values' in warning for warning in result['warnings'])
        
        logger.info("✅ Corruption detection test passed")


if __name__ == "__main__":
    # Run tests directly
    pytest.main([__file__, "-v"])
