#!/usr/bin/env python3
"""
Signature Validation Tests

Tests to ensure data integrity and consistency by validating:
1. Data signatures (checksums, hashes)
2. Data completeness and consistency
3. Timestamp validation and ordering
4. Data range validation
5. Cross-venue data consistency
"""

import pytest
import asyncio
import hashlib
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
from config import get_config

logger = logging.getLogger(__name__)

class SignatureValidationTests:
    """Base class for signature validation tests"""
    
    def __init__(self):
        self.config = get_config()
        self.tardis_connector = None
        
    async def setup(self):
        """Setup test environment"""
        self.tardis_connector = TardisConnector(api_key=self.config.tardis.api_key)
        await self.tardis_connector._create_session()
        
    async def teardown(self):
        """Cleanup test environment"""
        if self.tardis_connector:
            await self.tardis_connector.close()
    
    def calculate_data_signature(self, df: pd.DataFrame, columns: List[str] = None) -> str:
        """
        Calculate a signature (hash) for the data to detect changes
        
        Args:
            df: DataFrame to calculate signature for
            columns: Specific columns to include in signature (default: all)
            
        Returns:
            SHA256 hash of the data
        """
        if df.empty:
            return hashlib.sha256(b'').hexdigest()
        
        # Use specified columns or all columns
        if columns:
            df_subset = df[columns].copy()
        else:
            df_subset = df.copy()
        
        # Sort by timestamp to ensure consistent ordering
        if 'timestamp' in df_subset.columns:
            df_subset = df_subset.sort_values('timestamp')
        
        # Convert to string representation for hashing
        data_str = df_subset.to_string(index=False)
        return hashlib.sha256(data_str.encode('utf-8')).hexdigest()
    
    def validate_timestamp_ordering(self, df: pd.DataFrame, timestamp_col: str = 'timestamp') -> Dict[str, Any]:
        """
        Validate that timestamps are properly ordered and within expected ranges
        
        Args:
            df: DataFrame with timestamp data
            timestamp_col: Name of timestamp column
            
        Returns:
            Dict with validation results
        """
        result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'timestamp_stats': {}
        }
        
        if df.empty:
            result['warnings'].append("Empty DataFrame - no timestamps to validate")
            return result
        
        if timestamp_col not in df.columns:
            result['errors'].append(f"Timestamp column '{timestamp_col}' not found")
            result['valid'] = False
            return result
        
        timestamps = df[timestamp_col].dropna()
        
        if len(timestamps) == 0:
            result['warnings'].append("No valid timestamps found")
            return result
        
        # Check for monotonic ordering
        is_monotonic = timestamps.is_monotonic_increasing
        if not is_monotonic:
            result['errors'].append("Timestamps are not monotonically increasing")
            result['valid'] = False
        
        # Check for reasonable timestamp ranges (microseconds since epoch)
        min_ts = timestamps.min()
        max_ts = timestamps.max()
        
        # Expected range: 2020-2030 (roughly)
        expected_min = 1577836800000000  # 2020-01-01
        expected_max = 1893456000000000  # 2030-01-01
        
        if min_ts < expected_min:
            result['warnings'].append(f"Minimum timestamp {min_ts} seems too old")
        if max_ts > expected_max:
            result['warnings'].append(f"Maximum timestamp {max_ts} seems too far in future")
        
        result['timestamp_stats'] = {
            'count': len(timestamps),
            'min': int(min_ts),
            'max': int(max_ts),
            'range_hours': (max_ts - min_ts) / (1000000 * 3600),
            'is_monotonic': is_monotonic
        }
        
        return result
    
    def validate_data_ranges(self, df: pd.DataFrame, data_type: str) -> Dict[str, Any]:
        """
        Validate that data values are within expected ranges
        
        Args:
            df: DataFrame to validate
            data_type: Type of data (trades, book_snapshot_5, etc.)
            
        Returns:
            Dict with validation results
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
                'amount': {'min': 0, 'max': 1e9, 'name': 'Trade Amount'},
                'side': {'values': ['buy', 'sell'], 'name': 'Trade Side'}
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
                'last_price': {'min': 0, 'max': 1e6, 'name': 'Last Price'},
                'index_price': {'min': 0, 'max': 1e6, 'name': 'Index Price'},
                'mark_price': {'min': 0, 'max': 1e6, 'name': 'Mark Price'}
            },
            'liquidations': {
                'price': {'min': 0, 'max': 1e6, 'name': 'Liquidation Price'},
                'amount': {'min': 0, 'max': 1e9, 'name': 'Liquidation Amount'},
                'side': {'values': ['buy', 'sell'], 'name': 'Liquidation Side'}
            }
        }
        
        if data_type not in range_checks:
            result['warnings'].append(f"No range checks defined for data type: {data_type}")
            return result
        
        checks = range_checks[data_type]
        
        for column, check in checks.items():
            if column not in df.columns:
                continue
            
            col_data = df[column].dropna()
            if len(col_data) == 0:
                continue
            
            col_name = check['name']
            
            # Check numeric ranges
            if 'min' in check and 'max' in check:
                min_val = col_data.min()
                max_val = col_data.max()
                
                if min_val < check['min']:
                    result['errors'].append(f"{col_name} minimum value {min_val} below expected {check['min']}")
                    result['valid'] = False
                
                if max_val > check['max']:
                    result['errors'].append(f"{col_name} maximum value {max_val} above expected {check['max']}")
                    result['valid'] = False
                
                result['range_stats'][column] = {
                    'min': float(min_val),
                    'max': float(max_val),
                    'count': len(col_data),
                    'mean': float(col_data.mean()) if len(col_data) > 0 else 0
                }
            
            # Check categorical values
            elif 'values' in check:
                unique_values = set(col_data.unique())
                expected_values = set(check['values'])
                
                invalid_values = unique_values - expected_values
                if invalid_values:
                    result['errors'].append(f"{col_name} contains invalid values: {invalid_values}")
                    result['valid'] = False
                
                result['range_stats'][column] = {
                    'unique_values': list(unique_values),
                    'count': len(col_data)
                }
        
        return result
    
    def validate_data_consistency(self, df: pd.DataFrame, data_type: str) -> Dict[str, Any]:
        """
        Validate internal data consistency
        
        Args:
            df: DataFrame to validate
            data_type: Type of data
            
        Returns:
            Dict with validation results
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
        
        # Check for duplicate timestamps (should be rare but possible)
        if 'timestamp' in df.columns:
            timestamp_counts = df['timestamp'].value_counts()
            duplicates = timestamp_counts[timestamp_counts > 1]
            
            if len(duplicates) > 0:
                result['warnings'].append(f"Found {len(duplicates)} duplicate timestamps")
                result['consistency_stats']['duplicate_timestamps'] = len(duplicates)
        
        # Check for missing values in critical columns
        critical_columns = {
            'trades': ['timestamp', 'price', 'amount', 'side'],
            'book_snapshot_5': ['timestamp', 'asks_0_price', 'bids_0_price'],
            'derivative_ticker': ['timestamp', 'last_price'],
            'liquidations': ['timestamp', 'price', 'amount', 'side']
        }
        
        if data_type in critical_columns:
            critical_cols = critical_columns[data_type]
            missing_stats = {}
            
            for col in critical_cols:
                if col in df.columns:
                    missing_count = df[col].isna().sum()
                    if missing_count > 0:
                        missing_stats[col] = int(missing_count)
                        result['warnings'].append(f"Column '{col}' has {missing_count} missing values")
            
            result['consistency_stats']['missing_values'] = missing_stats
        
        # Check for negative values where they shouldn't exist
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        negative_stats = {}
        
        for col in numeric_columns:
            if col in ['timestamp', 'local_timestamp', 'expiration', 'funding_timestamp']:
                continue  # Skip timestamp columns
            
            negative_count = (df[col] < 0).sum()
            if negative_count > 0:
                negative_stats[col] = int(negative_count)
                result['warnings'].append(f"Column '{col}' has {negative_count} negative values")
        
        result['consistency_stats']['negative_values'] = negative_stats
        
        return result
    
    async def download_and_validate_data(self, tardis_exchange: str, tardis_symbol: str, 
                                       date: datetime, data_type: str) -> Dict[str, Any]:
        """
        Download data and perform comprehensive validation
        
        Args:
            tardis_exchange: Exchange name
            tardis_symbol: Symbol name
            date: Date to download
            data_type: Type of data
            
        Returns:
            Dict with validation results
        """
        result = {
            'tardis_exchange': tardis_exchange,
            'tardis_symbol': tardis_symbol,
            'date': date.isoformat(),
            'data_type': data_type,
            'valid': True,
            'errors': [],
            'warnings': [],
            'signatures': {},
            'validation_results': {}
        }
        
        try:
            # Download and process data
            download_results = await self.tardis_connector.download_daily_data_direct(
                tardis_exchange=tardis_exchange,
                tardis_symbol=tardis_symbol,
                date=date,
                data_types=[data_type]
            )
            
            if data_type not in download_results:
                result['errors'].append(f"No data returned for {data_type}")
                result['valid'] = False
                return result
            
            df = download_results[data_type]
            
            if df.empty:
                result['warnings'].append("Empty DataFrame returned")
                return result
            
            # Calculate data signature
            result['signatures']['data_hash'] = self.calculate_data_signature(df)
            result['signatures']['row_count'] = len(df)
            
            # Validate timestamp ordering
            timestamp_validation = self.validate_timestamp_ordering(df)
            result['validation_results']['timestamp'] = timestamp_validation
            if not timestamp_validation['valid']:
                result['valid'] = False
                result['errors'].extend(timestamp_validation['errors'])
            result['warnings'].extend(timestamp_validation['warnings'])
            
            # Validate data ranges
            range_validation = self.validate_data_ranges(df, data_type)
            result['validation_results']['ranges'] = range_validation
            if not range_validation['valid']:
                result['valid'] = False
                result['errors'].extend(range_validation['errors'])
            result['warnings'].extend(range_validation['warnings'])
            
            # Validate data consistency
            consistency_validation = self.validate_data_consistency(df, data_type)
            result['validation_results']['consistency'] = consistency_validation
            if not consistency_validation['valid']:
                result['valid'] = False
                result['errors'].extend(consistency_validation['errors'])
            result['warnings'].extend(consistency_validation['warnings'])
            
        except Exception as e:
            result['errors'].append(f"Validation failed: {str(e)}")
            result['valid'] = False
        
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


@pytest.fixture
async def signature_validator():
    """Fixture for signature validation tests"""
    validator = SignatureValidationTests()
    await validator.setup()
    yield validator
    await validator.teardown()


@pytest.mark.asyncio
@pytest.mark.skip(reason="Signature validation methods not implemented - TODO: Implement validation logic")
class TestDataSignatures:
    """Test data signature validation"""
    
    async def test_data_signature_consistency(self, signature_validator):
        """Test that data signatures are consistent across multiple downloads"""
        venue = 'binance'
        instrument = TEST_INSTRUMENTS[venue]['trades']
        
        # Download same data multiple times
        signatures = []
        for i in range(3):
            result = await signature_validator.download_and_validate_data(
                instrument['tardis_exchange'],
                instrument['tardis_symbol'],
                TEST_DATE,
                'trades'
            )
            
            if result['valid'] and 'data_hash' in result['signatures']:
                signatures.append(result['signatures']['data_hash'])
        
        # All signatures should be identical
        if len(signatures) > 1:
            assert len(set(signatures)) == 1, f"Data signatures not consistent: {signatures}"
            logger.info(f"✅ Data signature consistency verified for {venue}")
    
    async def test_timestamp_ordering_all_venues(self, signature_validator):
        """Test timestamp ordering for all venues and data types"""
        results = []
        
        for venue, instruments in TEST_INSTRUMENTS.items():
            for data_type, instrument in instruments.items():
                logger.info(f"Testing timestamp ordering for {venue}:{data_type}")
                
                result = await signature_validator.download_and_validate_data(
                    instrument['tardis_exchange'],
                    instrument['tardis_symbol'],
                    TEST_DATE,
                    data_type
                )
                
                results.append({
                    'venue': venue,
                    'data_type': data_type,
                    'valid': result['valid'],
                    'timestamp_validation': result['validation_results'].get('timestamp', {})
                })
                
                # Assert timestamp ordering is valid
                timestamp_validation = result['validation_results'].get('timestamp', {})
                assert timestamp_validation.get('valid', False), f"Timestamp ordering failed for {venue}:{data_type}: {timestamp_validation.get('errors', [])}"
        
        logger.info(f"✅ Timestamp ordering validated for {len(results)} venue/data_type combinations")
    
    async def test_data_ranges_all_types(self, signature_validator):
        """Test data ranges for all data types"""
        results = []
        
        for venue, instruments in TEST_INSTRUMENTS.items():
            for data_type, instrument in instruments.items():
                logger.info(f"Testing data ranges for {venue}:{data_type}")
                
                result = await signature_validator.download_and_validate_data(
                    instrument['tardis_exchange'],
                    instrument['tardis_symbol'],
                    TEST_DATE,
                    data_type
                )
                
                results.append({
                    'venue': venue,
                    'data_type': data_type,
                    'valid': result['valid'],
                    'range_validation': result['validation_results'].get('ranges', {})
                })
                
                # Assert data ranges are valid
                range_validation = result['validation_results'].get('ranges', {})
                assert range_validation.get('valid', False), f"Data ranges failed for {venue}:{data_type}: {range_validation.get('errors', [])}"
        
        logger.info(f"✅ Data ranges validated for {len(results)} venue/data_type combinations")
    
    async def test_data_consistency_all_types(self, signature_validator):
        """Test data consistency for all data types"""
        results = []
        
        for venue, instruments in TEST_INSTRUMENTS.items():
            for data_type, instrument in instruments.items():
                logger.info(f"Testing data consistency for {venue}:{data_type}")
                
                result = await signature_validator.download_and_validate_data(
                    instrument['tardis_exchange'],
                    instrument['tardis_symbol'],
                    TEST_DATE,
                    data_type
                )
                
                results.append({
                    'venue': venue,
                    'data_type': data_type,
                    'valid': result['valid'],
                    'consistency_validation': result['validation_results'].get('consistency', {})
                })
                
                # Assert data consistency is valid
                consistency_validation = result['validation_results'].get('consistency', {})
                assert consistency_validation.get('valid', False), f"Data consistency failed for {venue}:{data_type}: {consistency_validation.get('errors', [])}"
        
        logger.info(f"✅ Data consistency validated for {len(results)} venue/data_type combinations")


@pytest.mark.asyncio
@pytest.mark.skip(reason="Signature validation methods not implemented - TODO: Implement validation logic")
class TestCrossVenueConsistency:
    """Test consistency across different venues"""
    
    async def test_price_consistency_across_venues(self, signature_validator):
        """Test that prices are consistent across venues for the same asset"""
        # Test BTC prices across different venues
        btc_venues = [
            ('binance', 'BTCUSDT'),
            ('deribit', 'BTC-PERPETUAL')
        ]
        
        prices = {}
        
        for venue, symbol in btc_venues:
            result = await signature_validator.download_and_validate_data(
                venue, symbol, TEST_DATE, 'trades'
            )
            
            if result['valid'] and 'validation_results' in result:
                # Extract price statistics
                range_stats = result['validation_results'].get('ranges', {}).get('range_stats', {})
                if 'price' in range_stats:
                    prices[venue] = {
                        'min': range_stats['price']['min'],
                        'max': range_stats['price']['max'],
                        'mean': range_stats['price']['mean']
                    }
        
        # Compare prices across venues (should be within reasonable range)
        if len(prices) > 1:
            venue_names = list(prices.keys())
            price1 = prices[venue_names[0]]['mean']
            price2 = prices[venue_names[1]]['mean']
            
            # Prices should be within 10% of each other
            price_diff = abs(price1 - price2) / max(price1, price2)
            assert price_diff < 0.1, f"Price difference too large between {venue_names[0]} ({price1}) and {venue_names[1]} ({price2})"
            
            logger.info(f"✅ Price consistency verified across venues: {prices}")


if __name__ == "__main__":
    # Run tests directly
    pytest.main([__file__, "-v"])
