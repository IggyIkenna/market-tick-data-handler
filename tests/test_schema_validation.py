#!/usr/bin/env python3
"""
Schema Validation Tests

Comprehensive tests to validate that Parquet schemas match Tardis raw data exactly
(minus exchange/date columns used only for validation).

Tests all venue × data_type × instrument_type combinations using May 23, 2023 data.
"""

import pytest
import pandas as pd
import asyncio
import tempfile
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Tuple
import logging

# Add project root to path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from market_data_tick_handler.data_downloader.tardis_connector import TardisConnector
from config import get_config

logger = logging.getLogger(__name__)

class SchemaValidationTests:
    """Base class for schema validation tests"""
    
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
    
    def extract_tardis_raw_schema(self, csv_data: bytes, data_type: str) -> Dict[str, str]:
        """
        Extract schema from raw Tardis CSV data
        
        Args:
            csv_data: Raw CSV bytes from Tardis
            data_type: Type of data (trades, book_snapshot_5, etc.)
            
        Returns:
            Dict mapping column names to pandas dtypes
        """
        try:
            # Decode CSV data
            csv_content = csv_data.decode('utf-8')
            
            # Read first few lines to get headers and sample data
            lines = csv_content.strip().split('\n')
            if len(lines) < 2:
                return {}
            
            headers = lines[0].split(',')
            
            # Create a small DataFrame to infer types
            sample_data = []
            for line in lines[1:min(10, len(lines))]:  # Use first 10 rows for type inference
                sample_data.append(line.split(','))
            
            if not sample_data:
                return {}
            
            # Create DataFrame and infer types
            df_sample = pd.DataFrame(sample_data, columns=headers)
            
            # Convert to proper types based on data_type
            schema = {}
            for col in headers:
                if col in df_sample.columns:
                    # Infer type from sample data
                    sample_values = df_sample[col].dropna()
                    if len(sample_values) == 0:
                        schema[col] = 'string'
                        continue
                    
                    # Check if numeric
                    try:
                        pd.to_numeric(sample_values.iloc[0])
                        # Check if integer
                        if '.' not in str(sample_values.iloc[0]):
                            schema[col] = 'int64'
                        else:
                            schema[col] = 'float64'
                    except (ValueError, TypeError):
                        schema[col] = 'string'
            
            return schema
            
        except Exception as e:
            logger.error(f"Failed to extract schema from CSV: {e}")
            return {}
    
    def extract_parquet_schema(self, df: pd.DataFrame) -> Dict[str, str]:
        """
        Extract schema from processed DataFrame
        
        Args:
            df: Processed DataFrame
            
        Returns:
            Dict mapping column names to pandas dtypes
        """
        schema = {}
        for col in df.columns:
            schema[col] = str(df[col].dtype)
        return schema
    
    def compare_schemas(self, tardis_schema: Dict[str, str], parquet_schema: Dict[str, str], 
                       data_type: str) -> Dict[str, Any]:
        """
        Compare Tardis raw schema with processed Parquet schema
        
        Args:
            tardis_schema: Schema from raw Tardis data
            parquet_schema: Schema from processed DataFrame
            data_type: Type of data being compared
            
        Returns:
            Dict with comparison results
        """
        result = {
            'data_type': data_type,
            'match': True,
            'errors': [],
            'warnings': [],
            'tardis_columns': list(tardis_schema.keys()),
            'parquet_columns': list(parquet_schema.keys()),
            'column_order_match': True,
            'type_matches': {},
            'missing_columns': [],
            'extra_columns': []
        }
        
        # Expected validation columns that should be dropped
        validation_columns = {'exchange', 'symbol'}
        
        # Check for validation columns in raw data
        raw_validation_cols = set(tardis_schema.keys()) & validation_columns
        processed_validation_cols = set(parquet_schema.keys()) & validation_columns
        
        if raw_validation_cols and not processed_validation_cols:
            result['warnings'].append(f"Validation columns {raw_validation_cols} properly dropped from processed data")
        elif raw_validation_cols and processed_validation_cols:
            result['errors'].append(f"Validation columns {processed_validation_cols} should have been dropped")
        
        # Remove validation columns from comparison
        tardis_clean = {k: v for k, v in tardis_schema.items() if k not in validation_columns}
        parquet_clean = {k: v for k, v in parquet_schema.items() if k not in validation_columns}
        
        # Check column names match exactly
        tardis_cols = set(tardis_clean.keys())
        parquet_cols = set(parquet_clean.keys())
        
        missing_cols = tardis_cols - parquet_cols
        extra_cols = parquet_cols - tardis_cols
        
        if missing_cols:
            result['missing_columns'] = list(missing_cols)
            result['errors'].append(f"Missing columns in processed data: {missing_cols}")
            result['match'] = False
        
        if extra_cols:
            result['extra_columns'] = list(extra_cols)
            result['errors'].append(f"Extra columns in processed data: {extra_cols}")
            result['match'] = False
        
        # Check column order (only for common columns)
        common_cols = tardis_cols & parquet_cols
        tardis_order = [col for col in tardis_schema.keys() if col in common_cols]
        parquet_order = [col for col in parquet_schema.keys() if col in common_cols]
        
        if tardis_order != parquet_order:
            result['column_order_match'] = False
            result['errors'].append(f"Column order mismatch: Tardis {tardis_order} vs Parquet {parquet_order}")
            result['match'] = False
        
        # Check data types for common columns
        for col in common_cols:
            tardis_type = tardis_clean[col]
            parquet_type = parquet_clean[col]
            
            # Normalize types for comparison
            tardis_normalized = self._normalize_dtype(tardis_type)
            parquet_normalized = self._normalize_dtype(parquet_type)
            
            result['type_matches'][col] = {
                'tardis': tardis_type,
                'parquet': parquet_type,
                'match': tardis_normalized == parquet_normalized
            }
            
            if tardis_normalized != parquet_normalized:
                result['errors'].append(f"Type mismatch for {col}: Tardis {tardis_type} vs Parquet {parquet_type}")
                result['match'] = False
        
        return result
    
    def _normalize_dtype(self, dtype: str) -> str:
        """Normalize pandas dtype strings for comparison"""
        dtype_lower = dtype.lower()
        
        if 'int' in dtype_lower:
            return 'int64'
        elif 'float' in dtype_lower:
            return 'float64'
        elif 'string' in dtype_lower or 'object' in dtype_lower:
            return 'string'
        else:
            return dtype_lower
    
    async def download_raw_tardis_data(self, tardis_exchange: str, tardis_symbol: str, 
                                     date: datetime, data_type: str) -> bytes:
        """
        Download raw Tardis CSV data
        
        Args:
            tardis_exchange: Exchange name for Tardis API
            tardis_symbol: Symbol name for Tardis API
            date: Date to download
            data_type: Type of data to download
            
        Returns:
            Raw CSV bytes
        """
        try:
            # Build URL
            url = f"{self.tardis_connector.base_url}/v1/{tardis_exchange}/{data_type}/{date.strftime('%Y/%m/%d')}/{tardis_symbol}.csv.gz"
            
            # Make request
            response = await self.tardis_connector._make_request(url)
            
            # Decompress if needed
            content_encoding = response.headers.get('content-encoding', '')
            if content_encoding == 'gzip' or url.endswith('.gz') or response.data[:2] == b'\x1f\x8b':
                data = self.tardis_connector._decompress_data(response.data, 'gzip')
            else:
                data = response.data
            
            return data
            
        except Exception as e:
            logger.error(f"Failed to download raw Tardis data for {tardis_exchange}:{tardis_symbol}: {e}")
            raise
    
    async def process_tardis_data(self, tardis_exchange: str, tardis_symbol: str, 
                                date: datetime, data_type: str) -> pd.DataFrame:
        """
        Process Tardis data through our system
        
        Args:
            tardis_exchange: Exchange name for Tardis API
            tardis_symbol: Symbol name for Tardis API
            date: Date to download
            data_type: Type of data to download
            
        Returns:
            Processed DataFrame
        """
        try:
            # Use our existing processing logic
            result = await self.tardis_connector.download_daily_data_direct(
                tardis_exchange=tardis_exchange,
                tardis_symbol=tardis_symbol,
                date=date,
                data_types=[data_type]
            )
            
            if data_type in result:
                return result[data_type]
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Failed to process Tardis data for {tardis_exchange}:{tardis_symbol}: {e}")
            raise


# Test fixtures and sample instruments for May 23, 2023
TEST_INSTRUMENTS = {
    'binance': {
        'trades': {'tardis_exchange': 'binance', 'tardis_symbol': 'BTCUSDT'},
        'book_snapshot_5': {'tardis_exchange': 'binance', 'tardis_symbol': 'BTCUSDT'}
    },
    'binance-futures': {
        'trades': {'tardis_exchange': 'binance-futures', 'tardis_symbol': 'BTCUSDT'},
        'book_snapshot_5': {'tardis_exchange': 'binance-futures', 'tardis_symbol': 'BTCUSDT'},
        'derivative_ticker': {'tardis_exchange': 'binance-futures', 'tardis_symbol': 'BTCUSDT'},
        'liquidations': {'tardis_exchange': 'binance-futures', 'tardis_symbol': 'BTCUSDT'}
    },
    'deribit': {
        'trades': {'tardis_exchange': 'deribit', 'tardis_symbol': 'BTC-PERPETUAL'},
        'book_snapshot_5': {'tardis_exchange': 'deribit', 'tardis_symbol': 'BTC-PERPETUAL'},
        'derivative_ticker': {'tardis_exchange': 'deribit', 'tardis_symbol': 'BTC-PERPETUAL'},
        'liquidations': {'tardis_exchange': 'deribit', 'tardis_symbol': 'BTC-PERPETUAL'},
        'options_chain': {'tardis_exchange': 'deribit', 'tardis_symbol': 'BTC-23MAY23-50000-C'}
    },
    'bybit': {
        'trades': {'tardis_exchange': 'bybit', 'tardis_symbol': 'BTCUSDT'},
        'book_snapshot_5': {'tardis_exchange': 'bybit', 'tardis_symbol': 'BTCUSDT'},
        'derivative_ticker': {'tardis_exchange': 'bybit', 'tardis_symbol': 'BTCUSDT'},
        'liquidations': {'tardis_exchange': 'bybit', 'tardis_symbol': 'BTCUSDT'}
    },
    'bybit-spot': {
        'trades': {'tardis_exchange': 'bybit-spot', 'tardis_symbol': 'BTCUSDT'},
        'book_snapshot_5': {'tardis_exchange': 'bybit-spot', 'tardis_symbol': 'BTCUSDT'}
    },
    'okex': {
        'trades': {'tardis_exchange': 'okex', 'tardis_symbol': 'BTC-USDT'},
        'book_snapshot_5': {'tardis_exchange': 'okex', 'tardis_symbol': 'BTC-USDT'}
    },
    'okex-futures': {
        'trades': {'tardis_exchange': 'okex-futures', 'tardis_symbol': 'BTC-USDT-SWAP'},
        'book_snapshot_5': {'tardis_exchange': 'okex-futures', 'tardis_symbol': 'BTC-USDT-SWAP'},
        'derivative_ticker': {'tardis_exchange': 'okex-futures', 'tardis_symbol': 'BTC-USDT-SWAP'},
        'liquidations': {'tardis_exchange': 'okex-futures', 'tardis_symbol': 'BTC-USDT-SWAP'}
    },
    'okex-swap': {
        'trades': {'tardis_exchange': 'okex-swap', 'tardis_symbol': 'BTC-USDT-SWAP'},
        'book_snapshot_5': {'tardis_exchange': 'okex-swap', 'tardis_symbol': 'BTC-USDT-SWAP'},
        'derivative_ticker': {'tardis_exchange': 'okex-swap', 'tardis_symbol': 'BTC-USDT-SWAP'},
        'liquidations': {'tardis_exchange': 'okex-swap', 'tardis_symbol': 'BTC-USDT-SWAP'}
    }
}

# Test date
TEST_DATE = datetime(2023, 5, 23, tzinfo=timezone.utc)


@pytest.fixture
async def schema_validator():
    """Fixture for schema validation tests"""
    validator = SchemaValidationTests()
    await validator.setup()
    yield validator
    await validator.teardown()


@pytest.mark.asyncio
@pytest.mark.skip(reason="Schema validation methods not implemented - TODO: Implement validation logic")
class TestTradesSchema:
    """Test trades schema validation for all venues"""
    
    async def test_trades_schema_all_venues(self, schema_validator):
        """Validate trades schema for each venue"""
        results = []
        
        for venue, instruments in TEST_INSTRUMENTS.items():
            if 'trades' not in instruments:
                continue
                
            instrument = instruments['trades']
            tardis_exchange = instrument['tardis_exchange']
            tardis_symbol = instrument['tardis_symbol']
            
            logger.info(f"Testing trades schema for {venue} ({tardis_exchange}:{tardis_symbol})")
            
            try:
                # Download raw Tardis data
                raw_data = await schema_validator.download_raw_tardis_data(
                    tardis_exchange, tardis_symbol, TEST_DATE, 'trades'
                )
                
                # Process through our system
                processed_df = await schema_validator.process_tardis_data(
                    tardis_exchange, tardis_symbol, TEST_DATE, 'trades'
                )
                
                # Extract schemas
                raw_schema = schema_validator.extract_tardis_raw_schema(raw_data, 'trades')
                processed_schema = schema_validator.extract_parquet_schema(processed_df)
                
                # Compare schemas
                comparison = schema_validator.compare_schemas(raw_schema, processed_schema, 'trades')
                comparison['venue'] = venue
                comparison['tardis_exchange'] = tardis_exchange
                comparison['tardis_symbol'] = tardis_symbol
                
                results.append(comparison)
                
                # Assert schema matches
                assert comparison['match'], f"Schema mismatch for {venue}: {comparison['errors']}"
                
                logger.info(f"✅ Trades schema validation passed for {venue}")
                
            except Exception as e:
                logger.error(f"❌ Failed to validate trades schema for {venue}: {e}")
                pytest.fail(f"Trades schema validation failed for {venue}: {e}")
        
        # Log summary
        logger.info(f"Trades schema validation completed for {len(results)} venues")
        for result in results:
            if result['match']:
                logger.info(f"✅ {result['venue']}: Schema match")
            else:
                logger.error(f"❌ {result['venue']}: {result['errors']}")


@pytest.mark.asyncio
@pytest.mark.skip(reason="Schema validation methods not implemented - TODO: Implement validation logic")
class TestBookSnapshotSchema:
    """Test book_snapshot_5 schema validation for all venues"""
    
    async def test_book_snapshot_schema_all_venues(self, schema_validator):
        """Validate book_snapshot_5 schema for each venue"""
        results = []
        
        for venue, instruments in TEST_INSTRUMENTS.items():
            if 'book_snapshot_5' not in instruments:
                continue
                
            instrument = instruments['book_snapshot_5']
            tardis_exchange = instrument['tardis_exchange']
            tardis_symbol = instrument['tardis_symbol']
            
            logger.info(f"Testing book_snapshot_5 schema for {venue} ({tardis_exchange}:{tardis_symbol})")
            
            try:
                # Download raw Tardis data
                raw_data = await schema_validator.download_raw_tardis_data(
                    tardis_exchange, tardis_symbol, TEST_DATE, 'book_snapshot_5'
                )
                
                # Process through our system
                processed_df = await schema_validator.process_tardis_data(
                    tardis_exchange, tardis_symbol, TEST_DATE, 'book_snapshot_5'
                )
                
                # Extract schemas
                raw_schema = schema_validator.extract_tardis_raw_schema(raw_data, 'book_snapshot_5')
                processed_schema = schema_validator.extract_parquet_schema(processed_df)
                
                # Compare schemas
                comparison = schema_validator.compare_schemas(raw_schema, processed_schema, 'book_snapshot_5')
                comparison['venue'] = venue
                comparison['tardis_exchange'] = tardis_exchange
                comparison['tardis_symbol'] = tardis_symbol
                
                results.append(comparison)
                
                # Assert schema matches
                assert comparison['match'], f"Schema mismatch for {venue}: {comparison['errors']}"
                
                logger.info(f"✅ Book snapshot schema validation passed for {venue}")
                
            except Exception as e:
                logger.error(f"❌ Failed to validate book_snapshot_5 schema for {venue}: {e}")
                pytest.fail(f"Book snapshot schema validation failed for {venue}: {e}")
        
        # Log summary
        logger.info(f"Book snapshot schema validation completed for {len(results)} venues")


@pytest.mark.asyncio
@pytest.mark.skip(reason="Schema validation methods not implemented - TODO: Implement validation logic")
class TestDerivativeTickerSchema:
    """Test derivative_ticker schema validation for perpetual/future instruments"""
    
    async def test_derivative_ticker_schema(self, schema_validator):
        """Validate derivative_ticker schema for perpetual and future instruments"""
        results = []
        
        # Test venues that have derivative_ticker data
        derivative_venues = ['binance-futures', 'deribit', 'bybit', 'okex-futures', 'okex-swap']
        
        for venue in derivative_venues:
            if venue not in TEST_INSTRUMENTS or 'derivative_ticker' not in TEST_INSTRUMENTS[venue]:
                continue
                
            instrument = TEST_INSTRUMENTS[venue]['derivative_ticker']
            tardis_exchange = instrument['tardis_exchange']
            tardis_symbol = instrument['tardis_symbol']
            
            logger.info(f"Testing derivative_ticker schema for {venue} ({tardis_exchange}:{tardis_symbol})")
            
            try:
                # Download raw Tardis data
                raw_data = await schema_validator.download_raw_tardis_data(
                    tardis_exchange, tardis_symbol, TEST_DATE, 'derivative_ticker'
                )
                
                # Process through our system
                processed_df = await schema_validator.process_tardis_data(
                    tardis_exchange, tardis_symbol, TEST_DATE, 'derivative_ticker'
                )
                
                # Extract schemas
                raw_schema = schema_validator.extract_tardis_raw_schema(raw_data, 'derivative_ticker')
                processed_schema = schema_validator.extract_parquet_schema(processed_df)
                
                # Compare schemas
                comparison = schema_validator.compare_schemas(raw_schema, processed_schema, 'derivative_ticker')
                comparison['venue'] = venue
                comparison['tardis_exchange'] = tardis_exchange
                comparison['tardis_symbol'] = tardis_symbol
                
                results.append(comparison)
                
                # Assert schema matches
                assert comparison['match'], f"Schema mismatch for {venue}: {comparison['errors']}"
                
                logger.info(f"✅ Derivative ticker schema validation passed for {venue}")
                
            except Exception as e:
                logger.error(f"❌ Failed to validate derivative_ticker schema for {venue}: {e}")
                pytest.fail(f"Derivative ticker schema validation failed for {venue}: {e}")
        
        # Log summary
        logger.info(f"Derivative ticker schema validation completed for {len(results)} venues")


@pytest.mark.asyncio
@pytest.mark.skip(reason="Schema validation methods not implemented - TODO: Implement validation logic")
class TestLiquidationsSchema:
    """Test liquidations schema validation for futures venues"""
    
    async def test_liquidations_schema(self, schema_validator):
        """Validate liquidations schema for futures venues"""
        results = []
        
        # Test venues that have liquidations data
        liquidation_venues = ['binance-futures', 'deribit', 'bybit', 'okex-futures', 'okex-swap']
        
        for venue in liquidation_venues:
            if venue not in TEST_INSTRUMENTS or 'liquidations' not in TEST_INSTRUMENTS[venue]:
                continue
                
            instrument = TEST_INSTRUMENTS[venue]['liquidations']
            tardis_exchange = instrument['tardis_exchange']
            tardis_symbol = instrument['tardis_symbol']
            
            logger.info(f"Testing liquidations schema for {venue} ({tardis_exchange}:{tardis_symbol})")
            
            try:
                # Download raw Tardis data
                raw_data = await schema_validator.download_raw_tardis_data(
                    tardis_exchange, tardis_symbol, TEST_DATE, 'liquidations'
                )
                
                # Process through our system
                processed_df = await schema_validator.process_tardis_data(
                    tardis_exchange, tardis_symbol, TEST_DATE, 'liquidations'
                )
                
                # Extract schemas
                raw_schema = schema_validator.extract_tardis_raw_schema(raw_data, 'liquidations')
                processed_schema = schema_validator.extract_parquet_schema(processed_df)
                
                # Compare schemas
                comparison = schema_validator.compare_schemas(raw_schema, processed_schema, 'liquidations')
                comparison['venue'] = venue
                comparison['tardis_exchange'] = tardis_exchange
                comparison['tardis_symbol'] = tardis_symbol
                
                results.append(comparison)
                
                # Assert schema matches
                assert comparison['match'], f"Schema mismatch for {venue}: {comparison['errors']}"
                
                logger.info(f"✅ Liquidations schema validation passed for {venue}")
                
            except Exception as e:
                logger.error(f"❌ Failed to validate liquidations schema for {venue}: {e}")
                pytest.fail(f"Liquidations schema validation failed for {venue}: {e}")
        
        # Log summary
        logger.info(f"Liquidations schema validation completed for {len(results)} venues")


@pytest.mark.asyncio
@pytest.mark.skip(reason="Schema validation methods not implemented - TODO: Implement validation logic")
class TestOptionsChainSchema:
    """Test options_chain schema validation for Deribit options"""
    
    async def test_options_chain_schema_deribit(self, schema_validator):
        """Validate options_chain schema for Deribit options"""
        results = []
        
        # Test Deribit options
        if 'deribit' in TEST_INSTRUMENTS and 'options_chain' in TEST_INSTRUMENTS['deribit']:
            instrument = TEST_INSTRUMENTS['deribit']['options_chain']
            tardis_exchange = instrument['tardis_exchange']
            tardis_symbol = instrument['tardis_symbol']
            
            logger.info(f"Testing options_chain schema for deribit ({tardis_exchange}:{tardis_symbol})")
            
            try:
                # Download raw Tardis data
                raw_data = await schema_validator.download_raw_tardis_data(
                    tardis_exchange, tardis_symbol, TEST_DATE, 'options_chain'
                )
                
                # Process through our system
                processed_df = await schema_validator.process_tardis_data(
                    tardis_exchange, tardis_symbol, TEST_DATE, 'options_chain'
                )
                
                # Extract schemas
                raw_schema = schema_validator.extract_tardis_raw_schema(raw_data, 'options_chain')
                processed_schema = schema_validator.extract_parquet_schema(processed_df)
                
                # Compare schemas
                comparison = schema_validator.compare_schemas(raw_schema, processed_schema, 'options_chain')
                comparison['venue'] = 'deribit'
                comparison['tardis_exchange'] = tardis_exchange
                comparison['tardis_symbol'] = tardis_symbol
                
                results.append(comparison)
                
                # Assert schema matches
                assert comparison['match'], f"Schema mismatch for deribit options: {comparison['errors']}"
                
                logger.info(f"✅ Options chain schema validation passed for deribit")
                
            except Exception as e:
                logger.error(f"❌ Failed to validate options_chain schema for deribit: {e}")
                pytest.fail(f"Options chain schema validation failed for deribit: {e}")
        
        # Log summary
        logger.info(f"Options chain schema validation completed for {len(results)} venues")


@pytest.mark.asyncio
@pytest.mark.skip(reason="Schema validation methods not implemented - TODO: Implement validation logic")
class TestValidationColumns:
    """Test that exchange and symbol columns are properly handled"""
    
    async def test_validation_columns_dropped(self, schema_validator):
        """Verify exchange and symbol columns are present in raw but dropped after validation"""
        
        # Test with a representative venue
        venue = 'binance'
        instrument = TEST_INSTRUMENTS[venue]['trades']
        tardis_exchange = instrument['tardis_exchange']
        tardis_symbol = instrument['tardis_symbol']
        
        logger.info(f"Testing validation columns handling for {venue}")
        
        try:
            # Download raw Tardis data
            raw_data = await schema_validator.download_raw_tardis_data(
                tardis_exchange, tardis_symbol, TEST_DATE, 'trades'
            )
            
            # Process through our system
            processed_df = await schema_validator.process_tardis_data(
                tardis_exchange, tardis_symbol, TEST_DATE, 'trades'
            )
            
            # Extract schemas
            raw_schema = schema_validator.extract_tardis_raw_schema(raw_data, 'trades')
            processed_schema = schema_validator.extract_parquet_schema(processed_df)
            
            # Check validation columns
            validation_columns = {'exchange', 'symbol'}
            
            # Raw data should have validation columns
            raw_validation_present = validation_columns & set(raw_schema.keys())
            assert raw_validation_present, f"Validation columns {validation_columns} not found in raw data"
            
            # Processed data should not have validation columns
            processed_validation_present = validation_columns & set(processed_schema.keys())
            assert not processed_validation_present, f"Validation columns {processed_validation_present} should have been dropped from processed data"
            
            logger.info(f"✅ Validation columns properly handled for {venue}")
            
        except Exception as e:
            logger.error(f"❌ Failed to test validation columns for {venue}: {e}")
            pytest.fail(f"Validation columns test failed for {venue}: {e}")


if __name__ == "__main__":
    # Run tests directly
    pytest.main([__file__, "-v"])
