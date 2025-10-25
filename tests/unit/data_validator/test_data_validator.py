"""
Unit tests for data validator
"""

import pytest
from unittest.mock import Mock, patch
import pandas as pd
from datetime import datetime, timezone
from market_data_tick_handler.data_validator.data_validator import DataValidator


class TestDataValidator:
    """Test DataValidator class"""
    
    def test_init(self, mock_gcs_client):
        """Test validator initialization"""
        mock_client, mock_bucket = mock_gcs_client
        validator = DataValidator("test-bucket")
        assert validator.gcs_bucket == "test-bucket"
        assert validator.client == mock_client
        assert validator.bucket == mock_bucket
    
    def test_check_missing_data_no_instruments(self, mock_gcs_client):
        """Test missing data check with no instruments"""
        mock_client, mock_bucket = mock_gcs_client
        validator = DataValidator("test-bucket")
        
        # Mock empty instrument definitions
        with patch.object(validator, '_get_instrument_definitions') as mock_get_instruments:
            mock_get_instruments.return_value = pd.DataFrame()
            
            start_date = datetime(2023, 5, 23, tzinfo=timezone.utc)
            end_date = datetime(2023, 5, 25, tzinfo=timezone.utc)
            
            result = validator.check_missing_data(start_date, end_date)
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0
    
    def test_check_missing_data_with_instruments(self, mock_gcs_client):
        """Test missing data check with instruments"""
        mock_client, mock_bucket = mock_gcs_client
        validator = DataValidator("test-bucket")
        
        # Mock instrument definitions
        instruments_df = pd.DataFrame({
            'instrument_key': ['BINANCE:SPOT_PAIR:BTC-USDT', 'DERIBIT:PERP:BTC-USD'],
            'data_types': ['trades,book_snapshot_5', 'trades,book_snapshot_5,derivative_ticker,liquidations'],
            'venue': ['BINANCE', 'DERIBIT'],
            'instrument_type': ['SPOT_PAIR', 'PERP']
        })
        
        with patch.object(validator, '_get_instrument_definitions') as mock_get_instruments:
            mock_get_instruments.return_value = instruments_df
            
            # Mock GCS data availability
            with patch.object(validator, '_check_data_availability') as mock_check_availability:
                mock_check_availability.return_value = {
                    'BINANCE:SPOT_PAIR:BTC-USDT': ['trades'],  # Missing book_snapshot_5
                    'DERIBIT:PERP:BTC-USD': ['trades', 'book_snapshot_5']  # Missing derivative_ticker, liquidations
                }
                
                start_date = datetime(2023, 5, 23, tzinfo=timezone.utc)
                end_date = datetime(2023, 5, 23, tzinfo=timezone.utc)
                
                result = validator.check_missing_data(start_date, end_date)
                
                assert isinstance(result, pd.DataFrame)
                assert len(result) == 3  # 1 missing for BTC-USDT, 2 missing for BTC-USD
                
                # Check specific missing data
                missing_data = result.to_dict('records')
                assert any(item['instrument_key'] == 'BINANCE:SPOT_PAIR:BTC-USDT' and item['data_type'] == 'book_snapshot_5' for item in missing_data)
                assert any(item['instrument_key'] == 'DERIBIT:PERP:BTC-USD' and item['data_type'] == 'derivative_ticker' for item in missing_data)
                assert any(item['instrument_key'] == 'DERIBIT:PERP:BTC-USD' and item['data_type'] == 'liquidations' for item in missing_data)
    
    def test_check_missing_data_with_venue_filter(self, mock_gcs_client):
        """Test missing data check with venue filter"""
        mock_client, mock_bucket = mock_gcs_client
        validator = DataValidator("test-bucket")
        
        # Mock instrument definitions
        instruments_df = pd.DataFrame({
            'instrument_key': ['BINANCE:SPOT_PAIR:BTC-USDT', 'DERIBIT:PERP:BTC-USD'],
            'data_types': ['trades,book_snapshot_5', 'trades,book_snapshot_5,derivative_ticker,liquidations'],
            'venue': ['BINANCE', 'DERIBIT'],
            'instrument_type': ['SPOT_PAIR', 'PERP']
        })
        
        with patch.object(validator, '_get_instrument_definitions') as mock_get_instruments:
            mock_get_instruments.return_value = instruments_df
            
            # Mock GCS data availability
            with patch.object(validator, '_check_data_availability') as mock_check_availability:
                mock_check_availability.return_value = {
                    'BINANCE:SPOT_PAIR:BTC-USDT': ['trades']  # Only check BINANCE
                }
                
                start_date = datetime(2023, 5, 23, tzinfo=timezone.utc)
                end_date = datetime(2023, 5, 23, tzinfo=timezone.utc)
                
                result = validator.check_missing_data(start_date, end_date, venues=['BINANCE'])
                
                assert isinstance(result, pd.DataFrame)
                assert len(result) == 1  # Only BINANCE missing data
                assert result.iloc[0]['instrument_key'] == 'BINANCE:SPOT_PAIR:BTC-USDT'
                assert result.iloc[0]['data_type'] == 'book_snapshot_5'
    
    def test_check_missing_data_with_instrument_type_filter(self, mock_gcs_client):
        """Test missing data check with instrument type filter"""
        mock_client, mock_bucket = mock_gcs_client
        validator = DataValidator("test-bucket")
        
        # Mock instrument definitions
        instruments_df = pd.DataFrame({
            'instrument_key': ['BINANCE:SPOT_PAIR:BTC-USDT', 'DERIBIT:PERP:BTC-USD'],
            'data_types': ['trades,book_snapshot_5', 'trades,book_snapshot_5,derivative_ticker,liquidations'],
            'venue': ['BINANCE', 'DERIBIT'],
            'instrument_type': ['SPOT_PAIR', 'PERP']
        })
        
        with patch.object(validator, '_get_instrument_definitions') as mock_get_instruments:
            mock_get_instruments.return_value = instruments_df
            
            # Mock GCS data availability
            with patch.object(validator, '_check_data_availability') as mock_check_availability:
                mock_check_availability.return_value = {
                    'DERIBIT:PERP:BTC-USD': ['trades']  # Only check PERP
                }
                
                start_date = datetime(2023, 5, 23, tzinfo=timezone.utc)
                end_date = datetime(2023, 5, 23, tzinfo=timezone.utc)
                
                result = validator.check_missing_data(start_date, end_date, instrument_types=['PERP'])
                
                assert isinstance(result, pd.DataFrame)
                assert len(result) == 3  # PERP missing 3 data types
                assert all(item['instrument_key'] == 'DERIBIT:PERP:BTC-USD' for item in result.to_dict('records'))
    
    def test_get_instrument_definitions(self, mock_gcs_client):
        """Test getting instrument definitions from GCS"""
        mock_client, mock_bucket = mock_gcs_client
        validator = DataValidator("test-bucket")
        
        # Mock GCS blob
        mock_blob = Mock()
        mock_blob.exists.return_value = True
        mock_bucket.blob.return_value = mock_blob
        
        # Mock parquet data
        mock_instruments_df = pd.DataFrame({
            'instrument_key': ['BINANCE:SPOT_PAIR:BTC-USDT'],
            'data_types': ['trades,book_snapshot_5'],
            'venue': ['BINANCE'],
            'instrument_type': ['SPOT_PAIR']
        })
        
        with patch('pandas.read_parquet') as mock_read_parquet:
            mock_read_parquet.return_value = mock_instruments_df
            
            date = datetime(2023, 5, 23, tzinfo=timezone.utc)
            result = validator._get_instrument_definitions(date)
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            assert result.iloc[0]['instrument_key'] == 'BINANCE:SPOT_PAIR:BTC-USDT'
    
    def test_get_instrument_definitions_not_found(self, mock_gcs_client):
        """Test getting instrument definitions when file doesn't exist"""
        mock_client, mock_bucket = mock_gcs_client
        validator = DataValidator("test-bucket")
        
        # Mock GCS blob that doesn't exist
        mock_blob = Mock()
        mock_blob.exists.return_value = False
        mock_bucket.blob.return_value = mock_blob
        
        date = datetime(2023, 5, 23, tzinfo=timezone.utc)
        result = validator._get_instrument_definitions(date)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
    
    def test_check_data_availability(self, mock_gcs_client):
        """Test checking data availability in GCS"""
        mock_client, mock_bucket = mock_gcs_client
        validator = DataValidator("test-bucket")
        
        # Mock GCS blob listing
        mock_blob1 = Mock()
        mock_blob1.name = "raw_tick_data/by_date/day-2023-05-23/data_type-trades/BINANCE:SPOT_PAIR:BTC-USDT.parquet"
        
        mock_blob2 = Mock()
        mock_blob2.name = "raw_tick_data/by_date/day-2023-05-23/data_type-book_snapshot_5/BINANCE:SPOT_PAIR:BTC-USDT.parquet"
        
        mock_bucket.list_blobs.return_value = [mock_blob1, mock_blob2]
        
        instruments = {
            'BINANCE:SPOT_PAIR:BTC-USDT': ['trades', 'book_snapshot_5', 'derivative_ticker']
        }
        
        date = datetime(2023, 5, 23, tzinfo=timezone.utc)
        result = validator._check_data_availability(instruments, date)
        
        assert 'BINANCE:SPOT_PAIR:BTC-USDT' in result
        assert 'trades' in result['BINANCE:SPOT_PAIR:BTC-USDT']
        assert 'book_snapshot_5' in result['BINANCE:SPOT_PAIR:BTC-USDT']
        assert 'derivative_ticker' not in result['BINANCE:SPOT_PAIR:BTC-USDT']
    
    def test_check_data_availability_no_data(self, mock_gcs_client):
        """Test checking data availability when no data exists"""
        mock_client, mock_bucket = mock_gcs_client
        validator = DataValidator("test-bucket")
        
        # Mock empty GCS blob listing
        mock_bucket.list_blobs.return_value = []
        
        instruments = {
            'BINANCE:SPOT_PAIR:BTC-USDT': ['trades', 'book_snapshot_5']
        }
        
        date = datetime(2023, 5, 23, tzinfo=timezone.utc)
        result = validator._check_data_availability(instruments, date)
        
        assert 'BINANCE:SPOT_PAIR:BTC-USDT' in result
        assert len(result['BINANCE:SPOT_PAIR:BTC-USDT']) == 0
    
    def test_generate_missing_data_report(self, mock_gcs_client):
        """Test generating missing data report"""
        mock_client, mock_bucket = mock_gcs_client
        validator = DataValidator("test-bucket")
        
        # Mock missing data
        missing_data = pd.DataFrame({
            'date': ['2023-05-23', '2023-05-23'],
            'instrument_key': ['BINANCE:SPOT_PAIR:BTC-USDT', 'DERIBIT:PERP:BTC-USD'],
            'data_type': ['book_snapshot_5', 'derivative_ticker']
        })
        
        with patch.object(validator, 'check_missing_data') as mock_check:
            mock_check.return_value = missing_data
            
            start_date = datetime(2023, 5, 23, tzinfo=timezone.utc)
            end_date = datetime(2023, 5, 23, tzinfo=timezone.utc)
            
            result = validator.generate_missing_data_report(start_date, end_date)
            
            assert isinstance(result, dict)
            assert 'missing_data' in result
            assert 'summary' in result
            assert 'total_missing' in result['summary']
            assert result['summary']['total_missing'] == 2
