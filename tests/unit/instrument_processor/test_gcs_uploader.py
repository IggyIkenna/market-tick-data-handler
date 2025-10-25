"""
Unit tests for GCS uploader
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from datetime import datetime, timezone
from market_data_tick_handler.instrument_processor.gcs_uploader import InstrumentGCSUploader


class TestInstrumentGCSUploader:
    """Test InstrumentGCSUploader class"""
    
    def test_init(self, mock_gcs_client):
        """Test uploader initialization"""
        mock_client, mock_bucket = mock_gcs_client
        uploader = InstrumentGCSUploader("test-bucket")
        assert uploader.bucket_name == "test-bucket"
    
    def test_validate_instrument_definitions_valid(self, sample_instrument_definition):
        """Test validation with valid instrument definitions"""
        uploader = InstrumentGCSUploader("test-bucket")
        
        # Convert to dict for testing
        inst_dict = sample_instrument_definition.model_dump()
        
        # Should not raise any exceptions
        validated = uploader._validate_instrument_definitions([inst_dict])
        assert len(validated) == 1
        assert validated[0]['instrument_key'] == 'BINANCE:SPOT_PAIR:BTC-USDT'
    
    def test_validate_instrument_definitions_invalid(self):
        """Test validation with invalid instrument definitions"""
        uploader = InstrumentGCSUploader("test-bucket")
        
        # Invalid instrument definition (missing required fields)
        invalid_inst = {
            'instrument_key': 'INVALID',
            'venue': 'BINANCE'
            # Missing required fields
        }
        
        with pytest.raises(ValueError):
            uploader._validate_instrument_definitions([invalid_inst])
    
    def test_generate_partition_path(self):
        """Test partition path generation"""
        uploader = InstrumentGCSUploader("test-bucket")
        
        date = datetime(2023, 5, 23, tzinfo=timezone.utc)
        path = uploader._generate_partition_path(date)
        
        expected = "instrument_availability/by_date/day-2023-05-23/instruments.parquet"
        assert path == expected
    
    def test_create_parquet_dataframe(self, sample_instrument_definition):
        """Test creating parquet dataframe from instrument definitions"""
        uploader = InstrumentGCSUploader("test-bucket")
        
        # Convert to dict for testing
        inst_dict = sample_instrument_definition.model_dump()
        
        df = uploader._create_parquet_dataframe([inst_dict])
        
        assert len(df) == 1
        assert df.iloc[0]['instrument_key'] == 'BINANCE:SPOT_PAIR:BTC-USDT'
        assert df.iloc[0]['venue'] == 'BINANCE'
        assert df.iloc[0]['instrument_type'] == 'SPOT_PAIR'
        assert df.iloc[0]['data_types'] == 'trades,book_snapshot_5'
        
        # Check that partitioning columns are added
        assert 'year' in df.columns
        assert 'month' in df.columns
        assert 'day' in df.columns
        assert 'date' in df.columns
        assert df.iloc[0]['year'] == 2023
        assert df.iloc[0]['month'] == 5
        assert df.iloc[0]['day'] == 23
        assert df.iloc[0]['date'] == '2023-05-23'
    
    @patch('pandas.DataFrame.to_parquet')
    def test_upload_instruments_success(self, mock_to_parquet, sample_instrument_definition, mock_gcs_client):
        """Test successful instrument upload"""
        mock_client, mock_bucket = mock_gcs_client
        uploader = InstrumentGCSUploader("test-bucket")
        
        # Convert to dict for testing
        inst_dict = sample_instrument_definition.model_dump()
        
        date = datetime(2023, 5, 23, tzinfo=timezone.utc)
        
        # Mock the upload process
        mock_blob = Mock()
        mock_bucket.blob.return_value = mock_blob
        
        result = uploader.upload_instruments([inst_dict], date)
        
        assert result['success'] is True
        assert result['uploaded_files'] == 1
        assert result['total_instruments'] == 1
        assert result['errors'] == []
        
        # Verify blob was created and uploaded
        mock_bucket.blob.assert_called_once()
        mock_blob.upload_from_string.assert_called_once()
    
    @patch('pandas.DataFrame.to_parquet')
    def test_upload_instruments_validation_error(self, mock_to_parquet, mock_gcs_client):
        """Test upload with validation error"""
        mock_client, mock_bucket = mock_gcs_client
        uploader = InstrumentGCSUploader("test-bucket")
        
        # Invalid instrument definition
        invalid_inst = {
            'instrument_key': 'INVALID',
            'venue': 'BINANCE'
            # Missing required fields
        }
        
        date = datetime(2023, 5, 23, tzinfo=timezone.utc)
        
        result = uploader.upload_instruments([invalid_inst], date)
        
        assert result['success'] is False
        assert result['uploaded_files'] == 0
        assert result['total_instruments'] == 1
        assert len(result['errors']) == 1
        assert 'validation' in result['errors'][0].lower()
    
    @patch('pandas.DataFrame.to_parquet')
    def test_upload_instruments_upload_error(self, mock_to_parquet, sample_instrument_definition, mock_gcs_client):
        """Test upload with GCS upload error"""
        mock_client, mock_bucket = mock_gcs_client
        uploader = InstrumentGCSUploader("test-bucket")
        
        # Convert to dict for testing
        inst_dict = sample_instrument_definition.model_dump()
        
        # Mock upload error
        mock_blob = Mock()
        mock_blob.upload_from_string.side_effect = Exception("Upload failed")
        mock_bucket.blob.return_value = mock_blob
        
        date = datetime(2023, 5, 23, tzinfo=timezone.utc)
        
        result = uploader.upload_instruments([inst_dict], date)
        
        assert result['success'] is False
        assert result['uploaded_files'] == 0
        assert result['total_instruments'] == 1
        assert len(result['errors']) == 1
        assert 'upload failed' in result['errors'][0].lower()
    
    def test_upload_instruments_empty_list(self, mock_gcs_client):
        """Test upload with empty instrument list"""
        mock_client, mock_bucket = mock_gcs_client
        uploader = InstrumentGCSUploader("test-bucket")
        
        date = datetime(2023, 5, 23, tzinfo=timezone.utc)
        
        result = uploader.upload_instruments([], date)
        
        assert result['success'] is True
        assert result['uploaded_files'] == 0
        assert result['total_instruments'] == 0
        assert result['errors'] == []
    
    def test_upload_instruments_multiple_instruments(self, mock_gcs_client):
        """Test upload with multiple instruments"""
        mock_client, mock_bucket = mock_gcs_client
        uploader = InstrumentGCSUploader("test-bucket")
        
        # Create multiple instrument definitions
        inst1 = {
            'instrument_key': 'BINANCE:SPOT_PAIR:BTC-USDT',
            'venue': 'BINANCE',
            'instrument_type': 'SPOT_PAIR',
            'available_from_datetime': '2023-05-23T00:00:00+00:00',
            'available_to_datetime': '2024-05-23T00:00:00+00:00',
            'data_types': 'trades,book_snapshot_5',
            'base_asset': 'BTC',
            'quote_asset': 'USDT',
            'settle_asset': 'USDT',
            'exchange_raw_symbol': 'BTCUSDT',
            'tardis_symbol': 'BTC-USDT',
            'tardis_exchange': 'binance',
            'data_provider': 'tardis',
            'venue_type': 'centralized',
            'asset_class': 'crypto',
            'inverse': False,
            'tick_size': '0.01',
            'min_size': '0.001',
            'settlement_type': '',
            'underlying': '',
            'ccxt_symbol': 'BTC/USDT',
            'ccxt_exchange': 'binance'
        }
        
        inst2 = {
            'instrument_key': 'DERIBIT:PERP:BTC-USD',
            'venue': 'DERIBIT',
            'instrument_type': 'PERP',
            'available_from_datetime': '2023-05-23T00:00:00+00:00',
            'available_to_datetime': '2024-05-23T00:00:00+00:00',
            'data_types': 'trades,book_snapshot_5,derivative_ticker,liquidations',
            'base_asset': 'BTC',
            'quote_asset': 'USD',
            'settle_asset': 'USD',
            'exchange_raw_symbol': 'BTC-PERPETUAL',
            'tardis_symbol': 'BTC-USD',
            'tardis_exchange': 'deribit',
            'data_provider': 'tardis',
            'venue_type': 'centralized',
            'asset_class': 'crypto',
            'inverse': False,
            'tick_size': '0.5',
            'min_size': '1',
            'settlement_type': '',
            'underlying': '',
            'ccxt_symbol': 'BTC/USD',
            'ccxt_exchange': 'deribit'
        }
        
        date = datetime(2023, 5, 23, tzinfo=timezone.utc)
        
        with patch('pandas.DataFrame.to_parquet'):
            result = uploader.upload_instruments([inst1, inst2], date)
        
        assert result['success'] is True
        assert result['uploaded_files'] == 1  # One file with both instruments
        assert result['total_instruments'] == 2
        assert result['errors'] == []
