"""
Integration tests for instrument pipeline
"""

import pytest
import os
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime, timezone
from market_data_tick_handler.instrument_processor.canonical_key_generator import CanonicalInstrumentKeyGenerator
from market_data_tick_handler.instrument_processor.gcs_uploader import InstrumentGCSUploader


@pytest.mark.integration
@pytest.mark.skipif(not os.getenv('INTEGRATION_TESTS'), reason="Integration tests require external services")
class TestInstrumentPipeline:
    """Test instrument generation and upload pipeline"""
    
    @pytest.mark.asyncio
    async def test_instrument_generation_pipeline(self, mock_gcs_client):
        """Test complete instrument generation pipeline"""
        mock_client, mock_bucket = mock_gcs_client
        
        # Mock Tardis API response
        mock_symbols = [
            {
                'exchange': 'binance',
                'symbol': 'BTCUSDT',
                'base_asset': 'BTC',
                'quote_asset': 'USDT',
                'type': 'spot'
            },
            {
                'exchange': 'deribit',
                'symbol': 'BTC-PERPETUAL',
                'base_asset': 'BTC',
                'quote_asset': 'USD',
                'type': 'perpetual'
            }
        ]
        
        with patch.object(CanonicalInstrumentKeyGenerator, '_fetch_exchange_symbols') as mock_fetch:
            mock_fetch.return_value = mock_symbols
            
            # Create generator
            generator = CanonicalInstrumentKeyGenerator("test_api_key")
            
            # Generate instruments
            date = datetime(2023, 5, 23, tzinfo=timezone.utc)
            instruments = await generator.generate_instruments_for_date(
                date, 
                exchanges=['binance', 'deribit']
            )
            
            assert len(instruments) == 2
            
            # Check first instrument (SPOT_PAIR)
            spot_instrument = instruments[0]
            assert spot_instrument['instrument_key'] == 'BINANCE:SPOT_PAIR:BTC-USDT'
            assert spot_instrument['venue'] == 'BINANCE'
            assert spot_instrument['instrument_type'] == 'SPOT_PAIR'
            assert spot_instrument['data_types'] == 'trades,book_snapshot_5'
            
            # Check second instrument (PERP)
            perp_instrument = instruments[1]
            assert perp_instrument['instrument_key'] == 'DERIBIT:PERP:BTC-USD'
            assert perp_instrument['venue'] == 'DERIBIT'
            assert perp_instrument['instrument_type'] == 'PERP'
            assert perp_instrument['data_types'] == 'trades,book_snapshot_5,derivative_ticker,liquidations'
    
    @pytest.mark.asyncio
    async def test_instrument_upload_pipeline(self, mock_gcs_client, sample_instrument_definition):
        """Test instrument upload pipeline"""
        mock_client, mock_bucket = mock_gcs_client
        
        # Mock successful upload
        mock_blob = Mock()
        mock_bucket.blob.return_value = mock_blob
        
        # Create uploader
        uploader = InstrumentGCSUploader("test-bucket")
        
        # Convert to dict for testing
        inst_dict = sample_instrument_definition.model_dump()
        
        # Upload instruments
        date = datetime(2023, 5, 23, tzinfo=timezone.utc)
        result = uploader.upload_instruments([inst_dict], date)
        
        assert result['success'] is True
        assert result['uploaded_files'] == 1
        assert result['total_instruments'] == 1
        assert result['errors'] == []
        
        # Verify blob was created and uploaded
        mock_bucket.blob.assert_called_once()
        mock_blob.upload_from_string.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_instrument_pipeline_with_multiple_exchanges(self, mock_gcs_client):
        """Test instrument pipeline with multiple exchanges"""
        mock_client, mock_bucket = mock_gcs_client
        
        # Mock Tardis API responses for different exchanges
        mock_symbols_binance = [
            {
                'exchange': 'binance',
                'symbol': 'BTCUSDT',
                'base_asset': 'BTC',
                'quote_asset': 'USDT',
                'type': 'spot'
            }
        ]
        
        mock_symbols_deribit = [
            {
                'exchange': 'deribit',
                'symbol': 'BTC-PERPETUAL',
                'base_asset': 'BTC',
                'quote_asset': 'USD',
                'type': 'perpetual'
            },
            {
                'exchange': 'deribit',
                'symbol': 'BTC-29DEC23-50000-C',
                'base_asset': 'BTC',
                'quote_asset': 'USD',
                'type': 'option',
                'expiry': '2023-12-29',
                'strike': '50000',
                'option_type': 'C'
            }
        ]
        
        with patch.object(CanonicalInstrumentKeyGenerator, '_fetch_exchange_symbols') as mock_fetch:
            def side_effect(exchange):
                if exchange == 'binance':
                    return mock_symbols_binance
                elif exchange == 'deribit':
                    return mock_symbols_deribit
                return []
            
            mock_fetch.side_effect = side_effect
            
            # Create generator
            generator = CanonicalInstrumentKeyGenerator("test_api_key")
            
            # Generate instruments
            date = datetime(2023, 5, 23, tzinfo=timezone.utc)
            instruments = await generator.generate_instruments_for_date(
                date, 
                exchanges=['binance', 'deribit']
            )
            
            assert len(instruments) == 3
            
            # Check instrument types
            instrument_keys = [inst['instrument_key'] for inst in instruments]
            assert 'BINANCE:SPOT_PAIR:BTC-USDT' in instrument_keys
            assert 'DERIBIT:PERP:BTC-USD' in instrument_keys
            assert 'DERIBIT:OPTION:BTC-USD-231229-50000-CALL' in instrument_keys
    
    @pytest.mark.asyncio
    async def test_instrument_pipeline_error_handling(self, mock_gcs_client):
        """Test instrument pipeline error handling"""
        mock_client, mock_bucket = mock_gcs_client
        
        # Mock API error
        with patch.object(CanonicalInstrumentKeyGenerator, '_fetch_exchange_symbols') as mock_fetch:
            mock_fetch.side_effect = Exception("API Error")
            
            # Create generator
            generator = CanonicalInstrumentKeyGenerator("test_api_key")
            
            # Generate instruments should handle error gracefully
            date = datetime(2023, 5, 23, tzinfo=timezone.utc)
            instruments = await generator.generate_instruments_for_date(
                date, 
                exchanges=['binance']
            )
            
            # Should return empty list on error
            assert len(instruments) == 0
    
    @pytest.mark.asyncio
    async def test_instrument_pipeline_upload_error_handling(self, mock_gcs_client, sample_instrument_definition):
        """Test instrument upload error handling"""
        mock_client, mock_bucket = mock_gcs_client
        
        # Mock upload error
        mock_blob = Mock()
        mock_blob.upload_from_string.side_effect = Exception("Upload failed")
        mock_bucket.blob.return_value = mock_blob
        
        # Create uploader
        uploader = InstrumentGCSUploader("test-bucket")
        
        # Convert to dict for testing
        inst_dict = sample_instrument_definition.model_dump()
        
        # Upload instruments
        date = datetime(2023, 5, 23, tzinfo=timezone.utc)
        result = uploader.upload_instruments([inst_dict], date)
        
        assert result['success'] is False
        assert result['uploaded_files'] == 0
        assert result['total_instruments'] == 1
        assert len(result['errors']) == 1
        assert 'upload failed' in result['errors'][0].lower()
    
    @pytest.mark.asyncio
    async def test_instrument_pipeline_validation_error(self, mock_gcs_client):
        """Test instrument pipeline with validation error"""
        mock_client, mock_bucket = mock_gcs_client
        
        # Create uploader
        uploader = InstrumentGCSUploader("test-bucket")
        
        # Invalid instrument definition
        invalid_inst = {
            'instrument_key': 'INVALID',
            'venue': 'BINANCE'
            # Missing required fields
        }
        
        # Upload instruments
        date = datetime(2023, 5, 23, tzinfo=timezone.utc)
        result = uploader.upload_instruments([invalid_inst], date)
        
        assert result['success'] is False
        assert result['uploaded_files'] == 0
        assert result['total_instruments'] == 1
        assert len(result['errors']) == 1
        assert 'validation' in result['errors'][0].lower()
