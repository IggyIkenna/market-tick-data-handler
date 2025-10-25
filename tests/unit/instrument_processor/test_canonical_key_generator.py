"""
Unit tests for canonical key generator
"""

import pytest
from unittest.mock import Mock, patch
from market_data_tick_handler.instrument_processor.canonical_key_generator import CanonicalInstrumentKeyGenerator


class TestCanonicalInstrumentKeyGenerator:
    """Test CanonicalInstrumentKeyGenerator class"""
    
    def test_init(self):
        """Test generator initialization"""
        generator = CanonicalInstrumentKeyGenerator("test_api_key")
        assert generator.api_key == "test_api_key"
        assert generator.base_url == "https://api.tardis.dev/v1"
    
    def test_normalize_option_type(self):
        """Test option type normalization"""
        generator = CanonicalInstrumentKeyGenerator("test_api_key")
        
        # Test CALL normalization
        assert generator._normalize_option_type('C') == 'CALL'
        assert generator._normalize_option_type('c') == 'CALL'
        assert generator._normalize_option_type('CALL') == 'CALL'
        assert generator._normalize_option_type('call') == 'CALL'
        
        # Test PUT normalization
        assert generator._normalize_option_type('P') == 'PUT'
        assert generator._normalize_option_type('p') == 'PUT'
        assert generator._normalize_option_type('PUT') == 'PUT'
        assert generator._normalize_option_type('put') == 'PUT'
    
    def test_get_tardis_data_types_spot_pair(self):
        """Test data type assignment for SPOT_PAIR"""
        generator = CanonicalInstrumentKeyGenerator("test_api_key")
        data_types = generator._get_tardis_data_types('SPOT_PAIR')
        expected = ['trades', 'book_snapshot_5']
        assert data_types == expected
    
    def test_get_tardis_data_types_perpetual(self):
        """Test data type assignment for PERP"""
        generator = CanonicalInstrumentKeyGenerator("test_api_key")
        data_types = generator._get_tardis_data_types('PERP')
        expected = ['trades', 'book_snapshot_5', 'derivative_ticker', 'liquidations']
        assert data_types == expected
    
    def test_get_tardis_data_types_future(self):
        """Test data type assignment for FUTURE"""
        generator = CanonicalInstrumentKeyGenerator("test_api_key")
        data_types = generator._get_tardis_data_types('FUTURE')
        expected = ['trades', 'book_snapshot_5', 'derivative_ticker', 'liquidations']
        assert data_types == expected
    
    def test_get_tardis_data_types_option(self):
        """Test data type assignment for OPTION"""
        generator = CanonicalInstrumentKeyGenerator("test_api_key")
        data_types = generator._get_tardis_data_types('OPTION')
        expected = ['trades', 'book_snapshot_5', 'options_chain', 'liquidations', 'derivative_ticker']
        assert data_types == expected
    
    def test_generate_spot_pair_key(self):
        """Test SPOT_PAIR key generation"""
        generator = CanonicalInstrumentKeyGenerator("test_api_key")
        
        symbol_info = {
            'exchange': 'binance',
            'symbol': 'BTCUSDT',
            'base_asset': 'BTC',
            'quote_asset': 'USDT',
            'type': 'spot'
        }
        
        key = generator._generate_instrument_key(symbol_info, '2023-05-23')
        
        assert key['instrument_key'] == 'BINANCE:SPOT_PAIR:BTC-USDT'
        assert key['venue'] == 'BINANCE'
        assert key['instrument_type'] == 'SPOT_PAIR'
        assert key['canonical_symbol'] == 'BTC-USDT'
        assert key['data_types'] == 'trades,book_snapshot_5'
        assert key['base_asset'] == 'BTC'
        assert key['quote_asset'] == 'USDT'
    
    def test_generate_perpetual_key(self):
        """Test PERP key generation"""
        generator = CanonicalInstrumentKeyGenerator("test_api_key")
        
        symbol_info = {
            'exchange': 'deribit',
            'symbol': 'BTC-PERPETUAL',
            'base_asset': 'BTC',
            'quote_asset': 'USD',
            'type': 'perpetual'
        }
        
        key = generator._generate_instrument_key(symbol_info, '2023-05-23')
        
        assert key['instrument_key'] == 'DERIBIT:PERP:BTC-USD'
        assert key['venue'] == 'DERIBIT'
        assert key['instrument_type'] == 'PERP'
        assert key['canonical_symbol'] == 'BTC-USD'
        assert key['data_types'] == 'trades,book_snapshot_5,derivative_ticker,liquidations'
        assert key['base_asset'] == 'BTC'
        assert key['quote_asset'] == 'USD'
    
    def test_generate_future_key(self):
        """Test FUTURE key generation"""
        generator = CanonicalInstrumentKeyGenerator("test_api_key")
        
        symbol_info = {
            'exchange': 'deribit',
            'symbol': 'BTC-29DEC23',
            'base_asset': 'BTC',
            'quote_asset': 'USD',
            'type': 'future',
            'expiry': '2023-12-29'
        }
        
        key = generator._generate_instrument_key(symbol_info, '2023-05-23')
        
        assert key['instrument_key'] == 'DERIBIT:FUTURE:BTC-USD:231229'
        assert key['venue'] == 'DERIBIT'
        assert key['instrument_type'] == 'FUTURE'
        assert key['canonical_symbol'] == 'BTC-USD'
        assert key['expiry'] == '231229'
        assert key['data_types'] == 'trades,book_snapshot_5,derivative_ticker,liquidations'
    
    def test_generate_option_key(self):
        """Test OPTION key generation"""
        generator = CanonicalInstrumentKeyGenerator("test_api_key")
        
        symbol_info = {
            'exchange': 'deribit',
            'symbol': 'BTC-29DEC23-50000-C',
            'base_asset': 'BTC',
            'quote_asset': 'USD',
            'type': 'option',
            'expiry': '2023-12-29',
            'strike': '50000',
            'option_type': 'C'
        }
        
        key = generator._generate_instrument_key(symbol_info, '2023-05-23')
        
        assert key['instrument_key'] == 'DERIBIT:OPTION:BTC-USD-231229-50000-CALL'
        assert key['venue'] == 'DERIBIT'
        assert key['instrument_type'] == 'OPTION'
        assert key['canonical_symbol'] == 'BTC-USD-231229-50000-CALL'
        assert key['expiry'] == '231229'
        assert key['strike'] == '50000'
        assert key['option_type'] == 'CALL'
        assert key['data_types'] == 'trades,book_snapshot_5,options_chain,liquidations,derivative_ticker'
    
    def test_generate_option_key_put(self):
        """Test OPTION key generation for PUT"""
        generator = CanonicalInstrumentKeyGenerator("test_api_key")
        
        symbol_info = {
            'exchange': 'deribit',
            'symbol': 'BTC-29DEC23-50000-P',
            'base_asset': 'BTC',
            'quote_asset': 'USD',
            'type': 'option',
            'expiry': '2023-12-29',
            'strike': '50000',
            'option_type': 'P'
        }
        
        key = generator._generate_instrument_key(symbol_info, '2023-05-23')
        
        assert key['instrument_key'] == 'DERIBIT:OPTION:BTC-USD-231229-50000-PUT'
        assert key['option_type'] == 'PUT'
    
    @patch('aiohttp.ClientSession.get')
    @pytest.mark.asyncio
    async def test_fetch_exchange_symbols(self, mock_get):
        """Test fetching exchange symbols"""
        generator = CanonicalInstrumentKeyGenerator("test_api_key")
        
        # Mock response
        mock_response = Mock()
        mock_response.status = 200
        mock_response.json.return_value = {
            'data': [
                {
                    'exchange': 'binance',
                    'symbol': 'BTCUSDT',
                    'base_asset': 'BTC',
                    'quote_asset': 'USDT',
                    'type': 'spot'
                }
            ]
        }
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = None
        mock_get.return_value = mock_response
        
        symbols = await generator._fetch_exchange_symbols('binance')
        
        assert len(symbols) == 1
        assert symbols[0]['exchange'] == 'binance'
        assert symbols[0]['symbol'] == 'BTCUSDT'
    
    @patch('aiohttp.ClientSession.get')
    @pytest.mark.asyncio
    async def test_fetch_exchange_symbols_error(self, mock_get):
        """Test error handling when fetching exchange symbols"""
        generator = CanonicalInstrumentKeyGenerator("test_api_key")
        
        # Mock error response
        mock_response = Mock()
        mock_response.status = 404
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = None
        mock_get.return_value = mock_response
        
        symbols = await generator._fetch_exchange_symbols('nonexistent')
        
        assert symbols == []
    
    @patch.object(CanonicalInstrumentKeyGenerator, '_fetch_exchange_symbols')
    @pytest.mark.asyncio
    async def test_generate_instruments_for_date(self, mock_fetch):
        """Test generating instruments for a date"""
        generator = CanonicalInstrumentKeyGenerator("test_api_key")
        
        # Mock exchange symbols
        mock_fetch.return_value = [
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
        
        from datetime import datetime, timezone
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
        
        # Check second instrument (PERP)
        perp_instrument = instruments[1]
        assert perp_instrument['instrument_key'] == 'DERIBIT:PERP:BTC-USD'
        assert perp_instrument['venue'] == 'DERIBIT'
        assert perp_instrument['instrument_type'] == 'PERP'
