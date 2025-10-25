"""
Unit tests for models module
"""

import pytest
from datetime import datetime, timezone
from market_data_tick_handler.models import (
    InstrumentType, Venue, InstrumentKey, InstrumentDefinition,
    TradeData, BookSnapshot, Liquidations, DerivativeTicker
)


class TestInstrumentType:
    """Test InstrumentType enum"""
    
    def test_instrument_type_values(self):
        """Test that all expected instrument types exist"""
        assert InstrumentType.SPOT_ASSET == "SPOT_ASSET"
        assert InstrumentType.SPOT_PAIR == "SPOT_PAIR"
        assert InstrumentType.PERP == "PERP"
        assert InstrumentType.PERPETUAL == "PERPETUAL"
        assert InstrumentType.FUTURE == "FUTURE"
        assert InstrumentType.OPTION == "OPTION"
    
    def test_perp_alias(self):
        """Test that PERP is an alias for PERPETUAL"""
        assert InstrumentType.PERP == "PERP"
        assert InstrumentType.PERPETUAL == "PERPETUAL"
        # They should be different values but both valid
        assert InstrumentType.PERP != InstrumentType.PERPETUAL


class TestVenue:
    """Test Venue enum"""
    
    def test_venue_values(self):
        """Test that all expected venues exist"""
        assert Venue.BINANCE == "BINANCE"
        assert Venue.DERIBIT == "DERIBIT"
        assert Venue.BYBIT == "BYBIT"
        assert Venue.OKX == "OKX"


class TestInstrumentKey:
    """Test InstrumentKey dataclass"""
    
    def test_spot_pair_key(self):
        """Test SPOT_PAIR instrument key creation"""
        key = InstrumentKey(
            venue=Venue.BINANCE,
            instrument_type=InstrumentType.SPOT_PAIR,
            symbol="BTC-USDT"
        )
        assert str(key) == "BINANCE:SPOT_PAIR:BTC-USDT"
        assert key.venue == Venue.BINANCE
        assert key.instrument_type == InstrumentType.SPOT_PAIR
        assert key.symbol == "BTC-USDT"
        assert key.expiry is None
        assert key.option_type is None
    
    def test_future_key(self):
        """Test FUTURE instrument key with expiry"""
        key = InstrumentKey(
            venue=Venue.DERIBIT,
            instrument_type=InstrumentType.FUTURE,
            symbol="BTC-USD",
            expiry="241225"
        )
        assert str(key) == "DERIBIT:FUTURE:BTC-USD:241225"
        assert key.expiry == "241225"
    
    def test_option_key(self):
        """Test OPTION instrument key with expiry and option type"""
        key = InstrumentKey(
            venue=Venue.DERIBIT,
            instrument_type=InstrumentType.OPTION,
            symbol="BTC-USD",
            expiry="241225",
            option_type="CALL"
        )
        assert str(key) == "DERIBIT:OPTION:BTC-USD:241225:CALL"
        assert key.expiry == "241225"
        assert key.option_type == "CALL"
    
    def test_from_string_spot_pair(self):
        """Test parsing SPOT_PAIR key from string"""
        key = InstrumentKey.from_string("BINANCE:SPOT_PAIR:BTC-USDT")
        assert key.venue == Venue.BINANCE
        assert key.instrument_type == InstrumentType.SPOT_PAIR
        assert key.symbol == "BTC-USDT"
        assert key.expiry is None
        assert key.option_type is None
    
    def test_from_string_future(self):
        """Test parsing FUTURE key from string"""
        key = InstrumentKey.from_string("DERIBIT:FUTURE:BTC-USD:241225")
        assert key.venue == Venue.DERIBIT
        assert key.instrument_type == InstrumentType.FUTURE
        assert key.symbol == "BTC-USD"
        assert key.expiry == "241225"
        assert key.option_type is None
    
    def test_from_string_option(self):
        """Test parsing OPTION key from string"""
        key = InstrumentKey.from_string("DERIBIT:OPTION:BTC-USD:241225:CALL")
        assert key.venue == Venue.DERIBIT
        assert key.instrument_type == InstrumentType.OPTION
        assert key.symbol == "BTC-USD"
        assert key.expiry == "241225"
        assert key.option_type == "CALL"
    
    def test_invalid_string_format(self):
        """Test that invalid string format raises error"""
        with pytest.raises(ValueError):
            InstrumentKey.from_string("INVALID_FORMAT")
        
        with pytest.raises(ValueError):
            InstrumentKey.from_string("BINANCE:INVALID_TYPE:BTC-USDT")


class TestTradeData:
    """Test TradeData dataclass"""
    
    def test_trade_data_creation(self, sample_trade_data):
        """Test creating trade data from dict"""
        trade = TradeData(**sample_trade_data)
        assert trade.timestamp == 1684800000000000
        assert trade.local_timestamp == 1684800000000000
        assert trade.id == "test_trade_1"
        assert trade.price == 50000.0
        assert trade.amount == 0.1
        assert trade.side == "buy"
    
    def test_trade_data_validation(self):
        """Test trade data validation"""
        # Valid data
        valid_data = {
            'timestamp': 1684800000000000,
            'local_timestamp': 1684800000000000,
            'id': 'test_trade_1',
            'price': 50000.0,
            'amount': 0.1,
            'side': 'buy'
        }
        trade = TradeData(**valid_data)
        assert trade.price == 50000.0
        
        # Invalid price
        invalid_data = valid_data.copy()
        invalid_data['price'] = -100.0
        with pytest.raises(ValueError, match="Price must be positive"):
            TradeData(**invalid_data)
        
        # Invalid amount
        invalid_data = valid_data.copy()
        invalid_data['amount'] = 0.0
        with pytest.raises(ValueError, match="Amount must be positive"):
            TradeData(**invalid_data)


class TestLiquidations:
    """Test Liquidations dataclass"""
    
    def test_liquidations_creation(self, sample_liquidation_data):
        """Test creating liquidations data"""
        liq = Liquidations(**sample_liquidation_data)
        assert liq.timestamp == 1684800000000000
        assert liq.local_timestamp == 1684800000000000
        assert liq.id == "test_liquidation_1"
        assert liq.side == "buy"  # 'buy' = short liquidated
        assert liq.price == 50000.0
        assert liq.amount == 0.1
    
    def test_liquidations_side_validation(self):
        """Test liquidations side field validation"""
        # Valid sides
        valid_data = {
            'timestamp': 1684800000000000,
            'local_timestamp': 1684800000000000,
            'id': 'test_1',
            'side': 'buy',
            'price': 50000.0,
            'amount': 0.1
        }
        liq = Liquidations(**valid_data)
        assert liq.side == "buy"
        
        valid_data['side'] = 'sell'
        liq = Liquidations(**valid_data)
        assert liq.side == "sell"
        
        # Invalid side
        invalid_data = valid_data.copy()
        invalid_data['side'] = 'invalid'
        with pytest.raises(ValueError, match="Side must be 'buy' or 'sell'"):
            Liquidations(**invalid_data)
    
    def test_liquidations_validation(self):
        """Test liquidations validation"""
        base_data = {
            'timestamp': 1684800000000000,
            'local_timestamp': 1684800000000000,
            'id': 'test_1',
            'side': 'buy',
            'price': 50000.0,
            'amount': 0.1
        }
        
        # Valid data
        liq = Liquidations(**base_data)
        assert liq.price == 50000.0
        
        # Invalid price
        invalid_data = base_data.copy()
        invalid_data['price'] = -100.0
        with pytest.raises(ValueError, match="Price must be positive"):
            Liquidations(**invalid_data)
        
        # Invalid amount
        invalid_data = base_data.copy()
        invalid_data['amount'] = 0.0
        with pytest.raises(ValueError, match="Amount must be positive"):
            Liquidations(**invalid_data)


class TestDerivativeTicker:
    """Test DerivativeTicker dataclass"""
    
    def test_derivative_ticker_creation(self, sample_derivative_ticker_data):
        """Test creating derivative ticker data"""
        ticker = DerivativeTicker(**sample_derivative_ticker_data)
        assert ticker.timestamp == 1684800000000000
        assert ticker.local_timestamp == 1684800000000000
        assert ticker.funding_rate == 0.0001
        assert ticker.predicted_funding_rate == 0.0002
        assert ticker.open_interest == 1000.0
        assert ticker.last_price == 50000.0
        assert ticker.index_price == 49950.0
        assert ticker.mark_price == 50000.0
        assert ticker.funding_timestamp == 1684800000000000


class TestInstrumentDefinition:
    """Test InstrumentDefinition model"""
    
    def test_instrument_definition_creation(self, sample_instrument_definition):
        """Test creating instrument definition"""
        inst = sample_instrument_definition
        assert inst.instrument_key == "BINANCE:SPOT_PAIR:BTC-USDT"
        assert inst.venue == "BINANCE"
        assert inst.instrument_type == "SPOT_PAIR"
        assert inst.data_types == "trades,book_snapshot_5"
        assert inst.base_asset == "BTC"
        assert inst.quote_asset == "USDT"
    
    def test_option_type_validation(self):
        """Test option type validation"""
        base_data = {
            'instrument_key': 'DERIBIT:OPTION:BTC-USD-241225-50000-CALL',
            'venue': 'DERIBIT',
            'instrument_type': 'OPTION',
            'available_from_datetime': '2023-05-23T00:00:00+00:00',
            'available_to_datetime': '2024-05-23T00:00:00+00:00',
            'data_types': 'trades,book_snapshot_5,options_chain',
            'base_asset': 'BTC',
            'quote_asset': 'USD',
            'settle_asset': 'USD',
            'exchange_raw_symbol': 'BTC-USD-241225-50000-C',
            'tardis_symbol': 'BTC-USD-241225-50000-C',
            'tardis_exchange': 'deribit',
            'data_provider': 'tardis',
            'venue_type': 'centralized',
            'asset_class': 'crypto',
            'inverse': False,
            'tick_size': '0.5',
            'min_size': '0.1',
            'settlement_type': '',
            'underlying': 'BTC-USD',
            'ccxt_symbol': 'BTC/USD-241225-50000-C',
            'ccxt_exchange': 'deribit',
            'strike': '50000',
            'option_type': 'CALL',
            'expiry': '2024-12-25T00:00:00+00:00'
        }
        
        # Valid option type
        inst = InstrumentDefinition(**base_data)
        assert inst.option_type == "CALL"
        
        # Invalid option type (C instead of CALL)
        invalid_data = base_data.copy()
        invalid_data['option_type'] = 'C'
        with pytest.raises(ValueError, match="Invalid option type: C. Must be CALL or PUT"):
            InstrumentDefinition(**invalid_data)
        
        # Invalid option type (P instead of PUT)
        invalid_data = base_data.copy()
        invalid_data['option_type'] = 'P'
        with pytest.raises(ValueError, match="Invalid option type: P. Must be CALL or PUT"):
            InstrumentDefinition(**invalid_data)
    
    def test_datetime_validation(self):
        """Test datetime field validation"""
        base_data = {
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
        
        # Valid datetime
        inst = InstrumentDefinition(**base_data)
        assert inst.available_from_datetime == '2023-05-23T00:00:00+00:00'
        
        # Invalid datetime format
        invalid_data = base_data.copy()
        invalid_data['available_from_datetime'] = 'invalid-datetime'
        with pytest.raises(ValueError, match="Invalid ISO datetime format"):
            InstrumentDefinition(**invalid_data)
        
        # Empty datetime
        invalid_data = base_data.copy()
        invalid_data['available_from_datetime'] = ''
        with pytest.raises(ValueError, match="Datetime string cannot be empty"):
            InstrumentDefinition(**invalid_data)
