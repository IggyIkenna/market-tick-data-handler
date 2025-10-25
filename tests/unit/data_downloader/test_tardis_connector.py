"""
Unit tests for Tardis connector
"""

import pytest
from unittest.mock import Mock, patch, AsyncMock
import pandas as pd
from datetime import datetime, timezone
from market_data_tick_handler.data_downloader.tardis_connector import TardisConnector


class TestTardisConnector:
    """Test TardisConnector class"""
    
    def test_init(self):
        """Test connector initialization"""
        connector = TardisConnector("test_api_key")
        assert connector.api_key == "test_api_key"
        assert connector.base_url == "https://datasets.tardis.dev"
        assert connector.timeout == 60  # From config default
        assert connector.max_retries == 3
    
    def test_init_with_custom_params(self):
        """Test connector initialization with custom parameters"""
        connector = TardisConnector(
            api_key="test_key"
        )
        assert connector.api_key == "test_key"
        assert connector.base_url == "https://datasets.tardis.dev"  # From config
        assert connector.timeout == 60  # From config
        assert connector.max_retries == 3  # From config
    
    def test_build_url_trades(self):
        """Test URL building for trades data"""
        connector = TardisConnector("test_api_key")
        
        url = connector._build_url("binance", "BTC-USDT", "2023-05-23", "trades")
        
        expected = "https://datasets.tardis.dev/v1/datasets/binance/btc-usdt/trades/2023/05/23"
        assert url == expected
    
    def test_build_url_book_snapshot(self):
        """Test URL building for book snapshot data"""
        connector = TardisConnector("test_api_key")
        
        url = connector._build_url("deribit", "BTC-USD", "2023-05-23", "book_snapshot_5")
        
        expected = "https://datasets.tardis.dev/v1/datasets/deribit/btc-usd/book_snapshot_5/2023/05/23"
        assert url == expected
    
    def test_build_url_derivative_ticker(self):
        """Test URL building for derivative ticker data"""
        connector = TardisConnector("test_api_key")
        
        url = connector._build_url("deribit", "BTC-USD", "2023-05-23", "derivative_ticker")
        
        expected = "https://datasets.tardis.dev/v1/datasets/deribit/btc-usd/derivative_ticker/2023/05/23"
        assert url == expected
    
    def test_build_url_liquidations(self):
        """Test URL building for liquidations data"""
        connector = TardisConnector("test_api_key")
        
        url = connector._build_url("deribit", "BTC-USD", "2023-05-23", "liquidations")
        
        expected = "https://datasets.tardis.dev/v1/datasets/deribit/btc-usd/liquidations/2023/05/23"
        assert url == expected
    
    def test_build_url_options_chain(self):
        """Test URL building for options chain data"""
        connector = TardisConnector("test_api_key")
        
        url = connector._build_url("deribit", "BTC-USD", "2023-05-23", "options_chain")
        
        expected = "https://datasets.tardis.dev/v1/datasets/deribit/btc-usd/options_chain/2023/05/23"
        assert url == expected
    
    def test_parse_csv_data_trades(self):
        """Test CSV parsing for trades data"""
        connector = TardisConnector("test_api_key")
        
        csv_data = "timestamp,local_timestamp,id,price,amount,side\n1684800000000000,1684800000000000,trade_1,50000.0,0.1,buy"
        
        result = connector._parse_csv_data(csv_data, "trades")
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]['timestamp'] == 1684800000000000
        assert result.iloc[0]['price'] == 50000.0
        assert result.iloc[0]['amount'] == 0.1
        assert result.iloc[0]['side'] == 'buy'
    
    def test_parse_csv_data_book_snapshot(self):
        """Test CSV parsing for book snapshot data"""
        connector = TardisConnector("test_api_key")
        
        csv_data = "timestamp,local_timestamp,bid_0_price,bid_0_amount,ask_0_price,ask_0_amount\n1684800000000000,1684800000000000,49950.0,1.0,50000.0,1.0"
        
        result = connector._parse_csv_data(csv_data, "book_snapshot_5")
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]['timestamp'] == 1684800000000000
        assert result.iloc[0]['bid_0_price'] == 49950.0
        assert result.iloc[0]['ask_0_price'] == 50000.0
    
    def test_parse_csv_data_liquidations(self):
        """Test CSV parsing for liquidations data"""
        connector = TardisConnector("test_api_key")
        
        csv_data = "timestamp,local_timestamp,id,side,price,amount\n1684800000000000,1684800000000000,liq_1,buy,50000.0,0.1"
        
        result = connector._parse_csv_data(csv_data, "liquidations")
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]['timestamp'] == 1684800000000000
        assert result.iloc[0]['side'] == 'buy'  # Should accept 'buy'/'sell'
        assert result.iloc[0]['price'] == 50000.0
        assert result.iloc[0]['amount'] == 0.1
    
    def test_parse_csv_data_derivative_ticker(self):
        """Test CSV parsing for derivative ticker data"""
        connector = TardisConnector("test_api_key")
        
        csv_data = "timestamp,local_timestamp,funding_rate,predicted_funding_rate,open_interest,last_price,index_price,mark_price\n1684800000000000,1684800000000000,0.0001,0.0002,1000.0,50000.0,49950.0,50000.0"
        
        result = connector._parse_csv_data(csv_data, "derivative_ticker")
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]['timestamp'] == 1684800000000000
        assert result.iloc[0]['funding_rate'] == 0.0001
        assert result.iloc[0]['open_interest'] == 1000.0
        assert result.iloc[0]['last_price'] == 50000.0
    
    def test_parse_csv_data_options_chain(self):
        """Test CSV parsing for options chain data"""
        connector = TardisConnector("test_api_key")
        
        csv_data = "timestamp,local_timestamp,option_type,strike,expiry,underlying,mark_price,mark_iv\n1684800000000000,1684800000000000,CALL,50000,2023-12-29,BTC-USD,1000.0,0.5"
        
        result = connector._parse_csv_data(csv_data, "options_chain")
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]['timestamp'] == 1684800000000000
        assert result.iloc[0]['option_type'] == 'CALL'
        assert result.iloc[0]['strike'] == 50000
        assert result.iloc[0]['mark_price'] == 1000.0
    
    def test_parse_csv_data_empty(self):
        """Test CSV parsing with empty data"""
        connector = TardisConnector("test_api_key")
        
        csv_data = "timestamp,price,amount,side\n"
        
        result = connector._parse_csv_data(csv_data, "trades")
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
    
    def test_parse_csv_data_invalid_format(self):
        """Test CSV parsing with invalid format"""
        connector = TardisConnector("test_api_key")
        
        csv_data = "invalid,csv,format"
        
        result = connector._parse_csv_data(csv_data, "trades")
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
    
    @patch('aiohttp.ClientSession.get')
    @pytest.mark.asyncio
    async def test_download_daily_data_success(self, mock_get):
        """Test successful daily data download"""
        connector = TardisConnector("test_api_key")
        
        # Mock successful response
        mock_response = Mock()
        mock_response.status = 200
        mock_response.text.return_value = "timestamp,price,amount,side\n1684800000000000,50000.0,0.1,buy"
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = None
        mock_get.return_value = mock_response
        
        date = datetime(2023, 5, 23, tzinfo=timezone.utc)
        result = await connector.download_daily_data("binance", "BTC-USDT", date, ["trades"])
        
        assert "trades" in result
        assert isinstance(result["trades"], pd.DataFrame)
        assert len(result["trades"]) == 1
        assert result["trades"].iloc[0]["price"] == 50000.0
    
    @patch('aiohttp.ClientSession.get')
    @pytest.mark.asyncio
    async def test_download_daily_data_not_found(self, mock_get):
        """Test daily data download with 404 error"""
        connector = TardisConnector("test_api_key")
        
        # Mock 404 response
        mock_response = Mock()
        mock_response.status = 404
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = None
        mock_get.return_value = mock_response
        
        date = datetime(2023, 5, 23, tzinfo=timezone.utc)
        result = await connector.download_daily_data("binance", "BTC-USDT", date, ["trades"])
        
        assert "trades" in result
        assert isinstance(result["trades"], pd.DataFrame)
        assert len(result["trades"]) == 0
    
    @patch('aiohttp.ClientSession.get')
    @pytest.mark.asyncio
    async def test_download_daily_data_server_error(self, mock_get):
        """Test daily data download with server error"""
        connector = TardisConnector("test_api_key")
        
        # Mock 500 response
        mock_response = Mock()
        mock_response.status = 500
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = None
        mock_get.return_value = mock_response
        
        date = datetime(2023, 5, 23, tzinfo=timezone.utc)
        
        with pytest.raises(Exception, match="Server error"):
            await connector.download_daily_data("binance", "BTC-USDT", date, ["trades"])
    
    @patch('aiohttp.ClientSession.get')
    @pytest.mark.asyncio
    async def test_download_daily_data_multiple_types(self, mock_get):
        """Test daily data download with multiple data types"""
        connector = TardisConnector("test_api_key")
        
        # Mock successful response for both data types
        mock_response = Mock()
        mock_response.status = 200
        mock_response.text.return_value = "timestamp,price,amount,side\n1684800000000000,50000.0,0.1,buy"
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = None
        mock_get.return_value = mock_response
        
        date = datetime(2023, 5, 23, tzinfo=timezone.utc)
        result = await connector.download_daily_data("binance", "BTC-USDT", date, ["trades", "book_snapshot_5"])
        
        assert "trades" in result
        assert "book_snapshot_5" in result
        assert isinstance(result["trades"], pd.DataFrame)
        assert isinstance(result["book_snapshot_5"], pd.DataFrame)
    
    @patch('aiohttp.ClientSession.get')
    @pytest.mark.asyncio
    async def test_download_daily_data_direct(self, mock_get):
        """Test direct daily data download method"""
        connector = TardisConnector("test_api_key")
        
        # Mock successful response
        mock_response = Mock()
        mock_response.status = 200
        mock_response.text.return_value = "timestamp,price,amount,side\n1684800000000000,50000.0,0.1,buy"
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = None
        mock_get.return_value = mock_response
        
        date = datetime(2023, 5, 23, tzinfo=timezone.utc)
        result = await connector.download_daily_data_direct("binance", "BTC-USDT", date, ["trades"])
        
        assert "trades" in result
        assert isinstance(result["trades"], pd.DataFrame)
        assert len(result["trades"]) == 1
        assert result["trades"].iloc[0]["price"] == 50000.0
