#!/usr/bin/env python3
"""
Error Handling Tests

Tests for robust error handling across the system:
1. Network failures and timeouts
2. API errors and rate limiting
3. Invalid data and parsing errors
4. GCS upload failures
5. Memory and resource exhaustion
6. Edge cases and boundary conditions
"""

import pytest
import pytest_asyncio
import asyncio
import aiohttp
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime, timezone
from typing import Dict, List, Any
import logging

# Add project root to path
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from market_data_tick_handler.data_downloader.tardis_connector import TardisConnector
from market_data_tick_handler.data_downloader.download_orchestrator import DownloadOrchestrator
from config import get_config

logger = logging.getLogger(__name__)

class ErrorHandlingTests:
    """Base class for error handling tests"""
    
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


# Test fixtures
TEST_DATE = datetime(2023, 5, 23, tzinfo=timezone.utc)


@pytest_asyncio.fixture
async def error_tester():
    """Fixture for error handling tests"""
    tester = ErrorHandlingTests()
    await tester.setup()
    try:
        yield tester
    finally:
        await tester.teardown()


@pytest.mark.asyncio
class TestNetworkErrors:
    """Test network error handling"""
    
    async def test_connection_timeout(self, error_tester):
        """Test handling of connection timeouts"""
        logger.info("Testing connection timeout handling")
        
        # Mock a timeout error
        with patch.object(error_tester.tardis_connector, '_make_request') as mock_request:
            mock_request.side_effect = asyncio.TimeoutError("Connection timeout")
            
            # Attempt to download data
            try:
                result = await error_tester.tardis_connector.download_daily_data_direct(
                    tardis_exchange='binance',
                    tardis_symbol='BTCUSDT',
                    date=TEST_DATE,
                    data_types=['trades']
                )
                
                # Should handle timeout gracefully
                assert 'trades' in result
                assert result['trades'] == []  # Should return empty list on error
                
            except Exception as e:
                # Should not raise unhandled exceptions
                pytest.fail(f"Unhandled exception during timeout: {e}")
        
        logger.info("✅ Connection timeout handling test passed")
    
    async def test_connection_refused(self, error_tester):
        """Test handling of connection refused errors"""
        logger.info("Testing connection refused handling")
        
        # Mock a connection refused error
        with patch.object(error_tester.tardis_connector, '_make_request') as mock_request:
            mock_request.side_effect = aiohttp.ClientConnectorError(
                connection_key=MagicMock(),
                os_error=OSError("Connection refused")
            )
            
            # Attempt to download data
            try:
                result = await error_tester.tardis_connector.download_daily_data_direct(
                    tardis_exchange='binance',
                    tardis_symbol='BTCUSDT',
                    date=TEST_DATE,
                    data_types=['trades']
                )
                
                # Should handle connection refused gracefully
                assert 'trades' in result
                assert result['trades'] == []  # Should return empty list on error
                
            except Exception as e:
                # Should not raise unhandled exceptions
                pytest.fail(f"Unhandled exception during connection refused: {e}")
        
        logger.info("✅ Connection refused handling test passed")
    
    async def test_dns_resolution_failure(self, error_tester):
        """Test handling of DNS resolution failures"""
        logger.info("Testing DNS resolution failure handling")
        
        # Mock a DNS resolution error
        with patch.object(error_tester.tardis_connector, '_make_request') as mock_request:
            mock_request.side_effect = aiohttp.ClientConnectorError(
                connection_key=MagicMock(),
                os_error=OSError("Name or service not known")
            )
            
            # Attempt to download data
            try:
                result = await error_tester.tardis_connector.download_daily_data_direct(
                    tardis_exchange='binance',
                    tardis_symbol='BTCUSDT',
                    date=TEST_DATE,
                    data_types=['trades']
                )
                
                # Should handle DNS failure gracefully
                assert 'trades' in result
                assert result['trades'] == []  # Should return empty list on error
                
            except Exception as e:
                # Should not raise unhandled exceptions
                pytest.fail(f"Unhandled exception during DNS failure: {e}")
        
        logger.info("✅ DNS resolution failure handling test passed")


@pytest.mark.asyncio
class TestAPIErrors:
    """Test API error handling"""
    
    async def test_http_404_error(self, error_tester):
        """Test handling of HTTP 404 errors"""
        logger.info("Testing HTTP 404 error handling")
        
        # Mock a 404 error
        with patch.object(error_tester.tardis_connector, '_make_request') as mock_request:
            mock_response = MagicMock()
            mock_response.status = 404
            mock_response.headers = {}
            mock_response.data = b'Not Found'
            
            mock_request.return_value = mock_response
            
            # Attempt to download data
            try:
                result = await error_tester.tardis_connector.download_daily_data_direct(
                    tardis_exchange='binance',
                    tardis_symbol='INVALID_SYMBOL',
                    date=TEST_DATE,
                    data_types=['trades']
                )
                
                # Should handle 404 gracefully
                assert 'trades' in result
                assert result['trades'] == []  # Should return empty list on error
                
            except Exception as e:
                # Should not raise unhandled exceptions
                pytest.fail(f"Unhandled exception during 404 error: {e}")
        
        logger.info("✅ HTTP 404 error handling test passed")
    
    async def test_http_500_error(self, error_tester):
        """Test handling of HTTP 500 errors"""
        logger.info("Testing HTTP 500 error handling")
        
        # Mock a 500 error
        with patch.object(error_tester.tardis_connector, '_make_request') as mock_request:
            mock_response = MagicMock()
            mock_response.status = 500
            mock_response.headers = {}
            mock_response.data = b'Internal Server Error'
            
            mock_request.return_value = mock_response
            
            # Attempt to download data
            try:
                result = await error_tester.tardis_connector.download_daily_data_direct(
                    tardis_exchange='binance',
                    tardis_symbol='BTCUSDT',
                    date=TEST_DATE,
                    data_types=['trades']
                )
                
                # Should handle 500 gracefully
                assert 'trades' in result
                assert result['trades'] == []  # Should return empty list on error
                
            except Exception as e:
                # Should not raise unhandled exceptions
                pytest.fail(f"Unhandled exception during 500 error: {e}")
        
        logger.info("✅ HTTP 500 error handling test passed")
    
    async def test_rate_limit_error(self, error_tester):
        """Test handling of rate limit errors"""
        logger.info("Testing rate limit error handling")
        
        # Mock a 429 rate limit error
        with patch.object(error_tester.tardis_connector, '_make_request') as mock_request:
            mock_response = MagicMock()
            mock_response.status = 429
            mock_response.headers = {'Retry-After': '60'}
            mock_response.data = b'Rate limit exceeded'
            
            mock_request.return_value = mock_response
            
            # Attempt to download data
            try:
                result = await error_tester.tardis_connector.download_daily_data_direct(
                    tardis_exchange='binance',
                    tardis_symbol='BTCUSDT',
                    date=TEST_DATE,
                    data_types=['trades']
                )
                
                # Should handle rate limit gracefully
                assert 'trades' in result
                assert result['trades'] == []  # Should return empty list on error
                
            except Exception as e:
                # Should not raise unhandled exceptions
                pytest.fail(f"Unhandled exception during rate limit error: {e}")
        
        logger.info("✅ Rate limit error handling test passed")
    
    async def test_authentication_error(self, error_tester):
        """Test handling of authentication errors"""
        logger.info("Testing authentication error handling")
        
        # Mock a 401 authentication error
        with patch.object(error_tester.tardis_connector, '_make_request') as mock_request:
            mock_response = MagicMock()
            mock_response.status = 401
            mock_response.headers = {}
            mock_response.data = b'Unauthorized'
            
            mock_request.return_value = mock_response
            
            # Attempt to download data
            try:
                result = await error_tester.tardis_connector.download_daily_data_direct(
                    tardis_exchange='binance',
                    tardis_symbol='BTCUSDT',
                    date=TEST_DATE,
                    data_types=['trades']
                )
                
                # Should handle authentication error gracefully
                assert 'trades' in result
                assert result['trades'] == []  # Should return empty list on error
                
            except Exception as e:
                # Should not raise unhandled exceptions
                pytest.fail(f"Unhandled exception during authentication error: {e}")
        
        logger.info("✅ Authentication error handling test passed")


@pytest.mark.asyncio
class TestDataParsingErrors:
    """Test data parsing error handling"""
    
    async def test_invalid_csv_data(self, error_tester):
        """Test handling of invalid CSV data"""
        logger.info("Testing invalid CSV data handling")
        
        # Mock invalid CSV data
        with patch.object(error_tester.tardis_connector, '_make_request') as mock_request:
            mock_response = MagicMock()
            mock_response.status = 200
            mock_response.headers = {'content-encoding': 'gzip'}
            mock_response.data = b'Invalid CSV data with broken structure'
            
            mock_request.return_value = mock_response
            
            # Attempt to download data
            try:
                result = await error_tester.tardis_connector.download_daily_data_direct(
                    tardis_exchange='binance',
                    tardis_symbol='BTCUSDT',
                    date=TEST_DATE,
                    data_types=['trades']
                )
                
                # Should handle invalid CSV gracefully
                assert 'trades' in result
                # Should return empty DataFrame or handle error gracefully
                assert result['trades'] is not None
                
            except Exception as e:
                # Should not raise unhandled exceptions
                pytest.fail(f"Unhandled exception during invalid CSV parsing: {e}")
        
        logger.info("✅ Invalid CSV data handling test passed")
    
    async def test_malformed_json_response(self, error_tester):
        """Test handling of malformed JSON responses"""
        logger.info("Testing malformed JSON response handling")
        
        # Mock malformed JSON response
        with patch.object(error_tester.tardis_connector, '_make_request') as mock_request:
            mock_response = MagicMock()
            mock_response.status = 200
            mock_response.headers = {}
            mock_response.data = b'{"incomplete": json structure'
            
            mock_request.return_value = mock_response
            
            # Attempt to download data
            try:
                result = await error_tester.tardis_connector.download_daily_data_direct(
                    tardis_exchange='binance',
                    tardis_symbol='BTCUSDT',
                    date=TEST_DATE,
                    data_types=['trades']
                )
                
                # Should handle malformed JSON gracefully
                assert 'trades' in result
                assert result['trades'] is not None
                
            except Exception as e:
                # Should not raise unhandled exceptions
                pytest.fail(f"Unhandled exception during malformed JSON parsing: {e}")
        
        logger.info("✅ Malformed JSON response handling test passed")
    
    async def test_empty_response(self, error_tester):
        """Test handling of empty responses"""
        logger.info("Testing empty response handling")
        
        # Mock empty response
        with patch.object(error_tester.tardis_connector, '_make_request') as mock_request:
            mock_response = MagicMock()
            mock_response.status = 200
            mock_response.headers = {}
            mock_response.data = b''
            
            mock_request.return_value = mock_response
            
            # Attempt to download data
            try:
                result = await error_tester.tardis_connector.download_daily_data_direct(
                    tardis_exchange='binance',
                    tardis_symbol='BTCUSDT',
                    date=TEST_DATE,
                    data_types=['trades']
                )
                
                # Should handle empty response gracefully
                assert 'trades' in result
                assert result['trades'] is not None
                
            except Exception as e:
                # Should not raise unhandled exceptions
                pytest.fail(f"Unhandled exception during empty response: {e}")
        
        logger.info("✅ Empty response handling test passed")


@pytest.mark.asyncio
class TestGCSUploadErrors:
    """Test GCS upload error handling"""
    
    async def test_gcs_upload_failure(self, error_tester):
        """Test handling of GCS upload failures"""
        logger.info("Testing GCS upload failure handling")
        
        # Create orchestrator with mocked GCS
        orchestrator = DownloadOrchestrator(
            gcs_bucket='test-bucket',
            api_key=error_tester.config.tardis.api_key,
            max_parallel_downloads=1,
            max_parallel_uploads=1,
            max_workers=1
        )
        
        # Mock GCS upload failure
        with patch.object(orchestrator.bucket, 'blob') as mock_blob:
            mock_blob_instance = MagicMock()
            mock_blob_instance.upload_from_filename.side_effect = Exception("GCS upload failed")
            mock_blob.return_value = mock_blob_instance
            
            # Mock successful download
            with patch.object(orchestrator.tardis_connector, 'download_daily_data_direct') as mock_download:
                mock_download.return_value = {
                    'trades': error_tester.tardis_connector._create_empty_dataframe_with_schema('trades')
                }
                
                # Attempt to upload
                try:
                    result = await orchestrator._upload_single_to_gcs(
                        download_results={'trades': error_tester.tardis_connector._create_empty_dataframe_with_schema('trades')},
                        target={'instrument_key': 'TEST:PERP:BTC-USDT'},
                        date=TEST_DATE
                    )
                    
                    # Should handle upload failure gracefully
                    assert isinstance(result, list)
                    assert len(result) == 0  # Should return empty list on failure
                    
                except Exception as e:
                    # Should not raise unhandled exceptions
                    pytest.fail(f"Unhandled exception during GCS upload failure: {e}")
        
        await orchestrator.tardis_connector.close()
        logger.info("✅ GCS upload failure handling test passed")
    
    async def test_gcs_permission_error(self, error_tester):
        """Test handling of GCS permission errors"""
        logger.info("Testing GCS permission error handling")
        
        # Create orchestrator with mocked GCS
        orchestrator = DownloadOrchestrator(
            gcs_bucket='test-bucket',
            api_key=error_tester.config.tardis.api_key,
            max_parallel_downloads=1,
            max_parallel_uploads=1,
            max_workers=1
        )
        
        # Mock GCS permission error
        with patch.object(orchestrator.bucket, 'blob') as mock_blob:
            mock_blob_instance = MagicMock()
            mock_blob_instance.upload_from_filename.side_effect = Exception("Permission denied")
            mock_blob.return_value = mock_blob_instance
            
            # Mock successful download
            with patch.object(orchestrator.tardis_connector, 'download_daily_data_direct') as mock_download:
                mock_download.return_value = {
                    'trades': error_tester.tardis_connector._create_empty_dataframe_with_schema('trades')
                }
                
                # Attempt to upload
                try:
                    result = await orchestrator._upload_single_to_gcs(
                        download_results={'trades': error_tester.tardis_connector._create_empty_dataframe_with_schema('trades')},
                        target={'instrument_key': 'TEST:PERP:BTC-USDT'},
                        date=TEST_DATE
                    )
                    
                    # Should handle permission error gracefully
                    assert isinstance(result, list)
                    assert len(result) == 0  # Should return empty list on failure
                    
                except Exception as e:
                    # Should not raise unhandled exceptions
                    pytest.fail(f"Unhandled exception during GCS permission error: {e}")
        
        await orchestrator.tardis_connector.close()
        logger.info("✅ GCS permission error handling test passed")


@pytest.mark.asyncio
class TestMemoryErrors:
    """Test memory error handling"""
    
    async def test_memory_exhaustion_handling(self, error_tester):
        """Test handling of memory exhaustion"""
        logger.info("Testing memory exhaustion handling")
        
        # Mock memory exhaustion
        with patch('psutil.virtual_memory') as mock_memory:
            mock_memory.return_value.percent = 99.9  # Simulate high memory usage
            
            # Create orchestrator with memory monitoring
            orchestrator = DownloadOrchestrator(
                gcs_bucket='test-bucket',
                api_key=error_tester.config.tardis.api_key,
                max_parallel_downloads=1,
                max_parallel_uploads=1,
                max_workers=1
            )
            
            # Mock memory monitor
            with patch.object(orchestrator.memory_monitor, 'is_memory_threshold_exceeded') as mock_threshold:
                mock_threshold.return_value = True
                
                # Attempt to process data
                try:
                    result = await orchestrator.download_and_upload_data(
                        date=TEST_DATE,
                        venues=['deribit'],
                        instrument_types=['PERP'],
                        data_types=['trades'],
                        max_instruments=1
                    )
                    
                    # Should handle memory exhaustion gracefully
                    assert result['status'] in ['success', 'no_targets']
                    
                except Exception as e:
                    # Should not raise unhandled exceptions
                    pytest.fail(f"Unhandled exception during memory exhaustion: {e}")
        
        await orchestrator.tardis_connector.close()
        logger.info("✅ Memory exhaustion handling test passed")


@pytest.mark.asyncio
class TestEdgeCases:
    """Test edge cases and boundary conditions"""
    
    async def test_invalid_date_handling(self, error_tester):
        """Test handling of invalid dates"""
        logger.info("Testing invalid date handling")
        
        # Test with invalid date
        invalid_date = datetime(1900, 1, 1, tzinfo=timezone.utc)
        
        try:
            result = await error_tester.tardis_connector.download_daily_data_direct(
                tardis_exchange='binance',
                tardis_symbol='BTCUSDT',
                date=invalid_date,
                data_types=['trades']
            )
            
            # Should handle invalid date gracefully
            assert 'trades' in result
            assert result['trades'] is not None
            
        except Exception as e:
            # Should not raise unhandled exceptions
            pytest.fail(f"Unhandled exception during invalid date: {e}")
        
        logger.info("✅ Invalid date handling test passed")
    
    async def test_invalid_symbol_handling(self, error_tester):
        """Test handling of invalid symbols"""
        logger.info("Testing invalid symbol handling")
        
        try:
            result = await error_tester.tardis_connector.download_daily_data_direct(
                tardis_exchange='binance',
                tardis_symbol='INVALID_SYMBOL_12345',
                date=TEST_DATE,
                data_types=['trades']
            )
            
            # Should handle invalid symbol gracefully
            assert 'trades' in result
            assert result['trades'] is not None
            
        except Exception as e:
            # Should not raise unhandled exceptions
            pytest.fail(f"Unhandled exception during invalid symbol: {e}")
        
        logger.info("✅ Invalid symbol handling test passed")
    
    async def test_invalid_data_type_handling(self, error_tester):
        """Test handling of invalid data types"""
        logger.info("Testing invalid data type handling")
        
        try:
            result = await error_tester.tardis_connector.download_daily_data_direct(
                tardis_exchange='binance',
                tardis_symbol='BTCUSDT',
                date=TEST_DATE,
                data_types=['invalid_data_type']
            )
            
            # Should handle invalid data type gracefully
            assert 'invalid_data_type' in result
            assert result['invalid_data_type'] == []  # Should return empty list
            
        except Exception as e:
            # Should not raise unhandled exceptions
            pytest.fail(f"Unhandled exception during invalid data type: {e}")
        
        logger.info("✅ Invalid data type handling test passed")


if __name__ == "__main__":
    # Run tests directly
    pytest.main([__file__, "-v"])
