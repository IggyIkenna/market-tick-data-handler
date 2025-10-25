"""
Performance tests for connection reuse
"""

import pytest
import os
import asyncio
import time
from unittest.mock import Mock, patch, AsyncMock
from market_data_tick_handler.data_downloader.tardis_connector import TardisConnector


@pytest.mark.performance
@pytest.mark.skipif(not os.getenv('PERFORMANCE_TESTS'), reason="Performance tests require external services")
class TestConnectionReusePerformance:
    """Test connection reuse performance"""
    
    @pytest.mark.asyncio
    async def test_connection_reuse_benchmark(self):
        """Benchmark connection reuse performance"""
        connector = TardisConnector("test_api_key")
        
        # Mock successful responses
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.text.return_value = "timestamp,price,amount,side\n1684800000000000,50000.0,0.1,buy"
        
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_get.return_value = mock_response
            
            # Test with connection reuse (same session)
            start_time = time.time()
            
            # Make 10 requests
            tasks = []
            for i in range(10):
                task = connector.download_daily_data(
                    "BINANCE:SPOT_PAIR:BTC-USDT", 
                    None,  # Will use current date
                    ["trades"]
                )
                tasks.append(task)
            
            results = await asyncio.gather(*tasks)
            
            duration = time.time() - start_time
            
            # Should complete in under 5 seconds
            assert duration < 5.0, f"Connection reuse test took {duration:.2f}s, expected <5s"
            
            # All requests should succeed
            assert len(results) == 10
            for result in results:
                assert "trades" in result
                assert len(result["trades"]) == 1
    
    @pytest.mark.asyncio
    async def test_connection_reuse_vs_new_connections(self):
        """Compare connection reuse vs new connections"""
        connector = TardisConnector("test_api_key")
        
        # Mock successful responses
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.text.return_value = "timestamp,price,amount,side\n1684800000000000,50000.0,0.1,buy"
        
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_get.return_value = mock_response
            
            # Test connection reuse
            start_time = time.time()
            
            tasks = []
            for i in range(5):
                task = connector.download_daily_data(
                    "binance", 
                    "BTC-USDT", 
                    None,
                    ["trades"]
                )
                tasks.append(task)
            
            await asyncio.gather(*tasks)
            reuse_duration = time.time() - start_time
            
            # Test new connections (simulate by creating new connector)
            start_time = time.time()
            
            tasks = []
            for i in range(5):
                new_connector = TardisConnector("test_api_key")
                task = new_connector.download_daily_data(
                    "binance", 
                    "BTC-USDT", 
                    None,
                    ["trades"]
                )
                tasks.append(task)
            
            await asyncio.gather(*tasks)
            new_connection_duration = time.time() - start_time
            
            # Connection reuse should be faster
            assert reuse_duration < new_connection_duration, \
                f"Connection reuse ({reuse_duration:.2f}s) should be faster than new connections ({new_connection_duration:.2f}s)"
    
    @pytest.mark.asyncio
    async def test_concurrent_requests_performance(self):
        """Test performance with concurrent requests"""
        connector = TardisConnector("test_api_key")
        
        # Mock successful responses
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.text.return_value = "timestamp,price,amount,side\n1684800000000000,50000.0,0.1,buy"
        
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_get.return_value = mock_response
            
            # Test with different concurrency levels
            concurrency_levels = [1, 2, 5, 10]
            
            for concurrency in concurrency_levels:
                start_time = time.time()
                
                # Create semaphore to limit concurrency
                semaphore = asyncio.Semaphore(concurrency)
                
                async def limited_request():
                    async with semaphore:
                        return await connector.download_daily_data(
                            "binance", 
                            "BTC-USDT", 
                            None,
                            ["trades"]
                        )
                
                # Make 10 requests with limited concurrency
                tasks = [limited_request() for _ in range(10)]
                results = await asyncio.gather(*tasks)
                
                duration = time.time() - start_time
                
                # Should complete in reasonable time
                assert duration < 10.0, f"Concurrency {concurrency} took {duration:.2f}s, expected <10s"
                
                # All requests should succeed
                assert len(results) == 10
                for result in results:
                    assert "trades" in result
    
    @pytest.mark.asyncio
    async def test_error_handling_performance(self):
        """Test performance with error handling"""
        connector = TardisConnector("test_api_key")
        
        # Mock mixed responses (some success, some errors)
        def mock_response_generator():
            responses = [
                Mock(status=200, text=lambda: "timestamp,price,amount,side\n1684800000000000,50000.0,0.1,buy"),
                Mock(status=404, text=lambda: "Not found"),
                Mock(status=200, text=lambda: "timestamp,price,amount,side\n1684800000000000,50000.0,0.1,buy"),
                Mock(status=500, text=lambda: "Server error"),
                Mock(status=200, text=lambda: "timestamp,price,amount,side\n1684800000000000,50000.0,0.1,buy"),
            ]
            for response in responses:
                response.__aenter__ = AsyncMock(return_value=response)
                response.__aexit__ = AsyncMock(return_value=None)
                yield response
        
        response_gen = mock_response_generator()
        
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_get.side_effect = lambda *args, **kwargs: next(response_gen)
            
            start_time = time.time()
            
            # Make 5 requests (some will fail)
            tasks = []
            for i in range(5):
                task = connector.download_daily_data(
                    "binance", 
                    "BTC-USDT", 
                    None,
                    ["trades"]
                )
                tasks.append(task)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            duration = time.time() - start_time
            
            # Should complete in reasonable time even with errors
            assert duration < 8.0, f"Error handling test took {duration:.2f}s, expected <8s"
            
            # Some should succeed, some should fail
            success_count = sum(1 for result in results if isinstance(result, dict) and "trades" in result)
            error_count = sum(1 for result in results if isinstance(result, Exception))
            
            assert success_count > 0, "Some requests should succeed"
            assert error_count > 0, "Some requests should fail"
            assert success_count + error_count == 5, "All requests should be processed"