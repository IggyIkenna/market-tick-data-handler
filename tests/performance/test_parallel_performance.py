"""
Performance tests for parallel processing
"""

import pytest
import os
import asyncio
import time
from unittest.mock import Mock, patch, AsyncMock
from market_data_tick_handler.data_downloader.tardis_connector import TardisConnector


@pytest.mark.performance
@pytest.mark.skipif(not os.getenv('PERFORMANCE_TESTS'), reason="Performance tests require external services")
class TestParallelPerformance:
    """Test parallel processing performance"""
    
    @pytest.mark.asyncio
    async def test_parallel_download_performance(self):
        """Test parallel download performance"""
        connector = TardisConnector("test_api_key")
        
        # Mock successful responses
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.text.return_value = "timestamp,price,amount,side\n1684800000000000,50000.0,0.1,buy"
        
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_get.return_value = mock_response
            
            start_time = time.time()
            
            # Make parallel requests
            tasks = []
            for i in range(5):
                task = connector.download_daily_data(
                    "BINANCE:SPOT_PAIR:BTC-USDT", 
                    None,
                    ["trades"]
                )
                tasks.append(task)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            duration = time.time() - start_time
            
            # Should complete in reasonable time
            assert duration < 8.0, f"Parallel download test took {duration:.2f}s, expected <8s"
            
            # All requests should complete
            assert len(results) == 5, f"Expected 5 results, got {len(results)}"
            
            # Some should succeed
            success_count = sum(1 for result in results if isinstance(result, dict))
            assert success_count > 0, "Some requests should succeed"
    
    @pytest.mark.asyncio
    async def test_concurrent_processing_performance(self):
        """Test concurrent processing performance"""
        connector = TardisConnector("test_api_key")
        
        # Mock successful responses
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.text.return_value = "timestamp,price,amount,side\n1684800000000000,50000.0,0.1,buy"
        
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_get.return_value = mock_response
            
            start_time = time.time()
            
            # Create concurrent tasks
            async def download_task(symbol):
                return await connector.download_daily_data(
                    f"BINANCE:SPOT_PAIR:{symbol}", 
                    None,
                    ["trades"]
                )
            
            # Run concurrent downloads
            symbols = ["BTC-USDT", "ETH-USDT", "ADA-USDT", "DOT-USDT", "LINK-USDT"]
            tasks = [download_task(symbol) for symbol in symbols]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            duration = time.time() - start_time
            
            # Should complete in reasonable time
            assert duration < 10.0, f"Concurrent processing test took {duration:.2f}s, expected <10s"
            
            # All requests should complete
            assert len(results) == 5, f"Expected 5 results, got {len(results)}"
            
            # Some should succeed
            success_count = sum(1 for result in results if isinstance(result, dict))
            assert success_count > 0, "Some requests should succeed"