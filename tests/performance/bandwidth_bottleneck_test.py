"""
Performance tests for bandwidth bottleneck detection
"""

import pytest
import asyncio
import time
from unittest.mock import Mock, patch, AsyncMock
from market_data_tick_handler.data_downloader.tardis_connector import TardisConnector


@pytest.mark.performance
class TestBandwidthBottleneck:
    """Test bandwidth bottleneck detection"""
    
    @pytest.mark.asyncio
    async def test_bandwidth_bottleneck_detection(self):
        """Test detection of bandwidth bottlenecks"""
        connector = TardisConnector("test_api_key")
        
        # Mock slow responses to simulate bandwidth bottleneck
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.text.return_value = "timestamp,price,amount,side\n1684800000000000,50000.0,0.1,buy"
        
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_get.return_value = mock_response
            
            # Simulate slow response by adding delay
            async def slow_response(*args, **kwargs):
                await asyncio.sleep(0.1)  # 100ms delay
                return mock_response
            
            mock_get.side_effect = slow_response
            
            start_time = time.time()
            
            # Make multiple requests
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
            
            # Should detect bandwidth bottleneck
            assert duration > 0.5, f"Bandwidth test completed too quickly: {duration:.2f}s"
            
            # All requests should complete
            assert len(results) == 5, f"Expected 5 results, got {len(results)}"
            
            # Some should succeed
            success_count = sum(1 for result in results if isinstance(result, dict))
            assert success_count > 0, "Some requests should succeed"
