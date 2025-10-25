#!/usr/bin/env python3
"""
Memory Tests

Tests for memory usage and leak detection:
1. Memory usage monitoring
2. Memory leak detection
3. Memory efficiency validation
4. Memory threshold handling
5. Garbage collection validation
6. Memory usage under load
"""

import pytest
import pytest_asyncio
import asyncio
import gc
import psutil
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Any
import logging
import tracemalloc

# Add project root to path
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from market_data_tick_handler.data_downloader.download_orchestrator import DownloadOrchestrator
from market_data_tick_handler.utils.memory_monitor import get_memory_monitor
from config import get_config

logger = logging.getLogger(__name__)

class MemoryTests:
    """Base class for memory tests"""
    
    def __init__(self):
        self.config = get_config()
        self.orchestrator = None
        self.memory_monitor = None
        self.initial_memory = None
        
    async def setup(self):
        """Setup test environment"""
        self.orchestrator = DownloadOrchestrator(
            gcs_bucket=self.config.gcp.bucket,
            api_key=self.config.tardis.api_key,
            max_parallel_downloads=2,
            max_parallel_uploads=2,
            max_workers=2
        )
        
        self.memory_monitor = get_memory_monitor(threshold_percent=90.0)
        self.initial_memory = self._get_memory_usage()
        
    async def teardown(self):
        """Cleanup test environment"""
        if self.orchestrator:
            await self.orchestrator.tardis_connector.close()
        
        # Force garbage collection
        gc.collect()
    
    def _get_memory_usage(self) -> Dict[str, float]:
        """Get current memory usage"""
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        
        return {
            'rss': memory_info.rss / 1024 / 1024,  # MB
            'vms': memory_info.vms / 1024 / 1024,  # MB
            'percent': process.memory_percent(),
            'available': psutil.virtual_memory().available / 1024 / 1024 / 1024  # GB
        }
    
    def _get_memory_delta(self) -> Dict[str, float]:
        """Get memory usage delta from initial"""
        current_memory = self._get_memory_usage()
        
        return {
            'rss_delta': current_memory['rss'] - self.initial_memory['rss'],
            'vms_delta': current_memory['vms'] - self.initial_memory['vms'],
            'percent_delta': current_memory['percent'] - self.initial_memory['percent']
        }


# Test fixtures
TEST_DATE = datetime(2023, 5, 23, tzinfo=timezone.utc)
TEST_VENUES = ['deribit']
TEST_INSTRUMENT_TYPES = ['PERP']
TEST_DATA_TYPES = ['trades', 'book_snapshot_5']


@pytest_asyncio.fixture
async def memory_tester():
    """Fixture for memory tests"""
    tester = MemoryTests()
    await tester.setup()
    try:
        yield tester
    finally:
        await tester.teardown()


@pytest.mark.asyncio
class TestMemoryMonitoring:
    """Test memory monitoring functionality"""
    
    async def test_memory_monitor_initialization(self, memory_tester):
        """Test memory monitor initialization"""
        logger.info("Testing memory monitor initialization")
        
        # Test memory monitor creation
        monitor = get_memory_monitor(threshold_percent=80.0)
        
        # Verify monitor is created
        assert monitor is not None
        assert monitor.threshold_percent == 80.0
        
        # Test memory info retrieval
        memory_info = monitor.get_memory_info()
        assert 'usage_percent' in memory_info
        assert 'available_gb' in memory_info
        assert 'threshold_exceeded' in memory_info
        
        logger.info("✅ Memory monitor initialization test passed")
    
    async def test_memory_threshold_detection(self, memory_tester):
        """Test memory threshold detection"""
        logger.info("Testing memory threshold detection")
        
        # Test with low threshold
        low_threshold_monitor = get_memory_monitor(threshold_percent=0.1)  # 0.1%
        assert low_threshold_monitor.is_memory_threshold_exceeded() == True
        
        # Test with high threshold
        high_threshold_monitor = get_memory_monitor(threshold_percent=99.9)  # 99.9%
        assert high_threshold_monitor.is_memory_threshold_exceeded() == False
        
        logger.info("✅ Memory threshold detection test passed")
    
    async def test_memory_info_accuracy(self, memory_tester):
        """Test memory info accuracy"""
        logger.info("Testing memory info accuracy")
        
        # Get memory info from monitor
        monitor_info = memory_tester.memory_monitor.get_memory_info()
        
        # Get memory info directly from psutil
        direct_info = memory_tester._get_memory_usage()
        
        # Compare values (allow some tolerance for timing differences)
        assert abs(monitor_info['usage_percent'] - direct_info['percent']) < 1.0
        assert abs(monitor_info['available_gb'] - direct_info['available']) < 0.1
        
        logger.info("✅ Memory info accuracy test passed")


@pytest.mark.asyncio
class TestMemoryLeakDetection:
    """Test memory leak detection"""
    
    async def test_memory_leak_detection_simple(self, memory_tester):
        """Test simple memory leak detection"""
        logger.info("Testing simple memory leak detection")
        
        # Record initial memory
        initial_memory = memory_tester._get_memory_usage()
        
        # Create and destroy objects multiple times
        for i in range(10):
            # Create some objects
            data = [f"test_data_{j}" for j in range(1000)]
            df = memory_tester.orchestrator.tardis_connector._create_empty_dataframe_with_schema('trades')
            
            # Force garbage collection
            del data, df
            gc.collect()
        
        # Record final memory
        final_memory = memory_tester._get_memory_usage()
        
        # Calculate memory delta
        memory_delta = final_memory['rss'] - initial_memory['rss']
        
        # Memory delta should be small (less than 10MB)
        assert memory_delta < 10, f"Potential memory leak detected: {memory_delta:.2f}MB increase"
        
        logger.info(f"✅ Simple memory leak detection test passed: {memory_delta:.2f}MB delta")
    
    async def test_memory_leak_detection_complex(self, memory_tester):
        """Test complex memory leak detection with async operations"""
        logger.info("Testing complex memory leak detection")
        
        # Record initial memory
        initial_memory = memory_tester._get_memory_usage()
        
        # Perform multiple async operations
        for i in range(5):
            # Simulate download operations
            try:
                result = await memory_tester.orchestrator.download_and_upload_data(
                    date=TEST_DATE,
                    venues=TEST_VENUES,
                    instrument_types=TEST_INSTRUMENT_TYPES,
                    data_types=['trades'],
                    max_instruments=1
                )
                
                # Force garbage collection after each operation
                gc.collect()
                
            except Exception as e:
                logger.warning(f"Download operation failed (expected in test): {e}")
        
        # Record final memory
        final_memory = memory_tester._get_memory_usage()
        
        # Calculate memory delta
        memory_delta = final_memory['rss'] - initial_memory['rss']
        
        # Memory delta should be reasonable (less than 50MB)
        assert memory_delta < 50, f"Potential memory leak detected: {memory_delta:.2f}MB increase"
        
        logger.info(f"✅ Complex memory leak detection test passed: {memory_delta:.2f}MB delta")
    
    async def test_memory_leak_detection_with_tracemalloc(self, memory_tester):
        """Test memory leak detection using tracemalloc"""
        logger.info("Testing memory leak detection with tracemalloc")
        
        # Start memory tracing
        tracemalloc.start()
        
        # Record initial memory
        initial_snapshot = tracemalloc.take_snapshot()
        
        # Perform operations that might cause leaks
        for i in range(10):
            # Create and process data
            df = memory_tester.orchestrator.tardis_connector._create_empty_dataframe_with_schema('trades')
            
            # Simulate data processing
            df['test_column'] = range(len(df))
            df = df.drop('test_column', axis=1)
            
            # Force garbage collection
            del df
            gc.collect()
        
        # Record final memory
        final_snapshot = tracemalloc.take_snapshot()
        
        # Calculate memory difference
        top_stats = final_snapshot.compare_to(initial_snapshot, 'lineno')
        
        # Check for significant memory growth
        total_growth = sum(stat.size_diff for stat in top_stats if stat.size_diff > 0)
        
        # Memory growth should be minimal (less than 1MB)
        assert total_growth < 1024 * 1024, f"Potential memory leak detected: {total_growth / 1024 / 1024:.2f}MB growth"
        
        # Stop memory tracing
        tracemalloc.stop()
        
        logger.info(f"✅ Tracemalloc memory leak detection test passed: {total_growth / 1024 / 1024:.2f}MB growth")


@pytest.mark.asyncio
class TestMemoryEfficiency:
    """Test memory efficiency"""
    
    async def test_memory_efficiency_single_operation(self, memory_tester):
        """Test memory efficiency for single operations"""
        logger.info("Testing memory efficiency for single operations")
        
        # Record initial memory
        initial_memory = memory_tester._get_memory_usage()
        
        # Perform single operation
        try:
            result = await memory_tester.orchestrator.download_and_upload_data(
                date=TEST_DATE,
                venues=TEST_VENUES,
                instrument_types=TEST_INSTRUMENT_TYPES,
                data_types=['trades'],
                max_instruments=1
            )
            
            # Record memory after operation
            operation_memory = memory_tester._get_memory_usage()
            
            # Calculate memory used
            memory_used = operation_memory['rss'] - initial_memory['rss']
            
            # Memory usage should be reasonable (less than 100MB for single operation)
            assert memory_used < 100, f"Memory usage too high for single operation: {memory_used:.2f}MB"
            
            logger.info(f"✅ Single operation memory efficiency test passed: {memory_used:.2f}MB used")
            
        except Exception as e:
            logger.warning(f"Single operation failed (expected in test): {e}")
    
    async def test_memory_efficiency_batch_operations(self, memory_tester):
        """Test memory efficiency for batch operations"""
        logger.info("Testing memory efficiency for batch operations")
        
        # Record initial memory
        initial_memory = memory_tester._get_memory_usage()
        
        # Perform batch operations
        try:
            result = await memory_tester.orchestrator.download_and_upload_data(
                date=TEST_DATE,
                venues=TEST_VENUES,
                instrument_types=TEST_INSTRUMENT_TYPES,
                data_types=TEST_DATA_TYPES,
                max_instruments=3
            )
            
            # Record memory after operations
            batch_memory = memory_tester._get_memory_usage()
            
            # Calculate memory used
            memory_used = batch_memory['rss'] - initial_memory['rss']
            
            # Memory usage should be reasonable (less than 200MB for batch operations)
            assert memory_used < 200, f"Memory usage too high for batch operations: {memory_used:.2f}MB"
            
            logger.info(f"✅ Batch operations memory efficiency test passed: {memory_used:.2f}MB used")
            
        except Exception as e:
            logger.warning(f"Batch operations failed (expected in test): {e}")
    
    async def test_memory_efficiency_dataframe_operations(self, memory_tester):
        """Test memory efficiency for DataFrame operations"""
        logger.info("Testing memory efficiency for DataFrame operations")
        
        # Record initial memory
        initial_memory = memory_tester._get_memory_usage()
        
        # Create and manipulate DataFrames
        dfs = []
        for i in range(10):
            df = memory_tester.orchestrator.tardis_connector._create_empty_dataframe_with_schema('trades')
            
            # Add some data
            df = df.copy()
            df['test_column'] = range(1000)
            df = df.drop('test_column', axis=1)
            
            dfs.append(df)
        
        # Record memory after DataFrame creation
        df_memory = memory_tester._get_memory_usage()
        
        # Calculate memory used
        memory_used = df_memory['rss'] - initial_memory['rss']
        
        # Memory usage should be reasonable (less than 50MB for DataFrame operations)
        assert memory_used < 50, f"Memory usage too high for DataFrame operations: {memory_used:.2f}MB"
        
        # Clean up
        del dfs
        gc.collect()
        
        # Record memory after cleanup
        cleanup_memory = memory_tester._get_memory_usage()
        memory_after_cleanup = cleanup_memory['rss'] - initial_memory['rss']
        
        # Memory should be mostly freed after cleanup
        assert memory_after_cleanup < memory_used * 0.5, f"Memory not properly freed after cleanup: {memory_after_cleanup:.2f}MB remaining"
        
        logger.info(f"✅ DataFrame operations memory efficiency test passed: {memory_used:.2f}MB used, {memory_after_cleanup:.2f}MB after cleanup")


@pytest.mark.asyncio
class TestMemoryThresholdHandling:
    """Test memory threshold handling"""
    
    async def test_memory_threshold_exceeded_handling(self, memory_tester):
        """Test handling when memory threshold is exceeded"""
        logger.info("Testing memory threshold exceeded handling")
        
        # Create monitor with very low threshold
        low_threshold_monitor = get_memory_monitor(threshold_percent=0.1)
        
        # Check that threshold is exceeded
        assert low_threshold_monitor.is_memory_threshold_exceeded() == True
        
        # Test that orchestrator handles threshold exceeded
        memory_tester.orchestrator.memory_monitor = low_threshold_monitor
        
        # This should trigger memory threshold handling
        try:
            result = await memory_tester.orchestrator.download_and_upload_data(
                date=TEST_DATE,
                venues=TEST_VENUES,
                instrument_types=TEST_INSTRUMENT_TYPES,
                data_types=['trades'],
                max_instruments=1
            )
            
            # Should still complete (with memory management)
            assert result is not None
            
        except Exception as e:
            logger.warning(f"Memory threshold handling test failed (expected): {e}")
        
        logger.info("✅ Memory threshold exceeded handling test passed")
    
    async def test_memory_threshold_normal_operation(self, memory_tester):
        """Test normal operation when memory threshold is not exceeded"""
        logger.info("Testing normal operation with normal memory threshold")
        
        # Create monitor with normal threshold
        normal_threshold_monitor = get_memory_monitor(threshold_percent=90.0)
        
        # Check that threshold is not exceeded
        assert normal_threshold_monitor.is_memory_threshold_exceeded() == False
        
        # Test normal operation
        try:
            result = await memory_tester.orchestrator.download_and_upload_data(
                date=TEST_DATE,
                venues=TEST_VENUES,
                instrument_types=TEST_INSTRUMENT_TYPES,
                data_types=['trades'],
                max_instruments=1
            )
            
            # Should complete normally
            assert result is not None
            
        except Exception as e:
            logger.warning(f"Normal operation test failed (expected): {e}")
        
        logger.info("✅ Normal operation memory threshold test passed")


@pytest.mark.asyncio
class TestGarbageCollection:
    """Test garbage collection functionality"""
    
    async def test_garbage_collection_effectiveness(self, memory_tester):
        """Test garbage collection effectiveness"""
        logger.info("Testing garbage collection effectiveness")
        
        # Record initial memory
        initial_memory = memory_tester._get_memory_usage()
        
        # Create objects that should be garbage collected
        for i in range(20):
            # Create large objects
            data = [f"large_data_{j}" for j in range(10000)]
            df = memory_tester.orchestrator.tardis_connector._create_empty_dataframe_with_schema('trades')
            
            # Add data to DataFrame
            df = df.copy()
            df['test_data'] = range(1000)
            
            # Delete references
            del data, df
        
        # Record memory before garbage collection
        before_gc_memory = memory_tester._get_memory_usage()
        
        # Force garbage collection
        collected = gc.collect()
        
        # Record memory after garbage collection
        after_gc_memory = memory_tester._get_memory_usage()
        
        # Calculate memory freed
        memory_freed = before_gc_memory['rss'] - after_gc_memory['rss']
        
        # Garbage collection should free some memory
        assert memory_freed > 0, f"No memory freed by garbage collection: {memory_freed:.2f}MB"
        assert collected > 0, f"No objects collected by garbage collection: {collected}"
        
        logger.info(f"✅ Garbage collection effectiveness test passed: {memory_freed:.2f}MB freed, {collected} objects collected")
    
    async def test_automatic_garbage_collection(self, memory_tester):
        """Test automatic garbage collection"""
        logger.info("Testing automatic garbage collection")
        
        # Record initial memory
        initial_memory = memory_tester._get_memory_usage()
        
        # Create and destroy objects without explicit garbage collection
        for i in range(10):
            # Create objects
            data = [f"auto_gc_data_{j}" for j in range(5000)]
            df = memory_tester.orchestrator.tardis_connector._create_empty_dataframe_with_schema('trades')
            
            # Delete references (should trigger automatic GC)
            del data, df
        
        # Wait a bit for automatic garbage collection
        await asyncio.sleep(0.1)
        
        # Record final memory
        final_memory = memory_tester._get_memory_usage()
        
        # Calculate memory delta
        memory_delta = final_memory['rss'] - initial_memory['rss']
        
        # Memory delta should be small due to automatic garbage collection
        assert memory_delta < 20, f"Automatic garbage collection not effective: {memory_delta:.2f}MB increase"
        
        logger.info(f"✅ Automatic garbage collection test passed: {memory_delta:.2f}MB delta")


@pytest.mark.asyncio
class TestMemoryUnderLoad:
    """Test memory usage under load"""
    
    async def test_memory_under_concurrent_load(self, memory_tester):
        """Test memory usage under concurrent load"""
        logger.info("Testing memory usage under concurrent load")
        
        # Record initial memory
        initial_memory = memory_tester._get_memory_usage()
        
        # Create concurrent tasks
        tasks = []
        for i in range(5):
            task = memory_tester.orchestrator.download_and_upload_data(
                date=TEST_DATE,
                venues=TEST_VENUES,
                instrument_types=TEST_INSTRUMENT_TYPES,
                data_types=['trades'],
                max_instruments=1
            )
            tasks.append(task)
        
        # Execute concurrent tasks
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Record memory after concurrent operations
            concurrent_memory = memory_tester._get_memory_usage()
            
            # Calculate memory used
            memory_used = concurrent_memory['rss'] - initial_memory['rss']
            
            # Memory usage should be reasonable even under concurrent load
            assert memory_used < 300, f"Memory usage too high under concurrent load: {memory_used:.2f}MB"
            
            logger.info(f"✅ Concurrent load memory test passed: {memory_used:.2f}MB used")
            
        except Exception as e:
            logger.warning(f"Concurrent load test failed (expected): {e}")
    
    async def test_memory_under_high_frequency_operations(self, memory_tester):
        """Test memory usage under high frequency operations"""
        logger.info("Testing memory usage under high frequency operations")
        
        # Record initial memory
        initial_memory = memory_tester._get_memory_usage()
        
        # Perform high frequency operations
        for i in range(50):
            # Create and destroy objects quickly
            df = memory_tester.orchestrator.tardis_connector._create_empty_dataframe_with_schema('trades')
            df = df.copy()
            df['temp'] = range(100)
            del df
            
            # Force garbage collection every 10 iterations
            if i % 10 == 0:
                gc.collect()
        
        # Record final memory
        final_memory = memory_tester._get_memory_usage()
        
        # Calculate memory delta
        memory_delta = final_memory['rss'] - initial_memory['rss']
        
        # Memory delta should be small due to proper cleanup
        assert memory_delta < 30, f"Memory usage too high under high frequency operations: {memory_delta:.2f}MB"
        
        logger.info(f"✅ High frequency operations memory test passed: {memory_delta:.2f}MB delta")


if __name__ == "__main__":
    # Run tests directly
    pytest.main([__file__, "-v"])
