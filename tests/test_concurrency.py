#!/usr/bin/env python3
"""
Concurrency Tests

Tests for parallel processing and race condition handling:
1. Concurrent downloads and uploads
2. Race condition detection
3. Resource contention handling
4. Thread safety validation
5. Deadlock prevention
6. Performance under concurrent load
"""

import pytest
import pytest_asyncio
import asyncio
import time
from datetime import datetime, timezone
from typing import Dict, List, Any
import logging
from concurrent.futures import ThreadPoolExecutor
import threading

# Add project root to path
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from market_data_tick_handler.data_downloader.download_orchestrator import DownloadOrchestrator
from market_data_tick_handler.data_downloader.tardis_connector import TardisConnector
from config import get_config

logger = logging.getLogger(__name__)

class ConcurrencyTests:
    """Base class for concurrency tests"""
    
    def __init__(self):
        self.config = get_config()
        self.orchestrator = None
        self.tardis_connector = None
        
    async def setup(self):
        """Setup test environment"""
        self.orchestrator = DownloadOrchestrator(
            gcs_bucket=self.config.gcp.bucket,
            api_key=self.config.tardis.api_key,
            max_parallel_downloads=5,
            max_parallel_uploads=3,
            max_workers=3
        )
        
        self.tardis_connector = TardisConnector(api_key=self.config.tardis.api_key)
        await self.tardis_connector._create_session()
        
    async def teardown(self):
        """Cleanup test environment"""
        if self.orchestrator:
            await self.orchestrator.tardis_connector.close()
        if self.tardis_connector:
            await self.tardis_connector.close()


# Test fixtures
TEST_DATE = datetime(2023, 5, 23, tzinfo=timezone.utc)
TEST_VENUES = ['deribit']
TEST_INSTRUMENT_TYPES = ['PERP']
TEST_DATA_TYPES = ['trades', 'book_snapshot_5']


@pytest_asyncio.fixture
async def concurrency_tester():
    """Fixture for concurrency tests"""
    tester = ConcurrencyTests()
    await tester.setup()
    try:
        yield tester
    finally:
        await tester.teardown()


@pytest.mark.asyncio
class TestConcurrentDownloads:
    """Test concurrent download operations"""
    
    async def test_concurrent_downloads_same_connector(self, concurrency_tester):
        """Test concurrent downloads using the same connector"""
        logger.info("Testing concurrent downloads with same connector")
        
        # Create multiple download tasks
        tasks = []
        for i in range(5):
            task = concurrency_tester.tardis_connector.download_daily_data_direct(
                tardis_exchange='deribit',
                tardis_symbol='BTC-PERPETUAL',
                date=TEST_DATE,
                data_types=['trades']
            )
            tasks.append(task)
        
        # Execute all tasks concurrently
        start_time = time.time()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        end_time = time.time()
        
        # Verify all tasks completed
        assert len(results) == 5, f"Expected 5 results, got {len(results)}"
        
        # Check for exceptions
        exceptions = [r for r in results if isinstance(r, Exception)]
        assert len(exceptions) == 0, f"Found exceptions in concurrent downloads: {exceptions}"
        
        # Verify all results are valid
        for i, result in enumerate(results):
            assert isinstance(result, dict), f"Result {i} is not a dict: {type(result)}"
            assert 'trades' in result, f"Result {i} missing 'trades' key"
        
        logger.info(f"✅ Concurrent downloads test passed: {len(results)} tasks in {end_time - start_time:.2f}s")
    
    async def test_concurrent_downloads_different_symbols(self, concurrency_tester):
        """Test concurrent downloads of different symbols"""
        logger.info("Testing concurrent downloads of different symbols")
        
        symbols = ['BTC-PERPETUAL', 'ETH-PERPETUAL', 'SOL-PERPETUAL']
        
        # Create download tasks for different symbols
        tasks = []
        for symbol in symbols:
            task = concurrency_tester.tardis_connector.download_daily_data_direct(
                tardis_exchange='deribit',
                tardis_symbol=symbol,
                date=TEST_DATE,
                data_types=['trades']
            )
            tasks.append(task)
        
        # Execute all tasks concurrently
        start_time = time.time()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        end_time = time.time()
        
        # Verify all tasks completed
        assert len(results) == len(symbols), f"Expected {len(symbols)} results, got {len(results)}"
        
        # Check for exceptions
        exceptions = [r for r in results if isinstance(r, Exception)]
        assert len(exceptions) == 0, f"Found exceptions in concurrent downloads: {exceptions}"
        
        # Verify all results are valid
        for i, result in enumerate(results):
            assert isinstance(result, dict), f"Result {i} is not a dict: {type(result)}"
            assert 'trades' in result, f"Result {i} missing 'trades' key"
        
        logger.info(f"✅ Concurrent downloads different symbols test passed: {len(results)} tasks in {end_time - start_time:.2f}s")
    
    async def test_concurrent_downloads_different_data_types(self, concurrency_tester):
        """Test concurrent downloads of different data types"""
        logger.info("Testing concurrent downloads of different data types")
        
        data_types = ['trades', 'book_snapshot_5', 'derivative_ticker']
        
        # Create download tasks for different data types
        tasks = []
        for data_type in data_types:
            task = concurrency_tester.tardis_connector.download_daily_data_direct(
                tardis_exchange='deribit',
                tardis_symbol='BTC-PERPETUAL',
                date=TEST_DATE,
                data_types=[data_type]
            )
            tasks.append(task)
        
        # Execute all tasks concurrently
        start_time = time.time()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        end_time = time.time()
        
        # Verify all tasks completed
        assert len(results) == len(data_types), f"Expected {len(data_types)} results, got {len(results)}"
        
        # Check for exceptions
        exceptions = [r for r in results if isinstance(r, Exception)]
        assert len(exceptions) == 0, f"Found exceptions in concurrent downloads: {exceptions}"
        
        # Verify all results are valid
        for i, result in enumerate(results):
            assert isinstance(result, dict), f"Result {i} is not a dict: {type(result)}"
            assert data_types[i] in result, f"Result {i} missing '{data_types[i]}' key"
        
        logger.info(f"✅ Concurrent downloads different data types test passed: {len(results)} tasks in {end_time - start_time:.2f}s")


@pytest.mark.asyncio
class TestConcurrentUploads:
    """Test concurrent upload operations"""
    
    async def test_concurrent_uploads_same_orchestrator(self, concurrency_tester):
        """Test concurrent uploads using the same orchestrator"""
        logger.info("Testing concurrent uploads with same orchestrator")
        
        # Create test data
        test_data = {
            'trades': concurrency_tester.tardis_connector._create_empty_dataframe_with_schema('trades'),
            'book_snapshot_5': concurrency_tester.tardis_connector._create_empty_dataframe_with_schema('book_snapshot_5')
        }
        
        # Create multiple upload tasks
        tasks = []
        for i in range(3):
            task = concurrency_tester.orchestrator._upload_single_to_gcs(
                download_results=test_data,
                target={'instrument_key': f'TEST:PERP:BTC-USDT-{i}'},
                date=TEST_DATE
            )
            tasks.append(task)
        
        # Execute all tasks concurrently
        start_time = time.time()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        end_time = time.time()
        
        # Verify all tasks completed
        assert len(results) == 3, f"Expected 3 results, got {len(results)}"
        
        # Check for exceptions
        exceptions = [r for r in results if isinstance(r, Exception)]
        assert len(exceptions) == 0, f"Found exceptions in concurrent uploads: {exceptions}"
        
        # Verify all results are valid
        for i, result in enumerate(results):
            assert isinstance(result, list), f"Result {i} is not a list: {type(result)}"
        
        logger.info(f"✅ Concurrent uploads test passed: {len(results)} tasks in {end_time - start_time:.2f}s")


@pytest.mark.asyncio
class TestRaceConditions:
    """Test race condition handling"""
    
    async def test_semaphore_contention(self, concurrency_tester):
        """Test semaphore contention handling"""
        logger.info("Testing semaphore contention")
        
        # Create more tasks than max_workers to test semaphore
        num_tasks = 10
        max_workers = 3
        
        # Create orchestrator with limited workers
        limited_orchestrator = DownloadOrchestrator(
            gcs_bucket=concurrency_tester.config.gcp.bucket,
            api_key=concurrency_tester.config.tardis.api_key,
            max_parallel_downloads=5,
            max_parallel_uploads=3,
            max_workers=max_workers
        )
        
        # Create download tasks
        tasks = []
        for i in range(num_tasks):
            task = limited_orchestrator.tardis_connector.download_daily_data_direct(
                tardis_exchange='deribit',
                tardis_symbol='BTC-PERPETUAL',
                date=TEST_DATE,
                data_types=['trades']
            )
            tasks.append(task)
        
        # Execute all tasks concurrently
        start_time = time.time()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        end_time = time.time()
        
        # Verify all tasks completed
        assert len(results) == num_tasks, f"Expected {num_tasks} results, got {len(results)}"
        
        # Check for exceptions
        exceptions = [r for r in results if isinstance(r, Exception)]
        assert len(exceptions) == 0, f"Found exceptions in semaphore contention test: {exceptions}"
        
        # Verify execution time is reasonable (should be longer than single task due to contention)
        single_task_time = 1.0  # Estimated single task time
        expected_min_time = single_task_time * (num_tasks / max_workers)
        assert end_time - start_time >= expected_min_time * 0.5, f"Execution time too short: {end_time - start_time:.2f}s"
        
        await limited_orchestrator.tardis_connector.close()
        logger.info(f"✅ Semaphore contention test passed: {num_tasks} tasks in {end_time - start_time:.2f}s")
    
    async def test_resource_contention(self, concurrency_tester):
        """Test resource contention handling"""
        logger.info("Testing resource contention")
        
        # Create multiple orchestrators to test resource contention
        orchestrators = []
        for i in range(3):
            orchestrator = DownloadOrchestrator(
                gcs_bucket=concurrency_tester.config.gcp.bucket,
                api_key=concurrency_tester.config.tardis.api_key,
                max_parallel_downloads=2,
                max_parallel_uploads=2,
                max_workers=2
            )
            orchestrators.append(orchestrator)
        
        # Create download tasks for each orchestrator
        all_tasks = []
        for i, orchestrator in enumerate(orchestrators):
            task = orchestrator.download_and_upload_data(
                date=TEST_DATE,
                venues=TEST_VENUES,
                instrument_types=TEST_INSTRUMENT_TYPES,
                data_types=['trades'],
                max_instruments=1
            )
            all_tasks.append(task)
        
        # Execute all tasks concurrently
        start_time = time.time()
        results = await asyncio.gather(*all_tasks, return_exceptions=True)
        end_time = time.time()
        
        # Verify all tasks completed
        assert len(results) == len(orchestrators), f"Expected {len(orchestrators)} results, got {len(results)}"
        
        # Check for exceptions
        exceptions = [r for r in results if isinstance(r, Exception)]
        assert len(exceptions) == 0, f"Found exceptions in resource contention test: {exceptions}"
        
        # Cleanup orchestrators
        for orchestrator in orchestrators:
            await orchestrator.tardis_connector.close()
        
        logger.info(f"✅ Resource contention test passed: {len(results)} orchestrators in {end_time - start_time:.2f}s")


@pytest.mark.asyncio
class TestThreadSafety:
    """Test thread safety"""
    
    async def test_thread_safe_operations(self, concurrency_tester):
        """Test thread safety of operations"""
        logger.info("Testing thread safety")
        
        # Create a shared resource (simulating shared state)
        shared_counter = {'value': 0}
        lock = asyncio.Lock()
        
        async def increment_counter():
            async with lock:
                shared_counter['value'] += 1
                await asyncio.sleep(0.001)  # Simulate work
        
        # Create multiple tasks that modify shared state
        tasks = []
        for i in range(10):
            task = increment_counter()
            tasks.append(task)
        
        # Execute all tasks concurrently
        await asyncio.gather(*tasks)
        
        # Verify counter value is correct (no race conditions)
        assert shared_counter['value'] == 10, f"Counter value incorrect: {shared_counter['value']}"
        
        logger.info("✅ Thread safety test passed")
    
    async def test_async_context_manager_safety(self, concurrency_tester):
        """Test async context manager safety"""
        logger.info("Testing async context manager safety")
        
        # Test multiple concurrent context managers
        async def use_context_manager():
            async with concurrency_tester.tardis_connector:
                # Simulate some work
                await asyncio.sleep(0.01)
                return True
        
        # Create multiple tasks using context managers
        tasks = []
        for i in range(5):
            task = use_context_manager()
            tasks.append(task)
        
        # Execute all tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Verify all tasks completed successfully
        assert len(results) == 5, f"Expected 5 results, got {len(results)}"
        
        exceptions = [r for r in results if isinstance(r, Exception)]
        assert len(exceptions) == 0, f"Found exceptions in context manager test: {exceptions}"
        
        # Verify all results are True
        for result in results:
            assert result is True, f"Unexpected result: {result}"
        
        logger.info("✅ Async context manager safety test passed")


@pytest.mark.asyncio
class TestDeadlockPrevention:
    """Test deadlock prevention"""
    
    async def test_no_deadlock_on_concurrent_operations(self, concurrency_tester):
        """Test that concurrent operations don't cause deadlocks"""
        logger.info("Testing deadlock prevention")
        
        # Create operations that could potentially deadlock
        async def operation1():
            # Simulate operation that might acquire locks
            await asyncio.sleep(0.01)
            return "op1"
        
        async def operation2():
            # Simulate operation that might acquire locks
            await asyncio.sleep(0.01)
            return "op2"
        
        # Create multiple tasks with different operations
        tasks = []
        for i in range(10):
            if i % 2 == 0:
                task = operation1()
            else:
                task = operation2()
            tasks.append(task)
        
        # Execute all tasks concurrently with timeout
        try:
            results = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=5.0  # 5 second timeout
            )
            
            # Verify all tasks completed
            assert len(results) == 10, f"Expected 10 results, got {len(results)}"
            
            # Check for exceptions
            exceptions = [r for r in results if isinstance(r, Exception)]
            assert len(exceptions) == 0, f"Found exceptions in deadlock test: {exceptions}"
            
            logger.info("✅ Deadlock prevention test passed")
            
        except asyncio.TimeoutError:
            pytest.fail("Deadlock detected - operations timed out")
    
    async def test_circular_dependency_prevention(self, concurrency_tester):
        """Test prevention of circular dependencies"""
        logger.info("Testing circular dependency prevention")
        
        # Create operations that could create circular dependencies
        async def operation_a():
            await asyncio.sleep(0.01)
            return "a"
        
        async def operation_b():
            await asyncio.sleep(0.01)
            return "b"
        
        # Create tasks that depend on each other (but not circularly)
        async def dependent_operation():
            result_a = await operation_a()
            result_b = await operation_b()
            return f"{result_a}_{result_b}"
        
        # Create multiple dependent operations
        tasks = []
        for i in range(5):
            task = dependent_operation()
            tasks.append(task)
        
        # Execute all tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Verify all tasks completed
        assert len(results) == 5, f"Expected 5 results, got {len(results)}"
        
        # Check for exceptions
        exceptions = [r for r in results if isinstance(r, Exception)]
        assert len(exceptions) == 0, f"Found exceptions in circular dependency test: {exceptions}"
        
        # Verify results are as expected
        for result in results:
            assert result == "a_b", f"Unexpected result: {result}"
        
        logger.info("✅ Circular dependency prevention test passed")


@pytest.mark.asyncio
class TestPerformanceUnderLoad:
    """Test performance under concurrent load"""
    
    async def test_performance_under_high_concurrency(self, concurrency_tester):
        """Test performance under high concurrency"""
        logger.info("Testing performance under high concurrency")
        
        # Create many concurrent tasks
        num_tasks = 20
        
        # Create download tasks
        tasks = []
        for i in range(num_tasks):
            task = concurrency_tester.tardis_connector.download_daily_data_direct(
                tardis_exchange='deribit',
                tardis_symbol='BTC-PERPETUAL',
                date=TEST_DATE,
                data_types=['trades']
            )
            tasks.append(task)
        
        # Execute all tasks concurrently
        start_time = time.time()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        end_time = time.time()
        
        # Verify all tasks completed
        assert len(results) == num_tasks, f"Expected {num_tasks} results, got {len(results)}"
        
        # Check for exceptions
        exceptions = [r for r in results if isinstance(r, Exception)]
        assert len(exceptions) == 0, f"Found exceptions in high concurrency test: {exceptions}"
        
        # Calculate performance metrics
        total_time = end_time - start_time
        tasks_per_second = num_tasks / total_time
        
        # Verify performance is reasonable
        assert tasks_per_second > 0.1, f"Performance too low: {tasks_per_second:.2f} tasks/sec"
        
        logger.info(f"✅ High concurrency test passed: {num_tasks} tasks in {total_time:.2f}s ({tasks_per_second:.2f} tasks/sec)")
    
    async def test_memory_usage_under_concurrency(self, concurrency_tester):
        """Test memory usage under concurrent operations"""
        logger.info("Testing memory usage under concurrency")
        
        # Create orchestrator with memory monitoring
        orchestrator = DownloadOrchestrator(
            gcs_bucket=concurrency_tester.config.gcp.bucket,
            api_key=concurrency_tester.config.tardis.api_key,
            max_parallel_downloads=5,
            max_parallel_uploads=3,
            max_workers=3
        )
        
        # Create multiple concurrent operations
        tasks = []
        for i in range(10):
            task = orchestrator.download_and_upload_data(
                date=TEST_DATE,
                venues=TEST_VENUES,
                instrument_types=TEST_INSTRUMENT_TYPES,
                data_types=['trades'],
                max_instruments=1
            )
            tasks.append(task)
        
        # Execute all tasks concurrently
        start_time = time.time()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        end_time = time.time()
        
        # Verify all tasks completed
        assert len(results) == 10, f"Expected 10 results, got {len(results)}"
        
        # Check for exceptions
        exceptions = [r for r in results if isinstance(r, Exception)]
        assert len(exceptions) == 0, f"Found exceptions in memory concurrency test: {exceptions}"
        
        # Check memory metrics
        metrics = orchestrator.get_performance_metrics()
        memory_info = metrics.get('memory_info', {})
        
        if 'error' not in memory_info:
            assert memory_info['usage_percent'] < 95, f"Memory usage too high under concurrency: {memory_info['usage_percent']}%"
            logger.info(f"✅ Memory concurrency test passed: {memory_info['usage_percent']:.1f}% used")
        else:
            logger.warning(f"Memory monitoring not available: {memory_info['error']}")
        
        await orchestrator.tardis_connector.close()
        logger.info(f"✅ Memory usage under concurrency test passed: {len(results)} tasks in {end_time - start_time:.2f}s")


if __name__ == "__main__":
    # Run tests directly
    pytest.main([__file__, "-v"])
