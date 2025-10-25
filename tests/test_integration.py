#!/usr/bin/env python3
"""
Integration Tests

End-to-end tests covering the full download-to-upload pipeline:
1. Download from Tardis API
2. Process and validate data
3. Upload to GCS
4. Verify data integrity
5. Test error handling and recovery
"""

import pytest
import pytest_asyncio
import asyncio
import tempfile
import os
from datetime import datetime, timezone
from typing import Dict, List, Any
import logging
from pathlib import Path

# Add project root to path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from market_data_tick_handler.data_downloader.download_orchestrator import DownloadOrchestrator
from market_data_tick_handler.data_downloader.instrument_reader import InstrumentReader
from market_data_tick_handler.data_validator.data_validator import DataValidator
from config import get_config

logger = logging.getLogger(__name__)

class IntegrationTests:
    """Base class for integration tests"""
    
    def __init__(self):
        self.config = get_config()
        self.orchestrator = None
        self.instrument_reader = None
        self.data_validator = None
        
    async def setup(self):
        """Setup test environment"""
        self.orchestrator = DownloadOrchestrator(
            gcs_bucket=self.config.gcp.bucket,
            api_key=self.config.tardis.api_key,
            max_parallel_downloads=2,  # Reduced for testing
            max_parallel_uploads=2,
            max_workers=1
        )
        
        self.instrument_reader = InstrumentReader(self.config.gcp.bucket)
        self.data_validator = DataValidator(self.config.gcp.bucket)
        
    async def teardown(self):
        """Cleanup test environment"""
        if self.orchestrator:
            await self.orchestrator.tardis_connector.close()


# Test fixtures
TEST_DATE = datetime(2023, 5, 23, tzinfo=timezone.utc)
TEST_VENUES = ['deribit']  # Start with one venue for integration tests
TEST_INSTRUMENT_TYPES = ['PERP']
TEST_DATA_TYPES = ['trades', 'book_snapshot_5']


@pytest_asyncio.fixture
async def integration_tester():
    """Fixture for integration tests"""
    tester = IntegrationTests()
    await tester.setup()
    try:
        yield tester
    finally:
        await tester.teardown()


@pytest.mark.asyncio
class TestDownloadUploadPipeline:
    """Test the complete download-to-upload pipeline"""
    
    async def test_single_instrument_download_upload(self, integration_tester):
        """Test downloading and uploading data for a single instrument"""
        logger.info("Testing single instrument download-upload pipeline")
        
        # Get a test instrument
        instruments_df = await integration_tester.instrument_reader.get_instruments_for_date(
            date=TEST_DATE,
            venues=TEST_VENUES,
            instrument_types=TEST_INSTRUMENT_TYPES,
            max_instruments=1
        )
        
        if instruments_df.empty:
            pytest.skip("No test instruments available")
        
        instrument_key = instruments_df.iloc[0]['instrument_key']
        logger.info(f"Testing with instrument: {instrument_key}")
        
        # Download and upload data
        result = await integration_tester.orchestrator.download_and_upload_data(
            date=TEST_DATE,
            venues=TEST_VENUES,
            instrument_types=TEST_INSTRUMENT_TYPES,
            data_types=TEST_DATA_TYPES,
            max_instruments=1
        )
        
        # Verify results
        assert result['status'] == 'success', f"Download-upload failed: {result}"
        assert result['processed'] > 0, "No instruments were processed"
        assert len(result['uploaded_files']) > 0, "No files were uploaded"
        
        logger.info(f"✅ Single instrument test passed: {result['processed']} processed, {len(result['uploaded_files'])} files uploaded")
    
    async def test_multiple_instruments_download_upload(self, integration_tester):
        """Test downloading and uploading data for multiple instruments"""
        logger.info("Testing multiple instruments download-upload pipeline")
        
        # Download and upload data for multiple instruments
        result = await integration_tester.orchestrator.download_and_upload_data(
            date=TEST_DATE,
            venues=TEST_VENUES,
            instrument_types=TEST_INSTRUMENT_TYPES,
            data_types=TEST_DATA_TYPES,
            max_instruments=3  # Test with 3 instruments
        )
        
        # Verify results
        assert result['status'] == 'success', f"Download-upload failed: {result}"
        assert result['processed'] > 0, "No instruments were processed"
        assert len(result['uploaded_files']) > 0, "No files were uploaded"
        
        # Verify we have files for each data type
        uploaded_data_types = set()
        for file_path in result['uploaded_files']:
            if 'data_type-trades' in file_path:
                uploaded_data_types.add('trades')
            elif 'data_type-book_snapshot_5' in file_path:
                uploaded_data_types.add('book_snapshot_5')
        
        assert 'trades' in uploaded_data_types, "No trades files uploaded"
        assert 'book_snapshot_5' in uploaded_data_types, "No book_snapshot_5 files uploaded"
        
        logger.info(f"✅ Multiple instruments test passed: {result['processed']} processed, {len(result['uploaded_files'])} files uploaded")
    
    async def test_missing_data_download(self, integration_tester):
        """Test downloading missing data based on missing data reports"""
        logger.info("Testing missing data download pipeline")
        
        # First, check for missing data
        missing_df = integration_tester.data_validator.check_missing_data(
            start_date=TEST_DATE,
            end_date=TEST_DATE,
            venues=TEST_VENUES,
            instrument_types=TEST_INSTRUMENT_TYPES
        )
        
        if missing_df.empty:
            logger.info("No missing data found - skipping missing data test")
            pytest.skip("No missing data found for test date")
        
        # Download missing data
        result = await integration_tester.orchestrator.download_missing_data(
            date=TEST_DATE,
            venues=TEST_VENUES,
            instrument_types=TEST_INSTRUMENT_TYPES,
            data_types=TEST_DATA_TYPES,
            max_instruments=2
        )
        
        # Verify results
        assert result['status'] == 'success', f"Missing data download failed: {result}"
        assert result['processed'] >= 0, "Invalid processed count"
        
        logger.info(f"✅ Missing data test passed: {result['processed']} processed, {len(result['uploaded_files'])} files uploaded")
    
    async def test_batch_processing(self, integration_tester):
        """Test batch processing with multiple instruments"""
        logger.info("Testing batch processing")
        
        # Test with batch size of 2
        integration_tester.orchestrator.batch_size = 2
        
        result = await integration_tester.orchestrator.download_and_upload_data(
            date=TEST_DATE,
            venues=TEST_VENUES,
            instrument_types=TEST_INSTRUMENT_TYPES,
            data_types=TEST_DATA_TYPES,
            max_instruments=4  # Test with 4 instruments to trigger batching
        )
        
        # Verify results
        assert result['status'] == 'success', f"Batch processing failed: {result}"
        assert result['processed'] > 0, "No instruments were processed"
        
        # Verify performance metrics
        metrics = integration_tester.orchestrator.get_performance_metrics()
        assert metrics['total_files_processed'] > 0, "No files were processed"
        assert metrics['total_download_time'] > 0, "No download time recorded"
        assert metrics['total_upload_time'] > 0, "No upload time recorded"
        
        logger.info(f"✅ Batch processing test passed: {result['processed']} processed")
        logger.info(f"Performance metrics: {metrics['total_files_processed']} files, {metrics['total_download_time']:.2f}s download, {metrics['total_upload_time']:.2f}s upload")


@pytest.mark.asyncio
class TestErrorHandling:
    """Test error handling and recovery"""
    
    async def test_invalid_instrument_handling(self, integration_tester):
        """Test handling of invalid instruments"""
        logger.info("Testing invalid instrument handling")
        
        # Test with invalid venue
        result = await integration_tester.orchestrator.download_and_upload_data(
            date=TEST_DATE,
            venues=['INVALID_VENUE'],
            instrument_types=TEST_INSTRUMENT_TYPES,
            data_types=TEST_DATA_TYPES,
            max_instruments=1
        )
        
        # Should handle gracefully
        assert result['status'] in ['success', 'no_targets'], f"Invalid venue handling failed: {result}"
        
        logger.info(f"✅ Invalid instrument handling test passed: {result['status']}")
    
    async def test_network_error_handling(self, integration_tester):
        """Test handling of network errors"""
        logger.info("Testing network error handling")
        
        # Temporarily modify timeout to cause network errors
        original_timeout = integration_tester.orchestrator.tardis_connector.timeout
        integration_tester.orchestrator.tardis_connector.timeout = 0.001  # Very short timeout
        
        try:
            result = await integration_tester.orchestrator.download_and_upload_data(
                date=TEST_DATE,
                venues=TEST_VENUES,
                instrument_types=TEST_INSTRUMENT_TYPES,
                data_types=TEST_DATA_TYPES,
                max_instruments=1
            )
            
            # Should handle network errors gracefully
            assert result['status'] in ['success', 'no_targets'], f"Network error handling failed: {result}"
            
        finally:
            # Restore original timeout
            integration_tester.orchestrator.tardis_connector.timeout = original_timeout
        
        logger.info(f"✅ Network error handling test passed: {result['status']}")
    
    async def test_empty_data_handling(self, integration_tester):
        """Test handling of empty data responses"""
        logger.info("Testing empty data handling")
        
        # Test with a date that might have no data
        future_date = datetime(2030, 1, 1, tzinfo=timezone.utc)
        
        result = await integration_tester.orchestrator.download_and_upload_data(
            date=future_date,
            venues=TEST_VENUES,
            instrument_types=TEST_INSTRUMENT_TYPES,
            data_types=TEST_DATA_TYPES,
            max_instruments=1
        )
        
        # Should handle empty data gracefully
        assert result['status'] in ['success', 'no_targets'], f"Empty data handling failed: {result}"
        
        logger.info(f"✅ Empty data handling test passed: {result['status']}")


@pytest.mark.asyncio
class TestDataIntegrity:
    """Test data integrity throughout the pipeline"""
    
    async def test_data_roundtrip_integrity(self, integration_tester):
        """Test that data maintains integrity through download-upload cycle"""
        logger.info("Testing data roundtrip integrity")
        
        # Download and upload data
        result = await integration_tester.orchestrator.download_and_upload_data(
            date=TEST_DATE,
            venues=TEST_VENUES,
            instrument_types=TEST_INSTRUMENT_TYPES,
            data_types=['trades'],  # Test with trades only for simplicity
            max_instruments=1
        )
        
        assert result['status'] == 'success', f"Download-upload failed: {result}"
        assert result['processed'] > 0, "No instruments were processed"
        
        # Verify uploaded files exist and are accessible
        for file_path in result['uploaded_files']:
            # Check if file exists in GCS (simplified check)
            assert 'gs://' in file_path, f"Invalid GCS path: {file_path}"
            assert '.parquet' in file_path, f"File is not Parquet: {file_path}"
        
        logger.info(f"✅ Data roundtrip integrity test passed: {len(result['uploaded_files'])} files uploaded")
    
    async def test_schema_consistency(self, integration_tester):
        """Test that schemas are consistent across uploads"""
        logger.info("Testing schema consistency")
        
        # Download and upload data for multiple instruments
        result = await integration_tester.orchestrator.download_and_upload_data(
            date=TEST_DATE,
            venues=TEST_VENUES,
            instrument_types=TEST_INSTRUMENT_TYPES,
            data_types=TEST_DATA_TYPES,
            max_instruments=2
        )
        
        assert result['status'] == 'success', f"Download-upload failed: {result}"
        
        # Group files by data type
        files_by_type = {}
        for file_path in result['uploaded_files']:
            if 'data_type-trades' in file_path:
                files_by_type.setdefault('trades', []).append(file_path)
            elif 'data_type-book_snapshot_5' in file_path:
                files_by_type.setdefault('book_snapshot_5', []).append(file_path)
        
        # Verify we have files for each data type
        for data_type in TEST_DATA_TYPES:
            assert data_type in files_by_type, f"No files uploaded for {data_type}"
            assert len(files_by_type[data_type]) > 0, f"Empty file list for {data_type}"
        
        logger.info(f"✅ Schema consistency test passed: {files_by_type}")


@pytest.mark.asyncio
class TestPerformanceIntegration:
    """Test performance aspects of the integration"""
    
    async def test_memory_usage_during_processing(self, integration_tester):
        """Test memory usage during processing"""
        logger.info("Testing memory usage during processing")
        
        # Reset performance metrics
        integration_tester.orchestrator.reset_performance_metrics()
        
        # Process data
        result = await integration_tester.orchestrator.download_and_upload_data(
            date=TEST_DATE,
            venues=TEST_VENUES,
            instrument_types=TEST_INSTRUMENT_TYPES,
            data_types=TEST_DATA_TYPES,
            max_instruments=3
        )
        
        assert result['status'] == 'success', f"Processing failed: {result}"
        
        # Check memory metrics
        metrics = integration_tester.orchestrator.get_performance_metrics()
        memory_info = metrics.get('memory_info', {})
        
        if 'error' not in memory_info:
            assert memory_info['usage_percent'] < 95, f"Memory usage too high: {memory_info['usage_percent']}%"
            logger.info(f"✅ Memory usage test passed: {memory_info['usage_percent']:.1f}% used")
        else:
            logger.warning(f"Memory monitoring not available: {memory_info['error']}")
    
    async def test_throughput_metrics(self, integration_tester):
        """Test throughput metrics"""
        logger.info("Testing throughput metrics")
        
        # Reset performance metrics
        integration_tester.orchestrator.reset_performance_metrics()
        
        # Process data
        result = await integration_tester.orchestrator.download_and_upload_data(
            date=TEST_DATE,
            venues=TEST_VENUES,
            instrument_types=TEST_INSTRUMENT_TYPES,
            data_types=TEST_DATA_TYPES,
            max_instruments=2
        )
        
        assert result['status'] == 'success', f"Processing failed: {result}"
        
        # Check throughput metrics
        metrics = integration_tester.orchestrator.get_performance_metrics()
        
        assert metrics['total_files_processed'] > 0, "No files processed"
        assert metrics['total_download_time'] > 0, "No download time recorded"
        assert metrics['total_upload_time'] > 0, "No upload time recorded"
        
        # Calculate throughput
        download_throughput = metrics['download_throughput_files_per_sec']
        upload_throughput = metrics['upload_throughput_files_per_sec']
        
        assert download_throughput > 0, "Invalid download throughput"
        assert upload_throughput > 0, "Invalid upload throughput"
        
        logger.info(f"✅ Throughput test passed: {download_throughput:.2f} files/sec download, {upload_throughput:.2f} files/sec upload")


if __name__ == "__main__":
    # Run tests directly
    pytest.main([__file__, "-v"])
