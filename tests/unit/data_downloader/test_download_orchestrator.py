"""
Unit tests for download orchestrator
"""

import pytest
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime, timezone
from market_data_tick_handler.data_downloader.download_orchestrator import DownloadOrchestrator


class TestDownloadOrchestrator:
    """Test DownloadOrchestrator class"""
    
    def test_init(self):
        """Test orchestrator initialization"""
        orchestrator = DownloadOrchestrator(
            gcs_bucket="test-bucket",
            api_key="test-api-key",
            max_workers=4,
            max_parallel_downloads=2
        )
        
        assert orchestrator.gcs_bucket == "test-bucket"
        assert orchestrator.api_key == "test-api-key"
        assert orchestrator.max_workers == 4
        assert orchestrator.max_parallel_downloads == 2
        assert orchestrator.batch_size == 1000  # Default batch size
    
    def test_init_defaults(self):
        """Test orchestrator initialization with defaults"""
        orchestrator = DownloadOrchestrator(
            gcs_bucket="test-bucket",
            api_key="test-api-key"
        )
        
        assert orchestrator.max_workers == 2  # Default value
        assert orchestrator.max_parallel_downloads == 50  # Default value
        assert orchestrator.batch_size == 1000  # Default value
    
    def test_apply_sharding(self, mock_tardis_connector):
        """Test sharding logic"""
        orchestrator = DownloadOrchestrator(
            tardis_connector=mock_tardis_connector,
            gcs_bucket="test-bucket"
        )
        
        # Create test targets
        targets = [
            {'instrument_key': f'BINANCE:SPOT_PAIR:BTC-USDT-{i}'} 
            for i in range(10)
        ]
        
        # Test shard 0 of 3
        sharded = orchestrator._apply_sharding(targets, 0, 3)
        assert len(sharded) == 4  # 10/3 = 3.33, so shard 0 gets 4 items
        
        # Test shard 1 of 3
        sharded = orchestrator._apply_sharding(targets, 1, 3)
        assert len(sharded) == 3  # Shard 1 gets 3 items
        
        # Test shard 2 of 3
        sharded = orchestrator._apply_sharding(targets, 2, 3)
        assert len(sharded) == 3  # Shard 2 gets 3 items
        
        # Verify all items are accounted for
        all_sharded = []
        for i in range(3):
            all_sharded.extend(orchestrator._apply_sharding(targets, i, 3))
        assert len(all_sharded) == 10
    
    def test_apply_sharding_single_shard(self, mock_tardis_connector):
        """Test sharding with single shard"""
        orchestrator = DownloadOrchestrator(
            tardis_connector=mock_tardis_connector,
            gcs_bucket="test-bucket"
        )
        
        targets = [
            {'instrument_key': f'BINANCE:SPOT_PAIR:BTC-USDT-{i}'} 
            for i in range(5)
        ]
        
        sharded = orchestrator._apply_sharding(targets, 0, 1)
        assert len(sharded) == 5
        assert sharded == targets
    
    def test_apply_sharding_empty_targets(self, mock_tardis_connector):
        """Test sharding with empty targets"""
        orchestrator = DownloadOrchestrator(
            tardis_connector=mock_tardis_connector,
            gcs_bucket="test-bucket"
        )
        
        sharded = orchestrator._apply_sharding([], 0, 3)
        assert len(sharded) == 0
    
    @patch('src.data_downloader.download_orchestrator.InstrumentReader')
    def test_get_download_targets_from_missing_data(self, mock_reader_class, mock_tardis_connector):
        """Test getting download targets from missing data"""
        orchestrator = DownloadOrchestrator(
            tardis_connector=mock_tardis_connector,
            gcs_bucket="test-bucket"
        )
        
        # Mock instrument reader
        mock_reader = Mock()
        mock_reader_class.return_value = mock_reader
        mock_reader.get_instruments_for_date.return_value = Mock()
        mock_reader.get_instruments_for_date.return_value.empty = False
        mock_reader.get_instruments_for_date.return_value.set_index.return_value.to_dict.return_value = {
            'BINANCE:SPOT_PAIR:BTC-USDT': {
                'tardis_exchange': 'binance',
                'tardis_symbol': 'BTC-USDT',
                'data_types': 'trades,book_snapshot_5'
            }
        }
        
        # Mock missing data DataFrame
        import pandas as pd
        missing_df = pd.DataFrame({
            'date': ['2023-05-23'],
            'instrument_key': ['BINANCE:SPOT_PAIR:BTC-USDT'],
            'data_type': ['trades']
        })
        
        targets = orchestrator._get_download_targets_from_missing_data(missing_df)
        
        assert len(targets) == 1
        assert targets[0]['instrument_key'] == 'BINANCE:SPOT_PAIR:BTC-USDT'
        assert targets[0]['tardis_exchange'] == 'binance'
        assert targets[0]['tardis_symbol'] == 'BTC-USDT'
        assert targets[0]['data_types'] == 'trades,book_snapshot_5'
    
    @patch('src.data_downloader.download_orchestrator.InstrumentReader')
    def test_get_download_targets_from_missing_data_empty(self, mock_reader_class, mock_tardis_connector):
        """Test getting download targets from empty missing data"""
        orchestrator = DownloadOrchestrator(
            tardis_connector=mock_tardis_connector,
            gcs_bucket="test-bucket"
        )
        
        # Mock empty missing data DataFrame
        import pandas as pd
        missing_df = pd.DataFrame()
        
        targets = orchestrator._get_download_targets_from_missing_data(missing_df)
        
        assert len(targets) == 0
    
    @patch('src.data_downloader.download_orchestrator.InstrumentReader')
    @pytest.mark.asyncio
    async def test_process_batch_parallel_success(self, mock_reader_class, mock_tardis_connector):
        """Test successful batch processing"""
        orchestrator = DownloadOrchestrator(
            tardis_connector=mock_tardis_connector,
            gcs_bucket="test-bucket",
            max_workers=2
        )
        
        # Mock successful download
        mock_tardis_connector.download_daily_data_direct.return_value = {
            'trades': Mock()
        }
        
        # Mock successful upload
        with patch.object(orchestrator, '_upload_batch') as mock_upload:
            mock_upload.return_value = {'uploaded_files': 1, 'errors': []}
            
            batch_targets = [
                {
                    'instrument_key': 'BINANCE:SPOT_PAIR:BTC-USDT',
                    'tardis_exchange': 'binance',
                    'tardis_symbol': 'BTC-USDT',
                    'data_types': 'trades,book_snapshot_5'
                }
            ]
            
            date = datetime(2023, 5, 23, tzinfo=timezone.utc)
            result = await orchestrator._process_batch_parallel(batch_targets, date, ['trades'])
            
            assert result['processed'] == 1
            assert result['failed'] == 0
            assert len(result['uploaded_files']) == 1
            assert result['errors'] == []
    
    @patch('src.data_downloader.download_orchestrator.InstrumentReader')
    @pytest.mark.asyncio
    async def test_process_batch_parallel_no_valid_data_types(self, mock_reader_class, mock_tardis_connector):
        """Test batch processing with no valid data types"""
        orchestrator = DownloadOrchestrator(
            tardis_connector=mock_tardis_connector,
            gcs_bucket="test-bucket",
            max_workers=2
        )
        
        batch_targets = [
            {
                'instrument_key': 'BINANCE:SPOT_PAIR:BTC-USDT',
                'tardis_exchange': 'binance',
                'tardis_symbol': 'BTC-USDT',
                'data_types': 'trades,book_snapshot_5'  # No derivative_ticker
            }
        ]
        
        date = datetime(2023, 5, 23, tzinfo=timezone.utc)
        result = await orchestrator._process_batch_parallel(batch_targets, date, ['derivative_ticker'])
        
        assert result['processed'] == 0
        assert result['failed'] == 1
        assert len(result['uploaded_files']) == 0
        assert len(result['errors']) == 1
        assert 'No valid data types' in result['errors'][0]
    
    @patch('src.data_downloader.download_orchestrator.InstrumentReader')
    @pytest.mark.asyncio
    async def test_process_batch_parallel_download_error(self, mock_reader_class, mock_tardis_connector):
        """Test batch processing with download error"""
        orchestrator = DownloadOrchestrator(
            tardis_connector=mock_tardis_connector,
            gcs_bucket="test-bucket",
            max_workers=2
        )
        
        # Mock download error
        mock_tardis_connector.download_daily_data_direct.side_effect = Exception("Download failed")
        
        batch_targets = [
            {
                'instrument_key': 'BINANCE:SPOT_PAIR:BTC-USDT',
                'tardis_exchange': 'binance',
                'tardis_symbol': 'BTC-USDT',
                'data_types': 'trades,book_snapshot_5'
            }
        ]
        
        date = datetime(2023, 5, 23, tzinfo=timezone.utc)
        result = await orchestrator._process_batch_parallel(batch_targets, date, ['trades'])
        
        assert result['processed'] == 0
        assert result['failed'] == 1
        assert len(result['uploaded_files']) == 0
        assert len(result['errors']) == 1
        assert 'Download failed' in result['errors'][0]
    
    @patch('src.data_downloader.download_orchestrator.InstrumentReader')
    @pytest.mark.asyncio
    async def test_download_and_upload_data_no_targets(self, mock_reader_class, mock_tardis_connector):
        """Test download with no targets"""
        orchestrator = DownloadOrchestrator(
            tardis_connector=mock_tardis_connector,
            gcs_bucket="test-bucket"
        )
        
        # Mock empty targets
        mock_reader = Mock()
        mock_reader_class.return_value = mock_reader
        mock_reader.get_download_targets.return_value = []
        
        date = datetime(2023, 5, 23, tzinfo=timezone.utc)
        result = await orchestrator.download_and_upload_data(date)
        
        assert result['status'] == 'no_targets'
        assert result['processed'] == 0
    
    @patch('src.data_downloader.download_orchestrator.InstrumentReader')
    @pytest.mark.asyncio
    async def test_download_and_upload_data_with_sharding(self, mock_reader_class, mock_tardis_connector):
        """Test download with sharding"""
        orchestrator = DownloadOrchestrator(
            tardis_connector=mock_tardis_connector,
            gcs_bucket="test-bucket",
            max_workers=2,
            batch_size=1
        )
        
        # Mock targets
        mock_reader = Mock()
        mock_reader_class.return_value = mock_reader
        mock_reader.get_download_targets.return_value = [
            {'instrument_key': f'BINANCE:SPOT_PAIR:BTC-USDT-{i}'} 
            for i in range(5)
        ]
        
        # Mock successful processing
        with patch.object(orchestrator, '_process_batch_parallel') as mock_process:
            mock_process.return_value = {
                'processed': 1,
                'failed': 0,
                'uploaded_files': [f'file_{i}'],
                'errors': []
            }
            
            date = datetime(2023, 5, 23, tzinfo=timezone.utc)
            result = await orchestrator.download_and_upload_data(
                date, 
                shard_index=0, 
                total_shards=2
            )
            
            assert result['status'] == 'success'
            assert result['processed'] == 5  # 5 batches * 1 processed each
            assert result['failed'] == 0
            assert len(result['uploaded_files']) == 5
