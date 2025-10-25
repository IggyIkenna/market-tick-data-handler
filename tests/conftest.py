"""
Shared pytest fixtures for the test suite
"""

import pytest
import pytest_asyncio
import tempfile
import os
from datetime import datetime, timezone
from unittest.mock import Mock, patch
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config import get_config
from market_data_tick_handler.models import InstrumentType, Venue, InstrumentKey, InstrumentDefinition


@pytest.fixture
def config():
    """Get test configuration"""
    return get_config()


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files"""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield tmp_dir


@pytest.fixture
def mock_gcs_client():
    """Mock GCS client for testing"""
    mock_client = Mock()
    mock_bucket = Mock()
    mock_client.bucket.return_value = mock_bucket
    return mock_client, mock_bucket


@pytest.fixture
def mock_tardis_connector():
    """Mock Tardis connector for testing"""
    mock_connector = Mock()
    mock_connector.download_daily_data_direct.return_value = {
        'trades': Mock(),
        'book_snapshot_5': Mock()
    }
    mock_connector._create_empty_dataframe_with_schema.return_value = Mock()
    return mock_connector


@pytest.fixture
def mock_download_orchestrator():
    """Mock download orchestrator for testing"""
    mock_orchestrator = Mock()
    mock_orchestrator.tardis_connector = Mock()
    mock_orchestrator.tardis_connector.download_daily_data_direct.return_value = {
        'trades': Mock(),
        'book_snapshot_5': Mock()
    }
    mock_orchestrator.tardis_connector._create_empty_dataframe_with_schema.return_value = Mock()
    return mock_orchestrator


@pytest.fixture
def mock_memory_monitor():
    """Mock memory monitor for testing"""
    mock_monitor = Mock()
    mock_monitor.threshold_percent = 80.0
    mock_monitor.is_memory_threshold_exceeded.return_value = False
    mock_monitor.get_memory_info.return_value = {
        'total_memory_gb': 8.0,
        'used_memory_gb': 4.0,
        'available_memory_gb': 4.0,
        'memory_usage_percent': 50.0
    }
    return mock_monitor


@pytest.fixture
def sample_instrument_key():
    """Sample instrument key for testing"""
    return InstrumentKey(
        venue=Venue.BINANCE,
        instrument_type=InstrumentType.SPOT_PAIR,
        symbol="BTC-USDT"
    )


@pytest.fixture
def sample_instrument_definition():
    """Sample instrument definition for testing"""
    return InstrumentDefinition(
        instrument_key="BINANCE:SPOT_PAIR:BTC-USDT",
        venue="BINANCE",
        instrument_type="SPOT_PAIR",
        available_from_datetime="2023-05-23T00:00:00+00:00",
        available_to_datetime="2024-05-23T00:00:00+00:00",
        data_types="trades,book_snapshot_5",
        base_asset="BTC",
        quote_asset="USDT",
        settle_asset="USDT",
        exchange_raw_symbol="BTCUSDT",
        tardis_symbol="BTC-USDT",
        tardis_exchange="binance",
        data_provider="tardis",
        venue_type="centralized",
        asset_class="crypto",
        inverse=False,
        tick_size="0.01",
        min_size="0.001",
        settlement_type="",
        underlying="",
        ccxt_symbol="BTC/USDT",
        ccxt_exchange="binance"
    )


@pytest.fixture
def sample_trade_data():
    """Sample trade data for testing"""
    return {
        'timestamp': 1684800000000000,  # 2023-05-23 00:00:00 UTC
        'local_timestamp': 1684800000000000,
        'id': 'test_trade_1',
        'price': 50000.0,
        'amount': 0.1,
        'side': 'buy'
    }


@pytest.fixture
def sample_liquidation_data():
    """Sample liquidation data for testing"""
    return {
        'timestamp': 1684800000000000,
        'local_timestamp': 1684800000000000,
        'id': 'test_liquidation_1',
        'side': 'buy',  # 'buy' = short liquidated
        'price': 50000.0,
        'amount': 0.1
    }


@pytest.fixture
def sample_derivative_ticker_data():
    """Sample derivative ticker data for testing"""
    return {
        'timestamp': 1684800000000000,
        'local_timestamp': 1684800000000000,
        'funding_rate': 0.0001,
        'predicted_funding_rate': 0.0002,
        'open_interest': 1000.0,
        'last_price': 50000.0,
        'index_price': 49950.0,
        'mark_price': 50000.0,
        'funding_timestamp': 1684800000000000
    }


@pytest.fixture
def test_date():
    """Test date for testing"""
    return datetime(2023, 5, 23, tzinfo=timezone.utc)


@pytest.fixture
def test_date_range():
    """Test date range for testing"""
    start = datetime(2023, 5, 23, tzinfo=timezone.utc)
    end = datetime(2023, 5, 25, tzinfo=timezone.utc)
    return start, end


@pytest.fixture(autouse=True)
def timeout_each_test():
    """Add timeout to each test to prevent hanging"""
    import signal
    import os
    
    def timeout_handler(signum, frame):
        raise TimeoutError("Test timed out after 30 seconds")
    
    # Only set timeout if not already set (avoid conflicts)
    if not hasattr(timeout_each_test, '_timeout_set'):
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(30)  # 30 second timeout
        timeout_each_test._timeout_set = True
    
    yield
    
    # Clean up timeout
    if hasattr(timeout_each_test, '_timeout_set'):
        signal.alarm(0)


@pytest.fixture(autouse=True)
def setup_test_env():
    """Set up test environment variables"""
    test_env = {
        'LOG_LEVEL': 'DEBUG',
        'LOG_DESTINATION': 'local',
        'GCS_BUCKET': 'test-bucket',
        'TARDIS_API_KEY': 'test_key',
        'GCP_PROJECT_ID': 'test-project',
        'TESTING_MODE': 'true'
    }
    
    with patch.dict(os.environ, test_env):
        yield


@pytest.fixture
def mock_http_responses():
    """Mock HTTP responses for testing"""
    with patch('aiohttp.ClientSession.get') as mock_get:
        # Mock successful CSV response
        mock_response = Mock()
        mock_response.status = 200
        mock_response.text.return_value = "timestamp,price,amount,side\n1684800000000000,50000.0,0.1,buy"
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = None
        mock_get.return_value = mock_response
        yield mock_get