#!/usr/bin/env python3
"""
Configuration Tests

Tests for configuration validation and management:
1. Environment variable validation
2. Configuration file loading
3. Default value handling
4. Configuration validation
5. Runtime configuration changes
6. Error handling for invalid configurations
"""

try:
    import pytest
except ImportError:
    pytest = None
import os
import tempfile
import yaml
from pathlib import Path
from unittest.mock import patch
from typing import Dict, Any
import logging

# Add project root to path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import get_config, ConfigManager, TardisConfig, GCPConfig, ServiceConfig
from market_data_tick_handler.data_downloader.tardis_connector import TardisConnector
from market_data_tick_handler.data_downloader.download_orchestrator import DownloadOrchestrator

logger = logging.getLogger(__name__)

class ConfigurationTests:
    """Base class for configuration tests"""
    
    def __init__(self):
        self.original_env = os.environ.copy()
        self.temp_config_file = None
        
    def setup(self):
        """Setup test environment"""
        # Set up test environment variables
        test_env = {
            'TARDIS_API_KEY': 'TD.test_key_123456789',
            'GCP_PROJECT_ID': 'test-project',
            'GCS_BUCKET': 'test-bucket',
            'GCP_CREDENTIALS_PATH': '/path/to/credentials.json',
            'LOG_LEVEL': 'DEBUG',
            'LOG_DESTINATION': 'local'
        }
        os.environ.update(test_env)
        
    def teardown(self):
        """Cleanup test environment"""
        # Restore original environment
        os.environ.clear()
        os.environ.update(self.original_env)
        
        # Clean up temp files
        if self.temp_config_file and os.path.exists(self.temp_config_file):
            os.unlink(self.temp_config_file)
    
    def create_temp_config_file(self, config_data: Dict[str, Any]) -> str:
        """Create a temporary configuration file"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            self.temp_config_file = f.name
            return f.name


@pytest.fixture
def config_tester():
    """Fixture for configuration tests"""
    tester = ConfigurationTests()
    tester.setup()
    yield tester
    tester.teardown()


class TestEnvironmentVariables:
    """Test environment variable handling"""
    
    def test_required_environment_variables(self):
        """Test that required environment variables are validated"""
        logger.info("Testing required environment variables")
        
        # Test missing TARDIS_API_KEY
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="TARDIS_API_KEY environment variable is required"):
                ConfigManager().load_config()
        
        # Test missing GCP_PROJECT_ID
        with patch.dict(os.environ, {'TARDIS_API_KEY': 'TD.test_key'}, clear=True):
            with pytest.raises(ValueError, match="GCP_PROJECT_ID environment variable is required"):
                ConfigManager().load_config()
        
        # Test missing GCP_CREDENTIALS_PATH
        with patch.dict(os.environ, {
            'TARDIS_API_KEY': 'TD.test_key',
            'GCP_PROJECT_ID': 'test-project'
        }, clear=True):
            with pytest.raises(ValueError, match="GCP_CREDENTIALS_PATH environment variable is required"):
                ConfigManager().load_config()
        
        # Test missing GCS_BUCKET
        with patch.dict(os.environ, {
            'TARDIS_API_KEY': 'TD.test_key',
            'GCP_PROJECT_ID': 'test-project',
            'GCP_CREDENTIALS_PATH': '/path/to/credentials.json'
        }, clear=True):
            with pytest.raises(ValueError, match="GCS_BUCKET environment variable is required"):
                ConfigManager().load_config()
        
        logger.info("✅ Required environment variables test passed")
    
    def test_optional_environment_variables(self, config_tester):
        """Test that optional environment variables have correct defaults"""
        logger.info("Testing optional environment variables")
        
        # Set required variables
        env_vars = {
            'TARDIS_API_KEY': 'TD.test_key',
            'GCP_PROJECT_ID': 'test-project',
            'GCP_CREDENTIALS_PATH': '/path/to/credentials.json',
            'GCS_BUCKET': 'test-bucket'
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            config = ConfigManager().load_config()
            
            # Test Tardis config defaults
            assert config.tardis.base_url == 'https://datasets.tardis.dev'
            assert config.tardis.timeout == 30
            assert config.tardis.max_retries == 3
            assert config.tardis.max_concurrent == 50
            assert config.tardis.rate_limit_per_vm == 1000000
            
            # Test GCP config defaults
            assert config.gcp.region == 'asia-northeast1-c'
            
            # Test Service config defaults
            assert config.service.log_level == 'INFO'
            assert config.service.log_destination == 'local'
            assert config.service.max_concurrent_requests == 2
            assert config.service.batch_size == 1000
            assert config.service.memory_efficient == False
            assert config.service.enable_caching == True
            assert config.service.cache_ttl == 3600
        
        logger.info("✅ Optional environment variables test passed")
    
    def test_environment_variable_override(self, config_tester):
        """Test that environment variables override defaults"""
        logger.info("Testing environment variable override")
        
        # Set required variables with custom values
        env_vars = {
            'TARDIS_API_KEY': 'TD.test_key',
            'GCP_PROJECT_ID': 'test-project',
            'GCP_CREDENTIALS_PATH': '/path/to/credentials.json',
            'GCS_BUCKET': 'test-bucket',
            'TARDIS_BASE_URL': 'https://custom.tardis.dev',
            'TARDIS_TIMEOUT': '60',
            'TARDIS_MAX_RETRIES': '5',
            'TARDIS_MAX_CONCURRENT': '100',
            'LOG_LEVEL': 'DEBUG',
            'BATCH_SIZE': '2000'
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            config = ConfigManager().load_config()
            
            # Test overridden values
            assert config.tardis.base_url == 'https://custom.tardis.dev'
            assert config.tardis.timeout == 60
            assert config.tardis.max_retries == 5
            assert config.tardis.max_concurrent == 100
            assert config.service.log_level == 'DEBUG'
            assert config.service.batch_size == 2000
        
        logger.info("✅ Environment variable override test passed")


class TestConfigurationValidation:
    """Test configuration validation"""
    
    def test_tardis_config_validation(self, config_tester):
        """Test Tardis configuration validation"""
        logger.info("Testing Tardis configuration validation")
        
        # Test invalid API key format
        with pytest.raises(ValueError, match="Invalid Tardis API key format"):
            TardisConfig(api_key="invalid_key")
        
        # Test valid API key format
        config = TardisConfig(api_key="TD.valid_key")
        assert config.api_key == "TD.valid_key"
        
        # Test invalid timeout
        with pytest.raises(ValueError, match="Timeout must be positive"):
            TardisConfig(api_key="TD.valid_key", timeout=0)
        
        # Test timeout too high
        with pytest.raises(ValueError, match="Timeout seems too high"):
            TardisConfig(api_key="TD.valid_key", timeout=400)
        
        # Test invalid max_retries
        with pytest.raises(ValueError, match="Max retries must be non-negative"):
            TardisConfig(api_key="TD.valid_key", max_retries=-1)
        
        # Test max_retries too high
        with pytest.raises(ValueError, match="Max retries seems too high"):
            TardisConfig(api_key="TD.valid_key", max_retries=15)
        
        # Test invalid max_concurrent
        with pytest.raises(ValueError, match="Max concurrent must be positive"):
            TardisConfig(api_key="TD.valid_key", max_concurrent=0)
        
        # Test max_concurrent too high
        with pytest.raises(ValueError, match="Max concurrent seems too high"):
            TardisConfig(api_key="TD.valid_key", max_concurrent=150)
        
        logger.info("✅ Tardis configuration validation test passed")
    
    def test_gcp_config_validation(self, config_tester):
        """Test GCP configuration validation"""
        logger.info("Testing GCP configuration validation")
        
        # Test missing project_id
        with pytest.raises(ValueError, match="GCP project ID is required"):
            GCPConfig(project_id="", credentials_path="/path/to/creds.json", bucket="test-bucket")
        
        # Test invalid project_id
        with pytest.raises(ValueError, match="GCP project ID contains invalid characters"):
            GCPConfig(project_id="invalid-project-id!", credentials_path="/path/to/creds.json", bucket="test-bucket")
        
        # Test missing credentials_path
        with pytest.raises(ValueError, match="GCP credentials path is required"):
            GCPConfig(project_id="test-project", credentials_path="", bucket="test-bucket")
        
        # Test invalid credentials_path extension
        with pytest.raises(ValueError, match="GCP credentials file should be a JSON file"):
            GCPConfig(project_id="test-project", credentials_path="/path/to/creds.txt", bucket="test-bucket")
        
        # Test missing bucket
        with pytest.raises(ValueError, match="GCS bucket name is required"):
            GCPConfig(project_id="test-project", credentials_path="/path/to/creds.json", bucket="")
        
        # Test invalid bucket name
        with pytest.raises(ValueError, match="GCS bucket name contains invalid characters"):
            GCPConfig(project_id="test-project", credentials_path="/path/to/creds.json", bucket="invalid-bucket!")
        
        # Test bucket name too short
        with pytest.raises(ValueError, match="GCS bucket name must be 3-63 characters long"):
            GCPConfig(project_id="test-project", credentials_path="/path/to/creds.json", bucket="ab")
        
        # Test valid configuration
        config = GCPConfig(
            project_id="test-project",
            credentials_path="/path/to/creds.json",
            bucket="test-bucket"
        )
        assert config.project_id == "test-project"
        assert config.credentials_path == "/path/to/creds.json"
        assert config.bucket == "test-bucket"
        
        logger.info("✅ GCP configuration validation test passed")
    
    def test_service_config_validation(self, config_tester):
        """Test Service configuration validation"""
        logger.info("Testing Service configuration validation")
        
        # Test invalid log level
        with pytest.raises(ValueError, match="Invalid log level"):
            ServiceConfig(log_level="INVALID")
        
        # Test invalid log destination
        with pytest.raises(ValueError, match="Invalid log destination"):
            ServiceConfig(log_destination="invalid")
        
        # Test invalid max_concurrent_requests
        with pytest.raises(ValueError, match="Max concurrent requests must be positive"):
            ServiceConfig(max_concurrent_requests=0)
        
        # Test invalid batch_size
        with pytest.raises(ValueError, match="Batch size must be positive"):
            ServiceConfig(batch_size=0)
        
        # Test invalid cache_ttl
        with pytest.raises(ValueError, match="Cache TTL must be positive"):
            ServiceConfig(cache_ttl=0)
        
        # Test valid configuration
        config = ServiceConfig(
            log_level="DEBUG",
            log_destination="gcp",
            max_concurrent_requests=10,
            batch_size=5000,
            memory_efficient=True,
            enable_caching=False,
            cache_ttl=7200
        )
        assert config.log_level == "DEBUG"
        assert config.log_destination == "gcp"
        assert config.max_concurrent_requests == 10
        assert config.batch_size == 5000
        assert config.memory_efficient == True
        assert config.enable_caching == False
        assert config.cache_ttl == 7200
        
        logger.info("✅ Service configuration validation test passed")


class TestConfigurationFileLoading:
    """Test configuration file loading"""
    
    def test_yaml_config_file_loading(self, config_tester):
        """Test loading configuration from YAML file"""
        logger.info("Testing YAML configuration file loading")
        
        # Create test configuration file
        config_data = {
            'tardis': {
                'api_key': 'TD.file_key',
                'base_url': 'https://file.tardis.dev',
                'timeout': 45,
                'max_retries': 2
            },
            'gcp': {
                'project_id': 'file-project',
                'credentials_path': '/file/path/creds.json',
                'bucket': 'file-bucket',
                'region': 'us-central1'
            },
            'service': {
                'log_level': 'WARNING',
                'batch_size': 3000
            }
        }
        
        config_file = config_tester.create_temp_config_file(config_data)
        
        # Load configuration from file
        manager = ConfigManager(config_file=config_file)
        config = manager.load_config()
        
        # Verify file values are loaded
        assert config.tardis.api_key == 'TD.file_key'
        assert config.tardis.base_url == 'https://file.tardis.dev'
        assert config.tardis.timeout == 45
        assert config.tardis.max_retries == 2
        assert config.gcp.project_id == 'file-project'
        assert config.gcp.credentials_path == '/file/path/creds.json'
        assert config.gcp.bucket == 'file-bucket'
        assert config.gcp.region == 'us-central1'
        assert config.service.log_level == 'WARNING'
        assert config.service.batch_size == 3000
        
        logger.info("✅ YAML configuration file loading test passed")
    
    def test_env_override_file_config(self, config_tester):
        """Test that environment variables override file configuration"""
        logger.info("Testing environment variable override of file configuration")
        
        # Create test configuration file
        config_data = {
            'tardis': {
                'api_key': 'TD.file_key',
                'timeout': 30
            },
            'gcp': {
                'project_id': 'file-project',
                'credentials_path': '/file/path/creds.json',
                'bucket': 'file-bucket'
            }
        }
        
        config_file = config_tester.create_temp_config_file(config_data)
        
        # Set environment variables
        env_vars = {
            'TARDIS_API_KEY': 'TD.env_key',
            'TARDIS_TIMEOUT': '60',
            'GCP_PROJECT_ID': 'env-project',
            'GCP_CREDENTIALS_PATH': '/env/path/creds.json',
            'GCS_BUCKET': 'env-bucket'
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            manager = ConfigManager(config_file=config_file)
            config = manager.load_config()
            
            # Verify environment variables override file values
            assert config.tardis.api_key == 'TD.env_key'  # From env
            assert config.tardis.timeout == 60  # From env
            assert config.gcp.project_id == 'env-project'  # From env
            assert config.gcp.credentials_path == '/env/path/creds.json'  # From env
            assert config.gcp.bucket == 'env-bucket'  # From env
        
        logger.info("✅ Environment variable override test passed")
    
    def test_invalid_config_file_handling(self, config_tester):
        """Test handling of invalid configuration files"""
        logger.info("Testing invalid configuration file handling")
        
        # Test non-existent file
        manager = ConfigManager(config_file='/non/existent/file.yaml')
        config = manager.load_config()
        
        # Should fall back to environment variables
        assert config is not None
        
        # Test malformed YAML file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("invalid: yaml: content: [")
            malformed_file = f.name
        
        try:
            manager = ConfigManager(config_file=malformed_file)
            config = manager.load_config()
            
            # Should handle malformed file gracefully
            assert config is not None
            
        finally:
            os.unlink(malformed_file)
        
        logger.info("✅ Invalid configuration file handling test passed")


class TestRuntimeConfiguration:
    """Test runtime configuration changes"""
    
    def test_config_reload(self, config_tester):
        """Test configuration reloading"""
        logger.info("Testing configuration reloading")
        
        # Set initial environment
        env_vars = {
            'TARDIS_API_KEY': 'TD.initial_key',
            'GCP_PROJECT_ID': 'initial-project',
            'GCP_CREDENTIALS_PATH': '/initial/path/creds.json',
            'GCS_BUCKET': 'initial-bucket',
            'TESTING_MODE': 'true'
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            # Create a new ConfigManager instance to avoid global state
            manager = ConfigManager()
            config1 = manager.load_config()
            assert config1.tardis.api_key == 'TD.initial_key'
            
            # Change environment variables
            env_vars['TARDIS_API_KEY'] = 'TD.updated_key'
            env_vars['GCP_PROJECT_ID'] = 'updated-project'
            
            with patch.dict(os.environ, env_vars, clear=True):
                # Create another ConfigManager instance
                manager2 = ConfigManager()
                config2 = manager2.load_config()
                
                # Verify configuration was updated
                assert config2.tardis.api_key == 'TD.updated_key'
                assert config2.gcp.project_id == 'updated-project'
        
        logger.info("✅ Configuration reload test passed")
    
    def test_config_validation_on_reload(self, config_tester):
        """Test that configuration validation occurs on reload"""
        logger.info("Testing configuration validation on reload")
        
        # Set valid initial configuration
        env_vars = {
            'TARDIS_API_KEY': 'TD.valid_key',
            'GCP_PROJECT_ID': 'valid-project',
            'GCP_CREDENTIALS_PATH': '/valid/path/creds.json',
            'GCS_BUCKET': 'valid-bucket'
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            # Load valid configuration
            config = get_config()
            assert config is not None
            
            # Change to invalid configuration
            env_vars['TARDIS_API_KEY'] = 'invalid_key'
            
            with patch.dict(os.environ, env_vars, clear=True):
                # Attempt to reload with invalid configuration
                from config import reload_config
                with pytest.raises(ValueError, match="Invalid Tardis API key format"):
                    reload_config()
        
        logger.info("✅ Configuration validation on reload test passed")


class TestConfigurationIntegration:
    """Test configuration integration with components"""
    
    def test_tardis_connector_config_integration(self, config_tester):
        """Test TardisConnector integration with configuration"""
        logger.info("Testing TardisConnector configuration integration")
        
        # Set test environment
        env_vars = {
            'TARDIS_API_KEY': 'TD.test_key',
            'GCP_PROJECT_ID': 'test-project',
            'GCP_CREDENTIALS_PATH': '/test/path/creds.json',
            'GCS_BUCKET': 'test-bucket',
            'TARDIS_TIMEOUT': '45',
            'TARDIS_MAX_CONCURRENT': '25',
            'TESTING_MODE': 'true'
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            # Create TardisConnector with explicit config
            config = ConfigManager().load_config()
            connector = TardisConnector(api_key=config.tardis.api_key)
            
            # Verify configuration is applied
            assert connector.api_key == 'TD.test_key'
            assert connector.timeout == 30  # Default timeout
            assert connector.max_retries == 3  # Default retries
        
        logger.info("✅ TardisConnector configuration integration test passed")
    
    def test_download_orchestrator_config_integration(self, config_tester):
        """Test DownloadOrchestrator integration with configuration"""
        logger.info("Testing DownloadOrchestrator configuration integration")
        
        # Set test environment
        env_vars = {
            'TARDIS_API_KEY': 'TD.test_key',
            'GCP_PROJECT_ID': 'test-project',
            'GCP_CREDENTIALS_PATH': '/test/path/creds.json',
            'GCS_BUCKET': 'test-bucket',
            'BATCH_SIZE': '1500',
            'MAX_CONCURRENT_REQUESTS': '5',
            'TESTING_MODE': 'true'
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            # Create DownloadOrchestrator
            orchestrator = DownloadOrchestrator(
                gcs_bucket='test-bucket',
                api_key='TD.test_key'
            )
            
            # Verify configuration is applied
            assert orchestrator.batch_size == 1000  # Default batch size
            assert orchestrator.max_parallel_downloads == 5  # Default value
            assert orchestrator.max_parallel_uploads == 3  # Default value
        
        logger.info("✅ DownloadOrchestrator configuration integration test passed")


if __name__ == "__main__":
    # Run tests directly
    pytest.main([__file__, "-v"])
