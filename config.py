"""
Configuration Management for Market Data Tick Handler

This module handles all configuration settings including environment variables,
file-based configuration, and validation.
"""

import os
import yaml
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from pathlib import Path
import logging
from dotenv import load_dotenv

@dataclass
class TardisConfig:
    """Tardis API configuration"""
    api_key: str
    base_url: str = "https://datasets.tardis.dev"
    timeout: int = 30
    max_retries: int = 3
    max_concurrent: int = 50
    max_parallel_uploads: int = 20
    rate_limit_per_vm: int = 1000000  # 1M calls per day per VM
    
    def __post_init__(self):
        """Validate Tardis configuration"""
        if not self.api_key:
            raise ValueError("Tardis API key is required")
        if not self.api_key.startswith('TD.'):
            raise ValueError("Invalid Tardis API key format")
        if self.timeout <= 0:
            raise ValueError("Timeout must be positive")
        if self.max_retries < 0:
            raise ValueError("Max retries must be non-negative")
        if self.max_concurrent <= 0:
            raise ValueError("Max concurrent must be positive")
        if self.max_parallel_uploads <= 0:
            raise ValueError("Max parallel uploads must be positive")
        if self.rate_limit_per_vm <= 0:
            raise ValueError("Rate limit per VM must be positive")

@dataclass
class GCPConfig:
    """Google Cloud Platform configuration"""
    project_id: str
    credentials_path: str
    bucket: str
    region: str = "asia-northeast1-c"
    
    def __post_init__(self):
        """Validate GCP configuration"""
        if not self.project_id:
            raise ValueError("GCP project ID is required")
        if not self.credentials_path:
            raise ValueError("GCP credentials path is required")
        if not self.bucket:
            raise ValueError("GCS bucket name is required")
        
        # Check if credentials file exists
        if not Path(self.credentials_path).exists():
            raise FileNotFoundError(f"GCP credentials file not found: {self.credentials_path}")

@dataclass
class ServiceConfig:
    """Service configuration"""
    log_level: str = "INFO"
    log_destination: str = "local"  # 'local', 'gcp', 'both'
    max_concurrent_requests: int = 2
    batch_size: int = 1000
    memory_efficient: bool = False
    enable_caching: bool = True
    cache_ttl: int = 3600  # 1 hour
    
    def __post_init__(self):
        """Validate service configuration"""
        valid_log_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if self.log_level.upper() not in valid_log_levels:
            raise ValueError(f"Invalid log level: {self.log_level}")
        
        valid_destinations = ['local', 'gcp', 'both']
        if self.log_destination not in valid_destinations:
            raise ValueError(f"Invalid log destination: {self.log_destination}")
        
        if self.max_concurrent_requests <= 0:
            raise ValueError("Max concurrent requests must be positive")
        if self.batch_size <= 0:
            raise ValueError("Batch size must be positive")
        if self.cache_ttl <= 0:
            raise ValueError("Cache TTL must be positive")

@dataclass
class ShardingConfig:
    """Sharding configuration"""
    shard_index: int = 0
    total_shards: int = 30
    instruments_per_shard: int = 2
    
    def __post_init__(self):
        """Validate sharding configuration"""
        if self.shard_index < 0 or self.shard_index >= self.total_shards:
            raise ValueError(f"Invalid shard index: {self.shard_index}")
        if self.total_shards <= 0:
            raise ValueError("Total shards must be positive")
        if self.instruments_per_shard <= 0:
            raise ValueError("Instruments per shard must be positive")

@dataclass
class OutputConfig:
    """Output configuration"""
    default_format: str = "json"  # 'json', 'csv', 'parquet'
    default_limit: int = 10000
    include_metadata: bool = True
    compression: str = "snappy"  # 'snappy', 'gzip', 'lz4', 'zstd'
    
    def __post_init__(self):
        """Validate output configuration"""
        valid_formats = ['json', 'csv', 'parquet']
        if self.default_format not in valid_formats:
            raise ValueError(f"Invalid output format: {self.default_format}")
        
        valid_compressions = ['snappy', 'gzip', 'lz4', 'zstd']
        if self.compression not in valid_compressions:
            raise ValueError(f"Invalid compression: {self.compression}")
        
        if self.default_limit <= 0:
            raise ValueError("Default limit must be positive")

@dataclass
class Config:
    """Main configuration class"""
    tardis: TardisConfig
    gcp: GCPConfig
    service: ServiceConfig = field(default_factory=ServiceConfig)
    sharding: ShardingConfig = field(default_factory=ShardingConfig)
    output: OutputConfig = field(default_factory=OutputConfig)
    
    # Runtime configuration
    debug: bool = False
    test_mode: bool = False
    
    def __post_init__(self):
        """Validate overall configuration"""
        if self.test_mode:
            self.service.log_level = "DEBUG"
            self.service.enable_caching = False

class ConfigManager:
    """Configuration manager for loading and validating settings"""
    
    def __init__(self, config_file: Optional[str] = None):
        self.config_file = config_file or self._find_config_file()
        self._config: Optional[Config] = None
        
        # Load .env file if it exists
        env_file = Path.cwd() / ".env"
        if env_file.exists():
            load_dotenv(env_file)
            logging.info(f"Loaded environment variables from {env_file}")
        else:
            logging.info("No .env file found, using system environment variables")
    
    def _find_config_file(self) -> Optional[str]:
        """Find configuration file in standard locations"""
        config_paths = [
            Path.home() / ".tick-data" / "config.yaml",
            Path.cwd() / "config.yaml",
            Path.cwd() / ".env",
            Path("/etc/tick-data/config.yaml")
        ]
        
        for path in config_paths:
            if path.exists():
                return str(path)
        
        return None
    
    def load_config(self) -> Config:
        """Load configuration from file and environment variables"""
        if self._config is not None:
            return self._config
        
        # Load from file if available
        file_config = {}
        if self.config_file and Path(self.config_file).exists():
            file_config = self._load_from_file(self.config_file)
        
        # Load from environment variables
        env_config = self._load_from_env()
        
        # Merge configurations (env overrides file)
        config_dict = {**file_config, **env_config}
        
        # Create configuration objects
        self._config = self._create_config(config_dict)
        return self._config
    
    def _load_from_file(self, config_file: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            with open(config_file, 'r') as f:
                if config_file.endswith('.yaml') or config_file.endswith('.yml'):
                    return yaml.safe_load(f) or {}
                else:
                    # Assume .env format
                    return self._parse_env_file(f.read())
        except Exception as e:
            logging.warning(f"Failed to load config file {config_file}: {e}")
            return {}
    
    def _parse_env_file(self, content: str) -> Dict[str, Any]:
        """Parse .env file content"""
        config = {}
        for line in content.split('\n'):
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip().strip('"\'')
                config[key.lower()] = value
        return config
    
    def _load_from_env(self) -> Dict[str, Any]:
        """Load configuration from environment variables"""
        config = {}
        
        # Tardis configuration
        tardis_api_key = os.getenv('TARDIS_API_KEY')
        if not tardis_api_key:
            raise ValueError("TARDIS_API_KEY environment variable is required")
        
        config['tardis'] = {
            'api_key': tardis_api_key,
            'base_url': os.getenv('TARDIS_BASE_URL', 'https://datasets.tardis.dev'),
            'timeout': int(os.getenv('TARDIS_TIMEOUT', '30')),
            'max_retries': int(os.getenv('TARDIS_MAX_RETRIES', '3')),
            'max_concurrent': int(os.getenv('MAX_CONCURRENT_REQUESTS', '2')),
            'rate_limit_per_vm': int(os.getenv('RATE_LIMIT_PER_VM', '1000000'))
        }
        
        # GCP configuration
        gcp_project_id = os.getenv('GCP_PROJECT_ID')
        gcp_credentials_path = os.getenv('GCP_CREDENTIALS_PATH')
        gcs_bucket = os.getenv('GCS_BUCKET')
        
        if not gcp_project_id:
            raise ValueError("GCP_PROJECT_ID environment variable is required")
        if not gcp_credentials_path:
            raise ValueError("GCP_CREDENTIALS_PATH environment variable is required")
        if not gcs_bucket:
            raise ValueError("GCS_BUCKET environment variable is required")
        
        config['gcp'] = {
            'project_id': gcp_project_id,
            'credentials_path': gcp_credentials_path,
            'bucket': gcs_bucket,
            'region': os.getenv('GCS_REGION', 'asia-northeast1-c')
        }
        
        # Service configuration
        config['service'] = {
            'log_level': os.getenv('LOG_LEVEL', 'INFO'),
            'log_destination': os.getenv('LOG_DESTINATION', 'local'),
            'max_concurrent_requests': int(os.getenv('MAX_CONCURRENT_REQUESTS', '2')),
            'batch_size': int(os.getenv('BATCH_SIZE', '1000')),
            'memory_efficient': os.getenv('MEMORY_EFFICIENT', 'false').lower() == 'true',
            'enable_caching': os.getenv('ENABLE_CACHING', 'true').lower() == 'true',
            'cache_ttl': int(os.getenv('CACHE_TTL', '3600'))
        }
        
        # Sharding configuration
        if os.getenv('SHARD_INDEX') is not None:
            config['sharding'] = {
                'shard_index': int(os.getenv('SHARD_INDEX', '0')),
                'total_shards': int(os.getenv('TOTAL_SHARDS', '30')),
                'instruments_per_shard': int(os.getenv('INSTRUMENTS_PER_SHARD', '2'))
            }
        
        # Output configuration
        config['output'] = {
            'default_format': os.getenv('OUTPUT_FORMAT', 'json'),
            'default_limit': int(os.getenv('DEFAULT_LIMIT', '10000')),
            'include_metadata': os.getenv('INCLUDE_METADATA', 'true').lower() == 'true',
            'compression': os.getenv('COMPRESSION', 'snappy')
        }
        
        # Runtime configuration
        config['debug'] = os.getenv('DEBUG', 'false').lower() == 'true'
        config['test_mode'] = os.getenv('TEST_MODE', 'false').lower() == 'true'
        
        return config
    
    def _create_config(self, config_dict: Dict[str, Any]) -> Config:
        """Create Config object from dictionary"""
        # Create Tardis config
        tardis_config = TardisConfig(
            api_key=config_dict.get('tardis', {}).get('api_key', ''),
            base_url=config_dict.get('tardis', {}).get('base_url', 'https://datasets.tardis.dev'),
            timeout=config_dict.get('tardis', {}).get('timeout', 30),
            max_retries=config_dict.get('tardis', {}).get('max_retries', 3),
            max_concurrent=config_dict.get('tardis', {}).get('max_concurrent', 50),
            max_parallel_uploads=config_dict.get('tardis', {}).get('max_parallel_uploads', 20),
            rate_limit_per_vm=config_dict.get('tardis', {}).get('rate_limit_per_vm', 1000000)
        )
        
        # Create GCP config
        gcp_config = GCPConfig(
            project_id=config_dict.get('gcp', {}).get('project_id', ''),
            credentials_path=config_dict.get('gcp', {}).get('credentials_path', ''),
            bucket=config_dict.get('gcp', {}).get('bucket', ''),
            region=config_dict.get('gcp', {}).get('region', 'asia-northeast1-c')
        )
        
        # Create service config
        service_config = ServiceConfig(
            log_level=config_dict.get('service', {}).get('log_level', 'INFO'),
            log_destination=config_dict.get('service', {}).get('log_destination', 'local'),
            max_concurrent_requests=config_dict.get('service', {}).get('max_concurrent_requests', 2),
            batch_size=config_dict.get('service', {}).get('batch_size', 1000),
            memory_efficient=config_dict.get('service', {}).get('memory_efficient', False),
            enable_caching=config_dict.get('service', {}).get('enable_caching', True),
            cache_ttl=config_dict.get('service', {}).get('cache_ttl', 3600)
        )
        
        # Create sharding config
        sharding_config = ShardingConfig(
            shard_index=config_dict.get('sharding', {}).get('shard_index', 0),
            total_shards=config_dict.get('sharding', {}).get('total_shards', 30),
            instruments_per_shard=config_dict.get('sharding', {}).get('instruments_per_shard', 2)
        )
        
        # Create output config
        output_config = OutputConfig(
            default_format=config_dict.get('output', {}).get('default_format', 'json'),
            default_limit=config_dict.get('output', {}).get('default_limit', 10000),
            include_metadata=config_dict.get('output', {}).get('include_metadata', True),
            compression=config_dict.get('output', {}).get('compression', 'snappy')
        )
        
        return Config(
            tardis=tardis_config,
            gcp=gcp_config,
            service=service_config,
            sharding=sharding_config,
            output=output_config,
            debug=config_dict.get('debug', False),
            test_mode=config_dict.get('test_mode', False)
        )
    
    def save_config(self, config: Config, file_path: str):
        """Save configuration to file"""
        config_dict = {
            'tardis': {
                'api_key': config.tardis.api_key,
                'base_url': config.tardis.base_url,
                'timeout': config.tardis.timeout,
                'max_retries': config.tardis.max_retries,
                'max_concurrent': config.tardis.max_concurrent,
                'rate_limit_per_vm': config.tardis.rate_limit_per_vm
            },
            'gcp': {
                'project_id': config.gcp.project_id,
                'credentials_path': config.gcp.credentials_path,
                'bucket': config.gcp.bucket,
                'region': config.gcp.region
            },
            'service': {
                'log_level': config.service.log_level,
                'log_destination': config.service.log_destination,
                'max_concurrent_requests': config.service.max_concurrent_requests,
                'batch_size': config.service.batch_size,
                'memory_efficient': config.service.memory_efficient,
                'enable_caching': config.service.enable_caching,
                'cache_ttl': config.service.cache_ttl
            },
            'sharding': {
                'shard_index': config.sharding.shard_index,
                'total_shards': config.sharding.total_shards,
                'instruments_per_shard': config.sharding.instruments_per_shard
            },
            'output': {
                'default_format': config.output.default_format,
                'default_limit': config.output.default_limit,
                'include_metadata': config.output.include_metadata,
                'compression': config.output.compression
            },
            'debug': config.debug,
            'test_mode': config.test_mode
        }
        
        # Ensure directory exists
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Save to file
        with open(file_path, 'w') as f:
            yaml.dump(config_dict, f, default_flow_style=False, indent=2)

# Global configuration instance
_config_manager = ConfigManager()
config = _config_manager.load_config()

def get_config() -> Config:
    """Get the global configuration instance"""
    return config

def reload_config() -> Config:
    """Reload configuration from sources"""
    global config
    config = _config_manager.load_config()
    return config
