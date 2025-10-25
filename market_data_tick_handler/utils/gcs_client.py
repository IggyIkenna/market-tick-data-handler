"""
Shared GCS Client Utility

Provides a singleton GCS client with connection pooling to be used across
all components of the market data handler for optimal performance.
"""

import logging
from typing import Optional
from google.cloud import storage
from google.auth import default

logger = logging.getLogger(__name__)

class SharedGCSClient:
    """Singleton GCS client with optimized connection pooling"""
    
    _instance: Optional['SharedGCSClient'] = None
    _client: Optional[storage.Client] = None
    
    def __new__(cls) -> 'SharedGCSClient':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._client is None:
            self._client = self._create_optimized_gcs_client()
    
    @classmethod
    def _create_optimized_gcs_client(cls) -> storage.Client:
        """Create an optimized GCS client with connection pooling"""
        try:
            # Get default credentials
            credentials, project = default()
            
            # Create GCS client with optimized settings
            client = storage.Client(
                credentials=credentials,
                project=project
            )
            
            # The Google Cloud client library already has built-in connection pooling
            # We can optimize it by configuring the underlying HTTP client
            if hasattr(client, '_http_internal'):
                # Configure connection pooling if available
                http_client = client._http_internal
                
                # Set keep-alive headers for connection reuse
                if hasattr(http_client, 'headers'):
                    http_client.headers.update({
                        'Connection': 'keep-alive',
                        'Keep-Alive': 'timeout=300, max=1000',
                        'User-Agent': 'MarketDataTickHandler/1.0.0'
                    })
            
            logger.info("✅ Shared GCS client created with connection pooling")
            return client
            
        except Exception as e:
            logger.error(f"❌ Failed to create GCS client: {e}")
            raise
    
    @property
    def client(self) -> storage.Client:
        """Get the shared GCS client instance"""
        return self._client
    
    def get_bucket(self, bucket_name: str) -> storage.Bucket:
        """Get a bucket using the shared client"""
        return self._client.bucket(bucket_name)

# Global singleton instance
_shared_gcs_client: Optional[SharedGCSClient] = None

def get_shared_gcs_client() -> storage.Client:
    """
    Get the shared GCS client instance.
    
    This function provides a singleton GCS client with connection pooling
    that should be used by all components for optimal performance.
    
    Returns:
        storage.Client: Optimized GCS client with connection pooling
    """
    global _shared_gcs_client
    if _shared_gcs_client is None:
        _shared_gcs_client = SharedGCSClient()
    return _shared_gcs_client.client

def get_shared_gcs_bucket(bucket_name: str) -> storage.Bucket:
    """
    Get a GCS bucket using the shared client.
    
    Args:
        bucket_name: Name of the GCS bucket
        
    Returns:
        storage.Bucket: GCS bucket instance using shared client
    """
    global _shared_gcs_client
    if _shared_gcs_client is None:
        _shared_gcs_client = SharedGCSClient()
    return _shared_gcs_client.get_bucket(bucket_name)
