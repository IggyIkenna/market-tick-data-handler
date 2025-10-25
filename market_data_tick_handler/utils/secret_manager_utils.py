"""
Secret Manager Utilities

Provides functionality to manage API keys and secrets in Google Cloud Secret Manager.
Supports uploading, retrieving, and managing secrets for various services.
"""

import logging
import json
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
try:
    from google.cloud import secretmanager
except ImportError:
    secretmanager = None
from google.cloud.exceptions import NotFound
try:
    from google.cloud.exceptions import AlreadyExists
except ImportError:
    AlreadyExists = Exception  # Fallback for older versions
from google.api_core import exceptions as gcp_exceptions

logger = logging.getLogger(__name__)

class SecretManagerUtils:
    """Utilities for managing secrets in Google Cloud Secret Manager"""
    
    def __init__(self, project_id: str):
        self.project_id = project_id
        if secretmanager is None:
            raise ImportError("google-cloud-secret-manager package is required for SecretManagerUtils")
        self.client = secretmanager.SecretManagerServiceClient()
    
    def create_secret(self, secret_id: str, description: str = "") -> bool:
        """Create a new secret in Secret Manager"""
        try:
            parent = f"projects/{self.project_id}"
            
            # Check if secret already exists
            if self.secret_exists(secret_id):
                logger.warning(f"Secret {secret_id} already exists")
                return True
            
            # Create the secret
            secret = {
                "replication": {
                    "automatic": {}
                }
            }
            
            if description:
                secret["labels"] = {"description": description}
            
            response = self.client.create_secret(
                request={
                    "parent": parent,
                    "secret_id": secret_id,
                    "secret": secret,
                }
            )
            
            logger.info(f"Created secret: {response.name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create secret {secret_id}: {e}")
            return False
    
    def secret_exists(self, secret_id: str) -> bool:
        """Check if a secret exists"""
        try:
            name = f"projects/{self.project_id}/secrets/{secret_id}"
            self.client.get_secret(request={"name": name})
            return True
        except NotFound:
            return False
        except Exception as e:
            logger.error(f"Error checking if secret exists: {e}")
            return False
    
    def add_secret_version(self, secret_id: str, secret_data: str, description: str = "") -> bool:
        """Add a new version to an existing secret"""
        try:
            name = f"projects/{self.project_id}/secrets/{secret_id}"
            
            # Convert string to bytes
            if isinstance(secret_data, str):
                secret_data = secret_data.encode('utf-8')
            
            # Add the secret version
            response = self.client.add_secret_version(
                request={
                    "parent": name,
                    "payload": {
                        "data": secret_data,
                    },
                }
            )
            
            logger.info(f"Added secret version: {response.name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to add secret version for {secret_id}: {e}")
            return False
    
    def get_secret_version(self, secret_id: str, version: str = "latest") -> Optional[str]:
        """Get a secret version"""
        try:
            name = f"projects/{self.project_id}/secrets/{secret_id}/versions/{version}"
            
            response = self.client.access_secret_version(request={"name": name})
            
            # Decode the secret data
            secret_data = response.payload.data.decode('utf-8')
            return secret_data
            
        except NotFound:
            logger.warning(f"Secret version not found: {secret_id}/{version}")
            return None
        except Exception as e:
            logger.error(f"Failed to get secret version {secret_id}/{version}: {e}")
            return None
    
    def list_secrets(self, filter_str: str = "") -> List[Dict[str, Any]]:
        """List all secrets in the project"""
        try:
            parent = f"projects/{self.project_id}"
            
            secrets = []
            for secret in self.client.list_secrets(request={"parent": parent, "filter": filter_str}):
                secrets.append({
                    "name": secret.name,
                    "secret_id": secret.name.split("/")[-1],
                    "create_time": secret.create_time,
                    "labels": dict(secret.labels) if secret.labels else {}
                })
            
            return secrets
            
        except Exception as e:
            logger.error(f"Failed to list secrets: {e}")
            return []
    
    def delete_secret(self, secret_id: str) -> bool:
        """Delete a secret (and all its versions)"""
        try:
            name = f"projects/{self.project_id}/secrets/{secret_id}"
            
            self.client.delete_secret(request={"name": name})
            logger.info(f"Deleted secret: {name}")
            return True
            
        except NotFound:
            logger.warning(f"Secret not found: {secret_id}")
            return True  # Already deleted
        except Exception as e:
            logger.error(f"Failed to delete secret {secret_id}: {e}")
            return False
    
    def upload_api_key(self, service_name: str, api_key: str, description: str = "") -> bool:
        """Upload an API key for a specific service"""
        try:
            secret_id = f"{service_name}-api-key"
            
            # Create secret if it doesn't exist
            if not self.secret_exists(secret_id):
                if not self.create_secret(secret_id, description or f"API key for {service_name}"):
                    return False
            
            # Add the API key as a new version
            return self.add_secret_version(secret_id, api_key, f"API key uploaded at {datetime.now(timezone.utc).isoformat()}")
            
        except Exception as e:
            logger.error(f"Failed to upload API key for {service_name}: {e}")
            return False
    
    def get_api_key(self, service_name: str) -> Optional[str]:
        """Get an API key for a specific service"""
        secret_id = f"{service_name}-api-key"
        return self.get_secret_version(secret_id)
    
    def upload_trading_keys(self, exchange: str, keys: Dict[str, str], description: str = "") -> bool:
        """Upload trading keys for an exchange"""
        try:
            secret_id = f"{exchange}-trading-keys"
            
            # Create secret if it doesn't exist
            if not self.secret_exists(secret_id):
                if not self.create_secret(secret_id, description or f"Trading keys for {exchange}"):
                    return False
            
            # Convert keys to JSON
            keys_json = json.dumps(keys, indent=2)
            
            # Add the keys as a new version
            return self.add_secret_version(secret_id, keys_json, f"Trading keys uploaded at {datetime.now(timezone.utc).isoformat()}")
            
        except Exception as e:
            logger.error(f"Failed to upload trading keys for {exchange}: {e}")
            return False
    
    def get_trading_keys(self, exchange: str) -> Optional[Dict[str, str]]:
        """Get trading keys for an exchange"""
        secret_id = f"{exchange}-trading-keys"
        keys_json = self.get_secret_version(secret_id)
        
        if keys_json:
            try:
                return json.loads(keys_json)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse trading keys JSON for {exchange}: {e}")
                return None
        
        return None
    
    def upload_config(self, config_name: str, config_data: Dict[str, Any], description: str = "") -> bool:
        """Upload configuration data"""
        try:
            secret_id = f"{config_name}-config"
            
            # Create secret if it doesn't exist
            if not self.secret_exists(secret_id):
                if not self.create_secret(secret_id, description or f"Configuration for {config_name}"):
                    return False
            
            # Convert config to JSON
            config_json = json.dumps(config_data, indent=2)
            
            # Add the config as a new version
            return self.add_secret_version(secret_id, config_json, f"Config uploaded at {datetime.now(timezone.utc).isoformat()}")
            
        except Exception as e:
            logger.error(f"Failed to upload config for {config_name}: {e}")
            return False
    
    def get_config(self, config_name: str) -> Optional[Dict[str, Any]]:
        """Get configuration data"""
        secret_id = f"{config_name}-config"
        config_json = self.get_secret_version(secret_id)
        
        if config_json:
            try:
                return json.loads(config_json)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse config JSON for {config_name}: {e}")
                return None
        
        return None
    
    def list_api_keys(self) -> List[str]:
        """List all API key secrets"""
        secrets = self.list_secrets()
        return [s["secret_id"] for s in secrets if s["secret_id"].endswith("-api-key")]
    
    def list_trading_keys(self) -> List[str]:
        """List all trading key secrets"""
        secrets = self.list_secrets()
        return [s["secret_id"] for s in secrets if s["secret_id"].endswith("-trading-keys")]
    
    def list_configs(self) -> List[str]:
        """List all configuration secrets"""
        secrets = self.list_secrets()
        return [s["secret_id"] for s in secrets if s["secret_id"].endswith("-config")]
