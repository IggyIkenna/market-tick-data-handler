"""
Google Cloud Secret Manager utilities for secure API key retrieval.

This module provides utilities to retrieve sensitive configuration values
from Google Cloud Secret Manager, with fallback to environment variables
for local development.
"""

import os
import logging
from typing import Optional, Dict, Any
from google.cloud import secretmanager
from google.auth.exceptions import DefaultCredentialsError
from google.api_core import exceptions as gcp_exceptions

logger = logging.getLogger(__name__)


class SecretManagerClient:
    """Client for retrieving secrets from Google Cloud Secret Manager."""
    
    def __init__(self, project_id: str, credentials_path: Optional[str] = None):
        """
        Initialize the Secret Manager client.
        
        Args:
            project_id: GCP project ID
            credentials_path: Path to service account credentials (optional)
        """
        self.project_id = project_id
        self.client = None
        self._initialize_client(credentials_path)
    
    def _initialize_client(self, credentials_path: Optional[str] = None):
        """Initialize the Secret Manager client with proper authentication."""
        try:
            if credentials_path and os.path.exists(credentials_path):
                # Use explicit credentials file
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
                logger.info(f"Using credentials from: {credentials_path}")
            elif credentials_path:
                logger.error(f"Specified credentials file does not exist: {credentials_path}")
                if credentials_path == "central-element-323112-e35fb0ddafe2.json":
                    logger.error("Missing credentials file 'central-element-323112-e35fb0ddafe2.json'")
                    logger.error("Try restoring it with: bash scripts/restore-credentials.sh")
                logger.info("Attempting to use default credentials")
            else:
                logger.info("No explicit credentials path provided, using default authentication")
            
            self.client = secretmanager.SecretManagerServiceClient()
            logger.info("Secret Manager client initialized successfully")
            
            # Test authentication by attempting to list secrets (without actually retrieving them)
            try:
                parent = f"projects/{self.project_id}"
                # Just get the first page to test authentication
                secrets_iter = self.client.list_secrets(request={"parent": parent, "page_size": 1})
                next(secrets_iter, None)  # Try to get first secret (or None if empty)
                logger.info("Secret Manager authentication verified")
            except Exception as auth_test_error:
                logger.warning(f"Secret Manager authentication test failed: {auth_test_error}")
                # Don't set client to None here - the actual secret retrieval might still work
            
        except DefaultCredentialsError as e:
            logger.error(f"Google Cloud credentials not found or invalid: {e}")
            logger.error("Possible solutions:")
            logger.error("1. Restore the credentials file: bash scripts/restore-credentials.sh")
            logger.error("2. Ensure GOOGLE_APPLICATION_CREDENTIALS points to central-element-323112-e35fb0ddafe2.json")
            logger.error("3. Run 'gcloud auth application-default login'")
            self.client = None
        except Exception as e:
            logger.error(f"Unexpected error initializing Secret Manager client: {e}")
            self.client = None
    
    def get_secret(self, secret_name: str, version: str = "latest") -> Optional[str]:
        """
        Retrieve a secret from Secret Manager.
        
        Args:
            secret_name: Name of the secret (without project prefix)
            version: Secret version (default: "latest")
            
        Returns:
            Secret value as string, or None if not found
        """
        if not self.client:
            logger.error("Secret Manager client not available - cannot retrieve secrets")
            return None
        
        try:
            # Construct the resource name
            secret_path = f"projects/{self.project_id}/secrets/{secret_name}/versions/{version}"
            logger.debug(f"Attempting to retrieve secret from path: {secret_path}")
            
            # Access the secret version
            response = self.client.access_secret_version(request={"name": secret_path})
            
            # Decode the secret value
            secret_value = response.payload.data.decode("UTF-8")
            logger.info(f"Successfully retrieved secret: {secret_name}")
            return secret_value
            
        except gcp_exceptions.NotFound:
            logger.error(f"Secret not found in Secret Manager: {secret_name} (project: {self.project_id})")
            logger.error("Please verify the secret name exists and check your access permissions")
            return None
        except gcp_exceptions.PermissionDenied:
            logger.error(f"Permission denied accessing secret: {secret_name} (project: {self.project_id})")
            logger.error("Possible solutions:")
            logger.error("1. Ensure your service account has 'Secret Manager Secret Accessor' role")
            logger.error("2. Restore credentials file: bash scripts/restore-credentials.sh")
            logger.error("3. Check if central-element-323112-e35fb0ddafe2.json exists in project root")
            return None
        except gcp_exceptions.Unauthenticated:
            logger.error(f"Authentication failed when accessing secret: {secret_name}")
            logger.error("Possible solutions:")
            logger.error("1. Restore credentials file: bash scripts/restore-credentials.sh")
            logger.error("2. Check if central-element-323112-e35fb0ddafe2.json exists in project root")
            logger.error("3. Run 'gcloud auth application-default login'")
            return None
        except Exception as e:
            logger.error(f"Unexpected error retrieving secret {secret_name}: {e}")
            logger.error(f"Secret path attempted: {secret_path}")
            return None
    
    def get_secrets(self, secret_names: list, version: str = "latest") -> Dict[str, Optional[str]]:
        """
        Retrieve multiple secrets from Secret Manager.
        
        Args:
            secret_names: List of secret names
            version: Secret version (default: "latest")
            
        Returns:
            Dictionary mapping secret names to their values
        """
        secrets = {}
        for secret_name in secret_names:
            secrets[secret_name] = self.get_secret(secret_name, version)
        return secrets


def get_tardis_api_key(
    project_id: str,
    credentials_path: Optional[str] = None,
    secret_name: str = "tardis-api-key",
    fallback_env_var: str = "TARDIS_API_KEY"
) -> Optional[str]:
    """
    Get Tardis API key from Secret Manager with fallback to environment variable.
    
    Args:
        project_id: GCP project ID
        credentials_path: Path to service account credentials (optional)
        secret_name: Name of the secret in Secret Manager
        fallback_env_var: Environment variable name for fallback
        
    Returns:
        API key string, or None if not found
    """
    logger.info(f"Attempting to retrieve Tardis API key (secret: {secret_name}, project: {project_id})")
    
    # Try Secret Manager first
    try:
        secret_client = SecretManagerClient(project_id, credentials_path)
        if secret_client.client is None:
            logger.warning("Secret Manager client initialization failed - skipping Secret Manager lookup")
        else:
            api_key = secret_client.get_secret(secret_name)
            
            if api_key:
                logger.info("Retrieved Tardis API key from Secret Manager")
                # Validate the key format
                if api_key.startswith('TD.'):
                    return api_key
                else:
                    logger.warning(f"Retrieved secret doesn't match expected Tardis API key format (should start with 'TD.')")
                    return api_key  # Return it anyway, let validation handle the error
            else:
                logger.warning(f"Secret {secret_name} not found in Secret Manager or returned None")
            
    except Exception as e:
        logger.error(f"Exception during Secret Manager retrieval: {e}")
        logger.info("Falling back to environment variable")
    
    # Fallback to environment variable
    logger.info(f"Attempting to retrieve Tardis API key from environment variable: {fallback_env_var}")
    api_key = os.getenv(fallback_env_var)
    if api_key:
        logger.info(f"Retrieved Tardis API key from environment variable: {fallback_env_var}")
        return api_key
    else:
        logger.warning(f"Environment variable {fallback_env_var} not set or empty")
    
    logger.error(f"Tardis API key not found in Secret Manager (secret: {secret_name}) or environment variable ({fallback_env_var})")
    logger.error("Possible solutions:")
    logger.error("1. Restore credentials file: bash scripts/restore-credentials.sh")
    logger.error("2. Check if central-element-323112-e35fb0ddafe2.json exists in project root")
    logger.error("3. Set TARDIS_API_KEY environment variable")
    return None


def get_secret_config(
    project_id: str,
    credentials_path: Optional[str] = None,
    secret_mappings: Dict[str, str] = None
) -> Dict[str, Optional[str]]:
    """
    Get multiple configuration values from Secret Manager with environment variable fallbacks.
    
    Args:
        project_id: GCP project ID
        credentials_path: Path to service account credentials (optional)
        secret_mappings: Dictionary mapping secret names to environment variable names
        
    Returns:
        Dictionary with configuration values
    """
    if secret_mappings is None:
        secret_mappings = {
            "tardis-api-key": "TARDIS_API_KEY",
            "gcp-bucket": "GCS_BUCKET",
            "gcp-region": "GCS_REGION"
        }
    
    config = {}
    
    try:
        secret_client = SecretManagerClient(project_id, credentials_path)
        
        for secret_name, env_var in secret_mappings.items():
            # Try Secret Manager first
            secret_value = secret_client.get_secret(secret_name)
            
            if secret_value:
                config[env_var] = secret_value
                logger.info(f"Retrieved {secret_name} from Secret Manager")
            else:
                # Fallback to environment variable
                env_value = os.getenv(env_var)
                if env_value:
                    config[env_var] = env_value
                    logger.info(f"Retrieved {env_var} from environment variable")
                else:
                    config[env_var] = None
                    logger.warning(f"Neither secret {secret_name} nor env var {env_var} found")
                    
    except Exception as e:
        logger.error(f"Error retrieving secrets: {e}")
        # Fallback to environment variables only
        for secret_name, env_var in secret_mappings.items():
            config[env_var] = os.getenv(env_var)
    
    return config


def create_secret_if_not_exists(
    project_id: str,
    secret_name: str,
    secret_value: str,
    credentials_path: Optional[str] = None
) -> bool:
    """
    Create a secret in Secret Manager if it doesn't exist.
    
    Args:
        project_id: GCP project ID
        secret_name: Name of the secret
        secret_value: Value to store
        credentials_path: Path to service account credentials (optional)
        
    Returns:
        True if secret was created or already exists, False otherwise
    """
    try:
        secret_client = SecretManagerClient(project_id, credentials_path)
        
        if not secret_client.client:
            logger.error("Secret Manager client not available")
            return False
        
        # Check if secret exists
        secret_path = f"projects/{project_id}/secrets/{secret_name}"
        
        try:
            secret_client.client.get_secret(request={"name": secret_path})
            logger.info(f"Secret {secret_name} already exists")
            return True
        except gcp_exceptions.NotFound:
            # Secret doesn't exist, create it
            pass
        
        # Create the secret
        parent = f"projects/{project_id}"
        secret = {"replication": {"automatic": {}}}
        
        secret_client.client.create_secret(
            request={
                "parent": parent,
                "secret_id": secret_name,
                "secret": secret,
            }
        )
        
        # Add the secret version
        version_path = f"projects/{project_id}/secrets/{secret_name}"
        secret_version = {"data": secret_value.encode("UTF-8")}
        
        secret_client.client.add_secret_version(
            request={"parent": version_path, "payload": secret_version}
        )
        
        logger.info(f"Successfully created secret: {secret_name}")
        return True
        
    except Exception as e:
        logger.error(f"Error creating secret {secret_name}: {e}")
        return False


def setup_tardis_secret(
    project_id: str,
    api_key: str,
    secret_name: str = "tardis-api-key",
    credentials_path: Optional[str] = None
) -> bool:
    """
    Set up Tardis API key in Secret Manager.
    
    Args:
        project_id: GCP project ID
        api_key: Tardis API key to store
        secret_name: Name of the secret
        credentials_path: Path to service account credentials (optional)
        
    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Setting up Tardis API key in Secret Manager: {secret_name}")
    return create_secret_if_not_exists(project_id, secret_name, api_key, credentials_path)
