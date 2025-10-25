#!/usr/bin/env python3
"""
Secret Manager Example

Demonstrates how to use the secret manager utilities to manage API keys
and trading keys in Google Cloud Secret Manager.
"""

import sys
import os
from datetime import datetime, timezone
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from market_data_tick_handler.utils.secret_manager_utils import SecretManagerUtils
from config import get_config

def main():
    """Demonstrate secret manager functionality"""
    print("Secret Manager Example")
    print("=" * 50)
    
    # Initialize the secret manager utils
    config = get_config()
    secret_manager = SecretManagerUtils(config.gcp.project_id)
    
    print(f"Using project: {config.gcp.project_id}")
    
    # Example 1: Upload API keys
    print("\n1. Uploading API Keys:")
    print("-" * 30)
    
    # Example API keys (in real usage, these would come from environment variables or user input)
    api_keys = {
        "tardis": "your-tardis-api-key-here",
        "binance": "your-binance-api-key-here",
        "deribit": "your-deribit-api-key-here"
    }
    
    for service, api_key in api_keys.items():
        if api_key != f"your-{service}-api-key-here":  # Skip placeholder keys
            success = secret_manager.upload_api_key(
                service_name=service,
                api_key=api_key,
                description=f"API key for {service} service"
            )
            print(f"  {service}: {'✅ Success' if success else '❌ Failed'}")
        else:
            print(f"  {service}: ⏭️ Skipped (placeholder key)")
    
    # Example 2: Upload trading keys
    print("\n2. Uploading Trading Keys:")
    print("-" * 30)
    
    trading_keys = {
        "binance": {
            "api_key": "your-binance-api-key",
            "secret_key": "your-binance-secret-key",
            "testnet": True
        },
        "deribit": {
            "client_id": "your-deribit-client-id",
            "client_secret": "your-deribit-client-secret",
            "testnet": True
        }
    }
    
    for exchange, keys in trading_keys.items():
        # Check if keys are placeholders
        if any("your-" in str(v) for v in keys.values()):
            print(f"  {exchange}: ⏭️ Skipped (placeholder keys)")
            continue
        
        success = secret_manager.upload_trading_keys(
            exchange=exchange,
            keys=keys,
            description=f"Trading keys for {exchange} exchange"
        )
        print(f"  {exchange}: {'✅ Success' if success else '❌ Failed'}")
    
    # Example 3: List existing secrets
    print("\n3. Listing Existing Secrets:")
    print("-" * 30)
    
    api_keys = secret_manager.list_api_keys()
    trading_keys = secret_manager.list_trading_keys()
    configs = secret_manager.list_configs()
    
    print(f"API Keys: {api_keys}")
    print(f"Trading Keys: {trading_keys}")
    print(f"Configs: {configs}")
    
    # Example 4: Retrieve secrets
    print("\n4. Retrieving Secrets:")
    print("-" * 30)
    
    for service in ["tardis", "binance", "deribit"]:
        api_key = secret_manager.get_api_key(service)
        if api_key:
            # Mask the key for display
            masked_key = api_key[:8] + "..." + api_key[-4:] if len(api_key) > 12 else "***"
            print(f"  {service} API key: {masked_key}")
        else:
            print(f"  {service} API key: Not found")
    
    # Example 5: Upload configuration
    print("\n5. Uploading Configuration:")
    print("-" * 30)
    
    config_data = {
        "tardis": {
            "base_url": "https://api.tardis.dev",
            "timeout": 30,
            "retry_attempts": 3
        },
        "exchanges": {
            "binance": {
                "rate_limit": 1200,
                "testnet": True
            },
            "deribit": {
                "rate_limit": 1000,
                "testnet": True
            }
        }
    }
    
    success = secret_manager.upload_config(
        config_name="trading",
        config_data=config_data,
        description="Trading configuration for all exchanges"
    )
    print(f"Trading config: {'✅ Success' if success else '❌ Failed'}")
    
    # Example 6: Retrieve configuration
    print("\n6. Retrieving Configuration:")
    print("-" * 30)
    
    retrieved_config = secret_manager.get_config("trading")
    if retrieved_config:
        print("Retrieved trading configuration:")
        import json
        print(json.dumps(retrieved_config, indent=2))
    else:
        print("No trading configuration found")
    
    print("\n" + "=" * 50)
    print("Secret Manager example complete!")
    print("This demonstrates how to manage API keys and secrets")
    print("in Google Cloud Secret Manager for secure storage.")

if __name__ == "__main__":
    main()