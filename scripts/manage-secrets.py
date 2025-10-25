#!/usr/bin/env python3
"""
Interactive Secret Management CLI

This script provides an interactive command-line interface for managing
secrets in Google Cloud Secret Manager.
"""

import sys
import os
import json
import argparse
from pathlib import Path
from typing import Dict, Any, Optional

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from market_data_tick_handler.utils.secret_manager_utils import SecretManagerUtils
from config import get_config


class SecretManagerCLI:
    """Interactive CLI for managing secrets"""
    
    def __init__(self, project_id: str):
        self.sm = SecretManagerUtils(project_id)
        self.project_id = project_id
    
    def list_secrets(self, secret_type: str = "all"):
        """List all secrets of a specific type"""
        print(f"\n📋 Listing {secret_type} secrets in project: {self.project_id}")
        print("-" * 60)
        
        if secret_type in ["all", "api-keys"]:
            api_keys = self.sm.list_api_keys()
            print(f"API Keys ({len(api_keys)}):")
            for key in api_keys:
                print(f"  • {key}")
        
        if secret_type in ["all", "trading-keys"]:
            trading_keys = self.sm.list_trading_keys()
            print(f"Trading Keys ({len(trading_keys)}):")
            for key in trading_keys:
                print(f"  • {key}")
        
        if secret_type in ["all", "configs"]:
            configs = self.sm.list_configs()
            print(f"Configurations ({len(configs)}):")
            for config in configs:
                print(f"  • {config}")
    
    def upload_api_key(self, service_name: str, api_key: str, description: str = ""):
        """Upload an API key"""
        print(f"\n🔑 Uploading API key for {service_name}...")
        
        success = self.sm.upload_api_key(service_name, api_key, description)
        if success:
            print(f"✅ Successfully uploaded API key for {service_name}")
        else:
            print(f"❌ Failed to upload API key for {service_name}")
        
        return success
    
    def upload_trading_keys(self, exchange: str, keys: Dict[str, Any], description: str = ""):
        """Upload trading keys"""
        print(f"\n🔐 Uploading trading keys for {exchange}...")
        
        success = self.sm.upload_trading_keys(exchange, keys, description)
        if success:
            print(f"✅ Successfully uploaded trading keys for {exchange}")
        else:
            print(f"❌ Failed to upload trading keys for {exchange}")
        
        return success
    
    def upload_config(self, config_name: str, config_data: Dict[str, Any], description: str = ""):
        """Upload configuration"""
        print(f"\n⚙️ Uploading configuration {config_name}...")
        
        success = self.sm.upload_config(config_name, config_data, description)
        if success:
            print(f"✅ Successfully uploaded configuration {config_name}")
        else:
            print(f"❌ Failed to upload configuration {config_name}")
        
        return success
    
    def get_secret(self, secret_type: str, name: str):
        """Retrieve a secret"""
        print(f"\n🔍 Retrieving {secret_type}: {name}")
        print("-" * 40)
        
        try:
            if secret_type == "api-key":
                secret = self.sm.get_api_key(name)
                if secret:
                    # Mask the key for display
                    masked = secret[:8] + "..." + secret[-4:] if len(secret) > 12 else "***"
                    print(f"API Key: {masked}")
                else:
                    print("❌ API key not found")
            
            elif secret_type == "trading-keys":
                secret = self.sm.get_trading_keys(name)
                if secret:
                    print("Trading Keys:")
                    for key, value in secret.items():
                        if "key" in key.lower() or "secret" in key.lower():
                            masked = str(value)[:8] + "..." + str(value)[-4:] if len(str(value)) > 12 else "***"
                            print(f"  {key}: {masked}")
                        else:
                            print(f"  {key}: {value}")
                else:
                    print("❌ Trading keys not found")
            
            elif secret_type == "config":
                secret = self.sm.get_config(name)
                if secret:
                    print("Configuration:")
                    print(json.dumps(secret, indent=2))
                else:
                    print("❌ Configuration not found")
            
            else:
                print(f"❌ Unknown secret type: {secret_type}")
        
        except Exception as e:
            print(f"❌ Error retrieving secret: {e}")
    
    def test_connection(self):
        """Test Secret Manager connection"""
        print(f"\n🔌 Testing Secret Manager connection...")
        print(f"Project ID: {self.project_id}")
        
        try:
            # Try to list secrets
            api_keys = self.sm.list_api_keys()
            print(f"✅ Connection successful! Found {len(api_keys)} API keys")
            return True
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False
    
    def interactive_mode(self):
        """Run in interactive mode"""
        print("🔐 Secret Manager CLI - Interactive Mode")
        print("=" * 50)
        
        while True:
            print("\nOptions:")
            print("1. List secrets")
            print("2. Upload API key")
            print("3. Upload trading keys")
            print("4. Upload configuration")
            print("5. Get secret")
            print("6. Test connection")
            print("7. Exit")
            
            choice = input("\nEnter your choice (1-7): ").strip()
            
            if choice == "1":
                secret_type = input("Secret type (all/api-keys/trading-keys/configs): ").strip() or "all"
                self.list_secrets(secret_type)
            
            elif choice == "2":
                service_name = input("Service name: ").strip()
                api_key = input("API key: ").strip()
                description = input("Description (optional): ").strip()
                self.upload_api_key(service_name, api_key, description)
            
            elif choice == "3":
                exchange = input("Exchange name: ").strip()
                print("Enter trading keys (press Enter for empty values):")
                keys = {}
                keys["api_key"] = input("API key: ").strip()
                keys["secret_key"] = input("Secret key: ").strip()
                testnet = input("Testnet (true/false): ").strip().lower() == "true"
                keys["testnet"] = testnet
                description = input("Description (optional): ").strip()
                self.upload_trading_keys(exchange, keys, description)
            
            elif choice == "4":
                config_name = input("Configuration name: ").strip()
                print("Enter configuration JSON (or press Enter to skip):")
                config_json = input().strip()
                if config_json:
                    try:
                        config_data = json.loads(config_json)
                        description = input("Description (optional): ").strip()
                        self.upload_config(config_name, config_data, description)
                    except json.JSONDecodeError:
                        print("❌ Invalid JSON format")
                else:
                    print("Skipping configuration upload")
            
            elif choice == "5":
                secret_type = input("Secret type (api-key/trading-keys/config): ").strip()
                name = input("Secret name: ").strip()
                self.get_secret(secret_type, name)
            
            elif choice == "6":
                self.test_connection()
            
            elif choice == "7":
                print("👋 Goodbye!")
                break
            
            else:
                print("❌ Invalid choice. Please try again.")


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(description="Secret Manager CLI")
    parser.add_argument("--project-id", help="GCP Project ID")
    parser.add_argument("--interactive", "-i", action="store_true", help="Run in interactive mode")
    parser.add_argument("--list", choices=["all", "api-keys", "trading-keys", "configs"], help="List secrets")
    parser.add_argument("--test", action="store_true", help="Test connection")
    
    # Upload commands
    parser.add_argument("--upload-api-key", nargs=2, metavar=("SERVICE", "KEY"), help="Upload API key")
    parser.add_argument("--upload-trading-keys", help="Upload trading keys (JSON file)")
    parser.add_argument("--upload-config", help="Upload configuration (JSON file)")
    
    # Get commands
    parser.add_argument("--get-api-key", help="Get API key")
    parser.add_argument("--get-trading-keys", help="Get trading keys")
    parser.add_argument("--get-config", help="Get configuration")
    
    args = parser.parse_args()
    
    # Get project ID
    project_id = args.project_id
    if not project_id:
        try:
            config = get_config()
            project_id = config.gcp.project_id
        except Exception:
            project_id = os.getenv('GCP_PROJECT_ID')
    
    if not project_id:
        print("❌ Project ID not found. Set GCP_PROJECT_ID environment variable or use --project-id")
        sys.exit(1)
    
    # Initialize CLI
    try:
        cli = SecretManagerCLI(project_id)
    except Exception as e:
        print(f"❌ Failed to initialize Secret Manager: {e}")
        sys.exit(1)
    
    # Handle commands
    if args.interactive:
        cli.interactive_mode()
    
    elif args.list:
        cli.list_secrets(args.list)
    
    elif args.test:
        success = cli.test_connection()
        sys.exit(0 if success else 1)
    
    elif args.upload_api_key:
        service, key = args.upload_api_key
        cli.upload_api_key(service, key)
    
    elif args.upload_trading_keys:
        with open(args.upload_trading_keys, 'r') as f:
            keys = json.load(f)
        exchange = input("Exchange name: ").strip()
        cli.upload_trading_keys(exchange, keys)
    
    elif args.upload_config:
        with open(args.upload_config, 'r') as f:
            config_data = json.load(f)
        config_name = input("Configuration name: ").strip()
        cli.upload_config(config_name, config_data)
    
    elif args.get_api_key:
        cli.get_secret("api-key", args.get_api_key)
    
    elif args.get_trading_keys:
        cli.get_secret("trading-keys", args.get_trading_keys)
    
    elif args.get_config:
        cli.get_secret("config", args.get_config)
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
