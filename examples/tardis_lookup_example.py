#!/usr/bin/env python3
"""
Tardis Lookup Example

Demonstrates how to use the instrument services to get data formatted
for Tardis tick data queries.
"""

import sys
import os
from datetime import datetime, timezone
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from market_data_tick_handler.instrument_services import InstrumentLister
from config import get_config

def main():
    """Demonstrate Tardis lookup functionality"""
    print("Tardis Lookup Example")
    print("=" * 50)
    
    # Initialize the instrument lister
    config = get_config()
    lister = InstrumentLister(config.gcp.bucket)
    
    date = datetime(2023, 8, 29, tzinfo=timezone.utc)
    
    # Example 1: Get BTC-USD options chain for specific expiry
    print("\n1. BTC-USD Options Chain for June 28, 2024:")
    print("-" * 50)
    
    result = lister.list_instruments(
        date=date,
        instrument_type='option',
        underlying='BTC-USD',
        expiry='2024-06-28',
        limit=5,
        format_type='tardis'
    )
    
    print(f"Found {result['total_instruments']} options in the chain")
    for inst in result['instruments']:
        print(f"  {inst['instrument_key']}")
        print(f"    Tardis Symbol: {inst['tardis_symbol']}")
        print(f"    Strike: {inst['strike']}, Type: {inst['option_type']}")
        print()
    
    # Example 2: Get ETH options for specific expiry
    print("\n2. ETH Options for June 28, 2024:")
    print("-" * 50)
    
    result = lister.list_instruments(
        date=date,
        instrument_type='option',
        base_asset='ETH',
        expiry='2024-06-28',
        limit=3,
        format_type='tardis'
    )
    
    print(f"Found {result['total_instruments']} ETH options")
    for inst in result['instruments']:
        print(f"  {inst['instrument_key']}")
        print(f"    Tardis Symbol: {inst['tardis_symbol']}")
        print(f"    Strike: {inst['strike']}, Type: {inst['option_type']}")
        print()
    
    # Example 3: Get statistics
    print("\n3. Instrument Statistics:")
    print("-" * 50)
    
    stats = lister.get_statistics(date)
    print(f"Total instruments: {stats['total_instruments']}")
    print(f"By venue: {stats['by_venue']}")
    print(f"By type: {stats['by_instrument_type']}")
    print(f"Top base assets: {stats['top_base_assets']}")
    
    # Example 4: Get JSON output for programmatic use
    print("\n4. JSON Output for Programmatic Use:")
    print("-" * 50)
    
    result = lister.list_instruments(
        date=date,
        instrument_type='option',
        underlying='BTC-USD',
        expiry='2024-06-28',
        limit=2,
        format_type='tardis'
    )
    
    print("JSON output:")
    import json
    print(json.dumps(result, indent=2, default=str))
    
    print("\n" + "=" * 50)
    print("Tardis lookup example complete!")
    print("This demonstrates how to use the instrument services")
    print("to get data formatted for Tardis tick data queries.")

if __name__ == "__main__":
    main()