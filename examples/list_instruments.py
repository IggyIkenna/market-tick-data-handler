 #!/usr/bin/env python3
"""
List Instrument Definitions for a Specific Date

A command-line utility to list all instrument definitions available for a specific date.
Returns a dictionary of all instrument keys and their data.

Usage:
    python examples/list_instruments.py --date 2023-08-29
    python examples/list_instruments.py --date 2023-08-29 --venue deribit
    python examples/list_instruments.py --date 2023-08-29 --instrument-type future
    python examples/list_instruments.py --date 2023-08-29 --base-asset BTC
    python examples/list_instruments.py --date 2023-08-29 --underlying BTC-USD
    python examples/list_instruments.py --date 2023-08-29 --expiry 2024-06-28
    python examples/list_instruments.py --date 2023-08-29 --instrument-type option --underlying ETH-USD --expiry 2024-06-28
    python examples/list_instruments.py --date 2023-08-29 --format json
    python examples/list_instruments.py --date 2023-08-29 --format table
    python examples/list_instruments.py --date 2023-08-29 --format tardis
    python examples/list_instruments.py --date 2023-08-29 --format tardis-json
    python examples/list_instruments.py --date 2023-08-29 --limit 10
"""

import sys
import os
import logging
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List
import pandas as pd

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import get_config
from market_data_tick_handler.instrument_services import InstrumentLister

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Note: All functionality is now provided by the InstrumentLister service

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='List instrument definitions for a specific date')
    parser.add_argument('--date', type=str, required=True, help='Date in YYYY-MM-DD format')
    parser.add_argument('--venue', type=str, help='Filter by venue (e.g., deribit, binance)')
    parser.add_argument('--instrument-type', type=str, help='Filter by instrument type (e.g., future, option, spot_pair)')
    parser.add_argument('--base-asset', type=str, help='Filter by base asset (e.g., BTC, ETH)')
    parser.add_argument('--quote-asset', type=str, help='Filter by quote asset (e.g., USDT, USDC, USD)')
    parser.add_argument('--underlying', type=str, help='Filter by underlying asset (e.g., BTC-USD, ETH-USD)')
    parser.add_argument('--expiry', type=str, help='Filter by expiry date (e.g., 2024-06-28, 240628)')
    parser.add_argument('--format', type=str, choices=['table', 'json', 'tardis', 'tardis-json'], default='table', help='Output format (table, json, tardis for Tardis table format, or tardis-json for Tardis JSON format)')
    parser.add_argument('--limit', type=int, help='Limit number of results shown')
    parser.add_argument('--stats', action='store_true', help='Show statistics summary')
    
    args = parser.parse_args()
    
    # Parse date
    try:
        target_date = datetime.strptime(args.date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    except ValueError:
        print(f"Error: Invalid date format '{args.date}'. Use YYYY-MM-DD format.")
        sys.exit(1)
    
    # Initialize instrument lister
    config = get_config()
    lister = InstrumentLister(config.gcp.bucket)
    
    try:
        # Show statistics if requested
        if args.stats:
            stats = lister.get_statistics(target_date)
            print(f"\n{'='*60}")
            print(f"INSTRUMENT STATISTICS")
            print(f"{'='*60}")
            print(f"Total instruments: {stats['total_instruments']}")
            
            print(f"\nBy Venue:")
            for venue, count in sorted(stats['by_venue'].items()):
                print(f"  {venue}: {count}")
            
            print(f"\nBy Instrument Type:")
            for inst_type, count in sorted(stats['by_instrument_type'].items()):
                print(f"  {inst_type}: {count}")
            
            print(f"\nTop 10 Base Assets:")
            for base_asset, count in sorted(stats['top_base_assets'].items(), key=lambda x: x[1], reverse=True)[:10]:
                print(f"  {base_asset}: {count}")
        else:
            # Determine format type and output method
            if args.format == 'tardis':
                format_type = 'tardis'
                output_method = 'table'
            elif args.format == 'tardis-json':
                format_type = 'tardis'
                output_method = 'json'
            elif args.format == 'json':
                format_type = 'full'
                output_method = 'json'
            else:  # table
                format_type = 'full'
                output_method = 'table'
            
            # Get instruments with filters applied
            logger.info(f"Loading instruments for {args.date}...")
            result = lister.list_instruments(
                date=target_date,
                venue=args.venue,
                instrument_type=args.instrument_type,
                base_asset=args.base_asset,
                quote_asset=args.quote_asset,
                underlying=args.underlying,
                expiry=args.expiry,
                limit=args.limit,
                format_type=format_type
            )
            
            if result['total_instruments'] == 0:
                print(f"No instruments found for date {args.date}")
                sys.exit(0)
            
            # Print results
            if output_method == 'json':
                print(json.dumps(result, indent=2, default=str))
            else:
                # For table display, we'll create a simple display from the dict data
                if result['instruments']:
                    # Create DataFrame for table display
                    data = []
                    for inst_data in result['instruments']:
                        if format_type == 'tardis':
                            data.append({
                                'Instrument Key': inst_data['instrument_key'],
                                'Tardis Symbol': inst_data['tardis_symbol'],
                                'Tardis Exchange': inst_data['tardis_exchange'],
                                'Data Types': inst_data.get('data_types', ''),
                                'Base': inst_data['base_asset'],
                                'Quote': inst_data['quote_asset'],
                                'Settle': inst_data.get('settle_asset', ''),
                                'Expiry': inst_data.get('expiry', '')[:10] if inst_data.get('expiry') else '',
                                'Strike': inst_data.get('strike', ''),
                                'Option Type': inst_data.get('option_type', ''),
                                'Inverse': inst_data.get('inverse', False)
                            })
                        else:
                            data.append({
                                'Instrument Key': inst_data['instrument_key'],
                                'Venue': inst_data['venue'],
                                'Type': inst_data['instrument_type'],
                                'Base': inst_data['base_asset'],
                                'Quote': inst_data['quote_asset'],
                                'Settle': inst_data.get('settle_asset', ''),
                                'Expiry': inst_data.get('expiry', '')[:10] if inst_data.get('expiry') else '',
                                'Strike': inst_data.get('strike', ''),
                                'Option Type': inst_data.get('option_type', ''),
                                'Inverse': inst_data.get('inverse', False),
                                'Data Types': inst_data.get('data_types', '')
                            })
                    
                    df = pd.DataFrame(data)
                    
                    # Set display options for better formatting
                    pd.set_option('display.max_columns', None)
                    pd.set_option('display.width', None)
                    pd.set_option('display.max_colwidth', 50)
                    
                    format_title = "TARDIS LOOKUP" if format_type == 'tardis' else "INSTRUMENT DEFINITIONS"
                    print(f"\n{'='*120}")
                    print(f"{format_title} - {len(data)} instruments found")
                    print(f"{'='*120}")
                    print(df.to_string(index=False))
                    
                    if args.limit and len(data) == args.limit:
                        print(f"\n... (showing first {args.limit} results, use --limit to see more)")
                else:
                    print("No instruments found matching the criteria.")
            
            # Show summary
            print(f"\nTotal instruments found: {result['total_instruments']}")
            if args.limit and result['total_instruments'] > args.limit:
                print(f"Showing first {args.limit} results. Use --limit to see more.")
        
    except Exception as e:
        logger.error(f"Error loading instruments: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
