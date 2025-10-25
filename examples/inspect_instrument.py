#!/usr/bin/env python3
"""
Instrument Inspection Tool

A command-line utility to inspect instrument definitions from GCS.
Users can specify an instrument ID and date to get full attribute details.

Usage:
    python examples/inspect_instrument.py BINANCE-FUTURES:PERPETUAL:SOL-USDT
    python examples/inspect_instrument.py BINANCE-FUTURES:PERPETUAL:SOL-USDT --date 2023-05-23
    python examples/inspect_instrument.py DERIBIT:OPTION:BTC-USD-241225-50000-CALL --date 2023-05-23
"""

import sys
import os
import logging
import argparse
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import get_config
from market_data_tick_handler.instrument_services import InstrumentInspector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Note: All functionality is now provided by the InstrumentInspector service

def main():
    """Main function with command-line argument parsing"""
    parser = argparse.ArgumentParser(
        description='Inspect instrument definitions from GCS',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python examples/inspect_instrument.py BINANCE-FUTURES:PERPETUAL:SOL-USDT
  python examples/inspect_instrument.py BINANCE-FUTURES:PERPETUAL:SOL-USDT --date 2023-05-23
  python examples/inspect_instrument.py DERIBIT:OPTION:BTC-USD-241225-50000-CALL --date 2023-05-23
  python examples/inspect_instrument.py BINANCE:SPOT_PAIR:BTC-USDT --date 2023-05-23 --summary
        """
    )
    
    parser.add_argument(
        'instrument_id',
        help='Instrument ID to inspect (e.g., BINANCE-FUTURES:PERPETUAL:SOL-USDT)'
    )
    
    parser.add_argument(
        '--date',
        type=str,
        default='2023-05-23',
        help='Date to query instruments for (YYYY-MM-DD format, default: 2023-05-23)'
    )
    
    parser.add_argument(
        '--summary',
        action='store_true',
        help='Show summary of all instruments for the date'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Parse date
    try:
        target_date = datetime.strptime(args.date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    except ValueError:
        logger.error(f"Invalid date format: {args.date}. Use YYYY-MM-DD format.")
        sys.exit(1)
    
    # Run inspection
    try:
        # Initialize inspector
        config = get_config()
        inspector = InstrumentInspector(config.gcp.bucket)
        
        # Inspect instrument
        result = inspector.inspect_instrument(
            instrument_id=args.instrument_id,
            date=target_date,
            show_summary=args.summary
        )
        
        if result['success']:
            print(result['formatted_attributes'])
            print(f"\n✅ Instrument validated successfully using Pydantic model")
        else:
            print(f"❌ Error: {result['error']}")
            
            if 'similar_instruments' in result and result['similar_instruments']:
                print(f"\nSimilar instruments found:")
                for similar in result['similar_instruments']:
                    print(f"  - {similar}")
            
            if 'summary' in result and result['summary']:
                summary = result['summary']
                print(f"\n{'='*80}")
                print(f"INSTRUMENT DEFINITIONS FOR {target_date.strftime('%Y-%m-%d')}")
                print(f"{'='*80}")
                print(f"Total instruments: {summary['total_instruments']}")
                print(f"Columns available: {summary['columns_available']}")
                
                if summary['by_venue_type']:
                    print(f"\nSummary by venue and instrument type:")
                    for (venue, inst_type), count in summary['by_venue_type'].items():
                        print(f"  {venue} {inst_type}: {count}")
            
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Script failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

