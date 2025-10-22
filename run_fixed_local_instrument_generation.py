#!/usr/bin/env python3
"""
Fixed local instrument generation script
- Processes each day separately (correct for daily partitioning)
- Creates final aggregate after all days are processed
- Handles multiple exchanges and full date range
"""

import sys
import os
sys.path.insert(0, os.getcwd())

from datetime import datetime, timezone, timedelta
import pandas as pd
from src.instrument_processor.canonical_key_generator import CanonicalInstrumentKeyGenerator
from src.instrument_processor.gcs_uploader import InstrumentGCSUploader
from config import get_config

def main():
    print('🎯 STARTING FIXED LOCAL INSTRUMENT GENERATION')
    print('=============================================')
    
    # Load configuration
    config = get_config()
    
    # Initialize components
    generator = CanonicalInstrumentKeyGenerator(config.tardis.api_key)
    # Instrument keys emit BASE-QUOTE symbols (e.g., BTC-USDT) to keep storage uploads consistent.
    gcs_uploader = InstrumentGCSUploader(config.gcp.bucket)
    
    # Date range - start with 3 days for testing
    start_date = datetime(2023, 5, 23, tzinfo=timezone.utc)
    end_date = datetime(2023, 5, 23, tzinfo=timezone.utc)  # 3 days for testing
    
    # Exchanges to process 
    exchanges = ['binance', 'binance-futures', 'deribit', 'bybit', 'bybit-spot', 'okex', 'okex-futures', 'okex-swap']
    
    print(f'📅 Processing range: {start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")}')
    print(f'🏢 Exchanges: {exchanges}')
    print()
    
    results = {
        'total_days': 0,
        'processed_days': 0,
        'total_instruments': 0,
        'total_parsing_failures': 0,
        'errors': [],
        'all_instruments': []  # Collect all instruments for final aggregate
    }
    
    # Process each day
    current_date = start_date
    while current_date <= end_date:
        results['total_days'] += 1
        print(f'📅 Processing {current_date.strftime("%Y-%m-%d")}...')
        
        try:
            # Process all exchanges for this day
            daily_instruments = []
            daily_parsing_failures = 0
            for exchange in exchanges:
                print(f'  🏢 Processing {exchange}...')
                exchange_data, exchange_stats = generator.process_exchange_symbols(exchange, current_date, current_date)
                
                if exchange_data:
                    # Convert to DataFrame
                    df = pd.DataFrame(list(exchange_data.values()))
                    daily_instruments.append(df)
                    print(f'    ✅ Generated {len(df)} instruments')
                    if len(df) > 0:
                        print(f'    📊 Sample venue: {df["venue"].iloc[0]}')
                        print(f'    📊 Sample instrument_type: {df["instrument_type"].iloc[0]}')
                    
                    # Track parsing failures
                    if exchange_stats and exchange_stats.get('failed_parsing', 0) > 0:
                        daily_parsing_failures += exchange_stats['failed_parsing']
                else:
                    print(f'    ⚠️ No data for {exchange}')
            
            # Combine all exchanges for this day
            if daily_instruments:
                combined_df = pd.concat(daily_instruments, ignore_index=True)
                results['total_instruments'] += len(combined_df)
                
                print(f'  📊 Total instruments for {current_date.strftime("%Y-%m-%d")}: {len(combined_df)}')
                print(f'  🏢 Venues: {combined_df["venue"].unique()}')
                print(f'  📊 Types: {combined_df["instrument_type"].unique()}')
                
                # Upload to GCS with canonical venue partitioning (separate files per day)
                gcs_paths = gcs_uploader.upload_instrument_definitions(combined_df, current_date)
                print(f'  ✅ Uploaded {len(combined_df)} instruments to GCS')
                print(f'  📁 GCS paths: {len(gcs_paths)} partitions created')
                for path_name, path in gcs_paths.items():
                    print(f'    {path_name}: {path}')
                
                # Collect for final aggregate
                daily_instruments_copy = combined_df.copy()
                daily_instruments_copy['date'] = current_date.strftime('%Y-%m-%d')
                results['all_instruments'].append(daily_instruments_copy)
                
                # Add parsing failures to total
                results['total_parsing_failures'] += daily_parsing_failures
                
                results['processed_days'] += 1
            else:
                print(f'  ⚠️ No instruments generated for {current_date.strftime("%Y-%m-%d")}')
                
        except Exception as e:
            error_msg = f'Error processing {current_date.strftime("%Y-%m-%d")}: {e}'
            print(f'  ❌ {error_msg}')
            results['errors'].append(error_msg)
            import traceback
            traceback.print_exc()
        
        # Move to next day
        current_date = current_date + timedelta(days=1)
        
        print()
    
    # Create and upload final aggregate file (THIS WAS MISSING!)
    if results['all_instruments']:
        print(f'📦 Creating final aggregate file...')
        try:
            # Combine all instruments from all days
            all_instruments_df = pd.concat(results['all_instruments'], ignore_index=True)
            print(f'📊 Total instruments in aggregate: {len(all_instruments_df)}')
            
            # Upload aggregate using the existing method
            aggregate_path = gcs_uploader.upload_aggregate_definitions(
                all_instruments_df, start_date, end_date
            )
            print(f'✅ Final aggregate uploaded: {aggregate_path}')
            
        except Exception as e:
            print(f'❌ Failed to create final aggregate: {e}')
            results['errors'].append(f"Aggregate creation: {e}")
    
    # Final summary
    print('🎉 FIXED LOCAL INSTRUMENT GENERATION COMPLETED')
    print('==============================================')
    print(f'📊 Total days: {results["total_days"]}')
    print(f'✅ Processed days: {results["processed_days"]}')
    print(f'📈 Total instruments: {results["total_instruments"]}')
    print(f'⚠️ Parsing failures: {results["total_parsing_failures"]}')
    print(f'❌ Errors: {len(results["errors"])}')
    
    if results['errors']:
        print('\nError details:')
        for error in results['errors'][:5]:  # Show first 5 errors
            print(f'  - {error}')
    
    return results

if __name__ == '__main__':
    main()
