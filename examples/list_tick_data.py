#!/usr/bin/env python3
"""
Tick Data Catalog Script

Lists existing tick data in GCS (inverse of missing-report mode).
Shows what data is available with file sizes, row counts, and metadata.

Usage:
    python examples/list_tick_data.py --start-date 2023-05-23 --end-date 2023-05-25
    python examples/list_tick_data.py --start-date 2023-05-23 --end-date 2023-05-23 \
      --venues deribit --data-types trades book_snapshot_5
    python examples/list_tick_data.py --start-date 2023-05-23 --end-date 2023-05-23 \
      --output tick_data_catalog.csv --format csv
    python examples/list_tick_data.py --start-date 2023-05-23 --end-date 2023-05-23 \
      --detailed --show-sizes
"""

import argparse
import asyncio
import logging
import pandas as pd
import sys
import io
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional
import json

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config import get_config
from market_data_tick_handler.data_client.data_client import DataClient

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TickDataCatalog:
    """Lists and catalogs existing tick data in GCS"""
    
    def __init__(self, config):
        self.config = config
        self.data_client = DataClient(config.gcp.bucket, config)
        self.gcs_bucket = "market-data-tick"  # Tick data bucket
    
    async def list_tick_data(
        self,
        start_date: datetime,
        end_date: datetime,
        venues: Optional[List[str]] = None,
        data_types: Optional[List[str]] = None,
        detailed: bool = False,
        show_sizes: bool = False
    ) -> Dict[str, Any]:
        """
        List existing tick data in GCS
        
        Args:
            start_date: Start date for catalog
            end_date: End date for catalog
            venues: Optional list of venues to filter
            data_types: Optional list of data types to filter
            detailed: Show detailed information
            show_sizes: Show file sizes and row counts
            
        Returns:
            Dictionary with catalog data
        """
        logger.info(f"📊 Cataloging tick data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        # Default data types if not provided
        if data_types is None:
            data_types = ['trades', 'book_snapshot_5', 'derivative_ticker', 'liquidations', 'options_chain']
        
        catalog = {
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
            'venues': venues or 'all',
            'data_types': data_types,
            'total_files': 0,
            'total_size_bytes': 0,
            'total_rows': 0,
            'date_coverage': {},
            'venue_coverage': {},
            'data_type_coverage': {},
            'files': []
        }
        
        # Process each day
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            logger.info(f"📅 Processing {date_str}")
            
            day_data = {
                'date': date_str,
                'venues': {},
                'total_files': 0,
                'total_size_bytes': 0,
                'total_rows': 0
            }
            
            # Process each data type
            for data_type in data_types:
                if data_type not in day_data['venues']:
                    day_data['venues'][data_type] = {}
                
                # Get files for this date and data type
                files = await self._get_tick_data_files(date_str, data_type, venues)
                
                for file_info in files:
                    # Extract venue and instrument from file path
                    venue, instrument = self._parse_file_path(file_info['blob_name'])
                    
                    if venues and venue not in venues:
                        continue
                    
                    if venue not in day_data['venues'][data_type]:
                        day_data['venues'][data_type][venue] = []
                    
                    # Get additional metadata if requested
                    if show_sizes or detailed:
                        file_info = await self._enrich_file_info(file_info)
                    
                    day_data['venues'][data_type][venue].append(file_info)
                    day_data['total_files'] += 1
                    day_data['total_size_bytes'] += file_info.get('file_size', 0)
                    day_data['total_rows'] += file_info.get('row_count', 0)
            
            # Update catalog
            catalog['date_coverage'][date_str] = day_data
            catalog['total_files'] += day_data['total_files']
            catalog['total_size_bytes'] += day_data['total_size_bytes']
            catalog['total_rows'] += day_data['total_rows']
            
            current_date += timedelta(days=1)
        
        # Calculate coverage statistics
        total_days = (end_date - start_date).days + 1
        catalog['coverage_percentage'] = (len(catalog['date_coverage']) / total_days) * 100
        
        return catalog
    
    async def _get_tick_data_files(self, date_str: str, data_type: str, venues: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get tick data files for a specific date and data type"""
        files = []
        
        # Construct GCS path pattern
        blob_pattern = f"raw_tick_data/by_date/day-{date_str}/data_type-{data_type}/"
        
        try:
            # List blobs matching the pattern
            blobs = list(self.data_client.client.list_blobs(self.gcs_bucket, prefix=blob_pattern))
            
            for blob in blobs:
                if blob.name.endswith('.csv.zst') or blob.name.endswith('.parquet'):
                    file_info = {
                        'blob_name': blob.name,
                        'file_path': f"gs://{self.gcs_bucket}/{blob.name}",
                        'file_size': blob.size or 0,
                        'created': blob.time_created,
                        'data_type': data_type,
                        'date': date_str
                    }
                    files.append(file_info)
                    
        except Exception as e:
            logger.warning(f"Failed to list files for {date_str}/{data_type}: {e}")
        
        return files
    
    def _parse_file_path(self, blob_name: str) -> tuple[str, str]:
        """Parse venue and instrument from blob name"""
        # Example: raw_tick_data/by_date/day-2023-05-23/data_type-trades/DERIBIT_BTC-PERPETUAL_trades_2023-05-23.parquet
        parts = blob_name.split('/')
        if len(parts) >= 4:
            filename = parts[-1]
            # Extract venue and instrument from filename
            if '_' in filename:
                venue_instrument = filename.split('_')[0]  # DERIBIT_BTC-PERPETUAL
                if '_' in venue_instrument:
                    venue, instrument = venue_instrument.split('_', 1)
                    return venue, instrument
        
        return "unknown", "unknown"
    
    async def _enrich_file_info(self, file_info: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich file info with additional metadata"""
        try:
            # Try to get row count from parquet metadata
            if file_info['blob_name'].endswith('.parquet'):
                blob = self.data_client.bucket.blob(file_info['blob_name'])
                parquet_data = blob.download_as_bytes()
                df = pd.read_parquet(io.BytesIO(parquet_data))
                file_info['row_count'] = len(df)
                file_info['columns'] = list(df.columns)
            else:
                file_info['row_count'] = 0
                file_info['columns'] = []
        except Exception as e:
            logger.warning(f"Failed to enrich file info for {file_info['blob_name']}: {e}")
            file_info['row_count'] = 0
            file_info['columns'] = []
        
        return file_info
    
    def format_catalog_output(self, catalog: Dict[str, Any], detailed: bool = False, show_sizes: bool = False) -> str:
        """Format catalog data for display"""
        output = []
        
        # Header
        output.append(f"📊 Tick Data Catalog ({catalog['start_date']} to {catalog['end_date']})")
        output.append("=" * 60)
        output.append("")
        
        # Summary
        total_size_mb = catalog['total_size_bytes'] / (1024 * 1024)
        output.append(f"Summary:")
        output.append(f"- Total files: {catalog['total_files']:,}")
        output.append(f"- Total size: {total_size_mb:.2f} MB")
        output.append(f"- Total rows: {catalog['total_rows']:,}")
        output.append(f"- Date coverage: {catalog['coverage_percentage']:.1f}%")
        output.append(f"- Venues: {', '.join(catalog['venues']) if isinstance(catalog['venues'], list) else 'all'}")
        output.append(f"- Data types: {', '.join(catalog['data_types'])}")
        output.append("")
        
        # Detailed breakdown by date
        if detailed:
            for date_str, day_data in catalog['date_coverage'].items():
                if day_data['total_files'] == 0:
                    continue
                
                output.append(f"Date: {date_str}")
                for data_type, venues in day_data['venues'].items():
                    if not venues:
                        continue
                    
                    output.append(f"  Data Type: {data_type}")
                    for venue, files in venues.items():
                        if not files:
                            continue
                        
                        output.append(f"    Venue: {venue}")
                        for file_info in files:
                            instrument = file_info.get('instrument', 'unknown')
                            size_mb = file_info.get('file_size', 0) / (1024 * 1024)
                            rows = file_info.get('row_count', 0)
                            
                            if show_sizes and rows > 0:
                                output.append(f"      ✅ {instrument} ({size_mb:.2f} MB, {rows:,} rows)")
                            elif show_sizes:
                                output.append(f"      ✅ {instrument} ({size_mb:.2f} MB)")
                            else:
                                output.append(f"      ✅ {instrument}")
                output.append("")
        
        return "\n".join(output)
    
    def export_catalog(self, catalog: Dict[str, Any], output_file: str, format: str = 'csv') -> None:
        """Export catalog to file"""
        if format.lower() == 'csv':
            # Flatten catalog for CSV export
            rows = []
            for date_str, day_data in catalog['date_coverage'].items():
                for data_type, venues in day_data['venues'].items():
                    for venue, files in venues.items():
                        for file_info in files:
                            rows.append({
                                'date': date_str,
                                'data_type': data_type,
                                'venue': venue,
                                'instrument': file_info.get('instrument', 'unknown'),
                                'file_size_bytes': file_info.get('file_size', 0),
                                'file_size_mb': file_info.get('file_size', 0) / (1024 * 1024),
                                'row_count': file_info.get('row_count', 0),
                                'file_path': file_info.get('file_path', ''),
                                'created': file_info.get('created', '')
                            })
            
            df = pd.DataFrame(rows)
            df.to_csv(output_file, index=False)
            logger.info(f"📄 Catalog exported to {output_file}")
            
        elif format.lower() == 'json':
            with open(output_file, 'w') as f:
                json.dump(catalog, f, indent=2, default=str)
            logger.info(f"📄 Catalog exported to {output_file}")
        
        else:
            raise ValueError(f"Unsupported format: {format}")

async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='List existing tick data in GCS',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all available tick data
  python examples/list_tick_data.py --start-date 2023-05-23 --end-date 2023-05-25

  # Filter by venue and data type
  python examples/list_tick_data.py --start-date 2023-05-23 --end-date 2023-05-23 \\
    --venues deribit --data-types trades book_snapshot_5

  # Export to CSV
  python examples/list_tick_data.py --start-date 2023-05-23 --end-date 2023-05-23 \\
    --output tick_data_catalog.csv --format csv

  # Show detailed stats with file sizes
  python examples/list_tick_data.py --start-date 2023-05-23 --end-date 2023-05-23 \\
    --detailed --show-sizes
        """
    )
    
    # Required arguments
    parser.add_argument('--start-date', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True, help='End date (YYYY-MM-DD)')
    
    # Optional filters
    parser.add_argument('--venues', nargs='+', help='Venues to filter (e.g., deribit binance)')
    parser.add_argument('--data-types', nargs='+', help='Data types to filter (e.g., trades book_snapshot_5)')
    
    # Output options
    parser.add_argument('--detailed', action='store_true', help='Show detailed breakdown')
    parser.add_argument('--show-sizes', action='store_true', help='Show file sizes and row counts')
    parser.add_argument('--output', help='Output file path')
    parser.add_argument('--format', choices=['csv', 'json'], default='csv', help='Output format')
    
    args = parser.parse_args()
    
    # Parse dates
    try:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    except ValueError as e:
        logger.error(f"Invalid date format: {e}")
        sys.exit(1)
    
    if start_date > end_date:
        logger.error("Start date must be before or equal to end date")
        sys.exit(1)
    
    # Load configuration
    try:
        config = get_config()
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        sys.exit(1)
    
    # Create catalog
    catalog_client = TickDataCatalog(config)
    
    try:
        # Generate catalog
        catalog = await catalog_client.list_tick_data(
            start_date=start_date,
            end_date=end_date,
            venues=args.venues,
            data_types=args.data_types,
            detailed=args.detailed,
            show_sizes=args.show_sizes
        )
        
        # Display results
        print(catalog_client.format_catalog_output(catalog, args.detailed, args.show_sizes))
        
        # Export if requested
        if args.output:
            catalog_client.export_catalog(catalog, args.output, args.format)
        
    except Exception as e:
        logger.error(f"Failed to generate catalog: {e}")
        sys.exit(1)

if __name__ == '__main__':
    asyncio.run(main())
