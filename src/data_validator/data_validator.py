"""
Data Validator Module

Function 4: Create missing data check comparing instrument definitions 
vs availability of data in GCS for those instruments over a query timeframe.
"""

import pandas as pd
from datetime import datetime, timezone, timedelta
from google.cloud import storage
import logging

logger = logging.getLogger(__name__)

class DataValidator:
    """Validates data availability in GCS against instrument definitions"""
    
    def __init__(self, gcs_bucket: str):
        self.gcs_bucket = gcs_bucket
        self.client = storage.Client()
        self.bucket = self.client.bucket(gcs_bucket)
    
    def check_missing_data(self, start_date: datetime, end_date: datetime,
                          venues: list = None, instrument_types: list = None) -> pd.DataFrame:
        """
        Check for missing data by comparing instrument definitions vs GCS availability
        
        Args:
            start_date: Start date for the check
            end_date: End date for the check
            venues: Optional venue filter
            instrument_types: Optional instrument type filter
            
        Returns:
            DataFrame with missing data analysis
        """
        logger.info(f"Checking missing data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        missing_data = []
        current_date = start_date
        
        while current_date <= end_date:
            logger.info(f"Checking data for {current_date.strftime('%Y-%m-%d')}")
            
            # Get expected instruments for this date
            expected_instruments_df = self._get_expected_instruments(current_date, venues, instrument_types)
            
            if expected_instruments_df.empty:
                continue
                
            expected_instruments = expected_instruments_df['instrument_key'].tolist()
            
            # Check what data is actually available in GCS
            available_data = self._get_available_data(current_date, venues, instrument_types)
            
            # Find missing data
            missing = self._find_missing_data(expected_instruments, available_data, current_date)
            missing_data.extend(missing)
            
            current_date += timedelta(days=1)
        
        if missing_data:
            df = pd.DataFrame(missing_data)
            logger.info(f"Found {len(df)} missing data entries")
        else:
            df = pd.DataFrame()
            logger.info("No missing data found")
        
        return df
    
    def _get_expected_instruments(self, date: datetime, venues: list = None, 
                                instrument_types: list = None) -> pd.DataFrame:
        """Get instruments that should have data for a given date with their data types"""
        # Try multiple paths for instrument definitions (fallback strategy)
        paths_to_try = [
            # Optimized single partition strategy
            f"instrument_availability/by_date/day-{date.strftime('%Y-%m-%d')}/instruments.parquet",
            # Legacy daily aggregated file
            f"instrument_availability/instruments_{date.strftime('%Y%m%d')}.parquet",
            # Legacy enhanced file
            f"instrument_availability/{date.strftime('%Y-%m-%d')}_enhanced.parquet"
        ]
        
        for path in paths_to_try:
            try:
                blob = self.bucket.blob(path)
                if blob.exists():
                    logger.info(f"Reading instrument definitions from: {path}")
                    import tempfile
                    with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp_file:
                        blob.download_to_filename(tmp_file.name)
                        df = pd.read_parquet(tmp_file.name)
                        
                        # Apply filters
                        if venues:
                            df = df[df['venue'].isin(venues)]
                        if instrument_types:
                            df = df[df['instrument_type'].isin(instrument_types)]
                        
                        logger.info(f"Found {len(df)} expected instruments for {date.strftime('%Y-%m-%d')}")
                        return df
            except Exception as e:
                logger.warning(f"Could not read from {path}: {e}")
                continue
        
        logger.warning(f"Could not find instrument definitions for {date.strftime('%Y-%m-%d')}")
        return pd.DataFrame()
    
    def _get_available_data(self, date: datetime, venues: list = None, 
                           instrument_types: list = None) -> list:
        """Get instruments that actually have data in GCS for a given date"""
        available_instruments = []
        
        # Check raw_tick_data partition using current optimized single partition strategy
        # Pattern: raw_tick_data/by_date/day-{date}/data_type-{type}/{instrument_key}.parquet
        date_str = date.strftime('%Y-%m-%d')
        prefix = f"raw_tick_data/by_date/day-{date_str}/"
        
        blobs = list(self.bucket.list_blobs(prefix=prefix))
        
        for blob in blobs:
            if blob.name.endswith('.parquet'):
                # Extract instrument key from path
                # Path format: raw_tick_data/by_date/day-2024-01-15/data_type-trades/{instrument_key}.parquet
                parts = blob.name.split('/')
                if len(parts) >= 5 and parts[3].startswith('data_type-'):
                    # The instrument key is the filename without .parquet extension
                    instrument_key = parts[4].replace('.parquet', '')
                    available_instruments.append(instrument_key)
        
        return list(set(available_instruments))  # Remove duplicates
    
    def _find_missing_data(self, expected: list, available: list, date: datetime) -> list:
        """Find instruments that are expected but not available"""
        missing = []
        
        for instrument_key in expected:
            if instrument_key not in available:
                missing.append({
                    'date': date.strftime('%Y-%m-%d'),
                    'instrument_key': instrument_key,
                    'status': 'missing',
                    'expected': True,
                    'available': False
                })
        
        return missing
    
    def check_missing_data_by_type(self, start_date: datetime, end_date: datetime,
                                 venues: list = None, instrument_types: list = None,
                                 data_types: list = None) -> pd.DataFrame:
        """
        Check for missing data by data type (trades, book_snapshot_5, etc.)
        
        Args:
            start_date: Start date for the check
            end_date: End date for the check
            venues: Optional venue filter
            instrument_types: Optional instrument type filter
            data_types: List of data types to check (e.g., ['trades', 'book_snapshot_5'])
            
        Returns:
            DataFrame with missing data analysis by type
        """
        if data_types is None:
            # Dynamically determine data types from instrument definitions
            data_types = self._get_all_available_data_types(start_date, end_date, venues, instrument_types)
        
        logger.info(f"Checking missing data by type from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        logger.info(f"Data types to check: {data_types}")
        
        missing_data = []
        current_date = start_date
        
        while current_date <= end_date:
            logger.info(f"Checking data for {current_date.strftime('%Y-%m-%d')}")
            
            # Get expected instruments for this date with their data types
            expected_instruments_df = self._get_expected_instruments(current_date, venues, instrument_types)
            
            if expected_instruments_df.empty:
                current_date += timedelta(days=1)
                continue
            
            # Check what data is actually available in GCS for each data type
            for data_type in data_types:
                available_data = self._get_available_data_by_type(current_date, data_type, venues, instrument_types)
                
                # Only check instruments that should have this data type using exact matching
                # Parse comma-separated data types and check for exact matches
                instruments_with_data_type = []
                for _, row in expected_instruments_df.iterrows():
                    if pd.notna(row['data_types']):
                        # Split comma-separated data types and strip whitespace
                        instrument_data_types = [dt.strip() for dt in str(row['data_types']).split(',')]
                        if data_type in instrument_data_types:
                            instruments_with_data_type.append(row['instrument_key'])
                
                logger.info(f"Checking {data_type}: {len(instruments_with_data_type)} instruments expected, {len(available_data)} available")
                
                # Find missing data for this type
                missing = self._find_missing_data_by_type(instruments_with_data_type, available_data, current_date, data_type)
                missing_data.extend(missing)
            
            current_date += timedelta(days=1)
        
        if missing_data:
            df = pd.DataFrame(missing_data)
            logger.info(f"Found {len(df)} missing data entries")
        else:
            df = pd.DataFrame()
            logger.info("No missing data found")
        
        return df
    
    def _get_available_data_by_type(self, date: datetime, data_type: str, 
                                  venues: list = None, instrument_types: list = None) -> list:
        """Get instruments that actually have data in GCS for a specific data type and date"""
        available_instruments = []
        
        # Check raw_tick_data partition for specific data type
        date_str = date.strftime('%Y-%m-%d')
        prefix = f"raw_tick_data/by_date/day-{date_str}/data_type-{data_type}/"
        
        blobs = list(self.bucket.list_blobs(prefix=prefix))
        
        for blob in blobs:
            if blob.name.endswith('.parquet'):
                # Extract instrument key from path
                # Path format: raw_tick_data/by_date/day-2024-01-15/data_type-trades/{instrument_key}.parquet
                parts = blob.name.split('/')
                if len(parts) >= 5 and parts[3] == f'data_type-{data_type}':
                    # The instrument key is the filename without .parquet extension
                    instrument_key = parts[4].replace('.parquet', '')
                    available_instruments.append(instrument_key)
        
        return list(set(available_instruments))  # Remove duplicates
    
    def _find_missing_data_by_type(self, expected: list, available: list, date: datetime, data_type: str) -> list:
        """Find instruments that are expected but not available for a specific data type"""
        missing = []
        
        for instrument_key in expected:
            if instrument_key not in available:
                missing.append({
                    'date': date.strftime('%Y-%m-%d'),
                    'instrument_key': instrument_key,
                    'data_type': data_type,
                    'status': 'missing',
                    'expected': True,
                    'available': False
                })
        
        return missing
    
    def _get_all_available_data_types(self, start_date: datetime, end_date: datetime,
                                    venues: list = None, instrument_types: list = None) -> list:
        """Get all unique data types available across all instruments in the date range"""
        all_data_types = set()
        
        current_date = start_date
        while current_date <= end_date:
            # Get instrument definitions for this date
            instruments_df = self._get_expected_instruments(current_date, venues, instrument_types)
            
            if not instruments_df.empty:
                # Extract all data types from the data_types column
                for data_types_str in instruments_df['data_types'].dropna():
                    if isinstance(data_types_str, str):
                        # Split comma-separated data types and add to set
                        types = [dt.strip() for dt in data_types_str.split(',')]
                        all_data_types.update(types)
            
            current_date += timedelta(days=1)
        
        # Convert to sorted list for consistent ordering
        return sorted(list(all_data_types))
    
    def generate_missing_data_report(self, start_date: datetime, end_date: datetime,
                                   venues: list = None, instrument_types: list = None,
                                   data_types: list = None, upload_to_gcs: bool = True) -> dict:
        """Generate a comprehensive missing data report"""
        logger.info("Generating missing data report")
        
        # Always use check_missing_data_by_type to get data type breakdown
        missing_df = self.check_missing_data_by_type(start_date, end_date, venues, instrument_types, data_types)
        
        if missing_df.empty:
            return {
                'status': 'complete',
                'missing_count': 0,
                'coverage_percentage': 100.0,
                'summary': 'All expected data is available'
            }
        
        # Calculate statistics
        total_days = (end_date - start_date).days + 1
        missing_count = len(missing_df)
        
        # Group by date to see daily coverage
        daily_coverage = missing_df.groupby('date').size().reset_index(name='missing_count')
        
        # Group by instrument to see which instruments are most problematic
        instrument_coverage = missing_df.groupby('instrument_key').size().reset_index(name='missing_days')
        
        report = {
            'status': 'incomplete',
            'missing_count': missing_count,
            'total_days': total_days,
            'coverage_percentage': ((total_days - missing_count) / total_days) * 100,
            'daily_coverage': daily_coverage.to_dict('records'),
            'instrument_coverage': instrument_coverage.to_dict('records'),
            'summary': f"Found {missing_count} missing data entries across {total_days} days"
        }
        
        # Add data type breakdown if checking by type
        if data_types and 'data_type' in missing_df.columns:
            data_type_coverage = missing_df.groupby('data_type').size().reset_index(name='missing_count')
            report['data_type_coverage'] = data_type_coverage.to_dict('records')
        
        # Upload missing data report to GCS if requested
        if upload_to_gcs and not missing_df.empty:
            self._upload_missing_data_report_to_gcs(missing_df, start_date, end_date, venues, instrument_types, data_types)
        
        return report
    
    def _upload_missing_data_report_to_gcs(self, missing_df: pd.DataFrame, start_date: datetime, 
                                         end_date: datetime, venues: list = None, 
                                         instrument_types: list = None, data_types: list = None):
        """Upload missing data report to GCS as daily parquet files"""
        try:
            # Create GCS path for missing data reports
            # Format: missing_data_reports/by_date/day-{date}/missing_data.parquet
            date_str = start_date.strftime('%Y-%m-%d')
            gcs_path = f"missing_data_reports/by_date/day-{date_str}/missing_data.parquet"
            
            # Add metadata to the DataFrame
            missing_df['report_date'] = date_str
            missing_df['venues_filter'] = ','.join(venues) if venues else 'all'
            missing_df['instrument_types_filter'] = ','.join(instrument_types) if instrument_types else 'all'
            missing_df['data_types_filter'] = ','.join(data_types) if data_types else 'all'
            missing_df['generated_at'] = datetime.now().isoformat()
            
            # Upload to GCS
            blob = self.bucket.blob(gcs_path)
            
            # Convert DataFrame to parquet bytes
            import io
            parquet_buffer = io.BytesIO()
            missing_df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
            parquet_buffer.seek(0)
            
            # Upload to GCS
            blob.upload_from_file(parquet_buffer, content_type='application/octet-stream')
            
            logger.info(f"📤 Uploaded missing data report to GCS: gs://{self.bucket.name}/{gcs_path}")
            logger.info(f"   - {len(missing_df)} missing entries for {date_str}")
            
        except Exception as e:
            logger.error(f"❌ Failed to upload missing data report to GCS: {e}")
    
    def get_missing_data_from_gcs(self, date: datetime) -> pd.DataFrame:
        """Read missing data report from GCS for a specific date"""
        try:
            date_str = date.strftime('%Y-%m-%d')
            gcs_path = f"missing_data_reports/by_date/day-{date_str}/missing_data.parquet"
            
            blob = self.bucket.blob(gcs_path)
            
            if not blob.exists():
                logger.info(f"No missing data report found for {date_str}")
                return pd.DataFrame()
            
            # Download and read parquet file
            import io
            parquet_data = blob.download_as_bytes()
            parquet_buffer = io.BytesIO(parquet_data)
            
            missing_df = pd.read_parquet(parquet_buffer)
            logger.info(f"📥 Loaded missing data report from GCS: {len(missing_df)} missing entries for {date_str}")
            
            return missing_df
            
        except Exception as e:
            logger.error(f"❌ Failed to read missing data report from GCS for {date_str}: {e}")
            return pd.DataFrame()


