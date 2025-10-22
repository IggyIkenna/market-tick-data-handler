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
            expected_instruments = self._get_expected_instruments(current_date, venues, instrument_types)
            
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
                                instrument_types: list = None) -> list:
        """Get instruments that should have data for a given date"""
        # Read instrument definitions from GCS
        aggregated_path = f"instrument_availability/instruments_{date.strftime('%Y%m%d')}.parquet"
        
        try:
            blob = self.bucket.blob(aggregated_path)
            if blob.exists():
                import tempfile
                with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp_file:
                    blob.download_to_filename(tmp_file.name)
                    df = pd.read_parquet(tmp_file.name)
                    
                    # Apply filters
                    if venues:
                        df = df[df['venue'].isin(venues)]
                    if instrument_types:
                        df = df[df['instrument_type'].isin(instrument_types)]
                    
                    return df['instrument_key'].tolist()
        except Exception as e:
            logger.warning(f"Could not read instrument definitions for {date.strftime('%Y-%m-%d')}: {e}")
        
        return []
    
    def _get_available_data(self, date: datetime, venues: list = None, 
                           instrument_types: list = None) -> list:
        """Get instruments that actually have data in GCS for a given date"""
        available_instruments = []
        
        # Check raw_tick_data partition
        prefix = f"raw_tick_data/by_date/year-{date.year}/month-{date.month:02d}/day-{date.day:02d}/"
        
        blobs = list(self.bucket.list_blobs(prefix=prefix))
        
        for blob in blobs:
            if blob.name.endswith('.parquet'):
                # Extract instrument key from path
                # Path format: raw_tick_data/by_date/year-2024/month-01/day-15/venue-deribit/instrument-{key}/{data_type}.parquet
                parts = blob.name.split('/')
                if len(parts) >= 7 and parts[6].startswith('instrument-'):
                    instrument_key = parts[6].replace('instrument-', '')
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
    
    def generate_missing_data_report(self, start_date: datetime, end_date: datetime,
                                   venues: list = None, instrument_types: list = None) -> dict:
        """Generate a comprehensive missing data report"""
        logger.info("Generating missing data report")
        
        missing_df = self.check_missing_data(start_date, end_date, venues, instrument_types)
        
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
        
        return {
            'status': 'incomplete',
            'missing_count': missing_count,
            'total_days': total_days,
            'coverage_percentage': ((total_days - missing_count) / total_days) * 100,
            'daily_coverage': daily_coverage.to_dict('records'),
            'instrument_coverage': instrument_coverage.to_dict('records'),
            'summary': f"Found {missing_count} missing data entries across {total_days} days"
        }

