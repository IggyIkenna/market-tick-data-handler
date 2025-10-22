"""
Instrument Reader Module

Reads instrument definitions from GCS and provides filtering capabilities
to determine which instruments to download data for.
"""

import pandas as pd
from datetime import datetime, timezone
from google.cloud import storage
import tempfile
import logging

logger = logging.getLogger(__name__)

class InstrumentReader:
    """Reads and filters instrument definitions from GCS"""
    
    def __init__(self, gcs_bucket: str):
        self.gcs_bucket = gcs_bucket
        self.client = storage.Client()
        self.bucket = self.client.bucket(gcs_bucket)
    
    def get_instruments_for_date(self, date: datetime, 
                                venue: str = None, 
                                instrument_type: str = None,
                                start_date: datetime = None,
                                end_date: datetime = None) -> pd.DataFrame:
        """
        Get instruments available for a specific date from aggregate files
        
        Args:
            date: Date to query instruments for
            venue: Optional venue filter (e.g., 'deribit', 'binance')
            instrument_type: Optional type filter (e.g., 'option', 'perpetual')
            start_date: Start date for aggregate file (defaults to 2023-05-23)
            end_date: End date for aggregate file (defaults to 2023-05-23)
            
        Returns:
            DataFrame with instrument definitions
        """
        # Default date range (configurable)
        if start_date is None:
            start_date = datetime(2023, 5, 23, tzinfo=timezone.utc)
        if end_date is None:
            end_date = datetime(2023, 5, 23, tzinfo=timezone.utc)
        
        logger.info(f"Reading instruments for {date.strftime('%Y-%m-%d')} from aggregate file")
        
        # Try to read from aggregate file first
        aggregate_path = f"instrument_availability/aggregate/instruments_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.parquet"
        
        try:
            blob = self.bucket.blob(aggregate_path)
            if blob.exists():
                logger.info(f"Loading from aggregate file: {aggregate_path}")
                with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp_file:
                    blob.download_to_filename(tmp_file.name)
                    df = pd.read_parquet(tmp_file.name)
                    logger.info(f"Loaded {len(df)} total instruments from aggregate file")
                    
                    # Filter instruments that were available on the target date
                    if 'available_from' in df.columns and 'available_to' in df.columns:
                        df['available_from'] = pd.to_datetime(df['available_from'])
                        df['available_to'] = pd.to_datetime(df['available_to'])
                        
                        # Filter for instruments available on the target date
                        available_instruments = df[
                            (df['available_from'] <= date) & 
                            (df['available_to'] >= date)
                        ]
                        
                        logger.info(f"Filtered {len(available_instruments)} instruments available on {date.strftime('%Y-%m-%d')} from {len(df)} total instruments")
                        df = available_instruments
                    else:
                        # If no date filtering columns, use all instruments
                        logger.warning(f"No date filtering columns found, using all {len(df)} instruments")
                    
                    # Apply additional filters
                    if venue:
                        df = df[df['venue'] == venue]
                    if instrument_type:
                        df = df[df['instrument_type'] == instrument_type]
                    
                    logger.info(f"Found {len(df)} instruments for {date.strftime('%Y-%m-%d')}")
                    return df
                    
        except Exception as e:
            logger.warning(f"Could not read aggregate file: {e}")
        
        # Fallback to daily aggregated file
        daily_path = f"instrument_availability/instruments_{date.strftime('%Y%m%d')}.parquet"
        
        try:
            blob = self.bucket.blob(daily_path)
            if blob.exists():
                logger.info(f"Falling back to daily file: {daily_path}")
                with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp_file:
                    blob.download_to_filename(tmp_file.name)
                    df = pd.read_parquet(tmp_file.name)
                    
                    # Apply filters
                    if venue:
                        df = df[df['venue'] == venue]
                    if instrument_type:
                        df = df[df['instrument_type'] == instrument_type]
                    
                    logger.info(f"Found {len(df)} instruments for {date.strftime('%Y-%m-%d')}")
                    return df
                    
        except Exception as e:
            logger.warning(f"Could not read daily file: {e}")
        
        # Final fallback to by_date partition (optimized single partition)
        date_path = f"instrument_availability/by_date/day-{date.strftime('%Y-%m-%d')}/instruments.parquet"
        
        try:
            blob = self.bucket.blob(date_path)
            if blob.exists():
                logger.info(f"Falling back to daily partition: {date_path}")
                with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp_file:
                    blob.download_to_filename(tmp_file.name)
                    df = pd.read_parquet(tmp_file.name)
                    
                    # Apply filters
                    if venue:
                        df = df[df['venue'] == venue]
                    if instrument_type:
                        df = df[df['instrument_type'] == instrument_type]
                    
                    logger.info(f"Found {len(df)} instruments for {date.strftime('%Y-%m-%d')}")
                    return df
                    
        except Exception as e:
            logger.error(f"Could not read instrument definitions: {e}")
        
        return pd.DataFrame()
    
    def get_download_targets(self, date: datetime, 
                           venues: list = None, 
                           instrument_types: list = None,
                           max_instruments: int = None,
                           start_date: datetime = None,
                           end_date: datetime = None) -> list:
        """
        Get download targets (tardis_exchange, tardis_symbol) for a date
        
        Args:
            date: Date to get targets for
            venues: List of venues to include
            instrument_types: List of instrument types to include
            max_instruments: Maximum number of instruments to return
            start_date: Start date for aggregate file (defaults to 2023-05-23)
            end_date: End date for aggregate file (defaults to 2023-05-23)
            
        Returns:
            List of dicts with 'tardis_exchange' and 'tardis_symbol' keys
        """
        df = self.get_instruments_for_date(date, 
                                         venue=None, 
                                         instrument_type=None,
                                         start_date=start_date,
                                         end_date=end_date)
        
        if df.empty:
            return []
        
        # Apply filters
        if venues:
            df = df[df['venue'].isin(venues)]
        if instrument_types:
            df = df[df['instrument_type'].isin(instrument_types)]
        
        # Limit results
        if max_instruments:
            df = df.head(max_instruments)
        
        # Extract download targets
        targets = []
        for _, row in df.iterrows():
            targets.append({
                'instrument_key': row['instrument_key'],
                'tardis_exchange': row['tardis_exchange'],
                'tardis_symbol': row['tardis_symbol'],
                'instrument_type': row['instrument_type'],
                'base_asset': row['base_asset'],
                'quote_asset': row['quote_asset']
            })
        
        logger.info(f"Generated {len(targets)} download targets")
        return targets

