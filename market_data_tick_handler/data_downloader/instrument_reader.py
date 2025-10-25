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

from ..utils.gcs_client import get_shared_gcs_client, get_shared_gcs_bucket

logger = logging.getLogger(__name__)

class InstrumentReader:
    """Reads and filters instrument definitions from GCS"""
    
    def __init__(self, gcs_bucket: str):
        self.gcs_bucket = gcs_bucket
        # Use shared GCS client for connection reuse and optimal performance
        self.client = get_shared_gcs_client()
        self.bucket = get_shared_gcs_bucket(gcs_bucket)
    
    def get_instruments_for_date(self, date: datetime, 
                                venue: str = None, 
                                instrument_type: str = None,
                                base_asset: str = None,
                                quote_asset: str = None,
                                underlying: str = None,
                                expiry: str = None,
                                start_date: datetime = None,
                                end_date: datetime = None) -> pd.DataFrame:
        """
        Get instruments available for a specific date from aggregate files
        
        Args:
            date: Date to query instruments for
            venue: Optional venue filter (e.g., 'deribit', 'binance')
            instrument_type: Optional type filter (e.g., 'option', 'perpetual')
            base_asset: Optional base asset filter (e.g., 'BTC', 'ETH')
            quote_asset: Optional quote asset filter (e.g., 'USDT', 'USDC', 'USD')
            underlying: Optional underlying asset filter (e.g., 'BTC-USD', 'ETH-USD')
            expiry: Optional expiry date filter (e.g., '2024-06-28', '240628')
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
        
        logger.info(f"Reading instruments for {date.strftime('%Y-%m-%d')} from daily file (faster and more accurate)")
        
        # Try to read from daily partition FIRST (faster, more accurate, up-to-date)
        date_path = f"instrument_availability/by_date/day-{date.strftime('%Y-%m-%d')}/instruments.parquet"
        
        try:
            blob = self.bucket.blob(date_path)
            if blob.exists():
                logger.info(f"Loading from daily file: {date_path}")
                with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp_file:
                    blob.download_to_filename(tmp_file.name)
                    df = pd.read_parquet(tmp_file.name)
                    logger.info(f"Loaded {len(df)} total instruments from daily file")
                    
                    # Filter instruments that were available on the target date
                    # Check for both possible column name formats
                    available_from_col = None
                    available_to_col = None
                    
                    if 'available_from_datetime' in df.columns and 'available_to_datetime' in df.columns:
                        available_from_col = 'available_from_datetime'
                        available_to_col = 'available_to_datetime'
                    elif 'available_from' in df.columns and 'available_to' in df.columns:
                        available_from_col = 'available_from'
                        available_to_col = 'available_to'
                    
                    if available_from_col and available_to_col:
                        # Convert to datetime if they're strings
                        df[available_from_col] = pd.to_datetime(df[available_from_col])
                        df[available_to_col] = pd.to_datetime(df[available_to_col])
                        
                        # Ensure timezone consistency
                        if df[available_from_col].dt.tz is None:
                            df[available_from_col] = df[available_from_col].dt.tz_localize('UTC')
                        if df[available_to_col].dt.tz is None:
                            df[available_to_col] = df[available_to_col].dt.tz_localize('UTC')
                        
                        # Filter for instruments available on the target date
                        available_instruments = df[
                            (df[available_from_col] <= date) & 
                            (df[available_to_col] >= date)
                        ]
                        
                        logger.info(f"Filtered {len(available_instruments)} instruments available on {date.strftime('%Y-%m-%d')} from {len(df)} total instruments using {available_from_col}/{available_to_col}")
                        df = available_instruments
                    else:
                        # If no date filtering columns, use all instruments
                        logger.warning(f"No date filtering columns found (looked for 'available_from_datetime'/'available_to_datetime' or 'available_from'/'available_to'), using all {len(df)} instruments")
                        logger.debug(f"Available columns: {list(df.columns)}")
                    
                    # Apply additional filters (case-insensitive)
                    if venue:
                        df = df[df['venue'].str.upper() == venue.upper()]
                    if instrument_type:
                        df = df[df['instrument_type'].str.upper() == instrument_type.upper()]
                    if base_asset:
                        df = df[df['base_asset'].str.upper() == base_asset.upper()]
                    if quote_asset:
                        df = df[df['quote_asset'].str.upper() == quote_asset.upper()]
                    if underlying:
                        df = df[df['underlying'].str.upper() == underlying.upper()]
                    if expiry:
                        df = df[df['expiry'].astype(str).str.contains(expiry, na=False)]
                    
                    logger.info(f"Found {len(df)} instruments for {date.strftime('%Y-%m-%d')}")
                    return df
                    
        except Exception as e:
            logger.warning(f"Could not read daily file: {e}")
        
        # Fallback to aggregate file (slower, potentially outdated)
        aggregate_path = f"instrument_availability/aggregate/instruments_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.parquet"
        
        try:
            blob = self.bucket.blob(aggregate_path)
            if blob.exists():
                logger.info(f"Falling back to aggregate file: {aggregate_path}")
                with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp_file:
                    blob.download_to_filename(tmp_file.name)
                    df = pd.read_parquet(tmp_file.name)
                    logger.info(f"Loaded {len(df)} total instruments from aggregate file")
                    
                    # Filter instruments that were available on the target date
                    # Check for both possible column name formats
                    available_from_col = None
                    available_to_col = None
                    
                    if 'available_from_datetime' in df.columns and 'available_to_datetime' in df.columns:
                        available_from_col = 'available_from_datetime'
                        available_to_col = 'available_to_datetime'
                    elif 'available_from' in df.columns and 'available_to' in df.columns:
                        available_from_col = 'available_from'
                        available_to_col = 'available_to'
                    
                    if available_from_col and available_to_col:
                        # Convert to datetime if they're strings
                        df[available_from_col] = pd.to_datetime(df[available_from_col])
                        df[available_to_col] = pd.to_datetime(df[available_to_col])
                        
                        # Ensure timezone consistency
                        if df[available_from_col].dt.tz is None:
                            df[available_from_col] = df[available_from_col].dt.tz_localize('UTC')
                        if df[available_to_col].dt.tz is None:
                            df[available_to_col] = df[available_to_col].dt.tz_localize('UTC')
                        
                        # Filter for instruments available on the target date
                        available_instruments = df[
                            (df[available_from_col] <= date) & 
                            (df[available_to_col] >= date)
                        ]
                        
                        logger.info(f"Filtered {len(available_instruments)} instruments available on {date.strftime('%Y-%m-%d')} from {len(df)} total instruments using {available_from_col}/{available_to_col}")
                        df = available_instruments
                    else:
                        # If no date filtering columns, use all instruments
                        logger.warning(f"No date filtering columns found (looked for 'available_from_datetime'/'available_to_datetime' or 'available_from'/'available_to'), using all {len(df)} instruments")
                        logger.debug(f"Available columns: {list(df.columns)}")
                    
                    # Apply additional filters (case-insensitive)
                    if venue:
                        df = df[df['venue'].str.upper() == venue.upper()]
                    if instrument_type:
                        df = df[df['instrument_type'].str.upper() == instrument_type.upper()]
                    if base_asset:
                        df = df[df['base_asset'].str.upper() == base_asset.upper()]
                    if quote_asset:
                        df = df[df['quote_asset'].str.upper() == quote_asset.upper()]
                    if underlying:
                        df = df[df['underlying'].str.upper() == underlying.upper()]
                    if expiry:
                        df = df[df['expiry'].astype(str).str.contains(expiry, na=False)]
                    
                    logger.info(f"Found {len(df)} instruments for {date.strftime('%Y-%m-%d')}")
                    return df
                    
        except Exception as e:
            logger.warning(f"Could not read aggregate file: {e}")
        
        # Final fallback to legacy daily file
        legacy_daily_path = f"instrument_availability/instruments_{date.strftime('%Y%m%d')}.parquet"
        
        try:
            blob = self.bucket.blob(legacy_daily_path)
            if blob.exists():
                logger.info(f"Final fallback to legacy daily file: {legacy_daily_path}")
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
            logger.error(f"Could not read any instrument definition files: {e}")
        
        return pd.DataFrame()
    
    def get_download_targets(self, date: datetime, 
                           venues: list = None, 
                           instrument_types: list = None,
                           base_assets: list = None,
                           quote_assets: list = None,
                           underlyings: list = None,
                           expiries: list = None,
                           max_instruments: int = None,
                           start_date: datetime = None,
                           end_date: datetime = None) -> list:
        """
        Get download targets (tardis_exchange, tardis_symbol) for a date
        
        Args:
            date: Date to get targets for
            venues: List of venues to include
            instrument_types: List of instrument types to include
            base_assets: List of base assets to include
            quote_assets: List of quote assets to include
            underlyings: List of underlying assets to include
            expiries: List of expiry dates to include
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
        
        # Apply filters (case-insensitive for venues)
        if venues:
            # Convert both to uppercase for case-insensitive comparison
            df = df[df['venue'].str.upper().isin([v.upper() for v in venues])]
        if instrument_types:
            df = df[df['instrument_type'].str.upper().isin([t.upper() for t in instrument_types])]
        if base_assets:
            df = df[df['base_asset'].str.upper().isin([a.upper() for a in base_assets])]
        if quote_assets:
            df = df[df['quote_asset'].str.upper().isin([a.upper() for a in quote_assets])]
        if underlyings:
            df = df[df['underlying'].str.upper().isin([u.upper() for u in underlyings])]
        if expiries:
            # Filter by expiry dates (supports various formats)
            expiry_mask = df['expiry'].astype(str).str.contains('|'.join(expiries), na=False)
            df = df[expiry_mask]
        
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
                'quote_asset': row['quote_asset'],
                'data_types': row.get('data_types', '')
            })
        
        logger.info(f"Generated {len(targets)} download targets")
        return targets
    
    def list_instruments(self, date: datetime,
                        venue: str = None,
                        instrument_type: str = None,
                        base_asset: str = None,
                        quote_asset: str = None,
                        underlying: str = None,
                        expiry: str = None,
                        limit: int = None,
                        format_type: str = 'full') -> dict:
        """
        List instruments with filtering and formatting options (package equivalent of list_instruments.py)
        
        Args:
            date: Date to query instruments for
            venue: Optional venue filter (e.g., 'deribit', 'binance')
            instrument_type: Optional type filter (e.g., 'option', 'perpetual')
            base_asset: Optional base asset filter (e.g., 'BTC', 'ETH')
            quote_asset: Optional quote asset filter (e.g., 'USDT', 'USDC', 'USD')
            underlying: Optional underlying asset filter (e.g., 'BTC-USD', 'ETH-USD')
            expiry: Optional expiry date filter (e.g., '2024-06-28', '240628')
            limit: Optional limit on number of results
            format_type: Output format ('full', 'tardis')
            
        Returns:
            Dict containing instruments and metadata
        """
        # Get instruments with filters
        df = self.get_instruments_for_date(
            date=date,
            venue=venue,
            instrument_type=instrument_type,
            base_asset=base_asset,
            quote_asset=quote_asset,
            underlying=underlying,
            expiry=expiry
        )
        
        if df.empty:
            return {
                "date": date.strftime('%Y-%m-%d'),
                "total_instruments": 0,
                "format": format_type,
                "instruments": []
            }
        
        # Apply limit
        if limit:
            df = df.head(limit)
        
        # Convert to list of dicts
        instruments = []
        for _, row in df.iterrows():
            if format_type == 'tardis':
                # Tardis lookup format
                instrument_data = {
                    'instrument_key': row['instrument_key'],
                    'tardis_symbol': row['tardis_symbol'],
                    'tardis_exchange': row['tardis_exchange'],
                    'data_types': row.get('data_types', ''),
                    'available_from': row.get('available_from_datetime', ''),
                    'available_to': row.get('available_to_datetime', ''),
                    # Key attributes that form the instrument ID
                    'venue': row['venue'],
                    'instrument_type': row['instrument_type'],
                    'base_asset': row['base_asset'],
                    'quote_asset': row['quote_asset'],
                    'settle_asset': row.get('settle_asset', ''),
                    'expiry': row.get('expiry', ''),
                    'strike': row.get('strike', ''),
                    'option_type': row.get('option_type', ''),
                    'inverse': row.get('inverse', False),
                    'underlying': row.get('underlying', ''),
                    'exchange_raw_symbol': row.get('exchange_raw_symbol', '')
                }
            else:
                # Full format
                instrument_data = row.to_dict()
            
            instruments.append(instrument_data)
        
        return {
            "date": date.strftime('%Y-%m-%d'),
            "total_instruments": len(instruments),
            "format": format_type,
            "instruments": instruments
        }
    
    def get_instrument_statistics(self, date: datetime) -> dict:
        """
        Get statistics about instruments for a specific date
        
        Args:
            date: Date to get statistics for
            
        Returns:
            Dict containing statistics
        """
        df = self.get_instruments_for_date(date)
        
        if df.empty:
            return {
                "total_instruments": 0,
                "by_venue": {},
                "by_instrument_type": {},
                "top_base_assets": {}
            }
        
        # Count by venue
        by_venue = df['venue'].value_counts().to_dict()
        
        # Count by instrument type
        by_instrument_type = df['instrument_type'].value_counts().to_dict()
        
        # Top base assets
        top_base_assets = df['base_asset'].value_counts().head(10).to_dict()
        
        return {
            "total_instruments": len(df),
            "by_venue": by_venue,
            "by_instrument_type": by_instrument_type,
            "top_base_assets": top_base_assets
        }

