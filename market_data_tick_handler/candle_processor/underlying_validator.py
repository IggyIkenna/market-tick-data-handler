"""
Underlying Group Validator for Options/Futures

Validates that all instruments sharing an underlying have complete tick data
before processing candles. This is critical for options chains where ATM IV
and 25-delta skew calculations require all options for an underlying.
"""

import pandas as pd
import logging
from datetime import datetime, timezone
from typing import Dict, List, Set, Tuple, Optional
from collections import defaultdict

logger = logging.getLogger(__name__)

class UnderlyingGroupValidator:
    """Validates underlying instrument groups for complete data availability"""
    
    def __init__(self, gcs_bucket: str):
        self.gcs_bucket = gcs_bucket
        from google.cloud import storage
        self.client = storage.Client()
        self.bucket = self.client.bucket(gcs_bucket)
    
    def build_underlying_cache(self, instruments_df: pd.DataFrame) -> Dict[str, List[str]]:
        """
        Build cache mapping underlying -> List[instrument_ids]
        
        Args:
            instruments_df: DataFrame with instrument definitions
            
        Returns:
            Dictionary mapping underlying asset to list of instrument keys
        """
        underlying_cache = defaultdict(list)
        
        for _, instrument in instruments_df.iterrows():
            instrument_key = instrument['instrument_key']
            instrument_type = instrument['instrument_type']
            
            # Determine underlying asset
            underlying = self._extract_underlying(instrument_key, instrument_type, instrument)
            
            if underlying:
                underlying_cache[underlying].append(instrument_key)
        
        logger.info(f"📊 Built underlying cache: {len(underlying_cache)} underlying assets")
        for underlying, instruments in underlying_cache.items():
            logger.info(f"  {underlying}: {len(instruments)} instruments")
        
        return dict(underlying_cache)
    
    def _extract_underlying(self, instrument_key: str, instrument_type: str, instrument: pd.Series) -> Optional[str]:
        """Extract underlying asset from instrument key"""
        
        if instrument_type in ['SPOT_PAIR', 'SPOT_ASSET']:
            # For spot pairs, use base asset as underlying
            return instrument.get('base_asset', '')
        
        elif instrument_type in ['PERPETUAL', 'PERP']:
            # For perpetuals, use base asset as underlying
            return instrument.get('base_asset', '')
        
        elif instrument_type == 'FUTURE':
            # For futures, use base asset as underlying
            return instrument.get('base_asset', '')
        
        elif instrument_type == 'OPTION':
            # For options, use underlying field or base asset
            underlying = instrument.get('underlying', '')
            if not underlying:
                underlying = instrument.get('base_asset', '')
            return underlying
        
        else:
            # Unknown instrument type
            logger.warning(f"Unknown instrument type for underlying extraction: {instrument_type}")
            return None
    
    def validate_underlying_groups(self, 
                                 underlying_cache: Dict[str, List[str]],
                                 available_data_df: pd.DataFrame,
                                 data_types: List[str]) -> Tuple[List[str], List[str]]:
        """
        Validate that all instruments in each underlying group have complete data
        
        Args:
            underlying_cache: Mapping of underlying -> instrument list
            available_data_df: DataFrame of available tick data
            data_types: List of required data types
            
        Returns:
            Tuple of (valid_instruments, skipped_instruments)
        """
        valid_instruments = []
        skipped_instruments = []
        
        for underlying, instrument_list in underlying_cache.items():
            logger.info(f"🔍 Validating underlying group: {underlying} ({len(instrument_list)} instruments)")
            
            # Check if all instruments in this group have complete data
            group_valid = self._validate_group_completeness(
                instrument_list, available_data_df, data_types, underlying
            )
            
            if group_valid:
                valid_instruments.extend(instrument_list)
                logger.info(f"  ✅ All instruments for {underlying} have complete data")
            else:
                skipped_instruments.extend(instrument_list)
                logger.warning(f"  ❌ Skipping {underlying} group due to incomplete data")
        
        logger.info(f"📊 Validation complete: {len(valid_instruments)} valid, {len(skipped_instruments)} skipped")
        return valid_instruments, skipped_instruments
    
    def _validate_group_completeness(self, 
                                   instrument_list: List[str],
                                   available_data_df: pd.DataFrame,
                                   data_types: List[str],
                                   underlying: str) -> bool:
        """Check if all instruments in a group have complete data"""
        
        if available_data_df.empty:
            logger.warning(f"  ⚠️ No available data found for {underlying}")
            return False
        
        # Create expected data entries for this group
        expected_entries = set()
        for instrument in instrument_list:
            for data_type in data_types:
                expected_entries.add(f"{instrument}|{data_type}")
        
        # Create available data entries
        available_entries = set()
        for _, row in available_data_df.iterrows():
            if row['instrument_key'] in instrument_list:
                available_entries.add(f"{row['instrument_key']}|{row['data_type']}")
        
        # Check completeness
        missing_entries = expected_entries - available_entries
        
        if missing_entries:
            logger.warning(f"  ⚠️ Missing data for {underlying}: {len(missing_entries)} entries")
            for entry in sorted(missing_entries)[:5]:  # Show first 5 missing entries
                logger.warning(f"    - {entry}")
            if len(missing_entries) > 5:
                logger.warning(f"    ... and {len(missing_entries) - 5} more")
            return False
        
        return True
    
    def filter_instruments_by_type(self, 
                                 instruments_df: pd.DataFrame,
                                 valid_instruments: List[str],
                                 instrument_types: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Filter instruments based on validation results and type preferences
        
        Args:
            instruments_df: DataFrame with instrument definitions
            valid_instruments: List of validated instrument keys
            instrument_types: Optional list of instrument types to include
            
        Returns:
            Filtered DataFrame
        """
        # Filter by validation results
        filtered_df = instruments_df[instruments_df['instrument_key'].isin(valid_instruments)]
        
        # Apply instrument type filter if specified
        if instrument_types:
            filtered_df = filtered_df[filtered_df['instrument_type'].isin(instrument_types)]
        
        logger.info(f"📊 Filtered instruments: {len(filtered_df)} instruments")
        
        # Log breakdown by type
        type_counts = filtered_df['instrument_type'].value_counts()
        for instrument_type, count in type_counts.items():
            logger.info(f"  {instrument_type}: {count} instruments")
        
        return filtered_df
    
    async def load_available_data(self, date: datetime) -> pd.DataFrame:
        """Load available tick data for a specific date"""
        
        date_str = date.strftime('%Y-%m-%d')
        
        try:
            # Load available data report
            blob_name = f"available_tick_data/by_date/day-{date_str}/available_data.parquet"
            blob = self.bucket.blob(blob_name)
            
            if not blob.exists():
                logger.warning(f"⚠️ No available data report found for {date_str}")
                return pd.DataFrame()
            
            # Download and read parquet file
            import io
            blob_data = blob.download_as_bytes()
            available_df = pd.read_parquet(io.BytesIO(blob_data))
            
            logger.info(f"📊 Loaded available data: {len(available_df)} entries for {date_str}")
            return available_df
            
        except Exception as e:
            logger.error(f"❌ Error loading available data for {date_str}: {e}")
            return pd.DataFrame()
    
    def get_validation_summary(self, 
                             valid_instruments: List[str],
                             skipped_instruments: List[str],
                             underlying_cache: Dict[str, List[str]]) -> Dict[str, any]:
        """Generate validation summary"""
        
        total_instruments = len(valid_instruments) + len(skipped_instruments)
        validation_rate = (len(valid_instruments) / total_instruments * 100) if total_instruments > 0 else 0
        
        # Count by underlying
        valid_by_underlying = defaultdict(int)
        skipped_by_underlying = defaultdict(int)
        
        for underlying, instrument_list in underlying_cache.items():
            valid_count = sum(1 for inst in instrument_list if inst in valid_instruments)
            skipped_count = len(instrument_list) - valid_count
            
            valid_by_underlying[underlying] = valid_count
            skipped_by_underlying[underlying] = skipped_count
        
        return {
            'total_instruments': total_instruments,
            'valid_instruments': len(valid_instruments),
            'skipped_instruments': len(skipped_instruments),
            'validation_rate': validation_rate,
            'valid_by_underlying': dict(valid_by_underlying),
            'skipped_by_underlying': dict(skipped_by_underlying),
            'underlying_groups': len(underlying_cache)
        }
