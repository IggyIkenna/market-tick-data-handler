"""
Instrument Inspector Service

Provides functionality to inspect and analyze individual instrument definitions
with detailed attribute display and validation.
"""

import logging
import pandas as pd
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from pathlib import Path

from ..data_downloader.instrument_reader import InstrumentReader
from ..models import InstrumentDefinition

logger = logging.getLogger(__name__)

class InstrumentInspector:
    """Service for inspecting individual instrument definitions"""
    
    def __init__(self, gcs_bucket: str):
        self.gcs_bucket = gcs_bucket
        self.reader = InstrumentReader(gcs_bucket)
    
    def find_instrument(self, df: pd.DataFrame, target_instrument: str) -> Optional[pd.Series]:
        """Find instrument in DataFrame using multiple matching strategies"""
        
        # Method 1: Exact match on instrument_key
        if 'instrument_key' in df.columns:
            exact_match = df[df['instrument_key'] == target_instrument]
            if not exact_match.empty:
                logger.info("Found exact match on instrument_key")
                return exact_match.iloc[0]
        
        # Method 2: Match by venue, instrument_type, and symbol components
        logger.info("No exact match found, trying component matching...")
        
        # Split the target instrument
        parts = target_instrument.split(':')
        if len(parts) >= 3:
            target_venue = parts[0]
            target_type = parts[1]
            target_symbol = parts[2]
            
            # Look for matches
            if all(col in df.columns for col in ['venue', 'instrument_type']):
                venue_match = df[df['venue'] == target_venue]
                if not venue_match.empty:
                    type_match = venue_match[venue_match['instrument_type'] == target_type]
                    if not type_match.empty:
                        # For PERPETUAL/SPOT_PAIR, look for base_asset-quote_asset match
                        if target_type in ['PERPETUAL', 'SPOT_PAIR'] and 'base_asset' in df.columns and 'quote_asset' in df.columns:
                            symbol_parts = target_symbol.split('-')
                            if len(symbol_parts) == 2:
                                base_asset, quote_asset = symbol_parts
                                asset_match = type_match[
                                    (type_match['base_asset'] == base_asset) & 
                                    (type_match['quote_asset'] == quote_asset)
                                ]
                                if not asset_match.empty:
                                    logger.info("Found match by venue, type, and asset components")
                                    return asset_match.iloc[0]
                        
                        # For OPTION, look for more complex matching
                        elif target_type == 'OPTION' and len(parts) >= 5:
                            # Format: BASE-QUOTE-YYMMDD-STRIKE-OPTION_TYPE
                            # Example: BTC-USD-241225-50000-CALL
                            symbol_parts = target_symbol.split('-')
                            if len(symbol_parts) >= 5:
                                base_asset = symbol_parts[0]
                                quote_asset = symbol_parts[1]
                                expiry_date = symbol_parts[2]
                                strike = symbol_parts[3]
                                option_type = symbol_parts[4]
                                
                                option_match = type_match[
                                    (type_match['base_asset'] == base_asset) & 
                                    (type_match['quote_asset'] == quote_asset) &
                                    (type_match['strike'] == strike) &
                                    (type_match['option_type'] == option_type)
                                ]
                                if not option_match.empty:
                                    logger.info("Found match by venue, type, and option components")
                                    return option_match.iloc[0]
        
        return None
    
    def format_instrument_attributes(self, instrument_def: InstrumentDefinition) -> str:
        """Format instrument attributes in a readable structure"""
        
        output = []
        output.append(f"\n{'='*80}")
        output.append(f"INSTRUMENT DETAILS: {instrument_def.instrument_key}")
        output.append(f"{'='*80}")
        
        output.append(f"\n📋 CORE IDENTIFICATION:")
        output.append(f"  Instrument Key: {instrument_def.instrument_key}")
        output.append(f"  Venue: {instrument_def.venue}")
        output.append(f"  Instrument Type: {instrument_def.instrument_type}")
        
        output.append(f"\n💰 ASSET INFORMATION:")
        output.append(f"  Base Asset: {instrument_def.base_asset}")
        output.append(f"  Quote Asset: {instrument_def.quote_asset}")
        output.append(f"  Settle Asset: {instrument_def.settle_asset}")
        
        output.append(f"\n📅 AVAILABILITY WINDOW:")
        output.append(f"  Available From: {instrument_def.available_from_datetime}")
        output.append(f"  Available To: {instrument_def.available_to_datetime}")
        
        output.append(f"\n📊 DATA TYPES:")
        output.append(f"  Available Data Types: {instrument_def.data_types}")
        
        output.append(f"\n🏢 EXCHANGE MAPPINGS:")
        output.append(f"  Exchange Raw Symbol: {instrument_def.exchange_raw_symbol}")
        output.append(f"  Tardis Symbol: {instrument_def.tardis_symbol}")
        output.append(f"  Tardis Exchange: {instrument_def.tardis_exchange}")
        
        output.append(f"\n🏷️ METADATA:")
        output.append(f"  Data Provider: {instrument_def.data_provider}")
        output.append(f"  Venue Type: {instrument_def.venue_type}")
        output.append(f"  Asset Class: {instrument_def.asset_class}")
        
        output.append(f"\n⚙️ TRADING PARAMETERS:")
        output.append(f"  Inverse: {instrument_def.inverse}")
        output.append(f"  Symbol Type: {instrument_def.symbol_type}")
        output.append(f"  Contract Type: {instrument_def.contract_type}")
        
        if instrument_def.contract_size is not None:
            output.append(f"  Contract Size: {instrument_def.contract_size}")
        if instrument_def.tick_size:
            output.append(f"  Tick Size: {instrument_def.tick_size}")
        if instrument_def.settlement_type:
            output.append(f"  Settlement Type: {instrument_def.settlement_type}")
        if instrument_def.underlying:
            output.append(f"  Underlying: {instrument_def.underlying}")
        if instrument_def.min_size:
            output.append(f"  Min Size: {instrument_def.min_size}")
        
        # Option-specific fields
        if instrument_def.strike:
            output.append(f"  Strike: {instrument_def.strike}")
        if instrument_def.option_type:
            output.append(f"  Option Type: {instrument_def.option_type}")
        if instrument_def.expiry:
            output.append(f"  Expiry: {instrument_def.expiry}")
        
        output.append(f"\n🔗 CCXT INTEGRATION:")
        output.append(f"  CCXT Symbol: {instrument_def.ccxt_symbol}")
        output.append(f"  CCXT Exchange: {instrument_def.ccxt_exchange}")
        
        return "\n".join(output)
    
    def inspect_instrument(self, instrument_id: str, date: datetime, show_summary: bool = False) -> Dict[str, Any]:
        """Inspect a specific instrument definition"""
        
        logger.info(f"Searching for instrument: {instrument_id}")
        logger.info(f"Date: {date.strftime('%Y-%m-%d')}")
        
        try:
            # Get instruments for the date
            df = self.reader.get_instruments_for_date(date)
            logger.info(f"Successfully loaded {len(df)} instrument definitions")
            
            if df.empty:
                return {
                    'success': False,
                    'error': 'No instrument definitions found for the specified date',
                    'instruments': []
                }
            
            # Show summary if requested
            summary = None
            if show_summary:
                summary = {
                    'total_instruments': len(df),
                    'columns_available': list(df.columns),
                    'by_venue_type': df.groupby(['venue', 'instrument_type']).size().to_dict() if 'venue' in df.columns and 'instrument_type' in df.columns else {}
                }
            
            # Find the specific instrument
            instrument_row = self.find_instrument(df, instrument_id)
            
            if instrument_row is None:
                # Find similar instruments
                similar_instruments = []
                if 'instrument_key' in df.columns:
                    parts = instrument_id.split(':')
                    if len(parts) >= 1:
                        venue = parts[0]
                        similar = df[df['instrument_key'].str.contains(venue, na=False)]
                        if not similar.empty:
                            similar_instruments = similar.head(10)['instrument_key'].tolist()
                
                return {
                    'success': False,
                    'error': f'Could not find instrument: {instrument_id}',
                    'similar_instruments': similar_instruments,
                    'summary': summary
                }
            
            # Convert to InstrumentDefinition for validation
            try:
                instrument_dict = instrument_row.to_dict()
                
                # Convert datetime objects to ISO strings for Pydantic validation
                for field in ['available_from_datetime', 'available_to_datetime', 'expiry']:
                    if field in instrument_dict and hasattr(instrument_dict[field], 'isoformat'):
                        instrument_dict[field] = instrument_dict[field].isoformat()
                
                # Handle NaN values
                for key, value in instrument_dict.items():
                    if pd.isna(value):
                        instrument_dict[key] = None
                
                instrument_def = InstrumentDefinition.from_dict(instrument_dict)
                
                return {
                    'success': True,
                    'instrument': instrument_def,
                    'formatted_attributes': self.format_instrument_attributes(instrument_def),
                    'summary': summary
                }
                
            except Exception as e:
                logger.error(f"Failed to validate instrument with Pydantic model: {e}")
                return {
                    'success': False,
                    'error': f'Failed to validate instrument with Pydantic model: {e}',
                    'raw_data': instrument_row.to_dict(),
                    'summary': summary
                }
            
        except Exception as e:
            logger.error(f"Failed to load instrument definitions: {e}")
            return {
                'success': False,
                'error': f'Failed to load instrument definitions: {e}',
                'instruments': []
            }
    
    def get_similar_instruments(self, instrument_id: str, date: datetime, limit: int = 10) -> List[str]:
        """Get similar instruments based on venue or type"""
        
        try:
            df = self.reader.get_instruments_for_date(date)
            
            if df.empty or 'instrument_key' not in df.columns:
                return []
            
            parts = instrument_id.split(':')
            if len(parts) >= 1:
                venue = parts[0]
                similar = df[df['instrument_key'].str.contains(venue, na=False)]
                if not similar.empty:
                    return similar.head(limit)['instrument_key'].tolist()
            
            return []
            
        except Exception as e:
            logger.error(f"Failed to get similar instruments: {e}")
            return []
