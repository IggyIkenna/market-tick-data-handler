"""
Instrument Lister Service

Provides functionality to list and filter instrument definitions with various
output formats and statistics.
"""

import logging
import pandas as pd
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
import json

from ..data_downloader.instrument_reader import InstrumentReader
from ..models import InstrumentDefinition

logger = logging.getLogger(__name__)

class InstrumentLister:
    """Service for listing and filtering instrument definitions"""
    
    def __init__(self, gcs_bucket: str):
        self.gcs_bucket = gcs_bucket
        self.reader = InstrumentReader(gcs_bucket)
    
    def format_instrument_summary(self, instrument_def: InstrumentDefinition) -> Dict[str, Any]:
        """Format instrument definition as a summary dictionary"""
        return {
            'instrument_key': instrument_def.instrument_key,
            'venue': instrument_def.venue,
            'instrument_type': instrument_def.instrument_type,
            'base_asset': instrument_def.base_asset,
            'quote_asset': instrument_def.quote_asset,
            'settle_asset': instrument_def.settle_asset,
            'available_from': instrument_def.available_from_datetime,
            'available_to': instrument_def.available_to_datetime,
            'data_types': instrument_def.data_types,
            'expiry': instrument_def.expiry,
            'strike': instrument_def.strike,
            'option_type': instrument_def.option_type,
            'inverse': instrument_def.inverse,
            'underlying': instrument_def.underlying,
            'exchange_raw_symbol': instrument_def.exchange_raw_symbol,
            'tardis_symbol': instrument_def.tardis_symbol,
            'tardis_exchange': instrument_def.tardis_exchange
        }
    
    def format_tardis_lookup(self, instrument_def: InstrumentDefinition) -> Dict[str, Any]:
        """Format instrument for Tardis tick data lookup with key attributes"""
        return {
            'instrument_key': instrument_def.instrument_key,
            'tardis_symbol': instrument_def.tardis_symbol,
            'tardis_exchange': instrument_def.tardis_exchange,
            'data_types': instrument_def.data_types,
            'available_from': instrument_def.available_from_datetime,
            'available_to': instrument_def.available_to_datetime,
            # Key attributes that form the instrument ID
            'venue': instrument_def.venue,
            'instrument_type': instrument_def.instrument_type,
            'base_asset': instrument_def.base_asset,
            'quote_asset': instrument_def.quote_asset,
            'settle_asset': instrument_def.settle_asset,
            'expiry': instrument_def.expiry,
            'strike': instrument_def.strike,
            'option_type': instrument_def.option_type,
            'inverse': instrument_def.inverse,
            'underlying': instrument_def.underlying,
            'exchange_raw_symbol': instrument_def.exchange_raw_symbol
        }
    
    def format_tardis_lookup_from_dict(self, instrument_data: Dict[str, Any]) -> Dict[str, Any]:
        """Format instrument data dict for Tardis tick data lookup with key attributes"""
        return {
            'instrument_key': instrument_data['instrument_key'],
            'tardis_symbol': instrument_data['tardis_symbol'],
            'tardis_exchange': instrument_data['tardis_exchange'],
            'data_types': instrument_data.get('data_types', ''),
            'available_from': instrument_data.get('available_from_datetime', ''),
            'available_to': instrument_data.get('available_to_datetime', ''),
            # Key attributes that form the instrument ID
            'venue': instrument_data['venue'],
            'instrument_type': instrument_data['instrument_type'],
            'base_asset': instrument_data['base_asset'],
            'quote_asset': instrument_data['quote_asset'],
            'settle_asset': instrument_data.get('settle_asset', ''),
            'expiry': instrument_data.get('expiry', ''),
            'strike': instrument_data.get('strike', ''),
            'option_type': instrument_data.get('option_type', ''),
            'inverse': instrument_data.get('inverse', False),
            'underlying': instrument_data.get('underlying', ''),
            'exchange_raw_symbol': instrument_data.get('exchange_raw_symbol', '')
        }
    
    def list_instruments(self, 
                        date: datetime,
                        venue: Optional[str] = None,
                        instrument_type: Optional[str] = None,
                        base_asset: Optional[str] = None,
                        quote_asset: Optional[str] = None,
                        underlying: Optional[str] = None,
                        expiry: Optional[str] = None,
                        limit: Optional[int] = None,
                        format_type: str = 'full') -> Dict[str, Any]:
        """List instruments with filtering and formatting options"""
        
        try:
            # Get instruments with filters applied
            df = self.reader.get_instruments_for_date(
                date,
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
            
            # Convert DataFrame to InstrumentDefinition objects
            instruments = []
            for _, row in df.iterrows():
                try:
                    # Convert pandas Timestamps to ISO strings for Pydantic validation
                    row_dict = row.to_dict()
                    
                    # Convert datetime fields to ISO strings
                    for field in ['available_from_datetime', 'available_to_datetime', 'expiry']:
                        if field in row_dict and pd.notna(row_dict[field]):
                            if hasattr(row_dict[field], 'isoformat'):
                                row_dict[field] = row_dict[field].isoformat()
                            else:
                                row_dict[field] = str(row_dict[field])
                    
                    inst = InstrumentDefinition(**row_dict)
                    instruments.append(inst)
                except Exception as e:
                    logger.warning(f"Failed to parse instrument: {e}")
                    continue
            
            # Apply limit
            if limit:
                instruments = instruments[:limit]
            
            # Convert to appropriate format
            if format_type == 'tardis':
                # Use dict-based formatting to avoid Pydantic validation issues
                instrument_data = []
                for inst in instruments:
                    try:
                        inst_dict = inst.to_dict() if hasattr(inst, 'to_dict') else inst.__dict__
                        instrument_data.append(self.format_tardis_lookup_from_dict(inst_dict))
                    except Exception as e:
                        logger.warning(f"Failed to format instrument: {e}")
                        continue
            else:
                instrument_data = [self.format_instrument_summary(inst) for inst in instruments]
            
            return {
                "date": date.strftime('%Y-%m-%d'),
                "total_instruments": len(instruments),
                "format": format_type,
                "instruments": instrument_data
            }
            
        except Exception as e:
            logger.error(f"Error listing instruments: {e}")
            return {
                "date": date.strftime('%Y-%m-%d'),
                "total_instruments": 0,
                "format": format_type,
                "instruments": [],
                "error": str(e)
            }
    
    def get_statistics(self, date: datetime) -> Dict[str, Any]:
        """Get statistics about instruments for a specific date"""
        
        try:
            df = self.reader.get_instruments_for_date(date)
            
            if df.empty:
                return {
                    "total_instruments": 0,
                    "by_venue": {},
                    "by_instrument_type": {},
                    "top_base_assets": {}
                }
            
            # Count by venue
            by_venue = df['venue'].value_counts().to_dict() if 'venue' in df.columns else {}
            
            # Count by instrument type
            by_instrument_type = df['instrument_type'].value_counts().to_dict() if 'instrument_type' in df.columns else {}
            
            # Top base assets
            top_base_assets = df['base_asset'].value_counts().head(10).to_dict() if 'base_asset' in df.columns else {}
            
            return {
                "total_instruments": len(df),
                "by_venue": by_venue,
                "by_instrument_type": by_instrument_type,
                "top_base_assets": top_base_assets
            }
            
        except Exception as e:
            logger.error(f"Error getting statistics: {e}")
            return {
                "total_instruments": 0,
                "by_venue": {},
                "by_instrument_type": {},
                "top_base_assets": {},
                "error": str(e)
            }
    
    def print_table_summary(self, instruments: List[InstrumentDefinition], limit: Optional[int] = None, format_type: str = 'full'):
        """Print instruments in a table format"""
        if not instruments:
            print("No instruments found matching the criteria.")
            return
        
        # Apply limit
        if limit:
            instruments = instruments[:limit]
        
        # Create DataFrame for nice table formatting
        data = []
        for inst in instruments:
            if format_type == 'tardis':
                # Focused format for Tardis lookup
                data.append({
                    'Instrument Key': inst.instrument_key,
                    'Tardis Symbol': inst.tardis_symbol,
                    'Tardis Exchange': inst.tardis_exchange,
                    'Data Types': inst.data_types if inst.data_types else '',
                    'Base': inst.base_asset,
                    'Quote': inst.quote_asset,
                    'Settle': inst.settle_asset,
                    'Expiry': inst.expiry[:10] if inst.expiry else '',
                    'Strike': inst.strike,
                    'Option Type': inst.option_type,
                    'Inverse': inst.inverse
                })
            else:
                # Full format
                data.append({
                    'Instrument Key': inst.instrument_key,
                    'Venue': inst.venue,
                    'Type': inst.instrument_type,
                    'Base': inst.base_asset,
                    'Quote': inst.quote_asset,
                    'Settle': inst.settle_asset,
                    'Expiry': inst.expiry[:10] if inst.expiry else '',
                    'Strike': inst.strike,
                    'Option Type': inst.option_type,
                    'Inverse': inst.inverse,
                    'Data Types': inst.data_types if inst.data_types else ''
                })
        
        df = pd.DataFrame(data)
        
        # Set display options for better formatting
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', 50)
        
        format_title = "TARDIS LOOKUP" if format_type == 'tardis' else "INSTRUMENT DEFINITIONS"
        print(f"\n{'='*120}")
        print(f"{format_title} - {len(instruments)} instruments found")
        print(f"{'='*120}")
        print(df.to_string(index=False))
        
        if limit and len(data) == limit:
            print(f"\n... (showing first {limit} results, use --limit to see more)")
    
    def print_json_summary(self, instruments: List[InstrumentDefinition], limit: Optional[int] = None, format_type: str = 'full'):
        """Print instruments in JSON format"""
        if not instruments:
            print(json.dumps({"instruments": [], "count": 0}, indent=2))
            return
        
        # Apply limit
        if limit:
            instruments = instruments[:limit]
        
        # Convert to appropriate format
        if format_type == 'tardis':
            instrument_data = [self.format_tardis_lookup(inst) for inst in instruments]
        else:
            instrument_data = [self.format_instrument_summary(inst) for inst in instruments]
        
        result = {
            "date": instruments[0].available_from_datetime[:10] if instruments else None,
            "total_instruments": len(instruments),
            "format": format_type,
            "instruments": instrument_data
        }
        
        print(json.dumps(result, indent=2, default=str))
    
    def print_statistics(self, instruments: List[InstrumentDefinition]):
        """Print statistics about the instruments"""
        if not instruments:
            print("No instruments found.")
            return
        
        # Count by venue
        venue_counts = {}
        for inst in instruments:
            venue_counts[inst.venue] = venue_counts.get(inst.venue, 0) + 1
        
        # Count by instrument type
        type_counts = {}
        for inst in instruments:
            type_counts[inst.instrument_type] = type_counts.get(inst.instrument_type, 0) + 1
        
        # Count by base asset
        base_asset_counts = {}
        for inst in instruments:
            base_asset_counts[inst.base_asset] = base_asset_counts.get(inst.base_asset, 0) + 1
        
        print(f"\n{'='*60}")
        print(f"INSTRUMENT STATISTICS")
        print(f"{'='*60}")
        print(f"Total instruments: {len(instruments)}")
        
        print(f"\nBy Venue:")
        for venue, count in sorted(venue_counts.items()):
            print(f"  {venue}: {count}")
        
        print(f"\nBy Instrument Type:")
        for inst_type, count in sorted(type_counts.items()):
            print(f"  {inst_type}: {count}")
        
        print(f"\nTop 10 Base Assets:")
        for base_asset, count in sorted(base_asset_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {base_asset}: {count}")
