#!/usr/bin/env python3
"""
Canonical Instrument Key Generator
Generates instrument keys following the INSTRUMENT_KEY.md specification
"""

import os
import sys
import json
import logging
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional
import re

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import get_config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CanonicalInstrumentKeyGenerator:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {"Authorization": f"Bearer {api_key}"}
        self.session = requests.Session()
        
        # Retry configuration
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def generate_instrument_key(self, exchange: str, symbol_type: str, symbol_id: str, symbol_info: Dict[str, Any]) -> Optional[str]:
        """
        Generate canonical instrument key following INSTRUMENT_KEY.md specification
        
        Format: VENUE:INSTRUMENT_TYPE:BASE_ASSET-QUOTE_ASSET-YYMMDD-STRIKE-OPTION_TYPE
        All components are uppercase, C/P replaced with CALL/PUT
        """
        
        # Map exchange names to canonical venues (uppercase)
        venue_mapping = {
            'binance': 'BINANCE',
            'binance-futures': 'BINANCE-FUTURES',  # Different venue - separate endpoint
            'deribit': 'DERIBIT',
            'bybit': 'BYBIT',
            'bybit-spot': 'BYBIT-SPOT',  # Different venue - separate endpoint
            'okex': 'OKX',
            'okex-futures': 'OKX-FUTURES',  # Different venue - separate endpoint
            'okex-swap': 'OKX-SWAP',  # Different venue - separate endpoint
            'upbit': 'UPBIT'
        }
        
        venue = venue_mapping.get(exchange)
        if not venue:
            logger.warning(f"Unknown exchange: {exchange}")
            return None
            
        # Map symbol types to canonical instrument types (uppercase)
        instrument_type_mapping = {
            'spot': 'SPOT_PAIR',
            'perpetual': 'PERP',
            'future': 'FUTURE',
            'option': 'OPTION',
            'combo': 'OPTION'  # Deribit combos are often options
        }
        
        instrument_type = instrument_type_mapping.get(symbol_type)
        if not instrument_type:
            logger.warning(f"Unknown symbol type: {symbol_type}")
            return None
            
        # Extract base and quote currencies (uppercase)
        base_asset = symbol_info.get('base_asset', '').upper()
        quote_asset = symbol_info.get('quote_asset', '').upper()
        
        if not base_asset or not quote_asset:
            logger.warning(f"Missing base or quote for {symbol_id}")
            return None
            
        # Generate canonical symbol based on instrument type
        if instrument_type == 'SPOT_PAIR':
            canonical_symbol = f"{base_asset}-{quote_asset}"
            
        elif instrument_type == 'PERP':
            canonical_symbol = f"{base_asset}-{quote_asset}"
            
        elif instrument_type == 'FUTURE':
            expiry = symbol_info.get('expiry', '')
            if expiry:
                # Convert expiry to YYMMDD format
                try:
                    expiry_date = datetime.strptime(expiry, "%Y-%m-%d")
                    expiry_str = expiry_date.strftime("%y%m%d")
                    canonical_symbol = f"{base_asset}-{quote_asset}-{expiry_str}"
                except:
                    canonical_symbol = f"{base_asset}-{quote_asset}"
            else:
                canonical_symbol = f"{base_asset}-{quote_asset}"
                
        elif instrument_type == 'OPTION':
            expiry = symbol_info.get('expiry', '')
            strike = symbol_info.get('strike', '')
            option_type = symbol_info.get('option_type', '')
            
            # Ensure all fields are present
            if not expiry or not strike or not option_type:
                logger.warning(f"Missing option fields for {symbol_id}: expiry={expiry}, strike={strike}, type={option_type}")
                canonical_symbol = f"{base_asset}-{quote_asset}"
            else:
                try:
                    expiry_date = datetime.strptime(expiry, "%Y-%m-%d")
                    expiry_str = expiry_date.strftime("%y%m%d")
                    
                    # Normalize option type to CALL/PUT (uppercase)
                    option_type_normalized = 'CALL' if option_type.upper() in ['C', 'CALL'] else 'PUT'
                    
                    # New format: BASE_ASSET-QUOTE_ASSET-YYMMDD-STRIKE-OPTION_TYPE
                    canonical_symbol = f"{base_asset}-{quote_asset}-{expiry_str}-{strike}-{option_type_normalized}"
                except Exception as e:
                    logger.warning(f"Failed to format option key for {symbol_id}: {e}")
                    canonical_symbol = f"{base_asset}-{quote_asset}"
        else:
            canonical_symbol = f"{base_asset}-{quote_asset}"
            
        # Generate full instrument key
        instrument_key = f"{venue}:{instrument_type}:{canonical_symbol}"
        
        return instrument_key

    def _normalize_option_type(self, option_type: str) -> str:
        """Normalize option type to canonical format (CALL/PUT)"""
        if not option_type:
            return ''
        
        option_type_upper = option_type.upper()
        if option_type_upper in ['C', 'CALL']:
            return 'CALL'
        elif option_type_upper in ['P', 'PUT']:
            return 'PUT'
        else:
            return option_type_upper  # Return as-is if unknown
    
    def _get_underlying_asset(self, symbol_type: str, base_asset: str, quote_asset: str) -> str:
        """Get underlying asset for options/futures: BASE-QUOTE format"""
        if symbol_type not in ['option', 'future']:
            return ''
        return f"{base_asset}-{quote_asset}"
    
    def _get_tardis_data_types(self, instrument_key: str) -> List[str]:
        """Get available Tardis data types for this instrument type based on hardcoded logic"""
        base_data_types = ["trades", "book_snapshot_5"]  # Removed quotes as it's embedded in book_snapshot_5
        
        if "SPOT_PAIR" in instrument_key:
            # Spot pairs: trades, book_snapshot_5
            return base_data_types
        elif "PERPETUAL" in instrument_key:
            # Perpetuals: trades, book_snapshot_5, derivative_ticker, liquidations
            return base_data_types + ["derivative_ticker", "liquidations"]
        elif "FUTURE" in instrument_key:
            # Futures: trades, book_snapshot_5, derivative_ticker, liquidations
            return base_data_types + ["derivative_ticker", "liquidations"]
        elif "OPTION" in instrument_key:
            # Options: trades, book_snapshot_5, options_chain
            return base_data_types + ["options_chain"] + ["liquidations"] + ["derivative_ticker"]
        else:
            # Default to base data types for unknown types
            return base_data_types

    def generate_attributes(self, exchange: str, symbol_type: str, symbol_id: str, symbol_info: Dict[str, Any], available_data_types: List[str]) -> Dict[str, Any]:
        """Generate attributes for the instrument"""
        
        attributes = {
            'base_asset': symbol_info.get('base_asset', ''),
            'quote_asset': symbol_info.get('quote_asset', ''),
            'settle_asset': symbol_info.get('settle_asset', ''),
            'exchange_raw_symbol': symbol_id,
            'tardis_symbol': symbol_id,
            'tardis_exchange': exchange,
            'data_provider': 'tardis',
            'data_types': available_data_types,
            'venue_type': 'exchange',
            'asset_class': 'crypto',
            'min_size': None,  # Unknown - to be populated later
            'ccxt_symbol': '',  # Unknown - to be populated later
            'ccxt_exchange': ''  # Unknown - to be populated later
        }
        
        # Add underlying field for derivatives
        if symbol_type in ['option', 'future']:
            attributes['underlying'] = self._get_underlying_asset(
                symbol_type, 
                symbol_info.get('base_asset', '').upper(),
                symbol_info.get('quote_asset', '').upper()
            )
        
        # Add instrument-specific attributes
        if symbol_type == 'perpetual':
            # Detect inverse perpetuals (coin-margined)
            margin_currency = symbol_info.get('settle_asset', '')
            quote_currency = symbol_info.get('quote_asset', '')
            is_inverse = margin_currency != quote_currency and margin_currency != ''
            attributes['inverse'] = is_inverse
            
        elif symbol_type == 'future':
            expiry = symbol_info.get('expiry', '')
            if expiry:
                try:
                    expiry_date = datetime.strptime(expiry, "%Y-%m-%d")
                    # Set expiry to 8am UTC (standard for crypto futures)
                    expiry_date = expiry_date.replace(hour=8, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
                    attributes['expiry'] = expiry_date  # Store as timezone-aware datetime object
                except:
                    pass
            attributes['contract_size'] = 1.0  # Default for crypto futures
            attributes['tick_size'] = None
            attributes['settlement_type'] = 'cash'
            
        elif symbol_type == 'option':
            expiry = symbol_info.get('expiry', '')
            strike = symbol_info.get('strike', '')
            option_type = symbol_info.get('option_type', '')
            
            if expiry:
                try:
                    expiry_date = datetime.strptime(expiry, "%Y-%m-%d")
                    # Set expiry to 8am UTC (standard for crypto options)
                    expiry_date = expiry_date.replace(hour=8, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
                    attributes['expiry'] = expiry_date  # Store as timezone-aware datetime object
                except:
                    pass
                    
            if strike:
                attributes['strike'] = strike
                
            if option_type:
                # Normalize to CALL/PUT
                attributes['option_type'] = 'CALL' if option_type.upper() in ['C', 'CALL'] else 'PUT'
                
            attributes['contract_size'] = 1.0  # Default for crypto options
            attributes['tick_size'] = None
            attributes['settlement_type'] = 'cash'
            
        return attributes

    def process_exchange_symbols(self, exchange: str, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Process symbols for an exchange and generate canonical instrument keys"""
        print(f"🔄 Processing {exchange} symbols...")
        logger.info(f"Processing {exchange} symbols...")
        
        # Get symbols from Tardis API
        url = f'https://api.tardis.dev/v1/exchanges/{exchange}'
        response = self.session.get(url, headers=self.headers)
        response.raise_for_status()
        
        exchange_data = response.json()
        symbols = exchange_data.get('availableSymbols', [])
        
        print(f"  📊 Found {len(symbols)} raw symbols from API")
        
        instruments = {}
        start_date_str = start_date.strftime("%Y-%m-%d")
        
        # Track processing stats
        stats = {
            'total_symbols': len(symbols),
            'processed': 0,
            'skipped_aggregate': 0,
            'skipped_date_range': 0,
            'skipped_filters': 0,
            'failed_parsing': 0,
            'generated': 0,
            'parsing_failures': []  # Track actual failure details
        }
        
        for symbol in symbols:
            symbol_id = symbol.get('id', '')
            symbol_type = symbol.get('type', '')
            stats['processed'] += 1
            
            # Skip most aggregate entries, but keep OPTIONS for Deribit only
            if symbol_id in ['SPOT', 'PERPETUALS', 'FUTURES', 'COMBOS']:
                stats['skipped_aggregate'] += 1
                continue
            elif symbol_id == 'OPTIONS' and exchange != 'deribit':
                stats['skipped_aggregate'] += 1
                continue
                
            # Parse availability dates and make timezone-aware UTC
            try:
                available_from = datetime.fromisoformat(symbol['availableSince'].replace('Z', '+00:00'))
                
                # Make timezone-aware UTC (same epoch, different timezone)
                if available_from.tzinfo is None:
                    available_from = available_from.replace(tzinfo=timezone.utc)
                
                # Handle availableTo - required for options/futures, optional for spot/perpetuals
                if 'availableTo' in symbol:
                    available_to = datetime.fromisoformat(symbol['availableTo'].replace('Z', '+00:00'))
                    if available_to.tzinfo is None:
                        available_to = available_to.replace(tzinfo=timezone.utc)
                else:
                    # No availableTo field - this is expected for spot/perpetuals
                    if symbol_type in ['spot', 'perpetual']:
                        # For spot/perpetuals, use a far future date since they don't expire
                        available_to = datetime(2099, 12, 31, tzinfo=timezone.utc)
                        if stats['failed_parsing'] <= 3:  # Only log first few
                            print(f"    ℹ️ No expiry date for {symbol_id} ({symbol_type}) - this is expected")
                    else:
                        # For options/futures, try to parse expiry from symbol name
                        parsed_expiry = self._try_parse_expiry_from_symbol(exchange, symbol_id, symbol_type)
                        if parsed_expiry:
                            # Use parsed expiry date
                            available_to = parsed_expiry
                            if stats['failed_parsing'] <= 3:  # Only log first few
                                print(f"    ℹ️ Parsed expiry date for {symbol_id} ({symbol_type}) from symbol name: {parsed_expiry.strftime('%Y-%m-%d')}")
                        else:
                            # For options/futures, availableTo is required
                            # Skip logging errors for combo instruments as they're expected to fail
                            if symbol_type != 'combo':
                                failure_msg = f"Missing expiry date for {symbol_id} ({symbol_type})"
                                stats['failed_parsing'] += 1
                                stats['parsing_failures'].append(failure_msg)
                                if stats['failed_parsing'] <= 3:
                                    print(f"    ❌ {failure_msg} - this is required")
                                continue
                            else:
                                # For combo instruments, skip silently without error logging
                                # Don't count as parsing failures since they're expected to fail
                                continue
                        
            except Exception as e:
                # Skip logging errors for combo instruments as they're expected to fail
                if symbol_type != 'combo':
                    failure_msg = f"Failed to parse dates for {symbol_id}: {e}"
                    stats['failed_parsing'] += 1
                    stats['parsing_failures'].append(failure_msg)
                    if stats['failed_parsing'] <= 3:  # Only log first few parsing errors
                        print(f"    ⚠️ {failure_msg}")
                # Don't count combo instruments as parsing failures since they're expected to fail
                continue
            
            # Adjust futures/options expiry times to 8am UTC for crypto exchanges
            if exchange in ['deribit', 'binance-futures', 'okex-futures', 'okex-swap', 'bybit'] and symbol_type in ['future', 'option']:
                # available_from: add 8 hours
                if available_from.hour == 0:
                    available_from = available_from.replace(hour=8)
                # available_to: set to 8am on the expiry day (subtract 16 hours from midnight of next day)
                if available_to.hour == 0:
                    available_to = (available_to - timedelta(hours=16))
                
            # Check if symbol is available for our date range
            # For backtesting: include instruments that were available on the query date
            if available_from.strftime("%Y-%m-%d") > end_date.strftime("%Y-%m-%d"):
                stats['skipped_date_range'] += 1
                continue
            # Only exclude if available_to is before our query date (instrument wasn't available yet)
            if available_to and available_to.strftime("%Y-%m-%d") < start_date_str:
                stats['skipped_date_range'] += 1
                continue
                
            # Skip data type filtering - we'll hardcode data types based on instrument type
            # This is much faster and more reliable than relying on Tardis API data types
            
            # Apply filters
            if exchange == "deribit" and symbol_type == "combo":
                stats['skipped_filters'] += 1
                continue  # Skip Deribit combos
                
            # Extract symbol details first to get quote currency (no data types needed for extraction)
            symbol_info = self._extract_symbol_details(exchange, symbol_id, symbol_type, [])
            
            if symbol_info:
                # Skip leveraged tokens
                if any(token in symbol_id for token in ['BTCUP', 'BTCDOWN', 'ETHUP', 'ETHDOWN', 'BNBUP', 'BNBDOWN', 'ADAUP', 'ADADOWN']):
                    stats['skipped_filters'] += 1
                    continue
                
                # Apply quote currency filtering based on parsed quote currency
                quote_currency = symbol_info.get('quote_asset', '')
                settle_asset = symbol_info.get('settle_asset', '')  # Use 'settle_asset' not 'margin'
                
                # Check if this is a coin-margined product
                # Coin-margined: settle_asset != quote_currency (e.g., BTC margin with USD quote)
                is_coin_margin = settle_asset and settle_asset != quote_currency
                
                if exchange == "upbit":
                    # Only KRW for Upbit
                    if quote_currency != "KRW":
                        stats['skipped_filters'] += 1
                        continue
                elif exchange == "deribit":
                    # Allow USD, USDT, and USDC for Deribit
                    if quote_currency not in ["USD", "USDT", "USDC"]:
                        stats['skipped_filters'] += 1
                        continue
                else:
                    # For all other exchanges, only allow USDT
                    if quote_currency != "USDT":
                        stats['skipped_filters'] += 1
                        continue
                
                # Update symbol_info expiry to match available_to for crypto futures/options
                if exchange in ['deribit', 'binance-futures', 'okex-futures', 'okex-swap', 'bybit'] and symbol_type in ['future', 'option']:
                    symbol_info['expiry'] = available_to.strftime("%Y-%m-%d")
                
                # Generate canonical instrument key
                instrument_key = self.generate_instrument_key(exchange, symbol_type, symbol_id, symbol_info)
                
                if instrument_key:
                    # Get hardcoded data types based on instrument type
                    hardcoded_data_types = self._get_tardis_data_types(instrument_key)
                    
                    # Generate attributes
                    attributes = self.generate_attributes(exchange, symbol_type, symbol_id, symbol_info, hardcoded_data_types)
                    
                    # Create flattened structure for Parquet/BigQuery compatibility
                    instruments[instrument_key] = {
                'instrument_key': instrument_key,
                
                # Extract canonical venue and instrument type from instrument_key
                'venue': instrument_key.split(':')[0] if ':' in instrument_key else exchange.upper(),
                'instrument_type': instrument_key.split(':')[1] if ':' in instrument_key else symbol_type.upper(),
                
                'available_from_datetime': available_from.isoformat(),
                'available_to_datetime': available_to.isoformat(),
                
                # Flatten data_types as comma-separated string (BigQuery friendly)
                'data_types': ','.join(hardcoded_data_types),
                
                # Flatten attributes dict to individual columns
                'base_asset': attributes.get('base_asset', '').upper(),
                'quote_asset': attributes.get('quote_asset', '').upper(),
                'settle_asset': attributes.get('settle_asset', '').upper(),
                'exchange_raw_symbol': attributes.get('exchange_raw_symbol', ''),
                'tardis_symbol': attributes.get('tardis_symbol', ''),
                'tardis_exchange': attributes.get('tardis_exchange', ''),
                'data_provider': attributes.get('data_provider', ''),
                'venue_type': attributes.get('venue_type', ''),
                'asset_class': attributes.get('asset_class', ''),
                'inverse': bool(attributes.get('inverse', False)),
                
                # Flatten symbol_info dict to individual columns with canonical values
                'symbol_type': symbol_info.get('type', '').upper(),
                'base_asset': symbol_info.get('base_asset', '').upper(),
                'quote_asset': symbol_info.get('quote_asset', '').upper(),
                'settle_asset': symbol_info.get('settle_asset', '').upper(),
                'contract_type': symbol_info.get('contract_type', '').upper(),
                'strike': symbol_info.get('strike', ''),
                'option_type': self._normalize_option_type(symbol_info.get('option_type', '')),
                
                # Additional fields from attributes with proper typing
                'expiry': attributes.get('expiry', ''),
                'contract_size': float(attributes.get('contract_size', 0)) if attributes.get('contract_size') else None,
                'tick_size': float(attributes.get('tick_size', 0)) if attributes.get('tick_size') else None,
                'settlement_type': attributes.get('settlement_type', ''),
                'underlying': attributes.get('underlying', ''),
                'min_size': attributes.get('min_size'),
                'ccxt_symbol': attributes.get('ccxt_symbol', ''),
                'ccxt_exchange': attributes.get('ccxt_exchange', ''),
                    }
                    
                    stats['generated'] += 1
        
        # After processing regular symbols, fetch individual Deribit options
        # Temporarily disabled to avoid hanging
        # if exchange == 'deribit':
        #     deribit_options = self.fetch_deribit_individual_options(exchange, start_date)
        #     instruments.update(deribit_options)
        
        # Print processing summary
        print(f"  ✅ Generated {stats['generated']} instruments")
        if stats['skipped_aggregate'] > 0:
            print(f"  ⏭️ Skipped {stats['skipped_aggregate']} aggregate symbols")
        if stats['skipped_date_range'] > 0:
            print(f"  📅 Skipped {stats['skipped_date_range']} symbols (date range)")
        if stats['skipped_filters'] > 0:
            print(f"  🔍 Skipped {stats['skipped_filters']} symbols (filters)")
        if stats['failed_parsing'] > 0:
            print(f"  ⚠️ Failed to parse {stats['failed_parsing']} symbols")
            # Show all parsing failures for debugging
            print(f"  🔍 All parsing failures:")
            for i, failure in enumerate(stats.get('parsing_failures', []), 1):
                print(f"    {i}. {failure}")
                    
        logger.info(f"Generated {len(instruments)} instrument keys for {exchange}")
        return instruments, stats

    def _extract_symbol_details(self, exchange: str, symbol_id: str, symbol_type: str, data_types: List[str]) -> Dict[str, Any]:
        """Extract detailed symbol information"""
        
        # Debug logging for problematic symbols
        if symbol_id in ['USDT-TRY', 'USDT-EUR', 'USDT-BRL', 'USDC-EUR', 'USDT-USDC']:
            print(f"    🔍 Parsing fiat pair: {symbol_id}")
        
        # Define exchange-specific patterns
        mapping_formats = {
            'deribit': {
                'expiry_pattern': re.compile(r'-(\d{6})-'),
                'expiry_pattern_alt': re.compile(r'-(\d{2}[A-Z]{3}\d{2})-'),
                'expiry_pattern_single_day': re.compile(r'-(\d{1}[A-Z]{3}\d{2})-'),  # For single-digit days like 7NOV25
                'expiry_pattern_future': re.compile(r'-(\d{2}[A-Z]{3}\d{2})$'),  # For futures ending with expiry date
                'expiry_pattern_future_yyyymmdd': re.compile(r'-(\d{6})$'),  # For futures ending with YYMMDD format
                'option_type_pattern': re.compile(r'-(C|P)$'),
                'option_strike_pattern': re.compile(r'-(\d+d?\d*)-')  # Handles both numeric and decimal strikes (1d14, 1d1, etc.)
            },
            'binance-futures': {
                'expiry_pattern': re.compile(r'_(\d{6})$'),
                'option_type_pattern': None,
                'option_strike_pattern': None
            },
            'bybit': {
                'expiry_pattern': re.compile(r'-(\d{2}[A-Z]{3}\d{2})$'),  # DDMMMyy format
                'expiry_pattern_alt': re.compile(r'([A-Z])(\d{2})$'),  # For symbols like BTCUSDM25
                'expiry_pattern_quarterly': re.compile(r'([A-Z])(\d{2})$'),  # For quarterly futures like BTCUSDZ25, BTCUSDH26
                'option_type_pattern': None,
                'option_strike_pattern': None
            },
            'okex-futures': {
                'expiry_pattern': re.compile(r'-(\d{6})$'),  # YYMMDD format for futures ending with expiry date
                'option_type_pattern': None,
                'option_strike_pattern': None
            },
            'okex-swap': {
                'expiry_pattern': None,  # Perpetuals don't have expiry
                'option_type_pattern': None,
                'option_strike_pattern': None
            },
            'upbit': {
                'expiry_pattern': None,  # Spot trading
                'option_type_pattern': None,
                'option_strike_pattern': None
            },
            'okex': {
                'expiry_pattern': re.compile(r'-(\d{6})$'),
                'option_type_pattern': None,
                'option_strike_pattern': None
            },
            'okex-spot': {
                'expiry_pattern': None,
                'option_type_pattern': None,
                'option_strike_pattern': None
            },
            'bybit-spot': {
                'expiry_pattern': None,
                'option_type_pattern': None,
                'option_strike_pattern': None
            }
        }
        
        def remove_suffix(s):
            suffixes = ["USDT", "USDC", "BUSD", "USD", "DAI", "GBP", "TUSD", "EUR", "TRY", "BRL", "JPY", "KRW", "CNY", "HKD"]
            for suffix in suffixes:
                if re.search(suffix, s, re.IGNORECASE):
                    s = re.sub(suffix, '', s, flags=re.IGNORECASE)
                    return s, suffix
            return s, "USD"
        
        def parse_upbit_symbol(symbol_id):
            """Parse Upbit symbols like BTC-ETH"""
            if '-' in symbol_id:
                parts = symbol_id.split('-')
                if len(parts) == 2:
                    return parts[0], parts[1]
            return None, None
        
        # Extract base and quote currency
        if exchange == 'upbit':
            # Handle Upbit format: BTC-ETH
            base_currency, quote_currency = parse_upbit_symbol(symbol_id)
            if not base_currency or not quote_currency:
                return {
                    'id': symbol_id,
                    'type': symbol_type,
                    'base_asset': '',
                    'quote_asset': '',
                    'settle_asset': '',
                    'contract_type': '',
                    'expiry': '',
                    'strike': '',
                    'option_type': ''
                }
            margin_currency = quote_currency  # KRW margin for Upbit
        elif exchange == 'deribit' and symbol_type in ['future', 'option']:
            # Handle Deribit coin margin futures/options
            if symbol_id == 'OPTIONS':
                # This is the aggregate options symbol - we'll handle it specially
                base_currency = "BTC"  # Default to BTC for options chain
                quote_currency = "USD"
                margin_currency = base_currency
            elif '-' in symbol_id:
                # Format: BTC-29DEC23-50000-C or BTC-29DEC23
                parts = symbol_id.split('-')
                base_currency = parts[0]  # BTC, ETH, SOL, etc.
                quote_currency = "USD"  # Deribit futures/options are USD quoted
                margin_currency = base_currency  # Coin margin
            else:
                return {
                    'id': symbol_id,
                    'type': symbol_type,
                    'base_asset': '',
                    'quote_asset': '',
                    'settle_asset': '',
                    'contract_type': '',
                    'expiry': '',
                    'strike': '',
                    'option_type': ''
                }
        else:
            # Handle other exchanges
            # First check if this is a fiat currency pair (e.g., USDT-TRY, USDC-EUR)
            if '-' in symbol_id and not any(char.isdigit() for char in symbol_id):
                # This looks like a fiat currency pair
                parts = symbol_id.split('-')
                if len(parts) == 2:
                    base_currency = parts[0]
                    quote_currency = parts[1]
                    margin_currency = quote_currency
                else:
                    # Fall back to original logic
                    string, quote_currency = remove_suffix(symbol_id)
                    match = re.match(r'^[0-9]*([A-Za-z]+|[A-Za-z0-9]+(?=-))', string)
                    
                    if not match:
                        return {
                            'id': symbol_id,
                            'type': symbol_type,
                            'base_asset': '',
                            'quote_asset': quote_currency,
                            'settle_asset': '',
                            'contract_type': '',
                            'expiry': '',
                            'strike': '',
                            'option_type': ''
                        }
                        
                    base_currency = match.group(1)
                    margin_currency = base_currency if quote_currency == "USD" else quote_currency
            else:
                # Original logic for crypto pairs
                string, quote_currency = remove_suffix(symbol_id)
                match = re.match(r'^[0-9]*([A-Za-z]+|[A-Za-z0-9]+(?=-))', string)
                
                if not match:
                    return {
                        'id': symbol_id,
                        'type': symbol_type,
                        'base_asset': '',
                        'quote_asset': quote_currency,
                        'settle_asset': '',
                        'contract_type': '',
                        'expiry': '',
                        'strike': '',
                        'option_type': ''
                    }
                    
                base_currency = match.group(1)
                margin_currency = base_currency if quote_currency == "USD" else quote_currency
        
        # Initialize symbol info
        symbol_info = {
            'id': symbol_id,
            'type': symbol_type,
            'base_asset': base_currency,
            'quote_asset': quote_currency,
            'settle_asset': margin_currency,
            'contract_type': '',
            'expiry': '',
            'strike': '',
            'option_type': ''
        }
        
        # Enhanced parsing for futures and options
        if exchange in mapping_formats and symbol_type in ['future', 'option']:
            mapping = mapping_formats[exchange]
            
            # Extract expiry
            expiry = None
            for pattern_key in ['expiry_pattern_single_day', 'expiry_pattern_alt', 'expiry_pattern_future', 'expiry_pattern_future_yyyymmdd', 'expiry_pattern']:
                pattern = mapping.get(pattern_key)
                if pattern:
                    expiry_match = pattern.search(symbol_id)
                    if expiry_match:
                        expiry_str = expiry_match.group(1)
                        try:
                            if pattern_key == 'expiry_pattern':
                                if exchange == 'bybit':
                                    # DDMMMyy format for Bybit
                                    day = int(expiry_str[:2])
                                    month_str = expiry_str[2:5]
                                    year = 2000 + int(expiry_str[5:7])
                                    
                                    month_map = {
                                        'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4,
                                        'MAY': 5, 'JUN': 6, 'JUL': 7, 'AUG': 8,
                                        'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
                                    }
                                    month = month_map.get(month_str)
                                    if month:
                                        expiry = datetime(year, month, day)
                                else:
                                    # YYMMDD format
                                    year = 2000 + int(expiry_str[:2])
                                    month = int(expiry_str[2:4])
                                    day = int(expiry_str[4:6])
                                    expiry = datetime(year, month, day)
                            elif pattern_key == 'expiry_pattern_alt':
                                if exchange == 'bybit' and len(expiry_match.groups()) == 2:
                                    # Bybit month code format: M25 (Month + Year)
                                    month_code = expiry_match.group(1)
                                    year = 2000 + int(expiry_match.group(2))
                                    
                                    month_map = {
                                        'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
                                        'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12
                                    }
                                    month = month_map.get(month_code)
                                    if month:
                                        # Use last day of month
                                        if month == 12:
                                            expiry = datetime(year + 1, 1, 1) - timedelta(days=1)
                                        else:
                                            expiry = datetime(year, month + 1, 1) - timedelta(days=1)
                                else:
                                    # DDMMMyy format
                                    day = int(expiry_str[:2])
                                    month_str = expiry_str[2:5]
                                    year = 2000 + int(expiry_str[5:7])
                                    
                                    month_map = {
                                        'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4,
                                        'MAY': 5, 'JUN': 6, 'JUL': 7, 'AUG': 8,
                                        'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
                                    }
                                    month = month_map.get(month_str)
                                    if month:
                                        expiry = datetime(year, month, day)
                            elif pattern_key == 'expiry_pattern_single_day':
                                # DMMMyy format for options with single-digit days (e.g., 7NOV25)
                                day = int(expiry_str[:1])
                                month_str = expiry_str[1:4]
                                year = 2000 + int(expiry_str[4:6])
                                
                                month_map = {
                                    'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4,
                                    'MAY': 5, 'JUN': 6, 'JUL': 7, 'AUG': 8,
                                    'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
                                }
                                month = month_map.get(month_str)
                                if month:
                                    expiry = datetime(year, month, day)
                            elif pattern_key == 'expiry_pattern_future':
                                # DDMMMyy format for futures ending with expiry date (e.g., BTC-26DEC25)
                                day = int(expiry_str[:2])
                                month_str = expiry_str[2:5]
                                year = 2000 + int(expiry_str[5:7])
                                
                                month_map = {
                                    'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4,
                                    'MAY': 5, 'JUN': 6, 'JUL': 7, 'AUG': 8,
                                    'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
                                }
                                month = month_map.get(month_str)
                                if month:
                                    expiry = datetime(year, month, day)
                            break
                        except:
                            continue
                            
            if expiry:
                symbol_info['expiry'] = expiry.strftime("%Y-%m-%d")
                
            # Extract option type (Call/Put) - only for options
            if symbol_type == 'option' and mapping.get('option_type_pattern'):
                option_type_match = mapping['option_type_pattern'].search(symbol_id)
                if option_type_match:
                    symbol_info['option_type'] = "Call" if option_type_match.group(1) == 'C' else "Put"
                    
            # Extract strike price - only for options
            if symbol_type == 'option' and mapping.get('option_strike_pattern'):
                strike_match = mapping['option_strike_pattern'].search(symbol_id)
                if strike_match:
                    strike_raw = strike_match.group(1)
                    # Convert Deribit decimal format (1d14 -> 1.14, 1d1 -> 1.1)
                    if 'd' in strike_raw:
                        strike_raw = strike_raw.replace('d', '.')
                    symbol_info['strike'] = strike_raw
                    
        return symbol_info

    def _try_parse_expiry_from_symbol(self, exchange: str, symbol_id: str, symbol_type: str) -> datetime:
        """Try to parse expiry date from symbol name for futures/options without availableTo field"""
        if symbol_type not in ['future', 'option']:
            return None
        
             
        # Define exchange-specific patterns
        mapping_formats = {
            'deribit': {
                'expiry_pattern': re.compile(r'-(\d{6})-'),
                'expiry_pattern_alt': re.compile(r'-(\d{2}[A-Z]{3}\d{2})-'),
                'expiry_pattern_single_day': re.compile(r'-(\d{1}[A-Z]{3}\d{2})-'),  # For single-digit days like 7NOV25
                'expiry_pattern_future': re.compile(r'-(\d{2}[A-Z]{3}\d{2})$'),
                'expiry_pattern_future_yyyymmdd': re.compile(r'-(\d{6})$'),  # For futures ending with YYMMDD format
                'expiry_pattern_quarterly': re.compile(r'([A-Z])(\d{2})$')  # For quarterly futures like BTCUSDZ25, BTCUSDH26
            },
            'binance-futures': {
                'expiry_pattern': re.compile(r'_(\d{6})$'),  # YYMMDD format for futures ending with expiry date
                'option_type_pattern': None,
                'option_strike_pattern': None
            },
            'bybit': {
                'expiry_pattern': re.compile(r'-(\d{2}[A-Z]{3}\d{2})$'),  # DDMMMyy format
                'expiry_pattern_alt': re.compile(r'([A-Z])(\d{2})$'),  # For symbols like BTCUSDM25
                'expiry_pattern_quarterly': re.compile(r'([A-Z])(\d{2})$'),  # For quarterly futures like BTCUSDZ25, BTCUSDH26
                'option_type_pattern': None,
                'option_strike_pattern': None
            },
            'okex-futures': {
                'expiry_pattern': re.compile(r'-(\d{6})$'),  # YYMMDD format for futures ending with expiry date
                'option_type_pattern': None,
                'option_strike_pattern': None
            }
        }
        
        if exchange not in mapping_formats:
            return None
            
        mapping = mapping_formats[exchange]
        
        # Try all patterns
        for pattern_key in ['expiry_pattern_single_day', 'expiry_pattern_alt', 'expiry_pattern_future', 'expiry_pattern_future_yyyymmdd', 'expiry_pattern_quarterly', 'expiry_pattern']:
            pattern = mapping.get(pattern_key)
            if pattern:
                expiry_match = pattern.search(symbol_id)
                if expiry_match:
                    expiry_str = expiry_match.group(1)
                    try:
                        if pattern_key == 'expiry_pattern_future':
                            # DDMMMyy format for futures ending with expiry date
                            day = int(expiry_str[:2])
                            month_str = expiry_str[2:5]
                            year = 2000 + int(expiry_str[5:7])
                            
                            month_map = {
                                'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4,
                                'MAY': 5, 'JUN': 6, 'JUL': 7, 'AUG': 8,
                                'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
                            }
                            month = month_map.get(month_str)
                            if month:
                                return datetime(year, month, day, tzinfo=timezone.utc)
                        elif pattern_key == 'expiry_pattern_alt':
                            # DDMMMyy format for options
                            day = int(expiry_str[:2])
                            month_str = expiry_str[2:5]
                            year = 2000 + int(expiry_str[5:7])
                            
                            month_map = {
                                'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4,
                                'MAY': 5, 'JUN': 6, 'JUL': 7, 'AUG': 8,
                                'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
                            }
                            month = month_map.get(month_str)
                            if month:
                                return datetime(year, month, day, tzinfo=timezone.utc)
                        elif pattern_key == 'expiry_pattern_single_day':
                            # DMMMyy format for options with single-digit days (e.g., 7NOV25)
                            day = int(expiry_str[:1])
                            month_str = expiry_str[1:4]
                            year = 2000 + int(expiry_str[4:6])
                            
                            month_map = {
                                'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4,
                                'MAY': 5, 'JUN': 6, 'JUL': 7, 'AUG': 8,
                                'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
                            }
                            month = month_map.get(month_str)
                            if month:
                                return datetime(year, month, day, tzinfo=timezone.utc)
                        elif pattern_key == 'expiry_pattern_future_yyyymmdd':
                                # YYMMDD format for futures ending with expiry date (e.g., BTC-USD-251226)
                                year = 2000 + int(expiry_str[:2])
                                month = int(expiry_str[2:4])
                                day = int(expiry_str[4:6])
                                return datetime(year, month, day, tzinfo=timezone.utc)
                        elif pattern_key == 'expiry_pattern_quarterly':
                            # Quarterly futures format: Z25 (Month + Year)
                            if exchange in ['deribit', 'bybit'] and len(expiry_match.groups()) == 2:
                                month_code = expiry_match.group(1)
                                year = 2000 + int(expiry_match.group(2))
                                
                                month_map = {
                                    'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
                                    'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12
                                }
                                month = month_map.get(month_code)
                                if month:
                                    # Use last day of month for quarterly futures
                                    if month == 12:
                                        return datetime(year + 1, 1, 1, tzinfo=timezone.utc) - timedelta(days=1)
                                    else:
                                        return datetime(year, month + 1, 1, tzinfo=timezone.utc) - timedelta(days=1)
                        elif pattern_key == 'expiry_pattern_alt':
                            # Bybit month code format: M25 (Month + Year)
                            if exchange == 'bybit' and len(expiry_match.groups()) == 2:
                                month_code = expiry_match.group(1)
                                year = 2000 + int(expiry_match.group(2))
                                
                                month_map = {
                                    'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
                                    'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12
                                }
                                month = month_map.get(month_code)
                                if month:
                                    # Use last day of month
                                    if month == 12:
                                        return datetime(year + 1, 1, 1, tzinfo=timezone.utc) - timedelta(days=1)
                                    else:
                                        return datetime(year, month + 1, 1, tzinfo=timezone.utc) - timedelta(days=1)
                            else:
                                # DDMMMyy format for other exchanges
                                day = int(expiry_str[:2])
                                month_str = expiry_str[2:5]
                                year = 2000 + int(expiry_str[5:7])
                                
                                month_map = {
                                    'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4,
                                    'MAY': 5, 'JUN': 6, 'JUL': 7, 'AUG': 8,
                                    'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
                                }
                                month = month_map.get(month_str)
                                if month:
                                    return datetime(year, month, day, tzinfo=timezone.utc)
                        elif pattern_key == 'expiry_pattern':
                            # YYMMDD format (for OKEx futures, Binance futures and others)
                            if exchange in ['okex-futures', 'binance-futures']:
                                year = 2000 + int(expiry_str[:2])
                                month = int(expiry_str[2:4])
                                day = int(expiry_str[4:6])
                                return datetime(year, month, day, tzinfo=timezone.utc)
                            elif exchange == 'bybit':
                                # DDMMMyy format for Bybit futures
                                day = int(expiry_str[:2])
                                month_str = expiry_str[2:5]
                                year = 2000 + int(expiry_str[5:7])
                                
                                month_map = {
                                    'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4,
                                    'MAY': 5, 'JUN': 6, 'JUL': 7, 'AUG': 8,
                                    'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
                                }
                                month = month_map.get(month_str)
                                if month:
                                    return datetime(year, month, day, tzinfo=timezone.utc)
                    except:
                        continue
        return None

    def generate_all_instruments(self, exchanges: List[str], test_date: datetime) -> Dict[str, Any]:
        """Generate canonical instrument keys for all exchanges"""
        all_instruments = {}
        
        for exchange in exchanges:
            try:
                instruments = self.process_exchange_symbols(exchange, test_date, test_date.strftime("%Y-%m-%d"))
                all_instruments.update(instruments)
            except Exception as e:
                logger.error(f"Error processing {exchange}: {e}")
                
        return all_instruments

    def save_instruments(self, instruments: Dict[str, Any], test_date: datetime, json_path: str = None, parquet_path: str = None):
        """Save instruments to JSON and Parquet"""
        if json_path is None:
            json_path = f"data/instrument_availability/{test_date.strftime('%Y-%m-%d')}_enhanced.json"
        if parquet_path is None:
            parquet_path = f"data/instrument_availability/{test_date.strftime('%Y-%m-%d')}_enhanced.parquet"
        
        # Create output directory
        os.makedirs(os.path.dirname(json_path), exist_ok=True)
        
        # Save as JSON
        with open(json_path, 'w') as f:
            json.dump(instruments, f, indent=2)
        logger.info(f"Instrument keys saved to {json_path}")
        
        # Convert to Parquet for efficient querying
        try:
            import pandas as pd
            
            # Flatten results for DataFrame
            flattened_data = []
            for instrument_key, details in instruments.items():
                # Convert datetime objects to ISO strings for Parquet compatibility
                expiry_value = details['attributes'].get('expiry', '')
                if expiry_value and hasattr(expiry_value, 'isoformat'):
                    expiry_str = expiry_value.isoformat()
                else:
                    expiry_str = str(expiry_value) if expiry_value else ''
                
                row = {
                    'instrument_key': instrument_key,
                    'venue': details['venue'],
                    'instrument_type': details['instrument_type'],
                    'symbol_id': details['symbol_id'],
                    'available_from_datetime': details['available_from_datetime'],
                    'available_to_datetime': details['available_to_datetime'],
                    'data_types': ','.join(details['data_types']),
                    'base_asset': details['attributes']['base_asset'],
                    'quote_asset': details['attributes']['quote_asset'],
                    'settle_asset': details['attributes']['settle_asset'],
                    'expiry': expiry_str,
                    'strike': details['attributes'].get('strike', ''),
                    'option_type': details['attributes'].get('option_type', ''),
                    'contract_size': str(details['attributes'].get('contract_size', '')),
                    'tick_size': str(details['attributes'].get('tick_size', '')),
                    'settlement_type': details['attributes'].get('settlement_type', ''),
                    'inverse': str(details['attributes'].get('inverse', '')),
                    'exchange_raw_symbol': details['attributes']['exchange_raw_symbol'],
                    'tardis_symbol': details['attributes']['tardis_symbol'],
                    'tardis_exchange': details['attributes']['tardis_exchange'],
                    'underlying': details['attributes'].get('underlying', '')
                }
                flattened_data.append(row)
            
            # Create DataFrame and save as Parquet
            df = pd.DataFrame(flattened_data)
            df.to_parquet(parquet_path, index=False)
            logger.info(f"Parquet file saved to {parquet_path}")
            
        except ImportError:
            logger.warning("pandas not available, skipping Parquet conversion")

    def fetch_deribit_individual_options(self, exchange: str, date: datetime) -> Dict[str, Any]:
        """
        Fetch individual Deribit option contracts from options_chain data.
        The OPTIONS symbol provides access to options_chain data which contains all individual options.
        """
        instruments = {}
        
        # Fetch options_chain data for this date
        date_str = date.strftime('%Y/%m/%d')
        url = f'https://datasets.tardis.dev/v1/deribit/options_chain/{date_str}/OPTIONS.csv.gz'
        
        try:
            response = self.session.get(url, headers=self.headers, stream=True, timeout=30)
            response.raise_for_status()
            
            # Decompress and parse CSV
            import gzip
            import csv
            from io import StringIO
            
            decompressed = gzip.decompress(response.content).decode('utf-8')
            reader = csv.DictReader(StringIO(decompressed))
            
            # Track unique options
            seen_options = set()
            
            for row in reader:
                # Extract option details from row
                # Columns include: timestamp, symbol, underlying_price, etc.
                option_symbol = row.get('symbol', '')
                
                if option_symbol and option_symbol not in seen_options:
                    seen_options.add(option_symbol)
                    
                    # Parse option symbol (format: BTC-29MAR24-50000-C)
                    symbol_info = self._parse_deribit_option_symbol(option_symbol)
                    
                    if symbol_info:
                        # Generate instrument key following INSTRUMENT_KEY.md
                        # Format: deribit:Option:BTC-USD-50000-240329-C
                        instrument_key = self.generate_instrument_key('deribit', 'option', option_symbol, symbol_info)
                        
                        if instrument_key:
                            # Determine availability dates for this specific option
                            # Use listing date and expiry from symbol
                            attributes = self.generate_attributes('deribit', 'option', option_symbol, symbol_info, ['trades', 'book_snapshot_5', 'options_chain'])
                            
                            instruments[instrument_key] = {
                                'instrument_key': instrument_key,
                                'venue': 'deribit',
                                'instrument_type': 'option',
                                'symbol_id': option_symbol,
                                'available_from': symbol_info.get('listing_date', date.strftime("%Y-%m-%d")),
                                'available_to': symbol_info.get('expiry', date.strftime("%Y-%m-%d")),
                                'available_from_datetime': symbol_info.get('listing_datetime', date.isoformat()),
                                'available_to_datetime': symbol_info.get('expiry_datetime', date.isoformat()),
                                'data_types': ['trades', 'book_snapshot_5', 'options_chain'],
                                'attributes': attributes,
                                'symbol_info': symbol_info
                            }
            
            logger.info(f"Found {len(instruments)} individual Deribit options for {date.strftime('%Y-%m-%d')}")
            return instruments
            
        except Exception as e:
            logger.warning(f"Failed to fetch Deribit options for {date.strftime('%Y-%m-%d')}: {e}")
            return {}

    def _parse_deribit_option_symbol(self, symbol: str) -> Dict[str, Any]:
        """
        Parse Deribit option symbol: BTC-29MAR24-50000-C
        Returns symbol_info dict with base, quote, expiry, strike, option_type
        """
        parts = symbol.split('-')
        if len(parts) != 4:
            return None
        
        base_asset = parts[0]  # BTC, ETH, SOL
        expiry_str = parts[1]  # 29MAR24
        strike = parts[2]      # 50000
        option_type = parts[3] # C or P
        
        # Parse expiry (format: DDMMMyy)
        try:
            from datetime import datetime, timedelta, timezone
            expiry_date = datetime.strptime(expiry_str, '%d%b%y')
            # Deribit options expire at 8am UTC
            expiry_datetime = expiry_date.replace(hour=8, minute=0, second=0, tzinfo=timezone.utc)
        except:
            return None
        
        return {
            'id': symbol,
            'type': 'option',
            'base_asset': base_asset,
            'quote_asset': 'USD',
            'settle_asset': base_asset,  # Coin-margined
            'expiry_datetime': expiry_datetime.isoformat(),
            'strike': strike,
            'option_type': 'C' if option_type == 'C' else 'P',
            # Estimate listing date (typically 1-3 months before expiry)
            'listing_datetime': (expiry_datetime - timedelta(days=90)).isoformat()
        }

def generate_daily_instruments(start_date: datetime, end_date: datetime, output_dir: str):
    """Generate instruments for each day in range"""
    import pandas as pd
    
    config = get_config()
    api_key = config.tardis.api_key
    
    # Define exchanges to check
    exchanges = [
        "binance", "binance-futures", "deribit", 
        "bybit", "bybit-spot", "okex", 
        "okex-futures", "okex-swap", "upbit"
    ]
    
    # Initialize generator
    generator = CanonicalInstrumentKeyGenerator(api_key)
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate instruments for each day
    current_date = start_date
    all_instruments = {}
    daily_stats = {}
    
    while current_date <= end_date:
        logger.info(f"Processing {current_date.strftime('%Y-%m-%d')}")
        
        # Generate instruments for this date
        instruments = generator.generate_all_instruments(exchanges, current_date)
        
        # Save daily files
        json_path = f"{output_dir}/{current_date.strftime('%Y-%m-%d')}_enhanced.json"
        parquet_path = f"{output_dir}/{current_date.strftime('%Y-%m-%d')}_enhanced.parquet"
        
        generator.save_instruments(instruments, current_date, json_path, parquet_path)
        
        # Track statistics
        daily_stats[current_date.strftime('%Y-%m-%d')] = {
            'total_instruments': len(instruments),
            'by_type': {}
        }
        
        # Group by type for stats
        by_type = {}
        for instrument_key, details in instruments.items():
            instrument_type = details['instrument_type']
            if instrument_type not in by_type:
                by_type[instrument_type] = 0
            by_type[instrument_type] += 1
        
        daily_stats[current_date.strftime('%Y-%m-%d')]['by_type'] = by_type
        
        # Merge into all instruments (for aggregated view)
        all_instruments.update(instruments)
        
        current_date += timedelta(days=1)
    
    # Save aggregated file
    aggregated_path = f"{output_dir}/by_all/all_instruments_{start_date.strftime('%Y-%m-%d')}_to_{end_date.strftime('%Y-%m-%d')}.parquet"
    os.makedirs(os.path.dirname(aggregated_path), exist_ok=True)
    
    # Convert to DataFrame and save
    df_data = []
    for instrument_key, details in all_instruments.items():
        # Convert datetime objects to ISO strings for Parquet compatibility
        expiry_value = details['attributes'].get('expiry', '')
        if expiry_value and hasattr(expiry_value, 'isoformat'):
            expiry_str = expiry_value.isoformat()
        else:
            expiry_str = str(expiry_value) if expiry_value else ''
        
        df_data.append({
            'instrument_key': instrument_key,
            'venue': details['venue'],
            'instrument_type': details['instrument_type'],
            'symbol_id': details['symbol_id'],
            'available_from_datetime': details['available_from_datetime'],
            'available_to_datetime': details['available_to_datetime'],
            'data_types': ','.join(details['data_types']),
            'base_asset': details['attributes']['base_asset'],
            'quote_asset': details['attributes']['quote_asset'],
            'settle_asset': details['attributes']['settle_asset'],
            'expiry': expiry_str,
            'strike': details['attributes'].get('strike', ''),
            'option_type': details['attributes'].get('option_type', ''),
            'contract_size': str(details['attributes'].get('contract_size', '')),
            'tick_size': str(details['attributes'].get('tick_size', '')),
            'settlement_type': details['attributes'].get('settlement_type', ''),
            'inverse': str(details['attributes'].get('inverse', '')),
            'exchange_raw_symbol': details['attributes']['exchange_raw_symbol'],
            'tardis_symbol': details['attributes']['tardis_symbol'],
            'tardis_exchange': details['attributes']['tardis_exchange'],
            'underlying': details['attributes'].get('underlying', '')
        })
    
    df = pd.DataFrame(df_data)
    df.to_parquet(aggregated_path, index=False)
    
    # Save summary statistics
    summary_path = f"{output_dir}/summary_stats.json"
    with open(summary_path, 'w') as f:
        json.dump(daily_stats, f, indent=2)
    
    logger.info(f"Generated instruments for {len(daily_stats)} days")
    logger.info(f"Total unique instruments: {len(all_instruments)}")
    logger.info(f"Aggregated file saved to: {aggregated_path}")
    logger.info(f"Summary statistics saved to: {summary_path}")
    
    return all_instruments, daily_stats

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate canonical instrument keys')
    parser.add_argument('--start-date', type=str, default='2023-05-23', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, default='2023-05-23', help='End date (YYYY-MM-DD)')
    parser.add_argument('--output-dir', type=str, default='data/instrument_availability', help='Output directory')
    parser.add_argument('--single-day', action='store_true', help='Generate for single day only')
    
    args = parser.parse_args()
    
    config = get_config()
    api_key = config.tardis.api_key
    
    # Define exchanges to check
    exchanges = [
        "binance", "binance-futures", "deribit", 
        "bybit", "bybit-spot", "okex", 
        "okex-futures", "okex-swap", "upbit"
    ]
    
    # Parse dates and make timezone-aware UTC
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    
    # Initialize generator
    generator = CanonicalInstrumentKeyGenerator(api_key)
    
    if args.single_day or start_date == end_date:
        # Single day mode
        instruments = generator.generate_all_instruments(exchanges, start_date)
        generator.save_instruments(instruments, start_date)
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"CANONICAL INSTRUMENT KEYS - {start_date.strftime('%Y-%m-%d')}")
        print(f"{'='*60}")
        
        # Group by instrument type
        by_type = {}
        for instrument_key, details in instruments.items():
            instrument_type = details['instrument_type']
            if instrument_type not in by_type:
                by_type[instrument_type] = []
            by_type[instrument_type].append(instrument_key)
        
        total_instruments = len(instruments)
        
        for instrument_type, keys in by_type.items():
            print(f"\n{instrument_type.upper()}: {len(keys)} instruments")
            
            # Show sample keys
            for i, key in enumerate(sorted(keys)[:5]):
                print(f"  {i+1}. {key}")
            if len(keys) > 5:
                print(f"  ... and {len(keys) - 5} more")
        
        print(f"\nTotal instruments: {total_instruments}")
        
        # Show sample instrument details
        if instruments:
            sample_key = list(instruments.keys())[0]
            sample_details = instruments[sample_key]
            print(f"\nSample instrument details:")
            print(f"Key: {sample_key}")
            print(f"Venue: {sample_details['venue']}")
            print(f"Type: {sample_details['instrument_type']}")
            print(f"Symbol ID: {sample_details['symbol_id']}")
            print(f"Available: {sample_details['available_from_datetime']} to {sample_details['available_to_datetime']}")
            print(f"Data types: {', '.join(sample_details['data_types'])}")
            print(f"Attributes: {sample_details['attributes']}")
    else:
        # Date range mode
        all_instruments, daily_stats = generate_daily_instruments(start_date, end_date, args.output_dir)
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"CANONICAL INSTRUMENT KEYS - {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        print(f"{'='*60}")
        
        # Group by instrument type
        by_type = {}
        for instrument_key, details in all_instruments.items():
            instrument_type = details['instrument_type']
            if instrument_type not in by_type:
                by_type[instrument_type] = []
            by_type[instrument_type].append(instrument_key)
        
        total_instruments = len(all_instruments)
        
        for instrument_type, keys in by_type.items():
            print(f"\n{instrument_type.upper()}: {len(keys)} instruments")
            
            # Show sample keys
            for i, key in enumerate(sorted(keys)[:5]):
                print(f"  {i+1}. {key}")
            if len(keys) > 5:
                print(f"  ... and {len(keys) - 5} more")
        
        print(f"\nTotal instruments: {total_instruments}")
        print(f"Days processed: {len(daily_stats)}")
        
        # Show sample instrument details
        if all_instruments:
            sample_key = list(all_instruments.keys())[0]
            sample_details = all_instruments[sample_key]
            print(f"\nSample instrument details:")
            print(f"Key: {sample_key}")
            print(f"Venue: {sample_details['venue']}")
            print(f"Type: {sample_details['instrument_type']}")
            print(f"Symbol ID: {sample_details['symbol_id']}")
            print(f"Available: {sample_details['available_from_datetime']} to {sample_details['available_to_datetime']}")
            print(f"Data types: {', '.join(sample_details['data_types'])}")
            print(f"Attributes: {sample_details['attributes']}")

if __name__ == "__main__":
    main()
