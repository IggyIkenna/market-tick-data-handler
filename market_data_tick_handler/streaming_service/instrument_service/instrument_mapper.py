"""
Instrument Mapper

Maps VENUE names to CCXT exchange IDs and handles symbol conversion.
Uses exchange_raw_symbol or derives from canonical keys.
"""

import logging
from typing import Dict, Optional, Tuple, Any
import re

logger = logging.getLogger(__name__)


class InstrumentMapper:
    """
    Maps between different instrument naming conventions.
    
    Handles conversions between:
    - Canonical instrument keys (VENUE:TYPE:SYMBOL)
    - CCXT exchange IDs and symbols
    - Tardis exchange names and symbols
    - Exchange raw symbols
    """
    
    def __init__(self):
        """Initialize instrument mapper with predefined mappings"""
        
        # VENUE -> CCXT exchange mapping
        self.venue_to_ccxt = {
            'BINANCE-SPOT': 'binance',
            'BINANCE-FUTURES': 'binance',
            'BINANCE-MARGIN': 'binance',
            'DERIBIT': 'deribit',
            'COINBASE-SPOT': 'coinbase',
            'COINBASE-PRO': 'coinbase',
            'KRAKEN-SPOT': 'kraken',
            'KRAKEN-FUTURES': 'kraken',
            'BITFINEX-SPOT': 'bitfinex',
            'HUOBI-SPOT': 'huobi',
            'HUOBI-FUTURES': 'huobi',
            'OKEX-SPOT': 'okx',
            'OKEX-FUTURES': 'okx',
            'BYBIT-SPOT': 'bybit',
            'BYBIT-FUTURES': 'bybit'
        }
        
        # CCXT -> Tardis exchange mapping
        self.ccxt_to_tardis = {
            'binance': 'binance',
            'deribit': 'deribit',
            'coinbase': 'coinbase-pro',
            'kraken': 'kraken',
            'bitfinex': 'bitfinex',
            'huobi': 'huobi-dm',
            'okx': 'okex',
            'bybit': 'bybit'
        }
        
        # Reverse mappings
        self.ccxt_to_venue = {v: k for k, v in self.venue_to_ccxt.items()}
        self.tardis_to_ccxt = {v: k for k, v in self.ccxt_to_tardis.items()}
        
        logger.info("✅ InstrumentMapper initialized")
        logger.info(f"   VENUE mappings: {len(self.venue_to_ccxt)}")
        logger.info(f"   Tardis mappings: {len(self.ccxt_to_tardis)}")
    
    def venue_to_ccxt_exchange(self, venue: str) -> Optional[str]:
        """
        Convert VENUE to CCXT exchange ID.
        
        Args:
            venue: VENUE name (e.g., 'BINANCE-SPOT')
            
        Returns:
            CCXT exchange ID (e.g., 'binance') or None
        """
        return self.venue_to_ccxt.get(venue.upper())
    
    def ccxt_to_venue_name(self, ccxt_exchange: str) -> Optional[str]:
        """
        Convert CCXT exchange ID to VENUE name.
        
        Args:
            ccxt_exchange: CCXT exchange ID
            
        Returns:
            VENUE name or None
        """
        # For exchanges that have multiple venue types, default to spot
        if ccxt_exchange.lower() in self.ccxt_to_venue:
            return self.ccxt_to_venue[ccxt_exchange.lower()]
        
        # Try to find any venue for this exchange
        for venue, ccxt_id in self.venue_to_ccxt.items():
            if ccxt_id == ccxt_exchange.lower():
                return venue
        
        return None
    
    def ccxt_to_tardis_exchange(self, ccxt_exchange: str) -> Optional[str]:
        """Convert CCXT exchange ID to Tardis exchange name"""
        return self.ccxt_to_tardis.get(ccxt_exchange.lower())
    
    def tardis_to_ccxt_exchange(self, tardis_exchange: str) -> Optional[str]:
        """Convert Tardis exchange name to CCXT exchange ID"""
        return self.tardis_to_ccxt.get(tardis_exchange.lower())
    
    def parse_canonical_key(self, instrument_key: str) -> Optional[Dict[str, str]]:
        """
        Parse canonical instrument key into components.
        
        Args:
            instrument_key: Canonical key (e.g., 'BINANCE-SPOT:SPOT_PAIR:BTC-USDT')
            
        Returns:
            Dictionary with parsed components or None if invalid
        """
        try:
            # Handle different canonical key formats
            if instrument_key.count(':') >= 2:
                parts = instrument_key.split(':')
                venue = parts[0]
                instrument_type = parts[1]
                symbol_part = ':'.join(parts[2:])  # Handle complex symbols
                
                return {
                    'venue': venue,
                    'instrument_type': instrument_type,
                    'symbol': symbol_part,
                    'ccxt_exchange': self.venue_to_ccxt_exchange(venue),
                    'tardis_exchange': self.ccxt_to_tardis_exchange(
                        self.venue_to_ccxt_exchange(venue) or ''
                    )
                }
            
            return None
            
        except Exception as e:
            logger.warning(f"Error parsing canonical key {instrument_key}: {e}")
            return None
    
    def convert_symbol_to_ccxt(self, symbol: str, exchange: str) -> str:
        """
        Convert symbol to CCXT format for specific exchange.
        
        Args:
            symbol: Symbol to convert (e.g., 'BTC-USDT', 'BTCUSDT')
            exchange: CCXT exchange ID
            
        Returns:
            CCXT formatted symbol
        """
        # Exchange-specific symbol conversion
        if exchange.lower() == 'binance':
            # Binance CCXT uses '/' format
            if '-' in symbol:
                return symbol.replace('-', '/')
            elif symbol.isupper() and len(symbol) >= 6:
                # Try to split BTCUSDT -> BTC/USDT
                return self._split_binance_symbol(symbol)
            return symbol
        
        elif exchange.lower() == 'deribit':
            # Deribit symbols are usually kept as-is
            return symbol
        
        elif exchange.lower() == 'coinbase':
            # Coinbase uses '-' format
            if '/' in symbol:
                return symbol.replace('/', '-')
            return symbol
        
        else:
            # Default: try '/' format
            if '-' in symbol:
                return symbol.replace('-', '/')
            return symbol
    
    def _split_binance_symbol(self, symbol: str) -> str:
        """
        Split Binance concatenated symbol into base/quote format.
        
        Args:
            symbol: Concatenated symbol (e.g., 'BTCUSDT')
            
        Returns:
            Split symbol (e.g., 'BTC/USDT')
        """
        # Common quote currencies (ordered by length, longest first)
        quote_currencies = ['USDT', 'BUSD', 'USD', 'BTC', 'ETH', 'BNB', 'EUR', 'GBP']
        
        symbol_upper = symbol.upper()
        
        for quote in quote_currencies:
            if symbol_upper.endswith(quote):
                base = symbol_upper[:-len(quote)]
                if len(base) >= 2:  # Ensure base has reasonable length
                    return f"{base}/{quote}"
        
        # If no match found, return original
        return symbol
    
    def convert_symbol_from_ccxt(self, ccxt_symbol: str, target_format: str = 'dash') -> str:
        """
        Convert CCXT symbol to other formats.
        
        Args:
            ccxt_symbol: CCXT symbol (e.g., 'BTC/USDT')
            target_format: Target format ('dash', 'concat', 'original')
            
        Returns:
            Converted symbol
        """
        if target_format == 'dash':
            return ccxt_symbol.replace('/', '-')
        elif target_format == 'concat':
            return ccxt_symbol.replace('/', '')
        else:
            return ccxt_symbol
    
    def create_canonical_key(self, 
                           venue: str,
                           instrument_type: str,
                           symbol: str,
                           expiry: str = None,
                           strike: str = None,
                           option_type: str = None) -> str:
        """
        Create canonical instrument key from components.
        
        Args:
            venue: VENUE name
            instrument_type: Instrument type
            symbol: Symbol
            expiry: Expiry date (for futures/options)
            strike: Strike price (for options)
            option_type: Option type (for options)
            
        Returns:
            Canonical instrument key
        """
        key = f"{venue.upper()}:{instrument_type.upper()}:{symbol.upper()}"
        
        if instrument_type.upper() == 'OPTION' and expiry and strike and option_type:
            key += f"-{expiry}-{strike}-{option_type.upper()}"
        elif instrument_type.upper() == 'FUTURE' and expiry:
            key += f"-{expiry}"
        
        return key
    
    def get_exchange_info(self, exchange_identifier: str) -> Dict[str, Any]:
        """
        Get comprehensive exchange information from any identifier.
        
        Args:
            exchange_identifier: Any exchange identifier (VENUE, CCXT, Tardis)
            
        Returns:
            Dictionary with all exchange mappings
        """
        exchange_identifier = exchange_identifier.upper()
        
        # Try as VENUE first
        ccxt_exchange = self.venue_to_ccxt_exchange(exchange_identifier)
        if ccxt_exchange:
            return {
                'venue': exchange_identifier,
                'ccxt_exchange': ccxt_exchange,
                'tardis_exchange': self.ccxt_to_tardis_exchange(ccxt_exchange),
                'type': 'venue'
            }
        
        # Try as CCXT exchange
        exchange_identifier_lower = exchange_identifier.lower()
        if exchange_identifier_lower in self.ccxt_to_tardis:
            venue = self.ccxt_to_venue_name(exchange_identifier_lower)
            return {
                'venue': venue,
                'ccxt_exchange': exchange_identifier_lower,
                'tardis_exchange': self.ccxt_to_tardis[exchange_identifier_lower],
                'type': 'ccxt'
            }
        
        # Try as Tardis exchange
        ccxt_exchange = self.tardis_to_ccxt_exchange(exchange_identifier_lower)
        if ccxt_exchange:
            venue = self.ccxt_to_venue_name(ccxt_exchange)
            return {
                'venue': venue,
                'ccxt_exchange': ccxt_exchange,
                'tardis_exchange': exchange_identifier_lower,
                'type': 'tardis'
            }
        
        return {
            'venue': None,
            'ccxt_exchange': None,
            'tardis_exchange': None,
            'type': 'unknown'
        }
    
    def validate_mapping(self, venue: str, ccxt_symbol: str) -> bool:
        """
        Validate that a venue and CCXT symbol combination is valid.
        
        Args:
            venue: VENUE name
            ccxt_symbol: CCXT symbol
            
        Returns:
            True if valid combination
        """
        ccxt_exchange = self.venue_to_ccxt_exchange(venue)
        if not ccxt_exchange:
            return False
        
        # Basic symbol format validation
        if '/' not in ccxt_symbol and ccxt_exchange == 'binance':
            return False  # Binance CCXT symbols should have '/'
        
        return True


# Example usage and testing
if __name__ == "__main__":
    mapper = InstrumentMapper()
    
    # Test venue to CCXT mapping
    print("Venue to CCXT mappings:")
    test_venues = ['BINANCE-SPOT', 'DERIBIT', 'COINBASE-SPOT']
    for venue in test_venues:
        ccxt_id = mapper.venue_to_ccxt_exchange(venue)
        print(f"  {venue} -> {ccxt_id}")
    
    # Test canonical key parsing
    print("\nCanonical key parsing:")
    test_keys = [
        'BINANCE-SPOT:SPOT_PAIR:BTC-USDT',
        'DERIBIT:OPTION:BTC-USD-241225-50000-CALL',
        'BINANCE-FUTURES:PERPETUAL:BTC-USDT'
    ]
    for key in test_keys:
        parsed = mapper.parse_canonical_key(key)
        print(f"  {key}")
        print(f"    -> {parsed}")
    
    # Test symbol conversion
    print("\nSymbol conversion:")
    test_symbols = ['BTC-USDT', 'BTCUSDT', 'BTC/USDT']
    for symbol in test_symbols:
        ccxt_symbol = mapper.convert_symbol_to_ccxt(symbol, 'binance')
        print(f"  {symbol} -> {ccxt_symbol} (Binance)")
    
    # Test exchange info
    print("\nExchange info:")
    test_exchanges = ['BINANCE-SPOT', 'binance', 'coinbase-pro']
    for exchange in test_exchanges:
        info = mapper.get_exchange_info(exchange)
        print(f"  {exchange} -> {info}")
