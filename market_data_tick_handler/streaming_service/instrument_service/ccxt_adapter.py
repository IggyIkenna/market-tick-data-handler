"""
CCXT Adapter

Converts CCXT market data to InstrumentDefinition format.
Fills additional fields: tick_size, contract_size, min_size, ccxt_symbol, trading_fees, limits, precision.
Addresses Issue #004 - Live CCXT Instrument Definitions.
"""

import logging
import ccxt
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List
from dataclasses import asdict

from ...models import InstrumentDefinition

logger = logging.getLogger(__name__)


class CCXTAdapter:
    """
    Adapts CCXT market data to InstrumentDefinition format.
    
    Leverages logic from genInstrumentDefinitions.py but simplified for live data.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize CCXT adapter.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config or {}
        self.exchanges = {}
        self.markets_cache = {}
        self.cache_ttl = self.config.get('cache_ttl', 300)  # 5 minutes
        
        logger.info("✅ CCXTAdapter initialized")
    
    async def initialize_exchange(self, exchange_id: str, api_config: Dict[str, Any] = None) -> bool:
        """
        Initialize a CCXT exchange.
        
        Args:
            exchange_id: CCXT exchange ID (e.g., 'binance')
            api_config: API configuration (keys, sandbox mode, etc.)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not hasattr(ccxt, exchange_id):
                logger.error(f"❌ Exchange {exchange_id} not supported by CCXT")
                return False
            
            exchange_class = getattr(ccxt, exchange_id)
            
            # Default configuration
            config = {
                'apiKey': api_config.get('api_key') if api_config else None,
                'secret': api_config.get('secret') if api_config else None,
                'sandbox': api_config.get('sandbox', False) if api_config else False,
                'enableRateLimit': True,
                'timeout': 30000
            }
            
            # Remove None values
            config = {k: v for k, v in config.items() if v is not None}
            
            exchange = exchange_class(config)
            
            # Test connection by loading markets
            await exchange.load_markets()
            
            self.exchanges[exchange_id] = exchange
            self.markets_cache[exchange_id] = {
                'markets': exchange.markets,
                'timestamp': datetime.now(timezone.utc)
            }
            
            logger.info(f"✅ Initialized CCXT exchange: {exchange_id}")
            logger.info(f"   Markets loaded: {len(exchange.markets)}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error initializing {exchange_id}: {e}")
            return False
    
    async def get_instrument_definition(self, 
                                     exchange_id: str,
                                     symbol: str,
                                     base_currency: str = None,
                                     quote_currency: str = None,
                                     product_type: str = None) -> Optional[InstrumentDefinition]:
        """
        Get live instrument definition from CCXT.
        
        Args:
            exchange_id: CCXT exchange ID
            symbol: Trading symbol (CCXT format or exchange raw symbol)
            base_currency: Base currency filter
            quote_currency: Quote currency filter  
            product_type: Product type filter
            
        Returns:
            InstrumentDefinition or None if not found
        """
        try:
            # Ensure exchange is initialized
            if exchange_id not in self.exchanges:
                success = await self.initialize_exchange(exchange_id)
                if not success:
                    return None
            
            exchange = self.exchanges[exchange_id]
            
            # Refresh markets if cache is stale
            await self._refresh_markets_if_needed(exchange_id)
            
            # Find market by symbol
            market = None
            markets = self.markets_cache[exchange_id]['markets']
            
            # Try direct symbol lookup
            if symbol in markets:
                market = markets[symbol]
            else:
                # Search by criteria
                for market_symbol, market_data in markets.items():
                    if self._matches_criteria(market_data, symbol, base_currency, quote_currency, product_type):
                        market = market_data
                        break
            
            if not market:
                logger.warning(f"Market not found: {exchange_id}:{symbol}")
                return None
            
            # Convert to InstrumentDefinition
            return self._convert_to_instrument_definition(market, exchange_id)
            
        except Exception as e:
            logger.error(f"❌ Error getting instrument definition: {e}")
            return None
    
    def _matches_criteria(self, 
                         market: Dict[str, Any], 
                         symbol: str,
                         base_currency: str,
                         quote_currency: str,
                         product_type: str) -> bool:
        """Check if market matches search criteria"""
        # Symbol match (flexible)
        if symbol:
            if (market['symbol'] == symbol or 
                market['id'] == symbol or
                market.get('info', {}).get('symbol') == symbol):
                pass  # Symbol matches
            else:
                return False
        
        # Base currency match
        if base_currency and market['base'] != base_currency:
            # Handle 1000COIN instruments
            if not (market['base'].startswith('1000') and market['base'][4:] == base_currency):
                return False
        
        # Quote currency match
        if quote_currency and market['quote'] != quote_currency:
            return False
        
        # Product type match
        if product_type:
            ccxt_type = market.get('type', 'spot')
            if not self._type_matches(ccxt_type, product_type):
                return False
        
        return True
    
    def _type_matches(self, ccxt_type: str, product_type: str) -> bool:
        """Check if CCXT type matches requested product type"""
        type_mapping = {
            'spot': ['spot'],
            'swap': ['perpetual', 'perp', 'swap'],
            'future': ['future'],
            'option': ['option']
        }
        
        return product_type.lower() in type_mapping.get(ccxt_type, [ccxt_type])
    
    async def _refresh_markets_if_needed(self, exchange_id: str) -> None:
        """Refresh markets cache if TTL expired"""
        cache_entry = self.markets_cache.get(exchange_id)
        if not cache_entry:
            return
        
        now = datetime.now(timezone.utc)
        cache_age = (now - cache_entry['timestamp']).total_seconds()
        
        if cache_age > self.cache_ttl:
            logger.info(f"🔄 Refreshing markets cache for {exchange_id}")
            try:
                exchange = self.exchanges[exchange_id]
                await exchange.load_markets()
                
                self.markets_cache[exchange_id] = {
                    'markets': exchange.markets,
                    'timestamp': now
                }
                logger.info(f"✅ Refreshed {len(exchange.markets)} markets for {exchange_id}")
                
            except Exception as e:
                logger.error(f"❌ Error refreshing markets for {exchange_id}: {e}")
    
    def _convert_to_instrument_definition(self, 
                                        market: Dict[str, Any], 
                                        exchange_id: str) -> InstrumentDefinition:
        """
        Convert CCXT market data to InstrumentDefinition.
        
        Fills all required fields including live trading parameters.
        """
        now = datetime.now(timezone.utc)
        
        # Extract basic information
        symbol = market['symbol']
        base = market['base']
        quote = market['quote']
        market_type = market.get('type', 'spot')
        
        # Map to our venue format
        venue_mapping = {
            'binance': 'BINANCE-SPOT' if market_type == 'spot' else 'BINANCE-FUTURES',
            'deribit': 'DERIBIT',
            'coinbase': 'COINBASE-SPOT',
            'kraken': 'KRAKEN-SPOT',
            'bitfinex': 'BITFINEX-SPOT'
        }
        venue = venue_mapping.get(exchange_id, exchange_id.upper())
        
        # Generate instrument key
        if market_type == 'option':
            # Handle options
            expiry = market.get('expiry')
            strike = market.get('strike')
            option_type = market.get('optionType', 'call')
            
            expiry_str = expiry.strftime('%y%m%d') if expiry else '000000'
            strike_str = f"{int(strike)}" if strike else "0"
            
            instrument_key = f"{venue}:OPTION:{base}-{quote}-{expiry_str}-{strike_str}-{option_type.upper()}"
        elif market_type in ['future', 'swap']:
            # Handle futures/perpetuals
            if market.get('expiry'):
                expiry_str = market['expiry'].strftime('%y%m%d')
                instrument_key = f"{venue}:FUTURE:{base}-{quote}-{expiry_str}"
            else:
                instrument_key = f"{venue}:PERPETUAL:{base}-{quote}"
        else:
            # Handle spot
            instrument_key = f"{venue}:SPOT_PAIR:{base}-{quote}"
        
        # Extract trading parameters
        precision = market.get('precision', {})
        limits = market.get('limits', {})
        fees = market.get('fees', {})
        info = market.get('info', {})
        
        # Determine data types based on market type and exchange
        data_types = ['trades']
        if market_type in ['future', 'swap']:
            data_types.extend(['book_snapshot_5', 'derivative_ticker', 'liquidations'])
        elif market_type == 'option':
            data_types.extend(['book_snapshot_5', 'options_chain'])
        else:  # spot
            data_types.append('book_snapshot_5')
        
        # Create InstrumentDefinition
        return InstrumentDefinition(
            instrument_key=instrument_key,
            venue=venue,
            instrument_type=market_type.upper(),
            available_from_datetime=now.isoformat(),
            available_to_datetime=None,  # Live instruments don't have end date
            data_types=','.join(data_types),
            base_asset=base,
            quote_asset=quote,
            settle_asset=market.get('settle', quote),
            exchange_raw_symbol=market['id'],  # Exchange's native symbol
            tardis_symbol=self._convert_to_tardis_symbol(market['id'], exchange_id),
            tardis_exchange=self._convert_to_tardis_exchange(exchange_id),
            data_provider='CCXT',
            venue_type='centralized',
            asset_class='crypto',
            
            # Live trading parameters from CCXT
            tick_size=precision.get('price'),
            min_size=limits.get('amount', {}).get('min'),
            max_size=limits.get('amount', {}).get('max'),
            min_notional=limits.get('cost', {}).get('min'),
            max_notional=limits.get('cost', {}).get('max'),
            
            # Additional CCXT fields
            ccxt_symbol=symbol,
            ccxt_exchange=exchange_id,
            trading_fees_maker=fees.get('trading', {}).get('maker'),
            trading_fees_taker=fees.get('trading', {}).get('taker'),
            
            # Status and metadata
            active=market.get('active', True),
            last_updated=now.isoformat(),
            
            # Market info
            contract_size=info.get('contractSize', 1),
            lot_size=info.get('lotSize'),
            margin_required=market.get('percentage', False),
            
            # Additional precision info
            price_precision=precision.get('price'),
            amount_precision=precision.get('amount'),
            
            # Market limits
            price_limit_min=limits.get('price', {}).get('min'),
            price_limit_max=limits.get('price', {}).get('max'),
            
            # Options-specific fields
            strike_price=market.get('strike') if market_type == 'option' else None,
            expiry_datetime=market.get('expiry').isoformat() if market.get('expiry') else None,
            option_type=market.get('optionType') if market_type == 'option' else None
        )
    
    def _convert_to_tardis_symbol(self, ccxt_symbol: str, exchange_id: str) -> str:
        """Convert CCXT symbol to Tardis format"""
        # Simple conversion - may need exchange-specific logic
        tardis_mapping = {
            'binance': lambda s: s.lower().replace('/', ''),
            'deribit': lambda s: s,
            'coinbase': lambda s: s.replace('/', '-'),
        }
        
        converter = tardis_mapping.get(exchange_id, lambda s: s)
        return converter(ccxt_symbol)
    
    def _convert_to_tardis_exchange(self, exchange_id: str) -> str:
        """Convert CCXT exchange ID to Tardis exchange name"""
        tardis_mapping = {
            'binance': 'binance',
            'deribit': 'deribit', 
            'coinbase': 'coinbase-pro',
            'kraken': 'kraken',
            'bitfinex': 'bitfinex'
        }
        
        return tardis_mapping.get(exchange_id, exchange_id)
    
    async def get_all_instruments(self, 
                                exchange_id: str,
                                filters: Dict[str, Any] = None) -> List[InstrumentDefinition]:
        """
        Get all instrument definitions for an exchange.
        
        Args:
            exchange_id: CCXT exchange ID
            filters: Optional filters (base_currencies, quote_currencies, types)
            
        Returns:
            List of InstrumentDefinition objects
        """
        try:
            if exchange_id not in self.exchanges:
                success = await self.initialize_exchange(exchange_id)
                if not success:
                    return []
            
            await self._refresh_markets_if_needed(exchange_id)
            
            markets = self.markets_cache[exchange_id]['markets']
            instruments = []
            
            filters = filters or {}
            base_currencies = filters.get('base_currencies', [])
            quote_currencies = filters.get('quote_currencies', [])
            types = filters.get('types', [])
            
            for market in markets.values():
                # Apply filters
                if base_currencies and market['base'] not in base_currencies:
                    continue
                if quote_currencies and market['quote'] not in quote_currencies:
                    continue
                if types and market.get('type') not in types:
                    continue
                
                # Skip inactive markets
                if not market.get('active', True):
                    continue
                
                # Convert to InstrumentDefinition
                try:
                    instrument = self._convert_to_instrument_definition(market, exchange_id)
                    instruments.append(instrument)
                except Exception as e:
                    logger.warning(f"Error converting market {market['symbol']}: {e}")
                    continue
            
            logger.info(f"✅ Retrieved {len(instruments)} instruments from {exchange_id}")
            return instruments
            
        except Exception as e:
            logger.error(f"❌ Error getting all instruments from {exchange_id}: {e}")
            return []
    
    async def shutdown(self) -> None:
        """Shutdown adapter and close exchange connections"""
        logger.info("🛑 Shutting down CCXTAdapter...")
        
        for exchange_id, exchange in self.exchanges.items():
            try:
                if hasattr(exchange, 'close'):
                    await exchange.close()
                logger.info(f"   Closed {exchange_id}")
            except Exception as e:
                logger.warning(f"Error closing {exchange_id}: {e}")
        
        self.exchanges.clear()
        self.markets_cache.clear()
        
        logger.info("✅ CCXTAdapter shutdown complete")


# Example usage
if __name__ == "__main__":
    import asyncio
    
    async def test_ccxt_adapter():
        adapter = CCXTAdapter()
        
        # Test Binance
        success = await adapter.initialize_exchange('binance')
        if success:
            # Get specific instrument
            btc_usdt = await adapter.get_instrument_definition(
                'binance', 'BTC/USDT', 'BTC', 'USDT', 'spot'
            )
            if btc_usdt:
                print(f"BTC-USDT instrument: {btc_usdt.instrument_key}")
                print(f"  Tick size: {btc_usdt.tick_size}")
                print(f"  Min size: {btc_usdt.min_size}")
                print(f"  Active: {btc_usdt.active}")
            
            # Get all USDT pairs
            instruments = await adapter.get_all_instruments(
                'binance',
                filters={'quote_currencies': ['USDT'], 'types': ['spot']}
            )
            print(f"Found {len(instruments)} USDT spot pairs")
        
        await adapter.shutdown()
    
    asyncio.run(test_ccxt_adapter())
