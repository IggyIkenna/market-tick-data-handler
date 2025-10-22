"""
Tardis Connector for Market Data Tick Handler

This module handles all communication with the Tardis.dev API, including
authentication, rate limiting, retry logic, and data retrieval.
"""

import asyncio
import aiohttp
import gzip
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from pathlib import Path
import time
import random
import json
from decimal import Decimal
import pandas as pd
import io

from src.models import TradeData, BookSnapshot, DerivativeTicker, Liquidations, TickData, OptionsChain
from config import get_config

logger = logging.getLogger(__name__)

class TokenBucket:
    """Token bucket rate limiter"""
    
    def __init__(self, capacity: int, refill_period: int):
        self.capacity = capacity
        self.tokens = capacity
        self.last_refill = time.time()
        self.refill_period = refill_period  # seconds
    
    async def acquire(self, tokens: int = 1):
        """Acquire tokens from the bucket"""
        await self._refill()
        
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        
        # Calculate sleep time based on refill rate
        sleep_time = (tokens - self.tokens) / (self.capacity / self.refill_period)
        await asyncio.sleep(sleep_time)
        return await self.acquire(tokens)
    
    async def _refill(self):
        """Refill tokens based on time elapsed"""
        now = time.time()
        time_passed = now - self.last_refill
        
        if time_passed >= self.refill_period:
            # Full refill
            self.tokens = self.capacity
            self.last_refill = now
        else:
            # Partial refill based on time passed
            tokens_to_add = int((time_passed / self.refill_period) * self.capacity)
            self.tokens = min(self.capacity, self.tokens + tokens_to_add)
            self.last_refill = now

class RetryStrategy:
    """Base class for retry strategies"""
    
    def __init__(self, max_retries: int = 3):
        self.max_retries = max_retries
    
    async def execute(self, operation, *args, **kwargs):
        """Execute operation with retry logic"""
        for attempt in range(self.max_retries + 1):
            try:
                return await operation(*args, **kwargs)
            except Exception as e:
                if attempt == self.max_retries:
                    raise e
                
                # Improve error message logging
                error_msg = str(e) if str(e).strip() else f"{type(e).__name__}: {e}"
                delay = self._calculate_delay(attempt, e)
                logger.warning(f"Attempt {attempt + 1} failed: {error_msg}. Retrying in {delay:.2f}s")
                await asyncio.sleep(delay)
    
    def _calculate_delay(self, attempt: int, error: Exception) -> float:
        """Calculate delay before retry"""
        return 1.0  # Base implementation

class ExponentialBackoffRetry(RetryStrategy):
    """Exponential backoff retry strategy"""
    
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0, max_delay: float = 60.0):
        super().__init__(max_retries)
        self.base_delay = base_delay
        self.max_delay = max_delay
    
    def _calculate_delay(self, attempt: int, error: Exception) -> float:
        """Calculate exponential backoff delay"""
        delay = self.base_delay * (2 ** attempt)
        delay = min(delay, self.max_delay)
        # Add jitter to prevent thundering herd
        jitter = random.uniform(0, delay * 0.1)
        return delay + jitter

class RateLimitRetry(RetryStrategy):
    """Rate limit specific retry strategy"""
    
    def __init__(self, max_retries: int = 10):
        super().__init__(max_retries)
    
    def _calculate_delay(self, attempt: int, error: Exception) -> float:
        """Calculate delay based on rate limit error"""
        if hasattr(error, 'response') and error.response:
            retry_after = error.response.headers.get('Retry-After')
            if retry_after:
                return float(retry_after)
        
        # Default to exponential backoff with longer delays
        return min(60.0, 2 ** attempt)

@dataclass
class TardisResponse:
    """Response from Tardis API"""
    data: Dict[str, Any]
    status_code: int
    headers: Dict[str, str]
    content_length: int
    response_time: float

class TardisConnector:
    """Async HTTP client for Tardis.dev API"""
    
    def __init__(self, api_key: str = None, max_concurrent: int = None, rate_limit_per_vm: int = None):
        self.config = get_config()
        self.api_key = api_key or self.config.tardis.api_key
        self.base_url = self.config.tardis.base_url
        self.timeout = self.config.tardis.timeout
        self.max_concurrent = max_concurrent or self.config.tardis.max_concurrent
        self.rate_limit_per_vm = rate_limit_per_vm or self.config.tardis.rate_limit_per_vm
        
        # Rate limiting
        self.rate_limiter = TokenBucket(self.rate_limit_per_vm, 86400)  # 1M per day
        self.semaphore = asyncio.Semaphore(self.max_concurrent)
        
        # Retry strategies
        self.retry_strategies = {
            'network': ExponentialBackoffRetry(max_retries=2, base_delay=0.5),  # Reduced retries and faster initial delay
            'rate_limit': RateLimitRetry(max_retries=10),
            'api_error': ExponentialBackoffRetry(max_retries=3, base_delay=1.0)  # Reduced retries
        }
        
        # Session management
        self._session: Optional[aiohttp.ClientSession] = None
        
        # Data type mappings
        self.data_type_mappings = {
            'trades': 'trades',
            'book_snapshot_5': 'book_snapshot_5',
            'derivative_ticker': 'derivative_ticker',
            'liquidations': 'liquidations',
            'options_chain': 'options_chain'
        }
    
    async def __aenter__(self):
        """Async context manager entry"""
        await self._create_session()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self._close_session()
    
    async def _create_session(self):
        """Create aiohttp session"""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            headers = {
                'Authorization': f'Bearer {self.api_key}',
                'User-Agent': 'MarketDataTickHandler/1.0.0',
                'Accept': 'application/json, text/csv, application/gzip'
            }
            
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                headers=headers,
                connector=aiohttp.TCPConnector(limit=100, limit_per_host=30)
            )
    
    async def _close_session(self):
        """Close aiohttp session"""
        if self._session and not self._session.closed:
            await self._session.close()
    
    async def _make_request(self, url: str, params: Dict[str, Any] = None) -> TardisResponse:
        """Make HTTP request with retry logic"""
        await self._create_session()
        
        async def _request():
            async with self.semaphore:
                await self.rate_limiter.acquire()
                
                start_time = time.time()
                try:
                    async with self._session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=self.timeout)) as response:
                        content = await response.read()
                        response_time = time.time() - start_time
                        
                        if response.status == 429:
                            raise aiohttp.ClientResponseError(
                                request_info=response.request_info,
                                history=response.history,
                                status=429,
                                message="Rate limit exceeded"
                            )
                        
                        if response.status >= 400:
                            raise aiohttp.ClientResponseError(
                                request_info=response.request_info,
                                history=response.history,
                                status=response.status,
                                message=f"HTTP {response.status}"
                            )
                        
                        return TardisResponse(
                            data=content,
                            status_code=response.status,
                            headers=dict(response.headers),
                            content_length=len(content),
                            response_time=response_time
                        )
                except asyncio.TimeoutError:
                    raise aiohttp.ServerTimeoutError(f"Request timeout after {self.timeout}s")
                except aiohttp.ClientConnectorError as e:
                    raise aiohttp.ClientConnectorError(f"Connection error: {e}")
                except aiohttp.ClientError as e:
                    raise aiohttp.ClientError(f"Client error: {e}")
        
        # Determine retry strategy based on error type
        try:
            return await self.retry_strategies['network'].execute(_request)
        except aiohttp.ClientResponseError as e:
            if e.status == 429:
                return await self.retry_strategies['rate_limit'].execute(_request)
            else:
                return await self.retry_strategies['api_error'].execute(_request)
    
    def _decompress_data(self, data: bytes, content_encoding: str) -> bytes:
        """Decompress data based on content encoding"""
        if content_encoding == 'gzip':
            return gzip.decompress(data)

        else:
            return data
    
    def _parse_csv_data(self, data: bytes, data_type: str) -> pd.DataFrame:
        """Parse CSV data into pandas DataFrame with proper typing"""
        try:
            # Decode bytes to string
            csv_content = data.decode('utf-8')
            
            # Read CSV into pandas DataFrame
            df = pd.read_csv(io.StringIO(csv_content))
            
            # Remove exchange and symbol columns as requested
            if 'exchange' in df.columns:
                df = df.drop('exchange', axis=1)
            if 'symbol' in df.columns:
                df = df.drop('symbol', axis=1)
            
            # Apply proper typing based on data type
            if data_type == 'trades':
                df = self._type_trades_dataframe(df)
            elif data_type == 'book_snapshot_5':
                df = self._type_book_snapshot_dataframe(df)
            elif data_type == 'derivative_ticker':
                df = self._type_derivative_ticker_dataframe(df)
            elif data_type == 'liquidations':
                df = self._type_liquidations_dataframe(df)
            elif data_type == 'options_chain':
                df = self._type_options_chain_dataframe(df)
            elif data_type == 'quotes':
                df = self._type_quotes_dataframe(df)
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to parse CSV data: {e}")
            return pd.DataFrame()
    
    def _type_trades_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply proper typing to trades DataFrame"""
        # Convert timestamps to int64 (microseconds) - handle NaN values
        df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce').astype('Int64')
        df['local_timestamp'] = pd.to_numeric(df['local_timestamp'], errors='coerce').astype('Int64')
        
        # Convert price and amount to float64
        df['price'] = pd.to_numeric(df['price'], errors='coerce').astype('float64')
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce').astype('float64')
        
        # Keep side and id as strings
        df['side'] = df['side'].astype('string')
        df['id'] = df['id'].astype('string')
        
        return df
    
    def _type_book_snapshot_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply proper typing to book snapshot DataFrame"""
        # Convert timestamps to int64 (microseconds) - handle NaN values
        df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce').astype('Int64')
        df['local_timestamp'] = pd.to_numeric(df['local_timestamp'], errors='coerce').astype('Int64')
        
        # Convert all price and amount columns to float64
        for i in range(5):  # Level 5 book
            if f'bids[{i}].price' in df.columns:
                df[f'bids[{i}].price'] = pd.to_numeric(df[f'bids[{i}].price'], errors='coerce').astype('float64')
            if f'bids[{i}].amount' in df.columns:
                df[f'bids[{i}].amount'] = pd.to_numeric(df[f'bids[{i}].amount'], errors='coerce').astype('float64')
            if f'asks[{i}].price' in df.columns:
                df[f'asks[{i}].price'] = pd.to_numeric(df[f'asks[{i}].price'], errors='coerce').astype('float64')
            if f'asks[{i}].amount' in df.columns:
                df[f'asks[{i}].amount'] = pd.to_numeric(df[f'asks[{i}].amount'], errors='coerce').astype('float64')
        
        return df
    
    def _type_derivative_ticker_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply proper typing to derivative ticker DataFrame"""
        # Convert timestamp to int64 (microseconds) - handle NaN values
        df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce').astype('Int64')
        
        # Convert all numeric columns to float64
        numeric_cols = ['funding_rate', 'predicted_funding_rate', 'open_interest', 
                       'last_price', 'index_price', 'mark_price']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
        
        # Convert funding_timestamp to int64 - handle NaN values
        if 'funding_timestamp' in df.columns:
            df['funding_timestamp'] = pd.to_numeric(df['funding_timestamp'], errors='coerce').astype('Int64')
        
        return df
    
    def _type_liquidations_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply proper typing to liquidations DataFrame"""
        # Convert timestamp to int64 (microseconds) - handle NaN values
        df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce').astype('Int64')
        df['local_timestamp'] = pd.to_numeric(df['local_timestamp'], errors='coerce').astype('Int64')
        
        # Convert price and amount to float64
        df['price'] = pd.to_numeric(df['price'], errors='coerce').astype('float64')
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce').astype('float64')
        
        # Keep side and id as strings
        df['side'] = df['side'].astype('string')
        df['id'] = df['id'].astype('string')
        
        return df
    
    def _type_quotes_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply proper typing to quotes DataFrame"""
        # Convert timestamps to int64 (microseconds)
        df['timestamp'] = df['timestamp'].astype('int64')
        df['local_timestamp'] = df['local_timestamp'].astype('int64')
        
        # Convert price and amount to float64
        df['price'] = df['price'].astype('float64')
        df['amount'] = df['amount'].astype('float64')
        
        # Keep side as string
        df['side'] = df['side'].astype('string')
        
        return df
    
    def _type_options_chain_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply proper typing to options chain DataFrame"""
        # Convert timestamps to int64 (microseconds) - handle NaN values
        df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce').astype('Int64')
        df['local_timestamp'] = pd.to_numeric(df['local_timestamp'], errors='coerce').astype('Int64')
        df['expiration'] = pd.to_numeric(df['expiration'], errors='coerce').astype('Int64')
        
        # Convert numeric fields to float64
        numeric_fields = [
            'strike_price', 'open_interest', 'last_price', 'bid_price', 'bid_amount', 'bid_iv',
            'ask_price', 'ask_amount', 'ask_iv', 'mark_price', 'mark_iv', 'underlying_price',
            'delta', 'gamma', 'vega', 'theta', 'rho'
        ]
        
        for field in numeric_fields:
            if field in df.columns:
                df[field] = pd.to_numeric(df[field], errors='coerce').astype('float64')
        
        # Keep string fields as strings
        string_fields = ['exchange', 'symbol', 'type', 'underlying_index']
        for field in string_fields:
            if field in df.columns:
                df[field] = df[field].astype('string')
        
        return df
    
    def _parse_trades_data(self, records: List[Dict[str, Any]]) -> List[TradeData]:
        """Parse trades data into TradeData objects"""
        trades = []
        
        for record in records:
            try:
                # Parse timestamps (Tardis uses microseconds)
                timestamp = datetime.fromtimestamp(int(record['timestamp']) / 1000000, tz=None)
                local_timestamp = datetime.fromtimestamp(int(record['local_timestamp']) / 1000000, tz=None)
                
                trade = TradeData.from_datetimes(
                    timestamp=timestamp,
                    local_timestamp=local_timestamp,
                    price=Decimal(str(record['price'])),
                    amount=Decimal(str(record['amount'])),  # Tardis uses 'amount' not 'size'
                    side=record.get('side', 'buy'),
                    id=record.get('id', '')
                )
                trades.append(trade)
            except (KeyError, ValueError, TypeError) as e:
                logger.warning(f"Failed to parse trade record: {e}")
                continue
        
        return trades
    
    def _parse_book_snapshot_data(self, records: List[Dict[str, Any]]) -> List[BookSnapshot]:
        """Parse book snapshot data into BookSnapshot objects"""
        snapshots = []
        
        for record in records:
            try:
                # Parse timestamps (Tardis uses microseconds)
                timestamp = datetime.fromtimestamp(int(record['timestamp']) / 1000000, tz=None)
                local_timestamp = datetime.fromtimestamp(int(record['local_timestamp']) / 1000000, tz=None)
                
                # Parse bids and asks using Tardis column format
                bids = []
                asks = []
                
                for i in range(5):  # Level 5 book
                    bid_price_key = f'bids[{i}].price'
                    bid_amount_key = f'bids[{i}].amount'
                    ask_price_key = f'asks[{i}].price'
                    ask_amount_key = f'asks[{i}].amount'
                    
                    if bid_price_key in record and bid_amount_key in record:
                        bid_price = float(record[bid_price_key])
                        bid_amount = float(record[bid_amount_key])
                        if bid_price > 0 and bid_amount > 0:
                            bids.append({'price': bid_price, 'amount': bid_amount})
                    
                    if ask_price_key in record and ask_amount_key in record:
                        ask_price = float(record[ask_price_key])
                        ask_amount = float(record[ask_amount_key])
                        if ask_price > 0 and ask_amount > 0:
                            asks.append({'price': ask_price, 'amount': ask_amount})
                
                snapshot = BookSnapshot.from_datetimes(
                    timestamp=timestamp,
                    local_timestamp=local_timestamp,
                    bids=bids,
                    asks=asks
                )
                snapshots.append(snapshot)
            except (KeyError, ValueError, TypeError) as e:
                logger.warning(f"Failed to parse book snapshot record: {e}")
                continue
        
        return snapshots
    
    def _parse_derivative_ticker_data(self, records: List[Dict[str, Any]]) -> List[DerivativeTicker]:
        """Parse derivative ticker data into DerivativeTicker objects"""
        tickers = []
        
        for record in records:
            try:
                # Parse timestamp (Tardis uses microseconds)
                timestamp = datetime.fromtimestamp(int(record['timestamp']) / 1000000, tz=None)
                
                ticker = DerivativeTicker.from_datetimes(
                    timestamp=timestamp,
                    funding_rate=Decimal(str(record.get('funding_rate', 0))),
                    predicted_funding_rate=Decimal(str(record.get('predicted_funding_rate', 0))),
                    open_interest=Decimal(str(record.get('open_interest', 0))),
                    last_price=Decimal(str(record.get('last_price', 0))),
                    index_price=Decimal(str(record.get('index_price', 0))),
                    mark_price=Decimal(str(record.get('mark_price', 0))),
                    funding_timestamp=int(record.get('funding_timestamp', 0))
                )
                tickers.append(ticker)
            except (KeyError, ValueError, TypeError) as e:
                logger.warning(f"Failed to parse derivative ticker record: {e}")
                continue
        
        return tickers
    
    def _parse_liquidations_data(self, records: List[Dict[str, Any]]) -> List[Liquidations]:
        """Parse liquidations data into Liquidations objects"""
        liquidations = []
        
        for record in records:
            try:
                # Parse timestamps (Tardis uses microseconds)
                timestamp = datetime.fromtimestamp(int(record['timestamp']) / 1000000, tz=None)
                local_timestamp = datetime.fromtimestamp(int(record['local_timestamp']) / 1000000, tz=None)
                
                liquidation = Liquidations.from_datetimes(
                    timestamp=timestamp,
                    local_timestamp=local_timestamp,
                    id=record.get('id', ''),
                    side=record.get('side', ''),
                    price=Decimal(str(record.get('price', 0))),
                    amount=Decimal(str(record.get('amount', 0)))  # Tardis uses 'amount' not 'size'
                )
                liquidations.append(liquidation)
            except (KeyError, ValueError, TypeError) as e:
                logger.warning(f"Failed to parse liquidation record: {e}")
                continue
        
        return liquidations
    
    def _parse_options_chain_data(self, records: List[Dict[str, Any]]) -> List[OptionsChain]:
        """Parse options chain data into OptionsChain objects"""
        options = []
        
        for record in records:
            try:
                # Parse timestamps (Tardis uses microseconds)
                timestamp = int(record['timestamp'])
                local_timestamp = int(record['local_timestamp'])
                expiration = int(record['expiration'])
                
                option = OptionsChain(
                    exchange=record['exchange'],
                    symbol=record['symbol'],
                    timestamp=timestamp,
                    local_timestamp=local_timestamp,
                    type=record['type'],
                    strike_price=Decimal(str(record['strike_price'])),
                    expiration=expiration,
                    underlying_index=record['underlying_index'],
                    open_interest=Decimal(str(record['open_interest'])) if record.get('open_interest') else None,
                    last_price=Decimal(str(record['last_price'])) if record.get('last_price') else None,
                    bid_price=Decimal(str(record['bid_price'])) if record.get('bid_price') else None,
                    bid_amount=Decimal(str(record['bid_amount'])) if record.get('bid_amount') else None,
                    bid_iv=Decimal(str(record['bid_iv'])) if record.get('bid_iv') else None,
                    ask_price=Decimal(str(record['ask_price'])) if record.get('ask_price') else None,
                    ask_amount=Decimal(str(record['ask_amount'])) if record.get('ask_amount') else None,
                    ask_iv=Decimal(str(record['ask_iv'])) if record.get('ask_iv') else None,
                    mark_price=Decimal(str(record['mark_price'])) if record.get('mark_price') else None,
                    mark_iv=Decimal(str(record['mark_iv'])) if record.get('mark_iv') else None,
                    underlying_price=Decimal(str(record['underlying_price'])) if record.get('underlying_price') else None,
                    delta=Decimal(str(record['delta'])) if record.get('delta') else None,
                    gamma=Decimal(str(record['gamma'])) if record.get('gamma') else None,
                    vega=Decimal(str(record['vega'])) if record.get('vega') else None,
                    theta=Decimal(str(record['theta'])) if record.get('theta') else None,
                    rho=Decimal(str(record['rho'])) if record.get('rho') else None,
                )
                options.append(option)
            except (KeyError, ValueError, TypeError) as e:
                logger.warning(f"Failed to parse options chain record: {e}")
                continue
        
        return options
    
    async def download_daily_data(self, instrument_key: str, date: datetime, 
                                data_types: List[str]) -> Dict[str, Any]:
        """Download daily data for specific instrument and data types"""
        logger.info(f"Downloading data for {instrument_key} on {date.date()}")
        
        # Get Tardis exchange and symbol from instrument definitions
        # This should be loaded from GCS instrument definitions
        # For now, we'll parse from the instrument_key format
        # TODO: Load from GCS instrument definitions for better reliability
        
        # Parse venue from instrument_key (e.g., "DERIBIT:PERP:BTC-USDT" -> "deribit")
        venue = instrument_key.split(':')[0].lower()
        
        # Map venue to tardis exchange name
        venue_mapping = {
            'deribit': 'deribit',
            'binance': 'binance',
            'binance-futures': 'binance-futures',  # Different venue - separate endpoint
            'bybit': 'bybit',
            'bybit-spot': 'bybit-spot',  # Different venue - separate endpoint
            'okx': 'okex',
            'okx-futures': 'okex-futures',  # Different venue - separate endpoint
            'okx-swap': 'okex-swap'  # Different venue - separate endpoint
        }
        tardis_exchange = venue_mapping.get(venue, venue)
        
        # For now, we need to reconstruct the tardis_symbol from the instrument_key
        # This is a temporary solution - ideally we'd load from GCS instrument definitions
        symbol_part = instrument_key.split(':')[2]  # e.g., "BTC-USDT" (canonical format)
        # Use canonical format with '-' separators
        canonical_symbol_part = symbol_part
        
        # Convert canonical format back to exchange-specific format
        if venue == 'deribit':
            if 'PERP' in instrument_key:
                tardis_symbol = canonical_symbol_part + '-PERPETUAL'
            elif 'FUTURE' in instrument_key:
                # Extract expiry and convert format
                parts = canonical_symbol_part.split('-')
                if len(parts) >= 2:
                    base_quote = parts[0]  # e.g., "BTC-USD"
                    expiry = parts[1]     # e.g., "241225"
                    tardis_symbol = base_quote + '-' + expiry
                else:
                    tardis_symbol = canonical_symbol_part
            elif 'OPTION' in instrument_key:
                # Extract option details
                parts = canonical_symbol_part.split('-')
                if len(parts) >= 4:
                    base_quote = parts[0]  # e.g., "BTC-USD"
                    expiry = parts[1]     # e.g., "241225"
                    strike = parts[2]     # e.g., "50000"
                    option_type = parts[3]  # e.g., "CALL"
                    tardis_symbol = f"{base_quote}-{expiry}-{strike}-{option_type[0]}"
                else:
                    tardis_symbol = canonical_symbol_part
            else:
                tardis_symbol = canonical_symbol_part
        else:
            # For other exchanges, use simpler mapping
            # Convert canonical format (BTC-USDT) to exchange format (BTCUSDT)
            tardis_symbol = canonical_symbol_part.replace('-', '')
        
        return await self.download_daily_data_direct(tardis_exchange, tardis_symbol, date, data_types)

    async def download_daily_data_direct(self, tardis_exchange: str, tardis_symbol: str, 
                                       date: datetime, data_types: List[str]) -> Dict[str, Any]:
        """Download daily data directly using tardis_exchange and tardis_symbol"""
        logger.info(f"Downloading data for {tardis_exchange}:{tardis_symbol} on {date.date()}")
        
        result = {}
        
        for data_type in data_types:
            if data_type not in self.data_type_mappings:
                logger.warning(f"Unsupported data type: {data_type}")
                continue
            
            try:
                # Build URL
                url = f"{self.base_url}/v1/{tardis_exchange}/{data_type}/{date.strftime('%Y/%m/%d')}/{tardis_symbol}.csv.gz"
                logger.debug(f"Requesting URL: {url}")
                
                # Make request
                response = await self._make_request(url)
                
                # Decompress data
                content_encoding = response.headers.get('content-encoding', '')
                if content_encoding:
                    data = self._decompress_data(response.data, content_encoding)
                elif url.endswith('.gz') or response.data[:2] == b'\x1f\x8b':
                    # Auto-detect gzipped data
                    data = self._decompress_data(response.data, 'gzip')
                else:
                    data = response.data
                
                # Parse CSV data directly to DataFrame with proper typing
                df = self._parse_csv_data(data, data_type)
                
                if not df.empty:
                    result[data_type] = df
                    logger.info(f"Downloaded {len(df)} {data_type} records for {tardis_exchange}:{tardis_symbol}")
                
            except Exception as e:
                logger.error(f"Failed to download {data_type} for {tardis_exchange}:{tardis_symbol}: {e}")
                result[data_type] = []
        
        return result
    
    def save_to_parquet(self, df: pd.DataFrame, output_path: Path) -> None:
        """Save DataFrame to Parquet format with compression"""
        try:
            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save as Parquet with compression
            df.to_parquet(
                output_path,
                engine='pyarrow',
                compression='snappy',
                index=False
            )
            logger.info(f"Saved {len(df)} records to {output_path}")
            
        except Exception as e:
            logger.error(f"Failed to save DataFrame to Parquet: {e}")
            raise
    
    async def test_connection(self) -> bool:
        """Test connection to Tardis API"""
        try:
            # Test with a simple request
            url = f"{self.base_url}/v1/binance/trades/2024/01/01/BTCUSDT.csv.gz"
            response = await self._make_request(url)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    async def get_rate_limit_info(self) -> Dict[str, Any]:
        """Get current rate limit information"""
        return {
            'tokens_remaining': self.rate_limiter.tokens,
            'capacity': self.rate_limiter.capacity,
            'refill_period': self.rate_limiter.refill_period,
            'max_concurrent': self.max_concurrent
        }
    
    async def close(self):
        """Close the connector"""
        await self._close_session()

# Convenience function for creating connector
async def create_tardis_connector(api_key: str = None) -> TardisConnector:
    """Create and initialize Tardis connector"""
    connector = TardisConnector(api_key=api_key)
    await connector._create_session()
    return connector
