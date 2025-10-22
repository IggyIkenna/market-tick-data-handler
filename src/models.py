"""
Data Models for Market Data Tick Handler

This module defines all data models following the INSTRUMENT_KEY_SPEC.md format
with proper expiry, call/put, and margin currency support.
"""

from enum import Enum
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
from decimal import Decimal

class Venue(str, Enum):
    """Supported venues for instruments"""
    BINANCE = "binance"
    BINANCE_SPOT = "binance-spot"
    BINANCE_FUTURES = "binance-futures"
    BYBIT = "bybit"
    OKX = "okx"
    DERIBIT = "deribit"
    COINBASE = "coinbase"
    KRAKEN = "kraken"
    AAVE_V3 = "aave_v3"
    ETHERFI = "etherfi"
    LIDO = "lido"
    WALLET = "wallet"
    CME = "cme"
    NASDAQ = "nasdaq"

class InstrumentType(str, Enum):
    """Supported instrument types"""
    BASE_TOKEN = "BaseToken"
    SPOT_PAIR = "SPOT_ASSET"
    PERP = "Perp"
    FUTURE = "Future"
    OPTION = "Option"
    EQUITY = "Equity"
    INDEX = "Index"
    LST = "LST"
    A_TOKEN = "aToken"
    DEBT_TOKEN = "debtToken"

@dataclass
class InstrumentKey:
    """Instrument key following venue:instrument_type:symbol format"""
    venue: Venue
    instrument_type: InstrumentType
    symbol: str
    expiry: Optional[str] = None  # For futures/options
    option_type: Optional[str] = None  # C or P for options
    
    def __str__(self) -> str:
        """Format: venue:type:symbol:expiry:option_type"""
        parts = [self.venue.value, self.instrument_type.value, self.symbol]
        if self.expiry:
            parts.append(self.expiry)
        if self.option_type:
            parts.append(self.option_type)
        return ":".join(parts)
    
    @classmethod
    def from_string(cls, instrument_key_str: str) -> 'InstrumentKey':
        """Parse instrument key from string"""
        parts = instrument_key_str.split(':')
        if len(parts) < 3:
            raise ValueError(f"Invalid instrument key format: {instrument_key_str}")
        
        venue = Venue(parts[0])
        instrument_type = InstrumentType(parts[1])
        symbol = parts[2]
        expiry = parts[3] if len(parts) > 3 else None
        option_type = parts[4] if len(parts) > 4 else None
        
        return cls(
            venue=venue, 
            instrument_type=instrument_type, 
            symbol=symbol,
            expiry=expiry,
            option_type=option_type
        )

@dataclass
class OHLCV:
    """OHLCV data model"""
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    forward_filled: bool = False
    
    def model_dump(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'timestamp': self.timestamp,
            'open': float(self.open),
            'high': float(self.high),
            'low': float(self.low),
            'close': float(self.close),
            'volume': float(self.volume),
            'forward_filled': self.forward_filled
        }

@dataclass
class TradeData:
    """Trade data model"""
    timestamp: int
    local_timestamp: int
    price: Decimal
    amount: Decimal  # Tardis uses 'amount' not 'size'
    side: str  # 'buy' or 'sell'
    id: str
    
    def __post_init__(self):
        """Validate trade data"""
        if self.price <= 0:
            raise ValueError("Price must be positive")
        if self.amount <= 0:
            raise ValueError("Amount must be positive")
        if self.side not in ['buy', 'sell']:
            raise ValueError("Side must be 'buy' or 'sell'")
    
    def model_dump(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'timestamp': self.timestamp.replace(tzinfo=None).isoformat() + 'Z',  # UTC naive datetime
            'local_timestamp': self.local_timestamp.replace(tzinfo=None).isoformat() + 'Z',  # UTC naive datetime
            'price': float(self.price),
            'amount': float(self.amount),
            'side': self.side,
            'id': self.id,
        }

    @classmethod
    def from_datetimes(cls, timestamp: datetime, local_timestamp: datetime, **kwargs):
        # Force UTC labeling without conversion
        ts_us = int(timestamp.replace(tzinfo=timezone.utc).timestamp() * 1_000_000)
        local_us = int(local_timestamp.replace(tzinfo=timezone.utc).timestamp() * 1_000_000)
        return cls(timestamp=ts_us, local_timestamp=local_us, **kwargs)
    
@dataclass
class BookSnapshot:
    """Order book snapshot model"""
    timestamp: int
    local_timestamp: int
    bids: List[Dict[str, float]]  # [{"price": 65000.0, "amount": 0.5}, ...]
    asks: List[Dict[str, float]]  # [{"price": 65001.0, "amount": 0.3}, ...]
    
    def __post_init__(self):
        """Validate book snapshot data"""
        if len(self.bids) > 5:
            raise ValueError("Too many bid levels (max 5)")
        if len(self.asks) > 5:
            raise ValueError("Too many ask levels (max 5)")
        
        # Validate bid/ask prices
        for bid in self.bids:
            if bid['price'] <= 0 or bid['amount'] <= 0:
                raise ValueError("Invalid bid data")
        for ask in self.asks:
            if ask['price'] <= 0 or ask['amount'] <= 0:
                raise ValueError("Invalid ask data")
    
    def model_dump(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'timestamp': self.timestamp,
            'local_timestamp': self.local_timestamp,
            'bids': self.bids,
            'asks': self.asks
        }
        
    @classmethod
    def from_datetimes(cls, timestamp: datetime, local_timestamp: datetime, **kwargs):
        # Force UTC labeling without conversion
        ts_us = int(timestamp.replace(tzinfo=timezone.utc).timestamp() * 1_000_000)
        local_us = int(local_timestamp.replace(tzinfo=timezone.utc).timestamp() * 1_000_000)
        return cls(timestamp=ts_us, local_timestamp=local_us, **kwargs)

@dataclass
class DerivativeTicker:
    """Derivative ticker data model"""
    timestamp: datetime
    symbol: str
    price: Decimal
    volume_24h: Decimal
    open_interest: Optional[Decimal] = None
    funding_rate: Optional[Decimal] = None
    mark_price: Optional[Decimal] = None
    index_price: Optional[Decimal] = None
    
    def model_dump(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'timestamp': self.timestamp,
            'symbol': self.symbol,
            'price': float(self.price),
            'volume_24h': float(self.volume_24h),
            'open_interest': float(self.open_interest) if self.open_interest else None,
            'funding_rate': float(self.funding_rate) if self.funding_rate else None,
            'mark_price': float(self.mark_price) if self.mark_price else None,
            'index_price': float(self.index_price) if self.index_price else None
        }
        
    @classmethod
    def from_datetimes(cls, timestamp: datetime, **kwargs):
        # Force UTC labeling without conversion
        ts_us = int(timestamp.replace(tzinfo=timezone.utc).timestamp() * 1_000_000)
        return cls(timestamp=ts_us, **kwargs)

@dataclass
class Liquidations:
    """Liquidation data model"""
    timestamp: int
    local_timestamp: int
    side: str  # 'long' or 'short'
    amount: Decimal  # Tardis uses 'amount' not 'size'
    price: Decimal
    id: str
    
    def __post_init__(self):
        """Validate liquidation data"""
        if self.amount <= 0:
            raise ValueError("Amount must be positive")
        if self.price <= 0:
            raise ValueError("Price must be positive")
        if self.side not in ['long', 'short']:
            raise ValueError("Side must be 'long' or 'short'")
    
    def model_dump(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'timestamp': self.timestamp,
            'local_timestamp': self.local_timestamp,
            'side': self.side,
            'amount': float(self.amount),
            'price': float(self.price),
            'id': self.id
        }
        
        
    @classmethod
    def from_datetimes(cls, timestamp: datetime, local_timestamp: datetime, **kwargs):
        # Force UTC labeling without conversion
        ts_us = int(timestamp.replace(tzinfo=timezone.utc).timestamp() * 1_000_000)
        local_us = int(local_timestamp.replace(tzinfo=timezone.utc).timestamp() * 1_000_000)
        return cls(timestamp=ts_us, local_timestamp=local_us, **kwargs)

@dataclass
class OptionsChain:
    """Options chain data model following Tardis convention"""
    exchange: str
    symbol: str
    timestamp: int  # microseconds since epoch
    local_timestamp: int  # microseconds since epoch
    type: str  # 'put' or 'call'
    strike_price: Decimal
    expiration: int  # microseconds since epoch
    underlying_index: str
    open_interest: Optional[Decimal] = None
    last_price: Optional[Decimal] = None
    bid_price: Optional[Decimal] = None
    bid_amount: Optional[Decimal] = None
    bid_iv: Optional[Decimal] = None  # implied volatility
    ask_price: Optional[Decimal] = None
    ask_amount: Optional[Decimal] = None
    ask_iv: Optional[Decimal] = None  # implied volatility
    mark_price: Optional[Decimal] = None
    mark_iv: Optional[Decimal] = None  # implied volatility
    underlying_price: Optional[Decimal] = None
    delta: Optional[Decimal] = None
    gamma: Optional[Decimal] = None
    vega: Optional[Decimal] = None
    theta: Optional[Decimal] = None
    rho: Optional[Decimal] = None
    
    def __post_init__(self):
        """Validate options chain data"""
        if self.type not in ['put', 'call']:
            raise ValueError("Type must be 'put' or 'call'")
        if self.strike_price <= 0:
            raise ValueError("Strike price must be positive")
        if self.expiration <= 0:
            raise ValueError("Expiration must be positive")
        if self.timestamp <= 0:
            raise ValueError("Timestamp must be positive")
        if self.local_timestamp <= 0:
            raise ValueError("Local timestamp must be positive")
    
    def model_dump(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'exchange': self.exchange,
            'symbol': self.symbol,
            'timestamp': self.timestamp,
            'local_timestamp': self.local_timestamp,
            'type': self.type,
            'strike_price': float(self.strike_price),
            'expiration': self.expiration,
            'open_interest': float(self.open_interest) if self.open_interest else None,
            'last_price': float(self.last_price) if self.last_price else None,
            'bid_price': float(self.bid_price) if self.bid_price else None,
            'bid_amount': float(self.bid_amount) if self.bid_amount else None,
            'bid_iv': float(self.bid_iv) if self.bid_iv else None,
            'ask_price': float(self.ask_price) if self.ask_price else None,
            'ask_amount': float(self.ask_amount) if self.ask_amount else None,
            'ask_iv': float(self.ask_iv) if self.ask_iv else None,
            'mark_price': float(self.mark_price) if self.mark_price else None,
            'mark_iv': float(self.mark_iv) if self.mark_iv else None,
            'underlying_index': self.underlying_index,
            'underlying_price': float(self.underlying_price) if self.underlying_price else None,
            'delta': float(self.delta) if self.delta else None,
            'gamma': float(self.gamma) if self.gamma else None,
            'vega': float(self.vega) if self.vega else None,
            'theta': float(self.theta) if self.theta else None,
            'rho': float(self.rho) if self.rho else None,
        }

    @classmethod
    def from_datetimes(cls, timestamp: datetime, local_timestamp: datetime, **kwargs):
        """Create OptionsChain from datetime objects"""
        # Convert to microseconds since epoch
        ts_us = int(timestamp.replace(tzinfo=timezone.utc).timestamp() * 1_000_000)
        local_us = int(local_timestamp.replace(tzinfo=timezone.utc).timestamp() * 1_000_000)
        
        # Convert expiration datetime if provided
        if 'expiration' in kwargs and isinstance(kwargs['expiration'], datetime):
            exp_us = int(kwargs['expiration'].replace(tzinfo=timezone.utc).timestamp() * 1_000_000)
            kwargs['expiration'] = exp_us
        
        return cls(timestamp=ts_us, local_timestamp=local_us, **kwargs)

@dataclass
class TickData:
    """Tick data model for trades, book snapshots, etc."""
    timestamp: datetime
    instrument_key: str
    data_type: str
    venue: str
    raw_data: Dict[str, Any]
    
    def model_dump(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'timestamp': self.timestamp,
            'instrument_key': self.instrument_key,
            'data_type': self.data_type,
            'venue': self.venue,
            'raw_data': self.raw_data
        }

@dataclass
class GCSFileInfo:
    """GCS file information"""
    bucket: str
    path: str
    size_bytes: int
    created: Optional[datetime] = None
    modified: Optional[datetime] = None
    exists: bool = False

@dataclass
class GapInfo:
    """Gap information for missing data"""
    start_time: datetime
    end_time: datetime
    duration_minutes: int
    timeframe: str
    filled: bool = False
    
    def model_dump(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'start_time': self.start_time,
            'end_time': self.end_time,
            'duration_minutes': self.duration_minutes,
            'timeframe': self.timeframe,
            'filled': self.filled
        }

@dataclass
class DownloadResult:
    """Result of a download operation"""
    instrument_key: str
    success: bool
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    records_count: int = 0
    duration_seconds: float = 0.0
    
    @classmethod
    def aggregate(cls, results: List['DownloadResult']) -> 'AggregatedDownloadResult':
        """Aggregate multiple download results"""
        total_instruments = len(results)
        successful = sum(1 for r in results if r.success)
        failed = total_instruments - successful
        total_records = sum(r.records_count for r in results)
        total_duration = sum(r.duration_seconds for r in results)
        
        return AggregatedDownloadResult(
            total_instruments=total_instruments,
            successful=successful,
            failed=failed,
            percentage=(successful / total_instruments * 100) if total_instruments > 0 else 0,
            total_records=total_records,
            total_duration=total_duration,
            results=results
        )

@dataclass
class AggregatedDownloadResult:
    """Aggregated result of multiple downloads"""
    total_instruments: int
    successful: int
    failed: int
    percentage: float
    total_records: int
    total_duration: float
    results: List[DownloadResult]
    
    @property
    def missing_data(self) -> List[str]:
        """Get list of instruments with missing data"""
        return [r.instrument_key for r in self.results if not r.success]

@dataclass
class QueryResult:
    """Result of a data query"""
    success: bool
    data: Optional[Dict[str, Any]] = None
    total_records: int = 0
    query_time_ms: float = 0.0
    error: Optional[str] = None

@dataclass
class ValidationResult:
    """Result of data validation"""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    records_checked: int = 0
    
    def add_error(self, error: str):
        """Add validation error"""
        self.errors.append(error)
        self.is_valid = False
    
    def add_warning(self, warning: str):
        """Add validation warning"""
        self.warnings.append(warning)

@dataclass
class HealthStatus:
    """System health status"""
    status: str  # 'healthy', 'unhealthy', 'degraded'
    timestamp: datetime
    components: Dict[str, str] = None
    uptime_seconds: float = 0.0
    version: str = "1.0.0"
    
    def __post_init__(self):
        if self.components is None:
            self.components = {}

@dataclass
class ErrorResult:
    """Result of error handling"""
    success: bool
    result: Optional[Any] = None
    error: Optional[str] = None
    retry_count: int = 0
    next_retry_at: Optional[datetime] = None

class ErrorType(Enum):
    """Error types for classification"""
    NETWORK_ERROR = "network_error"
    API_ERROR = "api_error"
    AUTHENTICATION_ERROR = "authentication_error"
    RATE_LIMIT_ERROR = "rate_limit_error"
    DATA_ERROR = "data_error"
    STORAGE_ERROR = "storage_error"
    VALIDATION_ERROR = "validation_error"
    SYSTEM_ERROR = "system_error"

class ErrorSeverity(Enum):
    """Error severity levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"
