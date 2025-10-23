"""
Data Models for Market Data Tick Handler

This module defines all data models following the INSTRUMENT_KEY_SPEC.md format
with proper expiry, call/put, and margin currency support.
"""

from enum import Enum
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Union
from datetime import datetime, timezone
from decimal import Decimal
from pydantic import BaseModel, Field, field_validator, model_validator
import logging
import pandas as pd

logger = logging.getLogger(__name__)

class Venue(str, Enum):
    """Supported venues for instruments"""
    BINANCE = "BINANCE"
    BINANCE_FUTURES = "BINANCE-FUTURES"
    BYBIT = "BYBIT"
    BYBIT_SPOT = "BYBIT-SPOT"
    OKX = "OKX"
    OKX_SWAP = "OKX-SWAP"
    OKX_FUTURES = "OKX-FUTURES"
    DERIBIT = "DERIBIT"
    AAVE_V3 = "AAVE_V3"
    ETHERFI = "ETHERFI"
    LIDO = "LIDO"
    WALLET = "WALLET"
    CME = "CME"
    NASDAQ = "NASDAQ"

class InstrumentType(str, Enum):
    """Supported instrument types"""
    SPOT_ASSET = "SPOT_ASSET"
    SPOT_PAIR = "SPOT_PAIR"
    PERP = "PERP"  # Alias for PERPETUAL
    PERPETUAL = "PERPETUAL"
    FUTURE = "FUTURE"
    OPTION = "OPTION"
    EQUITY = "EQUITY"
    INDEX = "INDEX"
    LST = "LST"
    A_TOKEN = "A_TOKEN"
    DEBT_TOKEN = "DEBT_TOKEN"

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
    """Trade data model following Tardis schema (exchange/symbol dropped after validation)"""
    timestamp: int
    local_timestamp: int
    id: str
    side: str  # 'buy' or 'sell'
    price: float
    amount: float  # Tardis uses 'amount' not 'size'
    
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
            'timestamp': self.timestamp,
            'local_timestamp': self.local_timestamp,
            'id': self.id,
            'side': self.side,
            'price': self.price,
            'amount': self.amount,
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
    """Derivative ticker data model following Tardis schema (exchange/symbol dropped after validation)"""
    timestamp: int  # microseconds since epoch
    local_timestamp: int  # microseconds since epoch
    funding_rate: float
    predicted_funding_rate: float
    open_interest: float
    last_price: float
    index_price: float
    mark_price: float
    funding_timestamp: Optional[int] = None  # microseconds since epoch
    
    def model_dump(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'timestamp': self.timestamp,
            'local_timestamp': self.local_timestamp,
            'funding_rate': self.funding_rate,
            'predicted_funding_rate': self.predicted_funding_rate,
            'open_interest': self.open_interest,
            'last_price': self.last_price,
            'index_price': self.index_price,
            'mark_price': self.mark_price,
            'funding_timestamp': self.funding_timestamp
        }
        
    @classmethod
    def from_datetimes(cls, timestamp: datetime, local_timestamp: datetime, **kwargs):
        # Force UTC labeling without conversion
        ts_us = int(timestamp.replace(tzinfo=timezone.utc).timestamp() * 1_000_000)
        local_us = int(local_timestamp.replace(tzinfo=timezone.utc).timestamp() * 1_000_000)
        return cls(timestamp=ts_us, local_timestamp=local_us, **kwargs)

@dataclass
class Liquidations:
    """Liquidation data model following Tardis schema (exchange/symbol dropped after validation)"""
    timestamp: int
    local_timestamp: int
    id: str
    side: str  # 'buy' = short liquidated, 'sell' = long liquidated
    price: float
    amount: float  # Tardis uses 'amount' not 'size'
    
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
            'id': self.id,
            'side': self.side,
            'price': self.price,
            'amount': self.amount
        }
        
        
    @classmethod
    def from_datetimes(cls, timestamp: datetime, local_timestamp: datetime, **kwargs):
        # Force UTC labeling without conversion
        ts_us = int(timestamp.replace(tzinfo=timezone.utc).timestamp() * 1_000_000)
        local_us = int(local_timestamp.replace(tzinfo=timezone.utc).timestamp() * 1_000_000)
        return cls(timestamp=ts_us, local_timestamp=local_us, **kwargs)

@dataclass
class OptionsChain:
    """Options chain data model following Tardis schema (exchange/symbol dropped after validation)"""
    timestamp: int  # microseconds since epoch
    local_timestamp: int  # microseconds since epoch
    mark_price: float
    index_price: float
    bid_price: float
    bid_amount: float
    ask_price: float
    ask_amount: float
    delta: float
    gamma: float
    vega: float
    theta: float
    rho: float
    iv: float  # implied volatility (single field, not separate bid/ask/mark)
    open_interest: float
    volume: float
    funding_timestamp: Optional[int] = None  # microseconds since epoch
    
    def __post_init__(self):
        """Validate options chain data"""
        if self.timestamp <= 0:
            raise ValueError("Timestamp must be positive")
        if self.local_timestamp <= 0:
            raise ValueError("Local timestamp must be positive")
    
    def model_dump(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'timestamp': self.timestamp,
            'local_timestamp': self.local_timestamp,
            'mark_price': self.mark_price,
            'index_price': self.index_price,
            'bid_price': self.bid_price,
            'bid_amount': self.bid_amount,
            'ask_price': self.ask_price,
            'ask_amount': self.ask_amount,
            'delta': self.delta,
            'gamma': self.gamma,
            'vega': self.vega,
            'theta': self.theta,
            'rho': self.rho,
            'iv': self.iv,
            'open_interest': self.open_interest,
            'volume': self.volume,
            'funding_timestamp': self.funding_timestamp
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

class InstrumentDefinition(BaseModel):
    """
    Comprehensive Pydantic model for instrument definitions
    
    This model validates all fields that are generated and uploaded to GCS
    as instrument definitions, ensuring data integrity and schema compliance.
    """
    
    # Core identification fields
    instrument_key: str = Field(..., description="Canonical instrument key in format VENUE:INSTRUMENT_TYPE:SYMBOL")
    venue: str = Field(..., description="Venue identifier (e.g., BINANCE, DERIBIT)")
    instrument_type: str = Field(..., description="Instrument type (e.g., SPOT_PAIR, PERPETUAL, FUTURE, OPTION)")
    
    # Availability windows
    available_from_datetime: str = Field(..., description="ISO datetime string when instrument became available")
    available_to_datetime: str = Field(..., description="ISO datetime string when instrument expires/becomes unavailable")
    
    # Data types available for this instrument
    data_types: str = Field(..., description="Comma-separated list of available data types (e.g., 'trades,book_snapshot_5')")
    
    # Asset information
    base_asset: str = Field(..., description="Base asset symbol (e.g., BTC, ETH)")
    quote_asset: str = Field(..., description="Quote asset symbol (e.g., USDT, USD)")
    settle_asset: str = Field(..., description="Settlement asset symbol")
    
    # Exchange-specific identifiers
    exchange_raw_symbol: str = Field(..., description="Raw symbol from exchange API")
    tardis_symbol: str = Field(..., description="Symbol format used by Tardis API")
    tardis_exchange: str = Field(..., description="Exchange identifier used by Tardis API")
    
    # Metadata
    data_provider: str = Field(..., description="Data provider identifier")
    venue_type: str = Field(..., description="Type of venue (e.g., centralized, decentralized)")
    asset_class: str = Field(..., description="Asset class (e.g., crypto, equity, commodity)")
    
    # Trading parameters
    inverse: bool = Field(default=False, description="Whether this is an inverse contract")
    symbol_type: str = Field(default="", description="Raw instrument type from exchange")
    contract_type: str = Field(default="", description="Exchange-specific contract classification")
    
    # Option-specific fields
    strike: str = Field(default="", description="Strike price for options")
    option_type: str = Field(default="", description="Option type (C for call, P for put)")
    
    # Contract-specific fields
    expiry: Optional[str] = Field(default=None, description="Expiry datetime for futures/options (ISO string)")
    contract_size: Optional[float] = Field(default=None, description="Contract size/multiplier")
    tick_size: Optional[str] = Field(default="", description="Minimum price increment")
    settlement_type: Optional[str] = Field(default="", description="Settlement type (cash, physical)")
    underlying: Optional[str] = Field(default="", description="Underlying asset for derivatives")
    min_size: Optional[str] = Field(default="", description="Minimum order size")
    
    # CCXT integration fields
    ccxt_symbol: str = Field(default="", description="Symbol format for CCXT library")
    ccxt_exchange: str = Field(default="", description="Exchange identifier for CCXT library")
    
    # Note: validation_warnings removed to avoid circular reference issues
    
    @field_validator('instrument_key')
    @classmethod
    def validate_instrument_key(cls, v):
        """Validate instrument key format"""
        if not v or ':' not in v:
            raise ValueError(f"Invalid instrument key format: {v}")
        
        parts = v.split(':')
        if len(parts) < 3:
            raise ValueError(f"Instrument key must have at least 3 parts: {v}")
        
        # Validate venue (first part)
        venue = parts[0]
        valid_venues = [v.value for v in Venue]
        if venue not in valid_venues:
            # Warning will be collected in model_validator
            pass
        
        # Validate instrument type (second part)
        instrument_type = parts[1]
        valid_types = [t.value for t in InstrumentType]
        if instrument_type not in valid_types:
            # Warning will be collected in model_validator
            pass
        
        return v
    
    @field_validator('available_from_datetime', 'available_to_datetime')
    @classmethod
    def validate_datetime_strings(cls, v):
        """Validate ISO datetime strings"""
        if not v:
            raise ValueError("Datetime string cannot be empty")
        
        try:
            datetime.fromisoformat(v.replace('Z', '+00:00'))
        except ValueError:
            raise ValueError(f"Invalid ISO datetime format: {v}")
        
        return v
    
    @field_validator('data_types')
    @classmethod
    def validate_data_types(cls, v):
        """Validate data types string"""
        if not v:
            raise ValueError("Data types cannot be empty")
        
        valid_data_types = ['trades', 'book_snapshot_5', 'derivative_ticker', 'options_chain', 'liquidations']
        types = [t.strip() for t in v.split(',')]
        
        for data_type in types:
            if data_type not in valid_data_types:
                # Warning will be collected in model_validator
                pass
        
        return v
    
    @field_validator('expiry')
    @classmethod
    def validate_expiry(cls, v):
        """Validate expiry datetime for futures/options"""
        if v is None or v == "":
            return v
        
        # Handle datetime objects (convert to ISO string)
        if isinstance(v, (datetime, pd.Timestamp)):
            return v.isoformat()
        
        # Handle string datetime
        if isinstance(v, str):
            try:
                datetime.fromisoformat(v.replace('Z', '+00:00'))
                return v
            except ValueError:
                raise ValueError(f"Invalid expiry datetime format: {v}")
        
        return v
    
    @field_validator('option_type')
    @classmethod
    def validate_option_type(cls, v):
        """Validate option type"""
        if v and v not in ['C', 'P', 'CALL', 'PUT']:
            raise ValueError(f"Invalid option type: {v}. Must be C, P, CALL, or PUT")
        return v
    
    @field_validator('inverse')
    @classmethod
    def validate_inverse(cls, v):
        """Validate inverse field"""
        if not isinstance(v, bool):
            raise ValueError(f"Inverse must be boolean, got: {type(v)}")
        return v
    
    @field_validator('tick_size', 'min_size', 'settlement_type', 'underlying', 'ccxt_symbol', 'ccxt_exchange')
    @classmethod
    def validate_optional_strings(cls, v):
        """Validate optional string fields - convert None to empty string"""
        if v is None:
            return ""
        return str(v)
    
    @model_validator(mode='after')
    def validate_instrument_consistency(self):
        """Validate overall instrument consistency"""
        # Check instrument key components
        if self.instrument_key and ':' in self.instrument_key:
            parts = self.instrument_key.split(':')
            if len(parts) >= 2:
                venue = parts[0]
                instrument_type = parts[1]
                
                # Check venue
                valid_venues = [v.value for v in Venue]
                if venue not in valid_venues:
                    logger.warning(f"Unknown venue in instrument key: {venue}")
                
                # Check instrument type
                valid_types = [t.value for t in InstrumentType]
                if instrument_type not in valid_types:
                    logger.warning(f"Unknown instrument type in instrument key: {instrument_type}")
        
        # Check data types
        if self.data_types:
            valid_data_types = ['trades', 'book_snapshot_5', 'derivative_ticker', 'options_chain', 'liquidations']
            types = [t.strip() for t in self.data_types.split(',')]
            for data_type in types:
                if data_type not in valid_data_types:
                    logger.warning(f"Unknown data type: {data_type}")
        
        # Futures and options should have expiry
        if self.instrument_type in ['FUTURE', 'OPTION'] and not self.expiry:
            logger.warning(f"Futures/options should have expiry: {self.instrument_key}")
        
        # Options should have strike and option_type
        if self.instrument_type == 'OPTION':
            if not self.strike:
                logger.warning(f"Options should have strike price: {self.instrument_key}")
            if not self.option_type:
                logger.warning(f"Options should have option type: {self.instrument_key}")
        
        return self
    
    class Config:
        """Pydantic configuration"""
        validate_assignment = True
        use_enum_values = True
        extra = "forbid"  # Don't allow extra fields
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            Decimal: lambda v: float(v)
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for DataFrame creation"""
        return self.model_dump()
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'InstrumentDefinition':
        """Create from dictionary (e.g., from DataFrame row)"""
        return cls(**data)
    
    def validate_required_fields(self) -> List[str]:
        """Validate required fields and return list of missing fields"""
        missing_fields = []
        
        # Check required string fields
        required_string_fields = [
            'instrument_key', 'venue', 'instrument_type', 'available_from_datetime',
            'available_to_datetime', 'data_types', 'base_asset', 'quote_asset',
            'settle_asset', 'exchange_raw_symbol', 'tardis_symbol', 'tardis_exchange',
            'data_provider', 'venue_type', 'asset_class'
        ]
        
        for field in required_string_fields:
            value = getattr(self, field, None)
            if not value or (isinstance(value, str) and value.strip() == ""):
                missing_fields.append(field)
        
        return missing_fields
