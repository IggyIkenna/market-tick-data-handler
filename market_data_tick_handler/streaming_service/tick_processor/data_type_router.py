"""
Data Type Router

Routes different Tardis data types to appropriate processors.
Implements fallback strategies for missing data types.
Addresses Issue #003 - Missing Data Types in Live Stream.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Callable
from dataclasses import dataclass
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


@dataclass
class DataTypeConfig:
    """Configuration for a specific data type"""
    enabled: bool = True
    source: str = "tardis_realtime"  # tardis_realtime, trade_transformation, derivative_ticker_extraction
    batch_timeout: int = 60000  # milliseconds
    fallback: bool = False
    processor_class: Optional[str] = None


class DataTypeProcessor(ABC):
    """Abstract base class for data type processors"""
    
    @abstractmethod
    async def process(self, tick_data) -> Any:
        """Process tick data of specific type"""
        pass
    
    @abstractmethod
    async def shutdown(self) -> None:
        """Shutdown processor"""
        pass


class TradeProcessor(DataTypeProcessor):
    """Processor for trade data"""
    
    async def process(self, tick_data) -> Dict[str, Any]:
        """Process trade tick"""
        trade_data = tick_data.data
        
        return {
            'symbol': tick_data.symbol,
            'exchange': tick_data.exchange,
            'timestamp': tick_data.timestamp,
            'timestamp_out': datetime.now(timezone.utc),
            'price': float(trade_data.get('price', 0)),
            'amount': float(trade_data.get('amount', 0)),
            'side': trade_data.get('side', 'unknown'),
            'trade_id': trade_data.get('id', ''),
            'data_type': 'trades'
        }
    
    async def shutdown(self) -> None:
        pass


class BookSnapshotProcessor(DataTypeProcessor):
    """Processor for book snapshot data"""
    
    async def process(self, tick_data) -> Dict[str, Any]:
        """Process book snapshot tick"""
        book_data = tick_data.data
        
        return {
            'symbol': tick_data.symbol,
            'exchange': tick_data.exchange,
            'timestamp': tick_data.timestamp,
            'timestamp_out': datetime.now(timezone.utc),
            'bids': book_data.get('bids', []),
            'asks': book_data.get('asks', []),
            'bid_count': len(book_data.get('bids', [])),
            'ask_count': len(book_data.get('asks', [])),
            'data_type': 'book_snapshots'
        }
    
    async def shutdown(self) -> None:
        pass


class LiquidationProcessor(DataTypeProcessor):
    """Processor for liquidation data (can derive from trades)"""
    
    async def process(self, tick_data) -> Dict[str, Any]:
        """Process liquidation tick or derive from trade"""
        data = tick_data.data
        
        # If it's already liquidation data
        if tick_data.data_type == 'liquidation':
            return {
                'symbol': tick_data.symbol,
                'exchange': tick_data.exchange,
                'timestamp': tick_data.timestamp,
                'timestamp_out': datetime.now(timezone.utc),
                'price': float(data.get('price', 0)),
                'amount': float(data.get('amount', 0)),
                'side': data.get('side', 'unknown'),
                'liquidation_type': data.get('liquidation_type', 'unknown'),
                'data_type': 'liquidations'
            }
        
        # Derive from trade data (fallback strategy)
        if tick_data.data_type == 'trade':
            # Simple heuristic: large trades might be liquidations
            amount = float(data.get('amount', 0))
            if amount > 10.0:  # Configurable threshold
                return {
                    'symbol': tick_data.symbol,
                    'exchange': tick_data.exchange,
                    'timestamp': tick_data.timestamp,
                    'timestamp_out': datetime.now(timezone.utc),
                    'price': float(data.get('price', 0)),
                    'amount': amount,
                    'side': data.get('side', 'unknown'),
                    'liquidation_type': 'derived_from_trade',
                    'data_type': 'liquidations'
                }
        
        return None
    
    async def shutdown(self) -> None:
        pass


class DerivativeTickerProcessor(DataTypeProcessor):
    """Processor for derivative ticker data"""
    
    async def process(self, tick_data) -> Dict[str, Any]:
        """Process derivative ticker"""
        ticker_data = tick_data.data
        
        return {
            'symbol': tick_data.symbol,
            'exchange': tick_data.exchange,
            'timestamp': tick_data.timestamp,
            'timestamp_out': datetime.now(timezone.utc),
            'mark_price': float(ticker_data.get('mark_price', ticker_data.get('price', 0))),
            'index_price': float(ticker_data.get('index_price', ticker_data.get('price', 0))),
            'funding_rate': float(ticker_data.get('funding_rate', 0)),
            'open_interest': float(ticker_data.get('open_interest', 0)),
            'data_type': 'derivative_ticker'
        }
    
    async def shutdown(self) -> None:
        pass


class OptionsChainProcessor(DataTypeProcessor):
    """Processor for options chain data"""
    
    async def process(self, tick_data) -> Dict[str, Any]:
        """Process options chain data"""
        options_data = tick_data.data
        
        return {
            'symbol': tick_data.symbol,
            'exchange': tick_data.exchange,
            'timestamp': tick_data.timestamp,
            'timestamp_out': datetime.now(timezone.utc),
            'strike_price': float(options_data.get('strike_price', 0)),
            'expiry': options_data.get('expiry'),
            'option_type': options_data.get('option_type', 'call'),
            'bid': float(options_data.get('bid', 0)) if options_data.get('bid') else None,
            'ask': float(options_data.get('ask', 0)) if options_data.get('ask') else None,
            'volume': float(options_data.get('volume', 0)),
            'data_type': 'options_chain'
        }
    
    async def shutdown(self) -> None:
        pass


class FundingRateProcessor(DataTypeProcessor):
    """Processor for funding rates (can extract from derivative ticker)"""
    
    async def process(self, tick_data) -> Dict[str, Any]:
        """Process funding rate or extract from derivative ticker"""
        data = tick_data.data
        
        # If it's already funding rate data
        if tick_data.data_type == 'funding_rate':
            return {
                'symbol': tick_data.symbol,
                'exchange': tick_data.exchange,
                'timestamp': tick_data.timestamp,
                'timestamp_out': datetime.now(timezone.utc),
                'funding_rate': float(data.get('funding_rate', 0)),
                'next_funding_time': data.get('next_funding_time'),
                'data_type': 'funding_rates'
            }
        
        # Extract from derivative ticker (fallback strategy)
        if tick_data.data_type == 'derivative_ticker' and 'funding_rate' in data:
            return {
                'symbol': tick_data.symbol,
                'exchange': tick_data.exchange,
                'timestamp': tick_data.timestamp,
                'timestamp_out': datetime.now(timezone.utc),
                'funding_rate': float(data.get('funding_rate', 0)),
                'next_funding_time': data.get('next_funding_time'),
                'data_type': 'funding_rates'
            }
        
        return None
    
    async def shutdown(self) -> None:
        pass


class DataTypeRouter:
    """
    Routes data types to appropriate processors.
    Implements fallback strategies for missing data types.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize data type router.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config or {}
        self.processors = {}
        self.data_type_configs = self._load_data_type_configs()
        self.fallback_strategies = self._setup_fallback_strategies()
        
        # Initialize processors
        self._initialize_processors()
        
        logger.info("✅ DataTypeRouter initialized")
        logger.info(f"   Supported data types: {list(self.data_type_configs.keys())}")
    
    def _load_data_type_configs(self) -> Dict[str, DataTypeConfig]:
        """Load data type configurations"""
        default_configs = {
            'trades': DataTypeConfig(
                enabled=True,
                source='tardis_realtime',
                batch_timeout=60000,  # 1 minute for high-frequency data
                processor_class='TradeProcessor'
            ),
            'book_snapshots': DataTypeConfig(
                enabled=True,
                source='tardis_realtime',
                batch_timeout=60000,  # 1 minute for high-frequency data
                processor_class='BookSnapshotProcessor'
            ),
            'liquidations': DataTypeConfig(
                enabled=True,
                source='trade_transformation',
                batch_timeout=900000,  # 15 minutes for lower-frequency data
                fallback=True,
                processor_class='LiquidationProcessor'
            ),
            'derivative_ticker': DataTypeConfig(
                enabled=True,
                source='tardis_realtime',
                batch_timeout=900000,  # 15 minutes for lower-frequency data
                processor_class='DerivativeTickerProcessor'
            ),
            'options_chain': DataTypeConfig(
                enabled=True,
                source='tardis_realtime',
                batch_timeout=900000,  # 15 minutes for lower-frequency data
                processor_class='OptionsChainProcessor'
            ),
            'funding_rates': DataTypeConfig(
                enabled=True,
                source='derivative_ticker_extraction',
                batch_timeout=900000,  # 15 minutes for lower-frequency data
                fallback=True,
                processor_class='FundingRateProcessor'
            )
        }
        
        # Override with config if provided
        config_overrides = self.config.get('data_types', {})
        for data_type, override_config in config_overrides.items():
            if data_type in default_configs:
                for key, value in override_config.items():
                    setattr(default_configs[data_type], key, value)
        
        return default_configs
    
    def _setup_fallback_strategies(self) -> Dict[str, List[str]]:
        """Setup fallback strategies for data types"""
        return {
            'liquidations': ['trades'],  # Can derive from trades
            'funding_rates': ['derivative_ticker'],  # Can extract from derivative ticker
            'gaps': ['trades'],  # Can detect from trade analysis
            'candles': ['trades']  # Can aggregate from trades
        }
    
    def _initialize_processors(self) -> None:
        """Initialize processors for each data type"""
        processor_classes = {
            'TradeProcessor': TradeProcessor,
            'BookSnapshotProcessor': BookSnapshotProcessor,
            'LiquidationProcessor': LiquidationProcessor,
            'DerivativeTickerProcessor': DerivativeTickerProcessor,
            'OptionsChainProcessor': OptionsChainProcessor,
            'FundingRateProcessor': FundingRateProcessor
        }
        
        for data_type, config in self.data_type_configs.items():
            if config.enabled and config.processor_class in processor_classes:
                processor_class = processor_classes[config.processor_class]
                self.processors[data_type] = processor_class()
                logger.info(f"   Initialized {config.processor_class} for {data_type}")
    
    async def get_processor(self, data_type: str) -> Optional[DataTypeProcessor]:
        """
        Get processor for a specific data type.
        
        Args:
            data_type: Data type to get processor for
            
        Returns:
            Processor instance or None if not available
        """
        # Direct processor
        if data_type in self.processors:
            return self.processors[data_type]
        
        # Check fallback strategies
        if data_type in self.fallback_strategies:
            for fallback_type in self.fallback_strategies[data_type]:
                if fallback_type in self.processors:
                    logger.info(f"Using fallback processor {fallback_type} for {data_type}")
                    return self.processors[fallback_type]
        
        logger.warning(f"No processor found for data type: {data_type}")
        return None
    
    def is_supported(self, data_type: str) -> bool:
        """Check if data type is supported"""
        return (data_type in self.processors or 
                data_type in self.fallback_strategies)
    
    def get_supported_data_types(self) -> List[str]:
        """Get list of all supported data types"""
        supported = list(self.processors.keys())
        supported.extend(self.fallback_strategies.keys())
        return list(set(supported))
    
    def get_config(self, data_type: str) -> Optional[DataTypeConfig]:
        """Get configuration for a data type"""
        return self.data_type_configs.get(data_type)
    
    async def shutdown(self) -> None:
        """Shutdown all processors"""
        logger.info("🛑 Shutting down DataTypeRouter...")
        
        for processor in self.processors.values():
            await processor.shutdown()
        
        logger.info("✅ DataTypeRouter shutdown complete")


# Example usage
if __name__ == "__main__":
    import asyncio
    from datetime import datetime, timezone
    
    async def test_data_type_router():
        router = DataTypeRouter()
        
        # Test different data types
        test_cases = [
            {
                'symbol': 'BTC-USDT',
                'exchange': 'binance',
                'data_type': 'trades',
                'timestamp': datetime.now(timezone.utc),
                'data': {'price': 67000.0, 'amount': 0.1, 'side': 'buy'}
            },
            {
                'symbol': 'BTC-USDT',
                'exchange': 'binance',
                'data_type': 'liquidations',
                'timestamp': datetime.now(timezone.utc),
                'data': {'price': 67000.0, 'amount': 5.0, 'side': 'sell'}
            }
        ]
        
        for test_case in test_cases:
            processor = await router.get_processor(test_case['data_type'])
            if processor:
                print(f"Found processor for {test_case['data_type']}: {type(processor).__name__}")
            else:
                print(f"No processor found for {test_case['data_type']}")
        
        print(f"Supported data types: {router.get_supported_data_types()}")
        
        await router.shutdown()
    
    asyncio.run(test_data_type_router())
