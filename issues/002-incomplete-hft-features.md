# Issue #002: Incomplete HFT Features Implementation

## Problem Statement

The current HFT features implementation only contains basic/generic features (SMA, EMA, RSI, volatility) and is missing the advanced features specified in the HFT Features Specification document, limiting the system's capability for high-frequency trading strategies.

## Current State Analysis

**Files**: 
- `src/streaming_service/hft_features/feature_calculator.py`
- `market_data_tick_handler/streaming_service/hft_features/feature_calculator.py`
- `docs/HFT_FEATURES_SPECIFICATION.md`

### Current Implementation

The `HFTFeatureCalculator` class currently implements:

```python
@dataclass
class HFTFeatures:
    # Price-based features
    sma_5, sma_10, sma_20: Optional[float]
    ema_5, ema_10, ema_20: Optional[float]
    wma_5: Optional[float]
    
    # Basic momentum features
    price_momentum_3, price_momentum_5: Optional[float]
    price_velocity, price_acceleration: Optional[float]
    
    # Volume features
    volume_sma_5, volume_ema_5: Optional[float]
    volume_ratio: Optional[float]
    vwap: Optional[float]
    vwap_deviation: Optional[float]
    
    # Basic volatility features
    price_volatility_5, price_volatility_10: Optional[float]
    high_low_ratio: Optional[float]
    close_to_close_return: Optional[float]
    
    # Microstructure features (basic)
    trade_intensity: Optional[float]
    avg_trade_size: Optional[float]
    bid_ask_spread: Optional[float]
    order_flow_imbalance: Optional[float]
```

### Missing Advanced Features

Based on `docs/HFT_FEATURES_SPECIFICATION.md`, the following advanced features are missing:

#### 1. Trade Data Core Metrics
- `buy_volume_sum` - Total volume of buy-side trades
- `sell_volume_sum` - Total volume of sell-side trades  
- `size_avg` - Average trade size
- `price_vwap` - Volume-weighted average price
- `trade_count` - Number of trades

#### 2. Advanced Momentum Features
- `price_momentum_10` - 10-period momentum
- `price_momentum_20` - 20-period momentum
- `volume_momentum_5` - Volume momentum
- `volume_momentum_10` - 10-period volume momentum

#### 3. Advanced Volatility Features
- `price_volatility_20` - 20-period volatility
- `volume_volatility_5` - Volume volatility
- `volume_volatility_10` - 10-period volume volatility
- `high_low_volatility` - High-low volatility ratio

#### 4. Order Flow Features
- `order_flow_imbalance_5` - 5-period order flow imbalance
- `order_flow_imbalance_10` - 10-period order flow imbalance
- `aggressive_buy_ratio` - Ratio of aggressive buy orders
- `aggressive_sell_ratio` - Ratio of aggressive sell orders

#### 5. Market Microstructure Features
- `trade_size_volatility` - Volatility of trade sizes
- `price_impact` - Price impact of trades
- `liquidity_ratio` - Liquidity ratio metric
- `market_depth_imbalance` - Order book depth imbalance

#### 6. Advanced Technical Indicators
- `macd` - MACD indicator
- `macd_signal` - MACD signal line
- `macd_histogram` - MACD histogram
- `bollinger_upper` - Bollinger Bands upper
- `bollinger_lower` - Bollinger Bands lower
- `bollinger_width` - Bollinger Bands width

## Proposed Solutions

### Solution 1: Extend HFTFeatures Data Class

```python
@dataclass
class HFTFeatures:
    # ... existing fields ...
    
    # Advanced trade data metrics
    buy_volume_sum: Optional[float] = None
    sell_volume_sum: Optional[float] = None
    size_avg: Optional[float] = None
    price_vwap: Optional[float] = None
    trade_count: Optional[int] = None
    
    # Advanced momentum features
    price_momentum_10: Optional[float] = None
    price_momentum_20: Optional[float] = None
    volume_momentum_5: Optional[float] = None
    volume_momentum_10: Optional[float] = None
    
    # Advanced volatility features
    price_volatility_20: Optional[float] = None
    volume_volatility_5: Optional[float] = None
    volume_volatility_10: Optional[float] = None
    high_low_volatility: Optional[float] = None
    
    # Order flow features
    order_flow_imbalance_5: Optional[float] = None
    order_flow_imbalance_10: Optional[float] = None
    aggressive_buy_ratio: Optional[float] = None
    aggressive_sell_ratio: Optional[float] = None
    
    # Market microstructure features
    trade_size_volatility: Optional[float] = None
    price_impact: Optional[float] = None
    liquidity_ratio: Optional[float] = None
    market_depth_imbalance: Optional[float] = None
    
    # Advanced technical indicators
    macd: Optional[float] = None
    macd_signal: Optional[float] = None
    macd_histogram: Optional[float] = None
    bollinger_upper: Optional[float] = None
    bollinger_lower: Optional[float] = None
    bollinger_width: Optional[float] = None
```

### Solution 2: Modular Feature Calculator

Create separate calculators for different feature categories:

```python
class TradeDataCalculator:
    """Calculate trade data core metrics"""
    def calculate_buy_sell_volume(self, trades: List[Trade]) -> Tuple[float, float]
    def calculate_size_avg(self, trades: List[Trade]) -> float
    def calculate_price_vwap(self, trades: List[Trade]) -> float

class MomentumCalculator:
    """Calculate momentum features"""
    def calculate_price_momentum(self, prices: List[float], periods: int) -> float
    def calculate_volume_momentum(self, volumes: List[float], periods: int) -> float

class VolatilityCalculator:
    """Calculate volatility features"""
    def calculate_price_volatility(self, prices: List[float], periods: int) -> float
    def calculate_volume_volatility(self, volumes: List[float], periods: int) -> float

class OrderFlowCalculator:
    """Calculate order flow features"""
    def calculate_order_flow_imbalance(self, trades: List[Trade], periods: int) -> float
    def calculate_aggressive_ratios(self, trades: List[Trade]) -> Tuple[float, float]

class TechnicalIndicatorCalculator:
    """Calculate technical indicators"""
    def calculate_macd(self, prices: List[float]) -> Tuple[float, float, float]
    def calculate_bollinger_bands(self, prices: List[float], periods: int, std_dev: float) -> Tuple[float, float, float]
```

### Solution 3: Configuration-Driven Features

Use configuration to enable/disable features:

```yaml
# config/hft_features.yaml
features:
  basic:
    enabled: true
    include: [sma, ema, rsi, volatility]
  
  advanced:
    enabled: true
    include: [momentum, order_flow, microstructure]
  
  technical_indicators:
    enabled: true
    include: [macd, bollinger_bands, atr]
  
  performance:
    max_calculation_time_ms: 50
    cache_results: true
    parallel_calculation: true
```

## Implementation Tasks

### Phase 1: Data Structure Extension
- [ ] Extend `HFTFeatures` data class with missing fields
- [ ] Update `to_dict()` method to include new fields
- [ ] Add validation for new fields
- [ ] Update type hints and documentation

### Phase 2: Calculator Implementation
- [ ] Implement `TradeDataCalculator` for trade metrics
- [ ] Implement `MomentumCalculator` for momentum features
- [ ] Implement `VolatilityCalculator` for volatility features
- [ ] Implement `OrderFlowCalculator` for order flow features
- [ ] Implement `TechnicalIndicatorCalculator` for technical indicators

### Phase 3: Integration
- [ ] Integrate new calculators into `HFTFeatureCalculator`
- [ ] Add configuration management for feature selection
- [ ] Implement parallel calculation for performance
- [ ] Add caching for expensive calculations

### Phase 4: Testing and Optimization
- [ ] Add unit tests for each calculator
- [ ] Add integration tests for feature calculation
- [ ] Performance testing and optimization
- [ ] Memory usage optimization

## Dependencies and Risks

### Dependencies
- NumPy for mathematical calculations
- Pandas for data manipulation
- SciPy for statistical functions
- TA-Lib for technical indicators (optional)

### Risks
- **Performance Impact**: More features = more computation time
- **Memory Usage**: Storing more features increases memory usage
- **Complexity**: More complex codebase with multiple calculators
- **Data Quality**: Some features may require high-quality data

### Mitigation Strategies
- Implement feature caching and memoization
- Use parallel processing for independent calculations
- Add feature importance scoring
- Implement graceful degradation for missing data

## Success Criteria

1. **Feature Completeness**: All specified HFT features are implemented
2. **Performance**: Feature calculation completes within 50ms
3. **Accuracy**: Features match reference implementations
4. **Maintainability**: Modular design allows easy addition of new features
5. **Documentation**: All features are well-documented with examples

## Priority

**High** - This directly impacts the core value proposition of the HFT features system.

## Estimated Effort

- **Data Structure Extension**: 1-2 days
- **Calculator Implementation**: 2-3 weeks
- **Integration**: 1 week
- **Testing and Optimization**: 1-2 weeks
- **Total**: 4-6 weeks
