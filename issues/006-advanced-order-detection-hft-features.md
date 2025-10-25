# Issue #006: Advanced Order Detection HFT Features

## Problem Statement

The current HFT features system lacks advanced order detection capabilities needed for high-frequency trading strategies. We need to implement sophisticated features that can detect other market participants' orders, including large orders, icebergs, hidden orders, and various market manipulation patterns.

## Current State Analysis

### Current HFT Features

**Files**:
- `src/streaming_service/hft_features/feature_calculator.py`
- `market_data_tick_handler/streaming_service/hft_features/feature_calculator.py`
- `docs/HFT_FEATURES_SPECIFICATION.md`

The current system implements basic features:

```python
@dataclass
class HFTFeatures:
    # Basic microstructure features
    trade_intensity: Optional[float] = None
    avg_trade_size: Optional[float] = None
    bid_ask_spread: Optional[float] = None
    order_flow_imbalance: Optional[float] = None
    # ... other basic features
```

### Missing Advanced Order Detection Features

The system lacks sophisticated order detection capabilities:

1. **Large Order Detection**: Cannot identify iceberg orders, hidden orders, or large institutional orders
2. **Order Flow Toxicity**: No metrics for order flow toxicity or adverse selection
3. **Market Maker Detection**: Cannot identify market maker presence or behavior
4. **Spoofing/Layering Detection**: No detection of market manipulation patterns
5. **Execution Classification**: Cannot classify aggressive vs passive executions
6. **Order Book Analysis**: Limited analysis of order book dynamics and imbalances

## Proposed Solutions

### Solution 1: Advanced Order Detection Features

Extend the HFTFeatures data class with advanced order detection features:

```python
@dataclass
class AdvancedOrderDetectionFeatures:
    # Large order detection
    iceberg_probability: Optional[float] = None
    hidden_order_probability: Optional[float] = None
    large_order_impact: Optional[float] = None
    order_size_distribution: Optional[Dict[str, float]] = None
    
    # Order flow toxicity
    order_flow_toxicity: Optional[float] = None
    adverse_selection_risk: Optional[float] = None
    informed_trading_probability: Optional[float] = None
    price_impact_decay: Optional[float] = None
    
    # Market maker detection
    market_maker_presence: Optional[float] = None
    market_maker_activity: Optional[float] = None
    market_maker_profitability: Optional[float] = None
    market_maker_aggressiveness: Optional[float] = None
    
    # Spoofing/layering detection
    spoofing_probability: Optional[float] = None
    layering_probability: Optional[float] = None
    quote_stuffing_probability: Optional[float] = None
    manipulation_risk_score: Optional[float] = None
    
    # Execution classification
    aggressive_buy_ratio: Optional[float] = None
    aggressive_sell_ratio: Optional[float] = None
    passive_execution_ratio: Optional[float] = None
    execution_quality_score: Optional[float] = None
    
    # Order book analysis
    order_book_imbalance: Optional[float] = None
    order_book_pressure: Optional[float] = None
    order_book_resilience: Optional[float] = None
    order_book_volatility: Optional[float] = None
```

### Solution 2: Large Order Detection System

Implement sophisticated large order detection:

```python
class LargeOrderDetector:
    def __init__(self, config):
        self.config = config
        self.order_history = deque(maxlen=10000)
        self.volume_profile = {}
        self.size_thresholds = {
            'small': 0.1,    # 10% of average
            'medium': 0.5,   # 50% of average
            'large': 2.0,    # 200% of average
            'iceberg': 5.0   # 500% of average
        }
    
    def detect_iceberg_orders(self, trades: List[Trade]) -> List[IcebergOrder]:
        """Detect potential iceberg orders"""
        icebergs = []
        
        for i, trade in enumerate(trades):
            if self.is_potential_iceberg(trade, trades[i-10:i+10]):
                iceberg = IcebergOrder(
                    symbol=trade.symbol,
                    timestamp=trade.timestamp,
                    price=trade.price,
                    visible_size=trade.amount,
                    estimated_total_size=self.estimate_total_size(trade),
                    confidence=self.calculate_iceberg_confidence(trade)
                )
                icebergs.append(iceberg)
        
        return icebergs
    
    def is_potential_iceberg(self, trade: Trade, context_trades: List[Trade]) -> bool:
        """Determine if a trade is part of an iceberg order"""
        # Check if trade size is unusually large
        if trade.amount < self.get_size_threshold('large'):
            return False
        
        # Check for consistent price and timing patterns
        price_consistency = self.check_price_consistency(trade, context_trades)
        timing_pattern = self.check_timing_pattern(trade, context_trades)
        
        # Check for order book impact
        order_book_impact = self.calculate_order_book_impact(trade)
        
        return (price_consistency > 0.8 and 
                timing_pattern > 0.7 and 
                order_book_impact < 0.3)
    
    def detect_hidden_orders(self, order_book: OrderBook, trades: List[Trade]) -> List[HiddenOrder]:
        """Detect hidden orders from order book and trade analysis"""
        hidden_orders = []
        
        for trade in trades:
            if self.is_hidden_order_execution(trade, order_book):
                hidden_order = HiddenOrder(
                    symbol=trade.symbol,
                    timestamp=trade.timestamp,
                    price=trade.price,
                    size=trade.amount,
                    confidence=self.calculate_hidden_confidence(trade, order_book)
                )
                hidden_orders.append(hidden_order)
        
        return hidden_orders
```

### Solution 3: Order Flow Toxicity Analysis

Implement order flow toxicity metrics:

```python
class OrderFlowToxicityAnalyzer:
    def __init__(self, config):
        self.config = config
        self.trade_history = deque(maxlen=1000)
        self.price_history = deque(maxlen=1000)
        self.volume_history = deque(maxlen=1000)
    
    def calculate_order_flow_toxicity(self, trades: List[Trade]) -> float:
        """Calculate order flow toxicity using various metrics"""
        if len(trades) < 10:
            return 0.0
        
        # Calculate price impact
        price_impact = self.calculate_price_impact(trades)
        
        # Calculate volume-weighted price impact
        vwap_impact = self.calculate_vwap_impact(trades)
        
        # Calculate adverse selection risk
        adverse_selection = self.calculate_adverse_selection_risk(trades)
        
        # Calculate informed trading probability
        informed_trading = self.calculate_informed_trading_probability(trades)
        
        # Combine metrics
        toxicity = (
            price_impact * 0.3 +
            vwap_impact * 0.3 +
            adverse_selection * 0.2 +
            informed_trading * 0.2
        )
        
        return min(max(toxicity, 0.0), 1.0)
    
    def calculate_price_impact(self, trades: List[Trade]) -> float:
        """Calculate price impact of trades"""
        if len(trades) < 2:
            return 0.0
        
        price_changes = []
        for i in range(1, len(trades)):
            price_change = abs(trades[i].price - trades[i-1].price) / trades[i-1].price
            price_changes.append(price_change)
        
        return np.mean(price_changes) if price_changes else 0.0
    
    def calculate_adverse_selection_risk(self, trades: List[Trade]) -> float:
        """Calculate adverse selection risk"""
        if len(trades) < 10:
            return 0.0
        
        # Calculate correlation between trade size and price movement
        sizes = [trade.amount for trade in trades]
        prices = [trade.price for trade in trades]
        
        # Calculate price momentum
        price_momentum = np.diff(prices)
        
        # Calculate correlation
        correlation = np.corrcoef(sizes[1:], price_momentum)[0, 1]
        
        # Convert to risk score (0-1)
        return abs(correlation) if not np.isnan(correlation) else 0.0
```

### Solution 4: Market Maker Detection

Implement market maker detection and analysis:

```python
class MarketMakerDetector:
    def __init__(self, config):
        self.config = config
        self.order_patterns = {}
        self.execution_patterns = {}
    
    def detect_market_maker_presence(self, trades: List[Trade], order_book: OrderBook) -> float:
        """Detect market maker presence and activity"""
        if len(trades) < 50:
            return 0.0
        
        # Analyze trading patterns
        pattern_score = self.analyze_trading_patterns(trades)
        
        # Analyze order book behavior
        order_book_score = self.analyze_order_book_behavior(order_book)
        
        # Analyze execution characteristics
        execution_score = self.analyze_execution_characteristics(trades)
        
        # Combine scores
        presence_score = (
            pattern_score * 0.4 +
            order_book_score * 0.3 +
            execution_score * 0.3
        )
        
        return min(max(presence_score, 0.0), 1.0)
    
    def analyze_trading_patterns(self, trades: List[Trade]) -> float:
        """Analyze trading patterns for market maker characteristics"""
        # Check for consistent bid-ask spread
        spread_consistency = self.calculate_spread_consistency(trades)
        
        # Check for round-trip trading
        round_trip_ratio = self.calculate_round_trip_ratio(trades)
        
        # Check for inventory management
        inventory_management = self.calculate_inventory_management_score(trades)
        
        # Combine pattern scores
        pattern_score = (
            spread_consistency * 0.4 +
            round_trip_ratio * 0.3 +
            inventory_management * 0.3
        )
        
        return pattern_score
    
    def calculate_spread_consistency(self, trades: List[Trade]) -> float:
        """Calculate consistency of bid-ask spread"""
        # This would require order book data
        # For now, use price volatility as proxy
        prices = [trade.price for trade in trades]
        if len(prices) < 10:
            return 0.0
        
        price_volatility = np.std(prices) / np.mean(prices)
        # Lower volatility suggests more consistent spreads
        return max(0.0, 1.0 - price_volatility * 10)
```

### Solution 5: Spoofing and Layering Detection

Implement market manipulation detection:

```python
class MarketManipulationDetector:
    def __init__(self, config):
        self.config = config
        self.order_sequence = deque(maxlen=1000)
        self.cancel_patterns = {}
    
    def detect_spoofing(self, order_book: OrderBook, trades: List[Trade]) -> float:
        """Detect potential spoofing behavior"""
        if len(trades) < 20:
            return 0.0
        
        # Analyze order placement and cancellation patterns
        placement_pattern = self.analyze_order_placement_pattern(order_book)
        cancellation_pattern = self.analyze_cancellation_pattern(order_book)
        price_impact_pattern = self.analyze_price_impact_pattern(trades)
        
        # Combine patterns
        spoofing_score = (
            placement_pattern * 0.4 +
            cancellation_pattern * 0.4 +
            price_impact_pattern * 0.2
        )
        
        return min(max(spoofing_score, 0.0), 1.0)
    
    def detect_layering(self, order_book: OrderBook) -> float:
        """Detect potential layering behavior"""
        # Analyze order book structure
        book_imbalance = self.calculate_order_book_imbalance(order_book)
        price_levels = self.analyze_price_levels(order_book)
        order_distribution = self.analyze_order_distribution(order_book)
        
        # Combine metrics
        layering_score = (
            book_imbalance * 0.3 +
            price_levels * 0.3 +
            order_distribution * 0.4
        )
        
        return min(max(layering_score, 0.0), 1.0)
    
    def analyze_order_placement_pattern(self, order_book: OrderBook) -> float:
        """Analyze order placement patterns for spoofing"""
        # Check for large orders placed far from market
        # Check for rapid order placement and cancellation
        # Check for order size patterns
        return 0.0  # Placeholder
```

### Solution 6: Execution Classification System

Implement execution classification:

```python
class ExecutionClassifier:
    def __init__(self, config):
        self.config = config
        self.execution_history = deque(maxlen=1000)
    
    def classify_execution(self, trade: Trade, order_book: OrderBook) -> ExecutionClassification:
        """Classify trade execution as aggressive or passive"""
        # Determine if trade was aggressive (market order) or passive (limit order)
        aggressiveness = self.calculate_aggressiveness(trade, order_book)
        
        # Determine execution quality
        quality = self.calculate_execution_quality(trade, order_book)
        
        # Determine execution timing
        timing = self.calculate_execution_timing(trade)
        
        return ExecutionClassification(
            trade_id=trade.id,
            aggressiveness=aggressiveness,
            quality=quality,
            timing=timing,
            classification=self.determine_classification(aggressiveness, quality, timing)
        )
    
    def calculate_aggressiveness(self, trade: Trade, order_book: OrderBook) -> float:
        """Calculate aggressiveness of trade execution"""
        # Compare trade price to best bid/ask
        best_bid = order_book.best_bid_price
        best_ask = order_book.best_ask_price
        mid_price = (best_bid + best_ask) / 2
        
        if trade.side == 'buy':
            # Buy trade at or above ask is aggressive
            if trade.price >= best_ask:
                return 1.0
            else:
                return (trade.price - mid_price) / (best_ask - mid_price)
        else:
            # Sell trade at or below bid is aggressive
            if trade.price <= best_bid:
                return 1.0
            else:
                return (mid_price - trade.price) / (mid_price - best_bid)
```

## Implementation Tasks

### Phase 1: Data Structure Extension
- [ ] Extend HFTFeatures with advanced order detection fields
- [ ] Create supporting data classes (IcebergOrder, HiddenOrder, etc.)
- [ ] Update feature calculation pipeline
- [ ] Add configuration for order detection parameters

### Phase 2: Large Order Detection
- [ ] Implement LargeOrderDetector class
- [ ] Add iceberg order detection algorithms
- [ ] Add hidden order detection algorithms
- [ ] Implement order size analysis and profiling

### Phase 3: Order Flow Analysis
- [ ] Implement OrderFlowToxicityAnalyzer class
- [ ] Add price impact calculation methods
- [ ] Add adverse selection risk analysis
- [ ] Implement informed trading probability calculation

### Phase 4: Market Maker Detection
- [ ] Implement MarketMakerDetector class
- [ ] Add trading pattern analysis
- [ ] Add order book behavior analysis
- [ ] Implement market maker activity scoring

### Phase 5: Manipulation Detection
- [ ] Implement MarketManipulationDetector class
- [ ] Add spoofing detection algorithms
- [ ] Add layering detection algorithms
- [ ] Implement manipulation risk scoring

### Phase 6: Execution Classification
- [ ] Implement ExecutionClassifier class
- [ ] Add aggressiveness calculation
- [ ] Add execution quality metrics
- [ ] Implement execution timing analysis

## Dependencies and Risks

### Dependencies
- High-quality order book data
- Trade execution data with timestamps
- Machine learning libraries for pattern recognition
- Statistical analysis libraries
- Real-time data processing capabilities

### Risks
- **Data Quality**: Order detection requires high-quality data
- **False Positives**: Detection algorithms may produce false positives
- **Performance**: Complex algorithms may impact performance
- **Regulatory**: Some detection methods may have regulatory implications

### Mitigation Strategies
- Implement confidence scoring for all detections
- Add validation and backtesting capabilities
- Use machine learning for pattern recognition
- Add configuration for sensitivity tuning

## Success Criteria

1. **Detection Accuracy**: >90% accuracy for large order detection
2. **False Positive Rate**: <5% false positive rate for manipulation detection
3. **Performance**: Order detection completes within 50ms
4. **Coverage**: All major order types and patterns are detected
5. **Maintainability**: Easy to add new detection algorithms

## Priority

**High** - This is critical for advanced HFT strategies and risk management.

## Estimated Effort

- **Data Structure Extension**: 1 week
- **Large Order Detection**: 2-3 weeks
- **Order Flow Analysis**: 2-3 weeks
- **Market Maker Detection**: 2-3 weeks
- **Manipulation Detection**: 3-4 weeks
- **Execution Classification**: 1-2 weeks
- **Integration and Testing**: 2-3 weeks
- **Total**: 13-19 weeks
