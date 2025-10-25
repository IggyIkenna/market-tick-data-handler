# HFT Features Specification

## Overview

High-Frequency Trading (HFT) features are derived metrics calculated from raw tick data to capture market microstructure patterns, trading behavior, and execution quality. These features are essential for:

- **Execution Algorithms**: Optimizing trade execution timing and strategy
- **Market Making**: Understanding bid-ask dynamics and order flow
- **Risk Management**: Detecting unusual trading patterns and liquidity conditions
- **Alpha Generation**: Identifying short-term market inefficiencies

## Feature Categories

### 1. Trade Data Core Metrics

These features are computed from raw trade data and provide insights into trading activity and execution quality.

#### Volume-Based Features

**`buy_volume_sum`** - Total volume of buy-side trades
- **Calculation**: Sum of `size` where `side` = 'buy'
- **Aggregation**: SUM across timeframes
- **Use Case**: Measure buying pressure and market sentiment

**`sell_volume_sum`** - Total volume of sell-side trades  
- **Calculation**: Sum of `size` where `side` = 'sell'
- **Aggregation**: SUM across timeframes
- **Use Case**: Measure selling pressure and market sentiment

**`size_avg`** - Average trade size
- **Calculation**: Mean of `size` across all trades
- **Aggregation**: Recalculated for each timeframe (not aggregated)
- **Use Case**: Identify institutional vs retail trading patterns

**`price_vwap`** - Volume-weighted average price
- **Calculation**: `sum(price * size) / sum(size)`
- **Aggregation**: Recalculated for each timeframe (not aggregated)
- **Use Case**: Fair value price estimation, execution quality measurement

#### Activity Features

**`trade_count`** - Number of trades
- **Calculation**: Count of trade records
- **Aggregation**: SUM across timeframes
- **Use Case**: Market activity level, liquidity proxy

#### Price Features

**`open`** - Opening price
- **Calculation**: First `price` in timeframe
- **Aggregation**: FIRST across timeframes
- **Use Case**: Price level tracking

**`close`** - Closing price
- **Calculation**: Last `price` in timeframe
- **Aggregation**: LAST across timeframes
- **Use Case**: Price level tracking

**`high`** - Highest price
- **Calculation**: Maximum `price` in timeframe
- **Aggregation**: MAX across timeframes
- **Use Case**: Price range analysis

**`low`** - Lowest price
- **Calculation**: Minimum `price` in timeframe
- **Aggregation**: MIN across timeframes
- **Use Case**: Price range analysis

#### Latency Features

**`delay_median`** - Median execution delay
- **Calculation**: Median of `(local_timestamp - timestamp)`
- **Aggregation**: Recalculated for each timeframe (not aggregated)
- **Use Case**: Data quality assessment, network latency monitoring

**`delay_mean`** - Mean execution delay
- **Calculation**: Mean of `(local_timestamp - timestamp)`
- **Aggregation**: Recalculated for each timeframe (not aggregated)
- **Use Case**: Data quality assessment, network latency monitoring

**`delay_max`** - Maximum execution delay
- **Calculation**: Maximum of `(local_timestamp - timestamp)`
- **Aggregation**: MAX across timeframes
- **Use Case**: Data quality monitoring, outlier detection

**`delay_min`** - Minimum execution delay
- **Calculation**: Minimum of `(local_timestamp - timestamp)`
- **Aggregation**: MIN across timeframes
- **Use Case**: Data quality monitoring, baseline latency

### 2. Liquidation Trade Features

These features capture forced liquidation events, which are critical for understanding market stress and directional pressure.

#### Liquidation Volume Features

**`liquidation_buy_volume`** - Volume of buy-side liquidations
- **Calculation**: Sum of `size` where `side` = 'buy' AND `trade_type` = 'liquidation'
- **Aggregation**: SUM across timeframes
- **Use Case**: Measure forced selling pressure (buy liquidations = forced sells)

**`liquidation_sell_volume`** - Volume of sell-side liquidations
- **Calculation**: Sum of `size` where `side` = 'sell' AND `trade_type` = 'liquidation'
- **Aggregation**: SUM across timeframes
- **Use Case**: Measure forced buying pressure (sell liquidations = forced buys)

**`liquidation_count`** - Number of liquidation events
- **Calculation**: Count of trades where `trade_type` = 'liquidation'
- **Aggregation**: SUM across timeframes
- **Use Case**: Liquidation frequency, market stress indicator

#### Open Interest Change Signals

**`oi_change`** - Change in open interest vs previous interval
- **Calculation**: `current_open_interest - previous_open_interest`
- **Aggregation**: Recalculated for each timeframe
- **Use Case**: Position flow analysis, market sentiment

**`liquidation_with_rising_oi`** - Liquidations while OI rising
- **Calculation**: `liquidation_volume` when `oi_change > 0`
- **Aggregation**: SUM across timeframes
- **Use Case**: Momentum continuation signal (liquidations + new positions)

**`liquidation_with_falling_oi`** - Liquidations while OI falling
- **Calculation**: `liquidation_volume` when `oi_change < 0`
- **Aggregation**: SUM across timeframes
- **Use Case**: Exhaustion/mean reversion signal (liquidations + position reduction)

### 3. Derivatives Ticker Features

These features capture derivatives market conditions and are only available for perpetual and futures instruments.

#### Last Value Features (Derivatives Only)

**`funding_rate`** - Current funding rate
- **Calculation**: Last non-NaN value of `funding_rate`
- **Aggregation**: LAST non-NaN across timeframes
- **Use Case**: Cost of carry, market sentiment

**`index_price`** - Underlying index price
- **Calculation**: Last non-NaN value of `index_price`
- **Aggregation**: LAST non-NaN across timeframes
- **Use Case**: Fair value calculation, basis analysis

**`mark_price`** - Mark price for margin calculations
- **Calculation**: Last non-NaN value of `mark_price`
- **Aggregation**: LAST non-NaN across timeframes
- **Use Case**: Margin calculations, liquidation risk

**`open_interest`** - Total open interest
- **Calculation**: Last non-NaN value of `open_interest`
- **Aggregation**: LAST non-NaN across timeframes
- **Use Case**: Market depth, position flow analysis

**`predicted_funding_rate`** - Next funding rate prediction
- **Calculation**: Last non-NaN value of `predicted_funding_rate`
- **Aggregation**: LAST non-NaN across timeframes
- **Use Case**: Forward-looking funding cost estimation

### 4. Options Chain Features (Advanced)

These features require complex options chain analysis and are computationally intensive.

#### 25-Delta Skew Features

**`skew_25d_put_call_ratio`** - 25-delta put/call mark_iv ratio
- **Calculation**: 
  1. Group all options by underlying instrument
  2. Find closest expiry date
  3. Find 25-delta put and call options
  4. Calculate ratio: `put_mark_iv / call_mark_iv`
- **Aggregation**: Recalculated for each timeframe
- **Use Case**: Volatility smile analysis, market sentiment

**`atm_mark_iv`** - At-the-money (50-delta) mark_iv
- **Calculation**:
  1. Group all options by underlying instrument
  2. Find closest expiry date
  3. Find 50-delta option (closest to ATM)
  4. Extract `mark_iv` value
- **Aggregation**: Recalculated for each timeframe
- **Use Case**: Volatility level analysis, options pricing

## Feature Computation Strategy

### Timeframe Processing

HFT features are computed across multiple timeframes:

- **15s**: Primary timeframe for high-frequency analysis
- **1m**: Standard timeframe for most HFT strategies
- **5m, 15m, 1h, 4h, 24h**: Aggregated timeframes for longer-term analysis

### Aggregation Rules

#### Features Computed Fresh (Not Aggregated)
These features require recalculation at each timeframe for accuracy:

- `size_avg` - Average trade size
- `price_vwap` - Volume-weighted average price
- `delay_median`, `delay_mean` - Latency metrics
- `oi_change` - Open interest change
- `skew_25d_put_call_ratio` - Options skew
- `atm_mark_iv` - ATM volatility

#### Features Computed at 15s and Aggregated
These features are calculated at 15s and then aggregated:

- `buy_volume_sum`, `sell_volume_sum` - Volume aggregations (SUM)
- `trade_count` - Trade count (SUM)
- `delay_max`, `delay_min` - Latency extremes (MAX/MIN)
- `open`, `close` - Price levels (FIRST/LAST)
- `high`, `low` - Price extremes (MAX/MIN)
- `liquidation_buy_volume`, `liquidation_sell_volume` - Liquidation volumes (SUM)
- `liquidation_count` - Liquidation count (SUM)
- `funding_rate`, `index_price`, `mark_price`, `open_interest`, `predicted_funding_rate` - Derivatives ticker (LAST non-NaN)

### Missing Data Handling

- **Empty Intervals**: When no trades occur in a timeframe, all features are set to `NaN`
- **Missing Derivatives Data**: Derivatives ticker features are `NaN` for spot instruments
- **Missing Options Data**: Options chain features are `NaN` when no options are available
- **Downstream Handling**: Downstream services can choose to forward-fill, interpolate, or handle `NaN` values as appropriate

## Implementation Details

### Data Sources

- **Trade Data**: Raw tick data from Tardis.dev
- **Liquidation Data**: Trade records with `trade_type = 'liquidation'`
- **Derivatives Ticker**: Market data with funding rates, open interest, etc.
- **Options Chain**: Complete options chain data for underlying instruments

### Performance Considerations

- **Parquet Filtering**: Leverage Parquet's row group statistics for efficient timestamp filtering
- **Memory Management**: Process data in chunks to handle large datasets
- **Parallel Processing**: Compute features in parallel where possible
- **Caching**: Cache frequently accessed derivatives ticker data

### Quality Assurance

- **Validation**: Ensure all features are within reasonable bounds
- **Consistency**: Verify aggregation rules produce consistent results
- **Completeness**: Check for missing data and handle appropriately
- **Accuracy**: Validate against known market conditions and expected patterns

## Usage Examples

### Execution Service
```python
# Get 15s candles with HFT features for execution
candles = candle_reader.get_candles(
    instrument_id="BINANCE:SPOT_PAIR:BTC-USDT",
    timeframe="15s",
    start_date=start_date,
    end_date=end_date
)

# Analyze execution quality
execution_quality = {
    'avg_trade_size': candles['size_avg'].mean(),
    'price_impact': (candles['price_vwap'] - candles['close']).abs().mean(),
    'liquidation_pressure': candles['liquidation_buy_volume'].sum()
}
```

### Market Making
```python
# Analyze order flow and liquidity
order_flow = {
    'buy_pressure': candles['buy_volume_sum'] / (candles['buy_volume_sum'] + candles['sell_volume_sum']),
    'liquidation_events': candles['liquidation_count'].sum(),
    'funding_cost': candles['funding_rate'].iloc[-1]
}
```

### Risk Management
```python
# Monitor market stress indicators
risk_metrics = {
    'liquidation_volume': candles['liquidation_buy_volume'].sum() + candles['liquidation_sell_volume'].sum(),
    'volatility_skew': candles['skew_25d_put_call_ratio'].iloc[-1],
    'position_flow': candles['oi_change'].sum()
}
```

## Future Enhancements

### Additional Features
- **Order Book Imbalance**: Bid-ask volume ratios
- **Trade Size Distribution**: Percentiles of trade sizes
- **Price Impact**: Temporary vs permanent price impact
- **Cross-Asset Correlations**: Features across related instruments

### Advanced Analytics
- **Regime Detection**: Identify market regimes using HFT features
- **Anomaly Detection**: Flag unusual trading patterns
- **Predictive Models**: Use HFT features for short-term price prediction
- **Execution Optimization**: Real-time execution strategy adjustment

This specification provides a comprehensive foundation for implementing and using HFT features in high-frequency trading applications.
