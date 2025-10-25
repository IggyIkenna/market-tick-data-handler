# Schema Validation Report

## Overview

This report documents the validation of Parquet schemas against Tardis raw data to ensure exact matching (minus exchange/date columns used only for validation).

**Test Date**: May 23, 2023  
**Test Scope**: All venue × data_type × instrument_type combinations  
**Validation Criteria**: Exact column names, data types, and order matching

## Expected Schemas

Based on `tardis_schema.md`, the expected schemas for each data type are:

### trades
- `timestamp` (int64): Exchange timestamp in microseconds since epoch
- `local_timestamp` (int64): Local arrival timestamp in microseconds since epoch  
- `id` (string): Trade ID (if available)
- `side` (string): Trade side ('buy' or 'sell')
- `price` (float64): Trade price
- `amount` (float64): Trade amount

### book_snapshot_5
- `timestamp` (int64): Exchange timestamp in microseconds since epoch
- `local_timestamp` (int64): Local arrival timestamp in microseconds since epoch
- `asks[0].price` (float64): Ask price at level 0
- `asks[0].amount` (float64): Ask amount at level 0
- `bids[0].price` (float64): Bid price at level 0
- `bids[0].amount` (float64): Bid amount at level 0
- `asks[1].price` (float64): Ask price at level 1
- `asks[1].amount` (float64): Ask amount at level 1
- `bids[1].price` (float64): Bid price at level 1
- `bids[1].amount` (float64): Bid amount at level 1
- `asks[2].price` (float64): Ask price at level 2
- `asks[2].amount` (float64): Ask amount at level 2
- `bids[2].price` (float64): Bid price at level 2
- `bids[2].amount` (float64): Bid amount at level 2
- `asks[3].price` (float64): Ask price at level 3
- `asks[3].amount` (float64): Ask amount at level 3
- `bids[3].price` (float64): Bid price at level 3
- `bids[3].amount` (float64): Bid amount at level 3
- `asks[4].price` (float64): Ask price at level 4
- `asks[4].amount` (float64): Ask amount at level 4
- `bids[4].price` (float64): Bid price at level 4
- `bids[4].amount` (float64): Bid amount at level 4

### derivative_ticker
- `timestamp` (int64): Exchange timestamp in microseconds since epoch
- `local_timestamp` (int64): Local arrival timestamp in microseconds since epoch
- `funding_timestamp` (int64): Funding timestamp (if provided)
- `funding_rate` (float64): Current funding rate
- `predicted_funding_rate` (float64): Predicted funding rate
- `open_interest` (float64): Open interest value
- `last_price` (float64): Last traded price
- `index_price` (float64): Underlying index price
- `mark_price` (float64): Mark price

### liquidations
- `timestamp` (int64): Exchange timestamp in microseconds since epoch
- `local_timestamp` (int64): Local arrival timestamp in microseconds since epoch
- `id` (string): Liquidation ID (if available)
- `side` (string): Side: 'buy' = short liquidated, 'sell' = long liquidated
- `price` (float64): Liquidation price
- `amount` (float64): Liquidation amount

### options_chain
- `timestamp` (int64): Exchange timestamp in microseconds since epoch
- `local_timestamp` (int64): Local arrival timestamp in microseconds since epoch
- `type` (string): Option type (put/call)
- `strike_price` (float64): Strike price of the option
- `expiration` (int64): Expiration timestamp in microseconds since epoch
- `open_interest` (float64): Open interest for the option
- `last_price` (float64): Last traded price
- `bid_price` (float64): Best bid price for the option
- `bid_amount` (float64): Best bid amount
- `bid_iv` (float64): Bid implied volatility
- `ask_price` (float64): Best ask price for the option
- `ask_amount` (float64): Best ask amount
- `ask_iv` (float64): Ask implied volatility
- `mark_price` (float64): Mark price of the option
- `mark_iv` (float64): Mark implied volatility
- `underlying_index` (string): Underlying instrument symbol
- `underlying_price` (float64): Underlying asset price
- `delta` (float64): Option delta
- `gamma` (float64): Option gamma
- `vega` (float64): Option vega
- `theta` (float64): Option theta
- `rho` (float64): Option rho

## Test Venues and Instruments

### Venues Tested
- **binance**: Spot trading (BTCUSDT)
- **binance-futures**: Perpetual futures (BTCUSDT)
- **deribit**: Perpetual futures and options (BTC-PERPETUAL, BTC-23MAY23-50000-C)
- **bybit**: Perpetual futures (BTCUSDT)
- **bybit-spot**: Spot trading (BTCUSDT)
- **okex**: Spot trading (BTC-USDT)
- **okex-futures**: Futures (BTC-USDT-SWAP)
- **okex-swap**: Perpetual swaps (BTC-USDT-SWAP)

### Data Types per Venue
- **trades**: All venues
- **book_snapshot_5**: All venues
- **derivative_ticker**: binance-futures, deribit, bybit, okex-futures, okex-swap
- **liquidations**: binance-futures, deribit, bybit, okex-futures, okex-swap
- **options_chain**: deribit only

## Validation Results

### Schema Matching Criteria
1. **Column Names**: Must match exactly (case-sensitive)
2. **Data Types**: Must match exactly (int64, float64, string)
3. **Column Order**: Must match Tardis exactly
4. **Validation Columns**: Exchange and symbol columns must be present in raw data but dropped after validation
5. **All Other Columns**: Must be preserved exactly

### Test Results Summary

*Note: This section will be populated after running the tests*

## Potential Issues and Fixes

### Common Schema Issues

1. **Column Name Mismatches**
   - **Issue**: Different naming conventions between exchanges
   - **Fix**: Ensure consistent mapping in `tardis_connector.py`

2. **Data Type Inconsistencies**
   - **Issue**: String vs numeric types for IDs
   - **Fix**: Standardize type conversion in `_parse_csv_data` methods

3. **Missing Columns**
   - **Issue**: Some exchanges may not provide all expected columns
   - **Fix**: Handle missing columns gracefully with proper defaults

4. **Column Order Differences**
   - **Issue**: Processed data may have different column order
   - **Fix**: Preserve original Tardis column order in processing

### Validation Column Handling

The system should:
1. **Raw Data**: Include `exchange` and `symbol` columns for validation
2. **Validation**: Check that exchange/symbol match expected values
3. **Processed Data**: Drop `exchange` and `symbol` columns after validation
4. **Final Schema**: Contain only the core data columns

## Recommendations

1. **Run Tests Regularly**: Include schema validation in CI/CD pipeline
2. **Monitor Changes**: Track Tardis API schema changes
3. **Documentation**: Keep this report updated with any schema changes
4. **Error Handling**: Implement graceful handling of schema mismatches

## Test Execution

To run the schema validation tests:

```bash
# Run all schema validation tests
pytest tests/test_schema_validation.py -v

# Run specific test class
pytest tests/test_schema_validation.py::TestTradesSchema -v

# Run with detailed logging
pytest tests/test_schema_validation.py -v -s --log-cli-level=INFO
```

## Future Enhancements

1. **Automated Schema Updates**: Detect and handle Tardis schema changes automatically
2. **Performance Testing**: Include schema validation in performance benchmarks
3. **Cross-Venue Validation**: Ensure consistent schemas across venues
4. **Data Quality Checks**: Validate not just schema but data quality and ranges
