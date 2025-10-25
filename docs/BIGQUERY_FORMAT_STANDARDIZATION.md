# BigQuery Format Standardization

## Overview

This document explains the decision to standardize on BigQuery-compatible column naming for order book data throughout the entire data pipeline.

## Problem

The original Tardis API uses a format like `asks[0].price`, `bids[0].amount` for order book data, but BigQuery and downstream processing tools expect a more standard format like `ask_price_1`, `bid_volume_1`.

## Solution

We standardized on the BigQuery-compatible format throughout the entire pipeline:

### Format Changes

| Original Tardis Format | New Standardized Format | Description |
|------------------------|-------------------------|-------------|
| `asks[0].price` | `ask_price_1` | Ask price at level 1 |
| `asks[0].amount` | `ask_volume_1` | Ask volume at level 1 |
| `bids[0].price` | `bid_price_1` | Bid price at level 1 |
| `bids[0].amount` | `bid_volume_1` | Bid volume at level 1 |
| `asks[1].price` | `ask_price_2` | Ask price at level 2 |
| `asks[1].amount` | `ask_volume_2` | Ask volume at level 2 |
| ... | ... | ... |

### Key Changes

1. **1-based indexing**: Converted from 0-based (`asks[0]`) to 1-based (`ask_price_1`)
2. **Standardized naming**: Consistent `ask_price_X`, `ask_volume_X`, `bid_price_X`, `bid_volume_X` format
3. **Volume vs Amount**: Changed `amount` to `volume` for consistency with financial terminology

## Implementation

### Data Flow

1. **Tardis API**: Returns `asks[0].price`, `bids[0].amount` format
2. **Tardis Connector**: Converts to `ask_price_1`, `bid_volume_1` format during CSV parsing
3. **GCS Storage**: Stores data in BigQuery-compatible format
4. **BigQuery**: Direct compatibility with standardized column names
5. **Streaming**: Uses the same standardized format

### Code Changes

#### Tardis Connector (`src/data_downloader/tardis_connector.py`)

```python
def _type_book_snapshot_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
    """Convert Tardis format to BigQuery-compatible format"""
    for i in range(5):  # Level 5 book
        level = i + 1  # Convert 0-based to 1-based indexing
        
        # Convert asks
        if f'asks[{i}].price' in df.columns:
            df[f'ask_price_{level}'] = pd.to_numeric(df[f'asks[{i}].price'], errors='coerce').astype('float64')
            df = df.drop(f'asks[{i}].price', axis=1)
        if f'asks[{i}].amount' in df.columns:
            df[f'ask_volume_{level}'] = pd.to_numeric(df[f'asks[{i}].amount'], errors='coerce').astype('float64')
            df = df.drop(f'asks[{i}].amount', axis=1)
        
        # Convert bids
        if f'bids[{i}].price' in df.columns:
            df[f'bid_price_{level}'] = pd.to_numeric(df[f'bids[{i}].price'], errors='coerce').astype('float64')
            df = df.drop(f'bids[{i}].price', axis=1)
        if f'bids[{i}].amount' in df.columns:
            df[f'bid_volume_{level}'] = pd.to_numeric(df[f'bids[{i}].amount'], errors='coerce').astype('float64')
            df = df.drop(f'bids[{i}].amount', axis=1)
    
    return df
```

#### BookSnapshot Model (`src/models.py`)

```python
@dataclass
class BookSnapshot:
    """Order book snapshot model using BigQuery-compatible format"""
    timestamp: int
    local_timestamp: int
    # Level 1 - BigQuery-compatible format (1-based indexing)
    ask_price_1: float
    ask_volume_1: float
    bid_price_1: float
    bid_volume_1: float
    # ... levels 2-5
```

## Benefits

1. **BigQuery Compatibility**: Direct compatibility with BigQuery without additional transformations
2. **Consistency**: Same format used throughout the entire pipeline
3. **Performance**: No need for column renaming or transformations in downstream processing
4. **Maintainability**: Single source of truth for column naming
5. **Streaming Compatibility**: Same format works for both batch and streaming processing

## Migration Impact

- **Existing Data**: New format applies to all new data downloads
- **Historical Data**: Existing data in GCS uses the old format and will need migration if BigQuery compatibility is required
- **Streaming**: Streaming code will be updated to use the new format
- **Processing**: All downstream processing tools now use the standardized format

## Schema Documentation

The updated schema is documented in `tardis_schema.md` with the new BigQuery-compatible column names.

## Future Considerations

1. **Data Migration**: Consider migrating historical data to the new format
2. **API Compatibility**: Ensure all downstream consumers are updated to use the new format
3. **Testing**: Comprehensive testing of the new format across all components
4. **Documentation**: Update all documentation to reflect the new format

## Conclusion

This standardization significantly improves BigQuery compatibility and reduces complexity in downstream processing while maintaining data integrity and performance.
