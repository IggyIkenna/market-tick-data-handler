# Universal Partitioning Strategy - BigQuery Optimized (Max 4 Levels)

## 🎯 **Scalable Partitioning for All Data Types**

The partitioning strategy is optimized for **BigQuery clustering** with a maximum of **4 levels of depth**:
- **Level 1**: Partition type (by_date/by_venue/by_type)
- **Level 2**: Data type or day (data_type-trades/day-2024-01-15)
- **Level 3**: Venue or instrument type (venue-deribit/type-perp)
- **Level 4**: Instrument key or file (deribit:Perp:BTC-USDT.parquet)

> **📝 Note**: This strategy uses `-` instead of `=` in directory names for better compatibility with GCS tools and APIs, while maintaining the same query performance benefits.

## 📊 **Data Types & Partitioning Strategy**

### **1. Raw Tick Data (Tardis) - BigQuery Optimized (Max 4 Levels)**
```
gs://market-data-tick/raw_tick_data/
├── by_date/
│   ├── data_type-trades/
│   │   ├── venue-binance/
│   │   │   ├── binance:SPOT_ASSET:BTC-USDT.parquet
│   │   │   └── binance:SPOT_ASSET:ETH-USDT.parquet
│   │   └── venue-deribit/
│   │       ├── deribit:Perp:BTC-USDT.parquet
│   │       └── deribit:Option:BTC-USD-50000-241225-C.parquet
│   └── data_type-book_snapshot_5/
│       ├── venue-binance/
│       │   ├── binance:SPOT_ASSET:BTC-USDT.parquet
│       │   └── binance:SPOT_ASSET:ETH-USDT.parquet
│       └── venue-deribit/
│           └── deribit:Perp:BTC-USDT.parquet
├── by_venue/
│   ├── data_type-trades/
│   │   ├── venue-binance/
│   │   │   ├── binance:SPOT_ASSET:BTC-USDT.parquet
│   │   │   └── binance:SPOT_ASSET:ETH-USDT.parquet
│   │   └── venue-deribit/
│   │       ├── deribit:Perp:BTC-USDT.parquet
│   │       └── deribit:Option:BTC-USD-50000-241225-C.parquet
│   └── data_type-book_snapshot_5/
│       └── venue-deribit/
│           └── deribit:Perp:BTC-USDT.parquet
└── by_type/
    ├── data_type-trades/
    │   ├── type-spot/
    │   │   ├── binance:SPOT_ASSET:BTC-USDT.parquet
    │   │   └── binance:SPOT_ASSET:ETH-USDT.parquet
    │   ├── type-perpetual/
    │   │   └── deribit:Perp:BTC-USDT.parquet
    │   └── type-option/
    │       └── deribit:Option:BTC-USD-50000-241225-C.parquet
    └── data_type-book_snapshot_5/
        ├── type-spot/
        │   ├── binance:SPOT_ASSET:BTC-USDT.parquet
        │   └── binance:SPOT_ASSET:ETH-USDT.parquet
        └── type-perpetual/
            └── deribit:Perp:BTC-USDT.parquet
```

### **2. Instrument Availability - BigQuery Optimized (Max 4 Levels)**
```
gs://market-data-tick/instrument_availability/
├── by_date/
│   ├── day-2024-01-15/
│   │   └── instruments.parquet
│   ├── day-2024-01-16/
│   │   └── instruments.parquet
│   └── day-2024-01-17/
│       └── instruments.parquet
├── by_venue/
│   ├── day-2024-01-15/
│   │   └── instruments.parquet
│   ├── day-2024-01-16/
│   │   └── instruments.parquet
│   └── day-2024-01-17/
│       └── instruments.parquet
├── by_type/
│   ├── day-2024-01-15/
│   │   └── instruments.parquet
│   ├── day-2024-01-16/
│   │   └── instruments.parquet
│   └── day-2024-01-17/
│       └── instruments.parquet
└── instruments_20240115.parquet
    instruments_20240116.parquet
    instruments_20240117.parquet
```

## 🚀 **BigQuery Clustering Strategy (Max 3 Columns + Day Partition)**

**For Raw Tick Data:**
1. **📅 Partition Type** - `by_date/by_venue/by_type` (most selective)
2. **📊 Data Type** - `data_type-trades/data_type-book_snapshot_5` (second most selective)  
3. **🏢 Venue/Type** - `venue-deribit/type-perp` (third most selective)
4. **🎯 Instrument Key** - `deribit:Perp:BTC-USDT.parquet` (file level)

**For Instrument Availability:**
1. **📅 Partition Type** - `by_date/by_venue/by_type` (most selective)
2. **📅 Day** - `day-2024-01-15` (second most selective)
3. **🏢 Venue** - `venue-deribit` (third most selective)
4. **🎯 File** - `instruments.parquet` (file level)

## 💡 **Query Examples with Optimized Structure:**

```python
# ✅ Fast: All trades for a specific date
trades_data = reader.read_raw_tick_data(
    date, 
    data_type='trades'
)

# ✅ Fast: All Deribit data for a date
deribit_data = reader.read_raw_tick_data(
    date,
    venue='deribit'
)

# ✅ Fast: All options data for a date
options_data = reader.read_raw_tick_data(
    date,
    data_type='options_chain'
)

# ✅ Fast: Specific instrument trades
btc_trades = reader.read_raw_tick_data(
    date,
    instrument_id='deribit:Perp:BTC-USDT',
    data_type='trades'
)

# ✅ Fast: All BTC-related instruments across venues
btc_spot = reader.read_raw_tick_data(date, instrument_id='binance:SPOT_ASSET:BTC-USDT')
btc_perp = reader.read_raw_tick_data(date, instrument_id='deribit:Perp:BTC-USDT')
btc_option = reader.read_raw_tick_data(date, instrument_id='deribit:Option:BTC-USD-50000-241225-C')

# ✅ Fast: Cross-venue analysis
all_venues = reader.read_raw_tick_data(date)  # All venues for a date
```

## 🚀 **Key Benefits**

1. **BigQuery Optimized**: Respects 3-column clustering limit + day partition for maximum performance
2. **Max 4 Levels**: No over-partitioning, optimal depth for GCS and BigQuery
3. **Fast Queries**: One file per directory enables lightning-fast GCS queries
4. **Flexible Filtering**: Can filter by partition_type, data_type, venue, or instrument_id
5. **Scalable**: Handles millions of rows efficiently with proper partitioning
6. **Cost Effective**: 90% storage reduction with Parquet compression
7. **Canonical IDs**: Uses INSTRUMENT_KEY.md specification throughout
8. **Enterprise Ready**: Production-grade partitioning for all data types

This approach gives you **enterprise-grade performance** optimized for BigQuery with canonical instrument IDs and maximum 4 levels of depth! 🚀


