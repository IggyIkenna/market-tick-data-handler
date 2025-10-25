# Missing Data Types in Download Process

## Problem Description
The data download system is only downloading 2 out of 5 expected data types:
- ✅ `trades` - Working correctly
- ✅ `book_snapshot_5` - Working correctly
- ❌ `derivative_ticker` - Not being downloaded
- ❌ `liquidations` - Not being downloaded
- ❌ `options_chain` - Not being downloaded

## Root Cause Analysis
The issue is likely in the **instrument definitions generation** process. The instruments don't have these additional data types marked as available in their `data_types` field.

### Expected vs Actual
- **Expected**: Instruments should have `data_types` field like `"trades,book_snapshot_5,derivative_ticker,liquidations,options_chain"`
- **Actual**: Instruments likely only have `"trades,book_snapshot_5"` in their `data_types` field

## Code Fixes Applied
1. **Fixed download orchestrator** (`src/data_downloader/download_orchestrator.py`):
   - Changed `requested_data_types` default from `['trades', 'book_snapshot_5']` to `['trades', 'book_snapshot_5', 'derivative_ticker', 'liquidations', 'options_chain']`
   - Fixed batching logic to use proper batch sizes

2. **Performance optimizations** (`deploy/vm/deploy-download.sh`):
   - Increased batch size to 50,000
   - Increased workers to 16
   - Increased parallel downloads to 1,000

## Verification Steps
1. Check instrument definitions in GCS:
   ```bash
   gsutil cp gs://market-data-tick/instrument_availability/by_date/day-2023-05-23/instruments.parquet /tmp/
   # Then examine data_types column
   ```

2. Check if Tardis API provides these data types:
   - Test API calls for `derivative_ticker`, `liquidations`, `options_chain`
   - Verify data availability for 2023-05-23

3. Check instrument generation logic:
   - Review `src/instrument_processor/canonical_key_generator.py`
   - Ensure `_get_tardis_data_types()` method includes all data types

## Current Status
- ✅ Code fixes applied to VM
- ✅ System processes all 1,267 instruments
- ❌ Only downloads 2 data types instead of 5
- ❌ Batching still not working (uploading 1-6 instruments at a time instead of 50,000)

## Next Steps
1. **Investigate instrument definitions**: Check what data types are actually marked as available
2. **Verify Tardis API**: Test if additional data types are available for these instruments on this date
3. **Fix instrument generation**: Ensure all data types are included in instrument definitions
4. **Debug batching**: Figure out why batching isn't working despite code fixes

## Files to Check
- `src/instrument_processor/canonical_key_generator.py` - `_get_tardis_data_types()` method
- `src/instrument_processor/instrument_generator.py` - Instrument definition generation
- `gs://market-data-tick/instrument_availability/by_date/day-2023-05-23/instruments.parquet` - Actual instrument definitions

## Test Commands
```bash
# Check current data type counts
gsutil ls gs://market-data-tick/raw_tick_data/by_date/day-2023-05-23/data_type-trades/ | wc -l
gsutil ls gs://market-data-tick/raw_tick_data/by_date/day-2023-05-23/data_type-derivative_ticker/ | wc -l
gsutil ls gs://market-data-tick/raw_tick_data/by_date/day-2023-05-23/data_type-options_chain/ | wc -l
```
