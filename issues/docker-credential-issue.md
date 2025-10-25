# Docker Credential Issue and Data Types Problem

## Issue Summary

### 1. Docker Credential Problem
**Problem**: Docker build fails with credential error when building images locally:
```
ERROR: error getting credentials - err: exec: "docker-credential-desktop": executable file not found in $PATH
```

**Root Cause**: Docker Desktop credential helper not properly configured or missing.

### 2. Data Types Download Issue
**Problem**: System only downloads 2 out of 5 expected data types:
- ✅ `trades` - 1,267 files downloaded
- ✅ `book_snapshot_5` - 1,267 files downloaded  
- ❌ `derivative_ticker` - 0 files downloaded
- ❌ `liquidations` - 0 files downloaded
- ❌ `options_chain` - 0 files downloaded

**Root Cause**: Instrument definitions don't have these additional data types marked as available in their `data_types` field.

## Workarounds

### Docker Credential Workaround
1. **Use VM-based building**: Build Docker images directly on the VM instead of locally
2. **Manual credential setup**: Run `docker login` and configure credentials manually
3. **Use gcloud auth**: Configure Docker to use gcloud authentication:
   ```bash
   gcloud auth configure-docker asia-northeast1-docker.pkg.dev
   ```

### Data Types Workaround
1. **Check instrument definitions**: Verify that instruments have the correct `data_types` field populated
2. **Regenerate instrument definitions**: Run instrument generation with updated logic to include all data types
3. **Manual verification**: Check if Tardis API actually provides these data types for the instruments on the specific date

## Current Status
- VM deployed successfully with latest code
- Batching optimizations applied (50,000 batch size, 16 workers, 1000 parallel downloads)
- System processes all 1,267 instruments but only downloads trades and book_snapshot_5
- Memory usage very low (3.8% of 31GB), indicating room for much larger batches

## Next Steps
1. Investigate instrument definition generation to ensure all data types are marked as available
2. Verify Tardis API data availability for additional data types on 2023-05-23
3. Consider implementing fallback logic for missing data types
4. Test with different date ranges to see if data availability varies

## Files Modified
- `src/data_downloader/download_orchestrator.py` - Fixed batching and data types logic
- `deploy/vm/deploy-download.sh` - Updated performance settings
- VM deployment - Applied code fixes via Docker container file replacement

## Performance Metrics
- **Batch Size**: 50,000 (configured) vs 1-6 (actual)
- **Workers**: 16 (configured) vs 8 (previous)
- **Parallel Downloads**: 1,000 (configured) vs 500 (previous)
- **Memory Usage**: 3.8% (very low, room for optimization)
- **Processing Time**: ~2 minutes for 1,267 instruments
