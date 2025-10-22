# Archived Documentation

This directory contains historical documentation that has been archived because it no longer reflects the current codebase implementation.

## Why These Documents Were Archived

The codebase has evolved significantly, and these documents contain outdated information that contradicts the current implementation:

### Current Implementation Reality
- **Partitioning Strategy**: Single `by_date` partition only (`by_date/day-{date}/`)
- **No Triple Partitioning**: The old `by_date`, `by_venue`, `by_type` triple partitioning has been removed
- **No Orchestrator Directory**: `src/orchestrator/` has been eliminated, using `DownloadOrchestrator` directly
- **Deploy Structure**: Uses `deploy/local/` and `deploy/vm/` structure
- **Entry Point**: `src/main.py` with three modes (instruments, download, full-pipeline)
- **Validation**: `src/data_validator/data_validator.py` is integrated

## Archived Documents

### Partitioning Strategy Documents (Obsolete)
- `BIGQUERY_OPTIMIZATION_UPDATE.md` - References old triple partition removal
- `BIGQUERY_OPTIMIZATION_MAX_4_LEVELS.md` - References 4-level nesting no longer used
- `UNIVERSAL_PARTITIONING_STRATEGY_V2.md` - Documents triple partitioning (obsolete)
- `UNIVERSAL_PARTITIONING_STRATEGY.md` - Documents triple partitioning (obsolete)
- `COMPLETE_DATA_ARCHITECTURE.md` - References complex multi-tier partitioning not implemented
- `OPTIMIZED_SINGLE_PARTITION_STRATEGY.md` - Good info but superseded by implementation

### Historical Implementation Documents
- `SINGLE_PARTITION_IMPLEMENTATION_SUMMARY.md` - Historical implementation notes
- `ORCHESTRATOR_ANALYSIS.md` - Analysis of removed orchestrator
- `ORCHESTRATOR_ELIMINATION_SUMMARY.md` - Historical elimination notes
- `FILE_REORGANIZATION_SUMMARY.md` - Historical reorganization notes
- `SCRIPTS_CLEANUP_SUMMARY.md` - Historical cleanup notes

### Outdated Performance and Structure Documents
- `PERFORMANCE_RESULTS.md` - Outdated performance data
- `README_CLEAN.md` - Duplicate/outdated README
- `DERIBIT_OPTIONS_SOLUTION.md` - Incomplete solution, superseded by COMPLETE version
- `ORGANIZED_STRUCTURE.md` - References incorrect deployment structure

## Where to Find Current Information

For current, accurate documentation, see:

- `ARCHITECTURE_OVERVIEW.md` - Current architecture and file structure
- `MAIN_USAGE.md` - Current usage patterns and examples
- `QUICK_REFERENCE.md` - Current quick commands and structure
- `DEPLOYMENT_GUIDE.md` - Current deployment instructions
- `SETUP_GUIDE.md` - Current setup instructions
- `INSTRUMENT_KEY.md` - Canonical instrument key specification (unchanged)
- `DERIBIT_OPTIONS_COMPLETE_SOLUTION.md` - Current Deribit options solution

## Archive Date
December 2024 - Documentation cleanup to align with current codebase implementation.
