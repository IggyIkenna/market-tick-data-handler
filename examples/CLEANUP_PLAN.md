# Examples Cleanup Plan

## Keep (Essential for Core Functionality)

### Package Usage
- `package_usage_examples.py` - Core package usage examples
- `package_usage_example.py` - Alternative package usage

### Authentication
- `authentication_examples.py` - Different auth modes
- `secret_manager_example.py` - Secret Manager integration

### Instrument Discovery
- `inspect_instrument.py` - Instrument inspection
- `list_instruments.py` - List instruments
- `tardis_lookup_example.py` - Tardis API integration

### Documentation
- `parquet_performance_results.md` - Comprehensive performance results
- `parquet_performance_comparison.md` - Before/after comparison
- `README.md` - Examples overview

## Remove (One-time Tests/Redundant Documentation)

### Performance Test Scripts (13 files)
- `parquet_performance_test.py` - Complex one-time test
- `simple_parquet_test.py` - One-time test
- `performance_comparison_test.py` - One-time test
- `performance_comparison_test_mock.py` - One-time test
- `simple_performance_test.py` - One-time test
- `standalone_performance_test.py` - One-time test
- `sparse_data_access_example.py` - Documented in results
- `two_phase_optimization_example.py` - Documented in results
- `test_package_integration.py` - One-time test
- `download_and_test_instructions.md` - One-time instructions
- `parquet_test_summary.md` - Redundant with results.md
- `performance_test_summary.md` - Redundant
- `test_package_integration.py` - One-time test

## Benefits of Cleanup
1. **Reduced Confusion**: Only essential examples remain
2. **Faster Navigation**: Easier to find relevant examples
3. **Cleaner Repository**: Less clutter, more professional
4. **Focused Documentation**: Clear separation of results vs. examples

## Final State After Cleanup

### Files Kept: 9 files
- `package_usage_examples.py` - Comprehensive package usage
- `authentication_examples.py` - Authentication modes
- `secret_manager_example.py` - Secret Manager integration
- `inspect_instrument.py` - Instrument inspection
- `list_instruments.py` - List instruments
- `tardis_lookup_example.py` - Tardis API integration
- `parquet_performance_results.md` - Performance analysis
- `README.md` - Examples overview
- `CLEANUP_PLAN.md` - This cleanup documentation

### Files Removed: 15 files
- All one-time performance test scripts (11 files)
- Redundant package usage example (1 file)
- Redundant performance comparison (1 file)
- One-time test instructions (2 files)

## Total Reduction: 62% fewer files (15 removed, 9 kept)
