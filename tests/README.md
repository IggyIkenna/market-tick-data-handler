# Test Suite Documentation

This directory contains the comprehensive test suite for the Market Tick Data Handler project.

## Directory Structure

```
tests/
├── __init__.py
├── conftest.py           # Shared fixtures and configuration
├── run_quality_gates.py  # Quality gate runner
├── unit/                 # Unit tests (<5s total)
│   ├── test_models.py
│   ├── test_config.py
│   ├── instrument_processor/
│   │   ├── test_canonical_key_generator.py
│   │   └── test_gcs_uploader.py
│   ├── data_downloader/
│   │   ├── test_tardis_connector.py
│   │   ├── test_download_orchestrator.py
│   │   └── test_instrument_reader.py
│   ├── data_validator/
│   │   └── test_data_validator.py
│   └── utils/
│       ├── test_logger.py
│       ├── test_error_handler.py
│       ├── test_gcs_client.py
│       └── test_performance_monitor.py
├── integration/          # Integration tests (<25s total)
│   ├── test_instrument_pipeline.py
│   ├── test_download_pipeline.py
│   ├── test_validation_pipeline.py
│   ├── test_full_pipeline.py
│   └── test_gcs_integration.py
└── performance/          # Performance tests and benchmarks
    ├── test_connection_reuse.py
    ├── test_parallel_performance.py
    ├── test_gcs_performance.py
    ├── test_e2e_performance.py
    └── quality_gates.json
```

## Running Tests

### Run All Tests
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html
```

### Run Specific Test Categories
```bash
# Unit tests only
pytest tests/unit/ -m unit

# Integration tests only
pytest tests/integration/ -m integration

# Performance tests only
pytest tests/performance/ -m performance
```

### Run Quality Gates
```bash
# Run quality gates (used in deployment)
python tests/run_quality_gates.py

# Run quality gates as part of deployment
./deploy/local/run-main.sh [MODE] --run-quality-gates [OPTIONS]
```

## Test Categories

### Unit Tests
- **Duration**: <5 seconds total
- **Coverage**: 80%+ required
- **Purpose**: Test individual components in isolation
- **Mocking**: Heavy use of mocks for external dependencies

### Integration Tests
- **Duration**: <25 seconds total
- **Coverage**: 70%+ required
- **Purpose**: Test component interactions and workflows
- **Mocking**: Limited mocking, use real GCS test bucket

### Performance Tests
- **Duration**: <30 seconds total
- **Purpose**: Benchmark performance and ensure quality gates
- **Mocking**: Minimal mocking, focus on real performance

## Quality Gates

Quality gates ensure code quality before deployment:

### Unit Test Gates
- Maximum duration: 5 seconds
- Minimum coverage: 80%
- All tests must pass

### Integration Test Gates
- Maximum duration: 25 seconds
- Minimum coverage: 70%
- All tests must pass

### Performance Benchmarks
- Connection reuse: <5 seconds for 10 requests
- Parallel downloads: <8 seconds for 5 parallel downloads
- GCS uploads: <7 seconds for 10 uploads
- E2E pipeline: <25 seconds for small dataset

### Total Execution
- Maximum duration: 30 seconds for all tests combined

## Writing New Tests

### Unit Tests
```python
import pytest
from unittest.mock import Mock, patch
from src.module import ClassToTest

class TestClassToTest:
    def test_method_success(self):
        """Test successful method execution"""
        obj = ClassToTest()
        result = obj.method()
        assert result == expected_value
    
    def test_method_with_mock(self, mock_dependency):
        """Test method with mocked dependency"""
        obj = ClassToTest(mock_dependency)
        result = obj.method()
        assert result == expected_value
```

### Integration Tests
```python
import pytest
from src.module import IntegrationClass

@pytest.mark.integration
class TestIntegrationWorkflow:
    @pytest.mark.asyncio
    async def test_workflow_success(self, mock_gcs_client):
        """Test complete workflow"""
        workflow = IntegrationClass()
        result = await workflow.run()
        assert result['success'] is True
```

### Performance Tests
```python
import pytest
import time

@pytest.mark.performance
class TestPerformanceBenchmark:
    def test_benchmark_performance(self):
        """Benchmark specific operation"""
        start_time = time.time()
        
        # Perform operation
        result = perform_operation()
        
        duration = time.time() - start_time
        assert duration < 5.0, f"Operation took {duration:.2f}s, expected <5s"
```

## Test Fixtures

### Available Fixtures
- `config`: Test configuration
- `temp_dir`: Temporary directory for test files
- `mock_gcs_client`: Mocked GCS client
- `mock_tardis_connector`: Mocked Tardis connector
- `sample_instrument_key`: Sample instrument key
- `sample_instrument_definition`: Sample instrument definition
- `sample_trade_data`: Sample trade data
- `sample_liquidation_data`: Sample liquidation data
- `sample_derivative_ticker_data`: Sample derivative ticker data
- `test_date`: Test date
- `test_date_range`: Test date range
- `mock_http_responses`: Mocked HTTP responses

### Using Fixtures
```python
def test_with_fixture(sample_instrument_definition):
    """Test using a fixture"""
    inst = sample_instrument_definition
    assert inst.instrument_key == "BINANCE:SPOT_PAIR:BTC-USDT"
```

## Test Markers

### Available Markers
- `@pytest.mark.unit`: Unit tests
- `@pytest.mark.integration`: Integration tests
- `@pytest.mark.performance`: Performance tests
- `@pytest.mark.slow`: Slow tests (skip in CI)
- `@pytest.mark.gcs`: Tests requiring GCS access
- `@pytest.mark.tardis`: Tests requiring Tardis API access

### Using Markers
```python
@pytest.mark.unit
def test_unit_function():
    """Unit test"""
    pass

@pytest.mark.integration
@pytest.mark.gcs
def test_gcs_integration():
    """Integration test requiring GCS"""
    pass
```

## Quality Gates Integration

### Deployment Integration
Quality gates are integrated into the deployment process via the `--run-quality-gates` flag:

```bash
# Normal deployment (no quality gates)
./deploy/local/run-main.sh check-gaps --start-date 2023-05-23 --end-date 2023-05-25

# Deployment with quality gates (blocks if tests fail)
./deploy/local/run-main.sh check-gaps --run-quality-gates --start-date 2023-05-23 --end-date 2023-05-25

# All deployment modes support quality gates
./deploy/local/run-main.sh instruments --run-quality-gates --start-date 2023-05-23 --end-date 2023-05-25
./deploy/local/run-main.sh download --run-quality-gates --start-date 2023-05-23 --end-date 2023-05-25
./deploy/local/run-main.sh full-pipeline-ticks --run-quality-gates --start-date 2023-05-23 --end-date 2023-05-25
```

### Quality Gates Behavior
- **Optional**: Quality gates only run when `--run-quality-gates` flag is used
- **Blocking**: Deployment is completely blocked if quality gates fail
- **Comprehensive**: Includes dependency checks, environment validation, GCS connectivity, Tardis API validation, and test execution
- **Clear Feedback**: Detailed output showing what's being checked and why deployment failed

### Pre-Deployment Checks
When quality gates are enabled, the following checks are performed:
1. **Dependencies**: Verify required Python packages are installed
2. **Environment**: Check required environment variables are set
3. **GCS Credentials**: Validate GCS credentials file exists and is accessible
4. **GCS Connection**: Test actual connection to GCS bucket
5. **Tardis API**: Validate Tardis API key format and accessibility
6. **Quality Gates**: Run unit, integration, and performance tests
7. **System Resources**: Check disk space and memory availability

## Running Tests in CI/CD

### GitHub Actions Example
```yaml
- name: Run Quality Gates
  run: python tests/run_quality_gates.py

- name: Deploy with Quality Gates
  run: ./deploy/local/run-main.sh ${{ matrix.mode }} --run-quality-gates ${{ matrix.options }}
```

### Local Development
```bash
# Run tests before committing
python tests/run_quality_gates.py

# Run specific test category
pytest tests/unit/ -v

# Run with coverage
pytest --cov=src --cov-report=term-missing

# Test deployment with quality gates
./deploy/local/run-main.sh check-gaps --run-quality-gates --start-date 2023-05-23 --end-date 2023-05-25
```

## Troubleshooting

### Common Issues

1. **Tests taking too long**
   - Check for blocking operations
   - Use appropriate timeouts
   - Mock external dependencies

2. **Coverage too low**
   - Add tests for missing code paths
   - Check for untested error conditions
   - Verify all branches are covered

3. **Quality gates failing**
   - Check test duration limits
   - Verify coverage thresholds
   - Fix failing tests
   - Check environment variables are set correctly
   - Verify GCS credentials are accessible
   - Ensure Tardis API key is valid

4. **Mock issues**
   - Ensure mocks are properly configured
   - Check mock return values
   - Verify mock call expectations

### Debug Commands
```bash
# Run with verbose output
pytest -v -s

# Run specific test
pytest tests/unit/test_models.py::TestInstrumentType::test_instrument_type_values -v

# Run with debug output
pytest --pdb

# Show test coverage
pytest --cov=src --cov-report=html

# Test deployment script help
./deploy/local/run-main.sh --help

# Test quality gates integration
./deploy/local/run-main.sh check-gaps --run-quality-gates --start-date 2023-05-23 --end-date 2023-05-25
```

## Performance Guidelines

### Writing Performance Tests
1. Use realistic data sizes
2. Test with multiple concurrency levels
3. Measure both success and error scenarios
4. Set appropriate time limits
5. Document expected performance

### Benchmarking
1. Run tests multiple times for consistency
2. Use appropriate test data
3. Consider system load
4. Document performance expectations
5. Update benchmarks when performance improves

## Contributing

When adding new tests:
1. Follow the existing naming conventions
2. Use appropriate test markers
3. Add proper docstrings
4. Include both success and failure cases
5. Update this documentation if needed

## Resources

- [pytest Documentation](https://docs.pytest.org/)
- [pytest-asyncio Documentation](https://pytest-asyncio.readthedocs.io/)
- [pytest-mock Documentation](https://pytest-mock.readthedocs.io/)
- [Coverage.py Documentation](https://coverage.readthedocs.io/)