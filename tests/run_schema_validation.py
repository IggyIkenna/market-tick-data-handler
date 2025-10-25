#!/usr/bin/env python3
"""
Schema Validation Test Runner

Standalone script to run schema validation tests and generate reports.
Can be used for CI/CD or manual testing.
"""

import asyncio
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from tests.test_schema_validation import SchemaValidationTests, TEST_INSTRUMENTS, TEST_DATE

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('schema_validation_results.log')
    ]
)
logger = logging.getLogger(__name__)


class SchemaValidationRunner:
    """Runner for schema validation tests with reporting"""
    
    def __init__(self):
        self.validator = SchemaValidationTests()
        self.results = []
        
    async def run_all_tests(self) -> Dict[str, Any]:
        """Run all schema validation tests"""
        logger.info("Starting comprehensive schema validation tests")
        
        await self.validator.setup()
        
        try:
            # Test all combinations
            test_results = {
                'trades': await self._test_data_type('trades'),
                'book_snapshot_5': await self._test_data_type('book_snapshot_5'),
                'derivative_ticker': await self._test_data_type('derivative_ticker'),
                'liquidations': await self._test_data_type('liquidations'),
                'options_chain': await self._test_data_type('options_chain')
            }
            
            # Generate summary
            summary = self._generate_summary(test_results)
            
            return {
                'test_date': TEST_DATE.isoformat(),
                'summary': summary,
                'detailed_results': test_results,
                'timestamp': datetime.now().isoformat()
            }
            
        finally:
            await self.validator.teardown()
    
    async def _test_data_type(self, data_type: str) -> List[Dict[str, Any]]:
        """Test a specific data type across all relevant venues"""
        logger.info(f"Testing {data_type} schema validation")
        results = []
        
        for venue, instruments in TEST_INSTRUMENTS.items():
            if data_type not in instruments:
                continue
                
            instrument = instruments[data_type]
            tardis_exchange = instrument['tardis_exchange']
            tardis_symbol = instrument['tardis_symbol']
            
            logger.info(f"Testing {data_type} for {venue} ({tardis_exchange}:{tardis_symbol})")
            
            try:
                # Download raw Tardis data
                raw_data = await self.validator.download_raw_tardis_data(
                    tardis_exchange, tardis_symbol, TEST_DATE, data_type
                )
                
                # Process through our system
                processed_df = await self.validator.process_tardis_data(
                    tardis_exchange, tardis_symbol, TEST_DATE, data_type
                )
                
                # Extract schemas
                raw_schema = self.validator.extract_tardis_raw_schema(raw_data, data_type)
                processed_schema = self.validator.extract_parquet_schema(processed_df)
                
                # Compare schemas
                comparison = self.validator.compare_schemas(raw_schema, processed_schema, data_type)
                comparison['venue'] = venue
                comparison['tardis_exchange'] = tardis_exchange
                comparison['tardis_symbol'] = tardis_symbol
                
                results.append(comparison)
                
                if comparison['match']:
                    logger.info(f"✅ {data_type} schema validation passed for {venue}")
                else:
                    logger.error(f"❌ {data_type} schema validation failed for {venue}: {comparison['errors']}")
                
            except Exception as e:
                logger.error(f"❌ Failed to validate {data_type} schema for {venue}: {e}")
                results.append({
                    'venue': venue,
                    'tardis_exchange': tardis_exchange,
                    'tardis_symbol': tardis_symbol,
                    'data_type': data_type,
                    'match': False,
                    'errors': [f"Test failed: {str(e)}"],
                    'warnings': []
                })
        
        return results
    
    def _generate_summary(self, test_results: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Generate summary statistics"""
        total_tests = 0
        passed_tests = 0
        failed_tests = 0
        venues_tested = set()
        data_types_tested = set()
        
        all_errors = []
        all_warnings = []
        
        for data_type, results in test_results.items():
            data_types_tested.add(data_type)
            
            for result in results:
                total_tests += 1
                venues_tested.add(result['venue'])
                
                if result['match']:
                    passed_tests += 1
                else:
                    failed_tests += 1
                
                all_errors.extend(result.get('errors', []))
                all_warnings.extend(result.get('warnings', []))
        
        return {
            'total_tests': total_tests,
            'passed_tests': passed_tests,
            'failed_tests': failed_tests,
            'success_rate': (passed_tests / total_tests * 100) if total_tests > 0 else 0,
            'venues_tested': sorted(list(venues_tested)),
            'data_types_tested': sorted(list(data_types_tested)),
            'total_errors': len(all_errors),
            'total_warnings': len(all_warnings),
            'unique_errors': list(set(all_errors)),
            'unique_warnings': list(set(all_warnings))
        }
    
    def save_results(self, results: Dict[str, Any], output_file: str = None):
        """Save test results to JSON file"""
        if output_file is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f"schema_validation_results_{timestamp}.json"
        
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"Results saved to {output_file}")
        return output_file
    
    def print_summary(self, results: Dict[str, Any]):
        """Print a formatted summary of results"""
        summary = results['summary']
        
        print("\n" + "="*60)
        print("SCHEMA VALIDATION TEST RESULTS")
        print("="*60)
        print(f"Test Date: {results['test_date']}")
        print(f"Total Tests: {summary['total_tests']}")
        print(f"Passed: {summary['passed_tests']}")
        print(f"Failed: {summary['failed_tests']}")
        print(f"Success Rate: {summary['success_rate']:.1f}%")
        print(f"Venues Tested: {', '.join(summary['venues_tested'])}")
        print(f"Data Types Tested: {', '.join(summary['data_types_tested'])}")
        
        if summary['total_errors'] > 0:
            print(f"\nErrors ({summary['total_errors']}):")
            for error in summary['unique_errors']:
                print(f"  - {error}")
        
        if summary['total_warnings'] > 0:
            print(f"\nWarnings ({summary['total_warnings']}):")
            for warning in summary['unique_warnings']:
                print(f"  - {warning}")
        
        print("="*60)


async def main():
    """Main entry point"""
    runner = SchemaValidationRunner()
    
    try:
        # Run all tests
        results = await runner.run_all_tests()
        
        # Print summary
        runner.print_summary(results)
        
        # Save results
        output_file = runner.save_results(results)
        
        # Exit with appropriate code
        if results['summary']['failed_tests'] > 0:
            logger.error("Schema validation tests failed")
            sys.exit(1)
        else:
            logger.info("All schema validation tests passed")
            sys.exit(0)
            
    except Exception as e:
        logger.error(f"Schema validation runner failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
