#!/usr/bin/env python3
"""
Comprehensive Test Runner

Runs all test suites and generates comprehensive reports:
1. Schema validation tests
2. Signature validation tests
3. Integration tests
4. Data quality tests
5. Error handling tests
6. Concurrency tests
7. Configuration tests
8. Memory tests
9. Performance tests (parallel, connection reuse, bandwidth analysis)
"""

import asyncio
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('comprehensive_test_results.log')
    ]
)
logger = logging.getLogger(__name__)


class ComprehensiveTestRunner:
    """Runner for all test suites with comprehensive reporting"""
    
    def __init__(self):
        self.test_suites = [
            'test_schema_validation.py',
            'test_signature_validation.py',
            'test_integration.py',
            'test_data_quality.py',
            'test_error_handling.py',
            'test_concurrency.py',
            'test_configuration.py',
            'test_memory.py',
            'performance/test_parallel_performance.py',
            'performance/test_connection_reuse.py',
            'performance/test_gcs_connection_pooling.py',
            'performance/simple_bandwidth_test.py',
            'performance/accurate_bandwidth_test.py',
            'performance/bandwidth_bottleneck_test.py'
        ]
        self.results = {}
        self.start_time = None
        self.end_time = None
    
    def run_all_tests(self) -> Dict[str, Any]:
        """Run all test suites"""
        logger.info("Starting comprehensive test suite execution")
        self.start_time = time.time()
        
        # Run each test suite
        for test_suite in self.test_suites:
            logger.info(f"Running test suite: {test_suite}")
            
            try:
                # Run pytest for the test suite
                result = pytest.main([
                    f'tests/{test_suite}',
                    '-v',
                    '--tb=short',
                    '--durations=10'
                ])
                
                # Store result
                self.results[test_suite] = {
                    'status': 'passed' if result == 0 else 'failed',
                    'exit_code': result,
                    'timestamp': datetime.now().isoformat()
                }
                
                if result == 0:
                    logger.info(f"✅ {test_suite} passed")
                else:
                    logger.error(f"❌ {test_suite} failed with exit code {result}")
                
            except Exception as e:
                logger.error(f"❌ {test_suite} failed with exception: {e}")
                self.results[test_suite] = {
                    'status': 'error',
                    'exit_code': -1,
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                }
        
        self.end_time = time.time()
        
        # Generate summary
        summary = self._generate_summary()
        
        return {
            'test_suites': self.test_suites,
            'results': self.results,
            'summary': summary,
            'execution_time': self.end_time - self.start_time,
            'timestamp': datetime.now().isoformat()
        }
    
    def _generate_summary(self) -> Dict[str, Any]:
        """Generate summary statistics"""
        total_suites = len(self.test_suites)
        passed_suites = sum(1 for result in self.results.values() if result['status'] == 'passed')
        failed_suites = sum(1 for result in self.results.values() if result['status'] == 'failed')
        error_suites = sum(1 for result in self.results.values() if result['status'] == 'error')
        
        return {
            'total_suites': total_suites,
            'passed_suites': passed_suites,
            'failed_suites': failed_suites,
            'error_suites': error_suites,
            'success_rate': (passed_suites / total_suites * 100) if total_suites > 0 else 0,
            'execution_time': self.end_time - self.start_time if self.end_time and self.start_time else 0
        }
    
    def save_results(self, results: Dict[str, Any], output_file: str = None):
        """Save test results to JSON file"""
        if output_file is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f"comprehensive_test_results_{timestamp}.json"
        
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"Results saved to {output_file}")
        return output_file
    
    def print_summary(self, results: Dict[str, Any]):
        """Print a formatted summary of results"""
        summary = results['summary']
        
        print("\n" + "="*80)
        print("COMPREHENSIVE TEST SUITE RESULTS")
        print("="*80)
        print(f"Execution Time: {summary['execution_time']:.2f} seconds")
        print(f"Total Test Suites: {summary['total_suites']}")
        print(f"Passed: {summary['passed_suites']}")
        print(f"Failed: {summary['failed_suites']}")
        print(f"Errors: {summary['error_suites']}")
        print(f"Success Rate: {summary['success_rate']:.1f}%")
        print()
        
        # Print individual test suite results
        print("Test Suite Results:")
        print("-" * 40)
        for test_suite, result in results['results'].items():
            status_icon = "✅" if result['status'] == 'passed' else "❌"
            print(f"{status_icon} {test_suite}: {result['status']}")
            if result['status'] == 'error' and 'error' in result:
                print(f"    Error: {result['error']}")
        
        print("="*80)
        
        # Print recommendations
        if summary['failed_suites'] > 0 or summary['error_suites'] > 0:
            print("\nRecommendations:")
            print("- Review failed test suites for issues")
            print("- Check system configuration and dependencies")
            print("- Verify API keys and network connectivity")
            print("- Consider running individual test suites for detailed debugging")
        else:
            print("\n🎉 All test suites passed successfully!")
    
    def run_specific_suite(self, test_suite: str) -> Dict[str, Any]:
        """Run a specific test suite"""
        if test_suite not in self.test_suites:
            raise ValueError(f"Unknown test suite: {test_suite}")
        
        logger.info(f"Running specific test suite: {test_suite}")
        
        try:
            result = pytest.main([
                f'tests/{test_suite}',
                '-v',
                '--tb=short',
                '--durations=10'
            ])
            
            return {
                'test_suite': test_suite,
                'status': 'passed' if result == 0 else 'failed',
                'exit_code': result,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to run {test_suite}: {e}")
            return {
                'test_suite': test_suite,
                'status': 'error',
                'exit_code': -1,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Run comprehensive test suites')
    parser.add_argument('--suite', help='Run specific test suite')
    parser.add_argument('--output', help='Output file for results')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    
    runner = ComprehensiveTestRunner()
    
    try:
        if args.suite:
            # Run specific test suite
            result = runner.run_specific_suite(args.suite)
            print(f"Test suite {args.suite}: {result['status']}")
            if result['status'] == 'error':
                print(f"Error: {result['error']}")
            sys.exit(0 if result['status'] == 'passed' else 1)
        else:
            # Run all test suites
            results = runner.run_all_tests()
            
            # Print summary
            runner.print_summary(results)
            
            # Save results
            output_file = runner.save_results(results, args.output)
            
            # Exit with appropriate code
            if results['summary']['failed_suites'] > 0 or results['summary']['error_suites'] > 0:
                logger.error("Some test suites failed")
                sys.exit(1)
            else:
                logger.info("All test suites passed")
                sys.exit(0)
                
    except Exception as e:
        logger.error(f"Test runner failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
