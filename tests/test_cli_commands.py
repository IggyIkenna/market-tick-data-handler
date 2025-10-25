#!/usr/bin/env python3
"""
Simple CLI Command Test

Tests the batch candle pipeline commands without loading complex configurations:
1. Test available-tick-reports mode
2. Test candle-processing mode
3. Test run-full-pipeline-candles mode
4. Test bigquery-upload mode

This focuses on the CLI interface and avoids configuration/import issues.
"""

import subprocess
import logging
from datetime import datetime
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CLICommandTest:
    """Test CLI commands for batch candle processing"""
    
    def __init__(self, test_date: str = '2024-01-15'):
        self.test_date = test_date
        self.base_cmd = ['python', '-m', 'market_data_tick_handler.main']
        
    def run_command(self, mode: str, extra_args: list = None, timeout: int = 60) -> dict:
        """Run a CLI command and capture results"""
        
        cmd = self.base_cmd + [
            '--mode', mode,
            '--start-date', self.test_date,
            '--end-date', self.test_date
        ]
        
        if extra_args:
            cmd.extend(extra_args)
        
        logger.info(f"🚀 Testing command: {' '.join(cmd)}")
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout
            )
            
            if result.returncode == 0:
                logger.info(f"✅ Command succeeded")
                # Show last few lines of output
                lines = result.stdout.strip().split('\n')
                for line in lines[-5:]:
                    if line.strip():
                        logger.info(f"  {line}")
                
                return {
                    'status': 'success',
                    'returncode': result.returncode,
                    'stdout': result.stdout,
                    'stderr': result.stderr
                }
            else:
                logger.error(f"❌ Command failed with return code {result.returncode}")
                logger.error(f"Error output: {result.stderr}")
                
                return {
                    'status': 'failed',
                    'returncode': result.returncode,
                    'stdout': result.stdout,
                    'stderr': result.stderr
                }
                
        except subprocess.TimeoutExpired:
            logger.error(f"❌ Command timed out after {timeout}s")
            return {'status': 'timeout', 'timeout': timeout}
        except Exception as e:
            logger.error(f"❌ Command error: {e}")
            return {'status': 'error', 'error': str(e)}
    
    def test_all_commands(self):
        """Test all batch candle pipeline commands"""
        
        logger.info("🧪 TESTING BATCH CANDLE PIPELINE CLI COMMANDS")
        logger.info("="*60)
        logger.info(f"📅 Test date: {self.test_date}")
        
        results = {}
        
        # Test 1: Available tick reports
        logger.info("\n📊 Test 1: Available Tick Reports")
        logger.info("-" * 40)
        results['available-tick-reports'] = self.run_command(
            'available-tick-reports',
            ['--venues', 'binance']
        )
        
        # Test 2: Candle processing
        logger.info("\n🕯️ Test 2: Candle Processing")
        logger.info("-" * 40)
        results['candle-processing'] = self.run_command(
            'candle-processing',
            ['--venues', 'binance']
        )
        
        # Test 3: Full pipeline candles (without BigQuery)
        logger.info("\n🚀 Test 3: Full Pipeline Candles (without BigQuery)")
        logger.info("-" * 40)
        results['run-full-pipeline-candles'] = self.run_command(
            'run-full-pipeline-candles',
            ['--venues', 'binance']
        )
        
        # Test 4: Full pipeline candles (with BigQuery)
        logger.info("\n🚀 Test 4: Full Pipeline Candles (with BigQuery)")
        logger.info("-" * 40)
        results['run-full-pipeline-candles-bq'] = self.run_command(
            'run-full-pipeline-candles',
            ['--venues', 'binance', '--upload-to-bigquery']
        )
        
        # Test 5: BigQuery upload (standalone)
        logger.info("\n📤 Test 5: BigQuery Upload (standalone)")
        logger.info("-" * 40)
        results['bigquery-upload'] = self.run_command(
            'bigquery-upload'
        )
        
        # Print summary
        self.print_test_summary(results)
        
        return results
    
    def print_test_summary(self, results: dict):
        """Print test summary"""
        
        logger.info("\n" + "="*60)
        logger.info("🎯 TEST SUMMARY")
        logger.info("="*60)
        
        success_count = 0
        total_count = len(results)
        
        for test_name, result in results.items():
            status = result.get('status', 'unknown')
            if status == 'success':
                logger.info(f"✅ {test_name}: SUCCESS")
                success_count += 1
            elif status == 'failed':
                logger.info(f"❌ {test_name}: FAILED (code {result.get('returncode')})")
            elif status == 'timeout':
                logger.info(f"⏰ {test_name}: TIMEOUT ({result.get('timeout')}s)")
            else:
                logger.info(f"❓ {test_name}: {status}")
        
        logger.info(f"\n📊 Results: {success_count}/{total_count} tests passed")
        
        if success_count == total_count:
            logger.info("🎉 ALL TESTS PASSED! Batch candle pipeline is working correctly.")
        else:
            logger.warning("⚠️ Some tests failed. Check the error messages above.")
        
        logger.info("\n📋 Next Steps:")
        logger.info("1. If tests passed: Check GCS bucket for processed candles")
        logger.info("2. If tests passed: Check BigQuery for uploaded data")
        logger.info("3. Review any error messages for failed tests")
        logger.info("4. Verify timestamp_out and HFT features in output data")
        
        # Show expected file paths
        logger.info(f"\n📁 Expected GCS paths (if successful):")
        logger.info(f"  Processed candles: gs://bucket/processed_candles/by_date/day-{self.test_date}/")
        logger.info(f"  Available reports: gs://bucket/available_tick_data/by_date/day-{self.test_date}/")
        
        logger.info(f"\n🗃️ Expected BigQuery tables (if successful):")
        logger.info(f"  project.market_data_candles.candles_15s")
        logger.info(f"  project.market_data_candles.candles_1m")
        logger.info(f"  project.market_data_candles.candles_5m")

def main():
    """Run CLI command tests"""
    
    # You can adjust the test date here
    test_date = '2024-01-15'  # Change to a date where you have BINANCE data
    
    tester = CLICommandTest(test_date)
    results = tester.test_all_commands()
    
    return results

if __name__ == "__main__":
    main()
