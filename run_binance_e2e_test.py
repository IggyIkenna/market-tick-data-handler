#!/usr/bin/env python3
"""
Comprehensive Binance E2E Test Runner

Runs the complete end-to-end pipeline test with real GCS bucket and BigQuery:
1. Available tick data check
2. Optimized tick data queries  
3. Candle processing with HFT features
4. BigQuery upload with proper partitioning
5. BigQuery queries with clustering
6. Sample data download for validation
7. Streaming service integration test

Usage:
    python run_binance_e2e_test.py --bucket your-gcs-bucket --project your-project --date 2024-01-15
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from examples.test_e2e_binance_pipeline import BinanceE2ETest

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def run_comprehensive_test(bucket: str, project: str, test_date: datetime):
    """Run comprehensive E2E test"""
    
    logger.info("🚀 COMPREHENSIVE BINANCE E2E TEST")
    logger.info("="*60)
    logger.info(f"🪣 GCS Bucket: {bucket}")
    logger.info(f"🏗️ GCP Project: {project}")
    logger.info(f"📅 Test Date: {test_date.strftime('%Y-%m-%d')}")
    
    try:
        # Historical Batch Candle Pipeline Test
        logger.info("\n" + "="*60)
        logger.info("BATCH CANDLE PIPELINE TEST")
        logger.info("="*60)
        
        test = BinanceE2ETest()
        test.test_date = test_date
        test.test_start_time = test_date.replace(hour=9, minute=0)
        test.test_end_time = test_date.replace(hour=9, minute=30)
        
        await test.run_complete_test()
        
        logger.info("\n🎉 BATCH CANDLE PIPELINE E2E TEST COMPLETED SUCCESSFULLY!")
        
    except Exception as e:
        logger.error(f"❌ Comprehensive test failed: {e}")
        raise


def main():
    """Main entry point"""
    
    parser = argparse.ArgumentParser(description='Run comprehensive Binance E2E test')
    parser.add_argument('--bucket', required=True, help='GCS bucket name')
    parser.add_argument('--project', required=True, help='GCP project ID')
    parser.add_argument('--date', default='2024-01-15', help='Test date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Parse test date
    test_date = datetime.strptime(args.date, '%Y-%m-%d')
    
    # Set environment variables for the test
    import os
    os.environ['GCP_BUCKET'] = args.bucket
    os.environ['GCP_PROJECT_ID'] = args.project
    
    # Run the test
    asyncio.run(run_comprehensive_test(args.bucket, args.project, test_date))

if __name__ == "__main__":
    main()
