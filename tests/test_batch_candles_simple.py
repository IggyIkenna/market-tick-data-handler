#!/usr/bin/env python3
"""
Simple Batch Candle Pipeline Test

Focuses purely on batch candle processing without streaming components:
1. Check available tick data for BINANCE instruments
2. Query tick data with optimized Parquet
3. Process candles with HFT features and timestamp_out
4. Upload candles to BigQuery with proper partitioning
5. Query candles from BigQuery with clustering
6. Download sample data for validation
"""

import asyncio
import logging
import pandas as pd
from datetime import datetime, timezone, timedelta
from pathlib import Path
import sys
import os

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SimpleBatchCandleTest:
    """Simple test focused on batch candle processing"""
    
    def __init__(self):
        # Import here to avoid circular imports
        from market_data_tick_handler.config import get_config
        from market_data_tick_handler.data_client.data_client import DataClient
        from google.cloud import bigquery
        
        self.config = get_config()
        self.data_client = DataClient(self.config.gcp.bucket, self.config)
        self.bq_client = bigquery.Client(project=self.config.gcp.project_id)
        
        # Test instruments (BINANCE only - one per data type)
        self.test_instruments = {
            'spot': 'BINANCE:SPOT_PAIR:BTC-USDT',           # trades, book_snapshot_5
            'perpetual': 'BINANCE:PERPETUAL:BTC-USDT',      # trades, book_snapshot_5, derivative_ticker, liquidations  
            'future': 'BINANCE:FUTURE:BTC-240329',          # trades, book_snapshot_5, derivative_ticker
            'option': 'BINANCE:OPTION:BTC-240329-70000-C'  # trades, book_snapshot_5, options_chain
        }
        
        # Test date range (adjust to match your data)
        self.test_date = datetime(2024, 1, 15)
        self.test_start_time = self.test_date.replace(hour=9, minute=0)
        self.test_end_time = self.test_date.replace(hour=9, minute=30)
        
    async def run_test(self):
        """Run the complete batch candle test"""
        
        logger.info("🚀 Starting Batch Candle Pipeline Test")
        logger.info(f"📅 Test date: {self.test_date.strftime('%Y-%m-%d')}")
        logger.info(f"🕐 Time range: {self.test_start_time.strftime('%H:%M')} - {self.test_end_time.strftime('%H:%M')}")
        logger.info(f"🎯 Instruments: {list(self.test_instruments.values())}")
        
        results = {}
        
        try:
            # Step 1: Check available tick data
            logger.info("\n" + "="*50)
            logger.info("STEP 1: Checking Available Tick Data")
            logger.info("="*50)
            results['available_data'] = await self.check_available_data()
            
            # Step 2: Test candle processing via CLI
            logger.info("\n" + "="*50)
            logger.info("STEP 2: Testing Candle Processing via CLI")
            logger.info("="*50)
            results['candle_processing'] = await self.test_candle_processing_cli()
            
            # Step 3: Test BigQuery upload via CLI
            logger.info("\n" + "="*50)
            logger.info("STEP 3: Testing BigQuery Upload via CLI")
            logger.info("="*50)
            results['bigquery_upload'] = await self.test_bigquery_upload_cli()
            
            # Step 4: Query candles from BigQuery
            logger.info("\n" + "="*50)
            logger.info("STEP 4: Querying Candles from BigQuery")
            logger.info("="*50)
            results['bigquery_query'] = await self.query_from_bigquery()
            
            # Step 5: Download sample data
            logger.info("\n" + "="*50)
            logger.info("STEP 5: Downloading Sample Data for Validation")
            logger.info("="*50)
            results['sample_data'] = await self.download_sample_data()
            
            # Summary
            logger.info("\n" + "="*50)
            logger.info("🎉 BATCH CANDLE PIPELINE TEST COMPLETED SUCCESSFULLY")
            logger.info("="*50)
            self.print_summary(results)
            
        except Exception as e:
            logger.error(f"❌ Test failed: {e}")
            raise
    
    async def check_available_data(self):
        """Check what tick data is available for test instruments"""
        
        available_data = {}
        
        for instrument_type, instrument_id in self.test_instruments.items():
            logger.info(f"🔍 Checking {instrument_type}: {instrument_id}")
            
            # Data types to check based on instrument type
            if instrument_type == 'spot':
                data_types = ['trades', 'book_snapshot_5']
            elif instrument_type == 'perpetual':
                data_types = ['trades', 'book_snapshot_5', 'derivative_ticker', 'liquidations']
            elif instrument_type == 'future':
                data_types = ['trades', 'book_snapshot_5', 'derivative_ticker']
            else:  # option
                data_types = ['trades', 'book_snapshot_5', 'options_chain']
            
            instrument_data = {}
            
            for data_type in data_types:
                try:
                    # Check if data exists
                    date_str = self.test_date.strftime('%Y-%m-%d')
                    blob_name = f"raw_tick_data/by_date/day-{date_str}/data_type-{data_type}/{instrument_id}.parquet"
                    
                    blob = self.data_client.bucket.blob(blob_name)
                    exists = blob.exists()
                    
                    if exists:
                        # Get file size
                        blob.reload()
                        size_mb = blob.size / (1024 * 1024) if blob.size else 0
                        instrument_data[data_type] = {
                            'exists': True,
                            'size_mb': round(size_mb, 2),
                            'path': blob_name
                        }
                        logger.info(f"  ✅ {data_type}: {size_mb:.2f} MB")
                    else:
                        instrument_data[data_type] = {'exists': False}
                        logger.info(f"  ❌ {data_type}: Not found")
                        
                except Exception as e:
                    instrument_data[data_type] = {'exists': False, 'error': str(e)}
                    logger.error(f"  ❌ {data_type}: Error - {e}")
            
            available_data[instrument_type] = instrument_data
        
        return available_data
    
    async def test_candle_processing_cli(self):
        """Test candle processing using the CLI"""
        
        import subprocess
        
        logger.info("🕯️ Testing candle processing via CLI...")
        
        # Build command
        cmd = [
            'python', '-m', 'market_data_tick_handler.main',
            '--mode', 'candle-processing',
            '--start-date', self.test_date.strftime('%Y-%m-%d'),
            '--end-date', self.test_date.strftime('%Y-%m-%d'),
            '--venues', 'binance'
        ]
        
        try:
            logger.info(f"📡 Running: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            if result.returncode == 0:
                logger.info("✅ Candle processing completed successfully")
                logger.info("📤 Output:")
                for line in result.stdout.split('\n')[-10:]:  # Last 10 lines
                    if line.strip():
                        logger.info(f"  {line}")
                
                return {'status': 'success', 'stdout': result.stdout}
            else:
                logger.error("❌ Candle processing failed")
                logger.error(f"Error: {result.stderr}")
                return {'status': 'failed', 'error': result.stderr}
                
        except subprocess.TimeoutExpired:
            logger.error("❌ Candle processing timed out")
            return {'status': 'timeout'}
        except Exception as e:
            logger.error(f"❌ Error running candle processing: {e}")
            return {'status': 'error', 'error': str(e)}
    
    async def test_bigquery_upload_cli(self):
        """Test BigQuery upload using the CLI"""
        
        import subprocess
        
        logger.info("📤 Testing BigQuery upload via CLI...")
        
        # Build command
        cmd = [
            'python', '-m', 'market_data_tick_handler.main',
            '--mode', 'bigquery-upload',
            '--start-date', self.test_date.strftime('%Y-%m-%d'),
            '--end-date', self.test_date.strftime('%Y-%m-%d')
        ]
        
        try:
            logger.info(f"📡 Running: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            if result.returncode == 0:
                logger.info("✅ BigQuery upload completed successfully")
                logger.info("📤 Output:")
                for line in result.stdout.split('\n')[-10:]:  # Last 10 lines
                    if line.strip():
                        logger.info(f"  {line}")
                
                return {'status': 'success', 'stdout': result.stdout}
            else:
                logger.error("❌ BigQuery upload failed")
                logger.error(f"Error: {result.stderr}")
                return {'status': 'failed', 'error': result.stderr}
                
        except subprocess.TimeoutExpired:
            logger.error("❌ BigQuery upload timed out")
            return {'status': 'timeout'}
        except Exception as e:
            logger.error(f"❌ Error running BigQuery upload: {e}")
            return {'status': 'error', 'error': str(e)}
    
    async def query_from_bigquery(self):
        """Query candles from BigQuery to verify upload"""
        
        query_results = {}
        
        for timeframe in ['1m', '5m']:
            table_id = f"{self.config.gcp.project_id}.market_data_candles.candles_{timeframe}"
            
            query = f"""
                SELECT 
                    timestamp,
                    timestamp_out,
                    symbol,
                    exchange,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    trade_count,
                    buy_volume_sum,
                    sell_volume_sum,
                    price_vwap
                FROM `{table_id}`
                WHERE DATE(timestamp) = '{self.test_date.strftime('%Y-%m-%d')}'
                  AND exchange = 'binance'
                  AND symbol IN ('BTC-USDT', 'BTC-USDT-PERP')
                ORDER BY timestamp DESC
                LIMIT 10
            """
            
            try:
                query_job = self.bq_client.query(query)
                results = query_job.result()
                
                rows = list(results)
                query_results[timeframe] = {
                    'row_count': len(rows),
                    'sample_data': [dict(row) for row in rows[:3]]
                }
                
                logger.info(f"✅ BigQuery {timeframe}: {len(rows)} rows retrieved")
                
            except Exception as e:
                query_results[timeframe] = {'error': str(e)}
                logger.error(f"❌ BigQuery query failed for {timeframe}: {e}")
        
        return query_results
    
    async def download_sample_data(self):
        """Download sample BigQuery data for manual validation"""
        
        output_dir = Path("sample_binance_data")
        output_dir.mkdir(exist_ok=True)
        
        sample_data = {}
        
        # Download sample data for each timeframe and instrument
        for timeframe in ['1m', '5m']:
            table_id = f"{self.config.gcp.project_id}.market_data_candles.candles_{timeframe}"
            
            # Query for BINANCE BTC-USDT data
            query = f"""
                SELECT *
                FROM `{table_id}`
                WHERE DATE(timestamp) = '{self.test_date.strftime('%Y-%m-%d')}'
                  AND exchange = 'binance'
                  AND symbol = 'BTC-USDT'
                  AND timestamp BETWEEN '{self.test_start_time.isoformat()}'
                  AND '{self.test_end_time.isoformat()}'
                ORDER BY timestamp
            """
            
            try:
                # Execute query and save to CSV
                query_job = self.bq_client.query(query)
                df = query_job.to_dataframe()
                
                if not df.empty:
                    output_file = output_dir / f"binance_btc_usdt_{timeframe}_{self.test_date.strftime('%Y%m%d')}.csv"
                    df.to_csv(output_file, index=False)
                    
                    sample_data[timeframe] = {
                        'rows': len(df),
                        'file': str(output_file),
                        'columns': list(df.columns),
                        'has_timestamp_out': 'timestamp_out' in df.columns,
                        'has_hft_features': any(col.startswith(('buy_volume', 'sell_volume', 'price_vwap')) for col in df.columns),
                        'price_range': {
                            'min': float(df['low'].min()),
                            'max': float(df['high'].max())
                        } if 'low' in df.columns and 'high' in df.columns else None,
                        'volume_total': float(df['volume'].sum()) if 'volume' in df.columns else None
                    }
                    
                    logger.info(f"✅ Downloaded {timeframe}: {len(df)} rows → {output_file}")
                    logger.info(f"  📊 Columns: {len(df.columns)} (timestamp_out: {'✅' if 'timestamp_out' in df.columns else '❌'})")
                    logger.info(f"  🎯 HFT features: {'✅' if any(col.startswith(('buy_volume', 'sell_volume')) for col in df.columns) else '❌'}")
                else:
                    sample_data[timeframe] = {'rows': 0}
                    logger.info(f"⚠️ No data found for {timeframe}")
                    
            except Exception as e:
                sample_data[timeframe] = {'error': str(e)}
                logger.error(f"❌ Download failed for {timeframe}: {e}")
        
        return sample_data
    
    async def check_available_data(self):
        """Check what tick data is available"""
        
        available_data = {}
        
        for instrument_type, instrument_id in self.test_instruments.items():
            logger.info(f"🔍 Checking {instrument_type}: {instrument_id}")
            
            # Data types to check
            if instrument_type == 'spot':
                data_types = ['trades', 'book_snapshot_5']
            elif instrument_type == 'perpetual':
                data_types = ['trades', 'book_snapshot_5', 'derivative_ticker', 'liquidations']
            elif instrument_type == 'future':
                data_types = ['trades', 'book_snapshot_5', 'derivative_ticker']
            else:  # option
                data_types = ['trades', 'book_snapshot_5', 'options_chain']
            
            instrument_data = {}
            
            for data_type in data_types:
                try:
                    date_str = self.test_date.strftime('%Y-%m-%d')
                    blob_name = f"raw_tick_data/by_date/day-{date_str}/data_type-{data_type}/{instrument_id}.parquet"
                    
                    blob = self.data_client.bucket.blob(blob_name)
                    exists = blob.exists()
                    
                    if exists:
                        blob.reload()
                        size_mb = blob.size / (1024 * 1024) if blob.size else 0
                        instrument_data[data_type] = {
                            'exists': True,
                            'size_mb': round(size_mb, 2),
                            'path': blob_name
                        }
                        logger.info(f"  ✅ {data_type}: {size_mb:.2f} MB")
                    else:
                        instrument_data[data_type] = {'exists': False}
                        logger.info(f"  ❌ {data_type}: Not found")
                        
                except Exception as e:
                    instrument_data[data_type] = {'exists': False, 'error': str(e)}
                    logger.error(f"  ❌ {data_type}: Error - {e}")
            
            available_data[instrument_type] = instrument_data
        
        return available_data
    
    def print_summary(self, results):
        """Print test summary"""
        
        logger.info("📊 TEST SUMMARY:")
        
        # Available data summary
        if 'available_data' in results:
            logger.info("\n🔍 Available Data:")
            for instrument_type, data in results['available_data'].items():
                total_size = sum(d.get('size_mb', 0) for d in data.values() if isinstance(d, dict))
                available_types = [dt for dt, d in data.items() if isinstance(d, dict) and d.get('exists')]
                logger.info(f"  {instrument_type}: {len(available_types)} data types, {total_size:.2f} MB total")
        
        # Sample data summary
        if 'sample_data' in results:
            logger.info("\n💾 Sample Data Files:")
            for timeframe, data in results['sample_data'].items():
                if 'file' in data:
                    logger.info(f"  {timeframe}: {data['rows']} rows → {data['file']}")
                    logger.info(f"    timestamp_out: {'✅' if data.get('has_timestamp_out') else '❌'}")
                    logger.info(f"    HFT features: {'✅' if data.get('has_hft_features') else '❌'}")
        
        logger.info("\n🎯 Next Steps:")
        logger.info("1. Review sample CSV files in sample_binance_data/")
        logger.info("2. Compare with live Binance data for validation")
        logger.info("3. Verify HFT features are calculated correctly")
        logger.info("4. Check timestamp_out = local_timestamp + 200ms")

async def main():
    """Run the simple batch candle test"""
    
    test = SimpleBatchCandleTest()
    await test.run_test()

if __name__ == "__main__":
    asyncio.run(main())
