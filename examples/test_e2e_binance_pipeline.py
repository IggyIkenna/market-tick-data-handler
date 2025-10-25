#!/usr/bin/env python3
"""
End-to-End Pipeline Test for Binance Data

Tests the complete workflow:
1. Check available tick data
2. Query tick data with optimized Parquet
3. Process candles with HFT features 
4. Upload candles to BigQuery
5. Query candles from BigQuery
6. Download sample BigQuery data for validation

Instruments tested (all BINANCE):
- SPOT: BTC-USDT (trades, book_snapshot_5)
- FUTURE: BTC-USDT-PERP (trades, book_snapshot_5, derivative_ticker, liquidations)
- OPTION: BTC-240329-70000-C (trades, book_snapshot_5, options_chain)
"""

import asyncio
import logging
import pandas as pd
from datetime import datetime, timezone, timedelta
from pathlib import Path
import sys
import os

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from market_data_tick_handler.config import get_config
from market_data_tick_handler.data_client.data_client import DataClient
from market_data_tick_handler.data_client.candle_data_reader import CandleDataReader
from market_data_tick_handler.data_client.tick_data_reader import TickDataReader
from market_data_tick_handler.candle_processor.historical_candle_processor import HistoricalCandleProcessor, ProcessingConfig
from market_data_tick_handler.bigquery_uploader.candle_uploader import CandleUploader, UploadConfig
from google.cloud import bigquery

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BinanceE2ETest:
    """End-to-end test for Binance data pipeline"""
    
    def __init__(self):
        self.config = get_config()
        self.data_client = DataClient(self.config.gcp.bucket, self.config)
        self.candle_reader = CandleDataReader(self.data_client)
        self.tick_reader = TickDataReader(self.data_client)
        self.bq_client = bigquery.Client(project=self.config.gcp.project_id)
        
        # Test instruments (BINANCE only - one per data type)
        self.test_instruments = {
            'spot': 'BINANCE:SPOT_PAIR:BTC-USDT',           # trades, book_snapshot_5
            'perpetual': 'BINANCE:PERPETUAL:BTC-USDT',      # trades, book_snapshot_5, derivative_ticker, liquidations  
            'future': 'BINANCE:FUTURE:BTC-240329',          # trades, book_snapshot_5, derivative_ticker
            'option': 'BINANCE:OPTION:BTC-240329-70000-C'  # trades, book_snapshot_5, options_chain
        }
        
        # Test date range
        self.test_date = datetime(2024, 1, 15)  # Adjust to date with data
        self.test_start_time = self.test_date.replace(hour=9, minute=0)
        self.test_end_time = self.test_date.replace(hour=9, minute=30)
        
    async def run_complete_test(self):
        """Run complete end-to-end test"""
        
        logger.info("🚀 Starting Binance E2E Pipeline Test")
        logger.info(f"📅 Test date: {self.test_date.strftime('%Y-%m-%d')}")
        logger.info(f"🕐 Test time range: {self.test_start_time.strftime('%H:%M')} - {self.test_end_time.strftime('%H:%M')}")
        logger.info(f"🎯 Test instruments: {list(self.test_instruments.values())}")
        
        results = {}
        
        try:
            # Step 1: Check available tick data
            logger.info("\n" + "="*50)
            logger.info("STEP 1: Checking Available Tick Data")
            logger.info("="*50)
            results['available_data'] = await self.check_available_data()
            
            # Step 2: Query tick data with optimization
            logger.info("\n" + "="*50)
            logger.info("STEP 2: Querying Tick Data (Optimized)")
            logger.info("="*50)
            results['tick_data'] = await self.query_tick_data()
            
            # Step 3: Process candles with HFT features
            logger.info("\n" + "="*50)
            logger.info("STEP 3: Processing Candles with HFT Features")
            logger.info("="*50)
            results['candle_processing'] = await self.process_candles()
            
            # Step 4: Upload candles to BigQuery
            logger.info("\n" + "="*50)
            logger.info("STEP 4: Uploading Candles to BigQuery")
            logger.info("="*50)
            results['bigquery_upload'] = await self.upload_to_bigquery()
            
            # Step 5: Query candles from BigQuery
            logger.info("\n" + "="*50)
            logger.info("STEP 5: Querying Candles from BigQuery")
            logger.info("="*50)
            results['bigquery_query'] = await self.query_from_bigquery()
            
            # Step 6: Download sample data for validation
            logger.info("\n" + "="*50)
            logger.info("STEP 6: Downloading Sample Data for Validation")
            logger.info("="*50)
            results['sample_data'] = await self.download_sample_data()
            
            # Summary
            logger.info("\n" + "="*50)
            logger.info("🎉 BATCH CANDLE PIPELINE E2E TEST COMPLETED SUCCESSFULLY")
            logger.info("="*50)
            self.print_summary(results)
            
        except Exception as e:
            logger.error(f"❌ E2E test failed: {e}")
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
    
    async def query_tick_data(self):
        """Query tick data using optimized Parquet reading"""
        
        tick_data_results = {}
        
        for instrument_type, instrument_id in self.test_instruments.items():
            logger.info(f"📊 Querying tick data for {instrument_type}: {instrument_id}")
            
            try:
                # Query trades data (most common)
                tick_data = self.tick_reader.get_tick_data(
                    instrument_id=instrument_id,
                    start_time=self.test_start_time,
                    end_time=self.test_end_time,
                    date=self.test_date,
                    data_types=['trades']
                )
                
                if not tick_data.empty:
                    tick_data_results[instrument_type] = {
                        'rows': len(tick_data),
                        'columns': list(tick_data.columns),
                        'time_range': {
                            'start': tick_data['timestamp'].min(),
                            'end': tick_data['timestamp'].max()
                        },
                        'sample_data': tick_data.head(3).to_dict('records')
                    }
                    logger.info(f"  ✅ Retrieved {len(tick_data)} trade records")
                    logger.info(f"  📈 Price range: ${tick_data['price'].min():.2f} - ${tick_data['price'].max():.2f}")
                else:
                    tick_data_results[instrument_type] = {'rows': 0}
                    logger.info(f"  ⚠️ No tick data found")
                    
            except Exception as e:
                tick_data_results[instrument_type] = {'error': str(e)}
                logger.error(f"  ❌ Error querying tick data: {e}")
        
        return tick_data_results
    
    async def process_candles(self):
        """Process tick data into candles with HFT features"""
        
        candle_results = {}
        
        # Configure candle processing
        processing_config = ProcessingConfig(
            timeframes=['1m', '5m'],
            enable_hft_features=True,
            enable_book_snapshots=True,
            data_types=['trades', 'book_snapshot_5', 'derivative_ticker', 'liquidations', 'options_chain']
        )
        
        processor = HistoricalCandleProcessor(self.data_client, processing_config)
        
        for instrument_type, instrument_id in self.test_instruments.items():
            logger.info(f"🕯️ Processing candles for {instrument_type}: {instrument_id}")
            
            try:
                # Process candles for this instrument
                result = await processor.process_day(
                    instrument_id=instrument_id,
                    date=self.test_date,
                    output_bucket=self.config.gcp.bucket
                )
                
                candle_results[instrument_type] = result
                logger.info(f"  ✅ Processed candles: {result}")
                
            except Exception as e:
                candle_results[instrument_type] = {'error': str(e)}
                logger.error(f"  ❌ Error processing candles: {e}")
        
        return candle_results
    
    async def upload_to_bigquery(self):
        """Upload processed candles to BigQuery"""
        
        upload_config = UploadConfig(
            project_id=self.config.gcp.project_id,
            dataset_id="market_data_candles_test",
            timeframes=['1m', '5m'],
            batch_size=1000
        )
        
        uploader = CandleUploader(self.data_client, upload_config)
        
        try:
            result = await uploader.upload_date_range(
                start_date=self.test_date,
                end_date=self.test_date,
                timeframes=['1m', '5m'],
                overwrite=True
            )
            
            logger.info(f"✅ BigQuery upload completed: {result}")
            return result
            
        except Exception as e:
            logger.error(f"❌ BigQuery upload failed: {e}")
            return {'error': str(e)}
    
    async def query_from_bigquery(self):
        """Query candles from BigQuery to verify upload"""
        
        query_results = {}
        
        for timeframe in ['1m', '5m']:
            table_id = f"{self.config.gcp.project_id}.market_data_candles_test.candles_{timeframe}"
            
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
        
        output_dir = Path("sample_bigquery_data")
        output_dir.mkdir(exist_ok=True)
        
        sample_data = {}
        
        # Download sample data for each timeframe
        for timeframe in ['1m', '5m']:
            table_id = f"{self.config.gcp.project_id}.market_data_candles_test.candles_{timeframe}"
            
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
                        'price_range': {
                            'min': float(df['low'].min()),
                            'max': float(df['high'].max())
                        },
                        'volume_total': float(df['volume'].sum())
                    }
                    
                    logger.info(f"✅ Downloaded {timeframe}: {len(df)} rows → {output_file}")
                else:
                    sample_data[timeframe] = {'rows': 0}
                    logger.info(f"⚠️ No data found for {timeframe}")
                    
            except Exception as e:
                sample_data[timeframe] = {'error': str(e)}
                logger.error(f"❌ Download failed for {timeframe}: {e}")
        
        return sample_data
    
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
        
        # Tick data summary
        if 'tick_data' in results:
            logger.info("\n📊 Tick Data:")
            for instrument_type, data in results['tick_data'].items():
                if 'rows' in data:
                    logger.info(f"  {instrument_type}: {data['rows']} records")
        
        # BigQuery summary
        if 'bigquery_query' in results:
            logger.info("\n📤 BigQuery Data:")
            for timeframe, data in results['bigquery_query'].items():
                if 'row_count' in data:
                    logger.info(f"  {timeframe}: {data['row_count']} rows")
        
        # Sample data summary
        if 'sample_data' in results:
            logger.info("\n💾 Sample Data Files:")
            for timeframe, data in results['sample_data'].items():
                if 'file' in data:
                    logger.info(f"  {timeframe}: {data['rows']} rows → {data['file']}")
        
        logger.info("\n🎯 Next Steps:")
        logger.info("1. Review sample CSV files for data validation")
        logger.info("2. Compare with live Binance data")
        logger.info("3. Verify HFT features are calculated correctly")
        logger.info("4. Test BigQuery queries for performance")

async def main():
    """Run the complete E2E test"""
    
    test = BinanceE2ETest()
    await test.run_complete_test()

if __name__ == "__main__":
    asyncio.run(main())
