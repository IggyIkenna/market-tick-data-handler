"""
Package Usage Examples

Examples showing how to use the market data handler as a package/library
for different downstream use cases.
"""

import asyncio
import pandas as pd
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any

# Import the package components
from market_data_tick_handler.data_client import DataClient, TickDataReader, CandleDataReader, HFTFeaturesReader, MFTFeaturesReader
from market_data_tick_handler.candle_processor import HistoricalCandleProcessor, AggregatedCandleProcessor, BookSnapshotProcessor
from market_data_tick_handler.bigquery_uploader import CandleUploader, UploadOrchestrator
from config import get_config

async def example_features_service():
    """
    Example: Features Service
    Gets 1m candles for feature calculation
    """
    print("🔧 Features Service Example")
    print("=" * 50)
    
    # Initialize data client
    config = get_config()
    data_client = DataClient(config.gcp.bucket, config)
    candle_reader = CandleDataReader(data_client)
    
    # Get 1m candles for a specific instrument and date range
    instrument_id = "BINANCE:SPOT_PAIR:BTC-USDT"
    start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end_date = datetime(2024, 1, 2, tzinfo=timezone.utc)
    
    # Read 1m candles
    candles = candle_reader.get_candles(instrument_id, "1m", start_date, end_date)
    
    print(f"📊 Retrieved {len(candles)} 1m candles for {instrument_id}")
    print(f"📅 Date range: {candles['timestamp'].min()} to {candles['timestamp'].max()}")
    print(f"📈 Columns: {list(candles.columns)}")
    
    # Calculate some basic features
    if not candles.empty:
        candles['price_change'] = candles['close'] - candles['open']
        candles['price_change_pct'] = (candles['price_change'] / candles['open']) * 100
        candles['volume_ma'] = candles['volume'].rolling(window=20).mean()
        
        print(f"💰 Average price change: {candles['price_change_pct'].mean():.2f}%")
        print(f"📊 Average volume: {candles['volume'].mean():.2f}")
    
    return candles

async def example_execution_deployment():
    """
    Example: Execution Deployment
    Gets 15s candles and HFT features for high-frequency trading
    """
    print("\n⚡ Execution Deployment Example")
    print("=" * 50)
    
    # Initialize data client
    config = get_config()
    data_client = DataClient(config.gcp.bucket, config)
    candle_reader = CandleDataReader(data_client)
    hft_reader = HFTFeaturesReader(data_client)
    
    # Get 15s candles with HFT features
    instrument_id = "BINANCE:SPOT_PAIR:BTC-USDT"
    date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    
    # Read 15s candles
    candles_15s = candle_reader.get_candles(instrument_id, "15s", date, date + timedelta(days=1))
    
    # Read HFT features for 15s
    hft_features_15s = hft_reader.get_hft_features(instrument_id, "15s", date, date + timedelta(days=1))
    
    print(f"📊 Retrieved {len(candles_15s)} 15s candles for {instrument_id}")
    print(f"🔬 Retrieved {len(hft_features_15s)} HFT feature records")
    
    # Merge candles with HFT features
    if not candles_15s.empty and not hft_features_15s.empty:
        merged_data = pd.merge(
            candles_15s, 
            hft_features_15s, 
            on=['timestamp', 'symbol', 'exchange'], 
            how='left'
        )
        
        print(f"🔗 Merged data: {len(merged_data)} records")
        print(f"📈 HFT features available: {[col for col in merged_data.columns if col in hft_reader.hft_columns]}")
        
        # Calculate some execution metrics
        if 'delay_median' in merged_data.columns:
            avg_delay = merged_data['delay_median'].mean()
            print(f"⏱️ Average delay: {avg_delay:.2f}ms")
        
        if 'liquidation_count' in merged_data.columns:
            total_liquidations = merged_data['liquidation_count'].sum()
            print(f"💥 Total liquidations: {total_liquidations}")
    
    return candles_15s, hft_features_15s

async def example_analytics_tick_streaming():
    """
    Example: Analytics Service
    Streams tick data to BigQuery for analytics
    """
    print("\n📊 Analytics Service Example")
    print("=" * 50)
    
    # Initialize data client
    config = get_config()
    data_client = DataClient(config.gcp.bucket, config)
    tick_reader = TickDataReader(data_client)
    
    # Get tick data for analysis
    instrument_id = "BINANCE:SPOT_PAIR:BTC-USDT"
    date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    start_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)  # 12:00 UTC
    end_time = datetime(2024, 1, 1, 12, 5, 0, tzinfo=timezone.utc)    # 12:05 UTC
    
    # Read tick data for a 5-minute window
    tick_data = tick_reader.get_tick_data(instrument_id, start_time, end_time, date)
    
    print(f"📊 Retrieved {len(tick_data)} tick records for {instrument_id}")
    print(f"⏰ Time range: {start_time} to {end_time}")
    
    if not tick_data.empty:
        print(f"📈 Data types: {tick_data['data_type'].unique()}")
        print(f"💰 Price range: {tick_data['price'].min():.2f} to {tick_data['price'].max():.2f}")
        print(f"📊 Volume: {tick_data['amount'].sum():.2f}")
        
        # Calculate some analytics
        trades_data = tick_data[tick_data['data_type'] == 'trades']
        if not trades_data.empty:
            vwap = (trades_data['price'] * trades_data['amount']).sum() / trades_data['amount'].sum()
            print(f"📊 VWAP: {vwap:.2f}")
    
    return tick_data

async def example_backtesting_tick_data():
    """
    Example: Backtesting - Tick Data Only
    Gets tick data for backtesting with timestamp filtering
    """
    print("\n🔄 Backtesting Example - Tick Data")
    print("=" * 50)
    
    # Initialize data client
    config = get_config()
    data_client = DataClient(config.gcp.bucket, config)
    tick_reader = TickDataReader(data_client)
    
    # Get tick data for backtesting
    instrument_id = "BINANCE:SPOT_PAIR:BTC-USDT"
    date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    
    # Define backtesting time windows (e.g., trading hours)
    trading_windows = [
        (datetime(2024, 1, 1, 8, 0, 0, tzinfo=timezone.utc), datetime(2024, 1, 1, 8, 5, 0, tzinfo=timezone.utc)),
        (datetime(2024, 1, 1, 14, 0, 0, tzinfo=timezone.utc), datetime(2024, 1, 1, 14, 5, 0, tzinfo=timezone.utc)),
        (datetime(2024, 1, 1, 20, 0, 0, tzinfo=timezone.utc), datetime(2024, 1, 1, 20, 5, 0, tzinfo=timezone.utc))
    ]
    
    all_tick_data = []
    
    for start_time, end_time in trading_windows:
        print(f"📊 Loading tick data for {start_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')}")
        
        # Use streaming reader for memory efficiency
        for chunk in tick_reader.get_tick_data_streaming(
            instrument_id, start_time, end_time, date, chunk_size=1000
        ):
            all_tick_data.append(chunk)
    
    # Combine all chunks
    if all_tick_data:
        combined_data = pd.concat(all_tick_data, ignore_index=True)
        print(f"📊 Total tick records for backtesting: {len(combined_data)}")
        print(f"⏰ Time range: {combined_data['timestamp'].min()} to {combined_data['timestamp'].max()}")
        
        # Simulate backtesting logic
        print("🔄 Simulating backtesting...")
        # Your backtesting logic would go here
    
    return all_tick_data

async def example_backtesting_hft_features():
    """
    Example: Backtesting - HFT Features Only
    Gets HFT features for execution deployment backtesting
    """
    print("\n🔄 Backtesting Example - HFT Features")
    print("=" * 50)
    
    # Initialize data client
    config = get_config()
    data_client = DataClient(config.gcp.bucket, config)
    hft_reader = HFTFeaturesReader(data_client)
    
    # Get HFT features for backtesting
    instrument_id = "BINANCE:SPOT_PAIR:BTC-USDT"
    start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end_date = datetime(2024, 1, 7, tzinfo=timezone.utc)  # One week
    
    # Read HFT features for 15s and 1m timeframes
    hft_15s = hft_reader.get_hft_features(instrument_id, "15s", start_date, end_date)
    hft_1m = hft_reader.get_hft_features(instrument_id, "1m", start_date, end_date)
    
    print(f"📊 Retrieved {len(hft_15s)} 15s HFT feature records")
    print(f"📊 Retrieved {len(hft_1m)} 1m HFT feature records")
    
    if not hft_15s.empty:
        print(f"🔬 15s HFT features: {[col for col in hft_15s.columns if col in hft_reader.hft_columns]}")
        
        # Calculate some backtesting metrics
        if 'delay_median' in hft_15s.columns:
            avg_delay = hft_15s['delay_median'].mean()
            print(f"⏱️ Average 15s delay: {avg_delay:.2f}ms")
        
        if 'liquidation_count' in hft_15s.columns:
            total_liquidations = hft_15s['liquidation_count'].sum()
            print(f"💥 Total liquidations (15s): {total_liquidations}")
    
    return hft_15s, hft_1m

async def example_historical_candle_processing():
    """
    Example: Historical Candle Processing
    Processes historical tick data into candles
    """
    print("\n🕯️ Historical Candle Processing Example")
    print("=" * 50)
    
    # Initialize components
    config = get_config()
    data_client = DataClient(config.gcp.bucket, config)
    
    from market_data_tick_handler.candle_processor.historical_candle_processor import HistoricalCandleProcessor, ProcessingConfig
    
    # Configure processor
    processing_config = ProcessingConfig(
        timeframes=['15s', '1m'],
        enable_hft_features=True,
        enable_book_snapshots=True,
        data_types=['trades', 'book_snapshot_5', 'derivative_ticker', 'liquidations']
    )
    
    processor = HistoricalCandleProcessor(data_client, processing_config)
    
    # Process a single day
    instrument_id = "BINANCE:SPOT_PAIR:BTC-USDT"
    date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    
    print(f"🕯️ Processing candles for {instrument_id} on {date.strftime('%Y-%m-%d')}")
    
    result = await processor.process_day(instrument_id, date)
    
    print(f"✅ Processing completed:")
    print(f"📊 Timeframes processed: {list(result['timeframes'].keys())}")
    for tf, tf_result in result['timeframes'].items():
        print(f"  {tf}: {tf_result['candle_count']} candles")
    
    if result['errors']:
        print(f"❌ Errors: {len(result['errors'])}")
        for error in result['errors'][:3]:  # Show first 3 errors
            print(f"  - {error}")
    
    return result

async def example_bigquery_upload():
    """
    Example: BigQuery Upload
    Uploads processed candles to BigQuery for analytics
    """
    print("\n📤 BigQuery Upload Example")
    print("=" * 50)
    
    # Initialize components
    config = get_config()
    data_client = DataClient(config.gcp.bucket, config)
    
    from market_data_tick_handler.bigquery_uploader.candle_uploader import CandleUploader, UploadConfig
    from market_data_tick_handler.bigquery_uploader.upload_orchestrator import UploadOrchestrator, OrchestrationConfig
    
    # Configure upload
    upload_config = UploadConfig(
        project_id=config.gcp.project_id,
        dataset_id="market_data_candles",
        timeframes=['15s', '1m', '5m', '15m', '1h', '4h', '24h'],
        batch_size=1000
    )
    
    orchestration_config = OrchestrationConfig(
        max_concurrent_days=3,
        max_concurrent_timeframes=2,
        retry_failed_days=True
    )
    
    uploader = CandleUploader(data_client, upload_config)
    orchestrator = UploadOrchestrator(data_client, upload_config, orchestration_config)
    
    # Upload a single day
    date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    
    print(f"📤 Uploading candles for {date.strftime('%Y-%m-%d')}")
    
    result = await uploader.upload_day(date, overwrite=False)
    
    print(f"✅ Upload completed:")
    print(f"📊 Timeframes uploaded: {list(result['timeframes'].keys())}")
    for tf, tf_result in result['timeframes'].items():
        print(f"  {tf}: {tf_result['rows_uploaded']} rows")
    
    if result['errors']:
        print(f"❌ Errors: {len(result['errors'])}")
        for error in result['errors'][:3]:  # Show first 3 errors
            print(f"  - {error}")
    
    return result

async def main():
    """Run all examples"""
    print("🚀 Market Data Handler Package Examples")
    print("=" * 60)
    
    try:
        # Run examples
        await example_features_service()
        await example_execution_deployment()
        await example_analytics_tick_streaming()
        await example_backtesting_tick_data()
        await example_backtesting_hft_features()
        await example_historical_candle_processing()
        await example_bigquery_upload()
        
        print("\n✅ All examples completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Example failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
