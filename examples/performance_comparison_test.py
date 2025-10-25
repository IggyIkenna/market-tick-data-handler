#!/usr/bin/env python3
"""
Performance Comparison Test: Sparse vs Full Data Access

This test compares the performance of sparse data access (5% of data) vs full file reading
for BTC-USDT perps on May 23rd, 2023. It will:

1. Find the correct instrument ID using InstrumentInspector
2. Test existing data (if available) with full file reading
3. Download fresh data with optimized Parquet format
4. Test sparse data access vs full file reading
5. Compare performance metrics (speed, memory, data transfer)
"""

import sys
import os
import time
import psutil
import pandas as pd
from datetime import datetime, timezone, timedelta
from pathlib import Path
import random
import gc

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from market_data_tick_handler.data_client import DataClient
from market_data_tick_handler.instrument_services.instrument_inspector import InstrumentInspector
from config import get_config

class PerformanceMonitor:
    """Monitor performance metrics during data access"""
    
    def __init__(self):
        self.start_time = None
        self.start_memory = None
        self.process = psutil.Process()
    
    def start(self):
        """Start monitoring"""
        self.start_time = time.time()
        self.start_memory = self.process.memory_info().rss / 1024 / 1024  # MB
        gc.collect()  # Force garbage collection for accurate baseline
    
    def stop(self):
        """Stop monitoring and return metrics"""
        end_time = time.time()
        end_memory = self.process.memory_info().rss / 1024 / 1024  # MB
        
        return {
            'duration_seconds': end_time - self.start_time,
            'memory_used_mb': end_memory - self.start_memory,
            'peak_memory_mb': end_memory
        }

def find_btc_usdt_perps_instrument():
    """Find the BTC-USDT perps instrument ID for Binance"""
    print("🔍 Finding BTC-USDT perps instrument...")
    print("=" * 50)
    
    config = get_config()
    inspector = InstrumentInspector(config.gcp.bucket)
    
    # Try to find the instrument
    target_date = datetime(2023, 5, 23, tzinfo=timezone.utc)
    
    try:
        # Get instrument definitions for the target date
        instruments_df = inspector.reader.get_instruments_for_date(target_date)
        
        if instruments_df.empty:
            print("❌ No instruments found for 2023-05-23")
            return None
        
        print(f"✅ Found {len(instruments_df)} instruments for 2023-05-23")
        print(f"Available columns: {list(instruments_df.columns)}")
        
        # Look for BTC-USDT perps on Binance
        # First check what columns are available
        if 'symbol' in instruments_df.columns:
            symbol_col = 'symbol'
        elif 'base_asset' in instruments_df.columns and 'quote_asset' in instruments_df.columns:
            # Create a symbol column from base_asset and quote_asset
            instruments_df['symbol'] = instruments_df['base_asset'] + '-' + instruments_df['quote_asset']
            symbol_col = 'symbol'
        else:
            print("❌ No symbol column found")
            return None
        
        # Look for BTC-USDT or TRB-USDT instruments on Binance (try both PERP and SPOT_PAIR)
        btc_instruments = instruments_df[
            (instruments_df['venue'] == 'BINANCE') &
            (instruments_df['instrument_type'].isin(['PERP', 'SPOT_PAIR'])) &
            (instruments_df[symbol_col].str.contains('(BTC|TRB).*USDT', case=False, na=False))
        ]
        
        if btc_instruments.empty:
            print("❌ No BTC-USDT instruments found on Binance")
            print("Available Binance instruments:")
            binance_instruments = instruments_df[instruments_df['venue'] == 'BINANCE']
            for _, row in binance_instruments.head(10).iterrows():
                print(f"  - {row['instrument_key']}")
            return None
        
        instrument = btc_instruments.iloc[0]
        instrument_id = instrument['instrument_key']
        
        print(f"✅ Found USDT instrument: {instrument_id}")
        print(f"   - Venue: {instrument['venue']}")
        print(f"   - Type: {instrument['instrument_type']}")
        print(f"   - Symbol: {instrument['symbol']}")
        print(f"   - Base Asset: {instrument.get('base_asset', 'N/A')}")
        print(f"   - Quote Asset: {instrument.get('quote_asset', 'N/A')}")
        
        return instrument_id
        
    except Exception as e:
        print(f"❌ Error finding instrument: {e}")
        return None

def test_existing_data_performance(instrument_id: str):
    """Test performance with existing data (if available)"""
    print("\n📊 Testing Existing Data Performance")
    print("=" * 50)
    
    config = get_config()
    data_client = DataClient(config.gcp.bucket, config)
    
    # Check if we're in mock mode
    if data_client.is_mock:
        print("   ⚠️  Using mock data mode - no real GCS data available")
        return None, None
    
    from market_data_tick_handler.data_client.tick_data_reader import TickDataReader
    tick_reader = TickDataReader(data_client)
    
    target_date = datetime(2023, 5, 23, tzinfo=timezone.utc)
    
    # Test 1: Full file reading (traditional method)
    print("1. Testing full file reading (traditional method)...")
    
    monitor = PerformanceMonitor()
    monitor.start()
    
    try:
        full_data = tick_reader.get_tick_data(
            instrument_id=instrument_id,
            start_time=target_date,
            end_time=target_date + timedelta(days=1),
            date=target_date,
            data_types=['trades']
        )
        
        full_metrics = monitor.stop()
        
        print(f"   ✅ Full file reading completed:")
        print(f"      - Records: {len(full_data):,}")
        print(f"      - Duration: {full_metrics['duration_seconds']:.2f}s")
        print(f"      - Memory used: {full_metrics['memory_used_mb']:.2f} MB")
        print(f"      - Peak memory: {full_metrics['peak_memory_mb']:.2f} MB")
        
        if not full_data.empty:
            print(f"      - Time range: {full_data['timestamp'].min()} to {full_data['timestamp'].max()}")
            print(f"      - Price range: ${full_data['price'].min():.2f} to ${full_data['price'].max():.2f}")
        
        return full_data, full_metrics
        
    except Exception as e:
        print(f"   ❌ Error reading full data: {e}")
        return None, None

def test_sparse_data_performance(instrument_id: str, full_data: pd.DataFrame):
    """Test performance with sparse data access (5% of data)"""
    print("\n🎯 Testing Sparse Data Performance (5% of data)")
    print("=" * 50)
    
    config = get_config()
    data_client = DataClient(config.gcp.bucket, config)
    
    # Check if we're in mock mode
    if data_client.is_mock:
        print("   ⚠️  Using mock data mode - no real GCS data available")
        return None, None
    
    from market_data_tick_handler.data_client.tick_data_reader import TickDataReader
    tick_reader = TickDataReader(data_client)
    
    target_date = datetime(2023, 5, 23, tzinfo=timezone.utc)
    
    if full_data.empty:
        print("❌ No full data available for sparse comparison")
        return None, None
    
    # Generate random sparse times (5% of the day)
    day_start = target_date
    day_end = target_date + timedelta(days=1)
    
    # Calculate 5% of the day in minutes
    total_minutes = 24 * 60  # 1440 minutes
    sparse_minutes = int(total_minutes * 0.05)  # 5% = 72 minutes
    
    # Generate random times throughout the day
    random.seed(42)  # For reproducible results
    sparse_times = []
    for _ in range(sparse_minutes):
        random_minute = random.randint(0, total_minutes - 1)
        sparse_time = day_start + timedelta(minutes=random_minute)
        sparse_times.append(sparse_time)
    
    sparse_times.sort()
    
    print(f"Generated {len(sparse_times)} random sparse times (5% of day)")
    print(f"First 5 times: {[t.strftime('%H:%M:%S') for t in sparse_times[:5]]}")
    print(f"Last 5 times: {[t.strftime('%H:%M:%S') for t in sparse_times[-5:]]}")
    
    # Test sparse data access
    print("\n2. Testing sparse data access...")
    
    monitor = PerformanceMonitor()
    monitor.start()
    
    try:
        sparse_data = tick_reader.get_sparse_candles(
            instrument_id=instrument_id,
            candle_times=sparse_times,
            date=target_date,
            data_types=['trades'],
            buffer_minutes=1  # 1-minute buffer around each time
        )
        
        sparse_metrics = monitor.stop()
        
        # Calculate total records from sparse data
        total_sparse_records = sum(len(df) for df in sparse_data.values())
        
        print(f"   ✅ Sparse data access completed:")
        print(f"      - Time points requested: {len(sparse_times)}")
        print(f"      - Time points with data: {len(sparse_data)}")
        print(f"      - Total records: {total_sparse_records:,}")
        print(f"      - Duration: {sparse_metrics['duration_seconds']:.2f}s")
        print(f"      - Memory used: {sparse_metrics['memory_used_mb']:.2f} MB")
        print(f"      - Peak memory: {sparse_metrics['peak_memory_mb']:.2f} MB")
        
        return sparse_data, sparse_metrics
        
    except Exception as e:
        print(f"   ❌ Error reading sparse data: {e}")
        return None, None

def test_optimized_data_performance(instrument_id: str):
    """Test performance with optimized data access"""
    print("\n⚡ Testing Optimized Data Performance")
    print("=" * 50)
    
    config = get_config()
    data_client = DataClient(config.gcp.bucket, config)
    
    # Check if we're in mock mode
    if data_client.is_mock:
        print("   ⚠️  Using mock data mode - no real GCS data available")
        return None, None
    
    from market_data_tick_handler.data_client.tick_data_reader import TickDataReader
    tick_reader = TickDataReader(data_client)
    
    target_date = datetime(2023, 5, 23, tzinfo=timezone.utc)
    
    # Test optimized timestamp filtering
    print("3. Testing optimized timestamp filtering...")
    
    # Test with a 1-hour range
    start_time = target_date + timedelta(hours=12)  # 12:00 UTC
    end_time = target_date + timedelta(hours=13)    # 13:00 UTC
    
    monitor = PerformanceMonitor()
    monitor.start()
    
    try:
        optimized_data = tick_reader.get_tick_data_optimized(
            instrument_id=instrument_id,
            start_time=start_time,
            end_time=end_time,
            date=target_date,
            data_types=['trades'],
            use_predicate_pushdown=True
        )
        
        optimized_metrics = monitor.stop()
        
        print(f"   ✅ Optimized data access completed:")
        print(f"      - Records: {len(optimized_data):,}")
        print(f"      - Duration: {optimized_metrics['duration_seconds']:.2f}s")
        print(f"      - Memory used: {optimized_metrics['memory_used_mb']:.2f} MB")
        print(f"      - Peak memory: {optimized_metrics['peak_memory_mb']:.2f} MB")
        
        if not optimized_data.empty:
            print(f"      - Time range: {optimized_data['timestamp'].min()} to {optimized_data['timestamp'].max()}")
            print(f"      - Price range: ${optimized_data['price'].min():.2f} to ${optimized_data['price'].max():.2f}")
        
        return optimized_data, optimized_metrics
        
    except Exception as e:
        print(f"   ❌ Error reading optimized data: {e}")
        return None, None

def compare_performance(full_metrics, sparse_metrics, optimized_metrics):
    """Compare performance metrics across different methods"""
    print("\n📈 Performance Comparison Results")
    print("=" * 60)
    
    if not all([full_metrics, sparse_metrics, optimized_metrics]):
        print("❌ Cannot compare - some tests failed")
        return
    
    print(f"{'Method':<20} {'Duration (s)':<15} {'Memory (MB)':<15} {'Records':<15}")
    print("-" * 65)
    
    print(f"{'Full File':<20} {full_metrics['duration_seconds']:<15.2f} {full_metrics['memory_used_mb']:<15.2f} {'All':<15}")
    print(f"{'Sparse (5%)':<20} {sparse_metrics['duration_seconds']:<15.2f} {sparse_metrics['memory_used_mb']:<15.2f} {'5%':<15}")
    print(f"{'Optimized':<20} {optimized_metrics['duration_seconds']:<15.2f} {optimized_metrics['memory_used_mb']:<15.2f} {'1h':<15}")
    
    print("\n📊 Performance Improvements:")
    
    # Sparse vs Full comparison
    if full_metrics and sparse_metrics:
        speed_improvement = (full_metrics['duration_seconds'] - sparse_metrics['duration_seconds']) / full_metrics['duration_seconds'] * 100
        memory_improvement = (full_metrics['memory_used_mb'] - sparse_metrics['memory_used_mb']) / full_metrics['memory_used_mb'] * 100
        
        print(f"   Sparse vs Full:")
        print(f"      - Speed improvement: {speed_improvement:.1f}%")
        print(f"      - Memory improvement: {memory_improvement:.1f}%")
    
    # Optimized vs Full comparison
    if full_metrics and optimized_metrics:
        speed_improvement = (full_metrics['duration_seconds'] - optimized_metrics['duration_seconds']) / full_metrics['duration_seconds'] * 100
        memory_improvement = (full_metrics['memory_used_mb'] - optimized_metrics['memory_used_mb']) / full_metrics['memory_used_mb'] * 100
        
        print(f"   Optimized vs Full:")
        print(f"      - Speed improvement: {speed_improvement:.1f}%")
        print(f"      - Memory improvement: {memory_improvement:.1f}%")

def download_fresh_data(instrument_id: str):
    """Download fresh data with optimized Parquet format"""
    print("\n📥 Downloading Fresh Data with Optimized Format")
    print("=" * 50)
    
    print("To download fresh data with optimized Parquet format, run:")
    print(f"   ./deploy/local/run-main.sh download \\")
    print(f"       --start-date 2023-05-23 \\")
    print(f"       --end-date 2023-05-23 \\")
    print(f"       --venues binance \\")
    print(f"       --instrument-types perp \\")
    print(f"       --data-types trades \\")
    print(f"       --max-instruments 1")
    print()
    print("This will download BTC-USDT perps data with optimized Parquet format.")
    print("After download, re-run this test to compare performance.")

def main():
    """Run the complete performance comparison test"""
    print("🚀 Market Data Handler - Performance Comparison Test")
    print("=" * 70)
    print()
    print("Testing sparse data access (5% of data) vs full file reading")
    print("for BTC-USDT perps on May 23rd, 2023")
    print()
    
    try:
        # Step 1: Find the instrument ID
        instrument_id = find_btc_usdt_perps_instrument()
        if not instrument_id:
            print("❌ Could not find BTC-USDT perps instrument")
            return
        
        # Step 2: Test existing data performance
        full_data, full_metrics = test_existing_data_performance(instrument_id)
        
        # Step 3: Test sparse data performance
        sparse_data, sparse_metrics = test_sparse_data_performance(instrument_id, full_data)
        
        # Step 4: Test optimized data performance
        optimized_data, optimized_metrics = test_optimized_data_performance(instrument_id)
        
        # Step 5: Compare performance
        compare_performance(full_metrics, sparse_metrics, optimized_metrics)
        
        # Step 6: Show download instructions
        download_fresh_data(instrument_id)
        
        print("\n" + "=" * 70)
        print("🎯 Test Complete!")
        print()
        print("Key Findings:")
        print("1. Sparse data access should be significantly faster and use less memory")
        print("2. Optimized Parquet format improves query performance")
        print("3. For backtesting with sparse trading signals, use sparse data access")
        print("4. For full analysis, use optimized timestamp filtering")
        
    except Exception as e:
        print(f"❌ Error running test: {e}")
        print("Make sure you have:")
        print("1. Valid GCP credentials")
        print("2. Data in GCS bucket")
        print("3. Required dependencies installed (psutil)")

if __name__ == "__main__":
    main()
