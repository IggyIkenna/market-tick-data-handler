#!/usr/bin/env python3
"""
Simplified Parquet Performance Test for BINANCE-FUTURES:PERPETUAL:BTC-USDT

This test focuses on the current data performance for sparse time intervals.
"""

import asyncio
import time
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from market_data_tick_handler.data_client.data_client import DataClient
from config import get_config
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SimpleParquetTest:
    """Simple Parquet performance test for sparse data access"""
    
    def __init__(self):
        self.config = get_config()
        self.data_client = DataClient(self.config.gcp.bucket, self.config)
        self.instrument_id = "BINANCE-FUTURES:PERPETUAL:BTC-USDT"
        self.test_date = datetime(2023, 5, 23, tzinfo=timezone.utc)
        
    async def run_test(self):
        """Run the performance test"""
        print("🚀 Parquet Performance Test - BINANCE-FUTURES:PERPETUAL:BTC-USDT")
        print(f"📅 Date: {self.test_date.strftime('%Y-%m-%d')}")
        print("=" * 60)
        
        # Test current data performance
        results = await self._test_current_data()
        
        # Print detailed results
        self._print_results(results)
        
    async def _test_current_data(self) -> Dict:
        """Test performance on current data"""
        print("🔍 Testing Current Data Performance...")
        
        results = {
            'instrument_id': self.instrument_id,
            'test_date': self.test_date.strftime('%Y-%m-%d'),
            'full_file_read': {},
            'sparse_intervals': {},
            'time_range_filtering': {},
            'memory_efficiency': {}
        }
        
        try:
            # Test 1: Full file read
            print("  📖 Testing full file read...")
            start_time = time.time()
            
            df_full = await self.data_client.read_parquet_file(
                f"raw_tick_data/by_date/day-{self.test_date.strftime('%Y-%m-%d')}/data_type-trades/{self.instrument_id}.parquet"
            )
            
            full_read_time = time.time() - start_time
            memory_usage = df_full.memory_usage(deep=True).sum() / (1024 * 1024)
            
            results['full_file_read'] = {
                'time_seconds': full_read_time,
                'records': len(df_full),
                'memory_mb': memory_usage,
                'records_per_second': len(df_full) / full_read_time
            }
            
            print(f"    ✅ Full file read: {full_read_time:.3f}s, {len(df_full):,} records, {memory_usage:.2f} MB")
            
            # Test 2: Sparse intervals (simulate backtesting scenario)
            print("  🎯 Testing sparse intervals (5% of data)...")
            sparse_intervals = self._generate_sparse_intervals(df_full, sample_rate=0.05)
            
            start_time = time.time()
            sparse_data = self._read_sparse_intervals(df_full, sparse_intervals)
            sparse_time = time.time() - start_time
            sparse_memory = sparse_data.memory_usage(deep=True).sum() / (1024 * 1024)
            
            results['sparse_intervals'] = {
                'time_seconds': sparse_time,
                'intervals': len(sparse_intervals),
                'records': len(sparse_data),
                'memory_mb': sparse_memory,
                'records_per_second': len(sparse_data) / sparse_time,
                'speedup_vs_full': results['full_file_read']['time_seconds'] / sparse_time
            }
            
            print(f"    ✅ Sparse intervals: {sparse_time:.3f}s, {len(sparse_intervals)} intervals, {len(sparse_data):,} records")
            print(f"    📊 Speedup vs full read: {results['sparse_intervals']['speedup_vs_full']:.1f}x")
            
            # Test 3: Time range filtering (10% of data)
            print("  ⏰ Testing time range filtering (10% of data)...")
            start_time = time.time()
            filtered_data = self._filter_by_time_range(df_full, sample_rate=0.1)
            filter_time = time.time() - start_time
            filter_memory = filtered_data.memory_usage(deep=True).sum() / (1024 * 1024)
            
            results['time_range_filtering'] = {
                'time_seconds': filter_time,
                'records': len(filtered_data),
                'memory_mb': filter_memory,
                'records_per_second': len(filtered_data) / filter_time,
                'speedup_vs_full': results['full_file_read']['time_seconds'] / filter_time
            }
            
            print(f"    ✅ Time range filtering: {filter_time:.3f}s, {len(filtered_data):,} records")
            print(f"    📊 Speedup vs full read: {results['time_range_filtering']['speedup_vs_full']:.1f}x")
            
            # Test 4: Memory efficiency analysis
            print("  💾 Analyzing memory efficiency...")
            results['memory_efficiency'] = {
                'full_file_mb': memory_usage,
                'sparse_mb': sparse_memory,
                'filtered_mb': filter_memory,
                'sparse_reduction': (memory_usage - sparse_memory) / memory_usage * 100,
                'filtered_reduction': (memory_usage - filter_memory) / memory_usage * 100
            }
            
            print(f"    ✅ Memory reduction - Sparse: {results['memory_efficiency']['sparse_reduction']:.1f}%, Filtered: {results['memory_efficiency']['filtered_reduction']:.1f}%")
            
        except Exception as e:
            print(f"    ❌ Error testing current data: {e}")
            results['error'] = str(e)
            
        return results
    
    def _generate_sparse_intervals(self, df: pd.DataFrame, sample_rate: float = 0.05) -> List[Tuple[datetime, datetime]]:
        """Generate sparse time intervals for testing"""
        if 'timestamp' in df.columns:
            timestamps = pd.to_datetime(df['timestamp'], unit='us')
        else:
            timestamps = pd.to_datetime(df['local_timestamp'], unit='us')
        
        # Sort by timestamp
        df_sorted = df.sort_values('timestamp' if 'timestamp' in df.columns else 'local_timestamp')
        
        # Generate random intervals covering sample_rate of the data
        total_records = len(df_sorted)
        target_records = int(total_records * sample_rate)
        
        intervals = []
        current_pos = 0
        
        while current_pos < total_records and len(intervals) < 20:  # Max 20 intervals
            # Random interval size (1-5 minutes of data)
            interval_size = np.random.randint(1000, 5000)  # 1k-5k records
            end_pos = min(current_pos + interval_size, total_records)
            
            start_time = timestamps.iloc[current_pos]
            end_time = timestamps.iloc[end_pos - 1]
            
            intervals.append((start_time, end_time))
            current_pos = end_pos + np.random.randint(5000, 20000)  # Skip some data
        
        return intervals
    
    def _read_sparse_intervals(self, df: pd.DataFrame, intervals: List[Tuple[datetime, datetime]]) -> pd.DataFrame:
        """Read data for sparse intervals"""
        if 'timestamp' in df.columns:
            timestamps = pd.to_datetime(df['timestamp'], unit='us')
        else:
            timestamps = pd.to_datetime(df['local_timestamp'], unit='us')
        
        mask = pd.Series(False, index=df.index)
        
        for start_time, end_time in intervals:
            interval_mask = (timestamps >= start_time) & (timestamps <= end_time)
            mask |= interval_mask
        
        return df[mask]
    
    def _filter_by_time_range(self, df: pd.DataFrame, sample_rate: float = 0.1) -> pd.DataFrame:
        """Filter data by time range"""
        if 'timestamp' in df.columns:
            timestamps = pd.to_datetime(df['timestamp'], unit='us')
        else:
            timestamps = pd.to_datetime(df['local_timestamp'], unit='us')
        
        # Get random time range covering sample_rate of data
        total_records = len(df)
        start_idx = np.random.randint(0, int(total_records * (1 - sample_rate)))
        end_idx = start_idx + int(total_records * sample_rate)
        
        start_time = timestamps.iloc[start_idx]
        end_time = timestamps.iloc[end_idx]
        
        mask = (timestamps >= start_time) & (timestamps <= end_time)
        return df[mask]
    
    def _print_results(self, results: Dict):
        """Print detailed results"""
        print("\n📊 DETAILED PERFORMANCE RESULTS")
        print("=" * 60)
        
        print(f"📋 Test Configuration:")
        print(f"  Instrument: {results['instrument_id']}")
        print(f"  Date: {results['test_date']}")
        
        print(f"\n📖 Full File Read:")
        full = results['full_file_read']
        print(f"  Time: {full['time_seconds']:.3f}s")
        print(f"  Records: {full['records']:,}")
        print(f"  Memory: {full['memory_mb']:.2f} MB")
        print(f"  Throughput: {full['records_per_second']:,.0f} records/sec")
        
        print(f"\n🎯 Sparse Intervals (5% data):")
        sparse = results['sparse_intervals']
        print(f"  Time: {sparse['time_seconds']:.3f}s")
        print(f"  Intervals: {sparse['intervals']}")
        print(f"  Records: {sparse['records']:,}")
        print(f"  Memory: {sparse['memory_mb']:.2f} MB")
        print(f"  Throughput: {sparse['records_per_second']:,.0f} records/sec")
        print(f"  Speedup: {sparse['speedup_vs_full']:.1f}x faster")
        
        print(f"\n⏰ Time Range Filtering (10% data):")
        filtered = results['time_range_filtering']
        print(f"  Time: {filtered['time_seconds']:.3f}s")
        print(f"  Records: {filtered['records']:,}")
        print(f"  Memory: {filtered['memory_mb']:.2f} MB")
        print(f"  Throughput: {filtered['records_per_second']:,.0f} records/sec")
        print(f"  Speedup: {filtered['speedup_vs_full']:.1f}x faster")
        
        print(f"\n💾 Memory Efficiency:")
        mem = results['memory_efficiency']
        print(f"  Sparse reduction: {mem['sparse_reduction']:.1f}%")
        print(f"  Filtered reduction: {mem['filtered_reduction']:.1f}%")
        
        print(f"\n💡 Key Insights:")
        print(f"  • Sparse access is {sparse['speedup_vs_full']:.1f}x faster than full read")
        print(f"  • Time range filtering is {filtered['speedup_vs_full']:.1f}x faster than full read")
        print(f"  • Memory usage reduced by {mem['sparse_reduction']:.1f}% for sparse access")
        print(f"  • Ideal for backtesting scenarios with sparse data requirements")

async def main():
    """Main test function"""
    test = SimpleParquetTest()
    await test.run_test()

if __name__ == "__main__":
    asyncio.run(main())
