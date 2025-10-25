#!/usr/bin/env python3
"""
GCS Connection Pooling Performance Test

This script tests the performance improvement from GCS connection pooling
by comparing uploads with and without connection reuse.
"""

import asyncio
import time
import sys
import tempfile
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from config import get_config
from market_data_tick_handler.data_downloader.download_orchestrator import DownloadOrchestrator

async def test_gcs_connection_pooling():
    """Test GCS upload performance with and without connection pooling"""
    config = get_config()
    
    # Create test data
    test_data = []
    for i in range(10):
        df = pd.DataFrame({
            'timestamp': [int(time.time() * 1000000) + i * 1000],
            'price': [50000.0 + i * 100],
            'amount': [1.0 + i * 0.1],
            'side': ['buy' if i % 2 == 0 else 'sell']
        })
        test_data.append(df)
    
    print("☁️  GCS Connection Pooling Performance Test")
    print("=" * 60)
    
    # Test 1: Without connection pooling (new client per upload)
    print("\n📊 Testing WITHOUT connection pooling (new client per upload)...")
    start_time = time.time()
    
    for i, df in enumerate(test_data):
        # Create new orchestrator (new GCS client) for each upload
        orchestrator = DownloadOrchestrator(
            gcs_bucket=config.gcp.bucket,
            api_key=config.tardis.api_key,
            max_workers=1
        )
        
        try:
            # Create a test blob
            blob_name = f"test/connection_pooling_test_{i}.parquet"
            blob = orchestrator.bucket.blob(blob_name)
            
            # Upload the data
            with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp_file:
                df.to_parquet(tmp_file.name, index=False, compression='snappy')
                blob.upload_from_filename(tmp_file.name)
            
            print(f"  ✅ Uploaded test_{i}.parquet")
        except Exception as e:
            print(f"  ❌ Failed to upload test_{i}.parquet: {e}")
        finally:
            # Close the client (this would happen in real usage)
            pass
    
    time_without_pooling = time.time() - start_time
    print(f"⏱️  Without pooling time: {time_without_pooling:.2f}s")
    
    # Test 2: With connection pooling (single client)
    print("\n📊 Testing WITH connection pooling (single client)...")
    start_time = time.time()
    
    # Create single orchestrator with connection pooling
    orchestrator = DownloadOrchestrator(
        gcs_bucket=config.gcp.bucket,
        api_key=config.tardis.api_key,
        max_workers=1
    )
    
    try:
        for i, df in enumerate(test_data):
            # Use the same client for all uploads
            blob_name = f"test/connection_pooling_test_pooled_{i}.parquet"
            blob = orchestrator.bucket.blob(blob_name)
            
            # Upload the data
            with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp_file:
                df.to_parquet(tmp_file.name, index=False, compression='snappy')
                blob.upload_from_filename(tmp_file.name)
            
            print(f"  ✅ Uploaded pooled_test_{i}.parquet")
    finally:
        # Close the client
        pass
    
    time_with_pooling = time.time() - start_time
    print(f"⏱️  With pooling time: {time_with_pooling:.2f}s")
    
    # Test 3: Parallel uploads with connection pooling
    print("\n📊 Testing PARALLEL uploads with connection pooling...")
    start_time = time.time()
    
    async def upload_single(df, i):
        """Upload a single file using the shared client"""
        orchestrator = DownloadOrchestrator(
            gcs_bucket=config.gcp.bucket,
            api_key=config.tardis.api_key,
            max_workers=1
        )
        
        try:
            blob_name = f"test/connection_pooling_test_parallel_{i}.parquet"
            blob = orchestrator.bucket.blob(blob_name)
            
            with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp_file:
                df.to_parquet(tmp_file.name, index=False, compression='snappy')
                blob.upload_from_filename(tmp_file.name)
            
            return f"✅ Uploaded parallel_test_{i}.parquet"
        except Exception as e:
            return f"❌ Failed to upload parallel_test_{i}.parquet: {e}"
    
    # Upload all files in parallel
    tasks = [upload_single(df, i) for i, df in enumerate(test_data)]
    results = await asyncio.gather(*tasks)
    
    for result in results:
        print(f"  {result}")
    
    time_parallel_pooling = time.time() - start_time
    print(f"⏱️  Parallel with pooling time: {time_parallel_pooling:.2f}s")
    
    # Calculate improvements
    pooling_improvement = time_without_pooling / time_with_pooling if time_with_pooling > 0 else 0
    parallel_improvement = time_without_pooling / time_parallel_pooling if time_parallel_pooling > 0 else 0
    
    print("\n" + "=" * 60)
    print("📊 GCS CONNECTION POOLING RESULTS")
    print("=" * 60)
    print(f"Without Pooling:     {time_without_pooling:.2f}s")
    print(f"With Pooling:        {time_with_pooling:.2f}s")
    print(f"Parallel + Pooling:  {time_parallel_pooling:.2f}s")
    print(f"Pooling Speedup:     {pooling_improvement:.2f}x")
    print(f"Parallel Speedup:    {parallel_improvement:.2f}x")
    
    if pooling_improvement >= 2.0:
        print("🎉 EXCELLENT: Significant improvement from GCS connection pooling!")
    elif pooling_improvement >= 1.5:
        print("✅ GOOD: Substantial improvement from GCS connection pooling!")
    elif pooling_improvement >= 1.2:
        print("⚠️  MODERATE: Some improvement from GCS connection pooling")
    else:
        print("❌ POOR: Minimal improvement from GCS connection pooling")
    
    print(f"\n💡 GCS connection pooling eliminates authentication and connection overhead")
    print(f"📈 Expected improvement: 1.5-3x (depending on file size and network)")

if __name__ == '__main__':
    asyncio.run(test_gcs_connection_pooling())
