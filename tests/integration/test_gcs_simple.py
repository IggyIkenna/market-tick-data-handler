#!/usr/bin/env python3
"""
Simple GCS Performance Test

This script tests the performance improvement from reusing GCS client instances
vs creating new ones for each upload.
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
from google.cloud import storage

async def test_gcs_client_reuse():
    """Test GCS upload performance with and without client reuse"""
    config = get_config()
    
    # Create test data
    test_data = []
    for i in range(5):
        df = pd.DataFrame({
            'timestamp': [int(time.time() * 1000000) + i * 1000],
            'price': [50000.0 + i * 100],
            'amount': [1.0 + i * 0.1],
            'side': ['buy' if i % 2 == 0 else 'sell']
        })
        test_data.append(df)
    
    print("☁️  GCS Client Reuse Performance Test")
    print("=" * 60)
    
    # Test 1: Without client reuse (new client per upload)
    print("\n📊 Testing WITHOUT client reuse (new client per upload)...")
    start_time = time.time()
    
    for i, df in enumerate(test_data):
        # Create new client for each upload
        client = storage.Client()
        bucket = client.bucket(config.gcp.bucket)
        
        try:
            # Create a test blob
            blob_name = f"test/client_reuse_test_{i}.parquet"
            blob = bucket.blob(blob_name)
            
            # Upload the data
            with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp_file:
                df.to_parquet(tmp_file.name, index=False, compression='snappy')
                blob.upload_from_filename(tmp_file.name)
            
            print(f"  ✅ Uploaded test_{i}.parquet")
        except Exception as e:
            print(f"  ❌ Failed to upload test_{i}.parquet: {e}")
    
    time_without_reuse = time.time() - start_time
    print(f"⏱️  Without reuse time: {time_without_reuse:.2f}s")
    
    # Test 2: With client reuse (single client)
    print("\n📊 Testing WITH client reuse (single client)...")
    start_time = time.time()
    
    # Create single client for all uploads
    client = storage.Client()
    bucket = client.bucket(config.gcp.bucket)
    
    for i, df in enumerate(test_data):
        try:
            # Use the same client for all uploads
            blob_name = f"test/client_reuse_test_pooled_{i}.parquet"
            blob = bucket.blob(blob_name)
            
            # Upload the data
            with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp_file:
                df.to_parquet(tmp_file.name, index=False, compression='snappy')
                blob.upload_from_filename(tmp_file.name)
            
            print(f"  ✅ Uploaded pooled_test_{i}.parquet")
        except Exception as e:
            print(f"  ❌ Failed to upload pooled_test_{i}.parquet: {e}")
    
    time_with_reuse = time.time() - start_time
    print(f"⏱️  With reuse time: {time_with_reuse:.2f}s")
    
    # Test 3: Parallel uploads with shared client
    print("\n📊 Testing PARALLEL uploads with shared client...")
    start_time = time.time()
    
    async def upload_single(df, i, client, bucket):
        """Upload a single file using the shared client"""
        try:
            blob_name = f"test/client_reuse_test_parallel_{i}.parquet"
            blob = bucket.blob(blob_name)
            
            with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp_file:
                df.to_parquet(tmp_file.name, index=False, compression='snappy')
                blob.upload_from_filename(tmp_file.name)
            
            return f"✅ Uploaded parallel_test_{i}.parquet"
        except Exception as e:
            return f"❌ Failed to upload parallel_test_{i}.parquet: {e}"
    
    # Upload all files in parallel using shared client
    shared_client = storage.Client()
    shared_bucket = shared_client.bucket(config.gcp.bucket)
    
    tasks = [upload_single(df, i, shared_client, shared_bucket) for i, df in enumerate(test_data)]
    results = await asyncio.gather(*tasks)
    
    for result in results:
        print(f"  {result}")
    
    time_parallel_reuse = time.time() - start_time
    print(f"⏱️  Parallel with reuse time: {time_parallel_reuse:.2f}s")
    
    # Calculate improvements
    reuse_improvement = time_without_reuse / time_with_reuse if time_with_reuse > 0 else 0
    parallel_improvement = time_without_reuse / time_parallel_reuse if time_parallel_reuse > 0 else 0
    
    print("\n" + "=" * 60)
    print("📊 GCS CLIENT REUSE RESULTS")
    print("=" * 60)
    print(f"Without Reuse:     {time_without_reuse:.2f}s")
    print(f"With Reuse:        {time_with_reuse:.2f}s")
    print(f"Parallel + Reuse:  {time_parallel_reuse:.2f}s")
    print(f"Reuse Speedup:     {reuse_improvement:.2f}x")
    print(f"Parallel Speedup:  {parallel_improvement:.2f}x")
    
    if reuse_improvement >= 1.5:
        print("🎉 EXCELLENT: Significant improvement from GCS client reuse!")
    elif reuse_improvement >= 1.2:
        print("✅ GOOD: Substantial improvement from GCS client reuse!")
    elif reuse_improvement >= 1.1:
        print("⚠️  MODERATE: Some improvement from GCS client reuse")
    else:
        print("❌ POOR: Minimal improvement from GCS client reuse")
    
    print(f"\n💡 GCS client reuse eliminates authentication and connection overhead")
    print(f"📈 Expected improvement: 1.2-2x (depending on file size and network)")

if __name__ == '__main__':
    asyncio.run(test_gcs_client_reuse())
