#!/usr/bin/env python3
"""
Accurate Bandwidth Bottleneck Test

This script properly captures performance metrics to test if internet
bandwidth is the primary bottleneck with multiple concurrent processes.
"""

import asyncio
import subprocess
import time
import os
import re
from pathlib import Path

def run_download_process(process_id: str, max_workers: int = 2) -> dict:
    """Run a single download process and extract accurate metrics"""
    
    env = os.environ.copy()
    env['DOWNLOAD_MAX_WORKERS'] = str(max_workers)
    env['PROCESS_ID'] = process_id
    
    cmd = [
        'python', '-m', 'market_data_tick_handler.main.',
        '--mode', 'download',
        '--start-date', '2023-05-23',
        '--end-date', '2023-05-23',
        '--venues', 'deribit',
        '--max-instruments', '3'  # Fixed number for consistent comparison
    ]
    
    print(f"🚀 Starting Process {process_id} with {max_workers} workers")
    
    start_time = time.time()
    
    try:
        result = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True,
            timeout=180,
            cwd=Path(__file__).parent
        )
        
        execution_time = time.time() - start_time
        
        # Extract metrics using regex for better accuracy
        download_time = 0.0
        upload_time = 0.0
        files_processed = 0
        instruments_processed = 0
        download_throughput = 0.0
        upload_throughput = 0.0
        
        if result.stdout:
            # Look for specific log patterns
            download_match = re.search(r'Total Download Time: ([\d.]+)s', result.stdout)
            if download_match:
                download_time = float(download_match.group(1))
            
            upload_match = re.search(r'Total Upload Time: ([\d.]+)s', result.stdout)
            if upload_match:
                upload_time = float(upload_match.group(1))
            
            files_match = re.search(r'Total Files Processed: (\d+)', result.stdout)
            if files_match:
                files_processed = int(files_match.group(1))
            
            instruments_match = re.search(r'(\d+) processed, 0 failed', result.stdout)
            if instruments_match:
                instruments_processed = int(instruments_match.group(1))
            
            download_throughput_match = re.search(r'Download: ([\d.]+) files/sec', result.stdout)
            if download_throughput_match:
                download_throughput = float(download_throughput_match.group(1))
            
            upload_throughput_match = re.search(r'Upload: ([\d.]+) files/sec', result.stdout)
            if upload_throughput_match:
                upload_throughput = float(upload_throughput_match.group(1))
        
        return {
            'success': result.returncode == 0,
            'execution_time': execution_time,
            'download_time': download_time,
            'upload_time': upload_time,
            'files_processed': files_processed,
            'instruments_processed': instruments_processed,
            'download_throughput': download_throughput,
            'upload_throughput': upload_throughput,
            'process_id': process_id,
            'max_workers': max_workers
        }
        
    except subprocess.TimeoutExpired:
        return {
            'success': False,
            'execution_time': 180.0,
            'error': 'Timeout'
        }
    except Exception as e:
        return {
            'success': False,
            'execution_time': 0.0,
            'error': str(e)
        }

async def test_bandwidth_bottleneck():
    """Test bandwidth bottleneck with accurate metrics"""
    print("🌐 ACCURATE BANDWIDTH BOTTLENECK TEST")
    print("="*70)
    print("Testing if internet bandwidth limits concurrent downloads")
    print("="*70)
    
    # Test 1: Single process baseline
    print(f"\n📊 TEST 1: Single Process Baseline")
    print("-" * 50)
    
    result1 = run_download_process("1", max_workers=2)
    
    if result1['success']:
        print(f"✅ Completed in {result1['execution_time']:.2f}s")
        print(f"📁 Instruments: {result1['instruments_processed']}")
        print(f"📁 Files: {result1['files_processed']}")
        print(f"⏱️  Download: {result1['download_time']:.2f}s ({result1['download_throughput']:.2f} files/sec)")
        print(f"📤 Upload: {result1['upload_time']:.2f}s ({result1['upload_throughput']:.2f} files/sec)")
    else:
        print(f"❌ Failed: {result1.get('error', 'Unknown error')}")
        return
    
    # Test 2: Two concurrent processes (simulating 2 VMs)
    print(f"\n📊 TEST 2: Two Concurrent Processes (2 VMs)")
    print("-" * 50)
    
    start_time = time.time()
    
    # Run both processes concurrently
    loop = asyncio.get_event_loop()
    
    task1 = loop.run_in_executor(None, run_download_process, "1", 2)
    task2 = loop.run_in_executor(None, run_download_process, "2", 2)
    
    result2a, result2b = await asyncio.gather(task1, task2)
    
    total_time = time.time() - start_time
    
    if result2a['success'] and result2b['success']:
        total_instruments = result2a['instruments_processed'] + result2b['instruments_processed']
        total_files = result2a['files_processed'] + result2b['files_processed']
        total_download_time = max(result2a['download_time'], result2b['download_time'])
        total_upload_time = max(result2a['upload_time'], result2b['upload_time'])
        combined_download_throughput = result2a['download_throughput'] + result2b['download_throughput']
        combined_upload_throughput = result2a['upload_throughput'] + result2b['upload_throughput']
        
        print(f"✅ Both completed in {total_time:.2f}s")
        print(f"📁 Total instruments: {total_instruments}")
        print(f"📁 Total files: {total_files}")
        print(f"⏱️  Max download time: {total_download_time:.2f}s")
        print(f"📤 Max upload time: {total_upload_time:.2f}s")
        print(f"🚀 Combined download throughput: {combined_download_throughput:.2f} files/sec")
        print(f"🚀 Combined upload throughput: {combined_upload_throughput:.2f} files/sec")
        print(f"📊 Process 1: {result2a['execution_time']:.2f}s ({result2a['instruments_processed']} instruments, {result2a['files_processed']} files)")
        print(f"📊 Process 2: {result2b['execution_time']:.2f}s ({result2b['instruments_processed']} instruments, {result2b['files_processed']} files)")
        
        # Calculate efficiency metrics
        single_time = result1['execution_time']
        dual_time = max(result2a['execution_time'], result2b['execution_time'])
        time_efficiency = (single_time / dual_time) * 100
        
        single_throughput = result1['download_throughput']
        dual_throughput = combined_download_throughput
        throughput_efficiency = (dual_throughput / (single_throughput * 2)) * 100 if single_throughput > 0 else 0
        
        print(f"\n🔍 BOTTLENECK ANALYSIS:")
        print(f"• Single process time: {single_time:.2f}s")
        print(f"• Dual processes time: {dual_time:.2f}s (max of both)")
        print(f"• Time efficiency: {time_efficiency:.1f}%")
        print(f"• Single download throughput: {single_throughput:.2f} files/sec")
        print(f"• Dual download throughput: {dual_throughput:.2f} files/sec")
        print(f"• Throughput efficiency: {throughput_efficiency:.1f}%")
        
        # Determine bottleneck
        if throughput_efficiency > 80 and time_efficiency > 80:
            print(f"✅ BANDWIDTH IS NOT THE BOTTLENECK")
            print(f"   • Both time and throughput efficiency > 80%")
            print(f"   • Multiple VMs should provide excellent scaling")
        elif throughput_efficiency > 60 or time_efficiency > 60:
            print(f"⚠️  PARTIAL BANDWIDTH BOTTLENECK")
            print(f"   • Some efficiency loss detected")
            print(f"   • Multiple VMs will provide good but not perfect scaling")
        else:
            print(f"❌ BANDWIDTH IS THE PRIMARY BOTTLENECK")
            print(f"   • Significant efficiency loss detected")
            print(f"   • Internet connection is limiting performance")
        
        print(f"\n🎯 RECOMMENDATION:")
        if throughput_efficiency > 80 and time_efficiency > 80:
            print(f"🚀 Deploy multiple VMs for maximum performance!")
            print(f"   Expected scaling: Near-linear with number of VMs")
            print(f"   Cost-benefit: High (significant performance gain)")
        elif throughput_efficiency > 60 or time_efficiency > 60:
            print(f"✅ Consider 2-3 VMs for good performance")
            print(f"   Expected scaling: Good with diminishing returns")
            print(f"   Cost-benefit: Medium (moderate performance gain)")
        else:
            print(f"⚠️  Focus on optimizing single VM first")
            print(f"   Expected scaling: Limited by bandwidth")
            print(f"   Cost-benefit: Low (minimal performance gain)")
        
        print(f"\n💡 OPTIMIZATION STRATEGIES:")
        if throughput_efficiency < 60:
            print(f"1. Upgrade internet connection speed")
            print(f"2. Use CDN or edge locations closer to data sources")
            print(f"3. Implement data compression")
            print(f"4. Use multiple internet providers (load balancing)")
        else:
            print(f"1. Deploy multiple VMs with separate connections")
            print(f"2. Use geographic distribution (different regions)")
            print(f"3. Implement intelligent load balancing")
            print(f"4. Consider dedicated bandwidth allocation")
        
    else:
        print(f"❌ One or both processes failed")
        if not result2a['success']:
            print(f"   Process 1: {result2a.get('error', 'Unknown error')}")
        if not result2b['success']:
            print(f"   Process 2: {result2b.get('error', 'Unknown error')}")

if __name__ == '__main__':
    asyncio.run(test_bandwidth_bottleneck())
