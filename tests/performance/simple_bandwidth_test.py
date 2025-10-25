#!/usr/bin/env python3
"""
Simple Bandwidth Bottleneck Test

This script tests whether internet bandwidth is the primary bottleneck
by running multiple concurrent download processes.
"""

import asyncio
import subprocess
import time
import os
from pathlib import Path

def run_download_process(process_id: str, max_workers: int = 2) -> dict:
    """Run a single download process"""
    
    env = os.environ.copy()
    env['DOWNLOAD_MAX_WORKERS'] = str(max_workers)
    env['PROCESS_ID'] = process_id
    
    cmd = [
        'python', '-m', 'market_data_tick_handler.main.',
        '--mode', 'download',
        '--start-date', '2023-05-23',
        '--end-date', '2023-05-23',
        '--venues', 'deribit',
        '--max-instruments', '5'  # Smaller number for faster testing
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
        
        # Extract key metrics from logs
        download_time = 0.0
        upload_time = 0.0
        files_processed = 0
        
        if result.stdout:
            for line in result.stdout.split('\n'):
                if 'Total Download Time:' in line:
                    try:
                        download_time = float(line.split(':')[1].strip().replace('s', ''))
                    except:
                        pass
                elif 'Total Upload Time:' in line:
                    try:
                        upload_time = float(line.split(':')[1].strip().replace('s', ''))
                    except:
                        pass
                elif 'Total Files Processed:' in line:
                    try:
                        files_processed = int(line.split(':')[1].strip())
                    except:
                        pass
        
        return {
            'success': result.returncode == 0,
            'execution_time': execution_time,
            'download_time': download_time,
            'upload_time': upload_time,
            'files_processed': files_processed,
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
    """Test bandwidth bottleneck with concurrent processes"""
    print("🌐 SIMPLE BANDWIDTH BOTTLENECK TEST")
    print("="*60)
    print("Testing if internet bandwidth limits concurrent downloads")
    print("="*60)
    
    # Test 1: Single process baseline
    print(f"\n📊 TEST 1: Single Process Baseline")
    print("-" * 40)
    
    result1 = run_download_process("1", max_workers=2)
    
    if result1['success']:
        print(f"✅ Completed in {result1['execution_time']:.2f}s")
        print(f"📁 Files: {result1['files_processed']}")
        print(f"⏱️  Download: {result1['download_time']:.2f}s")
        print(f"📤 Upload: {result1['upload_time']:.2f}s")
    else:
        print(f"❌ Failed: {result1.get('error', 'Unknown error')}")
        return
    
    # Test 2: Two concurrent processes (simulating 2 VMs)
    print(f"\n📊 TEST 2: Two Concurrent Processes (2 VMs)")
    print("-" * 40)
    
    start_time = time.time()
    
    # Run both processes concurrently
    loop = asyncio.get_event_loop()
    
    task1 = loop.run_in_executor(None, run_download_process, "1", 2)
    task2 = loop.run_in_executor(None, run_download_process, "2", 2)
    
    result2a, result2b = await asyncio.gather(task1, task2)
    
    total_time = time.time() - start_time
    
    if result2a['success'] and result2b['success']:
        print(f"✅ Both completed in {total_time:.2f}s")
        print(f"📁 Total files: {result2a['files_processed'] + result2b['files_processed']}")
        print(f"📊 Process 1: {result2a['execution_time']:.2f}s ({result2a['files_processed']} files)")
        print(f"📊 Process 2: {result2b['execution_time']:.2f}s ({result2b['files_processed']} files)")
        
        # Calculate efficiency
        single_time = result1['execution_time']
        dual_time = max(result2a['execution_time'], result2b['execution_time'])
        efficiency = (single_time / dual_time) * 100
        
        print(f"\n🔍 BOTTLENECK ANALYSIS:")
        print(f"• Single process: {single_time:.2f}s")
        print(f"• Dual processes: {dual_time:.2f}s (max of both)")
        print(f"• Efficiency: {efficiency:.1f}%")
        
        if efficiency > 80:
            print(f"✅ BANDWIDTH IS NOT THE BOTTLENECK")
            print(f"   • Dual processes achieved {efficiency:.1f}% efficiency")
            print(f"   • Multiple VMs should provide good scaling")
        elif efficiency > 50:
            print(f"⚠️  PARTIAL BANDWIDTH BOTTLENECK")
            print(f"   • Dual processes achieved {efficiency:.1f}% efficiency")
            print(f"   • Some bandwidth sharing exists")
        else:
            print(f"❌ BANDWIDTH IS THE PRIMARY BOTTLENECK")
            print(f"   • Dual processes only achieved {efficiency:.1f}% efficiency")
            print(f"   • Internet connection is limiting performance")
        
        print(f"\n🎯 RECOMMENDATION:")
        if efficiency > 80:
            print(f"🚀 Deploy multiple VMs for maximum performance!")
            print(f"   Expected scaling: Near-linear with number of VMs")
        elif efficiency > 50:
            print(f"✅ Consider 2-3 VMs for good performance")
            print(f"   Expected scaling: Diminishing returns beyond 2-3 VMs")
        else:
            print(f"⚠️  Focus on optimizing single VM first")
            print(f"   Expected scaling: Limited by bandwidth")
        
    else:
        print(f"❌ One or both processes failed")
        if not result2a['success']:
            print(f"   Process 1: {result2a.get('error', 'Unknown error')}")
        if not result2b['success']:
            print(f"   Process 2: {result2b.get('error', 'Unknown error')}")

if __name__ == '__main__':
    asyncio.run(test_bandwidth_bottleneck())
