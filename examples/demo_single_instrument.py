#!/usr/bin/env python3
"""
Demo script for Single Instrument Processor

This script demonstrates how to use the single instrument processor
with different configurations and shows the latency statistics.
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from live_streaming.single_instrument_processor import CLIProcessor

async def demo_single_thread():
    """Demo with single thread"""
    print("🧪 Demo 1: Single Thread Processing")
    print("=" * 50)
    
    cli = CLIProcessor()
    await cli.process_instrument(
        instrument_key="BINANCE:SPOT_PAIR:BTC-USDT",
        duration=30,  # 30 seconds
        num_threads=1,
        stats_interval=5
    )

async def demo_multi_thread():
    """Demo with multiple threads"""
    print("\n🧪 Demo 2: Multi-Thread Processing")
    print("=" * 50)
    
    cli = CLIProcessor()
    await cli.process_instrument(
        instrument_key="BINANCE:SPOT_PAIR:ETH-USDT",
        duration=30,  # 30 seconds
        num_threads=4,
        stats_interval=5
    )

async def demo_list_instruments():
    """Demo listing instruments"""
    print("\n🧪 Demo 3: List Available Instruments")
    print("=" * 50)
    
    cli = CLIProcessor()
    await cli.list_instruments()

async def main():
    """Run all demos"""
    print("🚀 Single Instrument Processor Demo")
    print("=" * 60)
    
    try:
        # Demo 1: List instruments
        await demo_list_instruments()
        
        # Demo 2: Single thread processing
        await demo_single_thread()
        
        # Demo 3: Multi-thread processing
        await demo_multi_thread()
        
        print("\n✅ All demos completed!")
        print("\nTo run manually:")
        print("  python single_instrument_processor.py --list-instruments")
        print("  python single_instrument_processor.py --instrument BTC-USDT --duration 60 --threads 2")
        
    except KeyboardInterrupt:
        print("\n🛑 Demo interrupted by user")
    except Exception as e:
        print(f"\n❌ Demo error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
