#!/usr/bin/env python3
"""
Quick Usage Guide for Single Instrument Processor

This script shows the most common usage patterns and examples.
"""

import subprocess
import sys
from pathlib import Path

def run_command(cmd: str, description: str):
    """Run a command and show description"""
    print(f"\n📋 {description}")
    print(f"Command: {cmd}")
    print("-" * 60)
    
    try:
        result = subprocess.run(cmd.split(), capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            print("✅ Command executed successfully")
            if result.stdout:
                print("Output:")
                print(result.stdout)
        else:
            print("❌ Command failed")
            if result.stderr:
                print("Error:")
                print(result.stderr)
    except subprocess.TimeoutExpired:
        print("⏰ Command timed out (this is expected for long-running commands)")
    except Exception as e:
        print(f"❌ Error: {e}")

def main():
    """Show usage examples"""
    print("🚀 Single Instrument Processor - Usage Guide")
    print("=" * 60)
    
    # Change to project directory
    project_root = Path(__file__).parent.parent
    print(f"Project root: {project_root}")
    
    examples = [
        {
            "cmd": "python live_streaming/single_instrument_processor.py --list-instruments",
            "desc": "List all available Binance spot USDT instruments"
        },
        {
            "cmd": "python live_streaming/single_instrument_processor.py --instrument BTC-USDT --duration 30",
            "desc": "Process BTC-USDT for 30 seconds with 1 thread"
        },
        {
            "cmd": "python live_streaming/single_instrument_processor.py --instrument ETH-USDT --duration 60 --threads 2",
            "desc": "Process ETH-USDT for 1 minute with 2 threads"
        },
        {
            "cmd": "python live_streaming/single_instrument_processor.py --instrument ADA-USDT --duration 120 --threads 4 --stats-interval 5",
            "desc": "Process ADA-USDT for 2 minutes with 4 threads, stats every 5 seconds"
        },
        {
            "cmd": "python live_streaming/demo_single_instrument.py",
            "desc": "Run the demo script with multiple examples"
        }
    ]
    
    for example in examples:
        run_command(example["cmd"], example["desc"])
    
    print("\n" + "=" * 60)
    print("📚 Additional Information:")
    print("- Read SINGLE_INSTRUMENT_README.md for detailed documentation")
    print("- Use --help for command-line options")
    print("- Check logs for detailed processing information")
    print("- Use Ctrl+C to stop long-running processes")
    print("=" * 60)

if __name__ == "__main__":
    main()
