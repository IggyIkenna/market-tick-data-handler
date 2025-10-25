#!/usr/bin/env python3
"""
Quality Gate Runner

Runs all tests with time limits and coverage checks to ensure code quality
before deployment. Total execution time must be <30 seconds.
"""

import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, Any


def load_quality_gates() -> Dict[str, Any]:
    """Load quality gate configuration"""
    gates_file = Path(__file__).parent / "performance" / "quality_gates.json"
    with open(gates_file) as f:
        return json.load(f)


def run_tests(test_type: str, max_duration: int) -> tuple[bool, float, float]:
    """
    Run tests for a specific type
    
    Returns:
        (success, duration, coverage)
    """
    print(f"🧪 Running {test_type} tests...")
    
    start_time = time.time()
    
    # Build pytest command
    if test_type == "unit":
        # Unit tests don't have markers, run all tests in unit directory
        cmd = [
            "python", "-m", "pytest",
            f"tests/{test_type}/",
            "-v",
            "--tb=short",
            "--cov=src",
            "--cov-report=term-missing"
        ]
    else:
        cmd = [
            "python", "-m", "pytest",
            f"tests/{test_type}/",
            "-v",
            "--tb=short",
            f"-m {test_type}",
            "--cov=src",
            "--cov-report=term-missing"
        ]
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=max_duration + 5  # Add 5s buffer
        )
        
        duration = time.time() - start_time
        
        # Extract coverage from output
        coverage = 0.0
        for line in result.stdout.split('\n'):
            if 'TOTAL' in line and '%' in line:
                try:
                    coverage = float(line.split('%')[0].split()[-1])
                    break
                except (ValueError, IndexError):
                    pass
        
        success = result.returncode == 0 and duration <= max_duration
        
        if success:
            print(f"✅ {test_type} tests passed in {duration:.2f}s (coverage: {coverage:.1f}%)")
        else:
            print(f"❌ {test_type} tests failed")
            if duration > max_duration:
                print(f"   Duration {duration:.2f}s exceeded limit {max_duration}s")
            if result.returncode != 0:
                print(f"   Exit code: {result.returncode}")
                print("   STDOUT:", result.stdout[-500:])
                print("   STDERR:", result.stderr[-500:])
        
        return success, duration, coverage
        
    except subprocess.TimeoutExpired:
        duration = time.time() - start_time
        print(f"❌ {test_type} tests timed out after {duration:.2f}s")
        return False, duration, 0.0


def run_performance_benchmarks() -> tuple[bool, float]:
    """Run performance benchmarks"""
    print("🚀 Running performance benchmarks...")
    
    start_time = time.time()
    
    cmd = [
        "python", "-m", "pytest",
        "tests/performance/",
        "-v",
        "--tb=short",
        "-m performance"
    ]
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30
        )
        
        duration = time.time() - start_time
        success = result.returncode == 0
        
        if success:
            print(f"✅ Performance benchmarks passed in {duration:.2f}s")
        else:
            print(f"❌ Performance benchmarks failed")
            print("   STDOUT:", result.stdout[-500:])
            print("   STDERR:", result.stderr[-500:])
        
        return success, duration
        
    except subprocess.TimeoutExpired:
        duration = time.time() - start_time
        print(f"❌ Performance benchmarks timed out after {duration:.2f}s")
        return False, duration


def main():
    """Main quality gate runner"""
    print("🚀 Starting Quality Gates")
    print("=" * 50)
    
    gates = load_quality_gates()
    overall_start = time.time()
    
    # Track results
    results = {}
    
    # Run unit tests
    success, duration, coverage = run_tests("unit", gates["unit_tests"]["max_duration_seconds"])
    results["unit"] = {
        "success": success,
        "duration": duration,
        "coverage": coverage,
        "coverage_ok": coverage >= gates["unit_tests"]["min_coverage_percent"]
    }
    
    if not success or not results["unit"]["coverage_ok"]:
        print(f"❌ Unit tests failed quality gates")
        if not success:
            print(f"   Duration: {duration:.2f}s > {gates['unit_tests']['max_duration_seconds']}s")
        if not results["unit"]["coverage_ok"]:
            print(f"   Coverage: {coverage:.1f}% < {gates['unit_tests']['min_coverage_percent']}%")
        sys.exit(1)
    
    # Run integration tests
    success, duration, coverage = run_tests("integration", gates["integration_tests"]["max_duration_seconds"])
    results["integration"] = {
        "success": success,
        "duration": duration,
        "coverage": coverage,
        "coverage_ok": coverage >= gates["integration_tests"]["min_coverage_percent"]
    }
    
    if not success or not results["integration"]["coverage_ok"]:
        print(f"❌ Integration tests failed quality gates")
        if not success:
            print(f"   Duration: {duration:.2f}s > {gates['integration_tests']['max_duration_seconds']}s")
        if not results["integration"]["coverage_ok"]:
            print(f"   Coverage: {coverage:.1f}% < {gates['integration_tests']['min_coverage_percent']}%")
        sys.exit(1)
    
    # Run performance benchmarks
    success, duration = run_performance_benchmarks()
    results["performance"] = {
        "success": success,
        "duration": duration
    }
    
    if not success:
        print(f"❌ Performance benchmarks failed")
        sys.exit(1)
    
    # Check total execution time
    total_duration = time.time() - overall_start
    
    print("\n" + "=" * 50)
    print("📊 Quality Gate Summary")
    print("=" * 50)
    print(f"Unit Tests:     {results['unit']['duration']:.2f}s (coverage: {results['unit']['coverage']:.1f}%)")
    print(f"Integration:    {results['integration']['duration']:.2f}s (coverage: {results['integration']['coverage']:.1f}%)")
    print(f"Performance:    {results['performance']['duration']:.2f}s")
    print(f"Total Time:     {total_duration:.2f}s")
    print(f"Max Allowed:    {gates['total_execution']['max_seconds']}s")
    
    if total_duration > gates["total_execution"]["max_seconds"]:
        print(f"❌ Total execution time {total_duration:.2f}s exceeded limit {gates['total_execution']['max_seconds']}s")
        sys.exit(1)
    
    print("\n✅ All quality gates passed!")
    print("🚀 Ready for deployment!")


if __name__ == "__main__":
    main()
