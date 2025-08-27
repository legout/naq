#!/usr/bin/env python3
"""
Simple script to establish test coverage baseline.
"""

import json
import subprocess
import sys
from pathlib import Path
from datetime import datetime


def run_simple_coverage():
    """Run a simple coverage measurement on a subset of tests."""
    try:
        # Run pytest with coverage on a small subset of tests
        result = subprocess.run(
            ["uv", "run", "pytest", "tests/test_services/test_unit_worker.py", "-v", "--cov=src", "--cov-report=term-missing"],
            capture_output=True,
            text=True,
            cwd=Path.cwd()
        )
        
        # Extract coverage percentage from output
        coverage_percent = 0
        for line in result.stdout.split('\n'):
            if 'TOTAL' in line and 'coverage:' in line.lower():
                # Extract percentage from line like "TOTAL 100 50 50%"
                parts = line.split()
                if len(parts) >= 4:
                    try:
                        coverage_percent = float(parts[-1].replace('%', ''))
                    except ValueError:
                        pass
        
        return {
            "success": True,
            "coverage_percent": coverage_percent,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "exit_code": result.returncode,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


def save_baseline(data):
    """Save baseline data to file."""
    baseline_file = "coverage_baseline.json"
    try:
        with open(baseline_file, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"Baseline saved to {baseline_file}")
        return True
    except Exception as e:
        print(f"Error saving baseline: {e}")
        return False


def main():
    """Main function."""
    print("Running simple coverage baseline measurement...")
    
    # Run coverage measurement
    result = run_simple_coverage()
    
    if not result["success"]:
        print("Error running coverage measurement:")
        print(result.get("error", "Unknown error"))
        sys.exit(1)
    
    # Print results
    print(f"\nCoverage Results:")
    print(f"  Coverage: {result['coverage_percent']:.2f}%")
    print(f"  Exit Code: {result['exit_code']}")
    
    # Save baseline
    if save_baseline(result):
        print("Baseline established successfully!")
    else:
        print("Failed to save baseline")
        sys.exit(1)
    
    # Exit with appropriate code
    if result['coverage_percent'] < 50:  # Below minimum threshold
        print("Warning: Coverage below minimum threshold of 50%")
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()