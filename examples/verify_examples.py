#!/usr/bin/env python3
"""
Script to verify that all example files are syntactically correct.
This script attempts to import each Python file in the examples directory
to check for syntax errors and basic import issues.
"""

import os
import sys
import importlib.util
from pathlib import Path

def verify_python_file(file_path):
    """Try to import a Python file to check for syntax errors."""
    try:
        spec = importlib.util.spec_from_file_location("example_module", file_path)
        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)
            # Add to sys.modules to handle relative imports
            sys.modules["example_module"] = module
            spec.loader.exec_module(module)
            return True, None
    except Exception as e:
        return False, str(e)
    return False, "Failed to load module"

def main():
    """Main function to verify all example files."""
    examples_dir = Path(__file__).parent
    python_files = list(examples_dir.rglob("*.py"))
    
    # Filter out this verification script itself
    python_files = [f for f in python_files if f.name != "verify_examples.py" and f.name != "test_sync_client.py"]
    
    print(f"Verifying {len(python_files)} Python example files...\n")
    
    failed_files = []
    
    for file_path in python_files:
        relative_path = file_path.relative_to(examples_dir)
        print(f"Checking {relative_path}... ", end="")
        
        success, error = verify_python_file(file_path)
        if success:
            print("OK")
        else:
            print("FAILED")
            print(f"  Error: {error}")
            failed_files.append((relative_path, error))
    
    print(f"\nVerification complete: {len(python_files) - len(failed_files)}/{len(python_files)} files OK")
    
    if failed_files:
        print("\nFailed files:")
        for file_path, error in failed_files:
            print(f"  {file_path}: {error}")
        return 1
    else:
        print("All example files verified successfully!")
        return 0

if __name__ == "__main__":
    sys.exit(main())