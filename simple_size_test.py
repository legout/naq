#!/usr/bin/env python3
"""
Simple test script to verify size validation functionality.
"""
import os
import sys

# Set environment variables BEFORE importing any modules
os.environ['NAQ_SERIALIZATION_MAX_SIZE_BYTES'] = '50'  # Very small limit

# Add the src directory to the path
sys.path.insert(0, 'src')

from naq.serializers import _validate_serialized_data_size

print("Testing size validation with 50 byte limit...")

# Test with data that's too large
try:
    _validate_serialized_data_size(b'x' * 100, 'test data')
    print("❌ FAILED: Expected error but validation passed")
except Exception as e:
    print(f"✅ SUCCESS: Got expected error - {e}")

# Test with data that's within the limit
try:
    _validate_serialized_data_size(b'x' * 30, 'test data')
    print("✅ SUCCESS: Small data passed validation")
except Exception as e:
    print(f"❌ FAILED: Unexpected error for small data - {e}")

# Test with disabled limit
os.environ['NAQ_SERIALIZATION_MAX_SIZE_BYTES'] = '0'
import importlib
import naq.settings
import naq.serializers
importlib.reload(naq.settings)
importlib.reload(naq.serializers)
# Get the reloaded function
_validate_serialized_data_size = naq.serializers._validate_serialized_data_size

try:
    _validate_serialized_data_size(b'x' * 1000000, 'test data')
    print("✅ SUCCESS: Large data passed with disabled limit")
except Exception as e:
    print(f"❌ FAILED: Unexpected error with disabled limit - {e}")

print("\nSize validation test completed!")