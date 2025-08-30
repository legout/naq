#!/usr/bin/env python3
"""
Test script to verify size validation for serialized data.
"""
import os

# Set the environment variable before importing anything
os.environ['NAQ_SERIALIZATION_MAX_SIZE_BYTES'] = '100'  # Set a very small size limit

from src.naq.serializers import JsonSerializer, PickleSerializer
from src.naq.models.jobs import Job

# Test function
def test_func(x, y):
    return x + y

# Create a simple job
job = Job(function=test_func, args=(1, 2), kwargs={})
job.job_id = 'test-job-id'

print('Testing JsonSerializer with size limit...')
try:
    serialized = JsonSerializer.serialize_job(job)
    print('JsonSerializer: Unexpected success - data should have been too large')
    print(f'Serialized size: {len(serialized)} bytes')
except Exception as e:
    print('JsonSerializer: Expected error -', e)

print()
print('Testing PickleSerializer with size limit...')
try:
    serialized = PickleSerializer.serialize_job(job)
    print('PickleSerializer: Unexpected success - data should have been too large')
    print(f'Serialized size: {len(serialized)} bytes')
except Exception as e:
    print('PickleSerializer: Expected error -', e)