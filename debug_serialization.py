#!/usr/bin/env python3

from naq.models.jobs import Job
from naq.models.enums import JOB_STATUS
from naq.settings import JOB_SERIALIZER

def sample_task():
    return "test_result"

print(f"JOB_SERIALIZER setting: {JOB_SERIALIZER}")

# Create a job
job = Job(sample_task)

# Test serialization
print("Testing serialization...")
success_data = job.serialize_result(
    "test_result",
    JOB_STATUS.COMPLETED,
)

print(f"Serialized data type: {type(success_data)}")
print(f"Serialized data length: {len(success_data)}")

# Test deserialization
print("Testing deserialization...")
success_result = job.deserialize_result(success_data)

print(f"Deserialized result: {success_result}")
print(f"Status type: {type(success_result['status'])}")
print(f"Status value: {success_result['status']}")
print(f"Expected value: {JOB_STATUS.COMPLETED.value}")
print(f"Are they equal? {success_result['status'] == JOB_STATUS.COMPLETED.value}")