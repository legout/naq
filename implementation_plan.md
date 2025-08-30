# Implementation Plan for Serialization Module Enhancements

## Overview
This document outlines the implementation plan for completing all remaining tasks in the serialization module, starting with making debug logging in PickleSerializer configurable.

## Task Breakdown

### Phase 1: Configurable Debug Logging (Tasks 10-11)

#### Task 10: Add configurable debug logging settings to settings.py

**Objective**: Add configuration settings to control debug logging behavior in PickleSerializer.

**Implementation Steps**:
1. Add new configuration variables to `src/naq/settings.py`:
   ```python
   # Debug logging configuration for PickleSerializer
   PICKLE_DEBUG_LOGGING_ENABLED = _get_env_or_config(
       "NAQ_PICKLE_DEBUG_LOGGING_ENABLED", 
       ["serialization", "pickle_debug_logging_enabled"], 
       "False"
   )
   
   PICKLE_DEBUG_LOGGING_LEVEL = _get_env_or_config(
       "NAQ_PICKLE_DEBUG_LOGGING_LEVEL", 
       ["serialization", "pickle_debug_logging_level"], 
       "DEBUG"
   ).upper()
   
   PICKLE_DEBUG_LOGGING_INCLUDE_OBJECTS = _get_env_or_config(
       "NAQ_PICKLE_DEBUG_LOGGING_INCLUDE_OBJECTS", 
       ["serialization", "pickle_debug_logging_include_objects"], 
       "True"
   )
   ```

2. Convert string boolean values to actual booleans:
   ```python
   PICKLE_DEBUG_LOGGING_ENABLED = (
       PICKLE_DEBUG_LOGGING_ENABLED.lower() == "true"
       if isinstance(PICKLE_DEBUG_LOGGING_ENABLED, str)
       else PICKLE_DEBUG_LOGGING_ENABLED
   )
   
   PICKLE_DEBUG_LOGGING_INCLUDE_OBJECTS = (
       PICKLE_DEBUG_LOGGING_INCLUDE_OBJECTS.lower() == "true"
       if isinstance(PICKLE_DEBUG_LOGGING_INCLUDE_OBJECTS, str)
       else PICKLE_DEBUG_LOGGING_INCLUDE_OBJECTS
   )
   ```

3. Add these settings to the `Config` class:
   ```python
   # In Config.__init__
   self.pickle_debug_logging_enabled = pickle_debug_logging_enabled or PICKLE_DEBUG_LOGGING_ENABLED
   self.pickle_debug_logging_level = pickle_debug_logging_level or PICKLE_DEBUG_LOGGING_LEVEL
   self.pickle_debug_logging_include_objects = pickle_debug_logging_include_objects or PICKLE_DEBUG_LOGGING_INCLUDE_OBJECTS
   ```

4. Add corresponding class methods for environment loading:
   ```python
   @classmethod
   def _get_pickle_debug_logging_enabled_config(cls) -> bool:
       """Get pickle debug logging enabled configuration from environment or config."""
       enabled = _get_env_or_config(
           "NAQ_PICKLE_DEBUG_LOGGING_ENABLED", 
           ["serialization", "pickle_debug_logging_enabled"], 
           "False"
       )
       if isinstance(enabled, str):
           return enabled.lower() == "true"
       return enabled
   
   @classmethod
   def _get_pickle_debug_logging_level_config(cls) -> str:
       """Get pickle debug logging level configuration from environment or config."""
       return _get_env_or_config(
           "NAQ_PICKLE_DEBUG_LOGGING_LEVEL", 
           ["serialization", "pickle_debug_logging_level"], 
           "DEBUG"
       ).upper()
   
   @classmethod
   def _get_pickle_debug_logging_include_objects_config(cls) -> bool:
       """Get pickle debug logging include objects configuration from environment or config."""
       include_objects = _get_env_or_config(
           "NAQ_PICKLE_DEBUG_LOGGING_INCLUDE_OBJECTS", 
           ["serialization", "pickle_debug_logging_include_objects"], 
           "True"
       )
       if isinstance(include_objects, str):
           return include_objects.lower() == "true"
       return include_objects
   ```

#### Task 11: Modify PickleSerializer._log_serialization_debug_info() to use configuration settings

**Objective**: Update the debug logging function to respect the configuration settings.

**Implementation Steps**:
1. Import the new settings in `src/naq/serializers.py`:
   ```python
   from .settings import (
       # ... existing imports ...
       PICKLE_DEBUG_LOGGING_ENABLED,
       PICKLE_DEBUG_LOGGING_LEVEL,
       PICKLE_DEBUG_LOGGING_INCLUDE_OBJECTS,
   )
   ```

2. Modify the `_log_serialization_debug_info` method:
   ```python
   @staticmethod
   def _log_serialization_debug_info(job: Job, error: Exception) -> None:
       """Log detailed debug information for serialization failures."""
       # Check if debug logging is enabled
       if not PICKLE_DEBUG_LOGGING_ENABLED:
           return
       
       logger = loguru.logger.bind(job_id=job.job_id)
       
       # Use configured log level
       log_method = getattr(logger, PICKLE_DEBUG_LOGGING_LEVEL.lower(), logger.debug)
       
       log_method("=== DEBUG: Job kwargs analysis ===")
       log_method(f"Job kwargs keys: {list(job.kwargs.keys())}")
       log_method(
           f"Job kwargs types: {[(k, type(v).__name__) for k, v in job.kwargs.items()]}"
       )
       
       # Check for unpicklable objects if configured to include them
       if PICKLE_DEBUG_LOGGING_INCLUDE_OBJECTS:
           unpicklable_objects = PickleSerializer._find_unpicklable_objects(job)
           if unpicklable_objects:
               log_method("Found unpicklable objects in job kwargs", unpicklable_objects=unpicklable_objects)
           
           # Check for asyncio.Task objects
           task_objects = PickleSerializer._find_asyncio_tasks(job)
           if task_objects:
               log_method("Found asyncio.Task objects in job kwargs", task_objects=task_objects)
       
       log_method("=== END DEBUG: Job kwargs analysis ===")
   ```

### Phase 2: Data Validation (Tasks 12, 17)

#### Task 12: Add validation for serialized data

**Objective**: Implement validation mechanisms to ensure serialized data integrity.

**Implementation Steps**:
1. Create a new validation module `src/naq/utils/validation.py`:
   ```python
   from typing import Any, Dict, Union
   import hashlib
   import json
   from .exceptions import SerializationError
   
   def validate_serialized_data(data: bytes, serializer_type: str = "pickle") -> bool:
       """
       Validate serialized data integrity.
       
       Args:
           data: The serialized data to validate
           serializer_type: Type of serializer ("pickle" or "json")
           
       Returns:
           bool: True if data is valid, False otherwise
       """
       if not data or len(data) == 0:
           return False
       
       try:
           if serializer_type == "pickle":
               # Basic validation for pickle data
               import cloudpickle
               cloudpickle.loads(data)  # Try to deserialize to validate
               return True
           elif serializer_type == "json":
               # Basic validation for JSON data
               json.loads(data.decode('utf-8'))
               return True
       except Exception:
           return False
       
       return False
   
   def validate_job_payload(payload: Dict[str, Any]) -> bool:
       """
       Validate job payload structure.
       
       Args:
           payload: The job payload to validate
           
       Returns:
           bool: True if payload is valid, False otherwise
       """
       required_fields = {"job_id", "function", "args", "kwargs"}
       return all(field in payload for field in required_fields)
   ```

2. Update serializers to use validation:
   ```python
   # In PickleSerializer
   @staticmethod
   def deserialize_job(data: bytes) -> Job:
       # Validate data before deserialization
       if not validate_serialized_data(data, "pickle"):
           raise SerializationError("Invalid pickle data: failed validation")
       
       # ... existing deserialization code ...
   
   # In JsonSerializer
   @staticmethod
   def deserialize_job(data: bytes) -> Job:
       # Validate data before deserialization
       if not validate_serialized_data(data, "json"):
           raise SerializationError("Invalid JSON data: failed validation")
       
       # ... existing deserialization code ...
   ```

#### Task 17: Add input validation for deserialized data

**Objective**: Validate deserialized data before use.

**Implementation Steps**:
1. Add validation functions to `src/naq/utils/validation.py`:
   ```python
   def validate_deserialized_job(job: Job) -> bool:
       """
       Validate deserialized job object.
       
       Args:
           job: The deserialized job to validate
           
       Returns:
           bool: True if job is valid, False otherwise
       """
       if not job.job_id:
           return False
       
       if not callable(job.function):
           return False
       
       if not isinstance(job.args, tuple):
           return False
       
       if not isinstance(job.kwargs, dict):
           return False
       
       return True
   
   def validate_deserialized_result(result: Dict[str, Any]) -> bool:
       """
       Validate deserialized result dictionary.
       
       Args:
           result: The deserialized result to validate
           
       Returns:
           bool: True if result is valid, False otherwise
       """
       required_fields = {"status"}
       return all(field in result for field in required_fields)
   ```

2. Update serializers to use deserialized data validation:
   ```python
   # In both PickleSerializer and JsonSerializer
   @staticmethod
   def deserialize_job(data: bytes) -> Job:
       # ... existing deserialization code ...
       
       # Validate deserialized job
       if not validate_deserialized_job(job):
           raise SerializationError("Invalid job: failed validation")
       
       return job
   
   @staticmethod
   def deserialize_result(data: bytes) -> Dict[str, Any]:
       # ... existing deserialization code ...
       
       # Validate deserialized result
       if not validate_deserialized_result(result):
           raise SerializationError("Invalid result: failed validation")
       
       return result
   ```

### Phase 3: Security Enhancements (Tasks 15-16)

#### Task 15: Implement size limits for serialized data

**Objective**: Add configurable size limits to prevent memory exhaustion attacks.

**Implementation Steps**:
1. Add size limit configuration to `src/naq/settings.py`:
   ```python
   # Serialized data size limits (in bytes)
   PICKLE_MAX_SERIALIZED_SIZE = int(
       _get_env_or_config(
           "NAQ_PICKLE_MAX_SERIALIZED_SIZE", 
           ["serialization", "pickle_max_size"], 
           "10485760"  # 10MB default
       )
   )
   
   JSON_MAX_SERIALIZED_SIZE = int(
       _get_env_or_config(
           "NAQ_JSON_MAX_SERIALIZED_SIZE", 
           ["serialization", "json_max_size"], 
           "5242880"  # 5MB default
       )
   )
   ```

2. Add size validation to `src/naq/utils/validation.py`:
   ```python
   def validate_serialized_data_size(data: bytes, max_size: int) -> bool:
       """
       Validate serialized data size.
       
       Args:
           data: The serialized data to validate
           max_size: Maximum allowed size in bytes
           
       Returns:
           bool: True if data size is within limits, False otherwise
       """
       return len(data) <= max_size
   ```

3. Update serializers to check size limits:
   ```python
   # In PickleSerializer
   @staticmethod
   def deserialize_job(data: bytes) -> Job:
       # Check size limit
       if not validate_serialized_data_size(data, PICKLE_MAX_SERIALIZED_SIZE):
           raise SerializationError(f"Pickle data exceeds maximum size limit of {PICKLE_MAX_SERIALIZED_SIZE} bytes")
       
       # ... existing validation and deserialization code ...
   
   # In JsonSerializer
   @staticmethod
   def deserialize_job(data: bytes) -> Job:
       # Check size limit
       if not validate_serialized_data_size(data, JSON_MAX_SERIALIZED_SIZE):
           raise SerializationError(f"JSON data exceeds maximum size limit of {JSON_MAX_SERIALIZED_SIZE} bytes")
       
       # ... existing validation and deserialization code ...
   ```

#### Task 16: Consider adding a signature or checksum for serialized data

**Objective**: Implement data integrity verification through cryptographic signatures.

**Implementation Steps**:
1. Add signature configuration to `src/naq/settings.py`:
   ```python
   # Data integrity verification
   SERIALIZATION_USE_CHECKSUM = _get_env_or_config(
       "NAQ_SERIALIZATION_USE_CHECKSUM", 
       ["serialization", "use_checksum"], 
       "False"
   )
   
   SERIALIZATION_CHECKSUM_ALGORITHM = _get_env_or_config(
       "NAQ_SERIALIZATION_CHECKSUM_ALGORITHM", 
       ["serialization", "checksum_algorithm"], 
       "sha256"
   )
   
   SERIALIZATION_USE_SIGNATURE = _get_env_or_config(
       "NAQ_SERIALIZATION_USE_SIGNATURE", 
       ["serialization", "use_signature"], 
       "False"
   )
   
   SERIALIZATION_SIGNATURE_KEY = _get_env_or_config(
       "NAQ_SERIALIZATION_SIGNATURE_KEY", 
       ["serialization", "signature_key"], 
       ""
   )
   ```

2. Convert boolean values:
   ```python
   SERIALIZATION_USE_CHECKSUM = (
       SERIALIZATION_USE_CHECKSUM.lower() == "true"
       if isinstance(SERIALIZATION_USE_CHECKSUM, str)
       else SERIALIZATION_USE_CHECKSUM
   )
   
   SERIALIZATION_USE_SIGNATURE = (
       SERIALIZATION_USE_SIGNATURE.lower() == "true"
       if isinstance(SERIALIZATION_USE_SIGNATURE, str)
       else SERIALIZATION_USE_SIGNATURE
   )
   ```

3. Add checksum/signature functions to `src/naq/utils/validation.py`:
   ```python
   def add_checksum(data: bytes, algorithm: str = "sha256") -> bytes:
       """
       Add checksum to serialized data.
       
       Args:
           data: The serialized data
           algorithm: Hash algorithm to use
           
       Returns:
           bytes: Data with checksum appended
       """
       checksum = hashlib.new(algorithm, data).digest()
       return data + b":" + checksum
   
   def verify_checksum(data_with_checksum: bytes, algorithm: str = "sha256") -> bytes:
       """
       Verify checksum and return original data.
       
       Args:
           data_with_checksum: Data with checksum appended
           algorithm: Hash algorithm used
           
       Returns:
           bytes: Original data if checksum is valid
           
       Raises:
           SerializationError: If checksum is invalid
       """
       if b":" not in data_with_checksum:
           raise SerializationError("Invalid checksum format")
       
       data, checksum = data_with_checksum.rsplit(b":", 1)
       computed_checksum = hashlib.new(algorithm, data).digest()
       
       if computed_checksum != checksum:
           raise SerializationError("Checksum verification failed")
       
       return data
   
   def add_signature(data: bytes, key: str) -> bytes:
       """
       Add signature to serialized data.
       
       Args:
           data: The serialized data
           key: Signature key
           
       Returns:
           bytes: Data with signature appended
       """
       # This is a simplified example - in production, use proper cryptographic signing
       import hmac
       signature = hmac.new(key.encode(), data, hashlib.sha256).digest()
       return data + b":" + signature
   
   def verify_signature(data_with_signature: bytes, key: str) -> bytes:
       """
       Verify signature and return original data.
       
       Args:
           data_with_signature: Data with signature appended
           key: Signature key
           
       Returns:
           bytes: Original data if signature is valid
           
       Raises:
           SerializationError: If signature is invalid
       """
       if b":" not in data_with_signature:
           raise SerializationError("Invalid signature format")
       
       data, signature = data_with_signature.rsplit(b":", 1)
       
       # This is a simplified example - in production, use proper cryptographic verification
       import hmac
       computed_signature = hmac.new(key.encode(), data, hashlib.sha256).digest()
       
       if computed_signature != signature:
           raise SerializationError("Signature verification failed")
       
       return data
   ```

4. Update serializers to use checksums/signatures:
   ```python
   # In both PickleSerializer and JsonSerializer
   @staticmethod
   def serialize_job(job: Job) -> bytes:
       # ... existing serialization code ...
       
       # Add checksum if enabled
       if SERIALIZATION_USE_CHECKSUM:
           data = add_checksum(data, SERIALIZATION_CHECKSUM_ALGORITHM)
       
       # Add signature if enabled
       if SERIALIZATION_USE_SIGNATURE and SERIALIZATION_SIGNATURE_KEY:
           data = add_signature(data, SERIALIZATION_SIGNATURE_KEY)
       
       return data
   
   @staticmethod
   def deserialize_job(data: bytes) -> Job:
       # Verify signature if enabled
       if SERIALIZATION_USE_SIGNATURE and SERIALIZATION_SIGNATURE_KEY:
           data = verify_signature(data, SERIALIZATION_SIGNATURE_KEY)
       
       # Verify checksum if enabled
       if SERIALIZATION_USE_CHECKSUM:
           data = verify_checksum(data, SERIALIZATION_CHECKSUM_ALGORITHM)
       
       # ... existing validation and deserialization code ...
   ```

### Phase 4: Testing and Code Quality (Tasks 13-14)

#### Task 13: Add unit tests for edge cases in serialization/deserialization

**Objective**: Create comprehensive test suite covering edge cases.

**Implementation Steps**:
1. Create test file `tests/test_serializers_edge_cases.py`:
   ```python
   import pytest
   from src.naq.serializers import PickleSerializer, JsonSerializer
   from src.naq.models.jobs import Job
   from src.naq.exceptions import SerializationError
   import asyncio
   import sys
   
   class TestPickleSerializerEdgeCases:
       """Test edge cases for PickleSerializer."""
       
       def test_large_job_serialization(self):
           """Test serialization of jobs with large data."""
           large_data = {"x": "y" * 1000000}  # 1MB string
           job = Job(lambda: None, (), large_data)
           serialized = PickleSerializer.serialize_job(job)
           deserialized = PickleSerializer.deserialize_job(serialized)
           assert deserialized.kwargs == large_data
       
       def test_unpicklable_object_handling(self):
           """Test handling of unpicklable objects."""
           def test_func():
               pass
           
           # Create job with unpicklable object
           job = Job(test_func, (), {"file": open(__file__, "r")})
           with pytest.raises(SerializationError):
               PickleSerializer.serialize_job(job)
       
       def test_asyncio_task_handling(self):
           """Test handling of asyncio.Task objects."""
           async def async_func():
               pass
           
           task = asyncio.create_task(async_func())
           job = Job(lambda: None, (), {"task": task})
           with pytest.raises(SerializationError):
               PickleSerializer.serialize_job(job)
       
       def test_debug_logging_configuration(self):
           """Test that debug logging respects configuration."""
           # This would require mocking the configuration
           pass
   
   class TestJsonSerializerEdgeCases:
       """Test edge cases for JsonSerializer."""
       
       def test_non_importable_function(self):
           """Test handling of non-importable functions."""
           def local_func():
               pass
           
           job = Job(local_func, (), {})
           with pytest.raises(SerializationError):
               JsonSerializer.serialize_job(job)
       
       def test_complex_data_types(self):
           """Test handling of complex data types."""
           def test_func():
               pass
           
           # Test with dataclass
           from dataclasses import dataclass
           @dataclass
           class TestData:
               value: str
           
           job = Job(test_func, (), {"data": TestData("test")})
           serialized = JsonSerializer.serialize_job(job)
           deserialized = JsonSerializer.deserialize_job(serialized)
           assert deserialized.kwargs["data"]["value"] == "test"
       
       def test_exception_serialization(self):
           """Test serialization of exception classes."""
           def test_func():
               pass
           
           job = Job(test_func, (), {}, retry_on=(ValueError, TypeError))
           serialized = JsonSerializer.serialize_job(job)
           deserialized = JsonSerializer.deserialize_job(serialized)
           assert ValueError in deserialized.retry_on
           assert TypeError in deserialized.retry_on
   
   class TestDataValidation:
       """Test data validation functionality."""
       
       def test_pickle_data_validation(self):
           """Test validation of pickle data."""
           # Valid data
           job = Job(lambda: None, (), {})
           serialized = PickleSerializer.serialize_job(job)
           assert validate_serialized_data(serialized, "pickle")
           
           # Invalid data
           assert not validate_serialized_data(b"invalid", "pickle")
       
       def test_json_data_validation(self):
           """Test validation of JSON data."""
           # Valid data
           job = Job(lambda: None, (), {})
           serialized = JsonSerializer.serialize_job(job)
           assert validate_serialized_data(serialized, "json")
           
           # Invalid data
           assert not validate_serialized_data(b"invalid", "json")
       
       def test_size_limits(self):
           """Test size limit validation."""
           # Create data that exceeds size limit
           large_data = {"x": "y" * 10000000}  # 10MB string
           job = Job(lambda: None, (), large_data)
           
           # This should fail if size limits are properly configured
           with pytest.raises(SerializationError, match="exceeds maximum size limit"):
               JsonSerializer.serialize_job(job)
   ```

#### Task 14: Review and refactor module structure to resolve circular import dependencies

**Objective**: Analyze current import structure and refactor if needed to eliminate circular dependencies.

**Implementation Steps**:
1. Analyze current import dependencies:
   ```bash
   # Use a tool like pydeps or manually inspect imports
   pip install pydeps
   pydeps src/naq/serializers.py --show-deps
   ```

2. Identify any circular dependencies and refactor:
   - Move shared utilities to separate modules
   - Use lazy imports where necessary
   - Reorganize module structure if needed

3. Example refactoring if circular dependencies are found:
   ```python
   # Before: circular import between serializers and models
   # src/naq/serializers.py imports from src/naq/models.jobs
   # src/naq/models.jobs imports from src/naq.serializers
   
   # After: move shared code to utils module
   # src/naq/utils/serialization_helpers.py
   from typing import Any, Dict, Optional, Tuple
   
   def normalize_retry_strategy(retry_strategy: Any) -> str:
       """Normalize retry_strategy to a simple string value."""
       if retry_strategy is None:
           return "linear"
       if hasattr(retry_strategy, "value"):
           return retry_strategy.value
       return str(retry_strategy)
   ```

## Implementation Order

1. **Phase 1**: Configurable Debug Logging (Tasks 10-11)
2. **Phase 2**: Data Validation (Tasks 12, 17)
3. **Phase 3**: Security Enhancements (Tasks 15-16)
4. **Phase 4**: Testing and Code Quality (Tasks 13-14)

## Testing Strategy

1. Unit tests for each new feature
2. Integration tests for serializer workflows
3. Performance tests to ensure new features don't significantly impact performance
4. Security tests for validation and checksum/signature features

## Configuration Guide

Add documentation to `docs/api/serialization.md`:
```markdown
# Serialization Configuration

## Debug Logging

The PickleSerializer supports configurable debug logging to help troubleshoot serialization issues.

### Environment Variables

- `NAQ_PICKLE_DEBUG_LOGGING_ENABLED`: Enable/disable debug logging (default: False)
- `NAQ_PICKLE_DEBUG_LOGGING_LEVEL`: Log level for debug messages (default: DEBUG)
- `NAQ_PICKLE_DEBUG_LOGGING_INCLUDE_OBJECTS`: Include object analysis in logs (default: True)

### Example Configuration

```bash
export NAQ_PICKLE_DEBUG_LOGGING_ENABLED=true
export NAQ_PICKLE_DEBUG_LOGGING_LEVEL=DEBUG
export NAQ_PICKLE_DEBUG_LOGGING_INCLUDE_OBJECTS=true
```

## Data Validation

### Size Limits

- `NAQ_PICKLE_MAX_SERIALIZED_SIZE`: Maximum size for pickle data (default: 10MB)
- `NAQ_JSON_MAX_SERIALIZED_SIZE`: Maximum size for JSON data (default: 5MB)

### Data Integrity

- `NAQ_SERIALIZATION_USE_CHECKSUM`: Enable checksum verification (default: False)
- `NAQ_SERIALIZATION_CHECKSUM_ALGORITHM`: Hash algorithm for checksums (default: sha256)
- `NAQ_SERIALIZATION_USE_SIGNATURE`: Enable signature verification (default: False)
- `NAQ_SERIALIZATION_SIGNATURE_KEY`: Key for signature verification (default: "")
```

## Security Considerations

1. **Checksum vs Signature**: Use checksums for basic integrity verification, signatures for tamper-proof verification
2. **Size Limits**: Configure based on your application's requirements and security needs
3. **Debug Logging**: Disable debug logging in production environments to avoid leaking sensitive information
4. **Signature Keys**: Store signature keys securely, never in code or public configuration