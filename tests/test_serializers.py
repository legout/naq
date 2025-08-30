"""
Tests for the serializers module.

This module contains comprehensive tests for both PickleSerializer and JsonSerializer,
including edge cases, validation tests, and security-related tests.
"""

import asyncio
import json
import pytest
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import patch, MagicMock

from naq.exceptions import SerializationError
from naq.models.enums import JOB_STATUS
from naq.models.jobs import Job
from naq.serializers import (
    PickleSerializer,
    JsonSerializer,
    get_serializer,
    _normalize_retry_strategy,
)


class TestNormalizeRetryStrategy:
    """Test cases for _normalize_retry_strategy function."""

    def test_with_none(self) -> None:
        """Test with None input."""
        result = _normalize_retry_strategy(None)
        assert result == "linear"

    def test_with_enum_value(self) -> None:
        """Test with enum that has value attribute."""
        class MockEnum:
            value = "exponential"

        result = _normalize_retry_strategy(MockEnum())
        assert result == "exponential"

    def test_with_string(self) -> None:
        """Test with string input."""
        result = _normalize_retry_strategy("custom")
        assert result == "custom"

    def test_with_integer(self) -> None:
        """Test with integer input."""
        result = _normalize_retry_strategy(123)
        assert result == "123"


class TestPickleSerializer:
    """Test cases for PickleSerializer."""

    def test_serialize_job_basic(self) -> None:
        """Test basic job serialization."""
        def test_func(x: int) -> int:
            return x * 2

        job = Job(
            function=test_func,
            args=(5,),
            kwargs={"multiplier": 3},
            job_id="test-job-1",
        )

        serialized = PickleSerializer.serialize_job(job)
        assert isinstance(serialized, bytes)

        # Test deserialization
        deserialized = PickleSerializer.deserialize_job(serialized)
        assert deserialized.job_id == job.job_id
        assert deserialized.args == job.args
        assert deserialized.kwargs == job.kwargs
        assert deserialized.function(5) == 10  # Function should work

    def test_serialize_job_with_all_fields(self) -> None:
        """Test job serialization with all fields."""
        def test_func() -> str:
            return "test"

        job = Job(
            function=test_func,
            args=(),
            kwargs={},
            job_id="test-job-2",
            max_retries=3,
            retry_delay=60,
            queue_name="test_queue",
            depends_on=["job1", "job2"],
            result_ttl=3600,
            timeout=300,
        )

        serialized = PickleSerializer.serialize_job(job)
        deserialized = PickleSerializer.deserialize_job(serialized)

        assert deserialized.job_id == job.job_id
        assert deserialized.max_retries == job.max_retries
        assert deserialized.retry_delay == job.retry_delay
        assert deserialized.queue_name == job.queue_name
        assert deserialized.depends_on == job.depends_on
        assert deserialized.result_ttl == job.result_ttl
        assert deserialized.timeout == job.timeout

    def test_serialize_job_validation_missing_fields(self) -> None:
        """Test that validation catches missing required fields."""
        # Create an invalid payload directly
        invalid_payload = {"job_id": "test"}  # Missing function, args, kwargs
        
        with pytest.raises(SerializationError, match="Missing required field in job payload"):
            PickleSerializer._validate_job_payload(invalid_payload)

    def test_serialize_job_validation_invalid_types(self) -> None:
        """Test that validation catches invalid field types."""
        # Test invalid job_id type
        invalid_payload = {
            "job_id": 123,  # Should be string
            "function": b"test",
            "args": b"test",
            "kwargs": b"test",
        }
        
        with pytest.raises(SerializationError, match="job_id must be a string"):
            PickleSerializer._validate_job_payload(invalid_payload)

        # Test invalid function type
        invalid_payload["job_id"] = "test"
        invalid_payload["function"] = "not_bytes"  # Should be bytes
        
        with pytest.raises(SerializationError, match="function must be pickled bytes"):
            PickleSerializer._validate_job_payload(invalid_payload)

    def test_serialize_job_validation_negative_values(self) -> None:
        """Test that validation catches negative numeric values."""
        invalid_payload = {
            "job_id": "test",
            "function": b"test",
            "args": b"test",
            "kwargs": b"test",
            "max_retries": -1,  # Negative value
        }
        
        with pytest.raises(SerializationError, match="max_retries must be non-negative"):
            PickleSerializer._validate_job_payload(invalid_payload)

    def test_serialize_failed_job(self) -> None:
        """Test failed job serialization."""
        def test_func() -> str:
            return "test"

        job = Job(
            function=test_func,
            args=(),
            kwargs={},
            job_id="failed-job-1",
            error="Test error",
            traceback="Test traceback",
        )

        serialized = PickleSerializer.serialize_failed_job(job)
        assert isinstance(serialized, bytes)

    def test_serialize_failed_job_validation(self) -> None:
        """Test validation for failed job payload."""
        # Test missing required fields
        invalid_payload = {"job_id": "test"}  # Missing function_str, args_repr, kwargs_repr
        
        with pytest.raises(SerializationError, match="Missing required field in failed job payload"):
            PickleSerializer._validate_failed_job_payload(invalid_payload)

        # Test invalid field types
        invalid_payload = {
            "job_id": "test",
            "function_str": 123,  # Should be string
            "args_repr": "test",
            "kwargs_repr": "test",
        }
        
        with pytest.raises(SerializationError, match="function_str must be a string"):
            PickleSerializer._validate_failed_job_payload(invalid_payload)

    def test_serialize_result(self) -> None:
        """Test result serialization."""
        result = {"status": "completed", "data": [1, 2, 3]}
        
        serialized = PickleSerializer.serialize_result(
            result=result,
            status=JOB_STATUS.COMPLETED,
        )
        assert isinstance(serialized, bytes)

        # Test deserialization
        deserialized = PickleSerializer.deserialize_result(serialized)
        assert deserialized["status"] == JOB_STATUS.COMPLETED.value
        assert deserialized["result"] == result

    def test_serialize_result_with_error(self) -> None:
        """Test result serialization with error."""
        serialized = PickleSerializer.serialize_result(
            result=None,
            status=JOB_STATUS.FAILED,
            error="Test error",
            traceback_str="Test traceback",
        )
        assert isinstance(serialized, bytes)

        deserialized = PickleSerializer.deserialize_result(serialized)
        assert deserialized["status"] == JOB_STATUS.FAILED.value
        assert deserialized["error"] == "Test error"
        assert deserialized["traceback"] == "Test traceback"
        assert deserialized["result"] is None

    def test_serialize_result_validation(self) -> None:
        """Test validation for result payload."""
        # Test missing required fields
        invalid_payload = {"result": "test"}  # Missing status
        
        with pytest.raises(SerializationError, match="Missing required field in result payload"):
            PickleSerializer._validate_result_payload(invalid_payload)

        # Test invalid field types
        invalid_payload = {
            "status": 123,  # Should be string
            "result": "test",
        }
        
        with pytest.raises(SerializationError, match="status must be a string"):
            PickleSerializer._validate_result_payload(invalid_payload)

    def test_find_unpicklable_objects(self) -> None:
        """Test finding unpicklable objects in job kwargs."""
        def test_func() -> None:
            pass

        # Create an unpicklable object (socket)
        import socket
        unpicklable_obj = socket.socket()

        job = Job(
            function=test_func,
            args=(),
            kwargs={"socket": unpicklable_obj},
            job_id="test-unpicklable",
        )

        unpicklable_objects = PickleSerializer._find_unpicklable_objects(job)
        assert len(unpicklable_objects) == 1
        assert unpicklable_objects[0]["key"] == "socket"
        assert unpicklable_objects[0]["type"] == "socket"
        assert "cannot pickle" in unpicklable_objects[0]["error"].lower()

    def test_find_asyncio_tasks(self) -> None:
        """Test finding asyncio tasks in job kwargs."""
        def test_func() -> None:
            pass

        # Create an asyncio task with a running event loop
        async def create_task():
            async def async_func() -> str:
                return "test"
            return asyncio.create_task(async_func())
        
        # Run the async function to get the task
        task = asyncio.run(create_task())

        job = Job(
            function=test_func,
            args=(),
            kwargs={"task": task},
            job_id="test-task",
        )

        task_objects = PickleSerializer._find_asyncio_tasks(job)
        assert len(task_objects) == 1
        assert task_objects[0]["key"] == "task"
        assert "task_id" in task_objects[0]
        assert "task_state" in task_objects[0]
        
        # Clean up
        task.cancel()

    @patch('naq.serializers.PICKLE_DEBUG_LOGGING_ENABLED', False)
    def test_debug_logging_disabled(self) -> None:
        """Test that debug logging is disabled when configured."""
        def test_func() -> None:
            pass

        job = Job(
            function=test_func,
            args=(),
            kwargs={"bad": object()},  # This would cause logging
            job_id="test-debug",
        )

        # Should not raise an exception even with bad object
        PickleSerializer._log_serialization_debug_info(job, Exception("test"))

    @patch('naq.serializers.PICKLE_DEBUG_LOGGING_ENABLED', True)
    @patch('naq.serializers.PICKLE_DEBUG_LOGGING_INCLUDE_OBJECTS', True)
    @patch('naq.serializers.loguru.logger')
    def test_debug_logging_enabled(self, mock_logger: MagicMock) -> None:
        """Test that debug logging is enabled when configured."""
        def test_func() -> None:
            pass

        job = Job(
            function=test_func,
            args=(),
            kwargs={"bad": object()},  # This would cause logging
            job_id="test-debug",
        )

        PickleSerializer._log_serialization_debug_info(job, Exception("test"))
        
        # Verify logging was called
        assert mock_logger.bind.called
        
        # The test passes if we reach this point without exceptions
        # The actual logging behavior depends on the loguru implementation
        # which may not be fully captured by the mock


# Module-level functions for JsonSerializer tests
def json_test_func(x: int) -> int:
    """Test function for JSON serialization."""
    return x * 2


def json_dataclass_func(data) -> str:
    """Test function for dataclass serialization."""
    return f"{data.name}: {data.value}"


def json_unserializable_func() -> None:
    """Test function for unserializable data test."""
    pass


def json_qualname_test_func() -> None:
    """Test function for qualname test."""
    pass


def json_complex_func(data, config) -> dict:
    """Test function for complex data structures."""
    return {
        "result": data.count,
        "config": config,
        "items": data.items,
    }


class TestJsonSerializer:
    """Test cases for JsonSerializer."""

    def test_serialize_job_basic(self) -> None:
        """Test basic job serialization."""
        job = Job(
            function=json_test_func,
            args=(5,),
            kwargs={"multiplier": 3},
            job_id="test-job-1",
        )

        serialized = JsonSerializer.serialize_job(job)
        assert isinstance(serialized, bytes)

        # Test deserialization
        deserialized = JsonSerializer.deserialize_job(serialized)
        assert deserialized.job_id == job.job_id
        assert deserialized.args == job.args
        assert deserialized.kwargs == job.kwargs
        assert deserialized.function(5) == 10  # Function should work

    def test_serialize_job_with_dataclass(self) -> None:
        """Test job serialization with dataclass arguments."""
        @dataclass
        class TestData:
            value: int
            name: str

        job = Job(
            function=json_dataclass_func,
            args=(),
            kwargs={"data": TestData(42, "test")},
            job_id="test-dataclass",
        )

        serialized = JsonSerializer.serialize_job(job)
        deserialized = JsonSerializer.deserialize_job(serialized)

        assert deserialized.kwargs["data"]["value"] == 42
        assert deserialized.kwargs["data"]["name"] == "test"

    def test_serialize_job_validation(self) -> None:
        """Test validation for JSON job payload."""
        # Test missing required fields
        invalid_payload = {"job_id": "test"}  # Missing function, args, kwargs
        
        with pytest.raises(SerializationError, match="Missing required field in job payload"):
            JsonSerializer._validate_job_payload(invalid_payload)

        # Test invalid field types
        invalid_payload = {
            "job_id": 123,  # Should be string
            "function": "test",
            "args": [],
            "kwargs": {},
        }
        
        with pytest.raises(SerializationError, match="job_id must be a string"):
            JsonSerializer._validate_job_payload(invalid_payload)

        # Test invalid args type
        invalid_payload["job_id"] = "test"
        invalid_payload["args"] = "not_list"  # Should be list
        
        with pytest.raises(SerializationError, match="args must be a list"):
            JsonSerializer._validate_job_payload(invalid_payload)

    def test_serialize_job_with_unserializable_data(self) -> None:
        """Test that unserializable data raises appropriate error."""
        # Create an unserializable object
        unserializable_obj = object()

        job = Job(
            function=json_unserializable_func,
            args=(),
            kwargs={"bad": unserializable_obj},
            job_id="test-unserializable",
        )

        with pytest.raises(SerializationError, match="Object of type object is not JSON serializable"):
            JsonSerializer.serialize_job(job)

    def test_serialize_failed_job(self) -> None:
        """Test failed job serialization."""
        def test_func() -> str:
            return "test"

        job = Job(
            function=test_func,
            args=(),
            kwargs={},
            job_id="failed-job-1",
            error="Test error",
            traceback="Test traceback",
        )

        serialized = JsonSerializer.serialize_failed_job(job)
        assert isinstance(serialized, bytes)

    def test_serialize_failed_job_validation(self) -> None:
        """Test validation for failed job payload."""
        # Test missing required fields
        invalid_payload = {"job_id": "test"}  # Missing function_str, args_repr, kwargs_repr
        
        with pytest.raises(SerializationError, match="Missing required field in failed job payload"):
            JsonSerializer._validate_failed_job_payload(invalid_payload)

    def test_serialize_result(self) -> None:
        """Test result serialization."""
        result = {"status": "completed", "data": [1, 2, 3]}
        
        serialized = JsonSerializer.serialize_result(
            result=result,
            status=JOB_STATUS.COMPLETED,
        )
        assert isinstance(serialized, bytes)

        # Test deserialization
        deserialized = JsonSerializer.deserialize_result(serialized)
        assert deserialized["status"] == JOB_STATUS.COMPLETED.value
        assert deserialized["result"] == result

    def test_serialize_result_with_error(self) -> None:
        """Test result serialization with error."""
        serialized = JsonSerializer.serialize_result(
            result=None,
            status=JOB_STATUS.FAILED,
            error="Test error",
            traceback_str="Test traceback",
        )
        assert isinstance(serialized, bytes)

        deserialized = JsonSerializer.deserialize_result(serialized)
        assert deserialized["status"] == JOB_STATUS.FAILED.value
        assert deserialized["error"] == "Test error"
        assert deserialized["traceback"] == "Test traceback"
        assert deserialized["result"] is None

    def test_serialize_result_validation(self) -> None:
        """Test validation for result payload."""
        # Test missing required fields
        invalid_payload = {"result": "test"}  # Missing status
        
        with pytest.raises(SerializationError, match="Missing required field in result payload"):
            JsonSerializer._validate_result_payload(invalid_payload)

    def test_resolve_dotted_path(self) -> None:
        """Test resolving dotted paths."""
        # Test module:qualname format
        obj = JsonSerializer._resolve_dotted_path("json:JSONEncoder")
        assert obj is json.JSONEncoder

        # Test module.attr format (backwards compatibility)
        obj = JsonSerializer._resolve_dotted_path("json.JSONEncoder")
        assert obj is json.JSONEncoder

        # Test invalid format
        with pytest.raises(SerializationError, match="Invalid import path"):
            JsonSerializer._resolve_dotted_path("invalid")

        # Test non-existent module
        with pytest.raises(SerializationError, match="Could not import"):
            JsonSerializer._resolve_dotted_path("nonexistent:module")

    def test_qualname(self) -> None:
        """Test getting qualified name for objects."""
        # Test function
        qualname = JsonSerializer._qualname(json_qualname_test_func)
        assert "test_serializers" in qualname
        assert "json_qualname_test_func" in qualname

        # Test class
        qualname = JsonSerializer._qualname(JsonSerializer)
        assert "JsonSerializer" in qualname

        # Test object without module
        with pytest.raises(SerializationError, match="Object is not importable"):
            JsonSerializer._qualname(object())

    def test_encode_decode_exceptions(self) -> None:
        """Test encoding and decoding exception classes."""
        # Test encoding
        exc_tuple = (ValueError, TypeError, RuntimeError)
        encoded = JsonSerializer._encode_exceptions(exc_tuple)
        assert isinstance(encoded, list)
        assert len(encoded) == 3
        assert "ValueError" in str(encoded)
        assert "TypeError" in str(encoded)
        assert "RuntimeError" in str(encoded)

        # Test decoding
        decoded = JsonSerializer._decode_exceptions(encoded)
        assert decoded is not None
        assert len(decoded) == 3
        assert ValueError in decoded
        assert TypeError in decoded
        assert RuntimeError in decoded

        # Test with None
        assert JsonSerializer._encode_exceptions(None) is None
        assert JsonSerializer._decode_exceptions(None) is None

        # Test with invalid exception type
        with pytest.raises(SerializationError, match="retry_on/ignore_on must be exception classes"):
            JsonSerializer._encode_exceptions((ValueError, "not_exception"))

        # Test with non-exception class
        with pytest.raises(SerializationError, match="Imported.*is not an Exception type"):
            JsonSerializer._decode_exceptions(["builtins:str"])


class TestGetSerializer:
    """Test cases for get_serializer function."""

    @patch('naq.serializers.JOB_SERIALIZER', 'pickle')
    def test_get_pickle_serializer(self) -> None:
        """Test getting pickle serializer."""
        serializer = get_serializer()
        assert serializer is PickleSerializer

    @patch('naq.serializers.JOB_SERIALIZER', 'json')
    def test_get_json_serializer(self) -> None:
        """Test getting JSON serializer."""
        serializer = get_serializer()
        assert serializer is JsonSerializer

    @patch('naq.serializers.JOB_SERIALIZER', 'invalid')
    def test_get_invalid_serializer(self) -> None:
        """Test getting invalid serializer raises error."""
        with pytest.raises(SerializationError, match="Unknown serializer"):
            get_serializer()


class TestSerializerIntegration:
    """Integration tests for serializers."""

    def test_pickle_json_round_trip(self) -> None:
        """Test that a job serialized with pickle can be handled appropriately."""
        def test_func(x: int) -> int:
            return x * 2

        job = Job(
            function=test_func,
            args=(5,),
            kwargs={"multiplier": 3},
            job_id="round-trip-test",
        )

        # Serialize with pickle
        pickle_serialized = PickleSerializer.serialize_job(job)
        
        # Deserialize with pickle
        pickle_deserialized = PickleSerializer.deserialize_job(pickle_serialized)
        
        # Verify the job works correctly
        assert pickle_deserialized.function(5) == 10
        assert pickle_deserialized.args == (5,)
        assert pickle_deserialized.kwargs == {"multiplier": 3}

    def test_json_complex_data_structures(self) -> None:
        """Test JSON serializer with complex data structures."""
        @dataclass
        class NestedData:
            items: List[Dict[str, Any]]
            count: int

        nested_data = NestedData(
            items=[{"id": 1, "value": "test"}, {"id": 2, "value": "test2"}],
            count=2,
        )

        config = {"setting1": True, "setting2": [1, 2, 3]}

        job = Job(
            function=json_complex_func,
            args=(),
            kwargs={"data": nested_data, "config": config},
            job_id="complex-test",
        )

        # Serialize and deserialize with JSON
        json_serialized = JsonSerializer.serialize_job(job)
        json_deserialized = JsonSerializer.deserialize_job(json_serialized)

        # Verify the data is preserved
        assert json_deserialized.kwargs["data"]["count"] == 2
        assert json_deserialized.kwargs["data"]["items"] == nested_data.items
        assert json_deserialized.kwargs["config"] == config

    def test_error_handling_corrupted_data(self) -> None:
        """Test error handling with corrupted data."""
        # Test with corrupted pickle data
        corrupted_pickle = b"corrupted pickle data"
        
        with pytest.raises(SerializationError, match="Failed to unpickle job"):
            PickleSerializer.deserialize_job(corrupted_pickle)

        # Test with corrupted JSON data
        corrupted_json = b"corrupted json data"
        
        with pytest.raises(SerializationError, match="Failed to parse JSON payload"):
            JsonSerializer.deserialize_job(corrupted_json)

    def test_security_considerations(self) -> None:
        """Test security-related edge cases."""
        # Test that JSON serializer rejects dangerous objects
        def dangerous_func() -> None:
            pass

        # Create a job with a lambda (not importable by name)
        job = Job(
            function=lambda x: x,  # Lambda functions don't have proper qualname
            args=(),
            kwargs={},
            job_id="security-test",
        )

        # JSON serializer should reject this
        with pytest.raises(SerializationError, match="Object is not importable"):
            JsonSerializer.serialize_job(job)

        # Pickle serializer should handle it (but with security warning in docs)
        pickle_serialized = PickleSerializer.serialize_job(job)
        pickle_deserialized = PickleSerializer.deserialize_job(pickle_serialized)
        assert pickle_deserialized.function(5) == 5