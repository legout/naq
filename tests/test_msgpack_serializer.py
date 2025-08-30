"""
Tests for MsgPackSerializer functionality.
"""
import pytest
import time
from unittest.mock import patch, MagicMock

from naq.serializers import MsgPackSerializer
from naq.models.jobs import Job
from naq.models.enums import JOB_STATUS, RETRY_STRATEGY
from naq.exceptions import SerializationError


# Module-level test functions that are importable
def msgpack_test_func(x, y):
    """Test function for MessagePack serialization."""
    return x + y


def msgpack_dataclass_func(data):
    """Test function for dataclass serialization."""
    # Handle both dict and dataclass objects
    if isinstance(data, dict):
        return f"{data['name']}: {data['value']}"
    else:
        return f"{data.name}: {data.value}"


def msgpack_qualname_test_func():
    """Test function for qualname test."""
    pass


class TestMsgPackSerializer:
    """Test cases for MsgPackSerializer class."""

    def test_serialize_job_basic(self) -> None:
        """Test basic job serialization and deserialization."""
        job = Job(function=msgpack_test_func, args=(1, 2), kwargs={})
        job.job_id = 'test-job-id'

        # Serialize
        serialized = MsgPackSerializer.serialize_job(job)
        assert isinstance(serialized, bytes)
        assert len(serialized) > 0

        # Deserialize
        deserialized = MsgPackSerializer.deserialize_job(serialized)
        assert deserialized.job_id == job.job_id
        assert deserialized.function(1, 2) == 3
        assert deserialized.args == (1, 2)
        assert deserialized.kwargs == {}

    def test_serialize_job_with_all_fields(self) -> None:
        """Test job serialization with all fields populated."""
        job = Job(
            function=msgpack_test_func,
            args=(1, 2),
            kwargs={'key': 'value'},
            job_id='test-job-id',
            enqueue_time=time.time(),
            queue_name='test-queue',
            max_retries=3,
            retry_delay=60,
            retry_strategy=RETRY_STRATEGY.EXPONENTIAL,
            result_ttl=3600,
            timeout=300,
            depends_on=['job1', 'job2']
        )

        serialized = MsgPackSerializer.serialize_job(job)
        deserialized = MsgPackSerializer.deserialize_job(serialized)

        assert deserialized.job_id == job.job_id
        assert deserialized.queue_name == job.queue_name
        assert deserialized.max_retries == job.max_retries
        assert deserialized.retry_delay == job.retry_delay
        assert deserialized.retry_strategy == job.retry_strategy
        assert deserialized.result_ttl == job.result_ttl
        assert deserialized.timeout == job.timeout
        assert deserialized.depends_on == job.depends_on

    def test_serialize_job_with_dataclass(self) -> None:
        """Test job serialization with dataclass arguments."""
        from dataclasses import dataclass

        @dataclass
        class TestData:
            value: int
            name: str

        test_data = TestData(value=42, name="test")
        job = Job(function=msgpack_dataclass_func, args=(test_data,), kwargs={})
        job.job_id = 'test-job-id'

        serialized = MsgPackSerializer.serialize_job(job)
        deserialized = MsgPackSerializer.deserialize_job(serialized)

        result = deserialized.function(deserialized.args[0])
        assert result == "test: 42"

    def test_serialize_job_validation_missing_fields(self) -> None:
        """Test that validation catches missing required fields."""
        # This test would require creating a payload directly and calling _validate_job_payload
        # since the public methods always create valid payloads
        payload = {
            'job_id': 'test-job',
            'function': 'test_func',
            # Missing 'args' and 'kwargs'
        }

        with pytest.raises(SerializationError, match="Missing required field"):
            MsgPackSerializer._validate_job_payload(payload)

    def test_serialize_job_validation_invalid_types(self) -> None:
        """Test that validation catches invalid field types."""
        payload = {
            'job_id': 123,  # Should be string
            'function': 'test_func',
            'args': [],
            'kwargs': {}
        }

        with pytest.raises(SerializationError, match="job_id must be a string"):
            MsgPackSerializer._validate_job_payload(payload)

    def test_serialize_job_validation_negative_values(self) -> None:
        """Test that validation catches negative numeric values."""
        payload = {
            'job_id': 'test-job',
            'function': 'test_func',
            'args': [],
            'kwargs': {},
            'max_retries': -1  # Should be non-negative
        }

        with pytest.raises(SerializationError, match="max_retries must be non-negative"):
            MsgPackSerializer._validate_job_payload(payload)

    def test_serialize_failed_job(self) -> None:
        """Test failed job serialization and deserialization."""
        job = Job(function=msgpack_test_func, args=(1, 2), kwargs={})
        job.job_id = 'test-job-id'
        job.error = 'Test error message'
        job.traceback = 'Test traceback'
        job._finish_time = time.time()  # Mark as finished

        serialized = MsgPackSerializer.serialize_failed_job(job)
        deserialized = MsgPackSerializer.deserialize_failed_job(serialized)

        assert deserialized.job_id == job.job_id
        assert deserialized.error == job.error
        assert deserialized.traceback == job.traceback
        assert deserialized.status == JOB_STATUS.FAILED

    def test_serialize_failed_job_validation(self) -> None:
        """Test failed job payload validation."""
        payload = {
            'job_id': 'test-job',
            'function_str': 'test_func',
            'args_repr': '()',
            # Missing 'kwargs_repr'
        }

        with pytest.raises(SerializationError, match="Missing required field"):
            MsgPackSerializer._validate_failed_job_payload(payload)

    def test_serialize_result(self) -> None:
        """Test result serialization and deserialization."""
        result_data = {'key': 'value', 'number': 42}
        
        serialized = MsgPackSerializer.serialize_result(
            result=result_data,
            status=JOB_STATUS.COMPLETED
        )
        
        deserialized = MsgPackSerializer.deserialize_result(serialized)
        
        assert deserialized['status'] == JOB_STATUS.COMPLETED.value
        assert deserialized['result'] == result_data
        assert deserialized['error'] is None
        assert deserialized['traceback'] is None

    def test_serialize_result_with_error(self) -> None:
        """Test result serialization with error."""
        serialized = MsgPackSerializer.serialize_result(
            result=None,
            status=JOB_STATUS.FAILED,
            error='Test error',
            traceback_str='Test traceback'
        )
        
        deserialized = MsgPackSerializer.deserialize_result(serialized)
        
        assert deserialized['status'] == JOB_STATUS.FAILED.value
        assert deserialized['result'] is None
        assert deserialized['error'] == 'Test error'
        assert deserialized['traceback'] == 'Test traceback'

    def test_serialize_result_validation(self) -> None:
        """Test result payload validation."""
        payload = {
            # Missing 'status'
            'result': None,
            'error': None,
            'traceback': None
        }

        with pytest.raises(SerializationError, match="Missing required field"):
            MsgPackSerializer._validate_result_payload(payload)

    def test_msgpack_encode_decode(self) -> None:
        """Test MessagePack encoding and decoding."""
        payload = {
            'key1': 'value1',
            'key2': [1, 2, 3],
            'key3': {'nested': 'data'}
        }

        encoded = MsgPackSerializer._msgpack_encode(payload)
        assert isinstance(encoded, bytes)

        decoded = MsgPackSerializer._msgpack_decode(encoded)
        assert decoded == payload

    def test_msgpack_encode_invalid_data(self) -> None:
        """Test MessagePack encoding with invalid data."""
        # Use a complex object that can't be serialized
        class CustomObject:
            pass

        payload = {'key': CustomObject()}

        with pytest.raises(SerializationError, match="Failed to MessagePack-serialize"):
            MsgPackSerializer._msgpack_encode(payload)

    def test_msgpack_decode_invalid_data(self) -> None:
        """Test MessagePack decoding with invalid data."""
        invalid_data = b'\x93\x01\x02\x03'  # Incomplete MessagePack data

        with pytest.raises(SerializationError, match="Failed to parse MessagePack"):
            MsgPackSerializer._msgpack_decode(invalid_data)

    def test_resolve_dotted_path(self) -> None:
        """Test resolving dotted import paths."""
        # Test module:attribute format
        obj = MsgPackSerializer._resolve_dotted_path('time:time')
        assert callable(obj)

        # Test module.attr format (backwards compatibility)
        obj = MsgPackSerializer._resolve_dotted_path('time.time')
        assert callable(obj)

    def test_resolve_dotted_path_invalid(self) -> None:
        """Test resolving invalid dotted paths."""
        with pytest.raises(SerializationError, match="Invalid import path"):
            MsgPackSerializer._resolve_dotted_path('invalid_path')

        with pytest.raises(SerializationError, match="Could not import"):
            MsgPackSerializer._resolve_dotted_path('nonexistent.module:function')

    def test_qualname(self) -> None:
        """Test getting qualified names for objects."""
        qualname = MsgPackSerializer._qualname(msgpack_qualname_test_func)
        assert ':' in qualname
        assert 'test_msgpack_serializer' in qualname
        assert 'msgpack_qualname_test_func' in qualname

    def test_qualname_lambda(self) -> None:
        """Test that lambda functions raise an error."""
        lambda_func = lambda x: x

        with pytest.raises(SerializationError, match="Object is not importable"):
            MsgPackSerializer._qualname(lambda_func)

    def test_encode_args_kwargs(self) -> None:
        """Test encoding args and kwargs."""
        args = (1, 'two', [3, 4, 5])
        kwargs = {'key': 'value', 'nested': {'a': 1, 'b': 2}}

        encoded_args, encoded_kwargs = MsgPackSerializer._encode_args_kwargs(args, kwargs)

        assert encoded_args == [1, 'two', [3, 4, 5]]
        assert encoded_kwargs == {'key': 'value', 'nested': {'a': 1, 'b': 2}}

    def test_encode_args_kwargs_with_dataclass(self) -> None:
        """Test encoding args and kwargs with dataclasses."""
        from dataclasses import dataclass

        @dataclass
        class TestData:
            value: int
            name: str

        test_data = TestData(value=42, name="test")
        args = (test_data,)
        kwargs = {}

        encoded_args, encoded_kwargs = MsgPackSerializer._encode_args_kwargs(args, kwargs)

        assert encoded_args == [{'value': 42, 'name': 'test'}]
        assert encoded_kwargs == {}

    def test_encode_args_kwargs_invalid_data(self) -> None:
        """Test encoding args and kwargs with invalid data."""
        class CustomObject:
            pass

        args = (CustomObject(),)
        kwargs = {}

        with pytest.raises(SerializationError, match="Object of type CustomObject is not MessagePack serializable"):
            MsgPackSerializer._encode_args_kwargs(args, kwargs)

    def test_encode_decode_exceptions(self) -> None:
        """Test encoding and decoding exception classes."""
        exc_tuple = (ValueError, TypeError, RuntimeError)
        
        encoded = MsgPackSerializer._encode_exceptions(exc_tuple)
        assert isinstance(encoded, list)
        assert len(encoded) == 3
        
        decoded = MsgPackSerializer._decode_exceptions(encoded)
        assert decoded == exc_tuple

    def test_encode_exceptions_none(self) -> None:
        """Test encoding None for exceptions."""
        encoded = MsgPackSerializer._encode_exceptions(None)
        assert encoded is None

    def test_decode_exceptions_none(self) -> None:
        """Test decoding None for exceptions."""
        decoded = MsgPackSerializer._decode_exceptions(None)
        assert decoded is None

    def test_encode_exceptions_invalid_type(self) -> None:
        """Test encoding invalid exception types."""
        # Pass non-exception classes
        exc_tuple = (str, int)  # type: ignore

        with pytest.raises(SerializationError, match="retry_on/ignore_on must be exception classes"):
            MsgPackSerializer._encode_exceptions(exc_tuple)

    def test_decode_exceptions_invalid_path(self) -> None:
        """Test decoding invalid exception paths."""
        exc_paths = ['nonexistent.module:Exception']

        with pytest.raises(SerializationError, match="Could not import"):
            MsgPackSerializer._decode_exceptions(exc_paths)

    def test_integrity_metadata_enabled(self) -> None:
        """Test serialization with integrity metadata enabled."""
        job = Job(function=msgpack_test_func, args=(1, 2), kwargs={})
        job.job_id = 'test-job-id'

        with patch('naq.serializers.SERIALIZATION_CHECKSUM_ENABLED', True):
            with patch('naq.serializers.SERIALIZATION_CHECKSUM_ALGORITHM', 'sha256'):
                serialized = MsgPackSerializer.serialize_job(job)
                deserialized = MsgPackSerializer.deserialize_job(serialized)

                assert deserialized.job_id == job.job_id
                assert deserialized.function(1, 2) == 3

    def test_integrity_metadata_with_signature(self) -> None:
        """Test serialization with integrity metadata and signature enabled."""
        job = Job(function=msgpack_test_func, args=(1, 2), kwargs={})
        job.job_id = 'test-job-id'

        with patch('naq.serializers.SERIALIZATION_CHECKSUM_ENABLED', True):
            with patch('naq.serializers.SERIALIZATION_SIGNATURE_KEY', 'test-secret-key'):
                with patch('naq.serializers.SERIALIZATION_CHECKSUM_ALGORITHM', 'sha256'):
                    serialized = MsgPackSerializer.serialize_job(job)
                    deserialized = MsgPackSerializer.deserialize_job(serialized)

                    assert deserialized.job_id == job.job_id
                    assert deserialized.function(1, 2) == 3

    def test_integrity_metadata_backward_compatibility(self) -> None:
        """Test backward compatibility with data without integrity metadata."""
        job = Job(function=msgpack_test_func, args=(1, 2), kwargs={})
        job.job_id = 'test-job-id'

        # Serialize without integrity metadata
        serialized = MsgPackSerializer.serialize_job(job)

        # Try to deserialize with integrity metadata enabled
        with patch('naq.serializers.SERIALIZATION_CHECKSUM_ENABLED', True):
            deserialized = MsgPackSerializer.deserialize_job(serialized)

            assert deserialized.job_id == job.job_id
            assert deserialized.function(1, 2) == 3

    def test_large_data_size_validation(self) -> None:
        """Test validation of large serialized data."""
        job = Job(function=msgpack_test_func, args=(1, 2), kwargs={})
        job.job_id = 'test-job-id'

        with patch('naq.serializers.SERIALIZATION_MAX_SIZE_BYTES', 100):  # Very small limit
            with pytest.raises(SerializationError, match="size .* exceeds maximum allowed size"):
                MsgPackSerializer.serialize_job(job)

    def test_performance_comparison(self) -> None:
        """Test that MsgPackSerializer is faster than JsonSerializer."""
        import time
        from naq.serializers import JsonSerializer

        job = Job(function=msgpack_test_func, args=(1, 2), kwargs={'key': 'value'})
        job.job_id = 'performance-test-job'

        # Test MsgPackSerializer
        start_time = time.perf_counter()
        for _ in range(100):
            serialized = MsgPackSerializer.serialize_job(job)
            MsgPackSerializer.deserialize_job(serialized)
        msgpack_time = time.perf_counter() - start_time

        # Test JsonSerializer
        start_time = time.perf_counter()
        for _ in range(100):
            serialized = JsonSerializer.serialize_job(job)
            JsonSerializer.deserialize_job(serialized)
        json_time = time.perf_counter() - start_time

        # MsgPack should be faster (allowing for some variance)
        assert msgpack_time < json_time * 1.5, f"MsgPack was not faster: {msgpack_time} vs {json_time}"

    def test_serialized_size_comparison(self) -> None:
        """Test that MsgPackSerializer produces smaller output than JsonSerializer."""
        from naq.serializers import JsonSerializer

        job = Job(function=msgpack_test_func, args=(1, 2), kwargs={'key': 'value', 'list': [1, 2, 3, 4, 5]})
        job.job_id = 'size-test-job'

        msgpack_serialized = MsgPackSerializer.serialize_job(job)
        json_serialized = JsonSerializer.serialize_job(job)

        # MsgPack should be smaller (allowing for some variance)
        assert len(msgpack_serialized) < len(json_serialized) * 1.2, \
            f"MsgPack was not smaller: {len(msgpack_serialized)} vs {len(json_serialized)}"