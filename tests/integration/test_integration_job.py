import pytest
from datetime import datetime
from dataclasses import dataclass
from typing import Any, Optional

from naq.job import Job
from naq.exceptions import SerializationError
from naq.settings import (
    RETRY_STRATEGY_LINEAR,
)


# Test Utilities
@dataclass
class SerializableObject:
    """A custom object that can be serialized."""

    name: str
    value: Any
    timestamp: Optional[datetime] = None

    def to_dict(self):
        return {
            "name": self.name,
            "value": self.value,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
        }


class TestJobIntegration:
    """Integration tests for the Job module."""

    # Persistence & Retrieval Tests
    # @pytest.mark.asyncio
    # async
    def test_round_trip_serialization(self, mock_nats):
        """Test round-trip serialization of a Job with simple args/kwargs."""

        def sample_function(x: int, text: str = "default") -> str:
            return f"{x}: {text}"

        # Create original job
        original_job = Job(
            function=sample_function,
            args=(42,),
            kwargs={"text": "test"},
            queue_name="test_queue",
            max_retries=2,
            retry_delay=5,
            retry_strategy=RETRY_STRATEGY_LINEAR,
        )

        # Save and retrieve
        serialized = original_job.serialize()
        retrieved_job = Job.deserialize(serialized)

        # Verify all attributes match
        assert retrieved_job.job_id == original_job.job_id
        assert retrieved_job.args == original_job.args
        assert retrieved_job.kwargs == original_job.kwargs
        assert retrieved_job.queue_name == original_job.queue_name
        assert retrieved_job.max_retries == original_job.max_retries
        assert retrieved_job.retry_delay == original_job.retry_delay
        assert retrieved_job.retry_strategy == original_job.retry_strategy

        # Verify function still works
        assert (
            retrieved_job.function(*retrieved_job.args, **retrieved_job.kwargs)
            == "42: test"
        )

    # @pytest.mark.asyncio
    # async
    def test_complex_data_types(self, mock_nats):
        """Test serialization with complex data types."""
        nested_data = {
            "list": [1, 2, {"nested": True}],
            "dict": {"a": [1, 2, 3], "b": {"deep": "value"}},
        }

        custom_obj = SerializableObject(
            name="test", value=nested_data, timestamp=datetime.now()
        )

        def complex_function(data: dict, obj: SerializableObject) -> dict:
            return {"data": data, "obj": obj.to_dict()}

        # Create and serialize job
        original_job = Job(
            complex_function,
            args=(nested_data, custom_obj),
        )

        serialized = original_job.serialize()
        retrieved_job = Job.deserialize(serialized)

        # Verify complex data structure equality
        assert retrieved_job.args[0] == nested_data
        assert isinstance(retrieved_job.args[1], SerializableObject)
        assert retrieved_job.args[1].name == custom_obj.name
        assert retrieved_job.args[1].value == custom_obj.value
        assert retrieved_job.args[1].timestamp == custom_obj.timestamp

    # @pytest.mark.asyncio
    # async
    def test_timestamp_integrity(self, mock_nats, monkeypatch):
        """Test preservation of timestamp fields during serialization."""
        fixed_time = 1746972733.636187  # Fixed timestamp for testing
        monkeypatch.setattr("time.time", lambda: fixed_time)

        def noop():
            pass

        original_job = Job(noop)
        assert original_job.enqueue_time == fixed_time

        # Serialize and deserialize
        serialized = original_job.serialize()
        retrieved_job = Job.deserialize(serialized)

        assert retrieved_job.enqueue_time == fixed_time

    # Core Utilities Integration Tests
    def test_function_path_resolution(self):
        """Test function resolution from string paths."""

        def test_func():
            return "test"

        # Create job with function
        job = Job(test_func)

        # Get function string representation
        #func_repr = f"{test_func.__module__}.{test_func.__name__}"

        # Verify function matches when deserialized
        serialized = job.serialize()
        retrieved_job = Job.deserialize(serialized)

        assert retrieved_job.function() == "test"
        assert retrieved_job.function.__name__ == test_func.__name__

    # @pytest.mark.asyncio
    # async
    def test_serialization_with_custom_types(self, mock_nats):
        """Test serialization handling of custom types."""
        current_time = datetime.now()

        def func_with_custom_types(dt: datetime) -> str:
            return dt.isoformat()

        # Create job with datetime argument
        job = Job(func_with_custom_types, args=(current_time,))

        # Test serialization and deserialization
        serialized = job.serialize()
        retrieved_job = Job.deserialize(serialized)

        # Verify datetime was properly preserved
        assert isinstance(retrieved_job.args[0], datetime)
        assert retrieved_job.args[0] == current_time

    # # Custom Job Types Tests
    # def test_subclassed_job(self):
    #     """Test serialization of custom Job subclasses."""
    #     class CustomJob(Job):
    #         def __init__(self, *args, custom_attr=None, **kwargs):
    #             super().__init__(*args, **kwargs)
    #             self.custom_attr = custom_attr

    #     def sample_func():
    #         return "test"

    #     # Create custom job instance
    #     original_job = CustomJob(
    #         sample_func,
    #         custom_attr="test_value"
    #     )

    #     # Serialize and deserialize
    #     serialized = original_job.serialize()
    #     retrieved_job = Job.deserialize(serialized)

    #     # Basic job attributes should be preserved
    #     assert retrieved_job.function == original_job.function
    #     assert retrieved_job.job_id == original_job.job_id

    # Error Handling Tests
    def test_serialization_failure(self):
        """Test handling of non-serializable objects."""

        class UnserializableObject:
            def __getstate__(self):
                raise TypeError("Cannot serialize this object")

        def function_with_unserializable(obj):
            return str(obj)

        unserializable = UnserializableObject()
        job = Job(function_with_unserializable, args=(unserializable,))

        with pytest.raises(SerializationError):
            job.serialize()

    def test_corrupted_payload_handling(self):
        """Test handling of corrupted serialized data."""

        def sample_func():
            pass

        job = Job(sample_func)
        serialized = job.serialize()

        # Corrupt the serialized data
        corrupted = b"corrupted" + serialized[10:]

        with pytest.raises(SerializationError):
            Job.deserialize(corrupted)

    # @pytest.mark.asyncio
    # async
    def test_missing_required_fields(self, mock_nats):
        """Test handling of incomplete job data."""

        def sample_func():
            pass

        job = Job(sample_func)
        serialized = job.serialize()

        # Deserialize to dictionary and remove required field
        import cloudpickle

        data = cloudpickle.loads(serialized)
        del data["function"]
        corrupted = cloudpickle.dumps(data)

        with pytest.raises(SerializationError):
            Job.deserialize(corrupted)
