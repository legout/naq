"""
Unit tests for Job and JobResult models.

This module contains comprehensive tests for the Job and JobResult classes,
including creation, serialization, deserialization, and execution functionality.
"""

import asyncio
import pickle
import time
import traceback
import uuid
from typing import Any, Dict, List, Optional, Tuple, Union

import msgspec
import pytest

from naq.models.jobs import Job, JobResult
from naq.models.enums import JOB_STATUS


class TestJobResult:
    """Test cases for the JobResult class."""

    def test_job_result_creation(self) -> None:
        """Test basic JobResult creation."""
        job_id = "test-job-123"
        status = JOB_STATUS.COMPLETED
        result = {"output": "success"}
        start_time = 1625097600.0
        finish_time = 1625097601.5

        job_result = JobResult(
            job_id=job_id,
            status=status,
            result=result,
            start_time=start_time,
            finish_time=finish_time
        )

        assert job_result.job_id == job_id
        assert job_result.status == status
        assert job_result.result == result
        assert job_result.error is None
        assert job_result.traceback is None
        assert job_result.start_time == start_time
        assert job_result.finish_time == finish_time

    def test_job_result_creation_with_error(self) -> None:
        """Test JobResult creation with error information."""
        job_id = "test-job-123"
        status = JOB_STATUS.FAILED
        error = "Something went wrong"
        traceback_str = "Traceback (most recent call last):\n  File test.py, line 1\n    raise ValueError\nValueError"
        start_time = 1625097600.0
        finish_time = 1625097601.5

        job_result = JobResult(
            job_id=job_id,
            status=status,
            error=error,
            traceback=traceback_str,
            start_time=start_time,
            finish_time=finish_time
        )

        assert job_result.job_id == job_id
        assert job_result.status == status
        assert job_result.result is None
        assert job_result.error == error
        assert job_result.traceback == traceback_str
        assert job_result.start_time == start_time
        assert job_result.finish_time == finish_time

    def test_job_result_duration_ms(self) -> None:
        """Test JobResult duration_ms property."""
        # Test with valid times
        job_result = JobResult(
            job_id="test-job-123",
            status=JOB_STATUS.COMPLETED,
            start_time=1625097600.0,
            finish_time=1625097601.5
        )
        assert job_result.duration_ms == 1500.0

        # Test with missing start time
        job_result = JobResult(
            job_id="test-job-123",
            status=JOB_STATUS.COMPLETED,
            start_time=0.0,
            finish_time=1625097601.5
        )
        assert job_result.duration_ms is None

        # Test with missing finish time
        job_result = JobResult(
            job_id="test-job-123",
            status=JOB_STATUS.COMPLETED,
            start_time=1625097600.0,
            finish_time=0.0
        )
        assert job_result.duration_ms is None

    def test_job_result_from_job(self) -> None:
        """Test JobResult.from_job method."""
        # Create a job with execution state
        job = Job(
            function=lambda x: x * 2,
            args=(5,),
            job_id="test-job-123"
        )
        job._start_time = 1625097600.0
        job._finish_time = 1625097601.5
        job.result = 10
        job.error = None
        job.traceback = None

        job_result = JobResult.from_job(job)

        assert job_result.job_id == job.job_id
        assert job_result.status == job.status.value
        assert job_result.result == job.result
        assert job_result.error == job.error
        assert job_result.traceback == job.traceback
        assert job_result.start_time == job._start_time
        assert job_result.finish_time == job._finish_time

    def test_job_result_from_job_with_error(self) -> None:
        """Test JobResult.from_job method with failed job."""
        # Create a job with error state
        job = Job(
            function=lambda x: x / 0,
            args=(5,),
            job_id="test-job-123"
        )
        job._start_time = 1625097600.0
        job._finish_time = 1625097601.5
        job.result = None
        job.error = "division by zero"
        job.traceback = "Traceback (most recent call last):\n  File test.py, line 1\nZeroDivisionError"

        job_result = JobResult.from_job(job)

        assert job_result.job_id == job.job_id
        assert job_result.status == job.status.value
        assert job_result.result == job.result
        assert job_result.error == job.error
        assert job_result.traceback == job.traceback
        assert job_result.start_time == job._start_time
        assert job_result.finish_time == job._finish_time

    def test_job_result_serialization(self) -> None:
        """Test JobResult serialization and deserialization."""
        job_result = JobResult(
            job_id="test-job-123",
            status=JOB_STATUS.COMPLETED,
            result={"output": "success"},
            start_time=1625097600.0,
            finish_time=1625097601.5
        )

        # Test msgspec serialization
        encoder = msgspec.json.Encoder()
        decoder = msgspec.json.Decoder(JobResult)
        
        serialized = encoder.encode(job_result)
        deserialized = decoder.decode(serialized)
        
        assert deserialized.job_id == job_result.job_id
        assert deserialized.status == job_result.status.value if hasattr(job_result.status, 'value') else job_result.status
        assert deserialized.result == job_result.result
        assert deserialized.start_time == job_result.start_time
        assert deserialized.finish_time == job_result.finish_time


class TestJob:
    """Test cases for the Job class."""

    def test_job_creation_minimal(self) -> None:
        """Test minimal Job creation."""
        def simple_func(x: int) -> int:
            return x * 2

        job = Job(function=simple_func, args=(5,))

        assert job.function == simple_func
        assert job.args == (5,)
        assert job.kwargs == {}
        assert job.queue_name == "naq_default_queue"  # Default value
        assert job.max_retries == 0  # Default value
        assert job.retry_delay == 0  # Default value
        assert job.retry_strategy == "linear"  # Default value
        assert job.retry_on is None
        assert job.ignore_on is None
        assert job.depends_on is None
        assert job.result_ttl is None
        assert job.timeout is None
        assert job.enqueue_time > 0
        assert job.error is None
        assert job.traceback is None
        assert job._retry_count == 0
        assert job._start_time is None
        assert job._finish_time is None
        assert job.result is None
        assert job.job_id is not None
        assert isinstance(job.job_id, str)

    def test_job_creation_with_all_params(self) -> None:
        """Test Job creation with all parameters."""
        def simple_func(x: int, y: int) -> int:
            return x + y

        job_id = "custom-job-id"
        args = (5, 10)
        kwargs = {"z": 15}
        queue_name = "custom-queue"
        max_retries = 3
        retry_delay = 5.0
        retry_strategy = "exponential"
        retry_on = (ValueError, TypeError)
        ignore_on = (KeyError,)
        depends_on = ["job-1", "job-2"]
        result_ttl = 3600
        timeout = 30

        job = Job(
            function=simple_func,
            job_id=job_id,
            args=args,
            kwargs=kwargs,
            queue_name=queue_name,
            max_retries=max_retries,
            retry_delay=retry_delay,
            retry_strategy=retry_strategy,
            retry_on=retry_on,
            ignore_on=ignore_on,
            depends_on=depends_on,
            result_ttl=result_ttl,
            timeout=timeout
        )

        assert job.function == simple_func
        assert job.job_id == job_id
        assert job.args == args
        assert job.kwargs == kwargs
        assert job.queue_name == queue_name
        assert job.max_retries == max_retries
        assert job.retry_delay == retry_delay
        assert job.retry_strategy == retry_strategy
        assert job.retry_on == retry_on
        assert job.ignore_on == ignore_on
        assert job.depends_on == depends_on
        assert job.result_ttl == result_ttl
        assert job.timeout == timeout

    def test_job_post_init_retry_strategy_validation(self) -> None:
        """Test Job.__post_init__ validates retry strategy."""
        def simple_func() -> None:
            pass

        # Test valid strategies
        for strategy in ["linear", "exponential"]:
            job = Job(function=simple_func, retry_strategy=strategy)
            assert job.retry_strategy == strategy

        # Test invalid strategy
        with pytest.raises(ValueError, match="Invalid retry strategy"):
            Job(function=simple_func, retry_strategy="invalid")

        # Test enum strategy
        from naq.models.enums import RETRY_STRATEGY
        job = Job(function=simple_func, retry_strategy=RETRY_STRATEGY.LINEAR)
        assert job.retry_strategy == "linear"

    def test_job_post_init_args_kwargs_normalization(self) -> None:
        """Test Job.__post_init__ normalizes args and kwargs."""
        def simple_func() -> None:
            pass

        # Test None args
        job = Job(function=simple_func, args=None)
        assert job.args == ()

        # Test None kwargs
        job = Job(function=simple_func, kwargs=None)
        assert job.kwargs == {}

    def test_job_status_property(self) -> None:
        """Test Job.status property."""
        def simple_func() -> None:
            pass

        job = Job(function=simple_func)

        # Test PENDING status (default)
        assert job.status == JOB_STATUS.PENDING

        # Test RUNNING status
        job._start_time = time.time()
        assert job.status == JOB_STATUS.RUNNING

        # Test COMPLETED status
        job._finish_time = time.time()
        assert job.status == JOB_STATUS.COMPLETED

        # Test FAILED status
        job.error = "Something went wrong"
        assert job.status == JOB_STATUS.FAILED

    def test_job_dependency_ids_property(self) -> None:
        """Test Job.dependency_ids property."""
        def simple_func() -> None:
            pass

        # Test no dependencies
        job = Job(function=simple_func)
        assert job.dependency_ids == []

        # Test string dependency
        job = Job(function=simple_func, depends_on="job-123")
        assert job.dependency_ids == ["job-123"]

        # Test list of strings
        job = Job(function=simple_func, depends_on=["job-123", "job-456"])
        assert job.dependency_ids == ["job-123", "job-456"]

        # Test Job object dependency
        dep_job = Job(function=simple_func, job_id="dep-job-123")
        job = Job(function=simple_func, depends_on=dep_job)
        assert job.dependency_ids == ["dep-job-123"]

        # Test mixed dependencies
        job = Job(function=simple_func, depends_on=["job-123", dep_job])
        assert job.dependency_ids == ["job-123", "dep-job-123"]

    def test_job_retry_count_property(self) -> None:
        """Test Job.retry_count property."""
        def simple_func() -> None:
            pass

        job = Job(function=simple_func)
        assert job.retry_count == 0

        job._retry_count = 5
        assert job.retry_count == 5

    def test_job_increment_retry_count(self) -> None:
        """Test Job.increment_retry_count method."""
        def simple_func() -> None:
            pass

        job = Job(function=simple_func)
        assert job.retry_count == 0

        job.increment_retry_count()
        assert job.retry_count == 1

        job.increment_retry_count()
        assert job.retry_count == 2

    def test_job_should_retry(self) -> None:
        """Test Job.should_retry method."""
        def simple_func() -> None:
            pass

        # Test max retries exceeded
        job = Job(function=simple_func, max_retries=2)
        job._retry_count = 2
        assert not job.should_retry(ValueError("test"))

        # Test ignore_on
        job = Job(
            function=simple_func,
            max_retries=3,
            ignore_on=(ValueError,)
        )
        job._retry_count = 1
        assert not job.should_retry(ValueError("test"))

        # Test retry_on
        job = Job(
            function=simple_func,
            max_retries=3,
            retry_on=(ValueError,)
        )
        job._retry_count = 1
        assert job.should_retry(ValueError("test"))
        assert not job.should_retry(TypeError("test"))

        # Test default behavior (retry all exceptions)
        job = Job(function=simple_func, max_retries=3)
        job._retry_count = 1
        assert job.should_retry(ValueError("test"))
        assert job.should_retry(TypeError("test"))

        # Test no retries configured
        job = Job(function=simple_func, max_retries=0)
        assert not job.should_retry(ValueError("test"))

    def test_job_get_next_retry_delay(self) -> None:
        """Test Job.get_next_retry_delay method."""
        def simple_func() -> None:
            pass

        # Test linear strategy
        job = Job(function=simple_func, retry_delay=5.0, retry_strategy="linear")
        assert job.get_next_retry_delay() == 5.0
        job._retry_count = 1
        assert job.get_next_retry_delay() == 5.0

        # Test exponential strategy
        job = Job(function=simple_func, retry_delay=5.0, retry_strategy="exponential")
        assert job.get_next_retry_delay() == 5.0
        job._retry_count = 1
        assert job.get_next_retry_delay() == 10.0
        job._retry_count = 2
        assert job.get_next_retry_delay() == 20.0

        # Test sequence strategy
        job = Job(function=simple_func, retry_delay=[1.0, 2.0, 5.0, 10.0])
        assert job.get_next_retry_delay() == 1.0
        job._retry_count = 1
        assert job.get_next_retry_delay() == 2.0
        job._retry_count = 2
        assert job.get_next_retry_delay() == 5.0
        job._retry_count = 3
        assert job.get_next_retry_delay() == 10.0
        job._retry_count = 4
        assert job.get_next_retry_delay() == 10.0  # Last value

    @pytest.mark.asyncio
    async def test_job_execute_sync_function(self) -> None:
        """Test Job.execute with synchronous function."""
        def sync_func(x: int, y: int) -> int:
            return x + y

        job = Job(function=sync_func, args=(5, 10))
        result = await job.execute()

        assert result == 15
        assert job.result == 15
        assert job.error is None
        assert job.traceback is None
        assert job._start_time is not None
        assert job._finish_time is not None
        assert job.status == JOB_STATUS.COMPLETED

    @pytest.mark.asyncio
    async def test_job_execute_async_function(self) -> None:
        """Test Job.execute with asynchronous function."""
        async def async_func(x: int, y: int) -> int:
            await asyncio.sleep(0.01)  # Simulate async work
            return x * y

        job = Job(function=async_func, args=(5, 10))
        result = await job.execute()

        assert result == 50
        assert job.result == 50
        assert job.error is None
        assert job.traceback is None
        assert job._start_time is not None
        assert job._finish_time is not None
        assert job.status == JOB_STATUS.COMPLETED

    @pytest.mark.asyncio
    async def test_job_execute_with_error(self) -> None:
        """Test Job.execute with function that raises an exception."""
        def error_func() -> None:
            raise ValueError("Test error")

        job = Job(function=error_func)
        
        with pytest.raises(ValueError, match="Test error"):
            await job.execute()

        assert job.result is None
        assert job.error == "Test error"
        assert job.traceback is not None
        assert "ValueError: Test error" in job.traceback
        assert job._start_time is not None
        assert job._finish_time is not None
        assert job.status == JOB_STATUS.FAILED

    @pytest.mark.asyncio
    async def test_job_execute_with_kwargs(self) -> None:
        """Test Job.execute with keyword arguments."""
        def kwargs_func(x: int, y: int, z: int = 0) -> int:
            return x + y + z

        job = Job(function=kwargs_func, args=(5,), kwargs={"y": 10, "z": 15})
        result = await job.execute()

        assert result == 30
        assert job.result == 30

    def test_job_serialize_deserialize(self) -> None:
        """Test Job serialization and deserialization."""
        def simple_func(x: int, y: int) -> int:
            return x + y

        original_job = Job(
            function=simple_func,
            args=(5, 10),
            kwargs={"z": 15},
            queue_name="test-queue",
            max_retries=3,
            job_id="test-job-123"
        )

        # Serialize and deserialize
        serialized = original_job.serialize()
        deserialized_job = Job.deserialize(serialized)

        assert deserialized_job.job_id == original_job.job_id
        assert deserialized_job.args == original_job.args
        assert deserialized_job.kwargs == original_job.kwargs
        assert deserialized_job.queue_name == original_job.queue_name
        assert deserialized_job.max_retries == original_job.max_retries
        # Note: function is not directly comparable due to serialization

    def test_job_serialize_failed_job(self) -> None:
        """Test Job.serialize_failed_job method."""
        def error_func() -> None:
            raise ValueError("Test error")

        job = Job(function=error_func, job_id="test-job-123")
        job.error = "Test error"
        job.traceback = "Traceback (most recent call last):\nValueError: Test error"

        # Note: serialize_failed_job creates a different format than regular serialize
        # It's meant for storing failed job info, not for full job reconstruction
        serialized = job.serialize_failed_job()
        
        # We can't deserialize this with Job.deserialize() because it's a different format
        # Instead, let's just verify the serialization doesn't raise an error
        assert serialized is not None
        assert len(serialized) > 0

    def test_job_serialize_result_static_method(self) -> None:
        """Test Job.serialize_result static method."""
        result_data = {"output": "success"}
        status = JOB_STATUS.COMPLETED

        serialized = Job.serialize_result(result_data, status)
        deserialized = Job.deserialize_result(serialized)

        assert deserialized["status"] == status.value
        assert deserialized["result"] == result_data

    def test_job_serialize_result_with_error(self) -> None:
        """Test Job.serialize_result static method with error."""
        error = "Test error"
        traceback_str = "Traceback (most recent call last):\nValueError: Test error"
        status = JOB_STATUS.FAILED

        serialized = Job.serialize_result(
            result=None,
            status=status,
            error=error,
            traceback_str=traceback_str
        )
        deserialized = Job.deserialize_result(serialized)

        assert deserialized["status"] == status.value
        assert deserialized["result"] is None
        assert deserialized["error"] == error
        assert deserialized["traceback"] == traceback_str

    def test_job_repr(self) -> None:
        """Test Job.__repr__ method."""
        def simple_func() -> None:
            pass

        job = Job(function=simple_func, job_id="test-job-123")
        repr_str = repr(job)

        assert "Job test-job-123" in repr_str
        assert "simple_func" in repr_str

    def test_job_with_custom_job_id(self) -> None:
        """Test Job creation with custom job_id."""
        def simple_func() -> None:
            pass

        custom_id = "my-custom-job-id"
        job = Job(function=simple_func, job_id=custom_id)

        assert job.job_id == custom_id

    def test_job_with_default_job_id(self) -> None:
        """Test Job creation with default job_id."""
        def simple_func() -> None:
            pass

        job = Job(function=simple_func)

        # Should generate a UUID-based ID without dashes
        assert job.job_id is not None
        assert isinstance(job.job_id, str)
        assert "-" not in job.job_id
        assert len(job.job_id) == 32  # UUID without dashes

    def test_job_retry_delay_types(self) -> None:
        """Test Job with different retry_delay types."""
        def simple_func() -> None:
            pass

        # Test int
        job = Job(function=simple_func, retry_delay=5)
        assert job.retry_delay == 5

        # Test float
        job = Job(function=simple_func, retry_delay=5.5)
        assert job.retry_delay == 5.5

        # Test sequence
        job = Job(function=simple_func, retry_delay=[1, 2, 3])
        assert job.retry_delay == [1, 2, 3]