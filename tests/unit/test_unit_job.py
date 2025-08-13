import asyncio
import pytest

from typing import Any

from naq.models.jobs import Job, JobResult, RetryDelayType
from naq.models.enums import JOB_STATUS, RETRY_STRATEGY
from naq.exceptions import SerializationError


class TestJob:
    """Test cases for the Job class."""

    def test_init_default_parameters(self):
        """Test Job initialization with default parameters."""
        def sample_func(): pass

        job = Job(function=sample_func)

        assert callable(job.function)
        assert job.function == sample_func
        assert job.args == ()
        assert job.kwargs == {}
        assert isinstance(job.job_id, str)
        assert len(job.job_id) == 32  # UUID without hyphens
        assert job.queue_name == "naq_default_queue"
        assert job.max_retries == 0
        assert job.retry_delay == 0
        assert job.retry_strategy == RETRY_STRATEGY.LINEAR.value
        assert job.retry_on is None
        assert job.ignore_on is None
        assert job.depends_on is None

    def test_init_custom_parameters(self):
        """Test Job initialization with custom parameters."""
        def sample_func(): pass

        custom_id = "custom_id"
        custom_args = (1, "test")
        custom_kwargs = {"key": "value"}

        job = Job(
            function=sample_func,
            args=custom_args,
            kwargs=custom_kwargs,
            job_id=custom_id,
            queue_name="custom_queue",
            max_retries=3,
            retry_delay=60,
            retry_strategy=RETRY_STRATEGY.EXPONENTIAL,
            retry_on=(ValueError,),
            ignore_on=(TypeError,),
        )

        assert job.function == sample_func
        assert job.args == custom_args
        assert job.kwargs == custom_kwargs
        assert job.job_id == custom_id
        assert job.queue_name == "custom_queue"
        assert job.max_retries == 3
        assert job.retry_delay == 60
        assert job.retry_strategy == RETRY_STRATEGY.EXPONENTIAL.value
        assert job.retry_on == (ValueError,)
        assert job.ignore_on == (TypeError,)

    def test_unique_job_id_generation(self):
        """Test that each job gets a unique ID when not specified."""
        def sample_func(): pass

        job1 = Job(function=sample_func)
        job2 = Job(function=sample_func)

        assert job1.job_id != job2.job_id
        assert len(job1.job_id) == 32
        assert len(job2.job_id) == 32

    def test_job_serialization(self):
        """Test job serialization to dictionary."""
        def sample_func(x: int, y: str = "default") -> Any:
            return x, y

        job = Job(
            function=sample_func,
            args=(42,),
            kwargs={"y": "test"},
            retry_on=(ValueError, KeyError),
            ignore_on=(TypeError,),
        )

        serialized = job.serialize()
        print(f"Serialized job: {serialized}")  # Add log
        #print(f"Serialized job keys: {list(serialized.keys())}")  # Add log
        deserialized = Job.deserialize(serialized)
        # print(f"Deserialized job: {deserialized}")  # Avoid __repr__ triggering async code

        assert deserialized.job_id == job.job_id
        assert deserialized.queue_name == job.queue_name
        assert deserialized.max_retries == job.max_retries
        assert deserialized.retry_delay == job.retry_delay
        assert deserialized.args == job.args
        assert deserialized.kwargs == job.kwargs
        assert callable(deserialized.function)

    def test_serialization_error_handling(self):
        """Test error handling during serialization."""
        class UnpickleableObject:
            def __getstate__(self):
                raise TypeError("Cannot pickle this object")

        def sample_func(obj):
            return obj

        job = Job(function=sample_func, args=(UnpickleableObject(),))

        with pytest.raises(SerializationError):
            job.serialize()

    def test_dependency_handling(self):
        """Test job dependency handling."""
        def sample_func(): pass

        # Test with string dependency
        job1 = Job(function=sample_func, depends_on="job123")
        assert job1.dependency_ids == ["job123"]

        # Test with Job object dependency
        dep_job = Job(function=sample_func)
        job2 = Job(function=sample_func, depends_on=dep_job)
        assert job2.dependency_ids == [dep_job.job_id]

        # Test with multiple dependencies
        job3 = Job(function=sample_func, depends_on=["job1", dep_job])
        assert set(job3.dependency_ids) == {dep_job.job_id, "job1"}

    def test_retry_logic(self):
        """Test retry logic and delay calculations."""
        def sample_func(): pass

        # Test linear retry strategy
        job_linear = Job(
            function=sample_func,
            max_retries=3,
            retry_delay=10,
            retry_strategy=RETRY_STRATEGY.LINEAR
        )

        assert job_linear.should_retry(ValueError()) is True
        assert job_linear.get_next_retry_delay() == 10.0
        job_linear.increment_retry_count()
        assert job_linear.get_next_retry_delay() == 10.0

        # Test exponential retry strategy
        job_exp = Job(
            function=sample_func,
            max_retries=3,
            retry_delay=5,
            retry_strategy=RETRY_STRATEGY.EXPONENTIAL
        )
        print(f"Next retry delay (exponential): {job_exp.get_next_retry_delay()}")  # Add log
        assert job_exp.get_next_retry_delay() == 5.0
        job_exp.increment_retry_count()
        print(f"Next retry delay (exponential, after increment): {job_exp.get_next_retry_delay()}")  # Add log
        assert job_exp.get_next_retry_delay() == 10.0
        job_exp.increment_retry_count()
        print(f"Next retry delay (exponential, after increment): {job_exp.get_next_retry_delay()}")  # Add log
        assert job_exp.get_next_retry_delay() == 20.0

    @pytest.mark.asyncio
    async def test_execute_sync_function(self):
        """Test execution of a synchronous function."""
        def sample_func(x, y=1):
            return x + y

        job = Job(function=sample_func, args=(1,), kwargs={"y": 2})
        result = await job.execute()
        
        assert result == 3
        assert job.status == JOB_STATUS.COMPLETED
        assert job.result == 3
        assert job._start_time is not None
        assert job._finish_time is not None
        assert job.error is None
        assert job.traceback is None

    @pytest.mark.asyncio
    async def test_execute_async_function(self):
        """Test execution of an asynchronous function."""
        async def sample_async_func(x, y=1):
            await asyncio.sleep(0.01)  # Simulate async work
            return x + y

        job = Job(function=sample_async_func, args=(2,), kwargs={"y": 3})
        result = await job.execute()
        
        assert result == 5
        assert job.status == JOB_STATUS.COMPLETED
        assert job.result == 5
        assert job._start_time is not None
        assert job._finish_time is not None
        assert job.error is None
        assert job.traceback is None

    @pytest.mark.asyncio
    async def test_execute_with_error(self):
        """Test error handling during execution."""
        def failing_func():
            raise ValueError("Test error")

        job = Job(function=failing_func)
        with pytest.raises(ValueError, match="Test error"):
            await job.execute()
        
        assert job.status == JOB_STATUS.FAILED
        assert job._start_time is not None
        assert job._finish_time is not None
        assert job.error == "Test error"
        assert "ValueError: Test error" in job.traceback
        assert "failing_func" in job.traceback