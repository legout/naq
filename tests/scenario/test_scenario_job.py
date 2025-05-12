"""Scenario tests for the Job module."""
from freezegun import freeze_time

from naq.job import Job
from naq.settings import (
    RETRY_STRATEGY_EXPONENTIAL,
    RETRY_STRATEGY_LINEAR,
)
from naq.job import JobStatus


# --- Helper Functions ---
def dummy_task():
    """Simple task for testing."""
    return "dummy result"


def failing_task():
    """Task that raises an exception."""
    raise ValueError("Task failed")


# --- Test Scenarios ---

class TestJobChaining:
    """Test scenarios for job dependencies and chaining."""
    
    def test_job_dependency_definition(self):
        """Scenario: Define two jobs where Job B depends on Job A's ID."""
        # Create Job A
        job_a = Job(dummy_task)
        
        # Create Job B with dependency on Job A
        job_b = Job(dummy_task, depends_on=job_a)
        
        # Verify dependency is correctly set
        assert len(job_b.dependency_ids) == 1
        assert job_b.dependency_ids[0] == job_a.job_id
    
    def test_multiple_job_dependencies(self):
        """Scenario: Job depending on multiple other jobs."""
        job_a = Job(dummy_task)
        job_b = Job(dummy_task)
        job_c = Job(dummy_task, depends_on=[job_a, job_b])
        
        assert len(job_c.dependency_ids) == 2
        assert job_a.job_id in job_c.dependency_ids
        assert job_b.job_id in job_c.dependency_ids
    
    def test_job_dependency_by_id(self):
        """Scenario: Job dependency using string IDs."""
        job_a_id = "job123"
        job_b = Job(dummy_task, depends_on=job_a_id)
        
        assert len(job_b.dependency_ids) == 1
        assert job_b.dependency_ids[0] == job_a_id


class TestRetryPolicies:
    """Test scenarios for job retry policies."""
    
    def test_exponential_backoff(self):
        """Scenario: Job with exponential backoff retry strategy."""
        job = Job(
            failing_task,
            max_retries=3,
            retry_delay=2,
            retry_strategy=RETRY_STRATEGY_EXPONENTIAL
        )
        
        # First retry: 2 seconds
        assert job.get_next_retry_delay() == 2.0
        job.increment_retry_count()
        
        # Second retry: 4 seconds (2 * 2^1)
        assert job.get_next_retry_delay() == 4.0
        job.increment_retry_count()
        
        # Third retry: 8 seconds (2 * 2^2)
        assert job.get_next_retry_delay() == 8.0
    
    def test_custom_retry_sequence(self):
        """Scenario: Job with custom retry delay sequence."""
        job = Job(
            failing_task,
            max_retries=3,
            retry_delay=[5, 10, 15],
            retry_strategy=RETRY_STRATEGY_LINEAR
        )
        
        # Verify each retry delay in sequence
        assert job.get_next_retry_delay() == 5.0
        job.increment_retry_count()
        
        assert job.get_next_retry_delay() == 10.0
        job.increment_retry_count()
        
        assert job.get_next_retry_delay() == 15.0
    
    def test_retry_on_specific_exceptions(self):
        """Scenario: Job retries on specific exception types."""
        job = Job(
            failing_task,
            max_retries=2,
            retry_on=(ValueError,),
            ignore_on=(KeyError,)
        )
        
        # Should retry on ValueError
        assert job.should_retry(ValueError("test error")) is True
        
        # Should not retry on ignored exception
        assert job.should_retry(KeyError("test error")) is False
        
        # Should not retry on other exceptions
        assert job.should_retry(TypeError("test error")) is False


class TestTimeoutHandling:
    """Test scenarios for job timeout handling."""
    
    def test_job_timeout_definition(self):
        """Scenario: Job with timeout defined."""
        # Job result TTL serves as a timeout mechanism
        job = Job(dummy_task, result_ttl=5)  # 5 second TTL
        assert job.result_ttl == 5

    @freeze_time("2025-01-01 12:00:00")
    def test_job_timing_tracking(self):
        """Scenario: Track job timing attributes."""
        job = Job(dummy_task)
        assert job.enqueue_time == 1735732800.0  # 2025-01-01 12:00:00 UTC


class TestComplexArguments:
    """Test scenarios for complex job arguments and results."""
    
    def test_complex_nested_arguments(self):
        """Scenario: Job with complex nested arguments."""
        complex_args = {"nested": {"list": [1, 2, {"key": "value"}]}}
        complex_kwargs = {
            "data": {
                "items": [{"id": 1, "values": [1.0, 2.0]},
                         {"id": 2, "values": [3.0, 4.0]}]
            }
        }
        
        job = Job(dummy_task, args=(complex_args,), kwargs=complex_kwargs)
        
        # Verify arguments are stored correctly
        assert job.args[0] == complex_args
        assert job.kwargs == complex_kwargs
        
        # Test serialization preserves structure
        serialized = job.serialize()
        deserialized = Job.deserialize(serialized)
        
        assert deserialized.args[0] == complex_args
        assert deserialized.kwargs == complex_kwargs
    
    def test_complex_result_handling(self):
        """Scenario: Complex result serialization."""
        complex_result = {
            "status": "success",
            "data": [{"id": 1, "computed": [1.1, 2.2]},
                    {"id": 2, "computed": [3.3, 4.4]}],
            "metadata": {"timestamp": "2025-01-01", "version": "1.0"}
        }
        
        # Test result serialization
        result_bytes = Job.serialize_result(
            complex_result,
            JobStatus.COMPLETED
        )
        
        # Deserialize and verify
        result_data = Job.deserialize_result(result_bytes)
        assert result_data["status"] == JobStatus.COMPLETED
        assert result_data["result"] == complex_result


class TestRecurringJobs:
    """Test scenarios for recurring job representations."""
    
    def test_cron_job_representation(self):
        """Scenario: Job defined with cron attributes."""
        # While Job class doesn't directly handle cron strings,
        # it can store them in kwargs for the scheduler
        job = Job(
            dummy_task,
            kwargs={"cron": "0 0 * * *"}  # Daily at midnight
        )
        
        assert job.kwargs["cron"] == "0 0 * * *"
    
    def test_interval_job_representation(self):
        """Scenario: Job with interval representation."""
        # Interval information can be stored in kwargs
        job = Job(
            dummy_task,
            kwargs={"interval": 600}  # 10 minutes
        )
        
        assert job.kwargs["interval"] == 600


class TestDocumentationExamples:
    """Test scenarios based on documentation examples."""
    
    def test_job_dependencies_example(self):
        """Scenario: Model the job_dependencies.py example behavior."""
        # Create jobs as shown in the example
        def task_a(): pass
        def task_b(): pass
        def final_task(): pass
        
        job_a = Job(task_a)
        job_b = Job(task_b)
        job_final = Job(final_task, depends_on=[job_a, job_b])
        
        # Verify dependencies match the example
        assert len(job_final.dependency_ids) == 2
        assert job_a.job_id in job_final.dependency_ids
        assert job_b.job_id in job_final.dependency_ids
    
    def test_job_retries_example(self):
        """Scenario: Model the job_retries.py example behavior."""
        # Create job with retry configuration from the example
        async def flaky_task(message: str): pass
        
        job = Job(
            flaky_task,
            args=("Hello from retry example!",),
            max_retries=3,
            retry_delay=[2, 4, 6]
        )
        
        # Verify retry configuration matches example
        assert job.max_retries == 3
        assert job.retry_delay == [2, 4, 6]
        
        # Verify retry delays follow the sequence
        assert job.get_next_retry_delay() == 2.0
        job.increment_retry_count()
        assert job.get_next_retry_delay() == 4.0
        job.increment_retry_count()
        assert job.get_next_retry_delay() == 6.0