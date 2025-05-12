
from naq.job import Job
from naq.job import JobStatus

def test_basic_job_instantiation():
    """Test basic Job instantiation with minimal parameters."""
    def sample_func(x: int) -> int:
        return x * 2

    # Create a basic job
    job = Job(sample_func, args=(42,))

    # Verify basic attributes
    assert callable(job.function)
    assert job.function == sample_func
    assert job.args == (42,)
    assert job.kwargs == {}
    assert isinstance(job.job_id, str)
    assert len(job.job_id) == 32  # UUID without hyphens
    assert job.status == JobStatus.PENDING  # Default status


def test_job_id_uniqueness():
    """Test that each job gets a unique ID."""
    def noop(): pass

    # Create multiple jobs and verify unique IDs
    jobs = [Job(noop) for _ in range(3)]
    job_ids = [job.job_id for job in jobs]
    
    assert len(set(job_ids)) == len(jobs)  # All IDs should be unique


def test_job_serialization_roundtrip():
    """Test Job serialization and deserialization."""
    def test_func(x: int, y: str = "default") -> str:
        return f"{x}-{y}"

    original_job = Job(
        test_func,
        args=(42,),
        kwargs={"y": "test"},
        queue_name="test_queue"
    )

    # Serialize and deserialize
    serialized = original_job.serialize()
    deserialized_job = Job.deserialize(serialized)

    # Verify core attributes are preserved
    assert deserialized_job.job_id == original_job.job_id
    assert deserialized_job.queue_name == original_job.queue_name
    assert deserialized_job.args == original_job.args
    assert deserialized_job.kwargs == original_job.kwargs


#@pytest.mark.asyncio
#async 
def test_job_lifecycle_status(mock_nats):
    """Test basic job lifecycle status transitions."""
    def sample_task(): 
        return "test_result"
    
    job = Job(sample_task)

    # Initial state
    assert not hasattr(job, "start_time")
    assert not hasattr(job, "end_time")
    
    # Test result serialization for success case
    success_data = job.serialize_result(
        "test_result",
        JobStatus.COMPLETED,
    )
    success_result = job.deserialize_result(success_data)

    assert success_result["status"] == JobStatus.COMPLETED
    assert success_result["result"] == "test_result"
    assert success_result["error"] is None

    # Test result serialization for failure case
    error_data = job.serialize_result(
        None,
        JobStatus.FAILED,
        error="Test error",
        traceback_str="Test traceback"
    )
    error_result = job.deserialize_result(error_data)

    assert error_result["status"] == JobStatus.FAILED
    assert error_result["result"] is None
    assert error_result["error"] == "Test error"
    assert error_result["traceback"] == "Test traceback"


def test_simple_job_execution():
    """Test simple job execution preparation and validation."""
    def simple_task(x):
        return x * 2

    # Create and validate job
    job = Job(simple_task, args=(21,))
    
    # Verify job is properly configured for execution
    assert job.function == simple_task
    assert job.args == (21,)
    assert job.max_retries == 0  # default
    assert job.retry_delay == 0  # default
    assert not job.error
    assert not job.traceback
    
    # Verify function can be called as configured
    result = job.function(*job.args)
    assert result == 42  # Verify expected behavior