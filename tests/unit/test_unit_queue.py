import pytest
import pytest_asyncio
from unittest.mock import MagicMock, AsyncMock
from datetime import datetime, timedelta, timezone
import cloudpickle
from naq.queue.core import Queue
from naq.models import SCHEDULED_JOB_STATUS
from naq.settings import (
    NAQ_PREFIX,
    SCHEDULED_JOBS_KV_NAME
)

@pytest.mark.asyncio
class TestQueue:
    """Test cases for the Queue class."""
    
    @pytest_asyncio.fixture
    async def queue(self, mock_nats, mocker):
        """Setup a test queue with mocked NATS."""
        mock_nc, mock_js = mock_nats
        mocker.patch('naq.connection.get_nats_connection', return_value=mock_nc)
        mocker.patch('naq.connection.get_jetstream_context', return_value=mock_js)
        mocker.patch('naq.connection.ensure_stream')
        q = Queue(name="test")
        return q

    async def test_purge_queue(self, queue, mock_nats):
        """Test purging all jobs from the queue."""
        mock_nc, mock_js = mock_nats
        
        purged_count = await queue.purge()
        assert purged_count == 5
        
        mock_js.purge_stream.assert_awaited_once_with(
            name=queue.stream_name,
            subject=f"{NAQ_PREFIX}.queue.test"
        )

    async def test_invalid_retry_delay(self, queue):
        """Test that negative retry delay raises an error."""
        def sample_func(): pass
        
        with pytest.raises(ValueError, match=r"retry_delay .* negative"):
            await queue.enqueue(sample_func, retry_delay=-1)

    async def test_invalid_max_retries(self, queue):
        """Test that negative max_retries raises an error."""
        def sample_func(): pass
        
        with pytest.raises(ValueError, match=r"max_retries .* negative"):
            await queue.enqueue(sample_func, max_retries=-1)

    async def test_invalid_queue_name(self, mock_nats):
        """Test queue creation with invalid name raises error."""
        with pytest.raises(ValueError, match=r"Queue name .* empty"):
            Queue(name="")

        with pytest.raises(ValueError, match=r"Queue name .* invalid"):
            Queue(name="invalid/name")

    async def test_enqueue_with_retries(self, queue, mock_nats):
        """Test enqueueing a job with retry configuration."""
        mock_nc, mock_js = mock_nats
        
        def sample_func():
            pass
        
        job = await queue.enqueue(sample_func, max_retries=3, retry_delay=60)
        
        # Check that the job object has the correct retry settings
        assert job.max_retries == 3
        assert job.retry_delay == 60
        
        # Verify that the job was published (the actual retry logic is handled by the worker)
        mock_js.publish.assert_awaited_once()
        published_payload = mock_js.publish.await_args.kwargs.get('payload')
        assert isinstance(published_payload, bytes)
        # Further payload inspection could be done here if needed

    async def test_enqueue_at(self, queue, mock_nats):
        """Test scheduling a job for a specific time using enqueue_at."""
        mock_nc, mock_js = mock_nats
        
        def sample_func():
            pass
        
        run_at_datetime = datetime.now(timezone.utc) + timedelta(hours=1)
        job = await queue.enqueue_at(run_at_datetime, sample_func)
        
        assert job.job_id is not None
        assert job.queue_name == queue.name
        
        # Verify that the job was stored in the KV store, not published directly
        mock_js.publish.assert_not_awaited()
        mock_js.key_value.assert_awaited_once_with(bucket=SCHEDULED_JOBS_KV_NAME)
        
        kv_mock = await mock_js.key_value(bucket=SCHEDULED_JOBS_KV_NAME)
        kv_mock.put.assert_awaited_once()
        
        # Check the data put into KV store
        put_args = kv_mock.put.await_args
        assert put_args.args[0] == job.job_id.encode("utf-8") # key is job_id
        
        # Deserialize and check schedule_data
        schedule_data = cloudpickle.loads(put_args.args[1])
        assert schedule_data["job_id"] == job.job_id
        assert schedule_data["queue_name"] == queue.name
        assert abs(schedule_data["scheduled_timestamp_utc"] - run_at_datetime.timestamp()) < 1 # Check timestamp within a second

    async def test_enqueue_in(self, queue, mock_nats):
        """Test scheduling a job with a delay using enqueue_in."""
        mock_nc, mock_js = mock_nats
        
        def sample_func():
            pass
            
        delay_timedelta = timedelta(minutes=30)
        expected_run_time = datetime.now(timezone.utc) + delay_timedelta
        
        job = await queue.enqueue_in(delay_timedelta, sample_func)
        
        assert job.job_id is not None
        assert job.queue_name == queue.name
        
        mock_js.publish.assert_not_awaited()
        mock_js.key_value.assert_awaited_once_with(bucket=SCHEDULED_JOBS_KV_NAME)
        
        kv_mock = await mock_js.key_value(bucket=SCHEDULED_JOBS_KV_NAME)
        kv_mock.put.assert_awaited_once()
        
        put_args = kv_mock.put.await_args
        schedule_data = cloudpickle.loads(put_args.args[1])
        
        assert schedule_data["job_id"] == job.job_id
        # Check that the scheduled time is approximately correct
        assert abs(schedule_data["scheduled_timestamp_utc"] - expected_run_time.timestamp()) < 5 # Allow 5s diff for processing

    async def test_cancel_scheduled_job(self, queue, mock_nats):
        """Test canceling a scheduled job."""
        mock_nc, mock_js = mock_nats
        
        def sample_func(): pass
        run_at_datetime = datetime.now(timezone.utc) + timedelta(hours=1)
        job = await queue.enqueue_at(run_at_datetime, sample_func) # This will mock a put
        
        # Reset put mock for the cancel call if it uses put (it uses delete)
        kv_mock = await mock_js.key_value(bucket=SCHEDULED_JOBS_KV_NAME)
        kv_mock.put.reset_mock()
        
        # Mock KV get to return the entry we "put"
        mock_kv_entry = MagicMock()
        mock_kv_entry.value = cloudpickle.dumps({
            "job_id": job.job_id, "queue_name": queue.name, "status": "active",
            "scheduled_timestamp_utc": run_at_datetime.timestamp(),
            "function": cloudpickle.dumps(sample_func), "args": cloudpickle.dumps(()), "kwargs": cloudpickle.dumps({})
        })
        mock_kv_entry.key = job.job_id.encode("utf-8")
        mock_kv_entry.revision = 1
        kv_mock.get = AsyncMock(return_value=mock_kv_entry)
        
        cancel_result = await queue.cancel_scheduled_job(job.job_id)
        assert cancel_result is True
        
        kv_mock.delete.assert_awaited_once_with(
            job.job_id.encode("utf-8"),
            purge=True
        )

    async def test_pause_scheduled_job(self, queue, mock_nats):
        """Test pausing a scheduled job."""
        mock_nc, mock_js = mock_nats

        def sample_func(): pass
        run_at_datetime = datetime.now(timezone.utc) + timedelta(hours=1)
        job = await queue.enqueue_at(run_at_datetime, sample_func)

        kv_mock = await mock_js.key_value(bucket=SCHEDULED_JOBS_KV_NAME)
        
        # Mock KV get to return the entry we "put"
        original_schedule_data = {
            "job_id": job.job_id, "queue_name": queue.name, "status": SCHEDULED_JOB_STATUS.ACTIVE,
            "scheduled_timestamp_utc": run_at_datetime.timestamp(),
            "function": cloudpickle.dumps(sample_func), "args": cloudpickle.dumps(()), "kwargs": cloudpickle.dumps({})
        }
        mock_kv_entry = MagicMock()
        mock_kv_entry.value = cloudpickle.dumps(original_schedule_data)
        mock_kv_entry.key = job.job_id.encode("utf-8")
        mock_kv_entry.revision = 1
        kv_mock.get = AsyncMock(return_value=mock_kv_entry)
        
        pause_result = await queue.pause_scheduled_job(job.job_id)
        assert pause_result is True
        
        kv_mock.update.assert_awaited_once()
        update_args = kv_mock.update.await_args
        assert update_args.args[0] == job.job_id.encode("utf-8") # key
        
        updated_schedule_data = cloudpickle.loads(update_args.args[1]) # value
        assert updated_schedule_data["status"] == SCHEDULED_JOB_STATUS.PAUSED
        assert update_args.kwargs.get("last") == mock_kv_entry.revision # revision check

    async def test_resume_scheduled_job(self, queue, mock_nats):
        """Test resuming a paused scheduled job."""
        mock_nc, mock_js = mock_nats

        def sample_func(): pass
        run_at_datetime = datetime.now(timezone.utc) + timedelta(hours=1)
        job = await queue.enqueue_at(run_at_datetime, sample_func) # This will mock a put

        kv_mock = await mock_js.key_value(bucket=SCHEDULED_JOBS_KV_NAME)
        
        # Mock KV get to return a "paused" entry
        original_schedule_data = {
            "job_id": job.job_id, "queue_name": queue.name, "status": SCHEDULED_JOB_STATUS.PAUSED,
            "scheduled_timestamp_utc": run_at_datetime.timestamp(),
            "function": cloudpickle.dumps(sample_func), "args": cloudpickle.dumps(()), "kwargs": cloudpickle.dumps({})
        }
        mock_kv_entry = MagicMock()
        mock_kv_entry.value = cloudpickle.dumps(original_schedule_data)
        mock_kv_entry.key = job.job_id.encode("utf-8")
        mock_kv_entry.revision = 2 # Simulate a revision increment
        kv_mock.get = AsyncMock(return_value=mock_kv_entry)
        
        resume_result = await queue.resume_scheduled_job(job.job_id)
        assert resume_result is True
        
        kv_mock.update.assert_awaited_once()
        update_args = kv_mock.update.await_args
        assert update_args.args[0] == job.job_id.encode("utf-8") # key
        
        updated_schedule_data = cloudpickle.loads(update_args.args[1]) # value
        assert updated_schedule_data["status"] == SCHEDULED_JOB_STATUS.ACTIVE
        assert update_args.kwargs.get("last") == mock_kv_entry.revision # revision check

    async def test_overlapping_schedules(self, queue, mock_nats):
        """Test scheduling multiple jobs that overlap in time."""
        mock_nc, mock_js = mock_nats
        
        def task1(): pass
        def task2(): pass
        def task3(): pass
        
        # Schedule several jobs close together in time
        base_time = datetime.now(timezone.utc)
        schedule_times = [
            base_time + timedelta(minutes=30),  # task1 at base + 30min
            base_time + timedelta(minutes=30, seconds=1),  # task2 at base + 30min + 1sec
            base_time + timedelta(minutes=30, seconds=2),  # task3 at base + 30min + 2sec
        ]
        
        # Schedule all jobs
        kv_mock = await mock_js.key_value(bucket=SCHEDULED_JOBS_KV_NAME)
        
        # Create list to store schedule data for verification
        stored_schedules = []
        
        # Schedule the jobs
        jobs = [
            await queue.enqueue_at(schedule_times[0], task1),
            await queue.enqueue_at(schedule_times[1], task2),
            await queue.enqueue_at(schedule_times[2], task3),
        ]
        
        # Verify each job was stored correctly
        assert len(kv_mock.put.mock_calls) == 3, "Expected three jobs to be scheduled"
        
        # Collect all stored schedule data
        for i, call_args in enumerate(kv_mock.put.call_args_list):
            # Get stored data
            stored_data = cloudpickle.loads(call_args.args[1])
            stored_schedules.append(stored_data)
            
            # Verify basic job properties
            assert stored_data["job_id"] == jobs[i].job_id
            assert stored_data["queue_name"] == queue.name
            assert stored_data["status"] == SCHEDULED_JOB_STATUS.ACTIVE
            
            # Verify schedule time matches (within 1 second tolerance)
            expected_ts = schedule_times[i].timestamp()
            actual_ts = stored_data["scheduled_timestamp_utc"]
            assert abs(actual_ts - expected_ts) < 1, f"Job {i+1} schedule time mismatch"
            
            # Verify jobs are ordered correctly by timestamp
            if i > 0:
                prev_ts = stored_schedules[i-1]["scheduled_timestamp_utc"]
                assert actual_ts > prev_ts, f"Job {i+1} should be scheduled after job {i}"
        
        # Verify jobs were not published immediately
        mock_js.publish.assert_not_awaited()

    async def test_retry_configuration_validation(self, queue, mock_nats):
        """Test validation of retry configuration."""
        mock_nc, mock_js = mock_nats
        
        def sample_func(): pass
        
        # Test max retries validation
        with pytest.raises(ValueError, match="max_retries cannot be negative"):
            await queue.enqueue(sample_func, max_retries=-1)
        
        with pytest.raises(ValueError, match="retry_delay cannot be negative"):
            await queue.enqueue(sample_func, retry_delay=-1)
            
        # Test invalid retry delay type
        with pytest.raises(TypeError, match="retry_delay must be"):
            await queue.enqueue(sample_func, retry_delay="30")  # String instead of number
            
        # Verify valid configurations work
        job = await queue.enqueue(sample_func, max_retries=3, retry_delay=30)
        assert job.max_retries == 3
        assert job.retry_delay == 30

    async def test_retry_strategy_exponential_backoff(self, queue, mock_nats):
        """Test exponential backoff retry strategy."""
        mock_nc, mock_js = mock_nats
        
        def sample_func(): pass
        
        base_delay = 5  # 5 seconds base delay
        job = await queue.enqueue(
            sample_func, 
            max_retries=3,
            retry_delay=base_delay,
            retry_strategy="exponential"
        )
        
        assert job.retry_strategy == "exponential"
        assert job.retry_delay == base_delay
        
        # Verify serialized job data includes retry strategy
        mock_js.publish.assert_awaited_once()
        published_data = mock_js.publish.await_args.kwargs.get('payload')
        job_data = cloudpickle.loads(published_data)
        assert job_data["retry_strategy"] == "exponential"
        assert job_data["retry_delay"] == base_delay

    async def test_retry_strategy_linear_backoff(self, queue, mock_nats):
        """Test linear backoff retry strategy."""
        mock_nc, mock_js = mock_nats
        
        def sample_func(): pass
        
        base_delay = 10  # 10 seconds base delay
        job = await queue.enqueue(
            sample_func,
            max_retries=3,
            retry_delay=base_delay,
            retry_strategy="linear"
        )
        
        assert job.retry_strategy == "linear"
        assert job.retry_delay == base_delay
        
        # Verify serialized job data
        mock_js.publish.assert_awaited_once()
        published_data = mock_js.publish.await_args.kwargs.get('payload')
        job_data = cloudpickle.loads(published_data)
        assert job_data["retry_strategy"] == "linear"
        assert job_data["retry_delay"] == base_delay

    async def test_invalid_retry_strategy(self, queue, mock_nats):
        """Test invalid retry strategy handling."""
        mock_nc, mock_js = mock_nats
        
        def sample_func(): pass
        
        # Test with invalid strategy name
        with pytest.raises(ValueError, match="Invalid retry strategy"):
            await queue.enqueue(
                sample_func,
                max_retries=3,
                retry_delay=5,
                retry_strategy="invalid_strategy"
            )

    async def test_retry_with_custom_exception_handling(self, queue, mock_nats):
        """Test retry configuration with custom exception handling."""
        mock_nc, mock_js = mock_nats
        
        def sample_func(): pass
        
        # Set up custom retry conditions
        retry_on = (ValueError, KeyError)  # Retry on these exceptions
        ignore_on = (TypeError,)  # Don't retry on these
        
        job = await queue.enqueue(
            sample_func,
            max_retries=3,
            retry_delay=5,
            retry_on=retry_on,
            ignore_on=ignore_on
        )
        
        # Verify serialized job data includes exception handling config
        mock_js.publish.assert_awaited_once()
        published_data = mock_js.publish.await_args.kwargs.get('payload')
        job_data = cloudpickle.loads(published_data)
        
        # Verify exception class names are stored
        assert job_data["retry_on"] == ["ValueError", "KeyError"]
        assert job_data["ignore_on"] == ["TypeError"]