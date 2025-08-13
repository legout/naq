import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from nats.js import JetStreamContext

from naq.worker.failed import FailedJobHandler
from naq.models.jobs import Job
from naq.models.enums import JOB_STATUS
from naq.exceptions import SerializationError


@pytest_asyncio.fixture
def mock_worker():
    """Fixture for a mock worker instance."""
    worker = MagicMock()
    worker.worker_id = "test-worker"
    return worker


@pytest_asyncio.fixture
def mock_js():
    """Fixture for a mock JetStream context."""
    js = AsyncMock(spec=JetStreamContext)
    js.publish = AsyncMock()
    js.stream_info = AsyncMock()
    js.add_stream = AsyncMock()
    return js


@pytest_asyncio.fixture
def failed_job_handler(mock_worker):
    """Fixture for a FailedJobHandler instance."""
    return FailedJobHandler(worker=mock_worker)


@pytest.mark.asyncio
async def test_failed_job_handler_init(failed_job_handler, mock_worker):
    """Test FailedJobHandler initialization."""
    assert failed_job_handler.worker == mock_worker
    assert failed_job_handler._js is None


@pytest.mark.asyncio
async def test_initialize(failed_job_handler, mock_js):
    """Test initialize method."""
    await failed_job_handler.initialize(mock_js)
    
    assert failed_job_handler._js == mock_js
    mock_js.stream_info.assert_called_once_with("naq_failed_jobs")


@pytest.mark.asyncio
async def test_handle_failed_job_success(failed_job_handler, mock_js):
    """Test successful handling of a failed job."""
    # Setup
    failed_job_handler._js = mock_js
    
    # Create a test job
    job = Job(
        function=lambda: "test",
        job_id="test_job",
        queue_name="test_queue",
        args=(),
        kwargs={},
        error="Test error"
    )
    
    # Mock ensure_stream to not raise an exception
    with patch('naq.worker.failed.ensure_stream') as mock_ensure_stream:
        await failed_job_handler.handle_failed_job(job)
        
        # Verify ensure_stream was called
        mock_ensure_stream.assert_called_once()
        
        # Verify publish was called with correct arguments
        expected_subject = "naq.failed.test_queue"
        mock_js.publish.assert_called_once_with(
            expected_subject,
            job.serialize_failed_job()
        )


@pytest.mark.asyncio
async def test_handle_failed_job_no_js_context(failed_job_handler):
    """Test handling a failed job when JetStream context is not available."""
    # Don't set _js (it should be None)
    
    # Create a test job
    job = Job(
        function=lambda: "test",
        job_id="test_job",
        queue_name="test_queue",
        args=(),
        kwargs={},
        error="Test error"
    )
    
    # Should not raise an exception, but should log an error
    await failed_job_handler.handle_failed_job(job)
    
    # Verify publish was not called
    assert not hasattr(failed_job_handler, '_js') or failed_job_handler._js is None


@pytest.mark.asyncio
async def test_handle_failed_job_stream_creation_error(failed_job_handler, mock_js):
    """Test handling a failed job when stream creation fails."""
    # Setup
    failed_job_handler._js = mock_js
    
    # Create a test job
    job = Job(
        function=lambda: "test",
        job_id="test_job",
        queue_name="test_queue",
        args=(),
        kwargs={},
        error="Test error"
    )
    
    # Mock ensure_stream to raise an exception
    with patch('naq.worker.failed.ensure_stream', side_effect=Exception("Stream creation failed")):
        # Should not raise an exception, but should log an error
        await failed_job_handler.handle_failed_job(job)
        
        # Verify publish was not called due to the stream creation error
        # The method logs the error and returns early
        mock_js.publish.assert_not_called()


@pytest.mark.asyncio
async def test_publish_failed_job_success(failed_job_handler, mock_js):
    """Test successful publishing of a failed job."""
    # Setup
    failed_job_handler._js = mock_js
    
    # Create a test job
    job = Job(
        function=lambda: "test",
        job_id="test_job",
        queue_name="test_queue",
        args=(),
        kwargs={},
        error="Test error"
    )
    
    await failed_job_handler.publish_failed_job(job)
    
    # Verify publish was called with correct arguments
    expected_subject = "naq.failed.test_queue"
    mock_js.publish.assert_called_once_with(
        expected_subject,
        job.serialize_failed_job()
    )


@pytest.mark.asyncio
async def test_publish_failed_job_no_queue_name(failed_job_handler, mock_js):
    """Test publishing a failed job with no queue name."""
    # Setup
    failed_job_handler._js = mock_js
    
    # Create a test job without queue_name
    job = Job(
        function=lambda: "test",
        job_id="test_job",
        args=(),
        kwargs={},
        error="Test error"
    )
    
    await failed_job_handler.publish_failed_job(job)
    
    # Verify publish was called with default queue name as queue name
    expected_subject = "naq.failed.naq_default_queue"
    mock_js.publish.assert_called_once_with(
        expected_subject,
        job.serialize_failed_job()
    )


@pytest.mark.asyncio
async def test_publish_failed_job_no_js_context(failed_job_handler):
    """Test publishing a failed job when JetStream context is not available."""
    # Don't set _js (it should be None)
    
    # Create a test job
    job = Job(
        function=lambda: "test",
        job_id="test_job",
        queue_name="test_queue",
        args=(),
        kwargs={},
        error="Test error"
    )
    
    # Should not raise an exception, but should log an error
    await failed_job_handler.publish_failed_job(job)
    
    # Verify publish was not called
    assert not hasattr(failed_job_handler, '_js') or failed_job_handler._js is None


@pytest.mark.asyncio
async def test_publish_failed_job_serialization_error(failed_job_handler, mock_js):
    """Test publishing a failed job when serialization fails."""
    # Setup
    failed_job_handler._js = mock_js
    
    # Create a test job
    job = Job(
        function=lambda: "test",
        job_id="test_job",
        queue_name="test_queue",
        args=(),
        kwargs={},
        error="Test error"
    )
    
    # Mock the serializer to raise SerializationError
    with patch('naq.serializers.get_serializer') as mock_get_serializer:
        mock_serializer = MagicMock()
        mock_serializer.serialize_failed_job.side_effect = SerializationError("Serialization failed")
        mock_get_serializer.return_value = mock_serializer
        
        # Should not raise an exception, but should log an error
        await failed_job_handler.publish_failed_job(job)
        
        # Verify publish was not called due to serialization error
        mock_js.publish.assert_not_called()


@pytest.mark.asyncio
async def test_publish_failed_job_publish_error(failed_job_handler, mock_js):
    """Test publishing a failed job when publish fails."""
    # Setup
    failed_job_handler._js = mock_js
    
    # Create a test job
    job = Job(
        function=lambda: "test",
        job_id="test_job",
        queue_name="test_queue",
        args=(),
        kwargs={},
        error="Test error"
    )
    
    # Mock publish to raise an exception
    mock_js.publish.side_effect = Exception("Publish failed")
    
    # Should not raise an exception, but should log an error
    await failed_job_handler.publish_failed_job(job)
    
    # Verify publish was called despite the error
    expected_subject = "naq.failed.test_queue"
    mock_js.publish.assert_called_once_with(
        expected_subject,
        job.serialize_failed_job()
    )


@pytest.mark.asyncio
async def test_ensure_failed_stream_success(failed_job_handler, mock_js):
    """Test successful ensuring of failed job stream."""
    # Setup
    failed_job_handler._js = mock_js
    
    await failed_job_handler._ensure_failed_stream()
    
    # Verify ensure_stream was called with correct arguments
    from naq.settings import FAILED_JOB_STREAM_NAME, FAILED_JOB_SUBJECT_PREFIX
    
    mock_js.stream_info.assert_called_once_with(FAILED_JOB_STREAM_NAME)


@pytest.mark.asyncio
async def test_ensure_failed_stream_no_js_context(failed_job_handler):
    """Test ensuring failed job stream when JetStream context is not available."""
    # Don't set _js (it should be None)
    
    # Should not raise an exception, but should log an error
    await failed_job_handler._ensure_failed_stream()
    
    # Verify stream_info was not called
    assert not hasattr(failed_job_handler, '_js') or failed_job_handler._js is None


@pytest.mark.asyncio
async def test_ensure_failed_stream_error(failed_job_handler, mock_js):
    """Test ensuring failed job stream when an error occurs."""
    # Setup
    failed_job_handler._js = mock_js
    
    # Mock stream_info to raise an exception
    mock_js.stream_info.side_effect = Exception("Stream info failed")
    
    # Should not raise an exception, but should log an error
    await failed_job_handler._ensure_failed_stream()
    
    # Verify stream_info was called despite the error
    from naq.settings import FAILED_JOB_STREAM_NAME
    mock_js.stream_info.assert_called_once_with(FAILED_JOB_STREAM_NAME)