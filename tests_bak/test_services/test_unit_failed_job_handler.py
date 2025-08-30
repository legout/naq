import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from naq.worker.failed import FailedJobHandler
from naq.models.jobs import Job
from naq.models.enums import JOB_STATUS
from naq.exceptions import SerializationError
from naq.services.connection import ConnectionService
from naq.services.streams import StreamService
from naq.services.events import EventService


@pytest_asyncio.fixture
def mock_service_manager():
    """Fixture for a mock service manager instance."""
    service_manager = AsyncMock()
    service_manager.get_service = AsyncMock()
    return service_manager


@pytest_asyncio.fixture
def mock_connection_service():
    """Fixture for a mock connection service."""
    service = AsyncMock(spec=ConnectionService)
    service.publish = AsyncMock()
    return service


@pytest_asyncio.fixture
def mock_stream_service():
    """Fixture for a mock stream service."""
    service = AsyncMock(spec=StreamService)
    service.ensure_stream = AsyncMock()
    return service


@pytest_asyncio.fixture
def mock_event_service():
    """Fixture for a mock event service."""
    service = AsyncMock(spec=EventService)
    service.log_event = AsyncMock()
    return service


@pytest_asyncio.fixture
def failed_job_handler(mock_service_manager):
    """Fixture for a FailedJobHandler instance."""
    return FailedJobHandler(service_manager=mock_service_manager)


@pytest.mark.asyncio
async def test_failed_job_handler_init(failed_job_handler, mock_service_manager):
    """Test FailedJobHandler initialization."""
    assert failed_job_handler._service_manager == mock_service_manager
    assert failed_job_handler._connection_service is None
    assert failed_job_handler._stream_service is None
    assert failed_job_handler._event_service is None


@pytest.mark.asyncio
async def test_get_services(failed_job_handler, mock_service_manager,
                           mock_connection_service, mock_stream_service, mock_event_service):
    """Test _get_services method."""
    # Setup mocks
    mock_service_manager.get_service.side_effect = [
        mock_connection_service,
        mock_stream_service,
        mock_event_service
    ]
    
    # Call _get_services
    await failed_job_handler._get_services()
    
    # Verify services were retrieved
    assert failed_job_handler._connection_service == mock_connection_service
    assert failed_job_handler._stream_service == mock_stream_service
    assert failed_job_handler._event_service == mock_event_service
    
    # Verify service_manager.get_service was called with correct arguments
    expected_calls = [
        (("connection", ConnectionService), {}),
        (("stream", StreamService), {}),
        (("event", EventService), {})
    ]
    mock_service_manager.get_service.assert_has_calls(expected_calls)


@pytest.mark.asyncio
async def test_handle_failed_job_success(failed_job_handler, mock_service_manager,
                                        mock_connection_service, mock_stream_service, mock_event_service):
    """Test successful handling of a failed job."""
    # Setup mocks
    mock_service_manager.get_service.side_effect = [
        mock_connection_service,
        mock_stream_service,
        mock_event_service
    ]
    
    # Create a test job
    job = Job(
        function=lambda: "test",
        job_id="test_job",
        queue_name="test_queue",
        args=(),
        kwargs={},
        error="Test error"
    )
    
    # Call handle_failed_job
    await failed_job_handler.handle_failed_job(job)
    
    # Verify services were retrieved
    assert failed_job_handler._connection_service == mock_connection_service
    assert failed_job_handler._stream_service == mock_stream_service
    assert failed_job_handler._event_service == mock_event_service
    
    # Verify stream was ensured
    mock_stream_service.ensure_stream.assert_called_once()
    
    # Verify publish was called with correct arguments
    expected_subject = "naq.failed.test_queue"
    mock_connection_service.publish.assert_called_once_with(
        expected_subject,
        job.serialize_failed_job()
    )
    
    # Verify event was logged
    mock_event_service.log_event.assert_called_once()


@pytest.mark.asyncio
async def test_handle_failed_job_service_error(failed_job_handler, mock_service_manager):
    """Test handling a failed job when service retrieval fails."""
    # Setup mock to raise exception
    mock_service_manager.get_service.side_effect = Exception("Service not available")
    
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
    
    # Verify services were not set
    assert failed_job_handler._connection_service is None
    assert failed_job_handler._stream_service is None
    assert failed_job_handler._event_service is None


@pytest.mark.asyncio
async def test_handle_failed_job_stream_error(failed_job_handler, mock_service_manager,
                                             mock_connection_service, mock_stream_service, mock_event_service):
    """Test handling a failed job when stream creation fails."""
    # Setup mocks
    mock_service_manager.get_service.side_effect = [
        mock_connection_service,
        mock_stream_service,
        mock_event_service
    ]
    mock_stream_service.ensure_stream.side_effect = Exception("Stream creation failed")
    
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
    
    # Verify publish was not called due to the stream creation error
    mock_connection_service.publish.assert_not_called()


@pytest.mark.asyncio
async def test_publish_failed_job_success(failed_job_handler, mock_service_manager,
                                         mock_connection_service, mock_stream_service, mock_event_service):
    """Test successful publishing of a failed job."""
    # Setup mocks
    mock_service_manager.get_service.side_effect = [
        mock_connection_service,
        mock_stream_service,
        mock_event_service
    ]
    
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
    mock_connection_service.publish.assert_called_once_with(
        expected_subject,
        job.serialize_failed_job()
    )


@pytest.mark.asyncio
async def test_publish_failed_job_no_queue_name(failed_job_handler, mock_service_manager,
                                               mock_connection_service, mock_stream_service, mock_event_service):
    """Test publishing a failed job with no queue name."""
    # Setup mocks
    mock_service_manager.get_service.side_effect = [
        mock_connection_service,
        mock_stream_service,
        mock_event_service
    ]
    
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
    mock_connection_service.publish.assert_called_once_with(
        expected_subject,
        job.serialize_failed_job()
    )


@pytest.mark.asyncio
async def test_publish_failed_job_service_error(failed_job_handler, mock_service_manager):
    """Test publishing a failed job when service retrieval fails."""
    # Setup mock to raise exception
    mock_service_manager.get_service.side_effect = Exception("Service not available")
    
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
    
    # Verify services were not set
    assert failed_job_handler._connection_service is None
    assert failed_job_handler._stream_service is None
    assert failed_job_handler._event_service is None


@pytest.mark.asyncio
async def test_publish_failed_job_serialization_error(failed_job_handler, mock_service_manager,
                                                     mock_connection_service, mock_stream_service, mock_event_service):
    """Test publishing a failed job when serialization fails."""
    # Setup mocks
    mock_service_manager.get_service.side_effect = [
        mock_connection_service,
        mock_stream_service,
        mock_event_service
    ]
    
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
        mock_connection_service.publish.assert_not_called()


@pytest.mark.asyncio
async def test_publish_failed_job_publish_error(failed_job_handler, mock_service_manager,
                                              mock_connection_service, mock_stream_service, mock_event_service):
    """Test publishing a failed job when publish fails."""
    # Setup mocks
    mock_service_manager.get_service.side_effect = [
        mock_connection_service,
        mock_stream_service,
        mock_event_service
    ]
    mock_connection_service.publish.side_effect = Exception("Publish failed")
    
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
    
    # Verify publish was called despite the error
    expected_subject = "naq.failed.test_queue"
    mock_connection_service.publish.assert_called_once_with(
        expected_subject,
        job.serialize_failed_job()
    )


@pytest.mark.asyncio
async def test_ensure_failed_stream_success(failed_job_handler, mock_service_manager,
                                          mock_connection_service, mock_stream_service, mock_event_service):
    """Test successful ensuring of failed job stream."""
    # Setup mocks
    mock_service_manager.get_service.side_effect = [
        mock_connection_service,
        mock_stream_service,
        mock_event_service
    ]
    
    await failed_job_handler._ensure_failed_stream()
    
    # Verify ensure_stream was called with correct arguments
    from naq.settings import FAILED_JOB_STREAM_NAME, FAILED_JOB_SUBJECT_PREFIX
    
    mock_stream_service.ensure_stream.assert_called_once()


@pytest.mark.asyncio
async def test_ensure_failed_stream_service_error(failed_job_handler, mock_service_manager):
    """Test ensuring failed job stream when service retrieval fails."""
    # Setup mock to raise exception
    mock_service_manager.get_service.side_effect = Exception("Service not available")
    
    # Should not raise an exception, but should log an error
    await failed_job_handler._ensure_failed_stream()
    
    # Verify services were not set
    assert failed_job_handler._connection_service is None
    assert failed_job_handler._stream_service is None
    assert failed_job_handler._event_service is None


@pytest.mark.asyncio
async def test_ensure_failed_stream_error(failed_job_handler, mock_service_manager,
                                         mock_connection_service, mock_stream_service, mock_event_service):
    """Test ensuring failed job stream when an error occurs."""
    # Setup mocks
    mock_service_manager.get_service.side_effect = [
        mock_connection_service,
        mock_stream_service,
        mock_event_service
    ]
    mock_stream_service.ensure_stream.side_effect = Exception("Stream creation failed")
    
    # Should not raise an exception, but should log an error
    await failed_job_handler._ensure_failed_stream()
    
    # Verify ensure_stream was called despite the error
    mock_stream_service.ensure_stream.assert_called_once()