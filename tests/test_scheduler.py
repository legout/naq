# tests/test_scheduler.py
import asyncio
import signal
import time
import uuid
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

import anyio
import pytest

from naq.exceptions import NaqConnectionError
from naq.scheduler import Scheduler
from naq.nats_client import NatsClient


@pytest.fixture
def mock_nats_client():
    """Create a mock NatsClient for testing."""
    client = AsyncMock(spec=NatsClient)
    client.connect = AsyncMock()
    client.close = AsyncMock()
    client.get_kv_store = AsyncMock()
    client.get_jetstream = AsyncMock()
    return client


@pytest.fixture
def mock_kv_store():
    """Create a mock KV store for testing."""
    kv_store = AsyncMock()
    kv_store.get = AsyncMock()
    kv_store.put = AsyncMock()
    kv_store.delete = AsyncMock()
    kv_store.keys = AsyncMock()
    return kv_store


@pytest.fixture
def mock_jetstream():
    """Create a mock JetStream for testing."""
    jetstream = AsyncMock()
    jetstream.publish = AsyncMock()
    jetstream.subscribe = AsyncMock()
    return jetstream


@pytest.fixture
def scheduler_with_nats_client(mock_nats_client):
    """Create a Scheduler instance with a mock NatsClient."""
    return Scheduler(
        nats_client=mock_nats_client,
        poll_interval=0.1,  # Short interval for testing
        instance_id=f"test-instance-{uuid.uuid4().hex[:8]}",
        enable_ha=True
    )


@pytest.fixture
def scheduler_with_nats_url():
    """Create a Scheduler instance with a NATS URL."""
    return Scheduler(
        nats_url="nats://localhost:4222",
        poll_interval=0.1,  # Short interval for testing
        instance_id=f"test-instance-{uuid.uuid4().hex[:8]}",
        enable_ha=True
    )


def test_scheduler_initialization_with_nats_client(mock_nats_client):
    """Test Scheduler initialization with NatsClient."""
    scheduler = Scheduler(
        nats_client=mock_nats_client,
        poll_interval=0.5,
        instance_id="test-instance",
        enable_ha=False
    )
    
    assert scheduler._nats_client is mock_nats_client
    assert scheduler._nats_url is None
    assert scheduler._poll_interval == 0.5
    assert scheduler._instance_id == "test-instance"
    assert scheduler._enable_ha is False
    assert scheduler._running is False
    assert scheduler._leader_election is not None


def test_scheduler_initialization_with_nats_url():
    """Test Scheduler initialization with NATS URL."""
    scheduler = Scheduler(
        nats_url="nats://localhost:4222",
        poll_interval=0.5,
        instance_id="test-instance",
        enable_ha=False
    )
    
    assert scheduler._nats_url == "nats://localhost:4222"
    assert scheduler._nats_client is None
    assert scheduler._poll_interval == 0.5
    assert scheduler._instance_id == "test-instance"
    assert scheduler._enable_ha is False


def test_scheduler_initialization_without_connection_params():
    """Test Scheduler initialization without connection parameters."""
    with pytest.raises(ValueError, match="Either nats_url or nats_client must be provided"):
        Scheduler()


def test_scheduler_initialization_generates_instance_id(mock_nats_client):
    """Test that Scheduler generates an instance ID if none provided."""
    scheduler = Scheduler(nats_client=mock_nats_client)
    
    assert scheduler._instance_id is not None
    assert isinstance(scheduler._instance_id, str)
    assert len(scheduler._instance_id) > 0


@pytest.mark.asyncio
async def test_connect_with_nats_client(
    scheduler_with_nats_client,
    mock_nats_client,
    mock_kv_store,
    mock_jetstream
):
    """Test _connect with NatsClient."""
    # Setup mocks
    mock_nats_client.get_kv_store.return_value = mock_kv_store
    mock_nats_client.get_jetstream.return_value = mock_jetstream
    
    await scheduler_with_nats_client._connect()
    
    # Verify client was connected
    mock_nats_client.connect.assert_called_once()
    
    # Verify services were retrieved
    assert scheduler_with_nats_client._nats_client is mock_nats_client
    assert scheduler_with_nats_client._kv_store is mock_kv_store
    assert scheduler_with_nats_client._jetstream is mock_jetstream
    
    # Verify leader election was initialized
    assert scheduler_with_nats_client._leader_election._kv_store is mock_kv_store


@pytest.mark.asyncio
async def test_connect_with_nats_url(scheduler_with_nats_url):
    """Test _connect with NATS URL."""
    with patch('naq.scheduler.NatsClient') as mock_nats_client_class:
        # Setup mock client
        mock_nats_client = AsyncMock()
        mock_kv_store = AsyncMock()
        mock_jetstream = AsyncMock()
        
        mock_nats_client_class.return_value = mock_nats_client
        mock_nats_client.connect = AsyncMock()
        mock_nats_client.get_kv_store.return_value = mock_kv_store
        mock_nats_client.get_jetstream.return_value = mock_jetstream
        
        await scheduler_with_nats_url._connect()
        
        # Verify client was created
        mock_nats_client_class.assert_called_once_with(nats_url="nats://localhost:4222")
        
        # Verify client was connected
        mock_nats_client.connect.assert_called_once()
        
        # Verify services were retrieved
        assert scheduler_with_nats_url._nats_client is mock_nats_client
        assert scheduler_with_nats_url._kv_store is mock_kv_store
        assert scheduler_with_nats_url._jetstream is mock_jetstream


@pytest.mark.asyncio
async def test_connect_error_handling(scheduler_with_nats_client, mock_nats_client):
    """Test error handling in _connect."""
    mock_nats_client.connect.side_effect = Exception("Connection error")
    
    with pytest.raises(Exception, match="Connection error"):
        await scheduler_with_nats_client._connect()


@pytest.mark.asyncio
async def test_handle_leadership_transition_ha_enabled(
    scheduler_with_nats_client,
    mock_kv_store
):
    """Test _handle_leadership_transition with HA enabled."""
    # Setup mocks
    scheduler_with_nats_client._leader_election.try_become_leader = AsyncMock(return_value=True)
    scheduler_with_nats_client._leader_election.start_renewal_task = AsyncMock()
    
    # Mock is_leader property to return False initially
    with patch.object(scheduler_with_nats_client, 'is_leader', new_callable=PropertyMock) as mock_is_leader:
        mock_is_leader.side_effect = [False, True]  # First call returns False, second returns True
        
        await scheduler_with_nats_client._handle_leadership_transition()
        
        # Verify leader election was attempted
        scheduler_with_nats_client._leader_election.try_become_leader.assert_called_once()
        
        # Verify renewal task was started
        scheduler_with_nats_client._leader_election.start_renewal_task.assert_called_once_with(True)


@pytest.mark.asyncio
async def test_handle_leadership_transition_ha_disabled(scheduler_with_nats_client):
    """Test _handle_leadership_transition with HA disabled."""
    scheduler_with_nats_client._enable_ha = False
    
    with patch.object(scheduler_with_nats_client, 'is_leader', False):
        await scheduler_with_nats_client._handle_leadership_transition()
        
        # Verify leader election was set to True
        assert scheduler_with_nats_client._leader_election._is_leader is True


@pytest.mark.asyncio
async def test_handle_ha_leadership_become_leader(
    scheduler_with_nats_client,
    mock_kv_store
):
    """Test _handle_ha_leadership when becoming leader."""
    # Setup mocks
    scheduler_with_nats_client._leader_election.try_become_leader = AsyncMock(return_value=True)
    scheduler_with_nats_client._leader_election.start_renewal_task = AsyncMock()
    
    with patch.object(scheduler_with_nats_client, 'is_leader', False):
        await scheduler_with_nats_client._handle_ha_leadership(False)
        
        # Verify leader election was attempted
        scheduler_with_nats_client._leader_election.try_become_leader.assert_called_once()
        
        # Verify renewal task was started
        scheduler_with_nats_client._leader_election.start_renewal_task.assert_called_once_with(True)


@pytest.mark.asyncio
async def test_handle_ha_leadership_fail_to_become_leader(
    scheduler_with_nats_client,
    mock_kv_store
):
    """Test _handle_ha_leadership when failing to become leader."""
    # Setup mocks
    scheduler_with_nats_client._leader_election.try_become_leader = AsyncMock(return_value=False)
    
    with patch.object(scheduler_with_nats_client, 'is_leader', False):
        await scheduler_with_nats_client._handle_ha_leadership(False)
        
        # Verify leader election was attempted
        scheduler_with_nats_client._leader_election.try_become_leader.assert_called_once()
        
        # Verify renewal task was not started
        scheduler_with_nats_client._leader_election.start_renewal_task.assert_not_called()


@pytest.mark.asyncio
async def test_process_scheduled_jobs_as_leader(scheduler_with_nats_client, mock_jetstream):
    """Test _process_scheduled_jobs when instance is leader."""
    # Setup mocks
    scheduler_with_nats_client._jetstream = mock_jetstream
    
    with patch.object(scheduler_with_nats_client, 'is_leader', True):
        await scheduler_with_nats_client._process_scheduled_jobs()
        
        # Verify jobs were processed
        # Note: The actual implementation will need to be updated to use jetstream directly


@pytest.mark.asyncio
async def test_process_scheduled_jobs_as_follower(scheduler_with_nats_client, mock_jetstream):
    """Test _process_scheduled_jobs when instance is not leader."""
    scheduler_with_nats_client._jetstream = mock_jetstream
    
    with patch.object(scheduler_with_nats_client, 'is_leader', False):
        await scheduler_with_nats_client._process_scheduled_jobs()
        
        # Verify jobs were not processed
        # Note: The actual implementation will need to be updated to use jetstream directly


@pytest.mark.asyncio
async def test_process_scheduled_jobs_no_service(scheduler_with_nats_client):
    """Test _process_scheduled_jobs when jetstream is not available."""
    scheduler_with_nats_client._jetstream = None
    
    with patch.object(scheduler_with_nats_client, 'is_leader', True):
        await scheduler_with_nats_client._process_scheduled_jobs()
        
        # Should not raise an exception


@pytest.mark.asyncio
async def test_process_scheduled_jobs_error_handling(scheduler_with_nats_client, mock_jetstream):
    """Test error handling in _process_scheduled_jobs."""
    # Setup mocks
    mock_jetstream.publish.side_effect = Exception("Processing error")
    scheduler_with_nats_client._jetstream = mock_jetstream
    
    with patch.object(scheduler_with_nats_client, 'is_leader', True):
        # Should not raise an exception
        await scheduler_with_nats_client._process_scheduled_jobs()


@pytest.mark.asyncio
async def test_wait_for_next_cycle_shutdown_triggered(scheduler_with_nats_client):
    """Test _wait_for_next_cycle when shutdown is triggered."""
    scheduler_with_nats_client._shutdown_event.set()
    
    result = await scheduler_with_nats_client._wait_for_next_cycle(time.time())
    
    assert result is False


@pytest.mark.asyncio
async def test_wait_for_next_cycle_normal_wait(scheduler_with_nats_client):
    """Test _wait_for_next_cycle with normal wait."""
    cycle_start = time.time() - 0.05  # 50ms ago
    scheduler_with_nats_client._poll_interval = 0.1  # 100ms interval
    
    result = await scheduler_with_nats_client._wait_for_next_cycle(cycle_start)
    
    assert result is True


@pytest.mark.asyncio
async def test_wait_for_next_cycle_long_processing(scheduler_with_nats_client):
    """Test _wait_for_next_cycle when processing took longer than poll interval."""
    cycle_start = time.time() - 0.15  # 150ms ago
    scheduler_with_nats_client._poll_interval = 0.1  # 100ms interval
    
    result = await scheduler_with_nats_client._wait_for_next_cycle(cycle_start)
    
    assert result is True


@pytest.mark.asyncio
async def test_wait_for_next_cycle_shutdown_during_wait(scheduler_with_nats_client):
    """Test _wait_for_next_cycle when shutdown is triggered during wait."""
    cycle_start = time.time() - 0.05  # 50ms ago
    scheduler_with_nats_client._poll_interval = 0.2  # 200ms interval
    
    # Set shutdown event after a short delay
    async def set_shutdown_after_delay():
        await asyncio.sleep(0.1)
        scheduler_with_nats_client._shutdown_event.set()
    
    # Start the task to set shutdown event
    asyncio.create_task(set_shutdown_after_delay())
    
    result = await scheduler_with_nats_client._wait_for_next_cycle(cycle_start)
    
    assert result is False


@pytest.mark.asyncio
async def test_wait_for_next_cycle_error_handling(scheduler_with_nats_client):
    """Test error handling in _wait_for_next_cycle."""
    cycle_start = time.time() - 0.05  # 50ms ago
    scheduler_with_nats_client._poll_interval = 0.1  # 100ms interval
    
    # Mock anyio.move_on_after to raise an exception
    with patch('anyio.move_on_after', side_effect=Exception("Wait error")):
        result = await scheduler_with_nats_client._wait_for_next_cycle(cycle_start)
        
        assert result is True  # Should continue despite errors


@pytest.mark.asyncio
async def test_shutdown_normal_operation(
    scheduler_with_nats_client,
    mock_kv_store
):
    """Test normal shutdown operation."""
    # Setup mocks
    scheduler_with_nats_client._enable_ha = True
    scheduler_with_nats_client._leader_election.stop_renewal_task = AsyncMock()
    scheduler_with_nats_client._leader_election.release_lock = AsyncMock()
    
    await scheduler_with_nats_client._shutdown()
    
    # Verify leader election processes were stopped
    scheduler_with_nats_client._leader_election.stop_renewal_task.assert_called_once()
    scheduler_with_nats_client._leader_election.release_lock.assert_called_once()
    
    # Verify shutdown event is set
    assert scheduler_with_nats_client._shutdown_event.is_set()
    
    # Verify running flag is cleared
    assert scheduler_with_nats_client._running is False


@pytest.mark.asyncio
async def test_shutdown_ha_disabled(scheduler_with_nats_client):
    """Test shutdown when HA is disabled."""
    scheduler_with_nats_client._enable_ha = False
    
    await scheduler_with_nats_client._shutdown()
    
    # Verify shutdown event is set
    assert scheduler_with_nats_client._shutdown_event.is_set()
    
    # Verify running flag is cleared
    assert scheduler_with_nats_client._running is False


@pytest.mark.asyncio
async def test_shutdown_error_handling(scheduler_with_nats_client):
    """Test error handling in shutdown."""
    # Setup mocks to raise an exception
    scheduler_with_nats_client._enable_ha = True
    scheduler_with_nats_client._leader_election.stop_renewal_task = AsyncMock(side_effect=Exception("Stop error"))
    
    # Should not raise an exception
    await scheduler_with_nats_client._shutdown()
    
    # Verify shutdown event is set
    assert scheduler_with_nats_client._shutdown_event.is_set()
    
    # Verify running flag is cleared
    assert scheduler_with_nats_client._running is False


@pytest.mark.asyncio
async def test_close(scheduler_with_nats_client):
    """Test _close method."""
    # Set up services
    scheduler_with_nats_client._nats_client = AsyncMock()
    scheduler_with_nats_client._kv_store = AsyncMock()
    scheduler_with_nats_client._jetstream = AsyncMock()
    
    await scheduler_with_nats_client._close()
    
    # Verify services are cleared
    assert scheduler_with_nats_client._nats_client is None
    assert scheduler_with_nats_client._kv_store is None
    assert scheduler_with_nats_client._jetstream is None
    
    # Verify shutdown event is set
    assert scheduler_with_nats_client._shutdown_event.is_set()
    
    # Verify running flag is cleared
    assert scheduler_with_nats_client._running is False


@pytest.mark.asyncio
async def test_close_error_handling(scheduler_with_nats_client):
    """Test error handling in _close."""
    # Set up services
    scheduler_with_nats_client._nats_client = AsyncMock()
    scheduler_with_nats_client._kv_store = AsyncMock()
    scheduler_with_nats_client._jetstream = AsyncMock()
    
    # Mock anyio.create_task_group to raise an exception
    with patch('anyio.create_task_group', side_effect=Exception("Close error")):
        # Should not raise an exception
        await scheduler_with_nats_client._close()


def test_signal_handler(scheduler_with_nats_client):
    """Test signal handler."""
    # Mock signal numbers
    sig_int = signal.SIGINT
    sig_term = signal.SIGTERM
    
    # Test SIGINT
    scheduler_with_nats_client.signal_handler(sig_int, None)
    assert scheduler_with_nats_client._running is False
    assert scheduler_with_nats_client._shutdown_event.is_set()
    
    # Reset for next test
    scheduler_with_nats_client._running = True
    scheduler_with_nats_client._shutdown_event.clear()
    
    # Test SIGTERM
    scheduler_with_nats_client.signal_handler(sig_term, None)
    assert scheduler_with_nats_client._running is False
    assert scheduler_with_nats_client._shutdown_event.is_set()
    
    # Reset for next test
    scheduler_with_nats_client._running = True
    scheduler_with_nats_client._shutdown_event.clear()
    
    # Test unknown signal
    scheduler_with_nats_client.signal_handler(999, None)
    assert scheduler_with_nats_client._running is False
    assert scheduler_with_nats_client._shutdown_event.is_set()


def test_install_signal_handlers(scheduler_with_nats_client):
    """Test install_signal_handlers."""
    with patch('signal.signal') as mock_signal:
        scheduler_with_nats_client.install_signal_handlers()
        
        # Verify signal handlers were installed
        assert mock_signal.call_count == 2
        mock_signal.assert_any_call(signal.SIGINT, scheduler_with_nats_client.signal_handler)
        mock_signal.assert_any_call(signal.SIGTERM, scheduler_with_nats_client.signal_handler)


def test_is_leader_property_ha_enabled(scheduler_with_nats_client):
    """Test is_leader property when HA is enabled."""
    scheduler_with_nats_client._enable_ha = True
    
    # Test when leader election is_leader is False
    scheduler_with_nats_client._leader_election._is_leader = False
    assert scheduler_with_nats_client.is_leader is False
    
    # Test when leader election is_leader is True
    scheduler_with_nats_client._leader_election._is_leader = True
    assert scheduler_with_nats_client.is_leader is True


def test_is_leader_property_ha_disabled(scheduler_with_nats_client):
    """Test is_leader property when HA is disabled."""
    scheduler_with_nats_client._enable_ha = False
    
    # Should always return True when HA is disabled
    assert scheduler_with_nats_client.is_leader is True


@pytest.mark.asyncio
async def test_context_manager_enter_with_nats_url(scheduler_with_nats_url):
    """Test async context manager __aenter__ with NATS URL."""
    with patch('naq.scheduler.NatsClient') as mock_nats_client_class:
        # Setup mock client
        mock_nats_client = AsyncMock()
        mock_nats_client_class.return_value = mock_nats_client
        mock_nats_client.connect = AsyncMock()
        
        result = await scheduler_with_nats_url.__aenter__()
        
        # Verify client was created and connected
        mock_nats_client_class.assert_called_once_with(nats_url="nats://localhost:4222")
        mock_nats_client.connect.assert_called_once()
        
        # Verify client was set
        assert scheduler_with_nats_url._nats_client is mock_nats_client
        
        # Verify self is returned
        assert result is scheduler_with_nats_url


@pytest.mark.asyncio
async def test_context_manager_enter_with_nats_client(scheduler_with_nats_client):
    """Test async context manager __aenter__ with NatsClient."""
    result = await scheduler_with_nats_client.__aenter__()
    
    # Verify self is returned
    assert result is scheduler_with_nats_client


@pytest.mark.asyncio
async def test_context_manager_enter_error_handling(scheduler_with_nats_url):
    """Test error handling in async context manager __aenter__."""
    with patch('naq.scheduler.NatsClient') as mock_nats_client_class:
        # Setup mock client to raise an exception
        mock_nats_client = AsyncMock()
        mock_nats_client_class.return_value = mock_nats_client
        mock_nats_client.connect.side_effect = Exception("Connection error")
        
        with pytest.raises(Exception, match="Connection error"):
            await scheduler_with_nats_url.__aenter__()


@pytest.mark.asyncio
async def test_context_manager_exit_with_nats_url(scheduler_with_nats_url):
    """Test async context manager __aexit__ with NATS URL."""
    # Setup mock client
    mock_nats_client = AsyncMock()
    scheduler_with_nats_url._nats_client = mock_nats_client
    
    await scheduler_with_nats_url.__aexit__(None, None, None)
    
    # Verify client close was called
    mock_nats_client.close.assert_called_once()


@pytest.mark.asyncio
async def test_context_manager_exit_with_nats_client(scheduler_with_nats_client):
    """Test async context manager __aexit__ with NatsClient."""
    await scheduler_with_nats_client.__aexit__(None, None, None)
    
    # Verify _close was called
    assert scheduler_with_nats_client._nats_client is None
    assert scheduler_with_nats_client._kv_store is None
    assert scheduler_with_nats_client._jetstream is None


@pytest.mark.asyncio
async def test_context_manager_exit_error_handling(scheduler_with_nats_url):
    """Test error handling in async context manager __aexit__."""
    # Setup mock client
    mock_nats_client = AsyncMock()
    mock_nats_client.close.side_effect = Exception("Close error")
    scheduler_with_nats_url._nats_client = mock_nats_client
    
    # Should not raise an exception
    await scheduler_with_nats_url.__aexit__(None, None, None)