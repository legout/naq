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
from naq.services.base import ServiceManager
from naq.services.connection import ConnectionService
from naq.services.events import EventService
from naq.services.kv_stores import KVStoreService
from naq.services.scheduler import SchedulerService


@pytest.fixture
def mock_service_manager():
    """Create a mock ServiceManager for testing."""
    manager = AsyncMock(spec=ServiceManager)
    manager.get_service = AsyncMock()
    return manager


@pytest.fixture
def mock_connection_service():
    """Create a mock ConnectionService for testing."""
    return AsyncMock(spec=ConnectionService)


@pytest.fixture
def mock_kv_store_service():
    """Create a mock KVStoreService for testing."""
    service = AsyncMock(spec=KVStoreService)
    service.get = AsyncMock()
    service.set = AsyncMock()
    service.delete = AsyncMock()
    return service


@pytest.fixture
def mock_event_service():
    """Create a mock EventService for testing."""
    return AsyncMock(spec=EventService)


@pytest.fixture
def mock_scheduler_service():
    """Create a mock SchedulerService for testing."""
    service = AsyncMock(spec=SchedulerService)
    service.trigger_due_jobs = AsyncMock(return_value=(0, 0))
    return service


@pytest.fixture
def scheduler_with_service_manager(mock_service_manager):
    """Create a Scheduler instance with a mock ServiceManager."""
    return Scheduler(
        service_manager=mock_service_manager,
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


def test_scheduler_initialization_with_service_manager(mock_service_manager):
    """Test Scheduler initialization with ServiceManager."""
    scheduler = Scheduler(
        service_manager=mock_service_manager,
        poll_interval=0.5,
        instance_id="test-instance",
        enable_ha=False
    )
    
    assert scheduler._service_manager is mock_service_manager
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
    assert scheduler._service_manager is None
    assert scheduler._poll_interval == 0.5
    assert scheduler._instance_id == "test-instance"
    assert scheduler._enable_ha is False


def test_scheduler_initialization_without_connection_params():
    """Test Scheduler initialization without connection parameters."""
    with pytest.raises(ValueError, match="Either nats_url or service_manager must be provided"):
        Scheduler()


def test_scheduler_initialization_generates_instance_id(mock_service_manager):
    """Test that Scheduler generates an instance ID if none provided."""
    scheduler = Scheduler(service_manager=mock_service_manager)
    
    assert scheduler._instance_id is not None
    assert isinstance(scheduler._instance_id, str)
    assert len(scheduler._instance_id) > 0


@pytest.mark.asyncio
async def test_connect_with_service_manager(
    scheduler_with_service_manager,
    mock_service_manager,
    mock_connection_service,
    mock_kv_store_service,
    mock_event_service,
    mock_scheduler_service
):
    """Test _connect with ServiceManager."""
    # Setup mocks
    mock_service_manager.get_service.side_effect = [
        mock_connection_service,
        mock_kv_store_service,
        mock_event_service,
        mock_scheduler_service
    ]
    
    await scheduler_with_service_manager._connect()
    
    # Verify services were retrieved
    assert mock_service_manager.get_service.call_count == 4
    assert scheduler_with_service_manager._connection_service is mock_connection_service
    assert scheduler_with_service_manager._kv_store_service is mock_kv_store_service
    assert scheduler_with_service_manager._event_service is mock_event_service
    assert scheduler_with_service_manager._scheduler_service is mock_scheduler_service
    
    # Verify leader election was initialized
    assert scheduler_with_service_manager._leader_election._kv_store_service is mock_kv_store_service


@pytest.mark.asyncio
async def test_connect_with_nats_url(scheduler_with_nats_url):
    """Test _connect with NATS URL."""
    with patch('naq.scheduler.long_lived_service_context') as mock_context:
        # Setup mock context manager
        mock_context_manager = AsyncMock()
        mock_context.return_value.__aenter__.return_value = mock_context_manager
        mock_context_manager.get_service = AsyncMock()
        
        # Setup service mocks
        mock_connection_service = AsyncMock(spec=ConnectionService)
        mock_kv_store_service = AsyncMock(spec=KVStoreService)
        mock_event_service = AsyncMock(spec=EventService)
        mock_scheduler_service = AsyncMock(spec=SchedulerService)
        
        mock_context_manager.get_service.side_effect = [
            mock_connection_service,
            mock_kv_store_service,
            mock_event_service,
            mock_scheduler_service
        ]
        
        await scheduler_with_nats_url._connect()
        
        # Verify context was created
        mock_context.assert_called_once()
        
        # Verify services were retrieved
        assert mock_context_manager.get_service.call_count == 4
        assert scheduler_with_nats_url._connection_service is mock_connection_service
        assert scheduler_with_nats_url._kv_store_service is mock_kv_store_service
        assert scheduler_with_nats_url._event_service is mock_event_service
        assert scheduler_with_nats_url._scheduler_service is mock_scheduler_service


@pytest.mark.asyncio
async def test_connect_error_handling(scheduler_with_service_manager, mock_service_manager):
    """Test error handling in _connect."""
    mock_service_manager.get_service.side_effect = Exception("Connection error")
    
    with pytest.raises(Exception, match="Connection error"):
        await scheduler_with_service_manager._connect()


@pytest.mark.asyncio
async def test_handle_leadership_transition_ha_enabled(
    scheduler_with_service_manager,
    mock_kv_store_service
):
    """Test _handle_leadership_transition with HA enabled."""
    # Setup mocks
    scheduler_with_service_manager._leader_election.try_become_leader = AsyncMock(return_value=True)
    scheduler_with_service_manager._leader_election.start_renewal_task = AsyncMock()
    
    # Mock is_leader property to return False initially
    with patch.object(scheduler_with_service_manager, 'is_leader', new_callable=PropertyMock) as mock_is_leader:
        mock_is_leader.side_effect = [False, True]  # First call returns False, second returns True
        
        await scheduler_with_service_manager._handle_leadership_transition()
        
        # Verify leader election was attempted
        scheduler_with_service_manager._leader_election.try_become_leader.assert_called_once()
        
        # Verify renewal task was started
        scheduler_with_service_manager._leader_election.start_renewal_task.assert_called_once_with(True)


@pytest.mark.asyncio
async def test_handle_leadership_transition_ha_disabled(scheduler_with_service_manager):
    """Test _handle_leadership_transition with HA disabled."""
    scheduler_with_service_manager._enable_ha = False
    
    with patch.object(scheduler_with_service_manager, 'is_leader', False):
        await scheduler_with_service_manager._handle_leadership_transition()
        
        # Verify leader election was set to True
        assert scheduler_with_service_manager._leader_election._is_leader is True


@pytest.mark.asyncio
async def test_handle_ha_leadership_become_leader(
    scheduler_with_service_manager,
    mock_kv_store_service
):
    """Test _handle_ha_leadership when becoming leader."""
    # Setup mocks
    scheduler_with_service_manager._leader_election.try_become_leader = AsyncMock(return_value=True)
    scheduler_with_service_manager._leader_election.start_renewal_task = AsyncMock()
    
    with patch.object(scheduler_with_service_manager, 'is_leader', False):
        await scheduler_with_service_manager._handle_ha_leadership(False)
        
        # Verify leader election was attempted
        scheduler_with_service_manager._leader_election.try_become_leader.assert_called_once()
        
        # Verify renewal task was started
        scheduler_with_service_manager._leader_election.start_renewal_task.assert_called_once_with(True)


@pytest.mark.asyncio
async def test_handle_ha_leadership_fail_to_become_leader(
    scheduler_with_service_manager,
    mock_kv_store_service
):
    """Test _handle_ha_leadership when failing to become leader."""
    # Setup mocks
    scheduler_with_service_manager._leader_election.try_become_leader = AsyncMock(return_value=False)
    
    with patch.object(scheduler_with_service_manager, 'is_leader', False):
        await scheduler_with_service_manager._handle_ha_leadership(False)
        
        # Verify leader election was attempted
        scheduler_with_service_manager._leader_election.try_become_leader.assert_called_once()
        
        # Verify renewal task was not started
        scheduler_with_service_manager._leader_election.start_renewal_task.assert_not_called()


@pytest.mark.asyncio
async def test_process_scheduled_jobs_as_leader(scheduler_with_service_manager, mock_scheduler_service):
    """Test _process_scheduled_jobs when instance is leader."""
    # Setup mocks
    mock_scheduler_service.trigger_due_jobs.return_value = (5, 1)
    scheduler_with_service_manager._scheduler_service = mock_scheduler_service
    
    with patch.object(scheduler_with_service_manager, 'is_leader', True):
        await scheduler_with_service_manager._process_scheduled_jobs()
        
        # Verify jobs were processed
        mock_scheduler_service.trigger_due_jobs.assert_called_once()


@pytest.mark.asyncio
async def test_process_scheduled_jobs_as_follower(scheduler_with_service_manager, mock_scheduler_service):
    """Test _process_scheduled_jobs when instance is not leader."""
    scheduler_with_service_manager._scheduler_service = mock_scheduler_service
    
    with patch.object(scheduler_with_service_manager, 'is_leader', False):
        await scheduler_with_service_manager._process_scheduled_jobs()
        
        # Verify jobs were not processed
        mock_scheduler_service.trigger_due_jobs.assert_not_called()


@pytest.mark.asyncio
async def test_process_scheduled_jobs_no_service(scheduler_with_service_manager):
    """Test _process_scheduled_jobs when scheduler service is not available."""
    scheduler_with_service_manager._scheduler_service = None
    
    with patch.object(scheduler_with_service_manager, 'is_leader', True):
        await scheduler_with_service_manager._process_scheduled_jobs()
        
        # Should not raise an exception


@pytest.mark.asyncio
async def test_process_scheduled_jobs_error_handling(scheduler_with_service_manager, mock_scheduler_service):
    """Test error handling in _process_scheduled_jobs."""
    # Setup mocks
    mock_scheduler_service.trigger_due_jobs.side_effect = Exception("Processing error")
    scheduler_with_service_manager._scheduler_service = mock_scheduler_service
    
    with patch.object(scheduler_with_service_manager, 'is_leader', True):
        # Should not raise an exception
        await scheduler_with_service_manager._process_scheduled_jobs()


@pytest.mark.asyncio
async def test_wait_for_next_cycle_shutdown_triggered(scheduler_with_service_manager):
    """Test _wait_for_next_cycle when shutdown is triggered."""
    scheduler_with_service_manager._shutdown_event.set()
    
    result = await scheduler_with_service_manager._wait_for_next_cycle(time.time())
    
    assert result is False


@pytest.mark.asyncio
async def test_wait_for_next_cycle_normal_wait(scheduler_with_service_manager):
    """Test _wait_for_next_cycle with normal wait."""
    cycle_start = time.time() - 0.05  # 50ms ago
    scheduler_with_service_manager._poll_interval = 0.1  # 100ms interval
    
    result = await scheduler_with_service_manager._wait_for_next_cycle(cycle_start)
    
    assert result is True


@pytest.mark.asyncio
async def test_wait_for_next_cycle_long_processing(scheduler_with_service_manager):
    """Test _wait_for_next_cycle when processing took longer than poll interval."""
    cycle_start = time.time() - 0.15  # 150ms ago
    scheduler_with_service_manager._poll_interval = 0.1  # 100ms interval
    
    result = await scheduler_with_service_manager._wait_for_next_cycle(cycle_start)
    
    assert result is True


@pytest.mark.asyncio
async def test_wait_for_next_cycle_shutdown_during_wait(scheduler_with_service_manager):
    """Test _wait_for_next_cycle when shutdown is triggered during wait."""
    cycle_start = time.time() - 0.05  # 50ms ago
    scheduler_with_service_manager._poll_interval = 0.2  # 200ms interval
    
    # Set shutdown event after a short delay
    async def set_shutdown_after_delay():
        await asyncio.sleep(0.1)
        scheduler_with_service_manager._shutdown_event.set()
    
    # Start the task to set shutdown event
    asyncio.create_task(set_shutdown_after_delay())
    
    result = await scheduler_with_service_manager._wait_for_next_cycle(cycle_start)
    
    assert result is False


@pytest.mark.asyncio
async def test_wait_for_next_cycle_error_handling(scheduler_with_service_manager):
    """Test error handling in _wait_for_next_cycle."""
    cycle_start = time.time() - 0.05  # 50ms ago
    scheduler_with_service_manager._poll_interval = 0.1  # 100ms interval
    
    # Mock anyio.move_on_after to raise an exception
    with patch('anyio.move_on_after', side_effect=Exception("Wait error")):
        result = await scheduler_with_service_manager._wait_for_next_cycle(cycle_start)
        
        assert result is True  # Should continue despite errors


@pytest.mark.asyncio
async def test_shutdown_normal_operation(
    scheduler_with_service_manager,
    mock_kv_store_service
):
    """Test normal shutdown operation."""
    # Setup mocks
    scheduler_with_service_manager._enable_ha = True
    scheduler_with_service_manager._leader_election.stop_renewal_task = AsyncMock()
    scheduler_with_service_manager._leader_election.release_lock = AsyncMock()
    
    await scheduler_with_service_manager._shutdown()
    
    # Verify leader election processes were stopped
    scheduler_with_service_manager._leader_election.stop_renewal_task.assert_called_once()
    scheduler_with_service_manager._leader_election.release_lock.assert_called_once()
    
    # Verify shutdown event is set
    assert scheduler_with_service_manager._shutdown_event.is_set()
    
    # Verify running flag is cleared
    assert scheduler_with_service_manager._running is False


@pytest.mark.asyncio
async def test_shutdown_ha_disabled(scheduler_with_service_manager):
    """Test shutdown when HA is disabled."""
    scheduler_with_service_manager._enable_ha = False
    
    await scheduler_with_service_manager._shutdown()
    
    # Verify shutdown event is set
    assert scheduler_with_service_manager._shutdown_event.is_set()
    
    # Verify running flag is cleared
    assert scheduler_with_service_manager._running is False


@pytest.mark.asyncio
async def test_shutdown_error_handling(scheduler_with_service_manager):
    """Test error handling in shutdown."""
    # Setup mocks to raise an exception
    scheduler_with_service_manager._enable_ha = True
    scheduler_with_service_manager._leader_election.stop_renewal_task = AsyncMock(side_effect=Exception("Stop error"))
    
    # Should not raise an exception
    await scheduler_with_service_manager._shutdown()
    
    # Verify shutdown event is set
    assert scheduler_with_service_manager._shutdown_event.is_set()
    
    # Verify running flag is cleared
    assert scheduler_with_service_manager._running is False


@pytest.mark.asyncio
async def test_close(scheduler_with_service_manager):
    """Test _close method."""
    # Set up services
    scheduler_with_service_manager._connection_service = AsyncMock()
    scheduler_with_service_manager._kv_store_service = AsyncMock()
    scheduler_with_service_manager._event_service = AsyncMock()
    scheduler_with_service_manager._scheduler_service = AsyncMock()
    
    await scheduler_with_service_manager._close()
    
    # Verify services are cleared
    assert scheduler_with_service_manager._connection_service is None
    assert scheduler_with_service_manager._kv_store_service is None
    assert scheduler_with_service_manager._event_service is None
    assert scheduler_with_service_manager._scheduler_service is None
    
    # Verify shutdown event is set
    assert scheduler_with_service_manager._shutdown_event.is_set()
    
    # Verify running flag is cleared
    assert scheduler_with_service_manager._running is False


@pytest.mark.asyncio
async def test_close_error_handling(scheduler_with_service_manager):
    """Test error handling in _close."""
    # Set up services
    scheduler_with_service_manager._connection_service = AsyncMock()
    scheduler_with_service_manager._kv_store_service = AsyncMock()
    scheduler_with_service_manager._event_service = AsyncMock()
    scheduler_with_service_manager._scheduler_service = AsyncMock()
    
    # Mock anyio.create_task_group to raise an exception
    with patch('anyio.create_task_group', side_effect=Exception("Close error")):
        # Should not raise an exception
        await scheduler_with_service_manager._close()


def test_signal_handler(scheduler_with_service_manager):
    """Test signal handler."""
    # Mock signal numbers
    sig_int = signal.SIGINT
    sig_term = signal.SIGTERM
    
    # Test SIGINT
    scheduler_with_service_manager.signal_handler(sig_int, None)
    assert scheduler_with_service_manager._running is False
    assert scheduler_with_service_manager._shutdown_event.is_set()
    
    # Reset for next test
    scheduler_with_service_manager._running = True
    scheduler_with_service_manager._shutdown_event.clear()
    
    # Test SIGTERM
    scheduler_with_service_manager.signal_handler(sig_term, None)
    assert scheduler_with_service_manager._running is False
    assert scheduler_with_service_manager._shutdown_event.is_set()
    
    # Reset for next test
    scheduler_with_service_manager._running = True
    scheduler_with_service_manager._shutdown_event.clear()
    
    # Test unknown signal
    scheduler_with_service_manager.signal_handler(999, None)
    assert scheduler_with_service_manager._running is False
    assert scheduler_with_service_manager._shutdown_event.is_set()


def test_install_signal_handlers(scheduler_with_service_manager):
    """Test install_signal_handlers."""
    with patch('signal.signal') as mock_signal:
        scheduler_with_service_manager.install_signal_handlers()
        
        # Verify signal handlers were installed
        assert mock_signal.call_count == 2
        mock_signal.assert_any_call(signal.SIGINT, scheduler_with_service_manager.signal_handler)
        mock_signal.assert_any_call(signal.SIGTERM, scheduler_with_service_manager.signal_handler)


def test_is_leader_property_ha_enabled(scheduler_with_service_manager):
    """Test is_leader property when HA is enabled."""
    scheduler_with_service_manager._enable_ha = True
    
    # Test when leader election is_leader is False
    scheduler_with_service_manager._leader_election._is_leader = False
    assert scheduler_with_service_manager.is_leader is False
    
    # Test when leader election is_leader is True
    scheduler_with_service_manager._leader_election._is_leader = True
    assert scheduler_with_service_manager.is_leader is True


def test_is_leader_property_ha_disabled(scheduler_with_service_manager):
    """Test is_leader property when HA is disabled."""
    scheduler_with_service_manager._enable_ha = False
    
    # Should always return True when HA is disabled
    assert scheduler_with_service_manager.is_leader is True


@pytest.mark.asyncio
async def test_context_manager_enter_with_nats_url(scheduler_with_nats_url):
    """Test async context manager __aenter__ with NATS URL."""
    with patch('naq.scheduler.long_lived_service_context') as mock_context:
        # Setup mock context manager
        mock_context_manager = AsyncMock()
        mock_context.return_value.__aenter__.return_value = mock_context_manager
        
        result = await scheduler_with_nats_url.__aenter__()
        
        # Verify context was created
        mock_context.assert_called_once()
        
        # Verify service manager was set
        assert scheduler_with_nats_url._service_manager is mock_context_manager
        
        # Verify self is returned
        assert result is scheduler_with_nats_url


@pytest.mark.asyncio
async def test_context_manager_enter_with_service_manager(scheduler_with_service_manager):
    """Test async context manager __aenter__ with ServiceManager."""
    result = await scheduler_with_service_manager.__aenter__()
    
    # Verify self is returned
    assert result is scheduler_with_service_manager


@pytest.mark.asyncio
async def test_context_manager_enter_error_handling(scheduler_with_nats_url):
    """Test error handling in async context manager __aenter__."""
    with patch('naq.scheduler.long_lived_service_context') as mock_context:
        # Setup mock context manager to raise an exception
        mock_context.side_effect = Exception("Context error")
        
        with pytest.raises(Exception, match="Context error"):
            await scheduler_with_nats_url.__aenter__()


@pytest.mark.asyncio
async def test_context_manager_exit_with_nats_url(scheduler_with_nats_url):
    """Test async context manager __aexit__ with NATS URL."""
    # Setup mock service manager
    mock_service_manager = AsyncMock()
    scheduler_with_nats_url._service_manager = mock_service_manager
    
    await scheduler_with_nats_url.__aexit__(None, None, None)
    
    # Verify service manager __aexit__ was called
    mock_service_manager.__aexit__.assert_called_once_with(None, None, None)


@pytest.mark.asyncio
async def test_context_manager_exit_with_service_manager(scheduler_with_service_manager):
    """Test async context manager __aexit__ with ServiceManager."""
    await scheduler_with_service_manager.__aexit__(None, None, None)
    
    # Verify _close was called
    assert scheduler_with_service_manager._connection_service is None
    assert scheduler_with_service_manager._kv_store_service is None
    assert scheduler_with_service_manager._event_service is None
    assert scheduler_with_service_manager._scheduler_service is None


@pytest.mark.asyncio
async def test_context_manager_exit_error_handling(scheduler_with_nats_url):
    """Test error handling in async context manager __aexit__."""
    # Setup mock service manager
    mock_service_manager = AsyncMock()
    mock_service_manager.__aexit__.side_effect = Exception("Exit error")
    scheduler_with_nats_url._service_manager = mock_service_manager
    
    # Should not raise an exception
    await scheduler_with_nats_url.__aexit__(None, None, None)