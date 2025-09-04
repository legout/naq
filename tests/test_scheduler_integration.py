# tests/test_scheduler_integration.py
import asyncio
import time
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import anyio
import pytest

from naq.scheduler import LeaderElection, Scheduler
from naq.nats_client import NatsClient
from naq.settings import (
    SCHEDULER_LOCK_KV_NAME,
    SCHEDULER_LOCK_KEY,
    SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS,
    SCHEDULER_LOCK_TTL_SECONDS,
)


@pytest.fixture
def mock_nats_client():
    """Create a mock NatsClient for testing."""
    client = AsyncMock(spec=NatsClient)
    
    # Mock KV store operations
    mock_kv = AsyncMock()
    mock_kv.get = AsyncMock()
    mock_kv.create = AsyncMock()
    mock_kv.update = AsyncMock()
    mock_kv.delete = AsyncMock()
    client.get_kv_store = AsyncMock(return_value=mock_kv)
    
    # Mock trigger_due_jobs method
    client.trigger_due_jobs = AsyncMock(return_value=(0, 0))
    
    return client


@pytest.mark.asyncio
async def test_leader_election_integration(mock_nats_client):
    """Test leader election integration with multiple instances."""
    # Create two leader election instances
    instance1 = LeaderElection(
        instance_id=f"instance1-{uuid.uuid4().hex[:8]}",
        lock_ttl=SCHEDULER_LOCK_TTL_SECONDS,
        lock_renew_interval=SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS,
        client=mock_nats_client
    )
    
    instance2 = LeaderElection(
        instance_id=f"instance2-{uuid.uuid4().hex[:8]}",
        lock_ttl=SCHEDULER_LOCK_TTL_SECONDS,
        lock_renew_interval=SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS,
        client=mock_nats_client
    )
    
    # Initialize both instances
    await instance1.initialize()
    await instance2.initialize()
    
    # Get mock KV store
    mock_kv = await mock_nats_client.get_kv_store(SCHEDULER_LOCK_KV_NAME)
    
    # First instance should become leader
    mock_kv.create.return_value = True
    mock_kv.get.return_value = MagicMock(
        value=msgspec.msgpack.encode({
            "instance_id": instance1.instance_id,
            "timestamp": time.time(),
            "hostname": "host1",
            "pid": 12345,
            "start_time": time.time()
        })
    )
    
    result1 = await instance1.try_become_leader()
    assert result1 is True
    assert instance1.is_leader is True
    
    # Second instance should not become leader
    mock_kv.get.return_value = MagicMock(
        value=msgspec.msgpack.encode({
            "instance_id": instance1.instance_id,
            "timestamp": time.time(),
            "hostname": "host1",
            "pid": 12345,
            "start_time": time.time()
        })
    )
    
    result2 = await instance2.try_become_leader()
    assert result2 is False
    assert instance2.is_leader is False
    
    # First instance should be able to renew lock
    mock_kv.update.return_value = True
    mock_kv.get.return_value = MagicMock(
        value=msgspec.msgpack.encode({
            "instance_id": instance1.instance_id,
            "timestamp": time.time(),
            "hostname": "host1",
            "pid": 12345,
            "start_time": time.time()
        })
    )
    
    renew_result = await instance1._renew_lock()
    assert renew_result is True
    
    # First instance releases lock
    mock_kv.get.return_value = None  # Lock no longer exists after release
    await instance1.release_lock()
    assert instance1.is_leader is False
    
    # Now second instance should become leader
    mock_kv.create.return_value = True
    mock_kv.get.return_value = MagicMock(
        value=msgspec.msgpack.encode({
            "instance_id": instance2.instance_id,
            "timestamp": time.time(),
            "hostname": "host2",
            "pid": 12345,
            "start_time": time.time()
        })
    )
    
    result2 = await instance2.try_become_leader()
    assert result2 is True
    assert instance2.is_leader is True


@pytest.mark.asyncio
async def test_leader_election_lock_expiry(mock_nats_client):
    """Test leader election with lock expiry."""
    import msgspec
    
    instance = LeaderElection(
        instance_id=f"instance-{uuid.uuid4().hex[:8]}",
        lock_ttl=1,  # 1 second TTL for testing
        lock_renew_interval=0.5,  # 0.5 second renew interval
        client=mock_nats_client
    )
    
    await instance.initialize()
    
    # Get mock KV store
    mock_kv = await mock_nats_client.get_kv_store(SCHEDULER_LOCK_KV_NAME)
    
    # Instance becomes leader
    mock_kv.create.return_value = True
    mock_kv.get.return_value = MagicMock(
        value=msgspec.msgpack.encode({
            "instance_id": instance.instance_id,
            "timestamp": time.time(),
            "hostname": "host",
            "pid": 12345,
            "start_time": time.time()
        })
    )
    
    result = await instance.try_become_leader()
    assert result is True
    assert instance.is_leader is True
    
    # Lock expires
    mock_kv.get.return_value = MagicMock(
        value=msgspec.msgpack.encode({
            "instance_id": instance.instance_id,
            "timestamp": time.time() - 2,  # 2 seconds ago (expired)
            "hostname": "host",
            "pid": 12345,
            "start_time": time.time() - 2
        })
    )
    
    # Check lock health
    health = await instance.check_leader_lock_health()
    assert health["status"] == "expired"
    
    # Instance should no longer be able to renew lock
    renew_result = await instance._renew_lock()
    assert renew_result is False
    assert instance.is_leader is False


@pytest.mark.asyncio
async def test_scheduler_integration_with_ha(mock_nats_client):
    """Test scheduler integration with high availability."""
    import msgspec
    
    # Create two scheduler instances
    scheduler1 = Scheduler(
        client=mock_nats_client,
        poll_interval=0.1,
        instance_id=f"scheduler1-{uuid.uuid4().hex[:8]}",
        enable_ha=True
    )
    
    scheduler2 = Scheduler(
        client=mock_nats_client,
        poll_interval=0.1,
        instance_id=f"scheduler2-{uuid.uuid4().hex[:8]}",
        enable_ha=True
    )
    
    # Connect both schedulers
    await scheduler1._connect()
    await scheduler2._connect()
    
    # Get mock KV store
    mock_kv = await mock_nats_client.get_kv_store(SCHEDULER_LOCK_KV_NAME)
    
    # First scheduler becomes leader
    mock_kv.create.return_value = True
    mock_kv.get.return_value = MagicMock(
        value=msgspec.msgpack.encode({
            "instance_id": scheduler1._instance_id,
            "timestamp": time.time(),
            "hostname": "host1",
            "pid": 12345,
            "start_time": time.time()
        })
    )
    
    # Handle leadership transition for first scheduler
    await scheduler1._handle_leadership_transition()
    assert scheduler1.is_leader is True
    
    # Second scheduler should not become leader
    mock_kv.get.return_value = MagicMock(
        value=msgspec.msgpack.encode({
            "instance_id": scheduler1._instance_id,
            "timestamp": time.time(),
            "hostname": "host1",
            "pid": 12345,
            "start_time": time.time()
        })
    )
    
    await scheduler2._handle_leadership_transition()
    assert scheduler2.is_leader is False
    
    # First scheduler processes jobs
    mock_nats_client.trigger_due_jobs.return_value = (5, 0)
    await scheduler1._process_scheduled_jobs()
    mock_nats_client.trigger_due_jobs.assert_called_once()
    
    # Second scheduler does not process jobs
    mock_nats_client.trigger_due_jobs.reset_mock()
    await scheduler2._process_scheduled_jobs()
    mock_nats_client.trigger_due_jobs.assert_not_called()
    
    # First scheduler releases leadership
    mock_kv.get.return_value = None
    await scheduler1._leader_election.release_lock()
    assert scheduler1.is_leader is False
    
    # Second scheduler becomes leader
    mock_kv.create.return_value = True
    mock_kv.get.return_value = MagicMock(
        value=msgspec.msgpack.encode({
            "instance_id": scheduler2._instance_id,
            "timestamp": time.time(),
            "hostname": "host2",
            "pid": 12345,
            "start_time": time.time()
        })
    )
    
    await scheduler2._handle_leadership_transition()
    assert scheduler2.is_leader is True
    
    # Second scheduler now processes jobs
    mock_nats_client.trigger_due_jobs.reset_mock()
    await scheduler2._process_scheduled_jobs()
    mock_nats_client.trigger_due_jobs.assert_called_once()


@pytest.mark.asyncio
async def test_scheduler_integration_without_ha(mock_nats_client):
    """Test scheduler integration without high availability."""
    # Create two scheduler instances with HA disabled
    scheduler1 = Scheduler(
        client=mock_nats_client,
        poll_interval=0.1,
        instance_id=f"scheduler1-{uuid.uuid4().hex[:8]}",
        enable_ha=False
    )
    
    scheduler2 = Scheduler(
        client=mock_nats_client,
        poll_interval=0.1,
        instance_id=f"scheduler2-{uuid.uuid4().hex[:8]}",
        enable_ha=False
    )
    
    # Connect both schedulers
    await scheduler1._connect()
    await scheduler2._connect()
    
    # Both schedulers should be leaders
    assert scheduler1.is_leader is True
    assert scheduler2.is_leader is True
    
    # Both schedulers process jobs
    mock_nats_client.trigger_due_jobs.return_value = (5, 0)
    
    await scheduler1._process_scheduled_jobs()
    mock_nats_client.trigger_due_jobs.assert_called_once()
    
    mock_nats_client.trigger_due_jobs.reset_mock()
    await scheduler2._process_scheduled_jobs()
    mock_nats_client.trigger_due_jobs.assert_called_once()


@pytest.mark.asyncio
async def test_scheduler_run_cycle(mock_nats_client):
    """Test a single scheduler run cycle."""
    import msgspec
    
    scheduler = Scheduler(
        client=mock_nats_client,
        poll_interval=0.1,
        instance_id=f"scheduler-{uuid.uuid4().hex[:8]}",
        enable_ha=True
    )
    
    # Connect scheduler
    await scheduler._connect()
    
    # Get mock KV store
    mock_kv = await mock_nats_client.get_kv_store(SCHEDULER_LOCK_KV_NAME)
    
    # Set up mocks for leadership
    mock_kv.create.return_value = True
    mock_kv.get.return_value = MagicMock(
        value=msgspec.msgpack.encode({
            "instance_id": scheduler._instance_id,
            "timestamp": time.time(),
            "hostname": "host",
            "pid": 12345,
            "start_time": time.time()
        })
    )
    
    # Mock the renewal task to not actually run
    with patch.object(scheduler._leader_election, 'start_renewal_task') as mock_start_renewal:
        # Run a single cycle
        scheduler._running = True
        
        # Handle leadership transition
        await scheduler._handle_leadership_transition()
        assert scheduler.is_leader is True
        
        # Process scheduled jobs
        mock_nats_client.trigger_due_jobs.return_value = (3, 1)
        await scheduler._process_scheduled_jobs()
        mock_nats_client.trigger_due_jobs.assert_called_once()
        
        # Wait for next cycle (should return True to continue)
        cycle_start = time.time()
        result = await scheduler._wait_for_next_cycle(cycle_start)
        assert result is True
        
        # Stop the scheduler
        scheduler._running = False
        scheduler._shutdown_event.set()
        
        # Shutdown
        await scheduler._shutdown()


@pytest.mark.asyncio
async def test_scheduler_error_handling(mock_service_manager, mock_kv_store_service, mock_scheduler_service):
    """Test scheduler error handling."""
    scheduler = Scheduler(
        service_manager=mock_service_manager,
        poll_interval=0.1,
        instance_id=f"scheduler-{uuid.uuid4().hex[:8]}",
        enable_ha=True
    )
    
    # Connect scheduler
    await scheduler._connect()
    
    # Set up mocks for leadership
    mock_kv_store_service.set.return_value = True
    mock_kv_store_service.get.return_value = {
        "instance_id": scheduler._instance_id,
        "timestamp": time.time(),
        "hostname": "host"
    }
    
    # Mock the renewal task to not actually run
    with patch.object(scheduler._leader_election, 'start_renewal_task') as mock_start_renewal:
        # Handle leadership transition
        await scheduler._handle_leadership_transition()
        assert scheduler.is_leader is True
        
        # Test error handling in job processing
        mock_scheduler_service.trigger_due_jobs.side_effect = Exception("Job processing error")
        
        # Should not raise an exception
        await scheduler._process_scheduled_jobs()
        
        # Test error handling in wait for next cycle
        with patch('anyio.move_on_after', side_effect=Exception("Wait error")):
            # Should not raise an exception
            cycle_start = time.time()
            result = await scheduler._wait_for_next_cycle(cycle_start)
            assert result is True
        
        # Stop the scheduler
        scheduler._running = False
        scheduler._shutdown_event.set()
        
        # Shutdown
        await scheduler._shutdown()


@pytest.mark.asyncio
async def test_scheduler_context_manager(mock_service_manager, mock_kv_store_service, mock_scheduler_service):
    """Test scheduler as a context manager."""
    # Set up mocks for leadership
    mock_kv_store_service.set.return_value = True
    mock_kv_store_service.get.return_value = {
        "instance_id": "test-instance",
        "timestamp": time.time(),
        "hostname": "host"
    }
    
    async with Scheduler(
        service_manager=mock_service_manager,
        poll_interval=0.1,
        instance_id="test-instance",
        enable_ha=True
    ) as scheduler:
        # Verify scheduler is connected
        assert scheduler._connection_service is not None
        assert scheduler._kv_store_service is not None
        assert scheduler._event_service is not None
        assert scheduler._scheduler_service is not None
        
        # Handle leadership transition
        await scheduler._handle_leadership_transition()
        assert scheduler.is_leader is True
        
        # Process jobs
        mock_scheduler_service.trigger_due_jobs.return_value = (2, 0)
        await scheduler._process_scheduled_jobs()
        mock_scheduler_service.trigger_due_jobs.assert_called_once()
    
    # Verify services are cleared after exiting context
    assert scheduler._connection_service is None
    assert scheduler._kv_store_service is None
    assert scheduler._event_service is None
    assert scheduler._scheduler_service is None


@pytest.mark.asyncio
async def test_concurrent_leader_election_attempts(mock_kv_store_service):
    """Test concurrent leader election attempts."""
    # Create multiple leader election instances
    instances = []
    for i in range(5):
        instance = LeaderElection(
            instance_id=f"instance{i}-{uuid.uuid4().hex[:8]}",
            lock_ttl=SCHEDULER_LOCK_TTL_SECONDS,
            lock_renew_interval=SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS,
            kv_store_service=mock_kv_store_service
        )
        await instance.initialize()
        instances.append(instance)
    
    # All instances try to become leader concurrently
    async def try_become_leader(instance):
        # Mock the KV store operations
        mock_kv_store_service.set.return_value = True
        mock_kv_store_service.get.return_value = {
            "instance_id": instance.instance_id,
            "timestamp": time.time(),
            "hostname": f"host{i}"
        }
        return await instance.try_become_leader()
    
    # Run all attempts concurrently
    tasks = [try_become_leader(instance) for instance in instances]
    results = await asyncio.gather(*tasks)
    
    # Only one instance should have become leader
    leader_count = sum(1 for result in results if result is True)
    assert leader_count == 1
    
    # Find the leader
    leader = None
    for i, instance in enumerate(instances):
        if results[i] is True:
            leader = instance
            break
    
    # Verify the leader
    assert leader is not None
    assert leader.is_leader is True
    
    # Other instances should not be leaders
    for i, instance in enumerate(instances):
        if instance != leader:
            assert instance.is_leader is False


@pytest.mark.asyncio
async def test_leader_renewal_task(mock_kv_store_service):
    """Test leader renewal task."""
    instance = LeaderElection(
        instance_id=f"instance-{uuid.uuid4().hex[:8]}",
        lock_ttl=1,  # 1 second TTL for testing
        lock_renew_interval=0.2,  # 0.2 second renew interval
        kv_store_service=mock_kv_store_service
    )
    
    await instance.initialize()
    
    # Instance becomes leader
    mock_kv_store_service.set.return_value = True
    mock_kv_store_service.get.return_value = {
        "instance_id": instance.instance_id,
        "timestamp": time.time(),
        "hostname": "host"
    }
    
    result = await instance.try_become_leader()
    assert result is True
    assert instance.is_leader is True
    
    # Start renewal task
    await instance.start_renewal_task(True)
    
    # Let it run for a short time
    await asyncio.sleep(0.5)
    
    # Stop the renewal task
    await instance.stop_renewal_task()
    
    # Verify leadership is released
    assert instance.is_leader is False