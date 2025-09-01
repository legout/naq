# tests/test_scheduler_leader_election.py
import asyncio
import os
import time
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import anyio
import msgspec
import pytest

from naq.circuit_breaker import get_circuit_breaker
from naq.exceptions import (
    NaqConnectionError,
    LeaderElectionError,
    LockAcquisitionError,
    LockRenewalError,
    LockReleaseError,
    LockTimeoutError,
    LockConflictError,
    LockDataError,
)
from naq.scheduler import LeaderElection, LockData
from naq.settings import SCHEDULER_LOCK_KV_NAME, SCHEDULER_LOCK_KEY
from naq.utils.error_handling import ErrorHandler, create_error_context, wrap_naq_exception


@pytest.fixture
def mock_kv_store_service():
    """Create a mock KVStoreService for testing."""
    service = AsyncMock()
    service.get = AsyncMock()
    service.set = AsyncMock()
    service.delete = AsyncMock()
    service.get_kv_store = AsyncMock()
    return service


@pytest.fixture
def mock_kv_store():
    """Create a mock KV store for testing."""
    kv = AsyncMock()
    kv.get = AsyncMock()
    kv.create = AsyncMock()
    kv.update = AsyncMock()
    kv.delete = AsyncMock()
    return kv


@pytest.fixture
def mock_circuit_breaker():
    """Create a mock circuit breaker for testing."""
    circuit_breaker = AsyncMock()
    circuit_breaker.call = AsyncMock()
    return circuit_breaker


@pytest.fixture
def leader_election(mock_kv_store_service):
    """Create a LeaderElection instance for testing."""
    instance_id = f"test-instance-{uuid.uuid4().hex[:8]}"
    return LeaderElection(
        instance_id=instance_id,
        lock_ttl=30,
        lock_renew_interval=10,
        kv_store_service=mock_kv_store_service
    )


@pytest.mark.asyncio
async def test_leader_election_initialization(leader_election):
    """Test LeaderElection initialization."""
    assert leader_election.instance_id is not None
    assert leader_election.lock_ttl == 30
    assert leader_election.lock_renew_interval == 10
    assert leader_election._is_leader is False
    assert leader_election._lock_renewal_task is None
    assert leader_election._kv_store_service is not None
    assert hasattr(leader_election, '_last_lock_renewal')
    assert hasattr(leader_election, 'start_time')


@pytest.mark.asyncio
async def test_initialize_success(leader_election, mock_circuit_breaker):
    """Test successful initialization of leader election."""
    with patch('naq.scheduler.get_circuit_breaker', return_value=mock_circuit_breaker):
        await leader_election.initialize()
        # No exception should be raised
        assert leader_election._circuit_breaker is not None


@pytest.mark.asyncio
async def test_validate_kv_store_service_success(leader_election, mock_kv_store, mock_circuit_breaker):
    """Test successful KV store service validation."""
    leader_election._circuit_breaker = mock_circuit_breaker
    mock_circuit_breaker.call.return_value = mock_kv_store
    
    # Should not raise an exception
    await leader_election._validate_kv_store_service()
    
    mock_circuit_breaker.call.assert_called_once()


@pytest.mark.asyncio
async def test_validate_kv_store_service_no_service(mock_circuit_breaker):
    """Test KV store service validation without service."""
    leader_election = LeaderElection(
        instance_id="test-instance",
        kv_store_service=None
    )
    leader_election._circuit_breaker = mock_circuit_breaker
    
    with pytest.raises(NaqConnectionError, match="KVStoreService is required for leader election"):
        await leader_election._validate_kv_store_service()


@pytest.mark.asyncio
async def test_validate_kv_store_service_no_kv_store(leader_election, mock_circuit_breaker):
    """Test KV store service validation when KV store is not accessible."""
    leader_election._circuit_breaker = mock_circuit_breaker
    mock_circuit_breaker.call.return_value = None
    
    with pytest.raises(NaqConnectionError, match=f"KV store '{SCHEDULER_LOCK_KV_NAME}' is not accessible"):
        await leader_election._validate_kv_store_service()


@pytest.mark.asyncio
async def test_validate_kv_store_service_exception(leader_election, mock_circuit_breaker):
    """Test KV store service validation when an exception occurs."""
    leader_election._circuit_breaker = mock_circuit_breaker
    mock_circuit_breaker.call.side_effect = Exception("Connection error")
    
    with pytest.raises(NaqConnectionError, match="Failed to access KV store"):
        await leader_election._validate_kv_store_service()


@pytest.mark.asyncio
async def test_validate_kv_store_service_with_retry_success(leader_election, mock_kv_store, mock_circuit_breaker):
    """Test successful KV store service validation with retry."""
    leader_election._circuit_breaker = mock_circuit_breaker
    mock_circuit_breaker.call.return_value = mock_kv_store
    
    # Should not raise an exception
    await leader_election._validate_kv_store_service_with_retry()
    
    mock_circuit_breaker.call.assert_called_once()


@pytest.mark.asyncio
async def test_validate_kv_store_service_with_retry_no_service(mock_circuit_breaker):
    """Test KV store service validation with retry without service."""
    leader_election = LeaderElection(
        instance_id="test-instance",
        kv_store_service=None
    )
    leader_election._circuit_breaker = mock_circuit_breaker
    
    with pytest.raises(NaqConnectionError, match="KVStoreService is required for leader election"):
        await leader_election._validate_kv_store_service_with_retry()


@pytest.mark.asyncio
async def test_validate_kv_store_service_with_retry_succeeds_after_failure(leader_election, mock_kv_store, mock_circuit_breaker):
    """Test KV store service validation with retry succeeds after initial failure."""
    # Fail first call, succeed second call
    call_count = 0
    async def mock_get_kv_store(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise Exception("Temporary failure")
        return mock_kv_store
    
    leader_election._circuit_breaker = mock_circuit_breaker
    mock_circuit_breaker.call.side_effect = mock_get_kv_store
    
    # Should not raise an exception after retry
    await leader_election._validate_kv_store_service_with_retry()
    
    # Should have been called twice (initial failure + retry)
    assert call_count == 2


@pytest.mark.asyncio
async def test_validate_kv_store_service_with_retry_fails_after_max_retries(leader_election, mock_circuit_breaker):
    """Test KV store service validation with retry fails after max retries."""
    leader_election._circuit_breaker = mock_circuit_breaker
    mock_circuit_breaker.call.side_effect = Exception("Persistent failure")
    
    with pytest.raises(NaqConnectionError, match="Failed to access KV store.*after 3 attempts"):
        await leader_election._validate_kv_store_service_with_retry(max_retries=3)
    
    # Should have been called 3 times (initial + 2 retries)
    assert mock_circuit_breaker.call.call_count == 3


@pytest.mark.asyncio
async def test_initialize_without_kv_store():
    """Test initialization failure without KVStoreService."""
    leader_election = LeaderElection(
        instance_id="test-instance",
        kv_store_service=None
    )
    with pytest.raises(NaqConnectionError, match="KVStoreService is required for leader election"):
        await leader_election.initialize()


@pytest.fixture
def mock_kv_entry():
    """Create a mock KV entry for testing."""
    entry = MagicMock()
    entry.value = msgspec.msgpack.encode(LockData(
        instance_id="test-instance",
        timestamp=time.time(),
        hostname="test-host",
        pid=12345,
        start_time=time.time()
    ))
    entry.revision = 1
    return entry


@pytest.mark.asyncio
async def test_is_lock_held_by_other_no_lock(leader_election, mock_kv_store, mock_circuit_breaker):
    """Test _is_lock_held_by_other when no lock exists."""
    leader_election._circuit_breaker = mock_circuit_breaker
    
    # Mock circuit breaker calls
    async def mock_circuit_call(func):
        if func.__name__ == 'get_kv_store':
            return mock_kv_store
        elif func.__name__ == 'get_lock_entry':
            return None
    
    mock_circuit_breaker.call.side_effect = mock_circuit_call
    
    result = await leader_election._is_lock_held_by_other()
    
    assert result is False
    assert mock_circuit_breaker.call.call_count == 2  # get_kv_store and get_lock_entry


@pytest.mark.asyncio
async def test_is_lock_held_by_other_expired_lock(leader_election, mock_kv_store):
    """Test _is_lock_held_by_other when lock is expired."""
    expired_lock_data = LockData(
        instance_id="other-instance",
        timestamp=time.time() - 60,  # 60 seconds ago
        hostname="other-host",
        pid=12345,
        start_time=time.time() - 60
    )
    expired_entry = MagicMock()
    expired_entry.value = msgspec.msgpack.encode(expired_lock_data)
    mock_kv_store.get.return_value = expired_entry
    leader_election._kv_store_service.get_kv_store.return_value = mock_kv_store
    
    result = await leader_election._is_lock_held_by_other()
    
    assert result is False


@pytest.mark.asyncio
async def test_is_lock_held_by_other_valid_lock(leader_election, mock_kv_store):
    """Test _is_lock_held_by_other when lock is held by another instance."""
    valid_lock_data = LockData(
        instance_id="other-instance",
        timestamp=time.time() - 10,  # 10 seconds ago
        hostname="other-host",
        pid=12345,
        start_time=time.time() - 10
    )
    valid_entry = MagicMock()
    valid_entry.value = msgspec.msgpack.encode(valid_lock_data)
    mock_kv_store.get.return_value = valid_entry
    leader_election._kv_store_service.get_kv_store.return_value = mock_kv_store
    
    result = await leader_election._is_lock_held_by_other()
    
    assert result is True


@pytest.mark.asyncio
async def test_is_lock_held_by_other_our_lock(leader_election, mock_kv_store):
    """Test _is_lock_held_by_other when lock is held by us."""
    our_lock_data = LockData(
        instance_id=leader_election.instance_id,
        timestamp=time.time() - 10,  # 10 seconds ago
        hostname="our-host",
        pid=os.getpid(),
        start_time=time.time() - 10
    )
    our_entry = MagicMock()
    our_entry.value = msgspec.msgpack.encode(our_lock_data)
    mock_kv_store.get.return_value = our_entry
    leader_election._kv_store_service.get_kv_store.return_value = mock_kv_store
    
    result = await leader_election._is_lock_held_by_other()
    
    assert result is False


@pytest.mark.asyncio
async def test_check_leader_lock_health_no_lock(leader_election, mock_kv_store):
    """Test check_leader_lock_health when no lock exists."""
    mock_kv_store.get.return_value = None
    leader_election._kv_store_service.get_kv_store.return_value = mock_kv_store
    
    health = await leader_election.check_leader_lock_health()
    
    assert health["status"] == "no_lock"
    assert health["message"] == "No leader lock exists"
    assert health["is_leader"] is False
    assert health["instance_id"] == leader_election.instance_id


@pytest.mark.asyncio
async def test_check_leader_lock_health_healthy_lock(leader_election, mock_kv_store):
    """Test check_leader_lock_health when lock is healthy."""
    lock_data = LockData(
        instance_id=leader_election.instance_id,
        timestamp=time.time() - 10,
        hostname="our-host",
        pid=os.getpid(),
        start_time=time.time() - 10
    )
    lock_entry = MagicMock()
    lock_entry.value = msgspec.msgpack.encode(lock_data)
    mock_kv_store.get.return_value = lock_entry
    leader_election._kv_store_service.get_kv_store.return_value = mock_kv_store
    leader_election._is_leader = True
    
    health = await leader_election.check_leader_lock_health()
    
    assert health["status"] == "healthy"
    assert health["is_leader"] is True
    assert health["is_owned_by_us"] is True
    assert health["lock_owner"] == leader_election.instance_id
    assert health["lock_pid"] == os.getpid()
    assert "We hold the lock" in health["message"]


@pytest.mark.asyncio
async def test_check_leader_lock_health_expired_lock(leader_election, mock_kv_store):
    """Test check_leader_lock_health when lock is expired."""
    lock_data = LockData(
        instance_id="other-instance",
        timestamp=time.time() - 60,  # 60 seconds ago
        hostname="other-host",
        pid=12345,
        start_time=time.time() - 60
    )
    lock_entry = MagicMock()
    lock_entry.value = msgspec.msgpack.encode(lock_data)
    mock_kv_store.get.return_value = lock_entry
    leader_election._kv_store_service.get_kv_store.return_value = mock_kv_store
    
    health = await leader_election.check_leader_lock_health()
    
    assert health["status"] == "expired"
    assert health["is_leader"] is False
    assert health["is_owned_by_us"] is False
    assert health["lock_owner"] == "other-instance"
    assert "expired" in health["message"]


@pytest.mark.asyncio
async def test_check_leader_lock_health_error(leader_election):
    """Test check_leader_lock_health when there's an error."""
    leader_election._kv_store_service.get_kv_store.side_effect = Exception("Connection error")
    
    health = await leader_election.check_leader_lock_health()
    
    assert health["status"] == "error"
    assert "KVStoreService not available" in health["message"]
    assert health["is_leader"] is False


@pytest.mark.asyncio
async def test_acquire_lock_success(leader_election):
    """Test successful lock acquisition."""
    # First call to _is_lock_held_by_other returns False
    leader_election._is_lock_held_by_other = AsyncMock(return_value=False)
    
    # set returns success
    leader_election._kv_store_service.set.return_value = True
    
    # get returns our lock
    lock_data = {
        "instance_id": leader_election.instance_id,
        "timestamp": time.time(),
        "hostname": "our-host"
    }
    leader_election._kv_store_service.get.return_value = lock_data
    
    result = await leader_election._acquire_lock()
    
    assert result is True
    assert leader_election._is_leader is True
    leader_election._kv_store_service.set.assert_called_once()
    assert leader_election._last_lock_renewal > 0


@pytest.mark.asyncio
async def test_acquire_lock_held_by_other(leader_election):
    """Test lock acquisition when lock is held by another instance."""
    leader_election._is_lock_held_by_other = AsyncMock(return_value=True)
    
    result = await leader_election._acquire_lock()
    
    assert result is False
    assert leader_election._is_leader is False
    leader_election._kv_store_service.set.assert_not_called()


@pytest.mark.asyncio
async def test_acquire_lock_set_failure(leader_election):
    """Test lock acquisition when set fails."""
    leader_election._is_lock_held_by_other = AsyncMock(return_value=False)
    leader_election._kv_store_service.set.return_value = False
    
    result = await leader_election._acquire_lock()
    
    assert result is False
    assert leader_election._is_leader is False


@pytest.mark.asyncio
async def test_acquire_lock_verification_failure(leader_election):
    """Test lock acquisition when verification fails."""
    leader_election._is_lock_held_by_other = AsyncMock(return_value=False)
    leader_election._kv_store_service.set.return_value = True
    
    # get returns someone else's lock
    lock_data = {
        "instance_id": "other-instance",
        "timestamp": time.time(),
        "hostname": "other-host"
    }
    leader_election._kv_store_service.get.return_value = lock_data
    
    result = await leader_election._acquire_lock()
    
    assert result is False
    assert leader_election._is_leader is False


@pytest.mark.asyncio
async def test_try_become_leader_success(leader_election):
    """Test successful try_become_leader."""
    leader_election._is_lock_held_by_other = AsyncMock(return_value=False)
    leader_election.check_leader_lock_health = AsyncMock(return_value={"status": "no_lock"})
    leader_election._acquire_lock = AsyncMock(return_value=True)
    
    result = await leader_election.try_become_leader()
    
    assert result is True
    leader_election.check_leader_lock_health.assert_called_once()
    leader_election._acquire_lock.assert_called_once()


@pytest.mark.asyncio
async def test_try_become_leader_no_kv_store():
    """Test try_become_leader without KVStoreService."""
    leader_election = LeaderElection(
        instance_id="test-instance",
        kv_store_service=None
    )
    
    result = await leader_election.try_become_leader()
    
    assert result is False


@pytest.mark.asyncio
async def test_try_become_leader_lock_held_by_other(leader_election):
    """Test try_become_leader when lock is held by another instance."""
    leader_election._is_lock_held_by_other = AsyncMock(return_value=True)
    
    result = await leader_election.try_become_leader()
    
    assert result is False
    leader_election.check_leader_lock_health.assert_not_called()
    leader_election._acquire_lock.assert_not_called()


@pytest.mark.asyncio
async def test_try_become_leader_health_error(leader_election):
    """Test try_become_leader when health check returns error."""
    leader_election._is_lock_held_by_other = AsyncMock(return_value=False)
    leader_election.check_leader_lock_health = AsyncMock(return_value={"status": "error"})
    
    result = await leader_election.try_become_leader()
    
    assert result is False
    leader_election._acquire_lock.assert_not_called()


@pytest.mark.asyncio
async def test_renew_lock_success(leader_election):
    """Test successful lock renewal."""
    # get returns our lock
    lock_data = {
        "instance_id": leader_election.instance_id,
        "timestamp": time.time() - 10,
        "hostname": "our-host"
    }
    leader_election._kv_store_service.get.return_value = lock_data
    
    # set returns success
    leader_election._kv_store_service.set.return_value = True
    
    result = await leader_election._renew_lock()
    
    assert result is True
    leader_election._kv_store_service.set.assert_called_once()
    assert leader_election._last_lock_renewal > 0


@pytest.mark.asyncio
async def test_renew_lock_no_longer_leader(leader_election):
    """Test lock renewal when no longer the leader."""
    # get returns someone else's lock
    lock_data = {
        "instance_id": "other-instance",
        "timestamp": time.time() - 10,
        "hostname": "other-host"
    }
    leader_election._kv_store_service.get.return_value = lock_data
    
    result = await leader_election._renew_lock()
    
    assert result is False
    assert leader_election._is_leader is False
    leader_election._kv_store_service.set.assert_not_called()


@pytest.mark.asyncio
async def test_renew_lock_set_failure(leader_election):
    """Test lock renewal when set fails."""
    # get returns our lock
    lock_data = {
        "instance_id": leader_election.instance_id,
        "timestamp": time.time() - 10,
        "hostname": "our-host"
    }
    leader_election._kv_store_service.get.return_value = lock_data
    
    # set returns failure
    leader_election._kv_store_service.set.return_value = False
    
    result = await leader_election._renew_lock()
    
    assert result is False
    assert leader_election._is_leader is False


@pytest.mark.asyncio
async def test_start_renewal_task_success(leader_election):
    """Test successful start of renewal task."""
    leader_election._lock_renewal_task = None
    
    await leader_election.start_renewal_task(True)
    
    assert leader_election._lock_renewal_task is not None
    assert not leader_election._lock_renewal_task.done()


@pytest.mark.asyncio
async def test_start_renewal_task_already_running(leader_election):
    """Test start_renewal_task when task is already running."""
    mock_task = AsyncMock()
    mock_task.done.return_value = False
    leader_election._lock_renewal_task = mock_task
    
    await leader_election.start_renewal_task(True)
    
    # Should not create a new task
    assert leader_election._lock_renewal_task is mock_task


@pytest.mark.asyncio
async def test_start_renewal_task_no_kv_store():
    """Test start_renewal_task without KVStoreService."""
    leader_election = LeaderElection(
        instance_id="test-instance",
        kv_store_service=None
    )
    
    with pytest.raises(NaqConnectionError, match="KVStoreService is required for lock renewal"):
        await leader_election.start_renewal_task(True)


@pytest.mark.asyncio
async def test_renew_leader_lock_normal_operation(leader_election):
    """Test normal operation of _renew_leader_lock."""
    leader_election._renew_lock = AsyncMock(return_value=True)
    leader_election.check_leader_lock_health = AsyncMock(return_value={"status": "healthy", "is_owned_by_us": True})
    
    # Run for a short time
    task = asyncio.create_task(leader_election._renew_leader_lock(True))
    
    # Let it run for a bit
    await asyncio.sleep(0.1)
    
    # Stop the task
    leader_election._shutdown_event.set()
    
    # Wait for task to complete
    await task
    
    assert leader_election._renew_lock.called
    assert leader_election._is_leader is False


@pytest.mark.asyncio
async def test_renew_leader_lock_running_flag_false(leader_election):
    """Test _renew_leader_lock when running_flag is False."""
    leader_election._renew_lock = AsyncMock()
    leader_election.check_leader_lock_health = AsyncMock()
    
    await leader_election._renew_leader_lock(False)
    
    assert not leader_election._renew_lock.called
    assert leader_election._is_leader is False


@pytest.mark.asyncio
async def test_renew_leader_lock_renewal_failure(leader_election):
    """Test _renew_leader_lock when renewal fails."""
    leader_election._renew_lock = AsyncMock(return_value=False)
    leader_election.check_leader_lock_health = AsyncMock(return_value={"status": "healthy", "is_owned_by_us": True})
    
    # Run for a short time
    task = asyncio.create_task(leader_election._renew_leader_lock(True))
    
    # Let it run for a bit
    await asyncio.sleep(0.1)
    
    # Stop the task
    leader_election._shutdown_event.set()
    
    # Wait for task to complete
    await task
    
    assert leader_election._renew_lock.called
    assert leader_election._is_leader is False


@pytest.mark.asyncio
async def test_stop_renewal_task(leader_election):
    """Test stopping the renewal task."""
    mock_task = AsyncMock()
    mock_task.done.return_value = False
    leader_election._lock_renewal_task = mock_task
    
    await leader_election.stop_renewal_task()
    
    assert leader_election._shutdown_event.is_set()
    mock_task.cancel.assert_called_once()
    assert leader_election._is_leader is False


@pytest.mark.asyncio
async def test_release_lock_success(leader_election):
    """Test successful lock release."""
    leader_election._is_leader = True
    leader_election.check_leader_lock_health = AsyncMock(return_value={"is_owned_by_us": True})
    leader_election._kv_store_service.delete.return_value = None
    
    # Mock get to return no lock after deletion
    async def mock_get_after_delete(*args, **kwargs):
        if leader_election._kv_store_service.delete.call_count > 0:
            return None
        return {"instance_id": leader_election.instance_id}
    
    leader_election._kv_store_service.get.side_effect = mock_get_after_delete
    
    await leader_election.release_lock()
    
    assert leader_election._is_leader is False
    leader_election._kv_store_service.delete.assert_called_once_with(
        SCHEDULER_LOCK_KV_NAME, SCHEDULER_LOCK_KEY, purge=True
    )


@pytest.mark.asyncio
async def test_release_lock_not_leader(leader_election):
    """Test lock release when not the leader."""
    leader_election._is_leader = False
    
    await leader_election.release_lock()
    
    assert leader_election._is_leader is False
    leader_election._kv_store_service.delete.assert_not_called()


@pytest.mark.asyncio
async def test_release_lock_no_longer_owner(leader_election):
    """Test lock release when no longer the owner."""
    leader_election._is_leader = True
    leader_election.check_leader_lock_health = AsyncMock(return_value={"is_owned_by_us": False})
    
    await leader_election.release_lock()
    
    assert leader_election._is_leader is False
    leader_election._kv_store_service.delete.assert_not_called()


@pytest.mark.asyncio
async def test_release_lock_no_kv_store():
    """Test lock release without KVStoreService."""
    leader_election = LeaderElection(
        instance_id="test-instance",
        kv_store_service=None
    )
    
    await leader_election.release_lock()
    
    # Should not raise an exception


@pytest.mark.asyncio
async def test_is_leader_property(leader_election):
    """Test is_leader property."""
    assert leader_election.is_leader is False
    
    leader_election._is_leader = True
    assert leader_election.is_leader is True


@pytest.mark.asyncio
async def test_acquire_lock_timeout(leader_election, mock_kv_store):
    """Test lock acquisition timeout."""
    # Mock get_kv_store to return a mock KV store
    leader_election._kv_store_service.get_kv_store.return_value = mock_kv_store
    
    # Mock get to simulate a timeout
    async def mock_get_with_delay(*args, **kwargs):
        await asyncio.sleep(10)  # Longer than the timeout
        return None
    
    mock_kv_store.get.side_effect = mock_get_with_delay
    
    with pytest.raises(LockTimeoutError, match="Leader lock acquisition timed out"):
        await leader_election._acquire_lock()


@pytest.mark.asyncio
async def test_acquire_lock_connection_error(leader_election):
    """Test lock acquisition with connection error."""
    leader_election._kv_store_service.get_kv_store.side_effect = Exception("Connection error")
    
    with pytest.raises(LockAcquisitionError, match="Connection error acquiring leader lock"):
        await leader_election._acquire_lock()


@pytest.mark.asyncio
async def test_renew_lock_timeout(leader_election, mock_kv_store):
    """Test lock renewal timeout."""
    # Mock get_kv_store to return a mock KV store
    leader_election._kv_store_service.get_kv_store.return_value = mock_kv_store
    
    # Mock get to simulate a timeout
    async def mock_get_with_delay(*args, **kwargs):
        await asyncio.sleep(10)  # Longer than the timeout
        return None
    
    mock_kv_store.get.side_effect = mock_get_with_delay
    
    with pytest.raises(LockTimeoutError, match="Leader lock renewal timed out"):
        await leader_election._renew_lock()


@pytest.mark.asyncio
async def test_renew_lock_connection_error(leader_election):
    """Test lock renewal with connection error."""
    leader_election._kv_store_service.get_kv_store.side_effect = Exception("Connection error")
    
    with pytest.raises(LockRenewalError, match="Connection error renewing leader lock"):
        await leader_election._renew_lock()


@pytest.mark.asyncio
async def test_release_lock_connection_error(leader_election):
    """Test lock release with connection error."""
    leader_election._is_leader = True
    leader_election.check_leader_lock_health = AsyncMock(return_value={"is_owned_by_us": True})
    leader_election._kv_store_service.get_kv_store.side_effect = Exception("Connection error")
    
    with pytest.raises(LockReleaseError, match="Connection error releasing leader lock"):
        await leader_election.release_lock()


@pytest.mark.asyncio
async def test_circuit_breaker_integration(leader_election, mock_kv_store, mock_circuit_breaker):
    """Test that circuit breaker is properly integrated with KV store operations."""
    leader_election._circuit_breaker = mock_circuit_breaker
    
    # Mock circuit breaker to return the mock KV store
    async def mock_circuit_call(func):
        if func.__name__ == 'get_kv_store':
            return mock_kv_store
        elif func.__name__ == 'get_lock_entry':
            return None
        elif func.__name__ == 'update_lock':
            return True
        elif func.__name__ == 'delete_lock':
            return None
    
    mock_circuit_breaker.call.side_effect = mock_circuit_call
    
    # Test that circuit breaker is used in _is_lock_held_by_other
    await leader_election._is_lock_held_by_other()
    assert mock_circuit_breaker.call.called
    
    # Reset mock
    mock_circuit_breaker.reset_mock()
    
    # Test that circuit breaker is used in check_leader_lock_health
    await leader_election.check_leader_lock_health()
    assert mock_circuit_breaker.call.called
    
    # Reset mock
    mock_circuit_breaker.reset_mock()
    
    # Test that circuit breaker is used in _perform_lock_acquisition
    try:
        await leader_election._perform_lock_acquisition()
    except Exception:
        pass  # We don't care about the result, just that circuit breaker was called
    assert mock_circuit_breaker.call.called
    
    # Reset mock
    mock_circuit_breaker.reset_mock()
    
    # Test that circuit breaker is used in _perform_lock_renewal
    try:
        await leader_election._perform_lock_renewal(time.time())
    except Exception:
        pass  # We don't care about the result, just that circuit breaker was called
    assert mock_circuit_breaker.call.called
    
    # Reset mock
    mock_circuit_breaker.reset_mock()
    
    # Test that circuit breaker is used in _perform_lock_release
    try:
        await leader_election._perform_lock_release()
    except Exception:
        pass  # We don't care about the result, just that circuit breaker was called
    assert mock_circuit_breaker.call.called


@pytest.mark.asyncio
async def test_circuit_breaker_failure_handling(leader_election, mock_circuit_breaker):
    """Test that circuit breaker failures are properly handled."""
    leader_election._circuit_breaker = mock_circuit_breaker
    
    # Mock circuit breaker to raise an exception
    mock_circuit_breaker.call.side_effect = NaqConnectionError("Circuit breaker open")
    
    # Test that _is_lock_held_by_other handles circuit breaker failures
    result = await leader_election._is_lock_held_by_other()
    assert result is True  # Should conservatively return True
    
    # Test that check_leader_lock_health handles circuit breaker failures
    health = await leader_election.check_leader_lock_health()
    assert health["status"] == "error"
    
    # Test that _perform_lock_acquisition handles circuit breaker failures
    with pytest.raises(LockAcquisitionError):
        await leader_election._perform_lock_acquisition()
    
    # Test that _perform_lock_renewal handles circuit breaker failures
    with pytest.raises(LockRenewalError):
        await leader_election._perform_lock_renewal(time.time())
    
    # Test that _perform_lock_release handles circuit breaker failures
    with pytest.raises(LockReleaseError):
        await leader_election._perform_lock_release()