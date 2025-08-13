# Subtask 08: Update Connection Tests

## Overview
Update and expand the test suite to validate the new connection management system, including context managers, utilities, decorators, and migrated code.

## Objectives
- Create comprehensive unit tests for new connection components
- Update existing tests to work with new connection patterns
- Add integration tests for connection management
- Create performance and load tests for connection handling
- Ensure test coverage for error scenarios

## Implementation Details

### Test Files to Create/Update

#### 1. Unit Tests for New Components
**File: `tests/unit/test_connection_context_managers.py`**

```python
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from naq.connection.context_managers import (
    nats_connection,
    jetstream_context,
    nats_jetstream,
    nats_kv_store
)
from naq.connection import get_config


class TestNatsConnection:
    """Test NATS connection context manager."""
    
    @pytest.mark.asyncio
    async def test_context_manager_enters_and_exits(self):
        """Test that context manager properly enters and exits."""
        mock_conn = AsyncMock()
        mock_connect = AsyncMock(return_value=mock_conn)
        
        with patch('naq.connection.context_managers.nats.connect', mock_connect):
            async with nats_connection() as conn:
                assert conn == mock_conn
                mock_connect.assert_called_once()
        
        mock_conn.close.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_context_manager_handles_exceptions(self):
        """Test that context manager handles exceptions properly."""
        mock_conn = AsyncMock()
        mock_connect = AsyncMock(return_value=mock_conn)
        test_exception = ValueError("Test error")
        
        with patch('naq.connection.context_managers.nats.connect', mock_connect):
            with pytest.raises(ValueError, match="Test error"):
                async with nats_connection():
                    raise test_exception
        
        mock_conn.close.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_context_manager_uses_config(self):
        """Test that context manager uses provided config."""
        mock_config = MagicMock()
        mock_conn = AsyncMock()
        mock_connect = AsyncMock(return_value=mock_conn)
        
        with patch('naq.connection.context_managers.nats.connect', mock_connect):
            with patch('naq.connection.context_managers.get_config', return_value=mock_config):
                async with nats_connection():
                    pass
        
        mock_connect.assert_called_once_with(
            servers=mock_config.nats.servers,
            name=mock_config.nats.client_name,
            max_reconnect_attempts=mock_config.nats.max_reconnect_attempts,
            reconnect_time_wait=mock_config.nats.reconnect_time_wait,
            connection_timeout=mock_config.nats.connection_timeout,
        )


class TestJetStreamContext:
    """Test JetStream context context manager."""
    
    @pytest.mark.asyncio
    async def test_context_manager_creates_jetstream(self):
        """Test that JetStream context creates JetStream context."""
        mock_conn = AsyncMock()
        mock_js = AsyncMock()
        mock_conn.jetstream.return_value = mock_js
        
        async with jetstream_context(mock_conn) as js:
            assert js == mock_js
            mock_conn.jetstream.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_context_manager_handles_jetstream_errors(self):
        """Test that JetStream context handles JetStream errors."""
        mock_conn = AsyncMock()
        mock_js = AsyncMock()
        mock_conn.jetstream.return_value = mock_js
        test_exception = RuntimeError("JetStream error")
        
        with patch('naq.connection.context_managers.logger'):
            with pytest.raises(RuntimeError, match="JetStream error"):
                async with jetstream_context(mock_conn):
                    raise test_exception


class TestNatsJetstream:
    """Test combined NATS and JetStream context manager."""
    
    @pytest.mark.asyncio
    async def test_combined_context_manager(self):
        """Test that combined context manager works correctly."""
        mock_conn = AsyncMock()
        mock_js = AsyncMock()
        
        with patch('naq.connection.context_managers.nats_connection') as mock_nats_conn, \
             patch('naq.connection.context_managers.jetstream_context') as mock_js_ctx:
            
            mock_nats_conn.return_value.__aenter__.return_value = mock_conn
            mock_nats_conn.return_value.__aexit__.return_value = None
            mock_js_ctx.return_value.__aenter__.return_value = mock_js
            mock_js_ctx.return_value.__aexit__.return_value = None
            
            async with nats_jetstream() as (conn, js):
                assert conn == mock_conn
                assert js == mock_js


class TestNatsKVStore:
    """Test NATS KeyValue store context manager."""
    
    @pytest.mark.asyncio
    async def test_kv_store_context_manager(self):
        """Test that KV store context manager works correctly."""
        mock_conn = AsyncMock()
        mock_js = AsyncMock()
        mock_kv = AsyncMock()
        mock_bucket_name = "test_bucket"
        
        with patch('naq.connection.context_managers.nats_jetstream') as mock_nats_js, \
             patch('naq.connection.context_managers.logger'):
            
            mock_nats_js.return_value.__aenter__.return_value = (mock_conn, mock_js)
            mock_nats_js.return_value.__aexit__.return_value = None
            mock_js.key_value.return_value = mock_kv
            
            async with nats_kv_store(mock_bucket_name) as kv:
                assert kv == mock_kv
                mock_js.key_value.assert_called_once_with(mock_bucket_name)
    
    @pytest.mark.asyncio
    async def test_kv_store_handles_key_value_errors(self):
        """Test that KV store handles KeyValue errors."""
        mock_conn = AsyncMock()
        mock_js = AsyncMock()
        mock_bucket_name = "test_bucket"
        test_exception = RuntimeError("KV error")
        
        with patch('naq.connection.context_managers.nats_jetstream') as mock_nats_js, \
             patch('naq.connection.context_managers.logger'):
            
            mock_nats_js.return_value.__aenter__.return_value = (mock_conn, mock_js)
            mock_nats_js.return_value.__aexit__.return_value = None
            mock_js.key_value.side_effect = test_exception
            
            with pytest.raises(RuntimeError, match="KV error"):
                async with nats_kv_store(mock_bucket_name):
                    pass
```

**File: `tests/unit/test_connection_utils.py`**

```python
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from naq.connection.utils import (
    ConnectionMetrics,
    ConnectionMonitor,
    test_nats_connection,
    wait_for_nats_connection,
    get_connection_metrics,
    reset_connection_metrics
)


class TestConnectionMetrics:
    """Test ConnectionMetrics dataclass."""
    
    def test_metrics_initialization(self):
        """Test that metrics initialize with default values."""
        metrics = ConnectionMetrics()
        assert metrics.total_connections == 0
        assert metrics.active_connections == 0
        assert metrics.failed_connections == 0
        assert metrics.average_connection_time == 0.0


class TestConnectionMonitor:
    """Test ConnectionMonitor class."""
    
    @pytest.mark.asyncio
    async def test_record_connection_start(self):
        """Test recording connection start."""
        monitor = ConnectionMonitor()
        
        await monitor.record_connection_start()
        assert monitor.metrics.total_connections == 1
        assert monitor.metrics.active_connections == 1
    
    @pytest.mark.asyncio
    async def test_record_connection_end(self):
        """Test recording connection end."""
        monitor = ConnectionMonitor()
        
        await monitor.record_connection_start()
        await monitor.record_connection_end(1.5)
        
        assert monitor.metrics.active_connections == 0
        assert monitor.metrics.average_connection_time == 1.5
        assert monitor.metrics.successful_connections == 1
    
    @pytest.mark.asyncio
    async def test_record_connection_failure(self):
        """Test recording connection failure."""
        monitor = ConnectionMonitor()
        
        await monitor.record_connection_start()
        await monitor.record_connection_failure()
        
        assert monitor.metrics.active_connections == 1
        assert monitor.metrics.failed_connections == 1
    
    @pytest.mark.asyncio
    async def test_get_metrics(self):
        """Test getting current metrics."""
        monitor = ConnectionMonitor()
        
        await monitor.record_connection_start()
        await monitor.record_connection_end(2.0)
        
        metrics = monitor.get_metrics()
        assert metrics.total_connections == 1
        assert metrics.average_connection_time == 2.0
    
    @pytest.mark.asyncio
    async def test_reset_metrics(self):
        """Test resetting metrics."""
        monitor = ConnectionMonitor()
        
        await monitor.record_connection_start()
        await monitor.record_connection_end(1.0)
        
        monitor.reset_metrics()
        metrics = monitor.get_metrics()
        
        assert metrics.total_connections == 0
        assert metrics.average_connection_time == 0.0


class TestConnectionUtilities:
    """Test connection utility functions."""
    
    @pytest.mark.asyncio
    async def test_test_nats_connection_success(self):
        """Test successful NATS connection test."""
        mock_conn = AsyncMock()
        
        with patch('naq.connection.utils.nats_connection') as mock_nats_conn, \
             patch('naq.connection.utils.logger'):
            
            mock_nats_conn.return_value.__aenter__.return_value = mock_conn
            mock_nats_conn.return_value.__aexit__.return_value = None
            
            result = await test_nats_connection()
            assert result is True
            mock_conn.flush.assert_called_once_with(timeout=5.0)
    
    @pytest.mark.asyncio
    async def test_test_nats_connection_failure(self):
        """Test failed NATS connection test."""
        with patch('naq.connection.utils.nats_connection') as mock_nats_conn, \
             patch('naq.connection.utils.logger') as mock_logger:
            
            mock_nats_conn.return_value.__aenter__.side_effect = RuntimeError("Connection failed")
            mock_nats_conn.return_value.__aexit__.return_value = None
            
            result = await test_nats_connection()
            assert result is False
            mock_logger.error.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_wait_for_nats_connection_success(self):
        """Test successful waiting for NATS connection."""
        mock_conn = AsyncMock()
        
        with patch('naq.connection.utils.test_nats_connection') as mock_test, \
             patch('naq.connection.utils.nats_connection') as mock_nats_conn, \
             patch('naq.connection.utils.asyncio') as mock_asyncio:
            
            mock_test.side_effect = [False, True]  # Fail first, succeed second
            mock_nats_conn.return_value.__aenter__.return_value = mock_conn
            mock_nats_conn.return_value.__aexit__.return_value = None
            mock_asyncio.get_event_loop.return_value.time.side_effect = [0, 1, 2]  # Mock time progression
            
            result = await wait_for_nats_connection(timeout=5)
            assert result is True
            assert mock_test.call_count == 2
    
    @pytest.mark.asyncio
    async def test_wait_for_nats_connection_timeout(self):
        """Test timeout when waiting for NATS connection."""
        with patch('naq.connection.utils.test_nats_connection', return_value=False), \
             patch('naq.connection.utils.asyncio') as mock_asyncio:
            
            mock_asyncio.get_event_loop.return_value.time.side_effect = [0, 1, 2, 3, 4, 5]  # Time progresses to timeout
            
            result = await wait_for_nats_connection(timeout=5)
            assert result is False


class TestGlobalConnectionMonitor:
    """Test global connection monitor functions."""
    
    @pytest.mark.asyncio
    async def test_get_connection_metrics(self):
        """Test getting global connection metrics."""
        with patch('naq.connection.utils.connection_monitor') as mock_monitor:
            mock_monitor.get_metrics.return_value = ConnectionMetrics(total_connections=5)
            
            metrics = await get_connection_metrics()
            assert metrics.total_connections == 5
            mock_monitor.get_metrics.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_reset_connection_metrics(self):
        """Test resetting global connection metrics."""
        with patch('naq.connection.utils.connection_monitor') as mock_monitor:
            await reset_connection_metrics()
            mock_monitor.reset_metrics.assert_called_once()
```

**File: `tests/unit/test_connection_decorators.py`**

```python
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from naq.connection.decorators import (
    with_nats_connection,
    with_jetstream_context,
    with_kv_store,
    with_connection_monitoring,
    retry_on_connection_failure
)


class TestWithNatsConnection:
    """Test @with_nats_connection decorator."""
    
    @pytest.mark.asyncio
    async def test_decorator_injects_connection(self):
        """Test that decorator injects NATS connection."""
        mock_conn = AsyncMock()
        
        @with_nats_connection()
        async def test_function(conn, arg1, arg2=None):
            return f"test_{arg1}_{arg2}"
        
        with patch('naq.connection.decorators.nats_connection') as mock_nats_conn, \
             patch('naq.connection.decorators.get_config'):
            
            mock_nats_conn.return_value.__aenter__.return_value = mock_conn
            mock_nats_conn.return_value.__aexit__.return_value = None
            
            result = await test_function("value1", arg2="value2")
            assert result == "test_value1_value2"
            mock_nats_conn.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_decorator_handles_exceptions(self):
        """Test that decorator handles exceptions properly."""
        test_exception = ValueError("Test error")
        
        @with_nats_connection()
        async def test_function(conn):
            raise test_exception
        
        with patch('naq.connection.decorators.nats_connection') as mock_nats_conn, \
             patch('naq.connection.decorators.get_config'), \
             patch('naq.connection.decorators.logger'):
            
            mock_nats_conn.return_value.__aenter__.return_value = AsyncMock()
            mock_nats_conn.return_value.__aexit__.return_value = None
            
            with pytest.raises(ValueError, match="Test error"):
                await test_function()


class TestWithJetStreamContext:
    """Test @with_jetstream_context decorator."""
    
    @pytest.mark.asyncio
    async def test_decorator_injects_jetstream(self):
        """Test that decorator injects JetStream context."""
        mock_js = AsyncMock()
        
        @with_jetstream_context()
        async def test_function(js, stream_name):
            return f"stream_{stream_name}"
        
        with patch('naq.connection.decorators.nats_jetstream') as mock_nats_js, \
             patch('naq.connection.decorators.get_config'):
            
            mock_nats_js.return_value.__aenter__.return_value = (AsyncMock(), mock_js)
            mock_nats_js.return_value.__aexit__.return_value = None
            
            result = await test_function("test_stream")
            assert result == "stream_test_stream"
            mock_nats_js.assert_called_once()


class TestWithKVStore:
    """Test @with_kv_store decorator."""
    
    @pytest.mark.asyncio
    async def test_decorator_injects_kv_store(self):
        """Test that decorator injects KV store."""
        mock_kv = AsyncMock()
        
        @with_kv_store("test_bucket")
        async def test_function(kv, key, value):
            return f"put_{key}_{value}"
        
        with patch('naq.connection.decorators.nats_kv_store') as mock_nats_kv, \
             patch('naq.connection.decorators.get_config'):
            
            mock_nats_kv.return_value.__aenter__.return_value = mock_kv
            mock_nats_kv.return_value.__aexit__.return_value = None
            
            result = await test_function("key1", "value1")
            assert result == "put_key1_value1"
            mock_nats_kv.assert_called_once_with("test_bucket")


class TestWithConnectionMonitoring:
    """Test @with_connection_monitoring decorator."""
    
    @pytest.mark.asyncio
    async def test_decorator_monitors_successful_connection(self):
        """Test that decorator monitors successful connections."""
        with patch('naq.connection.decorators.connection_monitor') as mock_monitor:
            
            @with_connection_monitoring
            @with_nats_connection()
            async def test_function(conn):
                return "success"
            
            with patch('naq.connection.decorators.nats_connection') as mock_nats_conn, \
                 patch('naq.connection.decorators.get_config'):
                
                mock_nats_conn.return_value.__aenter__.return_value = AsyncMock()
                mock_nats_conn.return_value.__aexit__.return_value = None
                
                result = await test_function()
                assert result == "success"
                
                # Verify monitoring calls
                mock_monitor.record_connection_start.assert_called_once()
                mock_monitor.record_connection_end.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_decorator_monitors_failed_connection(self):
        """Test that decorator monitors failed connections."""
        test_exception = ValueError("Test error")
        
        with patch('naq.connection.decorators.connection_monitor') as mock_monitor:
            
            @with_connection_monitoring
            @with_nats_connection()
            async def test_function(conn):
                raise test_exception
            
            with patch('naq.connection.decorators.nats_connection') as mock_nats_conn, \
                 patch('naq.connection.decorators.get_config'), \
                 patch('naq.connection.decorators.logger'):
                
                mock_nats_conn.return_value.__aenter__.return_value = AsyncMock()
                mock_nats_conn.return_value.__aexit__.return_value = None
                
                with pytest.raises(ValueError, match="Test error"):
                    await test_function()
                
                # Verify monitoring calls
                mock_monitor.record_connection_start.assert_called_once()
                mock_monitor.record_connection_failure.assert_called_once()


class TestRetryOnConnectionFailure:
    """Test @retry_on_connection_failure decorator."""
    
    @pytest.mark.asyncio
    async def test_decorator_retries_on_connection_failure(self):
        """Test that decorator retries on connection failures."""
        from nats.errors import ConnectionClosedError
        
        @retry_on_connection_failure(max_retries=2)
        @with_nats_connection()
        async def test_function(conn):
            # Fail first two times, succeed third
            if test_function.call_count < 2:
                test_function.call_count += 1
                raise ConnectionClosedError("Connection lost")
            return "success"
        
        test_function.call_count = 0
        
        with patch('naq.connection.decorators.nats_connection') as mock_nats_conn, \
             patch('naq.connection.decorators.get_config'), \
             patch('naq.connection.decorators.logger') as mock_logger, \
             patch('naq.connection.decorators.asyncio') as mock_asyncio:
            
            mock_nats_conn.return_value.__aenter__.return_value = AsyncMock()
            mock_nats_conn.return_value.__aexit__.return_value = None
            mock_asyncio.sleep = AsyncMock()
            
            result = await test_function()
            assert result == "success"
            
            # Should have been called 3 times (initial + 2 retries)
            assert mock_nats_conn.call_count == 3
            assert mock_logger.warning.call_count == 2
    
    @pytest.mark.asyncio
    async def test_decorator_fails_after_max_retries(self):
        """Test that decorator fails after max retries."""
        from nats.errors import ConnectionClosedError
        
        @retry_on_connection_failure(max_retries=2)
        @with_nats_connection()
        async def test_function(conn):
            raise ConnectionClosedError("Connection lost")
        
        with patch('naq.connection.decorators.nats_connection') as mock_nats_conn, \
             patch('naq.connection.decorators.get_config'), \
             patch('naq.connection.decorators.logger') as mock_logger, \
             patch('naq.connection.decorators.asyncio') as mock_asyncio:
            
            mock_nats_conn.return_value.__aenter__.return_value = AsyncMock()
            mock_nats_conn.return_value.__aexit__.return_value = None
            mock_asyncio.sleep = AsyncMock()
            
            with pytest.raises(ConnectionClosedError):
                await test_function()
            
            # Should have been called 3 times (initial + 2 retries)
            assert mock_nats_conn.call_count == 3
            assert mock_logger.warning.call_count == 2
            assert mock_logger.error.call_count == 1
```

#### 2. Updated Integration Tests
**File: `tests/integration/test_connection_migration.py`**

```python
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from naq.connection.context_managers import nats_connection, nats_jetstream, nats_kv_store
from naq.connection.utils import test_nats_connection, wait_for_nats_connection


class TestConnectionMigration:
    """Integration tests for connection migration."""
    
    @pytest.mark.asyncio
    async def test_migration_preserves_functionality(self):
        """Test that migration preserves existing functionality."""
        # Mock the underlying NATS connection
        mock_conn = AsyncMock()
        mock_js = AsyncMock()
        mock_kv = AsyncMock()
        
        with patch('naq.connection.context_managers.nats.connect') as mock_connect, \
             patch('naq.connection.context_managers.logger'):
            
            mock_connect.return_value = mock_conn
            mock_conn.jetstream.return_value = mock_js
            mock_js.key_value.return_value = mock_kv
            
            # Test old pattern vs new pattern
            # Old pattern (simulated)
            old_result = await self._simulate_old_pattern(mock_conn, mock_js, mock_kv)
            
            # New pattern
            new_result = await self._simulate_new_pattern(mock_conn, mock_js, mock_kv)
            
            assert old_result == new_result
    
    async def _simulate_old_pattern(self, conn, js, kv):
        """Simulate old connection pattern."""
        try:
            # Simulate old pattern operations
            await js.add_stream(name="test_stream", subjects=["test.*"])
            await kv.put("test_key", "test_value")
            return "old_pattern_success"
        finally:
            await conn.close()
    
    async def _simulate_new_pattern(self, conn, js, kv):
        """Simulate new connection pattern."""
        async with nats_jetstream() as (c, j):
            await j.add_stream(name="test_stream", subjects=["test.*"])
            async with nats_kv_store("test_bucket") as k:
                await k.put("test_key", "test_value")
        return "new_pattern_success"
    
    @pytest.mark.asyncio
    async def test_connection_error_handling_migration(self):
        """Test that connection error handling is preserved after migration."""
        from nats.errors import ConnectionClosedError, TimeoutError
        
        # Test old pattern error handling
        old_error_caught = False
        try:
            # Simulate old pattern error
            raise ConnectionClosedError("Connection lost")
        except ConnectionClosedError:
            old_error_caught = True
        
        # Test new pattern error handling
        new_error_caught = False
        try:
            async with nats_connection():
                raise ConnectionClosedError("Connection lost")
        except ConnectionClosedError:
            new_error_caught = True
        
        assert old_error_caught == new_error_caught
        assert old_error_caught is True
    
    @pytest.mark.asyncio
    async def test_performance_comparison(self):
        """Test that new connection patterns don't degrade performance."""
        import time
        
        # Mock fast connections for testing
        with patch('naq.connection.context_managers.nats.connect') as mock_connect:
            mock_connect.return_value = AsyncMock()
            
            # Measure old pattern performance
            start_time = time.time()
            await self._simulate_old_pattern_performance()
            old_time = time.time() - start_time
            
            # Measure new pattern performance
            start_time = time.time()
            await self._simulate_new_pattern_performance()
            new_time = time.time() - start_time
            
            # New pattern should not be significantly slower
            assert new_time <= old_time * 1.5  # Allow 50% overhead
    
    async def _simulate_old_pattern_performance(self):
        """Simulate old pattern performance test."""
        # Simulate multiple connection operations
        for i in range(10):
            conn = await self._get_mock_connection()
            js = await self._get_mock_jetstream(conn)
            await js.publish(f"test.{i}", f"message_{i}".encode())
            await conn.close()
    
    async def _simulate_new_pattern_performance(self):
        """Simulate new pattern performance test."""
        # Simulate multiple connection operations with context managers
        for i in range(10):
            async with nats_connection() as conn:
                async with nats_jetstream() as (c, js):
                    await js.publish(f"test.{i}", f"message_{i}".encode()
```

#### 3. Performance Tests
**File: `tests/performance/test_connection_performance.py`**

```python
import pytest
import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch
from naq.connection.utils import measure_connection_performance, connection_monitor


class TestConnectionPerformance:
    """Performance tests for connection management."""
    
    @pytest.mark.asyncio
    async def test_connection_estimation_performance(self):
        """Test connection establishment performance measurement."""
        with patch('naq.connection.utils.nats_connection') as mock_nats_conn, \
             patch('naq.connection.utils.logger'):
            
            mock_nats_conn.return_value.__aenter__.return_value = AsyncMock()
            mock_nats_conn.return_value.__aexit__.return_value = None
            
            # Mock fast connections
            async def mock_fast_context():
                await asyncio.sleep(0.001)  # Simulate 1ms connection time
                yield AsyncMock()
            
            mock_nats_conn.return_value = mock_fast_context()
            
            performance = await measure_connection_performance(iterations=5)
            
            assert performance["average_time"] > 0
            assert performance["min_time"] > 0
            assert performance["max_time"] > 0
            assert performance["success_rate"] == 1.0
    
    @pytest.mark.asyncio
    async def test_connection_monitor_performance(self):
        """Test that connection monitoring doesn't significantly impact performance."""
        import time
        
        with patch('naq.connection.utils.nats_connection') as mock_nats_conn, \
             patch('naq.connection.utils.logger'):
            
            mock_nats_conn.return_value.__aenter__.return_value = AsyncMock()
            mock_nats_conn.return_value.__aexit__.return_value = None
            
            # Measure performance without monitoring
            start_time = time.time()
            for _ in range(100):
                async with nats_connection():
                    pass
            base_time = time.time() - start_time
            
            # Measure performance with monitoring
            start_time = time.time()
            for _ in range(100):
                async with nats_connection():
                    pass
            monitored_time = time.time() - start_time
            
            # Monitoring should add minimal overhead (< 10%)
            overhead = (monitored_time - base_time) / base_time
            assert overhead < 0.1
    
    @pytest.mark.asyncio
    async def test_concurrent_connections_performance(self):
        """Test performance under concurrent connection usage."""
        import concurrent.futures
        
        with patch('naq.connection.utils.nats_connection') as mock_nats_conn, \
             patch('naq.connection.utils.logger'):
            
            mock_nats_conn.return_value.__aenter__.return_value = AsyncMock()
            mock_nats_conn.return_value.__aexit__.return_value = None
            
            async def create_connection():
                async with nats_connection():
                    await asyncio.sleep(0.01)  # Simulate work
                    return "connection_created"
            
            # Test concurrent connections
            start_time = time.time()
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                loop = asyncio.get_event_loop()
                tasks = [loop.create_task(create_connection()) for _ in range(50)]
                results = await asyncio.gather(*tasks)
            concurrent_time = time.time() - start_time
            
            assert len(results) == 50
            assert all(result == "connection_created" for result in results)
            assert concurrent_time < 1.0  # Should complete in under 1 second
    
    @pytest.mark.asyncio
    async def test_memory_usage_under_load(self):
        """Test memory usage under high connection load."""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
        
        with patch('naq.connection.utils.nats_connection') as mock_nats_conn, \
             patch('naq.connection.utils.logger'):
            
            mock_nats_conn.return_value.__aenter__.return_value = AsyncMock()
            mock_nats_conn.return_value.__aexit__.return_value = None
            
            # Create many connections
            for _ in range(1000):
                async with nats_connection():
                    pass
            
            final_memory = process.memory_info().rss
            memory_increase = final_memory - initial_memory
            
            # Memory increase should be reasonable (< 100MB)
            assert memory_increase < 100 * 1024 * 1024  # 100MB
```

### Test Configuration

#### pytest Configuration
Update `pytest.ini` or `pyproject.toml`:

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
python_classes = ["Test*"]
python_functions = ["test_*"]
asyncio_mode = "auto"
markers = [
    "unit: marks tests as unit tests",
    "integration: marks tests as integration tests", 
    "performance: marks tests as performance tests",
    "slow: marks tests as slow running"
]
addopts = [
    "-v",
    "--tb=short",
    "--strict-markers",
    "--disable-warnings"
]
```

#### Test Coverage Requirements
Ensure comprehensive coverage of:
- Context manager lifecycle (enter, exit, error conditions)
- Connection failure handling
- Configuration parameter usage
- Resource cleanup verification
- Decorator functionality
- Performance characteristics
- Concurrent usage patterns
- Memory usage under load

### Requirements
- All new connection components have comprehensive unit tests
- Existing tests updated to work with new connection patterns
- Integration tests validate migration success
- Performance tests ensure no regressions
- Test coverage meets or exceeds 90%
- All tests pass in CI/CD environment
- Tests include both success and failure scenarios

## Success Criteria
- [ ] All new connection components have unit tests
- [ ] Existing tests updated and passing
- [ ] Integration tests validate migration
- [ ] Performance tests validate no regressions
- [ ] Test coverage meets requirements
- [ ] All tests pass in CI/CD
- [ ] Error scenarios properly tested

## Dependencies
- Task 01 (Connection Context Managers) - context manager implementation
- Task 02 (Connection Utilities) - utility functions
- Task 03 (Connection Decorators) - decorator implementation
- Task 05-07 (Migration tasks) - migrated code to test
- pytest framework
- pytest-asyncio for async tests
- pytest-mock for mocking

## Estimated Time
- 8-10 hours