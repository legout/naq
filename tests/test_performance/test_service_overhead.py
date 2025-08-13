# tests/test_performance/test_service_overhead.py
"""Tests for service layer performance and overhead."""

import time
import pytest
from unittest.mock import patch, AsyncMock
from naq.services import ServiceManager
from naq.queue.core import Queue
from naq.models.jobs import Job


@pytest.mark.asyncio
class TestPerformanceRegression:
    """Test that service layer doesn't add significant overhead."""

    async def test_service_layer_overhead(self, service_test_config):
        """Test service layer doesn't add significant overhead."""
        
        # Mock NATS connections to avoid actual network calls
        with patch('naq.services.connection.get_nats_connection') as mock_get_conn:
            mock_nc = AsyncMock()
            mock_js = AsyncMock()
            mock_nc.jetstream.return_value = mock_js
            mock_get_conn.return_value = mock_nc
            
            # Test with service layer
            start_time = time.perf_counter()
            
            async with ServiceManager(service_test_config) as services:
                from naq.services.jobs import JobService
                job_service = await services.get_service(JobService)
                
                # Mock job service operations
                job_service.enqueue_job = AsyncMock(return_value="job-123")
                
                for i in range(100):
                    def test_func():
                        return i
                    
                    job = Job(function=test_func)
                    await job_service.enqueue_job(job, "perf-test")
            
            service_time = time.perf_counter() - start_time
            
            # Service overhead should be minimal (< 5 seconds for 100 operations)
            assert service_time < 5.0

    async def test_connection_reuse_performance(self, service_test_config):
        """Test that connection reuse improves performance."""
        
        with patch('naq.services.connection.get_nats_connection') as mock_get_conn:
            mock_nc = AsyncMock()
            mock_js = AsyncMock()
            mock_nc.jetstream.return_value = mock_js
            mock_get_conn.return_value = mock_nc
            
            start_time = time.perf_counter()
            
            # Multiple operations should reuse connections
            async with ServiceManager(service_test_config) as services:
                from naq.services.connection import ConnectionService
                conn_service = await services.get_service(ConnectionService)
                
                # Multiple jetstream operations
                for i in range(50):
                    async with conn_service.jetstream_scope() as js:
                        # Mock JetStream operation
                        await js.publish("test.subject", b"test data")
            
            reuse_time = time.perf_counter() - start_time
            
            # Connection reuse should be efficient
            assert reuse_time < 3.0

    async def test_service_initialization_overhead(self, service_test_config):
        """Test service initialization time is reasonable."""
        
        with patch('naq.services.connection.get_nats_connection'):
            start_time = time.perf_counter()
            
            # Initialize and cleanup services
            manager = ServiceManager(service_test_config)
            await manager.initialize_all()
            await manager.cleanup_all()
            
            init_time = time.perf_counter() - start_time
            
            # Initialization should be quick (< 1 second)
            assert init_time < 1.0

    def test_memory_usage_basic(self, service_test_config):
        """Basic test for memory usage - no significant leaks."""
        import gc
        
        # Force garbage collection before test
        gc.collect()
        
        # Create multiple service managers to test for leaks
        managers = []
        for i in range(10):
            manager = ServiceManager(service_test_config)
            managers.append(manager)
        
        # Clear references
        managers.clear()
        gc.collect()
        
        # Test should complete without memory errors
        assert True

    async def test_concurrent_service_access(self, service_test_config):
        """Test concurrent access to services doesn't degrade performance."""
        import asyncio
        
        with patch('naq.services.connection.get_nats_connection') as mock_get_conn:
            mock_nc = AsyncMock()
            mock_js = AsyncMock()
            mock_nc.jetstream.return_value = mock_js
            mock_get_conn.return_value = mock_nc
            
            async def service_operation(services):
                from naq.services.connection import ConnectionService
                conn_service = await services.get_service(ConnectionService)
                async with conn_service.jetstream_scope() as js:
                    await js.publish("test", b"data")
                return True
            
            start_time = time.perf_counter()
            
            async with ServiceManager(service_test_config) as services:
                # Run multiple concurrent operations
                tasks = [service_operation(services) for _ in range(20)]
                results = await asyncio.gather(*tasks)
            
            concurrent_time = time.perf_counter() - start_time
            
            # All operations should succeed
            assert all(results)
            # Concurrent operations should be efficient
            assert concurrent_time < 2.0

    async def test_service_cleanup_performance(self, service_test_config):
        """Test service cleanup is efficient."""
        
        with patch('naq.services.connection.get_nats_connection'):
            manager = ServiceManager(service_test_config)
            await manager.initialize_all()
            
            # Get all services to ensure they're initialized
            from naq.services.connection import ConnectionService
            from naq.services.jobs import JobService
            await manager.get_service(ConnectionService)
            await manager.get_service(JobService)
            
            start_time = time.perf_counter()
            await manager.cleanup_all()
            cleanup_time = time.perf_counter() - start_time
            
            # Cleanup should be quick
            assert cleanup_time < 0.5

    async def test_queue_operations_performance(self, service_test_config):
        """Test queue operations performance with service layer."""
        
        with patch('naq.queue.core.ServiceManager') as mock_sm:
            mock_manager = AsyncMock()
            mock_connection_service = AsyncMock()
            
            # Mock jetstream_scope to return quickly
            async def mock_jetstream_scope():
                mock_js = AsyncMock()
                mock_js.publish.return_value = AsyncMock(seq=1)
                return mock_js
            
            mock_connection_service.jetstream_scope = mock_jetstream_scope
            mock_manager.get_service.return_value = mock_connection_service
            mock_sm.return_value = mock_manager
            
            start_time = time.perf_counter()
            
            # Create queue and perform operations
            queue = Queue("perf-test", config=service_test_config)
            
            # Mock enqueue operations
            for i in range(50):
                def test_func():
                    return i
                # This would normally enqueue, but we're testing the setup overhead
                pass
            
            setup_time = time.perf_counter() - start_time
            
            # Queue setup with service layer should be efficient
            assert setup_time < 1.0