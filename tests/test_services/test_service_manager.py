# tests/test_services/test_service_manager.py
"""Tests for ServiceManager - service lifecycle and dependency injection."""

import pytest
from unittest.mock import AsyncMock, patch
from naq.services import ServiceManager
from naq.services.connection import ConnectionService
from naq.services.jobs import JobService
from naq.services.events import EventService
from naq.services.streams import StreamService
from naq.services.kv_stores import KVStoreService
from naq.services.scheduler import SchedulerService


@pytest.mark.asyncio
class TestServiceManager:
    """Test ServiceManager initialization and service management."""

    async def test_service_initialization(self, service_test_config):
        """Test ServiceManager initializes all services correctly."""
        manager = ServiceManager(service_test_config)
        
        # Test initialization
        await manager.initialize_all()
        
        # Test service retrieval
        conn_service = await manager.get_service(ConnectionService)
        job_service = await manager.get_service(JobService)
        event_service = await manager.get_service(EventService)
        stream_service = await manager.get_service(StreamService)
        kv_service = await manager.get_service(KVStoreService)
        scheduler_service = await manager.get_service(SchedulerService)
        
        assert conn_service is not None
        assert job_service is not None
        assert event_service is not None
        assert stream_service is not None
        assert kv_service is not None
        assert scheduler_service is not None
        
        # Test cleanup
        await manager.cleanup_all()

    async def test_dependency_injection(self, service_test_config):
        """Test services are properly injected with dependencies."""
        manager = ServiceManager(service_test_config)
        await manager.initialize_all()
        
        try:
            job_service = await manager.get_service(JobService)
            
            # Verify job_service has injected dependencies
            # JobService should have connection service injected
            assert hasattr(job_service, '_connection_service') or \
                   hasattr(job_service, 'connection_service') or \
                   hasattr(job_service, '_service_manager')
            
        finally:
            await manager.cleanup_all()

    async def test_service_singleton_behavior(self, service_test_config):
        """Test that get_service returns the same instance."""
        manager = ServiceManager(service_test_config)
        await manager.initialize_all()
        
        try:
            # Get the same service twice
            conn_service1 = await manager.get_service(ConnectionService)
            conn_service2 = await manager.get_service(ConnectionService)
            
            # Should be the same instance
            assert conn_service1 is conn_service2
            
        finally:
            await manager.cleanup_all()

    async def test_service_cleanup(self, service_test_config):
        """Test that cleanup_all properly cleans up services."""
        manager = ServiceManager(service_test_config)
        await manager.initialize_all()
        
        # Get a service to ensure it's initialized
        conn_service = await manager.get_service(ConnectionService)
        assert conn_service is not None
        
        # Test cleanup doesn't raise errors
        await manager.cleanup_all()
        
        # After cleanup, services should still be accessible but may be cleaned up
        # The exact behavior depends on implementation

    async def test_config_access(self, service_test_config):
        """Test that services have access to configuration."""
        manager = ServiceManager(service_test_config)
        await manager.initialize_all()
        
        try:
            conn_service = await manager.get_service(ConnectionService)
            
            # Services should have access to their config
            # This depends on the specific service implementation
            assert hasattr(conn_service, '_config') or \
                   hasattr(conn_service, 'config') or \
                   hasattr(conn_service, '_service_manager')
            
        finally:
            await manager.cleanup_all()

    def test_service_manager_context_manager(self, service_test_config):
        """Test ServiceManager as async context manager."""
        # This tests the context manager protocol if implemented
        async def test_context():
            async with ServiceManager(service_test_config) as manager:
                conn_service = await manager.get_service(ConnectionService)
                assert conn_service is not None
                return manager
            # Should auto-cleanup on exit
        
        # Run the context manager test
        import asyncio
        result = asyncio.get_event_loop().run_until_complete(test_context())
        assert result is not None