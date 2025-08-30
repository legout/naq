"""Unit tests for long-lived components using service context."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import asyncio

from naq.queue.core import Queue
from naq.worker.core import Worker
from naq.scheduler import Scheduler
from naq.services.config import create_global_config, GlobalServiceConfig
from naq.services.base import ServiceManager
from naq.service_context import long_lived_service_context


class TestQueueServiceContext:
    """Test cases for Queue class using service context."""

    @pytest.mark.asyncio
    async def test_queue_uses_long_lived_context(self):
        """Test that Queue properly uses long-lived service context."""
        nats_url = "nats://localhost:4222"
        queue_name = "test_queue"
        
        with patch('naq.queue.core.long_lived_service_context') as mock_context:
            mock_service_manager = MagicMock(spec=ServiceManager)
            mock_context.return_value.__aenter__.return_value = mock_service_manager
            mock_context.return_value.__aexit__.return_value = None
            
            async with Queue(nats_url=nats_url, queue_name=queue_name) as queue:
                # Verify long_lived_service_context was called
                mock_context.assert_called_once()
                
                # Verify the context was entered
                mock_context.return_value.__aenter__.assert_called_once()
                
                # Verify service manager is stored
                assert queue._service_manager == mock_service_manager

    @pytest.mark.asyncio
    async def test_queue_with_custom_config(self):
        """Test that Queue uses custom configuration with service context."""
        nats_url = "nats://localhost:4222"
        queue_name = "test_queue"
        custom_config = GlobalServiceConfig(
            nats_url=nats_url,
            connection_timeout=30,
            request_timeout=10,
        )
        
        with patch('naq.queue.core.long_lived_service_context') as mock_context:
            mock_service_manager = MagicMock(spec=ServiceManager)
            mock_context.return_value.__aenter__.return_value = mock_service_manager
            mock_context.return_value.__aexit__.return_value = None
            
            async with Queue(nats_url=nats_url, queue_name=queue_name, config=custom_config) as queue:
                # Verify long_lived_service_context was called with custom config
                mock_context.assert_called_once_with(
                    nats_url=nats_url,
                    global_config=custom_config,
                    logger_name="naq.queue.core"
                )

    @pytest.mark.asyncio
    async def test_queue_services_available(self):
        """Test that Queue makes services available through service manager."""
        nats_url = "nats://localhost:4222"
        queue_name = "test_queue"
        
        with patch('naq.queue.core.long_lived_service_context') as mock_context:
            mock_service_manager = MagicMock(spec=ServiceManager)
            mock_service_manager.has_service.return_value = True
            mock_service_manager.get_service.return_value = MagicMock()
            mock_context.return_value.__aenter__.return_value = mock_service_manager
            mock_context.return_value.__aexit__.return_value = None
            
            async with Queue(nats_url=nats_url, queue_name=queue_name) as queue:
                # Verify services are available
                mock_service_manager.has_service.assert_any_call("connection")
                mock_service_manager.has_service.assert_any_call("jobs")
                mock_service_manager.has_service.assert_any_call("kv")

    @pytest.mark.asyncio
    async def test_queue_context_cleanup(self):
        """Test that Queue properly handles context cleanup."""
        nats_url = "nats://localhost:4222"
        queue_name = "test_queue"
        
        with patch('naq.queue.core.long_lived_service_context') as mock_context:
            mock_service_manager = MagicMock(spec=ServiceManager)
            mock_context.return_value.__aenter__.return_value = mock_service_manager
            mock_context.return_value.__aexit__.return_value = None
            
            async with Queue(nats_url=nats_url, queue_name=queue_name) as queue:
                pass
            
            # Verify the context was exited
            mock_context.return_value.__aexit__.assert_called_once()


class TestWorkerServiceContext:
    """Test cases for Worker class using service context."""

    @pytest.mark.asyncio
    async def test_worker_uses_long_lived_context(self):
        """Test that Worker properly uses long-lived service context."""
        nats_url = "nats://localhost:4222"
        queue_names = ["test_queue"]
        
        with patch('naq.worker.core.long_lived_service_context') as mock_context:
            mock_service_manager = MagicMock(spec=ServiceManager)
            mock_context.return_value.__aenter__.return_value = mock_service_manager
            mock_context.return_value.__aexit__.return_value = None
            
            async with Worker(nats_url=nats_url, queue_names=queue_names) as worker:
                # Verify long_lived_service_context was called
                mock_context.assert_called_once()
                
                # Verify the context was entered
                mock_context.return_value.__aenter__.assert_called_once()
                
                # Verify service manager is stored
                assert worker._service_manager == mock_service_manager

    @pytest.mark.asyncio
    async def test_worker_with_custom_config(self):
        """Test that Worker uses custom configuration with service context."""
        nats_url = "nats://localhost:4222"
        queue_names = ["test_queue"]
        custom_config = GlobalServiceConfig(
            nats_url=nats_url,
            connection_timeout=30,
            request_timeout=10,
        )
        
        with patch('naq.worker.core.long_lived_service_context') as mock_context:
            mock_service_manager = MagicMock(spec=ServiceManager)
            mock_context.return_value.__aenter__.return_value = mock_service_manager
            mock_context.return_value.__aexit__.return_value = None
            
            async with Worker(nats_url=nats_url, queue_names=queue_names, config=custom_config) as worker:
                # Verify long_lived_service_context was called with custom config
                mock_context.assert_called_once_with(
                    nats_url=nats_url,
                    global_config=custom_config,
                    logger_name="naq.worker.core"
                )

    @pytest.mark.asyncio
    async def test_worker_services_available(self):
        """Test that Worker makes services available through service manager."""
        nats_url = "nats://localhost:4222"
        queue_names = ["test_queue"]
        
        with patch('naq.worker.core.long_lived_service_context') as mock_context:
            mock_service_manager = MagicMock(spec=ServiceManager)
            mock_service_manager.has_service.return_value = True
            mock_service_manager.get_service.return_value = MagicMock()
            mock_context.return_value.__aenter__.return_value = mock_service_manager
            mock_context.return_value.__aexit__.return_value = None
            
            async with Worker(nats_url=nats_url, queue_names=queue_names) as worker:
                # Verify services are available
                mock_service_manager.has_service.assert_any_call("connection")
                mock_service_manager.has_service.assert_any_call("jobs")
                mock_service_manager.has_service.assert_any_call("kv")

    @pytest.mark.asyncio
    async def test_worker_connect_uses_service_manager(self):
        """Test that Worker._connect uses service manager."""
        nats_url = "nats://localhost:4222"
        queue_names = ["test_queue"]
        
        with patch('naq.worker.core.long_lived_service_context') as mock_context:
            mock_service_manager = MagicMock(spec=ServiceManager)
            mock_connection_service = MagicMock()
            mock_job_service = MagicMock()
            mock_kv_service = MagicMock()
            
            mock_service_manager.get_service.side_effect = lambda name: {
                "connection": mock_connection_service,
                "jobs": mock_job_service,
                "kv": mock_kv_service,
            }[name]
            
            mock_context.return_value.__aenter__.return_value = mock_service_manager
            mock_context.return_value.__aexit__.return_value = None
            
            async with Worker(nats_url=nats_url, queue_names=queue_names) as worker:
                # Call _connect method
                await worker._connect()
                
                # Verify services were retrieved
                # The actual calls include service class parameters, so we check just the name
                for call in mock_service_manager.get_service.call_args_list:
                    if call[0][0] == "connection":
                        break
                else:
                    assert False, "get_service('connection') call not found"
                
                for call in mock_service_manager.get_service.call_args_list:
                    if call[0][0] == "jobs":
                        break
                else:
                    assert False, "get_service('jobs') call not found"
                
                for call in mock_service_manager.get_service.call_args_list:
                    if call[0][0] == "kv_store":
                        break
                else:
                    assert False, "get_service('kv_store') call not found"


class TestSchedulerServiceContext:
    """Test cases for Scheduler class using service context."""

    @pytest.mark.asyncio
    async def test_scheduler_uses_long_lived_context(self):
        """Test that Scheduler properly uses long-lived service context."""
        nats_url = "nats://localhost:4222"
        
        with patch('naq.scheduler.long_lived_service_context') as mock_context:
            mock_service_manager = MagicMock(spec=ServiceManager)
            mock_context.return_value.__aenter__.return_value = mock_service_manager
            mock_context.return_value.__aexit__.return_value = None
            
            async with Scheduler(nats_url=nats_url) as scheduler:
                # Verify long_lived_service_context was called
                mock_context.assert_called_once()
                
                # Verify the context was entered
                mock_context.return_value.__aenter__.assert_called_once()
                
                # Verify service manager is stored
                assert scheduler._service_manager == mock_service_manager

    @pytest.mark.asyncio
    async def test_scheduler_with_custom_config(self):
        """Test that Scheduler uses custom configuration with service context."""
        nats_url = "nats://localhost:4222"
        custom_config = GlobalServiceConfig(
            nats_url=nats_url,
            connection_timeout=30,
            request_timeout=10,
        )
        
        with patch('naq.scheduler.long_lived_service_context') as mock_context:
            mock_service_manager = MagicMock(spec=ServiceManager)
            mock_context.return_value.__aenter__.return_value = mock_service_manager
            mock_context.return_value.__aexit__.return_value = None
            
            async with Scheduler(nats_url=nats_url, config=custom_config) as scheduler:
                # Verify long_lived_service_context was called with custom config
                # The logger name includes a dynamic instance ID, so we check the prefix
                mock_context.assert_called_once()
                call_args = mock_context.call_args
                assert call_args[1]['nats_url'] == nats_url
                assert call_args[1]['global_config'] == custom_config
                assert call_args[1]['logger_name'].startswith("naq.scheduler.")

    @pytest.mark.asyncio
    async def test_scheduler_services_available(self):
        """Test that Scheduler makes services available through service manager."""
        nats_url = "nats://localhost:4222"
        
        with patch('naq.scheduler.long_lived_service_context') as mock_context:
            mock_service_manager = MagicMock(spec=ServiceManager)
            mock_service_manager.has_service.return_value = True
            mock_service_manager.get_service.return_value = MagicMock()
            mock_context.return_value.__aenter__.return_value = mock_service_manager
            mock_context.return_value.__aexit__.return_value = None
            
            async with Scheduler(nats_url=nats_url) as scheduler:
                # Verify services are available
                # The actual calls include service class parameters, so we check just the name
                for call in mock_service_manager.has_service.call_args_list:
                    if call[0][0] == "connection":
                        break
                else:
                    assert False, "has_service('connection') call not found"
                
                for call in mock_service_manager.has_service.call_args_list:
                    if call[0][0] == "jobs":
                        break
                else:
                    assert False, "has_service('jobs') call not found"
                
                for call in mock_service_manager.has_service.call_args_list:
                    if call[0][0] == "kv_store":
                        break
                else:
                    assert False, "has_service('kv_store') call not found"

    @pytest.mark.asyncio
    async def test_scheduler_connect_uses_service_manager(self):
        """Test that Scheduler._connect uses service manager."""
        nats_url = "nats://localhost:4222"
        
        with patch('naq.scheduler.long_lived_service_context') as mock_context:
            mock_service_manager = MagicMock(spec=ServiceManager)
            mock_connection_service = MagicMock()
            mock_job_service = MagicMock()
            mock_kv_service = MagicMock()
            
            mock_service_manager.get_service.side_effect = lambda name, service_class: {
                "connection": mock_connection_service,
                "jobs": mock_job_service,
                "kv_store": mock_kv_service,
                "event": MagicMock(),
                "scheduler": MagicMock(),
            }[name]
            
            mock_context.return_value.__aenter__.return_value = mock_service_manager
            mock_context.return_value.__aexit__.return_value = None
            
            async with Scheduler(nats_url=nats_url) as scheduler:
                # Call _connect method
                await scheduler._connect()
                
                # Verify services were retrieved
                mock_service_manager.get_service.assert_any_call("connection")
                mock_service_manager.get_service.assert_any_call("jobs")
                mock_service_manager.get_service.assert_any_call("kv")


class TestLongLivedComponentsIntegration:
    """Integration tests for long-lived components."""

    @pytest.mark.asyncio
    async def test_multiple_components_share_service_context(self):
        """Test that multiple components can share the same service context."""
        nats_url = "nats://localhost:4222"
        
        with patch('naq.queue.core.long_lived_service_context') as mock_queue_context, \
             patch('naq.worker.core.long_lived_service_context') as mock_worker_context, \
             patch('naq.scheduler.long_lived_service_context') as mock_scheduler_context:
            
            mock_service_manager = MagicMock(spec=ServiceManager)
            
            # Configure all mocks to return the same service manager
            for mock_ctx in [mock_queue_context, mock_worker_context, mock_scheduler_context]:
                mock_ctx.return_value.__aenter__.return_value = mock_service_manager
                mock_ctx.return_value.__aexit__.return_value = None
            
            # Create components
            async with Queue(nats_url=nats_url, queue_name="test_queue") as queue, \
                      Worker(nats_url=nats_url, queue_names=["test_queue"]) as worker, \
                      Scheduler(nats_url=nats_url) as scheduler:
                
                # Verify all components use the same service manager
                assert queue._service_manager == mock_service_manager
                assert worker._service_manager == mock_service_manager
                assert scheduler._service_manager == mock_service_manager

    @pytest.mark.asyncio
    async def test_components_with_different_configs(self):
        """Test that components can use different configurations."""
        nats_url = "nats://localhost:4222"
        config1 = GlobalServiceConfig(nats_url=nats_url, connection_timeout=30)
        config2 = GlobalServiceConfig(nats_url=nats_url, connection_timeout=60)
        
        with patch('naq.queue.core.long_lived_service_context') as mock_queue_context, \
             patch('naq.worker.core.long_lived_service_context') as mock_worker_context:
            
            mock_service_manager1 = MagicMock(spec=ServiceManager)
            mock_service_manager2 = MagicMock(spec=ServiceManager)
            
            mock_queue_context.return_value.__aenter__.return_value = mock_service_manager1
            mock_queue_context.return_value.__aexit__.return_value = None
            
            mock_worker_context.return_value.__aenter__.return_value = mock_service_manager2
            mock_worker_context.return_value.__aexit__.return_value = None
            
            # Create components with different configs
            async with Queue(nats_url=nats_url, queue_name="test_queue", config=config1) as queue, \
                      Worker(nats_url=nats_url, queue_names=["test_queue"], config=config2) as worker:
                
                # Verify components use different service managers
                assert queue._service_manager == mock_service_manager1
                assert worker._service_manager == mock_service_manager2
                assert queue._service_manager != worker._service_manager

    @pytest.mark.asyncio
    async def test_component_lifecycle_management(self):
        """Test that components properly manage service lifecycle."""
        nats_url = "nats://localhost:4222"
        
        with patch('naq.queue.core.long_lived_service_context') as mock_context:
            mock_service_manager = MagicMock(spec=ServiceManager)
            mock_context.return_value.__aenter__.return_value = mock_service_manager
            mock_context.return_value.__aexit__.return_value = None
            
            # Test component lifecycle
            queue = None
            try:
                queue = Queue(nats_url=nats_url, queue_name="test_queue")
                async with queue:
                    # Component is active
                    assert queue._service_manager == mock_service_manager
                # Component is closed
                mock_context.return_value.__aexit__.assert_called_once()
            finally:
                if queue:
                    await queue.close()