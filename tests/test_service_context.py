"""
Tests for the service_context module.
"""

import asyncio
import pytest
import threading
from unittest.mock import AsyncMock, MagicMock, patch, ANY

from naq.service_context import (
    _prepare_service_config,
    _register_core_services,
    service_context,
    long_lived_service_context,
    run_with_service_context,
)
from naq.services.base import (
    ServiceManager,
    ServiceConfig,
    ServiceInitializationError,
    ServiceConfigurationError,
    ServiceRuntimeError,
)
from naq.services.config import create_global_config, GlobalServiceConfig
from naq.services.connection import ConnectionService
from naq.services.streams import StreamService
from naq.services.jobs import JobService
from naq.services.kv_stores import KVStoreService
from naq.services.events import EventService
from naq.exceptions import NaqException


class TestPrepareServiceConfig:
    """Test cases for _prepare_service_config function."""

    @pytest.mark.asyncio
    async def test_with_no_configs(self) -> None:
        """Test with no config provided."""
        config = await _prepare_service_config(
            config=None,
            global_config=None,
            nats_url=None,
            custom_settings=None,
        )
        
        assert isinstance(config, ServiceConfig)
        assert config.nats_url is not None
        assert config.custom_settings == {}

    @pytest.mark.asyncio
    async def test_with_nats_url(self) -> None:
        """Test with nats_url provided."""
        nats_url = "nats://localhost:4222"
        config = await _prepare_service_config(
            config=None,
            global_config=None,
            nats_url=nats_url,
            custom_settings=None,
        )
        
        assert config.nats_url == nats_url

    @pytest.mark.asyncio
    async def test_with_global_config(self) -> None:
        """Test with global_config provided."""
        global_config = create_global_config()
        global_config.nats_url = "nats://global:4222"
        
        config = await _prepare_service_config(
            config=None,
            global_config=global_config,
            nats_url=None,
            custom_settings=None,
        )
        
        assert config.nats_url == "nats://global:4222"

    @pytest.mark.asyncio
    async def test_with_custom_settings(self) -> None:
        """Test with custom_settings provided."""
        custom_settings = {"key1": "value1", "key2": "value2"}
        config = await _prepare_service_config(
            config=None,
            global_config=None,
            nats_url=None,
            custom_settings=custom_settings,
        )
        
        assert config.custom_settings == custom_settings

    @pytest.mark.asyncio
    async def test_with_existing_config_and_custom_settings(self) -> None:
        """Test with existing config and custom_settings."""
        existing_config = ServiceConfig(
            nats_url="nats://existing:4222",
            custom_settings={"existing": "value"}
        )
        custom_settings = {"new": "value"}
        
        config = await _prepare_service_config(
            config=existing_config,
            global_config=None,
            nats_url=None,
            custom_settings=custom_settings,
        )
        
        assert config.nats_url == "nats://existing:4222"
        assert config.custom_settings == {"existing": "value", "new": "value"}

    @pytest.mark.asyncio
    async def test_priority_nats_url_over_global_config(self) -> None:
        """Test that nats_url takes priority over global_config."""
        global_config = create_global_config()
        global_config.nats_url = "nats://global:4222"
        nats_url = "nats://explicit:4222"
        
        config = await _prepare_service_config(
            config=None,
            global_config=global_config,
            nats_url=nats_url,
            custom_settings=None,
        )
        
        assert config.nats_url == nats_url


class TestRegisterCoreServices:
    """Test cases for _register_core_services function."""

    @pytest.mark.asyncio
    async def test_register_all_services(self) -> None:
        """Test that all core services are registered correctly."""
        service_manager = ServiceManager(ServiceConfig())
        mock_log = MagicMock()
        
        # Mock the connection service to avoid actual NATS connection
        with patch.object(ConnectionService, 'initialize') as mock_init:
            mock_init.return_value = AsyncMock()
            
            with patch.object(StreamService, 'initialize') as mock_stream_init:
                mock_stream_init.return_value = AsyncMock()
                
                with patch.object(KVStoreService, 'initialize') as mock_kv_init:
                    mock_kv_init.return_value = AsyncMock()
                    
                    with patch.object(JobService, 'initialize') as mock_job_init:
                        mock_job_init.return_value = AsyncMock()
                        
                        with patch.object(EventService, 'initialize') as mock_event_init:
                            mock_event_init.return_value = AsyncMock()
                            
                            await _register_core_services(service_manager, mock_log)
                            
                            # Check that all services are registered
                            assert "connection" in service_manager.get_service_names()
                            assert "stream" in service_manager.get_service_names()
                            assert "kv" in service_manager.get_service_names()
                            assert "job" in service_manager.get_service_names()
                            assert "event" in service_manager.get_service_names()
                            
                            # Check that kv_store alias is registered
                            assert service_manager.has_service("kv_store")

    @pytest.mark.asyncio
    async def test_service_initialization_failure(self) -> None:
        """Test handling of service initialization failure."""
        service_manager = ServiceManager(ServiceConfig())
        mock_log = MagicMock()
        
        # Mock connection service to fail initialization
        with patch.object(ConnectionService, 'initialize') as mock_init:
            mock_init.side_effect = ServiceInitializationError("Connection failed")
            
            with pytest.raises(ServiceInitializationError):
                await _register_core_services(service_manager, mock_log)
            
            # Verify error was logged - the error is actually logged by the service manager
            # so we need to check that the service manager's logger was called
            # We can't easily mock that, so we'll just check that the function raises
            # the expected exception
            pass

    @pytest.mark.asyncio
    async def test_service_configuration_error(self) -> None:
        """Test handling of service configuration error."""
        service_manager = ServiceManager(ServiceConfig())
        mock_log = MagicMock()
        
        # Mock connection service to raise configuration error
        with patch.object(ServiceManager, 'register_service') as mock_register:
            mock_register.side_effect = ServiceConfigurationError("Invalid config")
            
            with pytest.raises(ServiceConfigurationError):
                await _register_core_services(service_manager, mock_log)
            
            # Verify error was logged - the error is actually logged by the service manager
            # so we need to check that the service manager's logger was called
            # We can't easily mock that, so we'll just check that the function raises
            # the expected exception
            pass


class TestServiceContext:
    """Test cases for service_context function."""

    @pytest.mark.asyncio
    async def test_basic_context(self) -> None:
        """Test basic service context functionality."""
        with patch.object(ConnectionService, 'initialize') as mock_init:
            mock_init.return_value = AsyncMock()
            
            with patch.object(StreamService, 'initialize') as mock_stream_init:
                mock_stream_init.return_value = AsyncMock()
                
                with patch.object(KVStoreService, 'initialize') as mock_kv_init:
                    mock_kv_init.return_value = AsyncMock()
                    
                    with patch.object(JobService, 'initialize') as mock_job_init:
                        mock_job_init.return_value = AsyncMock()
                        
                        with patch.object(EventService, 'initialize') as mock_event_init:
                            mock_event_init.return_value = AsyncMock()
                            
                            async with service_context() as service_manager:
                                assert isinstance(service_manager, ServiceManager)
                                assert "connection" in service_manager.get_service_names()
                                assert "stream" in service_manager.get_service_names()
                                assert "kv" in service_manager.get_service_names()
                                assert "job" in service_manager.get_service_names()
                                assert "event" in service_manager.get_service_names()

    @pytest.mark.asyncio
    async def test_context_with_nats_url(self) -> None:
        """Test service context with custom nats_url."""
        with patch.object(ConnectionService, 'initialize') as mock_init:
            mock_init.return_value = AsyncMock()
            
            with patch.object(StreamService, 'initialize') as mock_stream_init:
                mock_stream_init.return_value = AsyncMock()
                
                with patch.object(KVStoreService, 'initialize') as mock_kv_init:
                    mock_kv_init.return_value = AsyncMock()
                    
                    with patch.object(JobService, 'initialize') as mock_job_init:
                        mock_job_init.return_value = AsyncMock()
                        
                        with patch.object(EventService, 'initialize') as mock_event_init:
                            mock_event_init.return_value = AsyncMock()
                            
                            nats_url = "nats://custom:4222"
                            async with service_context(nats_url=nats_url) as service_manager:
                                assert service_manager._default_config.nats_url == nats_url

    @pytest.mark.asyncio
    async def test_context_with_custom_settings(self) -> None:
        """Test service context with custom settings."""
        with patch.object(ConnectionService, 'initialize') as mock_init:
            mock_init.return_value = AsyncMock()
            
            with patch.object(StreamService, 'initialize') as mock_stream_init:
                mock_stream_init.return_value = AsyncMock()
                
                with patch.object(KVStoreService, 'initialize') as mock_kv_init:
                    mock_kv_init.return_value = AsyncMock()
                    
                    with patch.object(JobService, 'initialize') as mock_job_init:
                        mock_job_init.return_value = AsyncMock()
                        
                        with patch.object(EventService, 'initialize') as mock_event_init:
                            mock_event_init.return_value = AsyncMock()
                            
                            custom_settings = {"custom": "value"}
                            async with service_context(custom_settings=custom_settings) as service_manager:
                                assert service_manager._default_config.custom_settings == custom_settings

    @pytest.mark.asyncio
    async def test_context_cleanup(self) -> None:
        """Test that services are cleaned up after context exit."""
        with patch.object(ConnectionService, 'initialize') as mock_init:
            mock_init.return_value = AsyncMock()
            
            with patch.object(StreamService, 'initialize') as mock_stream_init:
                mock_stream_init.return_value = AsyncMock()
                
                with patch.object(KVStoreService, 'initialize') as mock_kv_init:
                    mock_kv_init.return_value = AsyncMock()
                    
                    with patch.object(JobService, 'initialize') as mock_job_init:
                        mock_job_init.return_value = AsyncMock()
                        
                        with patch.object(EventService, 'initialize') as mock_event_init:
                            mock_event_init.return_value = AsyncMock()
                            
                            with patch.object(ServiceManager, 'cleanup_all') as mock_cleanup:
                                mock_cleanup.return_value = AsyncMock()
                                
                                async with service_context():
                                    pass
                                
                                # Verify cleanup was called
                                mock_cleanup.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_with_service_config(self) -> None:
        """Test service context with ServiceConfig provided."""
        with patch.object(ConnectionService, 'initialize') as mock_init:
            mock_init.return_value = AsyncMock()
            
            with patch.object(StreamService, 'initialize') as mock_stream_init:
                mock_stream_init.return_value = AsyncMock()
                
                with patch.object(KVStoreService, 'initialize') as mock_kv_init:
                    mock_kv_init.return_value = AsyncMock()
                    
                    with patch.object(JobService, 'initialize') as mock_job_init:
                        mock_job_init.return_value = AsyncMock()
                        
                        with patch.object(EventService, 'initialize') as mock_event_init:
                            mock_event_init.return_value = AsyncMock()
                            
                            config = ServiceConfig(nats_url="nats://config:4222")
                            async with service_context(config=config) as service_manager:
                                assert service_manager._default_config.nats_url == "nats://config:4222"

    @pytest.mark.asyncio
    async def test_kv_store_alias(self) -> None:
        """Test that kv_store alias works correctly."""
        with patch.object(ConnectionService, 'initialize') as mock_init:
            mock_init.return_value = AsyncMock()
            
            with patch.object(StreamService, 'initialize') as mock_stream_init:
                mock_stream_init.return_value = AsyncMock()
                
                with patch.object(KVStoreService, 'initialize') as mock_kv_init:
                    mock_kv_init.return_value = AsyncMock()
                    
                    with patch.object(JobService, 'initialize') as mock_job_init:
                        mock_job_init.return_value = AsyncMock()
                        
                        with patch.object(EventService, 'initialize') as mock_event_init:
                            mock_event_init.return_value = AsyncMock()
                            
                            async with service_context() as service_manager:
                                # Both 'kv' and 'kv_store' should point to the same service
                                kv_service = await service_manager.get_service("kv", KVStoreService)
                                kv_store_service = await service_manager.get_service("kv_store", KVStoreService)
                                assert kv_service is kv_store_service

    @pytest.mark.asyncio
    async def test_service_initialization_error_propagation(self) -> None:
        """Test that service initialization errors are properly propagated."""
        with patch.object(ConnectionService, 'initialize') as mock_init:
            mock_init.side_effect = ServiceInitializationError("Failed to initialize")
            
            with pytest.raises(ServiceInitializationError, match="Failed to initialize"):
                async with service_context():
                    pass

    @pytest.mark.asyncio
    async def test_unexpected_error_handling(self) -> None:
        """Test handling of unexpected errors."""
        with patch.object(ConnectionService, 'initialize') as mock_init:
            mock_init.side_effect = RuntimeError("Unexpected error")
            
            with pytest.raises(RuntimeError, match="Unexpected error"):
                async with service_context():
                    pass


class TestLongLivedServiceContext:
    """Test cases for long_lived_service_context function."""

    @pytest.mark.asyncio
    async def test_basic_context(self) -> None:
        """Test basic long-lived service context functionality."""
        service_manager = ServiceManager(ServiceConfig())
        
        # Mock all services to be already initialized
        with patch.object(service_manager, 'get_service') as mock_get:
            mock_get.return_value = AsyncMock()
            
            async with long_lived_service_context(service_manager) as manager:
                assert manager is service_manager
                # Verify get_service was called for all core services
                expected_calls = [
                    ("connection",),
                    ("stream",),
                    ("kv",),  # Use the actual service name, not the alias
                    ("job",),
                    ("event",),
                ]
                actual_calls = [call[0] for call in mock_get.call_args_list]
                for expected_call in expected_calls:
                    assert expected_call in actual_calls

    @pytest.mark.asyncio
    async def test_service_initialization_on_demand(self) -> None:
        """Test that services are initialized on demand."""
        
        # Create a mock service that's not initialized
        mock_service = AsyncMock()
        mock_service.is_initialized = False
        mock_service.initialize = AsyncMock()
        
        # Create a service manager and register our mock service
        service_manager = ServiceManager(ServiceConfig())
        service_manager._services["connection"] = mock_service
        service_manager._services["stream"] = mock_service
        service_manager._services["kv"] = mock_service
        service_manager._services["job"] = mock_service
        service_manager._services["event"] = mock_service
        service_manager._aliases["kv_store"] = "kv"
        
        async with long_lived_service_context(service_manager):
            # Verify initialize was called for each service
            assert mock_service.initialize.call_count == 5

    @pytest.mark.asyncio
    async def test_fail_fast_behavior(self) -> None:
        """Test fail-fast behavior for critical services."""
        service_manager = ServiceManager(ServiceConfig())
        
        # Mock get_service to raise an exception
        with patch.object(service_manager, 'get_service') as mock_get:
            mock_get.side_effect = ServiceInitializationError("Service failed")
            
            with pytest.raises(ServiceInitializationError, match="Service failed"):
                async with long_lived_service_context(service_manager):
                    pass

    @pytest.mark.asyncio
    async def test_no_cleanup_on_exit(self) -> None:
        """Test that services are not cleaned up on context exit."""
        service_manager = ServiceManager(ServiceConfig())
        
        with patch.object(service_manager, 'get_service') as mock_get:
            mock_get.return_value = AsyncMock()
            
            with patch.object(service_manager, 'cleanup_all') as mock_cleanup:
                async with long_lived_service_context(service_manager):
                    pass
                
                # Verify cleanup was not called
                mock_cleanup.assert_not_called()

    @pytest.mark.asyncio
    async def test_unexpected_error_handling(self) -> None:
        """Test handling of unexpected errors."""
        service_manager = ServiceManager(ServiceConfig())
        
        # Mock get_service to raise an unexpected error
        with patch.object(service_manager, 'get_service') as mock_get:
            mock_get.side_effect = RuntimeError("Unexpected error")
            
            with pytest.raises(RuntimeError, match="Unexpected error"):
                async with long_lived_service_context(service_manager):
                    pass


class TestRunWithServiceContext:
    """Test cases for run_with_service_context function."""

    def test_sync_function_execution(self) -> None:
        """Test execution of a synchronous function."""
        def test_function(service_manager, arg1, arg2, kwarg1=None):
            assert service_manager is not None
            assert arg1 == "value1"
            assert arg2 == "value2"
            assert kwarg1 == "kwvalue1"
            return "success"

        with patch.object(ConnectionService, 'initialize') as mock_init:
            mock_init.return_value = AsyncMock()
            
            with patch.object(StreamService, 'initialize') as mock_stream_init:
                mock_stream_init.return_value = AsyncMock()
                
                with patch.object(KVStoreService, 'initialize') as mock_kv_init:
                    mock_kv_init.return_value = AsyncMock()
                    
                    with patch.object(JobService, 'initialize') as mock_job_init:
                        mock_job_init.return_value = AsyncMock()
                        
                        with patch.object(EventService, 'initialize') as mock_event_init:
                            mock_event_init.return_value = AsyncMock()
                            
                            result = run_with_service_context(
                                test_function,
                                "value1",
                                "value2",
                                kwarg1="kwvalue1",
                            )
                            
                            assert result == "success"

    def test_with_existing_event_loop(self) -> None:
        """Test behavior when an event loop is already running."""
        def test_function(service_manager):
            return "thread_success"

        with patch.object(ConnectionService, 'initialize') as mock_init:
            mock_init.return_value = AsyncMock()
            
            with patch.object(StreamService, 'initialize') as mock_stream_init:
                mock_stream_init.return_value = AsyncMock()
                
                with patch.object(KVStoreService, 'initialize') as mock_kv_init:
                    mock_kv_init.return_value = AsyncMock()
                    
                    with patch.object(JobService, 'initialize') as mock_job_init:
                        mock_job_init.return_value = AsyncMock()
                        
                        with patch.object(EventService, 'initialize') as mock_event_init:
                            mock_event_init.return_value = AsyncMock()
                            
                            # Simulate running in a thread with existing event loop
                            result = None
                            exception = None

                            def run_in_thread():
                                nonlocal result, exception
                                try:
                                    # Create a new event loop for this thread
                                    loop = asyncio.new_event_loop()
                                    asyncio.set_event_loop(loop)
                                    
                                    result = run_with_service_context(test_function)
                                except Exception as e:
                                    exception = e
                                finally:
                                    loop.close()

                            thread = threading.Thread(target=run_in_thread)
                            thread.start()
                            thread.join(timeout=10)

                            if exception:
                                raise exception

                            assert result == "thread_success"

    def test_with_no_event_loop(self) -> None:
        """Test behavior when no event loop is running."""
        def test_function(service_manager):
            return "no_loop_success"

        with patch.object(ConnectionService, 'initialize') as mock_init:
            mock_init.return_value = AsyncMock()
            
            with patch.object(StreamService, 'initialize') as mock_stream_init:
                mock_stream_init.return_value = AsyncMock()
                
                with patch.object(KVStoreService, 'initialize') as mock_kv_init:
                    mock_kv_init.return_value = AsyncMock()
                    
                    with patch.object(JobService, 'initialize') as mock_job_init:
                        mock_job_init.return_value = AsyncMock()
                        
                        with patch.object(EventService, 'initialize') as mock_event_init:
                            mock_event_init.return_value = AsyncMock()
                            
                            # Ensure no event loop is running
                            asyncio.set_event_loop(None)
                            
                            result = run_with_service_context(test_function)
                            
                            assert result == "no_loop_success"

    def test_function_exception_propagation(self) -> None:
        """Test that function exceptions are properly propagated."""
        def test_function(service_manager):
            raise ValueError("Function error")

        with patch.object(ConnectionService, 'initialize') as mock_init:
            mock_init.return_value = AsyncMock()
            
            with patch.object(StreamService, 'initialize') as mock_stream_init:
                mock_stream_init.return_value = AsyncMock()
                
                with patch.object(KVStoreService, 'initialize') as mock_kv_init:
                    mock_kv_init.return_value = AsyncMock()
                    
                    with patch.object(JobService, 'initialize') as mock_job_init:
                        mock_job_init.return_value = AsyncMock()
                        
                        with patch.object(EventService, 'initialize') as mock_event_init:
                            mock_event_init.return_value = AsyncMock()
                            
                            with pytest.raises(ValueError, match="Function error"):
                                run_with_service_context(test_function)

    def test_with_custom_nats_url(self) -> None:
        """Test with custom nats_url parameter."""
        def test_function(service_manager):
            return service_manager._default_config.nats_url

        with patch.object(ConnectionService, 'initialize') as mock_init:
            mock_init.return_value = AsyncMock()
            
            with patch.object(StreamService, 'initialize') as mock_stream_init:
                mock_stream_init.return_value = AsyncMock()
                
                with patch.object(KVStoreService, 'initialize') as mock_kv_init:
                    mock_kv_init.return_value = AsyncMock()
                    
                    with patch.object(JobService, 'initialize') as mock_job_init:
                        mock_job_init.return_value = AsyncMock()
                        
                        with patch.object(EventService, 'initialize') as mock_event_init:
                            mock_event_init.return_value = AsyncMock()
                            
                            nats_url = "nats://custom:4222"
                            result = run_with_service_context(
                                test_function,
                                nats_url=nats_url,
                            )
                            
                            assert result == nats_url

    def test_with_custom_settings(self) -> None:
        """Test with custom_settings parameter."""
        def test_function(service_manager):
            return service_manager._default_config.custom_settings

        with patch.object(ConnectionService, 'initialize') as mock_init:
            mock_init.return_value = AsyncMock()
            
            with patch.object(StreamService, 'initialize') as mock_stream_init:
                mock_stream_init.return_value = AsyncMock()
                
                with patch.object(KVStoreService, 'initialize') as mock_kv_init:
                    mock_kv_init.return_value = AsyncMock()
                    
                    with patch.object(JobService, 'initialize') as mock_job_init:
                        mock_job_init.return_value = AsyncMock()
                        
                        with patch.object(EventService, 'initialize') as mock_event_init:
                            mock_event_init.return_value = AsyncMock()
                            
                            custom_settings = {"custom": "value"}
                            result = run_with_service_context(
                                test_function,
                                custom_settings=custom_settings,
                            )
                            
                            assert result == custom_settings
