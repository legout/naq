"""
KV Store Service Tests

This module contains tests for the KVStoreService, focusing on
service lifecycle management and basic functionality.
"""
from pathlib import Path
import sys
sys.path.append(Path(__file__).parent.as_posix())
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Any, Dict, Optional

from naq.services.base import ServiceConfig, ServiceInitializationError, ServiceRuntimeError
from naq.services.kv_stores import KVStoreService, KVStoreServiceConfig
from naq.services.connection import ConnectionService
from naq.exceptions import NaqException

from service_test_utils import (
    ServiceTestContext,
    create_mock_service,
    assert_service_initialized,
    assert_service_cleaned_up,
    assert_service_dependency
)
from service_lifecycle_utils import (
    ServiceLifecycleTracker,
    ServiceLifecycleTestHarness,
    MonitoredService,
    test_service_initialization_failure,
    test_service_cleanup_failure
)


@pytest_asyncio.fixture
async def kv_store_service_config() -> ServiceConfig:
    """
    Fixture providing a configuration for KVStoreService tests.
    
    Returns:
        A ServiceConfig instance with KV store-specific settings.
    """
    return ServiceConfig(
        nats_url="nats://localhost:4222",
        log_level="DEBUG",
        custom_settings={
            "bucket_name": "test_kv_store",
            "ttl": 3600,
            "history": 10,
            "replicas": 1,
            "auto_create_buckets": True
        }
    )


@pytest_asyncio.fixture
async def mock_connection_service() -> AsyncMock:
    """
    Fixture providing a mock ConnectionService.
    
    Returns:
        An AsyncMock instance configured as a ConnectionService.
    """
    return create_mock_service(ConnectionService)


@pytest_asyncio.fixture
async def kv_store_service_with_dependencies(
    kv_store_service_config: ServiceConfig,
    mock_connection_service: AsyncMock
) -> KVStoreService:
    """
    Fixture providing a KVStoreService instance with mocked dependencies.
    
    Args:
        kv_store_service_config: Configuration for the service.
        mock_connection_service: Mock ConnectionService dependency.
        
    Returns:
        A KVStoreService instance.
    """
    service = KVStoreService(
        config=kv_store_service_config,
        connection_service=mock_connection_service
    )
    try:
        await service.initialize()
        yield service
    finally:
        if service.is_initialized:
            await service.cleanup()


@pytest_asyncio.fixture
async def mock_kv_store_service() -> AsyncMock:
    """
    Fixture providing a mock KVStoreService.
    
    Returns:
        An AsyncMock instance configured as a KVStoreService.
    """
    return create_mock_service(KVStoreService)


@pytest_asyncio.fixture
async def test_data() -> Dict[str, Any]:
    """
    Fixture providing test data for KV store operations.
    
    Returns:
        A dictionary with test data.
    """
    return {
        "key": "test-key",
        "value": {"message": "Hello, world!", "timestamp": 1234567890},
        "updated_value": {"message": "Updated message!", "timestamp": 1234567891}
    }


class TestKVStoreServiceLifecycle:
    """Test cases for KVStoreService lifecycle management."""
    
    async def test_service_initialization_with_dependencies(
        self,
        kv_store_service_with_dependencies: KVStoreService
    ) -> None:
        """Test KVStoreService initialization with dependencies."""
        service = kv_store_service_with_dependencies
        assert_service_initialized(service)
        assert service.is_initialized is True
        assert service.kv_config is not None
        assert service.kv_config.bucket_name == "test_kv_store"
        assert service.kv_config.auto_create_buckets is True
    
    async def test_service_cleanup(
        self,
        kv_store_service_with_dependencies: KVStoreService
    ) -> None:
        """Test KVStoreService cleanup."""
        service = kv_store_service_with_dependencies
        assert service.is_initialized is True
        
        await service.cleanup()
        assert_service_cleaned_up(service)
        assert service.is_initialized is False
    
    async def test_service_context_manager(
        self,
        kv_store_service_config: ServiceConfig,
        mock_connection_service: AsyncMock
    ) -> None:
        """Test KVStoreService as a context manager."""
        async with KVStoreService(
            config=kv_store_service_config,
            connection_service=mock_connection_service
        ) as service:
            assert_service_initialized(service)
            assert service.is_initialized is True
        
        assert service.is_initialized is False
    
    async def test_service_initialization_with_invalid_config(self) -> None:
        """Test KVStoreService initialization with invalid configuration."""
        invalid_config = ServiceConfig(
            nats_url="nats://localhost:4222",
            log_level="DEBUG",
            custom_settings={
                "bucket_name": "",  # Empty bucket name should cause initialization to fail
                "auto_create_buckets": True
            }
        )
        
        with pytest.raises(ServiceInitializationError):
            async with ServiceTestContext(
                KVStoreService,
                invalid_config,
                connection_service=create_mock_service(ConnectionService)
            ):
                pass
    
    async def test_service_initialization_failure_tracking(
        self,
        failing_service_config: ServiceConfig,
        service_lifecycle_tracker: ServiceLifecycleTracker
    ) -> None:
        """Test that KVStoreService initialization failures are tracked correctly."""
        await test_service_initialization_failure(
            KVStoreService,
            failing_service_config,
            service_lifecycle_tracker
        )
    
    async def test_service_cleanup_failure_tracking(
        self,
        kv_store_service_config: ServiceConfig,
        service_lifecycle_tracker: ServiceLifecycleTracker
    ) -> None:
        """Test that KVStoreService cleanup failures are tracked correctly."""
        await test_service_cleanup_failure(
            KVStoreService,
            kv_store_service_config,
            service_lifecycle_tracker
        )
    
    async def test_service_lifecycle_with_harness(
        self,
        kv_store_service_config: ServiceConfig,
        mock_connection_service: AsyncMock,
        service_lifecycle_harness: ServiceLifecycleTestHarness
    ) -> None:
        """Test KVStoreService lifecycle using the test harness."""
        # Create and initialize service
        service = await service_lifecycle_harness.create_service(
            KVStoreService,
            kv_store_service_config,
            connection_service=mock_connection_service
        )
        await service.initialize()
        
        # Verify initialization
        service_lifecycle_harness.assert_all_services_initialized()
        service_lifecycle_harness.assert_service_lifecycle(KVStoreService)
        
        # Cleanup
        await service_lifecycle_harness.cleanup_all_services()
        service_lifecycle_harness.assert_all_services_cleaned_up()


class TestKVStoreServiceConfiguration:
    """Test cases for KVStoreService configuration handling."""
    
    async def test_default_configuration(self) -> None:
        """Test KVStoreService with default configuration."""
        service = KVStoreService()
        
        # Check that default configuration is applied
        assert service.kv_config is not None
        assert service.kv_config.bucket_name == "naq_kv_store"
        assert service.kv_config.ttl == 86400
        assert service.kv_config.history == 10
        assert service.kv_config.replicas == 1
        assert service.kv_config.auto_create_buckets is True
    
    async def test_custom_configuration(self, kv_store_service_config: ServiceConfig) -> None:
        """Test KVStoreService with custom configuration."""
        service = KVStoreService(config=kv_store_service_config)
        
        # Check that custom configuration is applied
        assert service.kv_config is not None
        assert service.kv_config.bucket_name == "test_kv_store"
        assert service.kv_config.ttl == 3600
        assert service.kv_config.history == 10
        assert service.kv_config.replicas == 1
        assert service.kv_config.auto_create_buckets is True
    
    async def test_configuration_property(self, kv_store_service_config: ServiceConfig) -> None:
        """Test KVStoreService configuration property."""
        service = KVStoreService(config=kv_store_service_config)
        
        # Check that the config property returns the correct configuration
        assert service.config == kv_store_service_config
        
        # Update the configuration
        new_config = ServiceConfig(nats_url="nats://localhost:4223")
        service.config = new_config
        assert service.config == new_config


class TestKVStoreServiceDependencies:
    """Test cases for KVStoreService dependencies."""
    
    async def test_connection_service_dependency(
        self,
        kv_store_service_with_dependencies: KVStoreService,
        mock_connection_service: AsyncMock
    ) -> None:
        """Test that KVStoreService has a connection service dependency."""
        service = kv_store_service_with_dependencies
        assert_service_dependency(service, '_connection_service', AsyncMock)
        assert service._connection_service == mock_connection_service


class TestKVStoreServiceProperties:
    """Test cases for KVStoreService properties."""
    
    async def test_kv_config_property(self, kv_store_service_config: ServiceConfig) -> None:
        """Test KVStoreService kv_config property."""
        service = KVStoreService(config=kv_store_service_config)
        
        # Check that the kv_config property returns the correct configuration
        assert service.kv_config is not None
        assert isinstance(service.kv_config, KVStoreServiceConfig)
        assert service.kv_config.bucket_name == "test_kv_store"


class TestKVStoreServiceMocking:
    """Test cases for mocking KVStoreService."""
    
    async def test_mock_service_creation(self, mock_kv_store_service: AsyncMock) -> None:
        """Test that mock KVStoreService is created correctly."""
        assert mock_kv_store_service is not None
        assert mock_kv_store_service._is_initialized is True
        assert mock_kv_store_service.initialize is not None
        assert mock_kv_store_service.cleanup is not None
        assert mock_kv_store_service.put is not None
        assert mock_kv_store_service.get is not None
        assert mock_kv_store_service.delete is not None
        assert mock_kv_store_service.get_kv_store is not None
    
    async def test_mock_service_methods(
        self,
        mock_kv_store_service: AsyncMock,
        test_data: Dict[str, Any]
    ) -> None:
        """Test that mock KVStoreService methods work correctly."""
        # Test that mock methods can be called
        await mock_kv_store_service.initialize()
        await mock_kv_store_service.cleanup()
        await mock_kv_store_service.put("test-bucket", test_data["key"], test_data["value"])
        await mock_kv_store_service.get("test-bucket", test_data["key"])
        await mock_kv_store_service.delete("test-bucket", test_data["key"])
        await mock_kv_store_service.get_kv_store("test-bucket")
        
        # Verify that the methods were called
        mock_kv_store_service.initialize.assert_called_once()
        mock_kv_store_service.cleanup.assert_called_once()
        mock_kv_store_service.put.assert_called_once()
        mock_kv_store_service.get.assert_called_once()
        mock_kv_store_service.delete.assert_called_once()
        mock_kv_store_service.get_kv_store.assert_called_once()
    
    async def test_mock_service_with_dependencies(
        self,
        mock_kv_store_service: AsyncMock,
        mock_connection_service: AsyncMock
    ) -> None:
        """Test that mock KVStoreService can be used with dependencies."""
        # Create a service that depends on the mock KV store service
        from naq.services.jobs import JobService
        
        job_service_config = ServiceConfig(
            nats_url="nats://localhost:4222",
            log_level="DEBUG",
            custom_settings={
                "enable_job_execution": True,
                "enable_result_storage": True,
                "enable_event_logging": True
            }
        )
        
        job_service = JobService(
            config=job_service_config,
            connection_service=mock_connection_service,
            kv_store_service=mock_kv_store_service
        )
        
        # Verify that the job service has the mock KV store service as a dependency
        assert_service_dependency(job_service, '_kv_store_service', AsyncMock)
        assert job_service._kv_store_service == mock_kv_store_service


class TestKVStoreServiceBasicOperations:
    """Test cases for basic KVStoreService operations."""
    
    async def test_put_and_get_operations(
        self,
        kv_store_service_with_dependencies: KVStoreService,
        test_data: Dict[str, Any]
    ) -> None:
        """Test basic put and get operations."""
        service = kv_store_service_with_dependencies
        
        # Put a value
        await service.put(
            service.kv_config.bucket_name,
            test_data["key"],
            test_data["value"],
            ttl=service.kv_config.ttl,
            serialize=True
        )
        
        # Get the value
        result = await service.get(
            service.kv_config.bucket_name,
            test_data["key"],
            deserialize=True
        )
        
        # Verify the result
        assert result == test_data["value"]
    
    async def test_delete_operation(
        self,
        kv_store_service_with_dependencies: KVStoreService,
        test_data: Dict[str, Any]
    ) -> None:
        """Test delete operation."""
        service = kv_store_service_with_dependencies
        
        # Put a value
        await service.put(
            service.kv_config.bucket_name,
            test_data["key"],
            test_data["value"],
            ttl=service.kv_config.ttl,
            serialize=True
        )
        
        # Delete the value
        deleted = await service.delete(
            service.kv_config.bucket_name,
            test_data["key"]
        )
        
        # Verify the deletion
        assert deleted is True
        
        # Try to get the deleted value
        result = await service.get(
            service.kv_config.bucket_name,
            test_data["key"],
            deserialize=True
        )
        
        # Verify that the value is gone
        assert result is None
    
    async def test_get_kv_store_operation(
        self,
        kv_store_service_with_dependencies: KVStoreService
    ) -> None:
        """Test get_kv_store operation."""
        service = kv_store_service_with_dependencies
        
        # Get the KV store
        kv_store = await service.get_kv_store(service.kv_config.bucket_name)
        
        # Verify that a KV store was returned
        assert kv_store is not None
    
    async def test_put_with_ttl(
        self,
        kv_store_service_with_dependencies: KVStoreService,
        test_data: Dict[str, Any]
    ) -> None:
        """Test put operation with TTL."""
        service = kv_store_service_with_dependencies
        
        # Put a value with a short TTL
        await service.put(
            service.kv_config.bucket_name,
            test_data["key"],
            test_data["value"],
            ttl=1,  # 1 second TTL
            serialize=True
        )
        
        # Get the value immediately
        result = await service.get(
            service.kv_config.bucket_name,
            test_data["key"],
            deserialize=True
        )
        
        # Verify the result
        assert result == test_data["value"]
        
        # Note: We can't easily test TTL expiration in unit tests
        # as it would require waiting for the TTL to expire