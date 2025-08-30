# Service Testing Patterns

This directory contains tests for NAQ services following the ServiceManager architecture.

## Testing Philosophy

Service tests should focus on:
1. **Service Lifecycle**: Testing initialization and cleanup behavior
2. **Service Dependencies**: Testing interactions with other services
3. **Service Configuration**: Testing configuration handling
4. **Service-Specific Functionality**: Testing core service methods

## Common Testing Patterns

### 1. Service Lifecycle Testing

All services should test their initialization and cleanup behavior:

```python
import pytest
from naq.services.base import ServiceConfig
from naq.services.your_service import YourService

@pytest_asyncio.fixture
async def service_config():
    """Fixture for service configuration."""
    return ServiceConfig(
        nats_url="nats://localhost:4222",
        log_level="DEBUG",
        custom_settings={
            "test_mode": True,
            # Service-specific settings
        }
    )

@pytest_asyncio.fixture
async def service_instance(service_config):
    """Fixture for service instance with proper lifecycle management."""
    service = YourService(config=service_config)
    try:
        await service.initialize()
        yield service
    finally:
        await service.cleanup()

async def test_service_initialization(service_instance):
    """Test service initialization."""
    assert service_instance.is_initialized is True

async def test_service_cleanup(service_instance):
    """Test service cleanup."""
    await service_instance.cleanup()
    assert service_instance.is_initialized is False
```

### 2. Service Dependencies Testing

When testing services with dependencies, use the mock fixtures from conftest.py:

```python
@pytest_asyncio.fixture
async def service_with_dependencies(mock_service_manager, service_config):
    """Fixture for service with mocked dependencies."""
    # Get mock services from the mock_service_manager
    mock_connection_service = mock_service_manager._mock_connection_service
    mock_kv_store_service = mock_service_manager._mock_kv_store_service
    
    # Create service with mocked dependencies
    service = YourService(
        config=service_config,
        connection_service=mock_connection_service,
        kv_store_service=mock_kv_store_service
    )
    
    try:
        await service.initialize()
        yield service
    finally:
        await service.cleanup()
```

### 3. Service Configuration Testing

Test service configuration handling:

```python
async def test_service_configuration():
    """Test service configuration handling."""
    config = ServiceConfig(
        custom_settings={
            "service_specific_setting": "test_value"
        }
    )
    
    service = YourService(config=config)
    assert service.service_config.service_specific_setting == "test_value"
```

### 4. Service-Specific Functionality Testing

Test core service methods:

```python
async def test_service_specific_method(service_instance):
    """Test a service-specific method."""
    result = await service_instance.service_method()
    assert result is not None
    # Add specific assertions based on the method behavior
```

## Using Fixtures from conftest.py

The following fixtures from conftest.py are available for service testing:

- `mock_service_manager`: Provides a mock ServiceManager with all services mocked
- `service_manager`: Provides a real ServiceManager instance for integration tests
- `service_test_config`: Provides test configuration for services
- `service_aware_nats_mock`: Provides a mock NATS client with JetStream support

## Test Organization

Tests should be organized by service:
- `test_connection_service.py`: Tests for ConnectionService
- `test_job_service.py`: Tests for JobService
- `test_event_service.py`: Tests for EventService
- `test_kv_store_service.py`: Tests for KVStoreService
- `test_stream_service.py`: Tests for StreamService
- `test_scheduler_service.py`: Tests for SchedulerService

## Best Practices

1. **Use Async Fixtures**: All service fixtures should be async to properly handle service lifecycle
2. **Cleanup Resources**: Always ensure services are properly cleaned up in fixtures
3. **Mock Dependencies**: Use mocked dependencies for unit tests to isolate service behavior
4. **Test Error Cases**: Test both success and error scenarios
5. **Use Type Hints**: All test functions should have proper type hints
6. **Follow Naming Conventions**: Use descriptive test names following pytest conventions

## Integration vs Unit Tests

- **Unit Tests**: Focus on individual service behavior with mocked dependencies
- **Integration Tests**: Focus on service interactions with real dependencies when needed

Use the `mock_service_manager` fixture for unit tests and `service_manager` fixture for integration tests.