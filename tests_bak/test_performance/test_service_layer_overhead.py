"""
Performance Regression Tests for Service Layer Overhead

This module contains performance tests to ensure that the service layer
abstraction does not introduce significant overhead compared to direct usage.
Tests measure initialization, method calls, and resource management overhead.
"""

import asyncio
import statistics
import time
from typing import Any, Dict, List, Optional

import msgspec
import pytest

from naq.services.base import BaseService, ServiceConfig, ServiceManager
from naq.services.connection import ConnectionService, ConnectionServiceConfig
from naq.services.jobs import JobService, JobServiceConfig
from naq.services.events import EventService, EventServiceConfig
from naq.services.worker import WorkerService, WorkerServiceConfig
from naq.services.kv_stores import KVStoreService, KVStoreServiceConfig
from naq.models.jobs import Job
from naq.models.events import JobEvent
from naq.models.enums import JobEventType


class SimpleTestService(BaseService):
    """A simple test service for benchmarking overhead."""
    
    def __init__(self, config: Optional[ServiceConfig] = None) -> None:
        super().__init__(config)
        self._call_count = 0
    
    async def _do_initialize(self) -> None:
        """Initialize the test service."""
        await asyncio.sleep(0.001)  # Simulate minimal initialization work
    
    async def _do_cleanup(self) -> None:
        """Clean up the test service."""
        await asyncio.sleep(0.001)  # Simulate minimal cleanup work
    
    async def simple_operation(self, value: int) -> int:
        """A simple operation to measure method call overhead."""
        self._call_count += 1
        return value * 2


class PerformanceTestConfig(msgspec.Struct):
    """Configuration for performance tests."""
    
    warmup_iterations: int = 5
    measurement_iterations: int = 50
    service_initialization_iterations: int = 20
    method_call_iterations: int = 1000
    manager_access_iterations: int = 1000
    
    # Performance thresholds (in seconds)
    max_service_init_time: float = 0.1
    max_method_call_time: float = 0.001
    max_manager_access_time: float = 0.0001
    max_service_cleanup_time: float = 0.1


@pytest.fixture
def perf_config() -> PerformanceTestConfig:
    """Fixture providing performance test configuration."""
    return PerformanceTestConfig()


def simple_task(x: int, y: int) -> int:
    """Simple task for job service testing."""
    return x + y


async def measure_execution_time(func, *args, **kwargs) -> float:
    """Measure execution time of an async function."""
    start_time = time.perf_counter()
    result = await func(*args, **kwargs)
    end_time = time.perf_counter()
    return end_time - start_time


class TestServiceInitializationOverhead:
    """Test service initialization overhead."""
    
    @pytest.mark.asyncio
    async def test_base_service_initialization(self, perf_config: PerformanceTestConfig) -> None:
        """Test that BaseService initialization is within acceptable limits."""
        # Warmup
        for _ in range(perf_config.warmup_iterations):
            service = SimpleTestService()
            await service.initialize()
            await service.cleanup()
        
        # Measurement
        init_times = []
        for _ in range(perf_config.service_initialization_iterations):
            service = SimpleTestService()
            init_time = await measure_execution_time(service.initialize)
            init_times.append(init_time)
            await service.cleanup()
        
        avg_init_time = statistics.mean(init_times)
        max_init_time = max(init_times)
        
        # Assertions
        assert avg_init_time <= perf_config.max_service_init_time, \
            f"Average service initialization time {avg_init_time:.4f}s exceeds threshold {perf_config.max_service_init_time}s"
        
        assert max_init_time <= perf_config.max_service_init_time * 2, \
            f"Maximum service initialization time {max_init_time:.4f}s exceeds 2x threshold"
    
    @pytest.mark.asyncio
    async def test_connection_service_initialization(self, perf_config: PerformanceTestConfig) -> None:
        """Test ConnectionService initialization performance."""
        config = ServiceConfig(custom_settings={
            "nats_url": "nats://localhost:4222",
            "max_reconnect_attempts": 1,
            "connection_timeout": 5.0
        })
        
        # Warmup
        for _ in range(perf_config.warmup_iterations):
            service = ConnectionService(config)
            try:
                await service.initialize()
                await service.cleanup()
            except Exception:
                # Skip if NATS is not available
                pytest.skip("NATS not available for connection service test")
                return
        
        # Measurement
        init_times = []
        for _ in range(perf_config.service_initialization_iterations):
            service = ConnectionService(config)
            try:
                init_time = await measure_execution_time(service.initialize)
                init_times.append(init_time)
                await service.cleanup()
            except Exception:
                # Skip if NATS is not available
                pytest.skip("NATS not available for connection service test")
                return
        
        avg_init_time = statistics.mean(init_times)
        
        # Connection service can be slower due to NATS connection
        assert avg_init_time <= perf_config.max_service_init_time * 3, \
            f"Average connection service initialization time {avg_init_time:.4f}s exceeds threshold"
    
    @pytest.mark.asyncio
    async def test_job_service_initialization(self, perf_config: PerformanceTestConfig) -> None:
        """Test JobService initialization performance."""
        config = ServiceConfig(custom_settings={
            "enable_job_execution": True,
            "enable_result_storage": False,  # Disable to avoid NATS dependency
            "enable_event_logging": False
        })
        
        # Warmup
        for _ in range(perf_config.warmup_iterations):
            service = JobService(config)
            await service.initialize()
            await service.cleanup()
        
        # Measurement
        init_times = []
        for _ in range(perf_config.service_initialization_iterations):
            service = JobService(config)
            init_time = await measure_execution_time(service.initialize)
            init_times.append(init_time)
            await service.cleanup()
        
        avg_init_time = statistics.mean(init_times)
        
        assert avg_init_time <= perf_config.max_service_init_time * 2, \
            f"Average job service initialization time {avg_init_time:.4f}s exceeds threshold"


class TestServiceMethodCallOverhead:
    """Test service method call overhead."""
    
    @pytest.mark.asyncio
    async def test_base_service_method_calls(self, perf_config: PerformanceTestConfig) -> None:
        """Test that service method calls are within acceptable limits."""
        service = SimpleTestService()
        await service.initialize()
        
        # Warmup
        for i in range(perf_config.warmup_iterations):
            await service.simple_operation(i)
        
        # Measurement
        call_times = []
        for i in range(perf_config.method_call_iterations):
            call_time = await measure_execution_time(service.simple_operation, i)
            call_times.append(call_time)
        
        await service.cleanup()
        
        avg_call_time = statistics.mean(call_times)
        max_call_time = max(call_times)
        
        # Assertions
        assert avg_call_time <= perf_config.max_method_call_time, \
            f"Average method call time {avg_call_time:.6f}s exceeds threshold {perf_config.max_method_call_time}s"
        
        assert max_call_time <= perf_config.max_method_call_time * 10, \
            f"Maximum method call time {max_call_time:.6f}s exceeds 10x threshold"
    
    @pytest.mark.asyncio
    async def test_service_manager_overhead(self, perf_config: PerformanceTestConfig) -> None:
        """Test ServiceManager access overhead."""
        manager = ServiceManager()
        await manager.register_service("test_service", SimpleTestService, initialize=False)
        
        # Warmup
        for _ in range(perf_config.warmup_iterations):
            service = await manager.get_service("test_service")
            await service.simple_operation(1)
        
        # Measurement
        access_times = []
        for _ in range(perf_config.manager_access_iterations):
            access_time = await measure_execution_time(manager.get_service, "test_service")
            access_times.append(access_time)
        
        await manager.cleanup_all()
        
        avg_access_time = statistics.mean(access_times)
        
        # Assertions
        assert avg_access_time <= perf_config.max_manager_access_time, \
            f"Average service manager access time {avg_access_time:.6f}s exceeds threshold {perf_config.max_manager_access_time}s"


class TestServiceCleanupOverhead:
    """Test service cleanup overhead."""
    
    @pytest.mark.asyncio
    async def test_base_service_cleanup(self, perf_config: PerformanceTestConfig) -> None:
        """Test that BaseService cleanup is within acceptable limits."""
        # Warmup
        for _ in range(perf_config.warmup_iterations):
            service = SimpleTestService()
            await service.initialize()
            await service.cleanup()
        
        # Measurement
        cleanup_times = []
        for _ in range(perf_config.service_initialization_iterations):
            service = SimpleTestService()
            await service.initialize()
            cleanup_time = await measure_execution_time(service.cleanup)
            cleanup_times.append(cleanup_time)
        
        avg_cleanup_time = statistics.mean(cleanup_times)
        max_cleanup_time = max(cleanup_times)
        
        # Assertions
        assert avg_cleanup_time <= perf_config.max_service_cleanup_time, \
            f"Average service cleanup time {avg_cleanup_time:.4f}s exceeds threshold {perf_config.max_service_cleanup_time}s"
        
        assert max_cleanup_time <= perf_config.max_service_cleanup_time * 2, \
            f"Maximum service cleanup time {max_cleanup_time:.4f}s exceeds 2x threshold"


class TestServiceManagerPerformance:
    """Test ServiceManager performance with multiple services."""
    
    @pytest.mark.asyncio
    async def test_multiple_service_registration(self, perf_config: PerformanceTestConfig) -> None:
        """Test registering multiple services."""
        manager = ServiceManager()
        
        # Warmup
        for i in range(perf_config.warmup_iterations):
            await manager.register_service(f"test_service_{i}", SimpleTestService, initialize=False)
        
        # Measurement
        registration_times = []
        for i in range(perf_config.measurement_iterations):
            start_time = time.perf_counter()
            await manager.register_service(f"perf_service_{i}", SimpleTestService, initialize=False)
            registration_time = time.perf_counter() - start_time
            registration_times.append(registration_time)
        
        await manager.cleanup_all()
        
        avg_registration_time = statistics.mean(registration_times)
        
        # Should be very fast since we're not initializing
        assert avg_registration_time <= 0.01, \
            f"Average service registration time {avg_registration_time:.6f}s exceeds threshold"
    
    @pytest.mark.asyncio
    async def test_service_manager_with_dependencies(self, perf_config: PerformanceTestConfig) -> None:
        """Test ServiceManager with service dependencies."""
        manager = ServiceManager()
        
        # Register services with dependencies
        await manager.register_service("connection", ConnectionService, initialize=False)
        await manager.register_service("kv_store", KVStoreService, initialize=False)
        await manager.register_service("event", EventService, initialize=False)
        await manager.register_service("job", JobService, initialize=False)
        
        # Warmup
        for _ in range(perf_config.warmup_iterations):
            try:
                service = await manager.get_service("job")
            except Exception:
                # Skip if dependencies are not available
                await manager.cleanup_all()
                pytest.skip("Service dependencies not available")
                return
        
        # Measurement
        access_times = []
        for _ in range(perf_config.measurement_iterations):
            try:
                access_time = await measure_execution_time(manager.get_service, "job")
                access_times.append(access_time)
            except Exception:
                # Skip if dependencies are not available
                await manager.cleanup_all()
                pytest.skip("Service dependencies not available")
                return
        
        await manager.cleanup_all()
        
        avg_access_time = statistics.mean(access_times)
        
        # Should be fast after first access (lazy initialization)
        assert avg_access_time <= perf_config.max_manager_access_time * 2, \
            f"Average dependent service access time {avg_access_time:.6f}s exceeds threshold"


class TestServiceConfigurationOverhead:
    """Test service configuration overhead."""
    
    @pytest.mark.asyncio
    async def test_configuration_extraction_performance(self, perf_config: PerformanceTestConfig) -> None:
        """Test configuration extraction performance."""
        
        def create_service_with_config():
            """Create a service with complex configuration."""
            custom_settings = {
                "setting1": "value1",
                "setting2": 42,
                "setting3": [1, 2, 3, 4, 5],
                "setting4": {"nested": "value"},
                "setting5": True,
                "setting6": 3.14159,
            }
            config = ServiceConfig(custom_settings=custom_settings)
            return SimpleTestService(config)
        
        # Warmup
        for _ in range(perf_config.warmup_iterations):
            service = create_service_with_config()
            await service.initialize()
            await service.cleanup()
        
        # Measurement
        creation_times = []
        for _ in range(perf_config.measurement_iterations):
            start_time = time.perf_counter()
            service = create_service_with_config()
            creation_time = time.perf_counter() - start_time
            creation_times.append(creation_time)
            await service.initialize()
            await service.cleanup()
        
        avg_creation_time = statistics.mean(creation_times)
        
        # Should be very fast
        assert avg_creation_time <= 0.001, \
            f"Average service creation with config time {avg_creation_time:.6f}s exceeds threshold"


class TestServiceMemoryUsage:
    """Test service memory usage patterns."""
    
    @pytest.mark.asyncio
    async def test_service_manager_memory_cleanup(self, perf_config: PerformanceTestConfig) -> None:
        """Test that ServiceManager properly cleans up memory."""
        import gc
        import sys
        
        # Get initial memory
        gc.collect()
        initial_objects = len(gc.get_objects())
        
        # Create and cleanup many services
        for i in range(perf_config.measurement_iterations):
            manager = ServiceManager()
            await manager.register_service(f"service_{i}", SimpleTestService)
            await manager.cleanup_all()
        
        # Force garbage collection
        gc.collect()
        final_objects = len(gc.get_objects())
        
        # Memory growth should be minimal
        object_growth = final_objects - initial_objects
        max_allowed_growth = perf_config.measurement_iterations * 10  # Allow some growth
        
        assert object_growth <= max_allowed_growth, \
            f"Memory growth {object_growth} objects exceeds maximum allowed {max_allowed_growth}"