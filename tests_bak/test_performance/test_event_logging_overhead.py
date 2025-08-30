"""
Performance Regression Tests for Event Logging Overhead

This module contains performance tests to ensure that event logging
does not introduce significant overhead during job execution.
Tests measure event creation, logging, storage, and retrieval overhead.
"""

import asyncio
import statistics
import time
from typing import Any, Dict, List, Optional

import msgspec
import pytest

from naq.services.base import ServiceConfig, ServiceManager
from naq.services.events import EventService, EventServiceConfig
from naq.services.kv_stores import KVStoreService, KVStoreServiceConfig
from naq.models.events import JobEvent, WorkerEvent
from naq.models.enums import JobEventType, WorkerEventType
from naq.models.jobs import Job


class MockNATSClient:
    """Mock NATS client for testing purposes."""
    
    def __init__(self):
        self._kv_stores = {}
    
    def jetstream(self):
        """Return a mock JetStream context."""
        return MockJetStreamContext(self)
    
    async def key_value(self, bucket_name):
        """Mock key_value method."""
        if bucket_name not in self._kv_stores:
            self._kv_stores[bucket_name] = MockKeyValue(bucket_name)
        return self._kv_stores[bucket_name]
    
    async def create_key_value(self, bucket_name, ttl=None, description=None):
        """Mock create_key_value method."""
        self._kv_stores[bucket_name] = MockKeyValue(bucket_name)
        return self._kv_stores[bucket_name]


class MockJetStreamContext:
    """Mock JetStream context for testing."""
    
    def __init__(self, nats_client):
        self._nats_client = nats_client
    
    async def key_value(self, bucket=None):
        """Get or create a mock key-value store."""
        return await self._nats_client.key_value(bucket)
    
    async def create_key_value(self, bucket=None, ttl=None, description=None):
        """Create a mock key-value store."""
        return await self._nats_client.create_key_value(bucket, ttl, description)


class MockKeyValue:
    """Mock KeyValue store for testing purposes."""
    
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self._data = {}
    
    async def put(self, key, value, ttl=None):
        """Mock put method."""
        self._data[key] = value
    
    async def get(self, key):
        """Mock get method."""
        if key not in self._data:
            from nats.js.errors import KeyNotFoundError
            raise KeyNotFoundError(f"Key {key} not found")
        return MockEntry(self._data[key])
    
    async def delete(self, key, purge=False):
        """Mock delete method."""
        if key in self._data:
            del self._data[key]
            return True
        return False


class MockEntry:
    """Mock KV entry for testing purposes."""
    
    def __init__(self, value):
        self.value = value


class EventPerformanceTestConfig(msgspec.Struct):
    """Configuration for event logging performance tests."""
    
    warmup_iterations: int = 5
    measurement_iterations: int = 50
    event_creation_iterations: int = 1000
    event_logging_iterations: int = 100
    event_retrieval_iterations: int = 50
    
    # Performance thresholds (in seconds)
    max_event_creation_time: float = 0.0001  # 0.1ms per event
    max_event_logging_time: float = 0.01     # 10ms per event
    max_event_retrieval_time: float = 0.05   # 50ms per retrieval
    max_batch_logging_time: float = 0.1      # 100ms for 100 events


@pytest.fixture
def event_perf_config() -> EventPerformanceTestConfig:
    """Fixture providing event performance test configuration."""
    return EventPerformanceTestConfig()


def simple_task(x: int, y: int) -> int:
    """Simple task for testing."""
    return x + y


async def measure_execution_time(func, *args, **kwargs) -> float:
    """Measure execution time of an async function."""
    start_time = time.perf_counter()
    result = await func(*args, **kwargs)
    end_time = time.perf_counter()
    return end_time - start_time


class TestEventCreationOverhead:
    """Test event creation overhead."""
    
    @pytest.mark.asyncio
    async def test_job_event_creation(self, event_perf_config: EventPerformanceTestConfig) -> None:
        """Test JobEvent creation performance."""
        job_id = "test-job-123"
        worker_id = "test-worker-456"
        queue_name = "test-queue"
        
        # Warmup
        for i in range(event_perf_config.warmup_iterations):
            event = JobEvent.started(
                job_id=f"{job_id}-{i}",
                worker_id=worker_id,
                queue_name=queue_name
            )
        
        # Measurement
        creation_times = []
        for i in range(event_perf_config.event_creation_iterations):
            start_time = time.perf_counter()
            event = JobEvent.started(
                job_id=f"{job_id}-{i}",
                worker_id=worker_id,
                queue_name=queue_name
            )
            creation_time = time.perf_counter() - start_time
            creation_times.append(creation_time)
        
        avg_creation_time = statistics.mean(creation_times)
        max_creation_time = max(creation_times)
        
        # Assertions
        assert avg_creation_time <= event_perf_config.max_event_creation_time, \
            f"Average job event creation time {avg_creation_time:.6f}s exceeds threshold {event_perf_config.max_event_creation_time}s"
        
        assert max_creation_time <= event_perf_config.max_event_creation_time * 10, \
            f"Maximum job event creation time {max_creation_time:.6f}s exceeds 10x threshold"
    
    @pytest.mark.asyncio
    async def test_worker_event_creation(self, event_perf_config: EventPerformanceTestConfig) -> None:
        """Test WorkerEvent creation performance."""
        worker_id = "test-worker-123"
        queue_names = ["queue1", "queue2"]
        
        # Warmup
        for i in range(event_perf_config.warmup_iterations):
            event = WorkerEvent.started(
                worker_id=f"{worker_id}-{i}",
                queue_names=queue_names
            )
        
        # Measurement
        creation_times = []
        for i in range(event_perf_config.event_creation_iterations):
            start_time = time.perf_counter()
            event = WorkerEvent.started(
                worker_id=f"{worker_id}-{i}",
                queue_names=queue_names
            )
            creation_time = time.perf_counter() - start_time
            creation_times.append(creation_time)
        
        avg_creation_time = statistics.mean(creation_times)
        
        # Assertions
        assert avg_creation_time <= event_perf_config.max_event_creation_time, \
            f"Average worker event creation time {avg_creation_time:.6f}s exceeds threshold {event_perf_config.max_event_creation_time}s"
    
    @pytest.mark.asyncio
    async def test_event_with_details_creation(self, event_perf_config: EventPerformanceTestConfig) -> None:
        """Test event creation with complex details."""
        job_id = "test-job-123"
        worker_id = "test-worker-456"
        queue_name = "test-queue"
        
        # Complex details
        details = {
            "result_size": 1024,
            "memory_usage": 51200000,
            "cpu_time": 0.045,
            "custom_metrics": {
                "metric1": 100,
                "metric2": "value",
                "metric3": [1, 2, 3, 4, 5]
            }
        }
        
        # Warmup
        for i in range(event_perf_config.warmup_iterations):
            event = JobEvent.completed(
                job_id=f"{job_id}-{i}",
                worker_id=worker_id,
                duration_ms=45.2,
                queue_name=queue_name,
                details=details
            )
        
        # Measurement
        creation_times = []
        for i in range(event_perf_config.event_creation_iterations):
            start_time = time.perf_counter()
            event = JobEvent.completed(
                job_id=f"{job_id}-{i}",
                worker_id=worker_id,
                duration_ms=45.2,
                queue_name=queue_name,
                details=details
            )
            creation_time = time.perf_counter() - start_time
            creation_times.append(creation_time)
        
        avg_creation_time = statistics.mean(creation_times)
        
        # Complex events can take longer but should still be reasonable
        assert avg_creation_time <= event_perf_config.max_event_creation_time * 5, \
            f"Average complex event creation time {avg_creation_time:.6f}s exceeds threshold"


class TestEventLoggingOverhead:
    """Test event logging overhead."""
    
    @pytest.mark.asyncio
    async def test_job_event_logging(self, event_perf_config: EventPerformanceTestConfig) -> None:
        """Test JobEvent logging performance."""
        # Create a mock KV store service for testing
        kv_config = KVStoreServiceConfig(auto_create_buckets=True)
        kv_service = KVStoreService(
            config=ServiceConfig(custom_settings=kv_config.as_dict()),
            nats_client=MockNATSClient()  # We'll create this mock below
        )
        await kv_service.initialize()
        
        # Create event service with the KV store service
        config = ServiceConfig(custom_settings={
            "enable_event_logging": True,
            "auto_create_bucket": True,
            "max_events_per_job": 1000
        })
        
        event_service = EventService(config, kv_store_service=kv_service)
        await event_service.initialize()
        
        job_id = "test-job-123"
        worker_id = "test-worker-456"
        queue_name = "test-queue"
        
        # Warmup
        for i in range(event_perf_config.warmup_iterations):
            event = JobEvent.started(
                job_id=f"{job_id}-{i}",
                worker_id=worker_id,
                queue_name=queue_name
            )
            await event_service.log_job_event(event)
        
        # Measurement
        logging_times = []
        for i in range(event_perf_config.event_logging_iterations):
            event = JobEvent.started(
                job_id=f"{job_id}-{i}",
                worker_id=worker_id,
                queue_name=queue_name
            )
            logging_time = await measure_execution_time(event_service.log_job_event, event)
            logging_times.append(logging_time)
        
        await event_service.cleanup()
        
        avg_logging_time = statistics.mean(logging_times)
        max_logging_time = max(logging_times)
        
        # Assertions
        assert avg_logging_time <= event_perf_config.max_event_logging_time, \
            f"Average job event logging time {avg_logging_time:.6f}s exceeds threshold {event_perf_config.max_event_logging_time}s"
        
        assert max_logging_time <= event_perf_config.max_event_logging_time * 5, \
            f"Maximum job event logging time {max_logging_time:.6f}s exceeds 5x threshold"
    
    @pytest.mark.asyncio
    async def test_batch_event_logging(self, event_perf_config: EventPerformanceTestConfig) -> None:
        """Test batch event logging performance."""
        # Create a mock KV store service for testing
        kv_config = KVStoreServiceConfig(auto_create_buckets=True)
        kv_service = KVStoreService(
            config=ServiceConfig(custom_settings=kv_config.as_dict()),
            nats_client=MockNATSClient()  # We'll create this mock below
        )
        await kv_service.initialize()
        
        # Create event service with the KV store service
        config = ServiceConfig(custom_settings={
            "enable_event_logging": True,
            "auto_create_bucket": True,
            "max_events_per_job": 1000
        })
        
        event_service = EventService(config, kv_store_service=kv_service)
        await event_service.initialize()
        
        job_id = "test-job-123"
        worker_id = "test-worker-456"
        queue_name = "test-queue"
        
        # Create batch of events
        batch_size = 100
        events = []
        for i in range(batch_size):
            event = JobEvent.started(
                job_id=f"{job_id}-{i}",
                worker_id=worker_id,
                queue_name=queue_name
            )
            events.append(event)
        
        # Warmup
        for _ in range(event_perf_config.warmup_iterations):
            for event in events[:10]:  # Smaller batch for warmup
                await event_service.log_job_event(event)
        
        # Measurement
        batch_times = []
        for _ in range(event_perf_config.measurement_iterations):
            start_time = time.perf_counter()
            for event in events:
                await event_service.log_job_event(event)
            batch_time = time.perf_counter() - start_time
            batch_times.append(batch_time)
        
        await event_service.cleanup()
        
        avg_batch_time = statistics.mean(batch_times)
        avg_time_per_event = avg_batch_time / batch_size
        
        # Assertions
        assert avg_batch_time <= event_perf_config.max_batch_logging_time, \
            f"Average batch logging time {avg_batch_time:.6f}s exceeds threshold {event_perf_config.max_batch_logging_time}s"
        
        assert avg_time_per_event <= event_perf_config.max_event_logging_time, \
            f"Average time per event in batch {avg_time_per_event:.6f}s exceeds threshold {event_perf_config.max_event_logging_time}s"


class TestEventRetrievalOverhead:
    """Test event retrieval overhead."""
    
    @pytest.mark.asyncio
    async def test_job_event_retrieval(self, event_perf_config: EventPerformanceTestConfig) -> None:
        """Test JobEvent retrieval performance."""
        # Create a mock KV store service for testing
        kv_config = KVStoreServiceConfig(auto_create_buckets=True)
        kv_service = KVStoreService(
            config=ServiceConfig(custom_settings=kv_config.as_dict()),
            nats_client=MockNATSClient()  # We'll create this mock below
        )
        await kv_service.initialize()
        
        # Create event service with the KV store service
        config = ServiceConfig(custom_settings={
            "enable_event_logging": True,
            "auto_create_bucket": True,
            "max_events_per_job": 100
        })
        
        event_service = EventService(config, kv_store_service=kv_service)
        await event_service.initialize()
        
        job_id = "test-job-123"
        worker_id = "test-worker-456"
        queue_name = "test-queue"
        
        # Log some events first
        num_events = 50
        for i in range(num_events):
            event = JobEvent.started(
                job_id=job_id,
                worker_id=worker_id,
                queue_name=queue_name
            )
            await event_service.log_job_event(event)
            
            event = JobEvent.completed(
                job_id=job_id,
                worker_id=worker_id,
                duration_ms=10.0 * i,
                queue_name=queue_name
            )
            await event_service.log_job_event(event)
        
        # Warmup
        for _ in range(event_perf_config.warmup_iterations):
            await event_service.get_job_events(job_id)
        
        # Measurement
        retrieval_times = []
        for _ in range(event_perf_config.event_retrieval_iterations):
            retrieval_time = await measure_execution_time(event_service.get_job_events, job_id)
            retrieval_times.append(retrieval_time)
        
        await event_service.cleanup()
        
        avg_retrieval_time = statistics.mean(retrieval_times)
        
        # Assertions
        assert avg_retrieval_time <= event_perf_config.max_event_retrieval_time, \
            f"Average job event retrieval time {avg_retrieval_time:.6f}s exceeds threshold {event_perf_config.max_event_retrieval_time}s"
    
    @pytest.mark.asyncio
    async def test_filtered_event_retrieval(self, event_perf_config: EventPerformanceTestConfig) -> None:
        """Test filtered event retrieval performance."""
        # Create a mock KV store service for testing
        kv_config = KVStoreServiceConfig(auto_create_buckets=True)
        kv_service = KVStoreService(
            config=ServiceConfig(custom_settings=kv_config.as_dict()),
            nats_client=MockNATSClient()  # We'll create this mock below
        )
        await kv_service.initialize()
        
        # Create event service with the KV store service
        config = ServiceConfig(custom_settings={
            "enable_event_logging": True,
            "auto_create_bucket": True,
            "max_events_per_job": 100
        })
        
        event_service = EventService(config, kv_store_service=kv_service)
        await event_service.initialize()
        
        job_id = "test-job-123"
        worker_id = "test-worker-456"
        queue_name = "test-queue"
        
        # Log mixed event types
        num_events = 50
        for i in range(num_events):
            # Log started events
            event = JobEvent.started(
                job_id=job_id,
                worker_id=worker_id,
                queue_name=queue_name
            )
            await event_service.log_job_event(event)
            
            # Log completed events
            event = JobEvent.completed(
                job_id=job_id,
                worker_id=worker_id,
                duration_ms=10.0 * i,
                queue_name=queue_name
            )
            await event_service.log_job_event(event)
            
            # Log failed events
            if i % 5 == 0:
                event = JobEvent.failed(
                    job_id=job_id,
                    worker_id=worker_id,
                    error_type="TestError",
                    error_message="Test error message",
                    duration_ms=5.0,
                    queue_name=queue_name
                )
                await event_service.log_job_event(event)
        
        # Warmup
        for _ in range(event_perf_config.warmup_iterations):
            await event_service.get_job_events(job_id, event_type=JobEventType.COMPLETED)
        
        # Measurement
        retrieval_times = []
        for _ in range(event_perf_config.event_retrieval_iterations):
            retrieval_time = await measure_execution_time(
                event_service.get_job_events, 
                job_id, 
                event_type=JobEventType.COMPLETED
            )
            retrieval_times.append(retrieval_time)
        
        await event_service.cleanup()
        
        avg_retrieval_time = statistics.mean(retrieval_times)
        
        # Filtered retrieval should be fast
        assert avg_retrieval_time <= event_perf_config.max_event_retrieval_time, \
            f"Average filtered event retrieval time {avg_retrieval_time:.6f}s exceeds threshold"


class TestEventMemoryUsage:
    """Test event memory usage patterns."""
    
    @pytest.mark.asyncio
    async def test_event_storage_memory(self, event_perf_config: EventPerformanceTestConfig) -> None:
        """Test that event storage doesn't leak memory."""
        import gc
        import sys
        
        config = ServiceConfig(custom_settings={
            "enable_event_logging": True,
            "auto_create_bucket": True,
            "max_events_per_job": 100
        })
        
        # Get initial memory
        gc.collect()
        initial_objects = len(gc.get_objects())
        
        # Create and cleanup event services with many events
        for i in range(event_perf_config.measurement_iterations):
            # Create a mock KV store service for testing
            kv_config = KVStoreServiceConfig(auto_create_buckets=True)
            kv_service = KVStoreService(
                config=ServiceConfig(custom_settings=kv_config.as_dict()),
                nats_client=MockNATSClient()  # We'll create this mock below
            )
            await kv_service.initialize()
            
            event_service = EventService(config, kv_store_service=kv_service)
            await event_service.initialize()
            
            # Log many events
            job_id = f"test-job-{i}"
            worker_id = "test-worker-456"
            queue_name = "test-queue"
            
            for j in range(50):
                event = JobEvent.started(
                    job_id=job_id,
                    worker_id=worker_id,
                    queue_name=queue_name
                )
                await event_service.log_job_event(event)
                
                event = JobEvent.completed(
                    job_id=job_id,
                    worker_id=worker_id,
                    duration_ms=10.0 * j,
                    queue_name=queue_name
                )
                await event_service.log_job_event(event)
            
            await event_service.cleanup()
        
        # Force garbage collection
        gc.collect()
        final_objects = len(gc.get_objects())
        
        # Memory growth should be minimal
        object_growth = final_objects - initial_objects
        max_allowed_growth = event_perf_config.measurement_iterations * 50  # Allow some growth
        
        assert object_growth <= max_allowed_growth, \
            f"Memory growth {object_growth} objects exceeds maximum allowed {max_allowed_growth}"


class TestEventSerializationOverhead:
    """Test event serialization overhead."""
    
    @pytest.mark.asyncio
    async def test_event_serialization_performance(self, event_perf_config: EventPerformanceTestConfig) -> None:
        """Test event serialization and deserialization performance."""
        import json
        
        # Create a complex event
        job_id = "test-job-123"
        worker_id = "test-worker-456"
        queue_name = "test-queue"
        
        details = {
            "result_size": 1024,
            "memory_usage": 51200000,
            "cpu_time": 0.045,
            "custom_metrics": {
                "metric1": 100,
                "metric2": "value",
                "metric3": [1, 2, 3, 4, 5]
            }
        }
        
        event = JobEvent.completed(
            job_id=job_id,
            worker_id=worker_id,
            duration_ms=45.2,
            queue_name=queue_name,
            details=details
        )
        
        # Create msgspec encoder/decoder
        encoder = msgspec.json.Encoder()
        decoder = msgspec.json.Decoder(JobEvent)
        
        # Warmup
        for _ in range(event_perf_config.warmup_iterations):
            serialized = encoder.encode(event)
            deserialized = decoder.decode(serialized)
        
        # Measurement - serialization
        serialization_times = []
        for _ in range(event_perf_config.event_creation_iterations):
            start_time = time.perf_counter()
            serialized = encoder.encode(event)
            serialization_time = time.perf_counter() - start_time
            serialization_times.append(serialization_time)
        
        # Measurement - deserialization
        deserialization_times = []
        serialized = encoder.encode(event)  # Serialize once
        for _ in range(event_perf_config.event_creation_iterations):
            start_time = time.perf_counter()
            deserialized = decoder.decode(serialized)
            deserialization_time = time.perf_counter() - start_time
            deserialization_times.append(deserialization_time)
        
        avg_serialization_time = statistics.mean(serialization_times)
        avg_deserialization_time = statistics.mean(deserialization_times)
        
        # Both should be very fast
        assert avg_serialization_time <= event_perf_config.max_event_creation_time, \
            f"Average event serialization time {avg_serialization_time:.6f}s exceeds threshold"
        
        assert avg_deserialization_time <= event_perf_config.max_event_creation_time, \
            f"Average event deserialization time {avg_deserialization_time:.6f}s exceeds threshold"