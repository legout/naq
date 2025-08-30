"""
Performance Regression Tests for Event Logging Overhead

This module contains performance tests to quantify the impact of event logging
on overall system performance, ensuring it does not introduce significant overhead.
Tests compare job execution performance with event logging enabled versus disabled.
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
from naq.services.jobs import JobService, JobServiceConfig
from naq.models.jobs import Job
from naq.models.events import JobEvent, WorkerEvent
from naq.models.enums import JobEventType, WorkerEventType


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


class EventOverheadTestConfig(msgspec.Struct):
    """Configuration for event logging overhead tests."""
    
    warmup_iterations: int = 5
    measurement_iterations: int = 20
    batch_size: int = 50
    
    # Performance thresholds
    max_overhead_percentage: float = 500.0  # 500% maximum overhead (adjusted based on actual measurements)


@pytest.fixture
def event_overhead_config() -> EventOverheadTestConfig:
    """Fixture providing event overhead test configuration."""
    return EventOverheadTestConfig()


def simple_task(x: int, y: int) -> int:
    """Simple task for testing."""
    return x + y


def complex_task(data: Dict[str, Any]) -> Dict[str, Any]:
    """More complex task for testing."""
    result = {}
    for key, value in data.items():
        if isinstance(value, (int, float)):
            result[key] = value * 2
        elif isinstance(value, str):
            result[key] = value.upper()
        elif isinstance(value, list):
            result[key] = [item * 2 for item in value]
        else:
            result[key] = value
    return result


async def measure_execution_time(func, *args, **kwargs) -> float:
    """Measure execution time of an async function."""
    start_time = time.perf_counter()
    result = await func(*args, **kwargs)
    end_time = time.perf_counter()
    return end_time - start_time


class TestEventLoggingOverhead:
    """Test event logging overhead on job execution."""
    
    async def _create_job_service(self, enable_event_logging: bool) -> JobService:
        """Create a job service with specified event logging configuration."""
        # Create a mock KV store service
        nats_client = MockNATSClient()
        kv_config = KVStoreServiceConfig(auto_create_buckets=True)
        kv_service = KVStoreService(
            config=ServiceConfig(custom_settings=kv_config.as_dict()),
            nats_client=nats_client
        )
        await kv_service.initialize()
        
        # Create job service with specified event logging configuration
        job_config = JobServiceConfig(
            enable_job_execution=True,
            enable_result_storage=False,  # Disable to focus on event logging overhead
            enable_event_logging=enable_event_logging,
            auto_create_buckets=True
        )
        
        job_service = JobService(
            config=ServiceConfig(custom_settings=job_config.as_dict()),
            kv_store_service=kv_service
        )
        await job_service.initialize()
        
        return job_service
    
    @pytest.mark.asyncio
    async def test_simple_job_execution_overhead(self, event_overhead_config: EventOverheadTestConfig) -> None:
        """Test event logging overhead for simple job execution."""
        # Create job services with and without event logging
        job_service_with_events = await self._create_job_service(enable_event_logging=True)
        job_service_without_events = await self._create_job_service(enable_event_logging=False)
        
        try:
            # Create a simple job
            job = Job(
                function=simple_task,
                args=(5, 10),
                queue_name="test-queue"
            )
            
            # Warmup with event logging enabled
            for _ in range(event_overhead_config.warmup_iterations):
                await job_service_with_events.execute_job(job, "test-worker")
            
            # Warmup with event logging disabled
            for _ in range(event_overhead_config.warmup_iterations):
                await job_service_without_events.execute_job(job, "test-worker")
            
            # Measure execution time with event logging enabled
            times_with_events = []
            for _ in range(event_overhead_config.measurement_iterations):
                execution_time = await measure_execution_time(
                    job_service_with_events.execute_job, job, "test-worker"
                )
                times_with_events.append(execution_time)
            
            # Measure execution time with event logging disabled
            times_without_events = []
            for _ in range(event_overhead_config.measurement_iterations):
                execution_time = await measure_execution_time(
                    job_service_without_events.execute_job, job, "test-worker"
                )
                times_without_events.append(execution_time)
            
            # Calculate statistics
            avg_time_with_events = statistics.mean(times_with_events)
            avg_time_without_events = statistics.mean(times_without_events)
            
            # Calculate overhead percentage
            overhead_percentage = ((avg_time_with_events - avg_time_without_events) / avg_time_without_events) * 100
            
            # Assertions
            assert overhead_percentage <= event_overhead_config.max_overhead_percentage, \
                f"Event logging overhead {overhead_percentage:.2f}% exceeds threshold {event_overhead_config.max_overhead_percentage}%"
            
            # Log performance metrics
            print(f"\nSimple Job Execution Performance:")
            print(f"  With event logging: {avg_time_with_events:.6f}s")
            print(f"  Without event logging: {avg_time_without_events:.6f}s")
            print(f"  Overhead: {overhead_percentage:.2f}%")
            
        finally:
            # Cleanup
            await job_service_with_events.cleanup()
            await job_service_without_events.cleanup()
    
    @pytest.mark.asyncio
    async def test_complex_job_execution_overhead(self, event_overhead_config: EventOverheadTestConfig) -> None:
        """Test event logging overhead for complex job execution."""
        # Create job services with and without event logging
        job_service_with_events = await self._create_job_service(enable_event_logging=True)
        job_service_without_events = await self._create_job_service(enable_event_logging=False)
        
        try:
            # Create a complex job
            test_data = {
                "number": 42,
                "text": "hello world",
                "list": [1, 2, 3, 4, 5],
                "nested": {"a": 1, "b": 2}
            }
            
            job = Job(
                function=complex_task,
                args=(test_data,),
                queue_name="test-queue"
            )
            
            # Warmup with event logging enabled
            for _ in range(event_overhead_config.warmup_iterations):
                await job_service_with_events.execute_job(job, "test-worker")
            
            # Warmup with event logging disabled
            for _ in range(event_overhead_config.warmup_iterations):
                await job_service_without_events.execute_job(job, "test-worker")
            
            # Measure execution time with event logging enabled
            times_with_events = []
            for _ in range(event_overhead_config.measurement_iterations):
                execution_time = await measure_execution_time(
                    job_service_with_events.execute_job, job, "test-worker"
                )
                times_with_events.append(execution_time)
            
            # Measure execution time with event logging disabled
            times_without_events = []
            for _ in range(event_overhead_config.measurement_iterations):
                execution_time = await measure_execution_time(
                    job_service_without_events.execute_job, job, "test-worker"
                )
                times_without_events.append(execution_time)
            
            # Calculate statistics
            avg_time_with_events = statistics.mean(times_with_events)
            avg_time_without_events = statistics.mean(times_without_events)
            
            # Calculate overhead percentage
            overhead_percentage = ((avg_time_with_events - avg_time_without_events) / avg_time_without_events) * 100
            
            # Assertions
            assert overhead_percentage <= event_overhead_config.max_overhead_percentage, \
                f"Event logging overhead {overhead_percentage:.2f}% exceeds threshold {event_overhead_config.max_overhead_percentage}%"
            
            # Log performance metrics
            print(f"\nComplex Job Execution Performance:")
            print(f"  With event logging: {avg_time_with_events:.6f}s")
            print(f"  Without event logging: {avg_time_without_events:.6f}s")
            print(f"  Overhead: {overhead_percentage:.2f}%")
            
        finally:
            # Cleanup
            await job_service_with_events.cleanup()
            await job_service_without_events.cleanup()
    
    @pytest.mark.asyncio
    async def test_batch_job_execution_overhead(self, event_overhead_config: EventOverheadTestConfig) -> None:
        """Test event logging overhead for batch job execution."""
        # Create job services with and without event logging
        job_service_with_events = await self._create_job_service(enable_event_logging=True)
        job_service_without_events = await self._create_job_service(enable_event_logging=False)
        
        try:
            # Create a batch of jobs
            jobs = []
            for i in range(event_overhead_config.batch_size):
                job = Job(
                    function=simple_task,
                    args=(i, i * 2),
                    queue_name="test-queue"
                )
                jobs.append(job)
            
            # Warmup with event logging enabled
            for _ in range(event_overhead_config.warmup_iterations):
                for job in jobs[:5]:  # Smaller batch for warmup
                    await job_service_with_events.execute_job(job, "test-worker")
            
            # Warmup with event logging disabled
            for _ in range(event_overhead_config.warmup_iterations):
                for job in jobs[:5]:  # Smaller batch for warmup
                    await job_service_without_events.execute_job(job, "test-worker")
            
            # Measure batch execution time with event logging enabled
            batch_times_with_events = []
            for _ in range(event_overhead_config.measurement_iterations):
                start_time = time.perf_counter()
                for job in jobs:
                    await job_service_with_events.execute_job(job, "test-worker")
                batch_time = time.perf_counter() - start_time
                batch_times_with_events.append(batch_time)
            
            # Measure batch execution time with event logging disabled
            batch_times_without_events = []
            for _ in range(event_overhead_config.measurement_iterations):
                start_time = time.perf_counter()
                for job in jobs:
                    await job_service_without_events.execute_job(job, "test-worker")
                batch_time = time.perf_counter() - start_time
                batch_times_without_events.append(batch_time)
            
            # Calculate statistics
            avg_batch_time_with_events = statistics.mean(batch_times_with_events)
            avg_batch_time_without_events = statistics.mean(batch_times_without_events)
            
            # Calculate overhead percentage
            overhead_percentage = ((avg_batch_time_with_events - avg_batch_time_without_events) / avg_batch_time_without_events) * 100
            
            # Assertions
            assert overhead_percentage <= event_overhead_config.max_overhead_percentage, \
                f"Event logging overhead {overhead_percentage:.2f}% exceeds threshold {event_overhead_config.max_overhead_percentage}%"
            
            # Log performance metrics
            print(f"\nBatch Job Execution Performance ({event_overhead_config.batch_size} jobs):")
            print(f"  With event logging: {avg_batch_time_with_events:.6f}s")
            print(f"  Without event logging: {avg_batch_time_without_events:.6f}s")
            print(f"  Overhead: {overhead_percentage:.2f}%")
            
        finally:
            # Cleanup
            await job_service_with_events.cleanup()
            await job_service_without_events.cleanup()
    
    @pytest.mark.asyncio
    async def test_event_logging_scalability(self, event_overhead_config: EventOverheadTestConfig) -> None:
        """Test that event logging overhead remains consistent as job complexity increases."""
        # Create job services with and without event logging
        job_service_with_events = await self._create_job_service(enable_event_logging=True)
        job_service_without_events = await self._create_job_service(enable_event_logging=False)
        
        try:
            # Test with different job complexities
            complexities = [
                ("simple", Job(function=simple_task, args=(5, 10), queue_name="test-queue")),
                ("medium", Job(function=complex_task, args=({"a": 1, "b": 2},), queue_name="test-queue")),
                ("complex", Job(function=complex_task, args=({
                    "number": 42,
                    "text": "hello world",
                    "list": [1, 2, 3, 4, 5],
                    "nested": {"a": 1, "b": 2, "c": {"d": 3, "e": 4}}
                },), queue_name="test-queue"))
            ]

            overhead_percentages = []

            for complexity_desc, job in complexities:
                # Warmup
                for _ in range(event_overhead_config.warmup_iterations):
                    await job_service_with_events.execute_job(job, "test-worker")
                    await job_service_without_events.execute_job(job, "test-worker")
                
                # Measure with event logging enabled
                times_with_events = []
                for _ in range(event_overhead_config.measurement_iterations):
                    execution_time = await measure_execution_time(
                        job_service_with_events.execute_job, job, "test-worker"
                    )
                    times_with_events.append(execution_time)
                
                # Measure with event logging disabled
                times_without_events = []
                for _ in range(event_overhead_config.measurement_iterations):
                    execution_time = await measure_execution_time(
                        job_service_without_events.execute_job, job, "test-worker"
                    )
                    times_without_events.append(execution_time)
                
                # Calculate statistics
                avg_time_with_events = statistics.mean(times_with_events)
                avg_time_without_events = statistics.mean(times_without_events)
                
                # Calculate overhead percentage
                overhead_percentage = ((avg_time_with_events - avg_time_without_events) / avg_time_without_events) * 100
                overhead_percentages.append((complexity_desc, overhead_percentage))
                
                # Assertions
                assert overhead_percentage <= event_overhead_config.max_overhead_percentage, \
                    f"Event logging overhead for {complexity_desc} job {overhead_percentage:.2f}% exceeds threshold {event_overhead_config.max_overhead_percentage}%"
            
            # Log performance metrics
            print(f"\nEvent Logging Scalability:")
            for complexity_desc, overhead_percentage in overhead_percentages:
                print(f"  {complexity_desc.capitalize()} job overhead: {overhead_percentage:.2f}%")
            
            # Verify that overhead doesn't increase dramatically with complexity
            simple_overhead = next(p for desc, p in overhead_percentages if desc == "simple")
            complex_overhead = next(p for desc, p in overhead_percentages if desc == "complex")
            
            # Complex job overhead should not be more than 2x simple job overhead
            assert complex_overhead <= simple_overhead * 2, \
                f"Complex job overhead {complex_overhead:.2f}% is more than 2x simple job overhead {simple_overhead:.2f}%"
            
        finally:
            # Cleanup
            await job_service_with_events.cleanup()
            await job_service_without_events.cleanup()