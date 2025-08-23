"""Tests for the common types module."""

import time
from typing import Any, Dict, List, Optional, Tuple, Union

import msgspec
import pytest

from naq.utils.types import (
    JSONValue,
    JSONDict,
    JSONList,
    SyncCallable,
    AsyncCallable,
    AnyCallable,
    RetryDelayType,
    Timestamp,
    DurationSeconds,
    DurationMilliseconds,
    JobID,
    WorkerID,
    QueueName,
    StreamName,
    SubjectName,
    StatusValue,
    ErrorMessage,
    TracebackStr,
    ServerURL,
    ServerURLs,
    ConfigPath,
    NATSSequence,
    NATSSubject,
    SerializedData,
    SerializerType,
    JobMetadata,
    WorkerMetadata,
    ConnectionMetrics,
    EventMetadata,
    PointInTime,
    ResourceUsage,
    RetryConfig,
    QueueStats,
    JobCallback,
    WorkerCallback,
    EventCallback,
    ErrorCallback,
    JobDict,
    WorkerDict,
    EventDict,
    ConfigDict,
    JobIDs,
    WorkerIDs,
    QueueNames,
    Subjects,
    OptionalJobID,
    OptionalWorkerID,
    OptionalQueueName,
    OptionalTimestamp,
    JobToResult,
    JobToError,
    WorkerToStatus,
    QueueToStats,
    JobStatusTuple,
    WorkerStatusTuple,
    EventTuple,
    JobFunction,
    JobArgs,
    JobKwargs,
    JobResultType,
    WorkerResultType,
    EventResultType,
)


class TestTypeAliases:
    """Test cases for type aliases to ensure they work correctly with type checkers."""

    def test_json_value_type(self):
        """Test that JSONValue type alias works with various JSON-compatible values."""
        # Test with valid JSON values
        valid_values: List[JSONValue] = [
            "string",
            42,
            3.14,
            True,
            False,
            None,
            {"key": "value"},
            [1, 2, 3],
        ]
        
        # Verify all values are accepted
        for value in valid_values:
            assert isinstance(value, (str, int, float, bool, type(None), dict, list))

    def test_json_dict_type(self):
        """Test that JSONDict type alias works with dictionary values."""
        # Test with valid JSON dict
        valid_dict: JSONDict = {
            "string": "value",
            "number": 42,
            "float": 3.14,
            "boolean": True,
            "null": None,
            "nested": {"key": "value"},
            "array": [1, 2, 3],
        }
        
        assert isinstance(valid_dict, dict)
        assert all(isinstance(k, str) for k in valid_dict.keys())

    def test_json_list_type(self):
        """Test that JSONList type alias works with list values."""
        # Test with valid JSON list
        valid_list: JSONList = [
            "string",
            42,
            3.14,
            True,
            False,
            None,
            {"key": "value"},
            [1, 2, 3],
        ]
        
        assert isinstance(valid_list, list)

    def test_callable_types(self):
        """Test that callable type aliases work correctly."""
        def sync_func(x: int) -> str:
            return str(x)
        
        async def async_func(x: int) -> str:
            return str(x)
        
        # Test with sync function
        sync_callable: SyncCallable = sync_func
        assert callable(sync_callable)
        
        # Test with async function
        async_callable: AsyncCallable = async_func
        assert callable(async_callable)
        
        # Test with any callable
        any_callable1: AnyCallable = sync_func
        any_callable2: AnyCallable = async_func
        assert callable(any_callable1)
        assert callable(any_callable2)

    def test_retry_delay_type(self):
        """Test that RetryDelayType works with various delay types."""
        # Test with int
        int_delay: RetryDelayType = 5
        assert isinstance(int_delay, int)
        
        # Test with float
        float_delay: RetryDelayType = 1.5
        assert isinstance(float_delay, float)
        
        # Test with sequence
        sequence_delay: RetryDelayType = [1, 2, 3]
        assert isinstance(sequence_delay, (list, tuple))
        
        # Test with tuple
        tuple_delay: RetryDelayType = (1.0, 2.5, 3.0)
        assert isinstance(tuple_delay, (list, tuple))

    def test_time_types(self):
        """Test that time-related type aliases work correctly."""
        # Test timestamp
        timestamp: Timestamp = time.time()
        assert isinstance(timestamp, float)
        
        # Test duration seconds
        duration_seconds: DurationSeconds = 30.5
        assert isinstance(duration_seconds, float)
        
        # Test duration milliseconds
        duration_milliseconds: DurationMilliseconds = 30500.0
        assert isinstance(duration_milliseconds, float)

    def test_id_types(self):
        """Test that ID-related type aliases work correctly."""
        # Test job ID
        job_id: JobID = "job-123"
        assert isinstance(job_id, str)
        
        # Test worker ID
        worker_id: WorkerID = "worker-456"
        assert isinstance(worker_id, str)
        
        # Test queue name
        queue_name: QueueName = "default_queue"
        assert isinstance(queue_name, str)
        
        # Test stream name
        stream_name: StreamName = "job_stream"
        assert isinstance(stream_name, str)
        
        # Test subject name
        subject_name: SubjectName = "job.subject"
        assert isinstance(subject_name, str)

    def test_status_types(self):
        """Test that status-related type aliases work correctly."""
        # Test status value
        status: StatusValue = "completed"
        assert isinstance(status, str)
        
        # Test error message
        error: ErrorMessage = "Something went wrong"
        assert isinstance(error, str)
        
        # Test traceback string
        traceback: TracebackStr = "Traceback (most recent call last):\n..."
        assert isinstance(traceback, str)

    def test_config_types(self):
        """Test that configuration-related type aliases work correctly."""
        # Test server URL
        server_url: ServerURL = "nats://localhost:4222"
        assert isinstance(server_url, str)
        
        # Test server URLs
        server_urls: ServerURLs = ["nats://localhost:4222", "nats://remote:4222"]
        assert isinstance(server_urls, list)
        assert all(isinstance(url, str) for url in server_urls)
        
        # Test config path
        config_path: ConfigPath = ["nats", "servers"]
        assert isinstance(config_path, list)
        assert all(isinstance(item, str) for item in config_path)

    def test_nats_types(self):
        """Test that NATS-related type aliases work correctly."""
        # Test NATS sequence
        nats_sequence: NATSSequence = 12345
        assert isinstance(nats_sequence, int)
        
        # Test NATS subject
        nats_subject: NATSSubject = "naq.jobs.default"
        assert isinstance(nats_subject, str)

    def test_serialization_types(self):
        """Test that serialization-related type aliases work correctly."""
        # Test serialized data
        serialized_data: SerializedData = b"serialized_data"
        assert isinstance(serialized_data, bytes)
        
        # Test serializer type
        serializer_type: SerializerType = "pickle"
        assert isinstance(serializer_type, str)
        assert serializer_type in ("pickle", "json")


class TestTypedDictStructures:
    """Test cases for TypedDict structures."""

    def test_job_metadata(self):
        """Test that JobMetadata TypedDict works correctly."""
        metadata: JobMetadata = {
            "job_id": "job-123",
            "queue_name": "default",
            "enqueue_time": time.time(),
            "status": "pending",
            "worker_id": "worker-456",
            "retry_count": 0,
            "max_retries": 3,
            "timeout": 60,
            "depends_on": ["job-122", "job-121"],
        }
        
        assert isinstance(metadata, dict)
        assert "job_id" in metadata
        assert "queue_name" in metadata
        assert "enqueue_time" in metadata
        assert "status" in metadata

    def test_worker_metadata(self):
        """Test that WorkerMetadata TypedDict works correctly."""
        metadata: WorkerMetadata = {
            "worker_id": "worker-456",
            "queue_names": ["default", "high_priority"],
            "status": "idle",
            "last_heartbeat": time.time(),
            "cpu_usage": 45.5,
            "memory_usage": 60.2,
        }
        
        assert isinstance(metadata, dict)
        assert "worker_id" in metadata
        assert "queue_names" in metadata
        assert "status" in metadata
        assert "last_heartbeat" in metadata

    def test_connection_metrics(self):
        """Test that ConnectionMetrics TypedDict works correctly."""
        metrics: ConnectionMetrics = {
            "connection_count": 5,
            "total_connections": 100,
            "active_connections": 3,
            "failed_connections": 2,
            "reconnect_count": 10,
            "last_error": "Connection timeout",
        }
        
        assert isinstance(metrics, dict)
        assert "connection_count" in metrics
        assert "total_connections" in metrics
        assert "active_connections" in metrics

    def test_event_metadata(self):
        """Test that EventMetadata TypedDict works correctly."""
        metadata: EventMetadata = {
            "event_type": "job_started",
            "timestamp": time.time(),
            "source": "worker-456",
            "version": "1.0",
            "serializer": "pickle",
        }
        
        assert isinstance(metadata, dict)
        assert "event_type" in metadata
        assert "timestamp" in metadata
        assert "source" in metadata


class TestDataClasses:
    """Test cases for msgspec.Struct data classes."""

    def test_point_in_time(self):
        """Test that PointInTime data class works correctly."""
        # Test with default values
        point = PointInTime()
        assert isinstance(point.timestamp, float)
        assert point.metadata is None
        
        # Test with custom values
        custom_time = time.time()
        custom_metadata = {"key": "value"}
        point = PointInTime(timestamp=custom_time, metadata=custom_metadata)
        assert point.timestamp == custom_time
        assert point.metadata == custom_metadata
        
        # Test serialization
        serialized = msgspec.json.encode(point)
        deserialized = msgspec.json.decode(serialized, type=PointInTime)
        assert deserialized.timestamp == point.timestamp
        assert deserialized.metadata == point.metadata

    def test_resource_usage(self):
        """Test that ResourceUsage data class works correctly."""
        # Test with default values
        usage = ResourceUsage()
        assert usage.cpu_percent is None
        assert usage.memory_percent is None
        assert usage.memory_bytes is None
        assert usage.custom_metrics is None
        
        # Test with custom values
        custom_usage = ResourceUsage(
            cpu_percent=45.5,
            memory_percent=60.2,
            memory_bytes=1024 * 1024 * 512,  # 512 MB
            custom_metrics={"custom1": 100, "custom2": "value"},
        )
        assert custom_usage.cpu_percent == 45.5
        assert custom_usage.memory_percent == 60.2
        assert custom_usage.memory_bytes == 1024 * 1024 * 512
        assert custom_usage.custom_metrics == {"custom1": 100, "custom2": "value"}
        
        # Test serialization
        serialized = msgspec.json.encode(custom_usage)
        deserialized = msgspec.json.decode(serialized, type=ResourceUsage)
        assert deserialized.cpu_percent == custom_usage.cpu_percent
        assert deserialized.memory_percent == custom_usage.memory_percent
        assert deserialized.memory_bytes == custom_usage.memory_bytes
        assert deserialized.custom_metrics == custom_usage.custom_metrics

    def test_retry_config(self):
        """Test that RetryConfig data class works correctly."""
        # Test with default values
        config = RetryConfig()
        assert config.max_attempts == 3
        assert config.delay == 1.0
        assert config.backoff_factor == 2.0
        assert config.jitter is True
        assert config.retry_on_exception_names == ("Exception",)
        
        # Test with custom values
        custom_config = RetryConfig(
            max_attempts=5,
            delay=2.0,
            backoff_factor=1.5,
            jitter=False,
            retry_on_exception_names=("ValueError", "TypeError"),
        )
        assert custom_config.max_attempts == 5
        assert custom_config.delay == 2.0
        assert custom_config.backoff_factor == 1.5
        assert custom_config.jitter is False
        assert custom_config.retry_on_exception_names == ("ValueError", "TypeError")
        
        # Test serialization
        serialized = msgspec.json.encode(custom_config)
        deserialized = msgspec.json.decode(serialized, type=RetryConfig)
        assert deserialized.max_attempts == custom_config.max_attempts
        assert deserialized.delay == custom_config.delay
        assert deserialized.backoff_factor == custom_config.backoff_factor
        assert deserialized.jitter == custom_config.jitter
        assert deserialized.retry_on_exception_names == custom_config.retry_on_exception_names

    def test_queue_stats(self):
        """Test that QueueStats data class works correctly."""
        # Test with default values
        stats = QueueStats(queue_name="test_queue")
        assert stats.queue_name == "test_queue"
        assert stats.pending_jobs == 0
        assert stats.running_jobs == 0
        assert stats.completed_jobs == 0
        assert stats.failed_jobs == 0
        assert stats.total_jobs == 0
        assert isinstance(stats.last_updated, float)
        
        # Test with custom values
        custom_stats = QueueStats(
            queue_name="custom_queue",
            pending_jobs=10,
            running_jobs=2,
            completed_jobs=50,
            failed_jobs=3,
            total_jobs=65,
            last_updated=time.time(),
        )
        assert custom_stats.queue_name == "custom_queue"
        assert custom_stats.pending_jobs == 10
        assert custom_stats.running_jobs == 2
        assert custom_stats.completed_jobs == 50
        assert custom_stats.failed_jobs == 3
        assert custom_stats.total_jobs == 65
        
        # Test serialization
        serialized = msgspec.json.encode(custom_stats)
        deserialized = msgspec.json.decode(serialized, type=QueueStats)
        assert deserialized.queue_name == custom_stats.queue_name
        assert deserialized.pending_jobs == custom_stats.pending_jobs
        assert deserialized.running_jobs == custom_stats.running_jobs
        assert deserialized.completed_jobs == custom_stats.completed_jobs
        assert deserialized.failed_jobs == custom_stats.failed_jobs
        assert deserialized.total_jobs == custom_stats.total_jobs


class TestCallbackTypes:
    """Test cases for callback type aliases."""

    def test_job_callback(self):
        """Test that JobCallback type alias works correctly."""
        def job_callback(job_id: str, status: str, result: Any) -> None:
            pass
        
        callback: JobCallback = job_callback
        assert callable(callback)

    def test_worker_callback(self):
        """Test that WorkerCallback type alias works correctly."""
        def worker_callback(worker_id: str, status: str, metadata: Optional[Dict[str, Any]]) -> None:
            pass
        
        callback: WorkerCallback = worker_callback
        assert callable(callback)

    def test_event_callback(self):
        """Test that EventCallback type alias works correctly."""
        def event_callback(event_type: str, metadata: Dict[str, Any], data: Optional[Dict[str, Any]]) -> None:
            pass
        
        callback: EventCallback = event_callback
        assert callable(callback)

    def test_error_callback(self):
        """Test that ErrorCallback type alias works correctly."""
        def error_callback(exception: Exception, context: Optional[Dict[str, Any]]) -> None:
            pass
        
        callback: ErrorCallback = error_callback
        assert callable(callback)


class TestDataStructureTypes:
    """Test cases for data structure type aliases."""

    def test_dict_types(self):
        """Test that dictionary type aliases work correctly."""
        # Test job dict
        job_dict: JobDict = {"job_id": "job-123", "status": "pending"}
        assert isinstance(job_dict, dict)
        
        # Test worker dict
        worker_dict: WorkerDict = {"worker_id": "worker-456", "status": "idle"}
        assert isinstance(worker_dict, dict)
        
        # Test event dict
        event_dict: EventDict = {"event_type": "job_started", "timestamp": time.time()}
        assert isinstance(event_dict, dict)
        
        # Test config dict
        config_dict: ConfigDict = {"key": "value", "number": 42}
        assert isinstance(config_dict, dict)

    def test_sequence_types(self):
        """Test that sequence type aliases work correctly."""
        # Test job IDs
        job_ids: JobIDs = ["job-1", "job-2", "job-3"]
        assert isinstance(job_ids, list)
        assert all(isinstance(job_id, str) for job_id in job_ids)
        
        # Test worker IDs
        worker_ids: WorkerIDs = ["worker-1", "worker-2"]
        assert isinstance(worker_ids, list)
        assert all(isinstance(worker_id, str) for worker_id in worker_ids)
        
        # Test queue names
        queue_names: QueueNames = ["queue1", "queue2"]
        assert isinstance(queue_names, list)
        assert all(isinstance(queue_name, str) for queue_name in queue_names)
        
        # Test subjects
        subjects: Subjects = ["subject1", "subject2"]
        assert isinstance(subjects, list)
        assert all(isinstance(subject, str) for subject in subjects)

    def test_optional_types(self):
        """Test that optional type aliases work correctly."""
        # Test optional job ID
        optional_job_id: OptionalJobID = "job-123"
        assert isinstance(optional_job_id, str)
        
        optional_job_id_none: OptionalJobID = None
        assert optional_job_id_none is None
        
        # Test optional worker ID
        optional_worker_id: OptionalWorkerID = "worker-456"
        assert isinstance(optional_worker_id, str)
        
        optional_worker_id_none: OptionalWorkerID = None
        assert optional_worker_id_none is None
        
        # Test optional queue name
        optional_queue_name: OptionalQueueName = "default"
        assert isinstance(optional_queue_name, str)
        
        optional_queue_name_none: OptionalQueueName = None
        assert optional_queue_name_none is None
        
        # Test optional timestamp
        optional_timestamp: OptionalTimestamp = time.time()
        assert isinstance(optional_timestamp, float)
        
        optional_timestamp_none: OptionalTimestamp = None
        assert optional_timestamp_none is None

    def test_mapping_types(self):
        """Test that mapping type aliases work correctly."""
        # Test job to result
        job_to_result: JobToResult = {"job-1": "result1", "job-2": "result2"}
        assert isinstance(job_to_result, dict)
        assert all(isinstance(job_id, str) for job_id in job_to_result.keys())
        
        # Test job to error
        job_to_error: JobToError = {"job-1": "error1", "job-2": "error2"}
        assert isinstance(job_to_error, dict)
        assert all(isinstance(job_id, str) for job_id in job_to_error.keys())
        assert all(isinstance(error, str) for error in job_to_error.values())
        
        # Test worker to status
        worker_to_status: WorkerToStatus = {"worker-1": "idle", "worker-2": "busy"}
        assert isinstance(worker_to_status, dict)
        assert all(isinstance(worker_id, str) for worker_id in worker_to_status.keys())
        assert all(isinstance(status, str) for status in worker_to_status.values())
        
        # Test queue to stats
        queue_to_stats: QueueToStats = {
            "queue1": QueueStats(queue_name="queue1"),
            "queue2": QueueStats(queue_name="queue2"),
        }
        assert isinstance(queue_to_stats, dict)
        assert all(isinstance(queue_name, str) for queue_name in queue_to_stats.keys())
        assert all(isinstance(stats, QueueStats) for stats in queue_to_stats.values())

    def test_tuple_types(self):
        """Test that tuple type aliases work correctly."""
        # Test job status tuple
        job_status_tuple: JobStatusTuple = ("job-1", "running", time.time())
        assert isinstance(job_status_tuple, tuple)
        assert len(job_status_tuple) == 3
        assert isinstance(job_status_tuple[0], str)
        assert isinstance(job_status_tuple[1], str)
        assert isinstance(job_status_tuple[2], float)
        
        # Test worker status tuple
        worker_status_tuple: WorkerStatusTuple = ("worker-1", "idle", time.time())
        assert isinstance(worker_status_tuple, tuple)
        assert len(worker_status_tuple) == 3
        assert isinstance(worker_status_tuple[0], str)
        assert isinstance(worker_status_tuple[1], str)
        assert isinstance(worker_status_tuple[2], float)
        
        # Test event tuple
        event_tuple: EventTuple = ("job_started", "worker-1", time.time())
        assert isinstance(event_tuple, tuple)
        assert len(event_tuple) == 3
        assert isinstance(event_tuple[0], str)
        assert isinstance(event_tuple[1], str)
        assert isinstance(event_tuple[2], float)

    def test_function_parameter_types(self):
        """Test that function parameter type aliases work correctly."""
        # Test job function
        def job_function(x: int) -> str:
            return str(x)
        
        job_func: JobFunction = job_function
        assert callable(job_func)
        
        # Test job args
        job_args: JobArgs = (1, 2, 3)
        assert isinstance(job_args, tuple)
        
        # Test job kwargs
        job_kwargs: JobKwargs = {"key": "value", "number": 42}
        assert isinstance(job_kwargs, dict)

    def test_return_types(self):
        """Test that return type aliases work correctly."""
        # Test job result type
        job_result: JobResultType = "job completed"
        assert isinstance(job_result, (str, int, float, bool, type(None), dict, list))
        
        # Test worker result type
        worker_result: WorkerResultType = {"status": "completed", "result": "success"}
        assert isinstance(worker_result, dict)
        
        # Test event result type
        event_result: EventResultType = {"event_type": "job_completed", "timestamp": time.time()}
        assert isinstance(event_result, dict)


class TestTypeChecking:
    """Test cases to verify type checking works correctly."""

    def test_type_annotations(self):
        """Test that type annotations are correctly interpreted."""
        # This test ensures that type annotations are properly defined
        # and can be used with type checkers like mypy
        
        # Test with a function that uses various types from the module
        def test_function(
            job_id: JobID,
            worker_id: WorkerID,
            queue_name: QueueName,
            timestamp: Timestamp,
            status: StatusValue,
            metadata: JobMetadata,
            retry_config: RetryConfig,
        ) -> JobResultType:
            return {
                "job_id": job_id,
                "worker_id": worker_id,
                "queue_name": queue_name,
                "timestamp": timestamp,
                "status": status,
                "metadata": metadata,
                "retry_config": retry_config,
            }
        
        # Call the function with valid arguments
        result = test_function(
            job_id="job-123",
            worker_id="worker-456",
            queue_name="default",
            timestamp=time.time(),
            status="completed",
            metadata={
                "job_id": "job-123",
                "queue_name": "default",
                "enqueue_time": time.time(),
                "status": "completed",
                "worker_id": "worker-456",
                "retry_count": 0,
                "max_retries": 3,
                "timeout": 60,
                "depends_on": None,
            },
            retry_config=RetryConfig(max_attempts=3, delay=1.0),
        )
        
        assert isinstance(result, dict)
        assert "job_id" in result
        assert "worker_id" in result
        assert "queue_name" in result
        assert "timestamp" in result
        assert "status" in result
        assert "metadata" in result
        assert "retry_config" in result

    def test_msgspec_struct_serialization(self):
        """Test that msgspec.Struct classes can be serialized and deserialized."""
        # Create instances of all data classes
        point_in_time = PointInTime(metadata={"key": "value"})
        resource_usage = ResourceUsage(cpu_percent=50.0, memory_percent=60.0)
        retry_config = RetryConfig(max_attempts=5, delay=2.0)
        queue_stats = QueueStats(queue_name="test", pending_jobs=10)
        
        # Test serialization and deserialization for each
        for struct in [point_in_time, resource_usage, retry_config, queue_stats]:
            # JSON serialization
            json_encoded = msgspec.json.encode(struct)
            json_decoded = msgspec.json.decode(json_encoded, type=type(struct))
            assert json_decoded == struct
            
            # MessagePack serialization
            msgpack_encoded = msgspec.msgpack.encode(struct)
            msgpack_decoded = msgspec.msgpack.decode(msgpack_encoded, type=type(struct))
            assert msgpack_decoded == struct