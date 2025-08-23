"""
Unit tests for the types module.

This module tests the type definitions and utility classes in the types module.
"""

import pytest
from typing import Dict, List, Optional, Union
import msgspec

from naq.utils.types import (
    # Type aliases
    JobID, WorkerID, QueueName, StreamName, SubjectName, Timestamp, Duration, TTL,
    RetryDelayType, JobDependency, FunctionArgs, FunctionKwargs, ExceptionTypes,
    ConfigDict, NatsServers, NatsAuth, NatsTLS, QueueNames, SubjectNames,
    JobStatusData, EventData, WorkerStatusData, MetricsDict, StatsDict,
    SerializedData, DeserializedData, JobCallback, EventCallback, ErrorCallback,
    AsyncJobCallback, AsyncEventCallback, AsyncErrorCallback,
    
    # TypedDict classes
    JobInfo, WorkerInfo, QueueInfo, ConnectionInfo, JobFilter, WorkerFilter,
    QueueFilter, JobMetrics, WorkerMetrics, QueueMetrics, SystemMetrics,
    
    # msgspec.Struct classes
    ConnectionMetrics, JobTiming, WorkerTiming, QueueTiming,
    
    # Collection type aliases
    JobInfoList, WorkerInfoList, QueueInfoList, ConnectionInfoList,
    JobMetricsList, WorkerMetricsList, QueueMetricsList, SystemMetricsList,
    
    # Timing dict type aliases
    JobTimingDict, WorkerTimingDict, QueueTimingDict,
)


class TestTypeAliases:
    """Test type aliases."""
    
    def test_basic_type_aliases(self):
        """Test basic type aliases."""
        # These should not raise any type errors
        job_id: JobID = "test-job-123"
        worker_id: WorkerID = "test-worker-456"
        queue_name: QueueName = "test-queue"
        stream_name: StreamName = "test-stream"
        subject_name: SubjectName = "test.subject"
        timestamp: Timestamp = 1234567890.0
        duration: Duration = 1000.0
        ttl: TTL = 3600
        
        assert isinstance(job_id, str)
        assert isinstance(worker_id, str)
        assert isinstance(queue_name, str)
        assert isinstance(stream_name, str)
        assert isinstance(subject_name, str)
        assert isinstance(timestamp, float)
        assert isinstance(duration, float)
        assert isinstance(ttl, int)
    
    def test_retry_delay_type(self):
        """Test RetryDelayType type alias."""
        # Test with int
        retry_int: RetryDelayType = 5
        assert isinstance(retry_int, int)
        
        # Test with float
        retry_float: RetryDelayType = 5.5
        assert isinstance(retry_float, float)
        
        # Test with sequence of int
        retry_seq_int: RetryDelayType = [1, 2, 3, 4, 5]
        assert isinstance(retry_seq_int, list)
        assert all(isinstance(x, int) for x in retry_seq_int)
        
        # Test with sequence of float
        retry_seq_float: RetryDelayType = [1.1, 2.2, 3.3]
        assert isinstance(retry_seq_float, list)
        assert all(isinstance(x, float) for x in retry_seq_float)
        
        # Test with sequence of mixed int and float
        retry_seq_mixed: RetryDelayType = [1, 2.2, 3]
        assert isinstance(retry_seq_mixed, list)
        assert all(isinstance(x, (int, float)) for x in retry_seq_mixed)
    
    def test_other_type_aliases(self):
        """Test other type aliases."""
        # Test function arguments
        args: FunctionArgs = (1, 2, 3)
        kwargs: FunctionKwargs = {"key": "value"}
        
        # Test exception types
        exc_types: ExceptionTypes = (ValueError, TypeError)
        
        # Test configuration dictionaries
        config: ConfigDict = {"key": "value"}
        
        # Test NATS parameters
        servers: NatsServers = ["nats://localhost:4222"]
        auth: NatsAuth = {"user": "admin", "pass": "password"}
        tls: NatsTLS = {"cert_file": "/path/to/cert"}
        
        # Test collections
        queue_names: QueueNames = ["queue1", "queue2"]
        subject_names: SubjectNames = ["subject1", "subject2"]
        
        # Test data dictionaries
        job_data: JobStatusData = {"status": "completed"}
        event_data: EventData = {"event_type": "started"}
        worker_data: WorkerStatusData = {"status": "busy"}
        
        # Test metrics
        metrics: MetricsDict = {"count": 10, "avg": 5.5}
        stats: StatsDict = {"total": 100, "success_rate": "95%"}
        
        # Test serialized data
        serialized: SerializedData = b"serialized_data"
        deserialized: DeserializedData = {"key": "value"}
        
        # Verify types
        assert isinstance(args, tuple)
        assert isinstance(kwargs, dict)
        assert isinstance(exc_types, tuple) or exc_types is None
        assert isinstance(config, dict)
        assert isinstance(servers, list)
        assert isinstance(auth, dict) or auth is None
        assert isinstance(tls, dict) or tls is None
        assert isinstance(queue_names, list)
        assert isinstance(subject_names, list)
        assert isinstance(job_data, dict)
        assert isinstance(event_data, dict)
        assert isinstance(worker_data, dict)
        assert isinstance(metrics, dict)
        assert isinstance(stats, dict)
        assert isinstance(serialized, bytes)


class TestTypedDictClasses:
    """Test TypedDict classes."""
    
    def test_job_info(self):
        """Test JobInfo TypedDict."""
        job_info: JobInfo = {
            "job_id": "job-123",
            "queue_name": "test-queue",
            "status": "completed",
            "created_at": 1234567890.0,
            "started_at": 1234567895.0,
            "finished_at": 1234567900.0,
            "error": None,
            "retry_count": 0
        }
        
        assert job_info["job_id"] == "job-123"
        assert job_info["queue_name"] == "test-queue"
        assert job_info["status"] == "completed"
        assert job_info["created_at"] == 1234567890.0
        assert job_info["started_at"] == 1234567895.0
        assert job_info["finished_at"] == 1234567900.0
        assert job_info["error"] is None
        assert job_info["retry_count"] == 0
    
    def test_worker_info(self):
        """Test WorkerInfo TypedDict."""
        worker_info: WorkerInfo = {
            "worker_id": "worker-123",
            "status": "busy",
            "queue_names": ["queue1", "queue2"],
            "last_heartbeat": 1234567890.0,
            "cpu_usage": 45.5,
            "memory_usage": 1024.0,
            "jobs_processed": 100,
            "jobs_failed": 5
        }
        
        assert worker_info["worker_id"] == "worker-123"
        assert worker_info["status"] == "busy"
        assert worker_info["queue_names"] == ["queue1", "queue2"]
        assert worker_info["last_heartbeat"] == 1234567890.0
        assert worker_info["cpu_usage"] == 45.5
        assert worker_info["memory_usage"] == 1024.0
        assert worker_info["jobs_processed"] == 100
        assert worker_info["jobs_failed"] == 5
    
    def test_queue_info(self):
        """Test QueueInfo TypedDict."""
        queue_info: QueueInfo = {
            "name": "test-queue",
            "stream_name": "test-stream",
            "subject": "naq.queue.test-queue",
            "pending_jobs": 10,
            "running_jobs": 2,
            "completed_jobs": 100,
            "failed_jobs": 5,
            "total_jobs": 117
        }
        
        assert queue_info["name"] == "test-queue"
        assert queue_info["stream_name"] == "test-stream"
        assert queue_info["subject"] == "naq.queue.test-queue"
        assert queue_info["pending_jobs"] == 10
        assert queue_info["running_jobs"] == 2
        assert queue_info["completed_jobs"] == 100
        assert queue_info["failed_jobs"] == 5
        assert queue_info["total_jobs"] == 117
    
    def test_connection_info(self):
        """Test ConnectionInfo TypedDict."""
        connection_info: ConnectionInfo = {
            "connected": True,
            "server_url": "nats://localhost:4222",
            "client_id": "client-123",
            "reconnects": 2,
            "last_error": None,
            "uptime_seconds": 3600.0
        }
        
        assert connection_info["connected"] is True
        assert connection_info["server_url"] == "nats://localhost:4222"
        assert connection_info["client_id"] == "client-123"
        assert connection_info["reconnects"] == 2
        assert connection_info["last_error"] is None
        assert connection_info["uptime_seconds"] == 3600.0
    
    def test_filter_typed_dicts(self):
        """Test filter TypedDict classes with optional fields."""
        # Test JobFilter with some fields
        job_filter: JobFilter = {
            "status": "completed",
            "queue_name": "test-queue",
            "limit": 10
        }
        
        assert job_filter["status"] == "completed"
        assert job_filter["queue_name"] == "test-queue"
        assert job_filter["limit"] == 10
        
        # Test WorkerFilter with no fields (empty dict)
        worker_filter: WorkerFilter = {}
        assert len(worker_filter) == 0
        
        # Test QueueFilter with all fields
        queue_filter: QueueFilter = {
            "name_pattern": "test-*",
            "has_jobs": True,
            "limit": 50,
            "offset": 0
        }
        
        assert queue_filter["name_pattern"] == "test-*"
        assert queue_filter["has_jobs"] is True
        assert queue_filter["limit"] == 50
        assert queue_filter["offset"] == 0
    
    def test_metrics_typed_dicts(self):
        """Test metrics TypedDict classes."""
        # Test JobMetrics
        job_metrics: JobMetrics = {
            "total_jobs": 100,
            "pending_jobs": 10,
            "running_jobs": 2,
            "completed_jobs": 85,
            "failed_jobs": 3,
            "retried_jobs": 5,
            "cancelled_jobs": 0,
            "avg_execution_time_ms": 1000.0,
            "max_execution_time_ms": 5000.0,
            "min_execution_time_ms": 100.0
        }
        
        assert job_metrics["total_jobs"] == 100
        assert job_metrics["avg_execution_time_ms"] == 1000.0
        
        # Test WorkerMetrics
        worker_metrics: WorkerMetrics = {
            "total_workers": 5,
            "active_workers": 4,
            "idle_workers": 1,
            "busy_workers": 3,
            "avg_cpu_usage": 45.5,
            "max_cpu_usage": 80.0,
            "avg_memory_usage": 1024.0,
            "max_memory_usage": 2048.0,
            "total_jobs_processed": 1000,
            "total_jobs_failed": 50
        }
        
        assert worker_metrics["total_workers"] == 5
        assert worker_metrics["avg_cpu_usage"] == 45.5
        
        # Test QueueMetrics
        queue_metrics: QueueMetrics = {
            "total_queues": 10,
            "total_jobs": 500,
            "avg_jobs_per_queue": 50.0,
            "max_jobs_per_queue": 200,
            "min_jobs_per_queue": 0,
            "empty_queues": 2,
            "non_empty_queues": 8
        }
        
        assert queue_metrics["total_queues"] == 10
        assert queue_metrics["avg_jobs_per_queue"] == 50.0
        
        # Test SystemMetrics
        system_metrics: SystemMetrics = {
            "uptime_seconds": 86400.0,
            "total_jobs": 1000,
            "total_workers": 10,
            "total_queues": 5,
            "jobs_per_second": 0.5,
            "avg_job_duration_ms": 1000.0,
            "system_load": 2.5,
            "memory_usage_mb": 4096.0,
            "disk_usage_mb": 10240.0
        }
        
        assert system_metrics["uptime_seconds"] == 86400.0
        assert system_metrics["jobs_per_second"] == 0.5


class TestMsgspecStructClasses:
    """Test msgspec.Struct classes."""
    
    def test_connection_metrics(self):
        """Test ConnectionMetrics struct."""
        metrics = ConnectionMetrics(
            connected=True,
            server_url="nats://localhost:4222",
            client_id="client-123",
            reconnects=2,
            last_error=None,
            uptime_seconds=3600.0,
            bytes_sent=1024,
            bytes_received=2048,
            messages_sent=100,
            messages_received=200,
            ping_rtt_ms=5.5
        )
        
        assert metrics.connected is True
        assert metrics.server_url == "nats://localhost:4222"
        assert metrics.client_id == "client-123"
        assert metrics.reconnects == 2
        assert metrics.last_error is None
        assert metrics.uptime_seconds == 3600.0
        assert metrics.bytes_sent == 1024
        assert metrics.bytes_received == 2048
        assert metrics.messages_sent == 100
        assert metrics.messages_received == 200
        assert metrics.ping_rtt_ms == 5.5
        
        # Test serialization
        serialized = msgspec.json.encode(metrics)
        deserialized = msgspec.json.decode(serialized, type=ConnectionMetrics)
        
        assert deserialized == metrics
    
    def test_job_timing(self):
        """Test JobTiming struct."""
        timing = JobTiming(
            job_id="job-123",
            created_at=1234567890.0,
            started_at=1234567895.0,
            finished_at=1234567900.0
        )
        
        assert timing.job_id == "job-123"
        assert timing.created_at == 1234567890.0
        assert timing.started_at == 1234567895.0
        assert timing.finished_at == 1234567900.0
        
        # Test properties
        assert timing.duration_ms == 5000.0  # 5 seconds in ms
        assert timing.wait_time_ms == 5000.0  # 5 seconds in ms
        
        # Test with missing times
        timing_no_start = JobTiming(
            job_id="job-456",
            created_at=1234567890.0
        )
        
        assert timing_no_start.duration_ms is None
        assert timing_no_start.wait_time_ms is None
        
        # Test serialization
        serialized = msgspec.json.encode(timing)
        deserialized = msgspec.json.decode(serialized, type=JobTiming)
        
        assert deserialized == timing
    
    def test_worker_timing(self):
        """Test WorkerTiming struct."""
        import time
        
        # Use current time for more realistic test
        current_time = time.time()
        start_time = current_time - 10  # 10 seconds ago
        heartbeat_time = current_time - 5  # 5 seconds ago
        
        timing = WorkerTiming(
            worker_id="worker-123",
            started_at=start_time,
            last_heartbeat=heartbeat_time,
            last_job_started=start_time + 2,
            last_job_completed=start_time + 8
        )
        
        assert timing.worker_id == "worker-123"
        assert timing.started_at == start_time
        assert timing.last_heartbeat == heartbeat_time
        assert timing.last_job_started == start_time + 2
        assert timing.last_job_completed == start_time + 8
        
        # Test properties
        assert timing.uptime_ms >= 9000.0  # At least 9 seconds in ms
        assert timing.uptime_ms <= 11000.0  # At most 11 seconds in ms
        assert timing.time_since_last_heartbeat_ms is not None
        
        # Test with minimal data
        timing_minimal = WorkerTiming(
            worker_id="worker-456",
            started_at=1234567890.0
        )
        
        assert timing_minimal.uptime_ms > 0
        assert timing_minimal.time_since_last_heartbeat_ms is None
        
        # Test serialization
        serialized = msgspec.json.encode(timing)
        deserialized = msgspec.json.decode(serialized, type=WorkerTiming)
        
        assert deserialized == timing
    
    def test_queue_timing(self):
        """Test QueueTiming struct."""
        timing = QueueTiming(
            queue_name="test-queue",
            created_at=1234567890.0,
            last_job_enqueued=1234567895.0,
            last_job_started=1234567900.0,
            last_job_completed=1234567905.0
        )
        
        assert timing.queue_name == "test-queue"
        assert timing.created_at == 1234567890.0
        assert timing.last_job_enqueued == 1234567895.0
        assert timing.last_job_started == 1234567900.0
        assert timing.last_job_completed == 1234567905.0
        
        # Test properties
        assert timing.uptime_ms > 0
        assert timing.time_since_last_activity_ms is not None
        
        # Test with minimal data
        timing_minimal = QueueTiming(
            queue_name="minimal-queue",
            created_at=1234567890.0
        )
        
        assert timing_minimal.uptime_ms > 0
        assert timing_minimal.time_since_last_activity_ms is None
        
        # Test serialization
        serialized = msgspec.json.encode(timing)
        deserialized = msgspec.json.decode(serialized, type=QueueTiming)
        
        assert deserialized == timing


class TestCollectionTypeAliases:
    """Test collection type aliases."""
    
    def test_collection_type_aliases(self):
        """Test collection type aliases."""
        # Test list type aliases
        job_info_list: JobInfoList = [
            {"job_id": "job-1", "queue_name": "queue1", "status": "completed",
             "created_at": 1234567890.0, "started_at": 1234567895.0,
             "finished_at": 1234567900.0, "error": None, "retry_count": 0}
        ]
        
        worker_info_list: WorkerInfoList = [
            {"worker_id": "worker-1", "status": "busy", "queue_names": ["queue1"],
             "last_heartbeat": 1234567890.0, "cpu_usage": 45.5, "memory_usage": 1024.0,
             "jobs_processed": 100, "jobs_failed": 5}
        ]
        
        queue_info_list: QueueInfoList = [
            {"name": "queue1", "stream_name": "stream1", "subject": "subject1",
             "pending_jobs": 10, "running_jobs": 2, "completed_jobs": 100,
             "failed_jobs": 5, "total_jobs": 117}
        ]
        
        connection_info_list: ConnectionInfoList = [
            {"connected": True, "server_url": "nats://localhost:4222",
             "client_id": "client-1", "reconnects": 0, "last_error": None,
             "uptime_seconds": 3600.0}
        ]
        
        # Verify types
        assert isinstance(job_info_list, list)
        assert isinstance(worker_info_list, list)
        assert isinstance(queue_info_list, list)
        assert isinstance(connection_info_list, list)
        
        # Test metrics list type aliases
        job_metrics_list: JobMetricsList = [
            {"total_jobs": 100, "pending_jobs": 10, "running_jobs": 2,
             "completed_jobs": 85, "failed_jobs": 3, "retried_jobs": 5,
             "cancelled_jobs": 0, "avg_execution_time_ms": 1000.0,
             "max_execution_time_ms": 5000.0, "min_execution_time_ms": 100.0}
        ]
        
        worker_metrics_list: WorkerMetricsList = [
            {"total_workers": 5, "active_workers": 4, "idle_workers": 1,
             "busy_workers": 3, "avg_cpu_usage": 45.5, "max_cpu_usage": 80.0,
             "avg_memory_usage": 1024.0, "max_memory_usage": 2048.0,
             "total_jobs_processed": 1000, "total_jobs_failed": 50}
        ]
        
        queue_metrics_list: QueueMetricsList = [
            {"total_queues": 10, "total_jobs": 500, "avg_jobs_per_queue": 50.0,
             "max_jobs_per_queue": 200, "min_jobs_per_queue": 0,
             "empty_queues": 2, "non_empty_queues": 8}
        ]
        
        system_metrics_list: SystemMetricsList = [
            {"uptime_seconds": 86400.0, "total_jobs": 1000, "total_workers": 10,
             "total_queues": 5, "jobs_per_second": 0.5, "avg_job_duration_ms": 1000.0,
             "system_load": 2.5, "memory_usage_mb": 4096.0, "disk_usage_mb": 10240.0}
        ]
        
        # Verify types
        assert isinstance(job_metrics_list, list)
        assert isinstance(worker_metrics_list, list)
        assert isinstance(queue_metrics_list, list)
        assert isinstance(system_metrics_list, list)


class TestTimingDictTypeAliases:
    """Test timing dict type aliases."""
    
    def test_timing_dict_type_aliases(self):
        """Test timing dict type aliases."""
        # Test JobTimingDict
        job_timing_dict: JobTimingDict = {
            "job_id": "job-123",
            "created_at": 1234567890.0,
            "started_at": 1234567895.0,
            "finished_at": 1234567900.0
        }
        
        # Test WorkerTimingDict
        worker_timing_dict: WorkerTimingDict = {
            "worker_id": "worker-123",
            "started_at": 1234567890.0,
            "last_heartbeat": 1234567900.0,
            "last_job_started": 1234567895.0,
            "last_job_completed": 1234567900.0
        }
        
        # Test QueueTimingDict
        queue_timing_dict: QueueTimingDict = {
            "queue_name": "test-queue",
            "created_at": 1234567890.0,
            "last_job_enqueued": 1234567895.0,
            "last_job_started": 1234567900.0,
            "last_job_completed": 1234567905.0
        }
        
        # Verify types
        assert isinstance(job_timing_dict, dict)
        assert isinstance(worker_timing_dict, dict)
        assert isinstance(queue_timing_dict, dict)
        
        # Verify content
        assert job_timing_dict["job_id"] == "job-123"
        assert worker_timing_dict["worker_id"] == "worker-123"
        assert queue_timing_dict["queue_name"] == "test-queue"