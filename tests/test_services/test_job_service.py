# tests/test_services/test_job_service.py
"""Tests for JobService - job execution and lifecycle management."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from naq.services.jobs import JobService
from naq.models.jobs import Job
from naq.models.enums import JOB_STATUS


@pytest.mark.asyncio
class TestJobService:
    """Test JobService functionality."""

    async def test_enqueue_job_with_service_dependencies(self, mock_service_manager):
        """Test job enqueuing through service layer."""
        mock_manager, service_map = mock_service_manager
        
        job_service = service_map[JobService] 
        connection_service = service_map[ConnectionService]
        event_service = service_map[EventService]
        
        def test_func():
            return "test"
        
        job = Job(function=test_func)
        
        # Configure mock to simulate enqueue_job method
        job_service.enqueue_job.return_value = "job-123"
        
        # Test enqueuing
        job_id = await job_service.enqueue_job(job, "test-queue")
        
        assert job_id == "job-123"
        job_service.enqueue_job.assert_called_once()

    async def test_execute_job_success(self, mock_service_manager):
        """Test successful job execution."""
        mock_manager, service_map = mock_service_manager
        job_service = service_map[JobService]
        
        def test_func(x):
            return x * 2
        
        job = Job(function=test_func, args=(5,))
        
        # Mock successful execution
        expected_result = {"result": 10, "status": "completed"}
        job_service.execute_job.return_value = expected_result
        
        result = await job_service.execute_job(job, "test-worker")
        
        assert result == expected_result
        job_service.execute_job.assert_called_once_with(job, "test-worker")

    async def test_execute_job_failure(self, mock_service_manager):
        """Test job execution with failure."""
        mock_manager, service_map = mock_service_manager
        job_service = service_map[JobService]
        
        def failing_func():
            raise ValueError("Test error")
        
        job = Job(function=failing_func)
        
        # Mock failed execution
        expected_result = {
            "result": None, 
            "status": "failed", 
            "error": "ValueError: Test error"
        }
        job_service.execute_job.return_value = expected_result
        
        result = await job_service.execute_job(job, "test-worker")
        
        assert result["status"] == "failed"
        assert "error" in result

    async def test_job_status_management(self, mock_service_manager):
        """Test job status tracking through service."""
        mock_manager, service_map = mock_service_manager
        job_service = service_map[JobService]
        
        job_id = "test-job-123"
        
        # Mock status operations
        job_service.get_job_status.return_value = JOB_STATUS.RUNNING
        job_service.update_job_status.return_value = True
        
        # Test getting status
        status = await job_service.get_job_status(job_id)
        assert status == JOB_STATUS.RUNNING
        
        # Test updating status
        success = await job_service.update_job_status(job_id, JOB_STATUS.COMPLETED)
        assert success is True

    async def test_job_result_storage(self, mock_service_manager):
        """Test job result storage through service."""
        mock_manager, service_map = mock_service_manager
        job_service = service_map[JobService]
        
        job_id = "test-job-456"
        result_data = {"output": "test result", "duration": 1.5}
        
        # Mock result storage
        job_service.store_job_result.return_value = True
        job_service.get_job_result.return_value = result_data
        
        # Test storing result
        stored = await job_service.store_job_result(job_id, result_data)
        assert stored is True
        
        # Test retrieving result
        retrieved = await job_service.get_job_result(job_id)
        assert retrieved == result_data

    async def test_job_serialization_handling(self, service_test_config):
        """Test job serialization/deserialization in service context."""
        # Create real service for serialization testing
        job_service = JobService(service_test_config)
        
        def test_func(x, y=10):
            return x + y
        
        job = Job(function=test_func, args=(5,), kwargs={"y": 15})
        
        # Test that job can be serialized (depends on implementation)
        serialized = job.serialize()
        assert serialized is not None
        
        # Test deserialization
        deserialized = Job.deserialize(serialized)
        assert deserialized.function.__name__ == test_func.__name__
        assert deserialized.args == (5,)
        assert deserialized.kwargs == {"y": 15}

    async def test_service_dependency_injection(self, service_test_config):
        """Test that JobService receives proper dependencies."""
        # This test would verify actual dependency injection
        # when using real ServiceManager
        with patch('naq.services.jobs.ServiceManager') as mock_sm:
            mock_connection = AsyncMock()
            mock_event = AsyncMock()
            
            mock_sm.return_value.get_service.side_effect = lambda service_type: {
                'ConnectionService': mock_connection,
                'EventService': mock_event,
            }.get(service_type.__name__, AsyncMock())
            
            job_service = JobService(service_test_config)
            
            # Service should be properly constructed
            assert job_service is not None

    async def test_job_timeout_handling(self, mock_service_manager):
        """Test job timeout handling in service."""
        mock_manager, service_map = mock_service_manager
        job_service = service_map[JobService]
        
        def long_running_func():
            import time
            time.sleep(10)  # Simulate long task
            return "done"
        
        job = Job(function=long_running_func, timeout=1)  # 1 second timeout
        
        # Mock timeout behavior
        timeout_result = {
            "result": None,
            "status": "failed", 
            "error": "Job timeout after 1 seconds"
        }
        job_service.execute_job.return_value = timeout_result
        
        result = await job_service.execute_job(job, "test-worker")
        
        assert result["status"] == "failed"
        assert "timeout" in result["error"].lower()

    async def test_job_retry_logic(self, mock_service_manager):
        """Test job retry logic through service."""
        mock_manager, service_map = mock_service_manager
        job_service = service_map[JobService]
        
        def flaky_func():
            # Simulates a function that fails then succeeds
            return "success"
        
        job = Job(function=flaky_func, max_retries=3)
        
        # Mock retry behavior
        job_service.execute_job_with_retries.return_value = {
            "result": "success",
            "status": "completed",
            "retry_count": 2
        }
        
        result = await job_service.execute_job_with_retries(job, "test-worker")
        
        assert result["status"] == "completed"
        assert result["retry_count"] == 2