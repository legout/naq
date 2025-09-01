"""
Tests for the Results module.

This module contains comprehensive tests for the Results class, including
NATS KV store integration, msgspec.Struct serialization, and error handling.
"""

import asyncio
import json
import msgspec
import pytest
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union
from unittest.mock import AsyncMock, MagicMock, patch

from naq.exceptions import JobNotFoundError, NaqException
from naq.models.enums import JOB_STATUS
from naq.models.jobs import JobResult
from naq.results import Results


class TestResults:
    """Test cases for Results class."""

    @pytest.fixture
    def mock_kv_store(self) -> AsyncMock:
        """Create a mock NATS KV store."""
        mock_kv = AsyncMock()
        mock_kv.get = AsyncMock()
        mock_kv.put = AsyncMock()
        mock_kv.delete = AsyncMock()
        mock_kv.keys = AsyncMock()
        return mock_kv

    @pytest.fixture
    def mock_nats_kv_store(self, mock_kv_store: AsyncMock) -> MagicMock:
        """Create a mock nats_kv_store context manager."""
        mock_context = MagicMock()
        mock_context.__aenter__.return_value = mock_kv_store
        mock_context.__aexit__.return_value = None
        return mock_context

    @pytest.fixture
    def results(self) -> Results:
        """Create a Results instance."""
        return Results()

    @pytest.fixture
    def sample_job_result(self) -> JobResult:
        """Create a sample job result for testing."""
        return JobResult(
            job_id="test-job-123",
            status=JOB_STATUS.COMPLETED.value,
            result={"data": "test result", "count": 42},
            start_time=(datetime.now() - timedelta(seconds=5)).timestamp(),
            finish_time=datetime.now().timestamp(),
        )

    @pytest.fixture
    def sample_failed_job_result(self) -> JobResult:
        """Create a sample failed job result for testing."""
        return JobResult(
            job_id="test-job-456",
            status=JOB_STATUS.FAILED.value,
            result=None,
            error="Test error message",
            traceback="Test traceback",
            start_time=(datetime.now() - timedelta(seconds=3)).timestamp(),
            finish_time=datetime.now().timestamp(),
        )

    @pytest.fixture
    def sample_job_result_dict(self) -> Dict[str, Any]:
        """Create a sample job result as a dictionary for testing."""
        return {
            "job_id": "test-job-789",
            "status": JOB_STATUS.COMPLETED.value,
            "result": {"data": "dict result", "value": 100},
            "started_at": datetime.now().isoformat(),
            "completed_at": datetime.now().isoformat(),
            "worker_id": "worker-3",
            "queue_name": "test_queue",
            "retries": 1,
        }

    def test_init(self) -> None:
        """Test Results initialization."""
        # Test that Results can be initialized with default parameters
        results = Results()
        
        # Verify that the nats_url is set to the default
        # Note: DEFAULT_NATS_URL might be a list, so we check the first element
        default_url = results.nats_url
        if isinstance(default_url, list):
            default_url = default_url[0]
        assert default_url == "nats://localhost:4222"
        
        # Test that Results can be initialized with a custom URL
        custom_url = "nats://custom:4222"
        results_custom = Results(custom_url)
        custom_url_actual = results_custom.nats_url
        if isinstance(custom_url_actual, list):
            custom_url_actual = custom_url_actual[0]
        assert custom_url_actual == custom_url

    @pytest.mark.asyncio
    async def test_add_job_result_success(
        self,
        results: Results,
        mock_nats_kv_store: MagicMock,
        mock_kv_store: AsyncMock,
        sample_job_result: JobResult
    ) -> None:
        """Test successful storage of a job result."""
        # Setup mock to return a valid KV store
        with patch('naq.results.nats_kv_store', return_value=mock_nats_kv_store):
            # Setup mock for Job.serialize_result
            with patch('naq.results.Job.serialize_result', return_value=b'serialized_result'):
                # Call the method
                result_data = {
                    "status": sample_job_result.status,
                    "result": sample_job_result.result,
                    "error": sample_job_result.error,
                    "traceback": sample_job_result.traceback,
                    "start_time": sample_job_result.start_time,
                    "finish_time": sample_job_result.finish_time,
                }
                await results.add_job_result(sample_job_result.job_id, result_data)
                
                # Verify the storage call
                mock_kv_store.put.assert_called_once_with(sample_job_result.job_id, b'serialized_result', ttl=604800)

    @pytest.mark.asyncio
    async def test_add_job_result_with_ttl(
        self,
        results: Results,
        mock_nats_kv_store: MagicMock,
        mock_kv_store: AsyncMock,
        sample_job_result: JobResult
    ) -> None:
        """Test storage of a job result with custom TTL."""
        ttl = 1800  # 30 minutes
        
        # Setup mock to return a valid KV store
        with patch('naq.results.nats_kv_store', return_value=mock_nats_kv_store):
            # Setup mock for Job.serialize_result
            with patch('naq.results.Job.serialize_result', return_value=b'serialized_result'):
                # Call the method with custom TTL
                result_data = {
                    "status": sample_job_result.status,
                    "result": sample_job_result.result,
                }
                await results.add_job_result(sample_job_result.job_id, result_data, result_ttl=ttl)
                
                # Verify the storage call with custom TTL
                mock_kv_store.put.assert_called_once_with(sample_job_result.job_id, b'serialized_result', ttl=ttl)

    @pytest.mark.asyncio
    async def test_add_job_result_invalid_job_id(
        self,
        results: Results
    ) -> None:
        """Test handling of invalid job_id."""
        # Call the method with invalid job_id and expect exception
        with pytest.raises(ValueError, match="job_id must be a non-empty string"):
            await results.add_job_result("", {"status": "completed"})

    @pytest.mark.asyncio
    async def test_add_job_result_invalid_result_data(
        self,
        results: Results
    ) -> None:
        """Test handling of invalid result_data."""
        # Call the method with invalid result_data and expect exception
        with pytest.raises(ValueError, match="result_data must be a non-empty dictionary"):
            await results.add_job_result("test-job", None)

    @pytest.mark.asyncio
    async def test_add_job_result_missing_status(
        self,
        results: Results
    ) -> None:
        """Test handling of missing status field."""
        # Call the method with missing status and expect exception
        with pytest.raises(ValueError, match="result_data must contain a 'status' field"):
            await results.add_job_result("test-job", {"result": "test"})

    @pytest.mark.asyncio
    async def test_fetch_job_result_success(
        self,
        results: Results,
        mock_nats_kv_store: MagicMock,
        mock_kv_store: AsyncMock,
        sample_job_result: JobResult
    ) -> None:
        """Test successful retrieval of a job result."""
        # Setup mock KV entry
        mock_entry = MagicMock()
        mock_entry.value = b'serialized_result'
        
        # Setup mock to return the entry
        mock_kv_store.get.return_value = mock_entry
        
        # Setup mock to return a valid KV store
        with patch('naq.results.nats_kv_store', return_value=mock_nats_kv_store):
            # Setup mock for Job.deserialize_result
            with patch('naq.results.Job.deserialize_result', return_value={
                "status": sample_job_result.status,
                "result": sample_job_result.result,
                "error": sample_job_result.error,
                "traceback": sample_job_result.traceback,
                "start_time": sample_job_result.start_time,
                "finish_time": sample_job_result.finish_time,
            }):
                # Call the method
                result = await results.fetch_job_result(sample_job_result.job_id)
                
                # Verify the result
                assert result["job_id"] == sample_job_result.job_id
                assert result["status"] == sample_job_result.status
                assert result["result"] == sample_job_result.result
                
                # Verify the get call
                mock_kv_store.get.assert_called_once_with(sample_job_result.job_id)

    @pytest.mark.asyncio
    async def test_fetch_job_result_not_found(
        self,
        results: Results,
        mock_nats_kv_store: MagicMock,
        mock_kv_store: AsyncMock
    ) -> None:
        """Test retrieval of a non-existent job result."""
        from nats.js.errors import KeyNotFoundError
        
        # Setup mock to raise KeyNotFoundError
        mock_kv_store.get.side_effect = KeyNotFoundError
        
        # Setup mock to return a valid KV store
        with patch('naq.results.nats_kv_store', return_value=mock_nats_kv_store):
            # Call the method and expect exception
            with pytest.raises(JobNotFoundError, match="Result for job non-existent-job not found"):
                await results.fetch_job_result("non-existent-job")
            
            # Verify the get call
            mock_kv_store.get.assert_called_once_with("non-existent-job")

    @pytest.mark.asyncio
    async def test_fetch_job_result_invalid_job_id(
        self,
        results: Results
    ) -> None:
        """Test handling of invalid job_id."""
        # Call the method with invalid job_id and expect exception
        with pytest.raises(ValueError, match="job_id must be a non-empty string"):
            await results.fetch_job_result("")

    @pytest.mark.asyncio
    async def test_list_all_job_results_success(
        self,
        results: Results,
        mock_nats_kv_store: MagicMock,
        mock_kv_store: AsyncMock
    ) -> None:
        """Test successful listing of all job results."""
        # Setup mock to return job IDs
        job_ids = ["job1", "job2", "job3"]
        mock_kv_store.keys.return_value = job_ids
        
        # Setup mock to return a valid KV store
        with patch('naq.results.nats_kv_store', return_value=mock_nats_kv_store):
            # Call the method
            result = await results.list_all_job_results()
            
            # Verify the result
            assert result == job_ids
            
            # Verify the keys call
            mock_kv_store.keys.assert_called_once()

    @pytest.mark.asyncio
    async def test_purge_all_job_results_success(
        self,
        results: Results,
        mock_nats_kv_store: MagicMock,
        mock_kv_store: AsyncMock
    ) -> None:
        """Test successful purging of all job results."""
        # Setup mock to return job IDs
        job_ids = ["job1", "job2", "job3"]
        mock_kv_store.keys.return_value = job_ids
        
        # Setup mock to return a valid KV store
        with patch('naq.results.nats_kv_store', return_value=mock_nats_kv_store):
            # Call the method
            await results.purge_all_job_results()
            
            # Verify the keys call
            mock_kv_store.keys.assert_called_once()
            
            # Verify delete calls for each key
            assert mock_kv_store.delete.call_count == 3
            mock_kv_store.delete.assert_any_call("job1")
            mock_kv_store.delete.assert_any_call("job2")
            mock_kv_store.delete.assert_any_call("job3")

    @pytest.mark.asyncio
    async def test_delete_job_result_success(
        self,
        results: Results,
        mock_nats_kv_store: MagicMock,
        mock_kv_store: AsyncMock
    ) -> None:
        """Test successful deletion of a job result."""
        job_id = "test-job-to-delete"
        
        # Setup mock to return a valid KV store
        with patch('naq.results.nats_kv_store', return_value=mock_nats_kv_store):
            # Call the method
            await results.delete_job_result(job_id)
            
            # Verify the delete call
            mock_kv_store.delete.assert_called_once_with(job_id)

    @pytest.mark.asyncio
    async def test_delete_job_result_not_found(
        self,
        results: Results,
        mock_nats_kv_store: MagicMock,
        mock_kv_store: AsyncMock
    ) -> None:
        """Test deletion of a non-existent job result."""
        from nats.js.errors import KeyNotFoundError
        
        # Setup mock to raise KeyNotFoundError
        mock_kv_store.delete.side_effect = KeyNotFoundError
        
        # Setup mock to return a valid KV store
        with patch('naq.results.nats_kv_store', return_value=mock_nats_kv_store):
            # Call the method - should not raise an exception
            await results.delete_job_result("non-existent-job")
            
            # Verify the delete call was still made
            mock_kv_store.delete.assert_called_once_with("non-existent-job")

    @pytest.mark.asyncio
    async def test_delete_job_result_invalid_job_id(
        self,
        results: Results
    ) -> None:
        """Test handling of invalid job_id."""
        # Call the method with invalid job_id and expect exception
        with pytest.raises(ValueError, match="job_id must be a non-empty string"):
            await results.delete_job_result("")

    def test_job_result_serialization(self, sample_job_result: JobResult) -> None:
        """Test JobResult serialization and deserialization."""
        # Serialize the job result
        serialized = msgspec.json.encode(sample_job_result)
        
        # Deserialize the job result
        deserialized = msgspec.json.decode(serialized, type=JobResult)
        
        # Verify they are equal
        assert deserialized == sample_job_result

    def test_job_result_with_error_serialization(self, sample_failed_job_result: JobResult) -> None:
        """Test JobResult with error serialization and deserialization."""
        # Serialize the failed job result
        serialized = msgspec.json.encode(sample_failed_job_result)
        
        # Deserialize the failed job result
        deserialized = msgspec.json.decode(serialized, type=JobResult)
        
        # Verify they are equal
        assert deserialized == sample_failed_job_result
        assert deserialized.error == sample_failed_job_result.error
        assert deserialized.traceback == sample_failed_job_result.traceback

    def test_job_result_duration_ms(self, sample_job_result: JobResult) -> None:
        """Test JobResult duration_ms property."""
        # Verify duration is calculated correctly
        duration = sample_job_result.duration_ms
        assert duration is not None
        assert duration > 0
        
        # Test with zero times
        zero_time_result = JobResult(
            job_id="test-job-zero",
            status=JOB_STATUS.COMPLETED.value,
            result="test",
            start_time=0.0,
            finish_time=0.0,
        )
        assert zero_time_result.duration_ms is None

    def test_job_result_from_job(self) -> None:
        """Test JobResult from_job class method."""
        from naq.models.jobs import Job
        
        def test_func() -> str:
            return "test result"
        
        # Create a job
        job = Job(
            function=test_func,
            args=(),
            kwargs={},
            job_id="test-job-from-job",
        )
        
        # Set some job properties
        job._start_time = datetime.now().timestamp() - 5
        job._finish_time = datetime.now().timestamp()
        job.result = "completed successfully"
        
        # Create JobResult from job
        result = JobResult.from_job(job)
        
        # Verify the result
        assert result.job_id == job.job_id
        assert result.status == job.status.value
        assert result.result == job.result
        assert result.start_time == job._start_time
        assert result.finish_time == job._finish_time