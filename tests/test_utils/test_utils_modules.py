# tests/test_utils/test_utils_modules.py
"""Tests for utility modules."""

import asyncio
import pytest
import logging
import time
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Any, Dict, List

from naq.utils.async_helpers import run_async_from_sync, gather_with_concurrency
from naq.utils.context_managers import timeout_context, error_context
from naq.utils.decorators import retry, timing, cache_result
from naq.utils.error_handling import (
    handle_nats_error, 
    handle_serialization_error,
    safe_execute
)
from naq.utils.logging import setup_logging, get_logger
from naq.utils.nats_helpers import (
    ensure_stream,
    create_consumer,
    publish_with_retry
)
from naq.utils.serialization import (
    serialize_job_data,
    deserialize_job_data,
    serialize_result,
    deserialize_result
)
from naq.utils.timing import measure_time, format_duration
from naq.utils.types import JobData, ConfigDict
from naq.utils.validation import (
    validate_job_config,
    validate_queue_name,
    validate_worker_config
)


class TestAsyncHelpers:
    """Test async helper utilities."""

    def test_run_async_from_sync(self):
        """Test running async functions from sync context."""
        async def async_task(x):
            await asyncio.sleep(0.01)
            return x * 2
        
        result = run_async_from_sync(async_task(5))
        assert result == 10

    @pytest.mark.asyncio
    async def test_gather_with_concurrency(self):
        """Test concurrent execution with limit."""
        async def slow_task(x):
            await asyncio.sleep(0.1)
            return x
        
        tasks = [slow_task(i) for i in range(5)]
        start_time = time.time()
        results = await gather_with_concurrency(tasks, concurrency_limit=2)
        duration = time.time() - start_time
        
        assert results == [0, 1, 2, 3, 4]
        # With limit of 2, should take at least 3 cycles (0.3s)
        assert duration >= 0.25


class TestContextManagers:
    """Test context manager utilities."""

    @pytest.mark.asyncio
    async def test_timeout_context_success(self):
        """Test timeout context with successful operation."""
        async with timeout_context(1.0):
            await asyncio.sleep(0.1)
        # Should complete without error

    @pytest.mark.asyncio
    async def test_timeout_context_timeout(self):
        """Test timeout context with timeout."""
        with pytest.raises(asyncio.TimeoutError):
            async with timeout_context(0.1):
                await asyncio.sleep(0.2)

    def test_error_context_no_error(self):
        """Test error context with no errors."""
        with error_context("test operation"):
            result = 1 + 1
        assert result == 2

    def test_error_context_with_error(self):
        """Test error context with error handling."""
        with pytest.raises(ValueError):
            with error_context("test operation"):
                raise ValueError("Test error")


class TestDecorators:
    """Test decorator utilities."""

    def test_retry_decorator_success(self):
        """Test retry decorator with successful operation."""
        call_count = 0
        
        @retry(max_attempts=3, delay=0.01)
        def sometimes_failing_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ValueError("Temporary failure")
            return "success"
        
        result = sometimes_failing_func()
        assert result == "success"
        assert call_count == 2

    def test_retry_decorator_exhausted(self):
        """Test retry decorator with exhausted attempts."""
        @retry(max_attempts=2, delay=0.01)
        def always_failing_func():
            raise ValueError("Always fails")
        
        with pytest.raises(ValueError):
            always_failing_func()

    def test_timing_decorator(self):
        """Test timing decorator."""
        @timing
        def slow_function():
            time.sleep(0.1)
            return "done"
        
        result = slow_function()
        assert result == "done"

    def test_cache_result_decorator(self):
        """Test cache result decorator."""
        call_count = 0
        
        @cache_result(ttl=1.0)
        def expensive_function(x):
            nonlocal call_count
            call_count += 1
            return x * 2
        
        # First call
        result1 = expensive_function(5)
        assert result1 == 10
        assert call_count == 1
        
        # Second call should use cache
        result2 = expensive_function(5)
        assert result2 == 10
        assert call_count == 1  # No additional call


class TestErrorHandling:
    """Test error handling utilities."""

    @pytest.mark.asyncio
    async def test_handle_nats_error(self):
        """Test NATS error handling."""
        from nats.errors import ConnectionClosedError
        
        error = ConnectionClosedError()
        handled = await handle_nats_error(error, operation="test_op")
        
        assert handled is not None
        assert "connection" in str(handled).lower()

    def test_handle_serialization_error(self):
        """Test serialization error handling."""
        import pickle
        
        class UnpicklableClass:
            def __getstate__(self):
                raise TypeError("Cannot pickle")
        
        obj = UnpicklableClass()
        handled = handle_serialization_error(obj, operation="test_serialize")
        
        assert handled is not None
        assert "serialization" in str(handled).lower()

    @pytest.mark.asyncio
    async def test_safe_execute_success(self):
        """Test safe execute with successful operation."""
        async def safe_operation():
            return "success"
        
        result = await safe_execute(safe_operation(), default="failed")
        assert result == "success"

    @pytest.mark.asyncio
    async def test_safe_execute_failure(self):
        """Test safe execute with failed operation."""
        async def failing_operation():
            raise ValueError("Test error")
        
        result = await safe_execute(failing_operation(), default="failed")
        assert result == "failed"


class TestLogging:
    """Test logging utilities."""

    def test_setup_logging_basic(self):
        """Test basic logging setup."""
        logger = setup_logging(level="INFO")
        assert logger.level == logging.INFO

    def test_setup_logging_with_config(self):
        """Test logging setup with configuration."""
        config = {
            "level": "DEBUG",
            "format": "%(name)s - %(message)s",
            "handlers": ["console"]
        }
        logger = setup_logging(**config)
        assert logger.level == logging.DEBUG

    def test_get_logger(self):
        """Test getting named logger."""
        logger = get_logger("test.module")
        assert logger.name == "test.module"


class TestNATSHelpers:
    """Test NATS helper utilities."""

    @pytest.mark.asyncio
    async def test_ensure_stream(self):
        """Test stream creation/validation."""
        mock_js = AsyncMock()
        mock_js.stream_info.side_effect = Exception("Stream not found")
        mock_js.add_stream.return_value = MagicMock()
        
        await ensure_stream(mock_js, "test-stream", ["test.subject"])
        
        mock_js.add_stream.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_consumer(self):
        """Test consumer creation."""
        mock_js = AsyncMock()
        mock_consumer = AsyncMock()
        mock_js.add_consumer.return_value = mock_consumer
        
        consumer = await create_consumer(
            mock_js, 
            "test-stream", 
            "test-consumer",
            filter_subject="test.subject"
        )
        
        assert consumer == mock_consumer
        mock_js.add_consumer.assert_called_once()

    @pytest.mark.asyncio
    async def test_publish_with_retry(self):
        """Test publishing with retry."""
        mock_js = AsyncMock()
        mock_js.publish.side_effect = [
            Exception("Network error"),  # First attempt fails
            MagicMock(seq=1)  # Second attempt succeeds
        ]
        
        result = await publish_with_retry(
            mock_js,
            "test.subject",
            b"test data",
            max_retries=2
        )
        
        assert result.seq == 1
        assert mock_js.publish.call_count == 2


class TestSerialization:
    """Test serialization utilities."""

    def test_serialize_job_data(self):
        """Test job data serialization."""
        job_data = {
            "job_id": "test-123",
            "function": lambda x: x * 2,
            "args": (5,),
            "kwargs": {"multiplier": 2}
        }
        
        serialized = serialize_job_data(job_data)
        assert isinstance(serialized, bytes)
        
        # Should be able to deserialize
        deserialized = deserialize_job_data(serialized)
        assert deserialized["job_id"] == "test-123"
        assert callable(deserialized["function"])

    def test_serialize_result(self):
        """Test result serialization."""
        result_data = {
            "job_id": "test-123",
            "status": "completed",
            "result": {"key": "value"},
            "error": None
        }
        
        serialized = serialize_result(result_data)
        assert isinstance(serialized, bytes)
        
        deserialized = deserialize_result(serialized)
        assert deserialized == result_data

    def test_serialization_error_handling(self):
        """Test serialization error handling."""
        class UnserializableClass:
            def __reduce__(self):
                raise TypeError("Cannot serialize")
        
        job_data = {"bad_obj": UnserializableClass()}
        
        with pytest.raises(Exception):
            serialize_job_data(job_data)


class TestTiming:
    """Test timing utilities."""

    def test_measure_time(self):
        """Test time measurement."""
        with measure_time() as timer:
            time.sleep(0.1)
        
        assert timer.elapsed >= 0.1
        assert timer.elapsed < 0.2

    def test_format_duration(self):
        """Test duration formatting."""
        # Test milliseconds
        assert "ms" in format_duration(0.001)
        
        # Test seconds
        assert "s" in format_duration(1.5)
        
        # Test minutes
        assert "m" in format_duration(120.0)


class TestTypes:
    """Test type definitions."""

    def test_job_data_type(self):
        """Test JobData type definition."""
        job_data: JobData = {
            "job_id": "test-123",
            "queue_name": "test-queue",
            "function": "builtins.print",
            "args": [],
            "kwargs": {}
        }
        
        # Type should validate (no runtime errors)
        assert job_data["job_id"] == "test-123"

    def test_config_dict_type(self):
        """Test ConfigDict type definition."""
        config: ConfigDict = {
            "nats": {"url": "nats://localhost:4222"},
            "workers": {"concurrency": 4},
            "events": {"enabled": True}
        }
        
        # Type should validate
        assert config["nats"]["url"] == "nats://localhost:4222"


class TestValidation:
    """Test validation utilities."""

    def test_validate_job_config_valid(self):
        """Test job config validation with valid config."""
        config = {
            "max_retries": 3,
            "retry_delay": 5.0,
            "timeout": 30.0
        }
        
        # Should not raise any errors
        validate_job_config(config)

    def test_validate_job_config_invalid(self):
        """Test job config validation with invalid config."""
        config = {
            "max_retries": -1,  # Invalid
            "retry_delay": "invalid",  # Invalid type
        }
        
        with pytest.raises(ValueError):
            validate_job_config(config)

    def test_validate_queue_name_valid(self):
        """Test queue name validation with valid names."""
        valid_names = ["test-queue", "queue_1", "my.queue", "queue123"]
        
        for name in valid_names:
            # Should not raise errors
            validate_queue_name(name)

    def test_validate_queue_name_invalid(self):
        """Test queue name validation with invalid names."""
        invalid_names = ["", "queue with spaces", "queue/with/slashes", "queue\nwith\nnewlines"]
        
        for name in invalid_names:
            with pytest.raises(ValueError):
                validate_queue_name(name)

    def test_validate_worker_config_valid(self):
        """Test worker config validation with valid config."""
        config = {
            "concurrency": 4,
            "heartbeat_interval": 30,
            "max_jobs": 100
        }
        
        # Should not raise errors
        validate_worker_config(config)

    def test_validate_worker_config_invalid(self):
        """Test worker config validation with invalid config."""
        config = {
            "concurrency": 0,  # Invalid
            "heartbeat_interval": -5,  # Invalid
        }
        
        with pytest.raises(ValueError):
            validate_worker_config(config)


class TestUtilsIntegration:
    """Test utils integration scenarios."""

    @pytest.mark.asyncio
    async def test_combined_utilities(self):
        """Test using multiple utilities together."""
        # Combine timing, retry, and async helpers
        call_count = 0
        
        @timing
        @retry(max_attempts=2, delay=0.01)
        async def complex_operation():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("First attempt fails")
            await asyncio.sleep(0.01)
            return "success"
        
        # Run async operation from sync context
        result = run_async_from_sync(complex_operation())
        assert result == "success"
        assert call_count == 2

    def test_error_handling_with_validation(self):
        """Test error handling combined with validation."""
        def process_config(config: Dict[str, Any]) -> Dict[str, Any]:
            validate_worker_config(config)
            return config
        
        # Valid config
        valid_config = {"concurrency": 4, "heartbeat_interval": 30}
        result = process_config(valid_config)
        assert result == valid_config
        
        # Invalid config
        invalid_config = {"concurrency": -1}
        with pytest.raises(ValueError):
            process_config(invalid_config)