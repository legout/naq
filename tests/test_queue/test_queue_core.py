# tests/test_queue/test_queue_core.py
"""Tests for queue core functionality."""

import asyncio
import pytest
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from naq.queue.core import Queue
from naq.queue.scheduled import ScheduledJobManager
from naq import Job, JOB_STATUS
from naq.models.schedules import Schedule
from naq.services import ServiceManager


class TestQueueCore:
    """Test core queue functionality."""

    @pytest.fixture
    def mock_service_manager(self):
        """Mock ServiceManager for testing."""
        mock_manager = AsyncMock(spec=ServiceManager)
        mock_connection_service = AsyncMock()
        mock_job_service = AsyncMock()
        mock_stream_service = AsyncMock()
        
        mock_manager.get_service.side_effect = lambda service_type: {
            'ConnectionService': mock_connection_service,
            'JobService': mock_job_service,
            'StreamService': mock_stream_service
        }.get(service_type.__name__, AsyncMock())
        
        return mock_manager

    @pytest.fixture
    def queue_config(self):
        """Basic queue configuration."""
        return {
            "nats": {"url": "nats://localhost:4222"},
            "streams": {"ensure_streams": True},
            "events": {"enabled": True}
        }

    @pytest.mark.asyncio
    async def test_queue_initialization(self, mock_service_manager, queue_config):
        """Test queue initialization."""
        with patch('naq.queue.core.ServiceManager', return_value=mock_service_manager):
            queue = Queue(name="test-queue", config=queue_config)
            await queue.initialize()
            
            assert queue.name == "test-queue"
            assert queue.stream_name == "naq-queue-test-queue"
            assert queue.subject == "naq.queue.test-queue"
            
            await queue.close()

    @pytest.mark.asyncio
    async def test_job_enqueue(self, mock_service_manager, queue_config):
        """Test job enqueueing."""
        def test_function(x):
            return x * 2
        
        with patch('naq.queue.core.ServiceManager', return_value=mock_service_manager):
            queue = Queue(name="test-queue", config=queue_config)
            await queue.initialize()
            
            # Mock job service
            job_service = await queue.service_manager.get_service(MagicMock())
            job_service.enqueue_job.return_value = "job-123"
            
            # Enqueue job
            job = await queue.enqueue(test_function, 5)
            
            assert job.job_id is not None
            assert job.function == test_function
            assert job.args == (5,)
            assert job.queue_name == "test-queue"
            
            job_service.enqueue_job.assert_called_once()
            
            await queue.close()

    @pytest.mark.asyncio
    async def test_job_enqueue_with_options(self, mock_service_manager, queue_config):
        """Test job enqueueing with options."""
        def test_function():
            return "result"
        
        with patch('naq.queue.core.ServiceManager', return_value=mock_service_manager):
            queue = Queue(name="test-queue", config=queue_config)
            await queue.initialize()
            
            job_service = await queue.service_manager.get_service(MagicMock())
            job_service.enqueue_job.return_value = "job-456"
            
            # Enqueue with options
            job = await queue.enqueue(
                test_function,
                max_retries=3,
                retry_delay=60,
                priority=5,
                result_ttl=3600
            )
            
            assert job.max_retries == 3
            assert job.retry_delay == 60
            assert job.priority == 5
            assert job.result_ttl == 3600
            
            await queue.close()

    @pytest.mark.asyncio
    async def test_scheduled_job_enqueue_at(self, mock_service_manager, queue_config):
        """Test scheduling job for specific time."""
        def test_function():
            return "scheduled result"
        
        run_at = datetime.now(timezone.utc) + timedelta(hours=1)
        
        with patch('naq.queue.core.ServiceManager', return_value=mock_service_manager):
            queue = Queue(name="test-queue", config=queue_config)
            await queue.initialize()
            
            # Mock scheduled job manager
            with patch.object(queue, '_scheduled_manager') as mock_scheduled:
                mock_scheduled.schedule_job.return_value = "scheduled-job-123"
                
                job = await queue.enqueue_at(run_at, test_function)
                
                assert job.job_id is not None
                assert job.function == test_function
                assert job.queue_name == "test-queue"
                
                mock_scheduled.schedule_job.assert_called_once()
                
            await queue.close()

    @pytest.mark.asyncio
    async def test_scheduled_job_enqueue_in(self, mock_service_manager, queue_config):
        """Test scheduling job with delay."""
        def test_function():
            return "delayed result"
        
        delay = timedelta(minutes=30)
        
        with patch('naq.queue.core.ServiceManager', return_value=mock_service_manager):
            queue = Queue(name="test-queue", config=queue_config)
            await queue.initialize()
            
            with patch.object(queue, '_scheduled_manager') as mock_scheduled:
                mock_scheduled.schedule_job.return_value = "delayed-job-123"
                
                job = await queue.enqueue_in(delay, test_function)
                
                assert job.job_id is not None
                mock_scheduled.schedule_job.assert_called_once()
                
                # Verify the computed run time is approximately correct
                call_args = mock_scheduled.schedule_job.call_args
                scheduled_time = call_args[1]['run_at']
                expected_time = datetime.now(timezone.utc) + delay
                
                # Allow 5 second tolerance
                assert abs((scheduled_time - expected_time).total_seconds()) < 5
                
            await queue.close()

    @pytest.mark.asyncio
    async def test_recurring_job_schedule(self, mock_service_manager, queue_config):
        """Test recurring job scheduling."""
        def recurring_function():
            return "recurring result"
        
        with patch('naq.queue.core.ServiceManager', return_value=mock_service_manager):
            queue = Queue(name="test-queue", config=queue_config)
            await queue.initialize()
            
            with patch.object(queue, '_scheduled_manager') as mock_scheduled:
                mock_scheduled.create_schedule.return_value = "schedule-123"
                
                schedule = await queue.schedule(
                    recurring_function,
                    cron="0 */6 * * *",  # Every 6 hours
                    max_runs=10
                )
                
                assert schedule.schedule_id is not None
                assert schedule.cron == "0 */6 * * *"
                assert schedule.max_runs == 10
                assert schedule.function == recurring_function
                
                mock_scheduled.create_schedule.assert_called_once()
                
            await queue.close()

    @pytest.mark.asyncio
    async def test_schedule_management(self, mock_service_manager, queue_config):
        """Test schedule management operations."""
        with patch('naq.queue.core.ServiceManager', return_value=mock_service_manager):
            queue = Queue(name="test-queue", config=queue_config)
            await queue.initialize()
            
            with patch.object(queue, '_scheduled_manager') as mock_scheduled:
                schedule_id = "test-schedule-123"
                
                # Test pause schedule
                mock_scheduled.pause_schedule.return_value = True
                result = await queue.pause_schedule(schedule_id)
                assert result is True
                mock_scheduled.pause_schedule.assert_called_once_with(schedule_id)
                
                # Test resume schedule
                mock_scheduled.resume_schedule.return_value = True
                result = await queue.resume_schedule(schedule_id)
                assert result is True
                mock_scheduled.resume_schedule.assert_called_once_with(schedule_id)
                
                # Test cancel schedule
                mock_scheduled.cancel_schedule.return_value = True
                result = await queue.cancel_schedule(schedule_id)
                assert result is True
                mock_scheduled.cancel_schedule.assert_called_once_with(schedule_id)
                
            await queue.close()

    @pytest.mark.asyncio
    async def test_job_management(self, mock_service_manager, queue_config):
        """Test job management operations."""
        with patch('naq.queue.core.ServiceManager', return_value=mock_service_manager):
            queue = Queue(name="test-queue", config=queue_config)
            await queue.initialize()
            
            with patch.object(queue, '_scheduled_manager') as mock_scheduled:
                job_id = "test-job-123"
                
                # Test pause job
                mock_scheduled.pause_job.return_value = True
                result = await queue.pause_scheduled_job(job_id)
                assert result is True
                mock_scheduled.pause_job.assert_called_once_with(job_id)
                
                # Test resume job
                mock_scheduled.resume_job.return_value = True
                result = await queue.resume_scheduled_job(job_id)
                assert result is True
                mock_scheduled.resume_job.assert_called_once_with(job_id)
                
                # Test cancel job
                mock_scheduled.cancel_job.return_value = True
                result = await queue.cancel_scheduled_job(job_id)
                assert result is True
                mock_scheduled.cancel_job.assert_called_once_with(job_id)
                
            await queue.close()

    @pytest.mark.asyncio
    async def test_queue_purge(self, mock_service_manager, queue_config):
        """Test queue purging."""
        with patch('naq.queue.core.ServiceManager', return_value=mock_service_manager):
            queue = Queue(name="test-queue", config=queue_config)
            await queue.initialize()
            
            stream_service = await queue.service_manager.get_service(MagicMock())
            stream_service.purge_stream.return_value = 5  # 5 jobs purged
            
            purged_count = await queue.purge()
            
            assert purged_count == 5
            stream_service.purge_stream.assert_called_once_with(
                queue.stream_name,
                subject=queue.subject
            )
            
            await queue.close()

    @pytest.mark.asyncio
    async def test_queue_info(self, mock_service_manager, queue_config):
        """Test getting queue information."""
        with patch('naq.queue.core.ServiceManager', return_value=mock_service_manager):
            queue = Queue(name="test-queue", config=queue_config)
            await queue.initialize()
            
            stream_service = await queue.service_manager.get_service(MagicMock())
            stream_service.get_stream_info.return_value = {
                "config": {"name": "naq-queue-test-queue"},
                "state": {"messages": 10, "bytes": 1024}
            }
            
            info = await queue.get_info()
            
            assert info["config"]["name"] == "naq-queue-test-queue"
            assert info["state"]["messages"] == 10
            assert info["state"]["bytes"] == 1024
            
            await queue.close()


class TestQueueValidation:
    """Test queue validation and error handling."""

    def test_invalid_queue_name(self):
        """Test queue creation with invalid name."""
        with pytest.raises(ValueError, match="Queue name.*empty"):
            Queue(name="")
        
        with pytest.raises(ValueError, match="Queue name.*invalid"):
            Queue(name="queue with spaces")
        
        with pytest.raises(ValueError, match="Queue name.*invalid"):
            Queue(name="queue/with/slashes")

    @pytest.mark.asyncio
    async def test_enqueue_validation(self, queue_config):
        """Test job enqueue validation."""
        with patch('naq.queue.core.ServiceManager'):
            queue = Queue(name="test-queue", config=queue_config)
            
            # Test invalid function
            with pytest.raises(TypeError):
                await queue.enqueue("not_a_function")
            
            # Test invalid retry settings
            with pytest.raises(ValueError):
                await queue.enqueue(lambda: None, max_retries=-1)
            
            with pytest.raises(ValueError):
                await queue.enqueue(lambda: None, retry_delay=-5)

    @pytest.mark.asyncio
    async def test_schedule_validation(self, queue_config):
        """Test schedule validation."""
        with patch('naq.queue.core.ServiceManager'):
            queue = Queue(name="test-queue", config=queue_config)
            
            # Test invalid cron expression
            with pytest.raises(ValueError):
                await queue.schedule(lambda: None, cron="invalid cron")
            
            # Test invalid interval
            with pytest.raises(ValueError):
                await queue.schedule(lambda: None, interval=-5)
            
            # Test missing schedule specification
            with pytest.raises(ValueError):
                await queue.schedule(lambda: None)  # No cron or interval


class TestQueuePerformance:
    """Test queue performance characteristics."""

    @pytest.mark.asyncio
    async def test_bulk_enqueue_performance(self, queue_config):
        """Test bulk enqueueing performance."""
        def test_function(i):
            return i * 2
        
        with patch('naq.queue.core.ServiceManager') as mock_sm:
            mock_service_manager = AsyncMock()
            mock_sm.return_value = mock_service_manager
            
            job_service = AsyncMock()
            job_service.enqueue_job.return_value = "job-id"
            mock_service_manager.get_service.return_value = job_service
            
            queue = Queue(name="test-queue", config=queue_config)
            await queue.initialize()
            
            start_time = time.time()
            
            # Enqueue 100 jobs
            jobs = []
            for i in range(100):
                job = await queue.enqueue(test_function, i)
                jobs.append(job)
            
            duration = time.time() - start_time
            
            # Should complete relatively quickly
            assert duration < 5.0  # 5 seconds max for 100 jobs
            assert len(jobs) == 100
            assert job_service.enqueue_job.call_count == 100
            
            await queue.close()

    @pytest.mark.asyncio
    async def test_concurrent_operations(self, queue_config):
        """Test concurrent queue operations."""
        def test_function(i):
            return f"result-{i}"
        
        with patch('naq.queue.core.ServiceManager') as mock_sm:
            mock_service_manager = AsyncMock()
            mock_sm.return_value = mock_service_manager
            
            job_service = AsyncMock()
            job_service.enqueue_job.return_value = "job-id"
            mock_service_manager.get_service.return_value = job_service
            
            queue = Queue(name="test-queue", config=queue_config)
            await queue.initialize()
            
            # Concurrent enqueue operations
            async def enqueue_batch(start, count):
                tasks = []
                for i in range(start, start + count):
                    task = queue.enqueue(test_function, i)
                    tasks.append(task)
                return await asyncio.gather(*tasks)
            
            start_time = time.time()
            
            # Run 3 concurrent batches of 10 jobs each
            batch_tasks = [
                enqueue_batch(0, 10),
                enqueue_batch(10, 10),
                enqueue_batch(20, 10)
            ]
            
            results = await asyncio.gather(*batch_tasks)
            duration = time.time() - start_time
            
            # Verify all jobs were enqueued
            total_jobs = sum(len(batch) for batch in results)
            assert total_jobs == 30
            
            # Concurrent operations should be efficient
            assert duration < 3.0
            
            await queue.close()


class TestQueueIntegration:
    """Test queue integration with services."""

    @pytest.mark.asyncio
    async def test_service_manager_integration(self, queue_config):
        """Test queue integration with ServiceManager."""
        with patch('naq.queue.core.ServiceManager') as mock_sm:
            mock_service_manager = AsyncMock()
            mock_sm.return_value = mock_service_manager
            
            # Mock various services
            connection_service = AsyncMock()
            job_service = AsyncMock()
            stream_service = AsyncMock()
            
            def get_service_side_effect(service_type):
                service_map = {
                    'ConnectionService': connection_service,
                    'JobService': job_service,
                    'StreamService': stream_service
                }
                return service_map.get(service_type.__name__, AsyncMock())
            
            mock_service_manager.get_service.side_effect = get_service_side_effect
            
            queue = Queue(name="test-queue", config=queue_config)
            await queue.initialize()
            
            # Verify service manager was initialized with config
            mock_sm.assert_called_once_with(queue_config)
            
            # Verify service manager initialization
            mock_service_manager.initialize_all.assert_called_once()
            
            await queue.close()
            
            # Verify cleanup
            mock_service_manager.cleanup_all.assert_called_once()

    @pytest.mark.asyncio
    async def test_event_logging_integration(self, queue_config):
        """Test queue integration with event logging."""
        def test_function():
            return "test result"
        
        with patch('naq.queue.core.ServiceManager') as mock_sm:
            mock_service_manager = AsyncMock()
            mock_sm.return_value = mock_service_manager
            
            job_service = AsyncMock()
            event_service = AsyncMock()
            
            def get_service_side_effect(service_type):
                if service_type.__name__ == 'JobService':
                    return job_service
                elif service_type.__name__ == 'EventService':
                    return event_service
                return AsyncMock()
            
            mock_service_manager.get_service.side_effect = get_service_side_effect
            job_service.enqueue_job.return_value = "job-123"
            
            queue = Queue(name="test-queue", config=queue_config)
            await queue.initialize()
            
            # Enqueue job
            job = await queue.enqueue(test_function)
            
            # Verify event logging was triggered
            event_service.log_job_enqueued.assert_called_once()
            
            await queue.close()

    @pytest.mark.asyncio
    async def test_configuration_propagation(self):
        """Test configuration propagation to services."""
        custom_config = {
            "nats": {
                "url": "nats://custom:4222",
                "client_name": "test-client"
            },
            "workers": {"concurrency": 8},
            "events": {"enabled": False}
        }
        
        with patch('naq.queue.core.ServiceManager') as mock_sm:
            mock_service_manager = AsyncMock()
            mock_sm.return_value = mock_service_manager
            
            queue = Queue(name="test-queue", config=custom_config)
            await queue.initialize()
            
            # Verify ServiceManager was created with custom config
            mock_sm.assert_called_once_with(custom_config)
            
            await queue.close()


class TestQueueErrorHandling:
    """Test queue error handling scenarios."""

    @pytest.mark.asyncio
    async def test_service_initialization_failure(self, queue_config):
        """Test handling of service initialization failure."""
        with patch('naq.queue.core.ServiceManager') as mock_sm:
            mock_service_manager = AsyncMock()
            mock_service_manager.initialize_all.side_effect = Exception("Service init failed")
            mock_sm.return_value = mock_service_manager
            
            queue = Queue(name="test-queue", config=queue_config)
            
            with pytest.raises(Exception, match="Service init failed"):
                await queue.initialize()

    @pytest.mark.asyncio
    async def test_enqueue_service_error(self, queue_config):
        """Test handling of job enqueue service errors."""
        def test_function():
            return "test"
        
        with patch('naq.queue.core.ServiceManager') as mock_sm:
            mock_service_manager = AsyncMock()
            mock_sm.return_value = mock_service_manager
            
            job_service = AsyncMock()
            job_service.enqueue_job.side_effect = Exception("Enqueue failed")
            mock_service_manager.get_service.return_value = job_service
            
            queue = Queue(name="test-queue", config=queue_config)
            await queue.initialize()
            
            with pytest.raises(Exception, match="Enqueue failed"):
                await queue.enqueue(test_function)
            
            await queue.close()

    @pytest.mark.asyncio
    async def test_connection_error_handling(self, queue_config):
        """Test handling of connection errors."""
        with patch('naq.queue.core.ServiceManager') as mock_sm:
            mock_service_manager = AsyncMock()
            mock_sm.return_value = mock_service_manager
            
            connection_service = AsyncMock()
            connection_service.test_connection.side_effect = ConnectionError("NATS unavailable")
            mock_service_manager.get_service.return_value = connection_service
            
            queue = Queue(name="test-queue", config=queue_config)
            await queue.initialize()
            
            # Queue should handle connection errors gracefully
            # The specific behavior depends on implementation
            
            await queue.close()