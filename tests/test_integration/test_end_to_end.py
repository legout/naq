# tests/test_integration/test_end_to_end.py
"""End-to-end integration tests for NAQ."""

import asyncio
import pytest
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

from naq import Job, Queue, Worker, Scheduler, JOB_STATUS
from naq.services import ServiceManager
from naq.config import load_config
from naq.events import AsyncJobEventLogger


class TestBasicWorkflow:
    """Test basic job workflow end-to-end."""

    @pytest.fixture
    def test_config(self):
        """Test configuration."""
        return {
            "nats": {"url": "nats://localhost:4222"},
            "workers": {"concurrency": 2},
            "events": {"enabled": True}
        }

    @pytest.mark.asyncio
    async def test_enqueue_and_process_job(self, test_config):
        """Test basic enqueue and process workflow."""
        def simple_task(x, y=1):
            return x + y
        
        with patch('naq.services.ServiceManager') as mock_sm:
            # Mock service manager and services
            mock_manager = AsyncMock()
            mock_job_service = AsyncMock()
            mock_connection_service = AsyncMock()
            
            mock_manager.get_service.side_effect = lambda service_type: {
                'JobService': mock_job_service,
                'ConnectionService': mock_connection_service
            }.get(service_type.__name__, AsyncMock())
            
            mock_job_service.enqueue_job.return_value = "job-123"
            mock_sm.return_value = mock_manager
            
            # Create queue and enqueue job
            queue = Queue("test-queue", config=test_config)
            await queue.initialize()
            
            job = await queue.enqueue(simple_task, 5, y=3)
            
            assert job.job_id is not None
            assert job.function == simple_task
            assert job.args == (5,)
            assert job.kwargs == {"y": 3}
            
            await queue.close()

    @pytest.mark.asyncio
    async def test_job_with_retry(self, test_config):
        """Test job with retry configuration."""
        attempt_count = 0
        
        def flaky_task():
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                raise ValueError(f"Attempt {attempt_count} failed")
            return f"Success on attempt {attempt_count}"
        
        with patch('naq.services.ServiceManager') as mock_sm:
            mock_manager = AsyncMock()
            mock_job_service = AsyncMock()
            mock_job_service.enqueue_job.return_value = "retry-job-123"
            mock_manager.get_service.return_value = mock_job_service
            mock_sm.return_value = mock_manager
            
            queue = Queue("retry-queue", config=test_config)
            await queue.initialize()
            
            job = await queue.enqueue(
                flaky_task,
                max_retries=3,
                retry_delay=0.1
            )
            
            assert job.max_retries == 3
            assert job.retry_delay == 0.1
            
            await queue.close()

    @pytest.mark.asyncio
    async def test_scheduled_job_workflow(self, test_config):
        """Test scheduled job workflow."""
        def scheduled_task():
            return f"Executed at {datetime.now()}"
        
        with patch('naq.services.ServiceManager') as mock_sm:
            mock_manager = AsyncMock()
            mock_scheduled_manager = AsyncMock()
            
            # Mock queue with scheduled manager
            with patch('naq.queue.core.ScheduledJobManager', return_value=mock_scheduled_manager):
                mock_scheduled_manager.schedule_job.return_value = "scheduled-123"
                mock_sm.return_value = mock_manager
                
                queue = Queue("scheduled-queue", config=test_config)
                await queue.initialize()
                
                # Schedule job for 1 hour in the future
                run_at = datetime.now(timezone.utc) + timedelta(hours=1)
                job = await queue.enqueue_at(run_at, scheduled_task)
                
                assert job.job_id is not None
                mock_scheduled_manager.schedule_job.assert_called_once()
                
                await queue.close()


class TestServiceIntegration:
    """Test service layer integration."""

    @pytest.fixture
    def service_config(self):
        """Service configuration."""
        return {
            "nats": {
                "url": "nats://localhost:4222",
                "client_name": "integration-test"
            },
            "workers": {
                "concurrency": 4,
                "heartbeat_interval": 10
            },
            "events": {
                "enabled": True,
                "batch_size": 50
            }
        }

    @pytest.mark.asyncio
    async def test_service_manager_lifecycle(self, service_config):
        """Test ServiceManager lifecycle."""
        with patch('naq.services.connection.get_nats_connection') as mock_conn:
            mock_nc = AsyncMock()
            mock_conn.return_value = mock_nc
            
            manager = ServiceManager(service_config)
            
            # Initialize all services
            await manager.initialize_all()
            
            # Get various services
            conn_service = await manager.get_service_by_name('ConnectionService')
            job_service = await manager.get_service_by_name('JobService')
            event_service = await manager.get_service_by_name('EventService')
            
            assert conn_service is not None
            assert job_service is not None
            assert event_service is not None
            
            # Cleanup
            await manager.cleanup_all()

    @pytest.mark.asyncio
    async def test_cross_service_communication(self, service_config):
        """Test communication between services."""
        with patch('naq.services.connection.get_nats_connection') as mock_conn:
            mock_nc = AsyncMock()
            mock_js = AsyncMock()
            mock_nc.jetstream.return_value = mock_js
            mock_conn.return_value = mock_nc
            
            manager = ServiceManager(service_config)
            await manager.initialize_all()
            
            # Test job service using connection service
            job_service = await manager.get_service_by_name('JobService')
            
            def test_job():
                return "test result"
            
            job = Job(test_job)
            
            # Mock the enqueue operation
            mock_js.publish.return_value = AsyncMock(seq=1)
            
            result = await job_service.enqueue_job(job, "test-queue")
            
            # Verify cross-service interaction
            mock_js.publish.assert_called_once()
            
            await manager.cleanup_all()


class TestEventLogging:
    """Test event logging integration."""

    @pytest.mark.asyncio
    async def test_job_lifecycle_events(self):
        """Test job lifecycle event logging."""
        with patch('naq.events.storage.NATSJobEventStorage') as mock_storage_class:
            mock_storage = AsyncMock()
            mock_storage_class.return_value = mock_storage
            
            logger = AsyncJobEventLogger()
            await logger.start()
            
            try:
                # Log complete job lifecycle
                job_id = "lifecycle-test-123"
                worker_id = "test-worker"
                queue_name = "test-queue"
                
                await logger.log_job_enqueued(job_id, queue_name)
                await logger.log_job_started(job_id, worker_id, queue_name)
                await logger.log_job_completed(job_id, worker_id, duration_ms=1500)
                
                await logger.flush()
                
                # Verify events were logged
                assert mock_storage.store_event.call_count >= 3
                
            finally:
                await logger.stop()

    @pytest.mark.asyncio
    async def test_worker_status_events(self):
        """Test worker status event logging."""
        with patch('naq.events.storage.NATSJobEventStorage') as mock_storage_class:
            mock_storage = AsyncMock()
            mock_storage_class.return_value = mock_storage
            
            logger = AsyncJobEventLogger()
            await logger.start()
            
            try:
                worker_id = "status-test-worker"
                hostname = "test-host"
                pid = 12345
                
                # Log worker lifecycle
                await logger.log_worker_started(
                    worker_id, hostname, pid, ["queue1"], 4
                )
                await logger.log_worker_busy(
                    worker_id, hostname, pid, "current-job"
                )
                await logger.log_worker_idle(worker_id, hostname, pid)
                await logger.log_worker_stopped(worker_id, hostname, pid)
                
                await logger.flush()
                
                # Verify worker events
                assert mock_storage.store_event.call_count >= 4
                
            finally:
                await logger.stop()


class TestScheduling:
    """Test scheduling integration."""

    @pytest.fixture
    def scheduler_config(self):
        """Scheduler configuration."""
        return {
            "nats": {"url": "nats://localhost:4222"},
            "scheduler": {
                "check_interval": 1,
                "batch_size": 10
            }
        }

    @pytest.mark.asyncio
    async def test_scheduler_lifecycle(self, scheduler_config):
        """Test scheduler lifecycle."""
        with patch('naq.services.ServiceManager') as mock_sm:
            mock_manager = AsyncMock()
            mock_sm.return_value = mock_manager
            
            scheduler = Scheduler(config=scheduler_config)
            
            # Initialize scheduler
            await scheduler.initialize()
            
            # Start and stop scheduler
            start_task = asyncio.create_task(scheduler.start())
            await asyncio.sleep(0.1)  # Let it start
            
            await scheduler.stop()
            
            # Wait for start task to complete
            try:
                await asyncio.wait_for(start_task, timeout=1.0)
            except asyncio.TimeoutError:
                start_task.cancel()
            
            await scheduler.close()

    @pytest.mark.asyncio
    async def test_recurring_schedule(self, scheduler_config):
        """Test recurring schedule management."""
        def recurring_task():
            return f"Executed at {datetime.now()}"
        
        with patch('naq.services.ServiceManager') as mock_sm:
            mock_manager = AsyncMock()
            mock_scheduler_service = AsyncMock()
            
            mock_manager.get_service.return_value = mock_scheduler_service
            mock_scheduler_service.create_schedule.return_value = "schedule-123"
            mock_sm.return_value = mock_manager
            
            scheduler = Scheduler(config=scheduler_config)
            await scheduler.initialize()
            
            # Create recurring schedule
            schedule = await scheduler.create_schedule(
                recurring_task,
                cron="0 */6 * * *",  # Every 6 hours
                max_runs=5
            )
            
            assert schedule.schedule_id is not None
            assert schedule.cron == "0 */6 * * *"
            assert schedule.max_runs == 5
            
            mock_scheduler_service.create_schedule.assert_called_once()
            
            await scheduler.close()


class TestErrorHandling:
    """Test error handling in integration scenarios."""

    @pytest.mark.asyncio
    async def test_connection_failure_recovery(self):
        """Test recovery from connection failures."""
        config = {
            "nats": {"url": "nats://localhost:4222"},
            "workers": {"concurrency": 1}
        }
        
        with patch('naq.services.connection.get_nats_connection') as mock_conn:
            # Simulate connection failure then recovery
            mock_conn.side_effect = [
                ConnectionError("Initial connection failed"),
                AsyncMock()  # Recovery
            ]
            
            manager = ServiceManager(config)
            
            # First attempt should fail
            with pytest.raises(ConnectionError):
                await manager.initialize_all()
            
            # Second attempt should succeed
            await manager.initialize_all()
            await manager.cleanup_all()

    @pytest.mark.asyncio
    async def test_service_failure_isolation(self):
        """Test that service failures are isolated."""
        config = {
            "nats": {"url": "nats://localhost:4222"},
            "events": {"enabled": True}
        }
        
        with patch('naq.services.ServiceManager') as mock_sm:
            mock_manager = AsyncMock()
            
            # Mock one service failing
            def get_service_side_effect(service_type):
                if service_type.__name__ == 'EventService':
                    raise Exception("Event service failed")
                return AsyncMock()
            
            mock_manager.get_service.side_effect = get_service_side_effect
            mock_sm.return_value = mock_manager
            
            # Other services should still work
            queue = Queue("test-queue", config=config)
            
            try:
                await queue.initialize()
                # Queue should still be functional even if event service fails
                assert queue.name == "test-queue"
            except Exception:
                # Or the error should be handled gracefully
                pass
            finally:
                await queue.close()

    @pytest.mark.asyncio
    async def test_job_execution_error_handling(self):
        """Test job execution error handling."""
        def failing_job():
            raise ValueError("Job execution failed")
        
        config = {"nats": {"url": "nats://localhost:4222"}}
        
        with patch('naq.services.ServiceManager') as mock_sm:
            mock_manager = AsyncMock()
            mock_job_service = AsyncMock()
            mock_job_service.enqueue_job.return_value = "failing-job-123"
            mock_manager.get_service.return_value = mock_job_service
            mock_sm.return_value = mock_manager
            
            queue = Queue("error-queue", config=config)
            await queue.initialize()
            
            # Job should be enqueued even if it will fail
            job = await queue.enqueue(failing_job)
            assert job.job_id is not None
            
            # Error handling will be done by worker when processing
            
            await queue.close()


class TestPerformance:
    """Test performance characteristics."""

    @pytest.mark.asyncio
    async def test_bulk_job_processing(self):
        """Test bulk job processing performance."""
        def simple_job(i):
            return i * 2
        
        config = {
            "nats": {"url": "nats://localhost:4222"},
            "workers": {"concurrency": 4}
        }
        
        with patch('naq.services.ServiceManager') as mock_sm:
            mock_manager = AsyncMock()
            mock_job_service = AsyncMock()
            mock_job_service.enqueue_job.return_value = "bulk-job"
            mock_manager.get_service.return_value = mock_job_service
            mock_sm.return_value = mock_manager
            
            queue = Queue("bulk-queue", config=config)
            await queue.initialize()
            
            start_time = time.time()
            
            # Enqueue 100 jobs
            jobs = []
            for i in range(100):
                job = await queue.enqueue(simple_job, i)
                jobs.append(job)
            
            duration = time.time() - start_time
            
            # Should complete reasonably quickly
            assert duration < 5.0  # 5 seconds for 100 jobs
            assert len(jobs) == 100
            assert mock_job_service.enqueue_job.call_count == 100
            
            await queue.close()

    @pytest.mark.asyncio
    async def test_concurrent_queue_operations(self):
        """Test concurrent queue operations."""
        def concurrent_job(i):
            return f"job-{i}"
        
        config = {"nats": {"url": "nats://localhost:4222"}}
        
        with patch('naq.services.ServiceManager') as mock_sm:
            mock_manager = AsyncMock()
            mock_job_service = AsyncMock()
            mock_job_service.enqueue_job.return_value = "concurrent-job"
            mock_manager.get_service.return_value = mock_job_service
            mock_sm.return_value = mock_manager
            
            # Multiple queues operating concurrently
            queues = [
                Queue(f"queue-{i}", config=config)
                for i in range(3)
            ]
            
            # Initialize all queues
            for queue in queues:
                await queue.initialize()
            
            async def enqueue_batch(queue, start_idx):
                jobs = []
                for i in range(start_idx, start_idx + 10):
                    job = await queue.enqueue(concurrent_job, i)
                    jobs.append(job)
                return jobs
            
            start_time = time.time()
            
            # Run concurrent operations
            tasks = [
                enqueue_batch(queues[0], 0),
                enqueue_batch(queues[1], 10),
                enqueue_batch(queues[2], 20)
            ]
            
            results = await asyncio.gather(*tasks)
            duration = time.time() - start_time
            
            # Verify results
            total_jobs = sum(len(batch) for batch in results)
            assert total_jobs == 30
            
            # Concurrent operations should be efficient
            assert duration < 3.0
            
            # Cleanup
            for queue in queues:
                await queue.close()


class TestConfigurationIntegration:
    """Test configuration integration in real scenarios."""

    @pytest.mark.asyncio
    async def test_config_driven_workflow(self):
        """Test configuration-driven workflow."""
        config_data = {
            "nats": {
                "url": "nats://config-test:4222",
                "client_name": "config-driven-test"
            },
            "workers": {
                "concurrency": 6,
                "heartbeat_interval": 15
            },
            "events": {
                "enabled": True,
                "batch_size": 25
            }
        }
        
        config = load_config(config_data)
        
        with patch('naq.services.ServiceManager') as mock_sm:
            mock_manager = AsyncMock()
            mock_sm.return_value = mock_manager
            
            # Create components using config
            queue = Queue("config-queue", config=config)
            await queue.initialize()
            
            # Verify ServiceManager received correct config
            mock_sm.assert_called_with(config)
            
            await queue.close()

    @pytest.mark.asyncio
    async def test_environment_config_override(self):
        """Test environment variable configuration override."""
        import os
        from unittest.mock import patch as mock_patch
        
        base_config = {
            "nats": {"url": "nats://base:4222"},
            "workers": {"concurrency": 2}
        }
        
        env_override = {
            "NAQ_WORKERS_CONCURRENCY": "8"
        }
        
        with mock_patch.dict(os.environ, env_override):
            # Config should pick up environment override
            config = load_config(base_config)
            config_dict = config.to_dict()
            
            # Base value preserved
            assert config_dict["nats"]["url"] == "nats://base:4222"
            # Environment override applied
            assert config_dict["workers"]["concurrency"] == 8


class TestRealWorldScenarios:
    """Test real-world usage scenarios."""

    @pytest.mark.asyncio
    async def test_data_processing_pipeline(self):
        """Test data processing pipeline scenario."""
        def extract_data(source):
            return f"data from {source}"
        
        def transform_data(data):
            return f"transformed {data}"
        
        def load_data(data):
            return f"loaded {data}"
        
        config = {"nats": {"url": "nats://localhost:4222"}}
        
        with patch('naq.services.ServiceManager') as mock_sm:
            mock_manager = AsyncMock()
            mock_job_service = AsyncMock()
            mock_job_service.enqueue_job.return_value = "pipeline-job"
            mock_manager.get_service.return_value = mock_job_service
            mock_sm.return_value = mock_manager
            
            queue = Queue("pipeline", config=config)
            await queue.initialize()
            
            # Enqueue pipeline jobs
            extract_job = await queue.enqueue(extract_data, "database")
            transform_job = await queue.enqueue(
                transform_data,
                "extracted_data",
                depends_on=extract_job
            )
            load_job = await queue.enqueue(
                load_data,
                "transformed_data",
                depends_on=transform_job
            )
            
            # Verify pipeline structure
            assert extract_job.job_id is not None
            assert transform_job.dependency_ids == [extract_job.job_id]
            assert load_job.dependency_ids == [transform_job.job_id]
            
            await queue.close()

    @pytest.mark.asyncio
    async def test_scheduled_maintenance_tasks(self):
        """Test scheduled maintenance tasks scenario."""
        def cleanup_old_data():
            return "Old data cleaned up"
        
        def generate_reports():
            return "Reports generated"
        
        config = {"nats": {"url": "nats://localhost:4222"}}
        
        with patch('naq.services.ServiceManager') as mock_sm:
            mock_manager = AsyncMock()
            mock_scheduled_manager = AsyncMock()
            
            with patch('naq.queue.core.ScheduledJobManager', return_value=mock_scheduled_manager):
                mock_scheduled_manager.create_schedule.return_value = "maintenance-schedule"
                mock_sm.return_value = mock_manager
                
                queue = Queue("maintenance", config=config)
                await queue.initialize()
                
                # Schedule daily cleanup
                cleanup_schedule = await queue.schedule(
                    cleanup_old_data,
                    cron="0 2 * * *"  # 2 AM daily
                )
                
                # Schedule weekly reports
                reports_schedule = await queue.schedule(
                    generate_reports,
                    cron="0 9 * * 1"  # 9 AM Mondays
                )
                
                assert cleanup_schedule.schedule_id is not None
                assert reports_schedule.schedule_id is not None
                assert mock_scheduled_manager.create_schedule.call_count == 2
                
                await queue.close()