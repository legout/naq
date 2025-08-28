"""Integration tests for synchronous API wrappers."""

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

from naq.queue.sync_api import (
    enqueue_sync,
    enqueue_at_sync,
    enqueue_in_sync,
    purge_queue_sync,
    cancel_scheduled_job_sync,
)
from naq.queue.sync_api import (
    enqueue_sync,
    enqueue_at_sync as queue_enqueue_at_sync,
    enqueue_in_sync as queue_enqueue_in_sync,
    purge_queue_sync as queue_purge_queue_sync,
    cancel_scheduled_job_sync as queue_cancel_scheduled_job_sync,
    pause_scheduled_job_sync,
    resume_scheduled_job_sync,
    modify_scheduled_job_sync,
    schedule_sync,
    close_sync_connections,
)
from naq.models.jobs import Job
from naq.services.config import create_global_config, GlobalServiceConfig


class TestSyncApiIntegration:
    """Integration tests for synchronous API wrappers."""

    def test_enqueue_sync_integration(self):
        """Test enqueue_sync with real service context."""
        def test_func(x):
            return x * 2
        
        # Mock the underlying async API to avoid network calls
        with patch('naq.sync_api.enqueue') as mock_enqueue:
            mock_job = MagicMock(spec=Job)
            mock_job.job_id = "test_job_id"
            mock_enqueue.return_value = mock_job
            
            result = enqueue_sync(test_func, 5)
            
            assert result == mock_job
            mock_enqueue.assert_called_once()
            
            # Verify the async function was called with correct parameters
            call_args = mock_enqueue.call_args
            assert call_args[1]['queue_name'] == 'default'
            assert call_args[1]['nats_url'] == 'nats://localhost:4222'
            assert call_args[1]['prefer_thread_local'] is False

    def test_enqueue_at_sync_integration(self):
        """Test enqueue_at_sync with real service context."""
        def test_func(x):
            return x * 2
        
        scheduled_time = datetime.now() + timedelta(hours=1)
        
        # Mock the underlying async API to avoid network calls
        with patch('naq.sync_api.enqueue_at') as mock_enqueue_at:
            mock_job = MagicMock(spec=Job)
            mock_job.job_id = "test_job_id"
            mock_enqueue_at.return_value = mock_job
            
            result = enqueue_at_sync(scheduled_time, test_func, 5)
            
            assert result == mock_job
            mock_enqueue_at.assert_called_once()
            
            # Verify the async function was called with correct parameters
            call_args = mock_enqueue_at.call_args
            assert call_args[0][0] == scheduled_time
            assert call_args[1]['queue_name'] == 'default'
            assert call_args[1]['nats_url'] == 'nats://localhost:4222'
            assert call_args[1]['prefer_thread_local'] is False

    def test_enqueue_in_sync_integration(self):
        """Test enqueue_in_sync with real service context."""
        def test_func(x):
            return x * 2
        
        delay = timedelta(hours=1)
        
        # Mock the underlying async API to avoid network calls
        with patch('naq.sync_api.enqueue_in') as mock_enqueue_in:
            mock_job = MagicMock(spec=Job)
            mock_job.job_id = "test_job_id"
            mock_enqueue_in.return_value = mock_job
            
            result = enqueue_in_sync(delay, test_func, 5)
            
            assert result == mock_job
            mock_enqueue_in.assert_called_once()
            
            # Verify the async function was called with correct parameters
            call_args = mock_enqueue_in.call_args
            assert call_args[0][0] == delay
            assert call_args[1]['queue_name'] == 'default'
            assert call_args[1]['nats_url'] == 'nats://localhost:4222'
            assert call_args[1]['prefer_thread_local'] is False

    def test_purge_queue_sync_integration(self):
        """Test purge_queue_sync with real service context."""
        # Mock the underlying async API to avoid network calls
        with patch('naq.sync_api.purge_queue') as mock_purge_queue:
            mock_purge_queue.return_value = 5
            
            result = purge_queue_sync()
            
            assert result == 5
            mock_purge_queue.assert_called_once()
            
            # Verify the async function was called with correct parameters
            call_args = mock_purge_queue.call_args
            assert call_args[1]['queue_name'] == 'default'
            assert call_args[1]['nats_url'] == 'nats://localhost:4222'
            assert call_args[1]['prefer_thread_local'] is False

    def test_cancel_scheduled_job_sync_integration(self):
        """Test cancel_scheduled_job_sync with real service context."""
        # Mock the underlying async API to avoid network calls
        with patch('naq.sync_api.cancel_scheduled_job') as mock_cancel:
            mock_cancel.return_value = True
            
            result = cancel_scheduled_job_sync("test_job_id")
            
            assert result is True
            mock_cancel.assert_called_once()
            
            # Verify the async function was called with correct parameters
            call_args = mock_cancel.call_args
            assert call_args[0][0] == "test_job_id"
            assert call_args[1]['nats_url'] == 'nats://localhost:4222'
            assert call_args[1]['prefer_thread_local'] is False

    


class TestQueueSyncApiIntegration:
    """Integration tests for queue synchronous API wrappers."""

    def test_enqueue_sync_integration(self):
        """Test enqueue_sync with real service context."""
        def test_func(x):
            return x * 2
        
        # Mock the underlying async API to avoid network calls
        with patch('naq.queue.sync_api.enqueue') as mock_enqueue:
            mock_job = MagicMock(spec=Job)
            mock_job.job_id = "test_job_id"
            mock_enqueue.return_value = mock_job
            
            result = enqueue_sync(test_func, 5)
            
            assert result == mock_job
            mock_enqueue.assert_called_once()
            
            # Verify the async function was called with correct parameters
            call_args = mock_enqueue.call_args
            assert call_args[1]['queue_name'] == 'default'
            assert call_args[1]['nats_url'] == 'nats://localhost:4222'
            assert call_args[1]['prefer_thread_local'] is False

    def test_queue_enqueue_at_sync_integration(self):
        """Test queue_enqueue_at_sync with real service context."""
        def test_func(x):
            return x * 2
        
        scheduled_time = datetime.now() + timedelta(hours=1)
        
        # Mock the underlying async API to avoid network calls
        with patch('naq.queue.sync_api.enqueue_at') as mock_enqueue_at:
            mock_job = MagicMock(spec=Job)
            mock_job.job_id = "test_job_id"
            mock_enqueue_at.return_value = mock_job
            
            result = queue_enqueue_at_sync(scheduled_time, test_func, 5)
            
            assert result == mock_job
            mock_enqueue_at.assert_called_once()
            
            # Verify the async function was called with correct parameters
            call_args = mock_enqueue_at.call_args
            assert call_args[0][0] == scheduled_time
            assert call_args[1]['queue_name'] == 'default'
            assert call_args[1]['nats_url'] == 'nats://localhost:4222'
            assert call_args[1]['prefer_thread_local'] is False

    def test_queue_enqueue_in_sync_integration(self):
        """Test queue_enqueue_in_sync with real service context."""
        def test_func(x):
            return x * 2
        
        delay = timedelta(hours=1)
        
        # Mock the underlying async API to avoid network calls
        with patch('naq.queue.sync_api.enqueue_in') as mock_enqueue_in:
            mock_job = MagicMock(spec=Job)
            mock_job.job_id = "test_job_id"
            mock_enqueue_in.return_value = mock_job
            
            result = queue_enqueue_in_sync(delay, test_func, 5)
            
            assert result == mock_job
            mock_enqueue_in.assert_called_once()
            
            # Verify the async function was called with correct parameters
            call_args = mock_enqueue_in.call_args
            assert call_args[0][0] == delay
            assert call_args[1]['queue_name'] == 'default'
            assert call_args[1]['nats_url'] == 'nats://localhost:4222'
            assert call_args[1]['prefer_thread_local'] is False

    def test_schedule_sync_integration(self):
        """Test schedule_sync with real service context."""
        def test_func(x):
            return x * 2
        
        # Mock the underlying async API to avoid network calls
        with patch('naq.queue.sync_api.schedule') as mock_schedule:
            mock_job = MagicMock(spec=Job)
            mock_job.job_id = "test_job_id"
            mock_schedule.return_value = mock_job
            
            result = schedule_sync(test_func, 5, cron="0 0 * * *")
            
            assert result == mock_job
            mock_schedule.assert_called_once()
            
            # Verify the async function was called with correct parameters
            call_args = mock_schedule.call_args
            assert call_args[1]['queue_name'] == 'default'
            assert call_args[1]['nats_url'] == 'nats://localhost:4222'
            assert call_args[1]['cron'] == "0 0 * * *"
            assert call_args[1]['prefer_thread_local'] is False

    def test_queue_purge_queue_sync_integration(self):
        """Test queue_purge_queue_sync with real service context."""
        # Mock the underlying async API to avoid network calls
        with patch('naq.queue.sync_api.purge_queue') as mock_purge_queue:
            mock_purge_queue.return_value = 5
            
            result = queue_purge_queue_sync()
            
            assert result == 5
            mock_purge_queue.assert_called_once()
            
            # Verify the async function was called with correct parameters
            call_args = mock_purge_queue.call_args
            assert call_args[1]['queue_name'] == 'default'
            assert call_args[1]['nats_url'] == 'nats://localhost:4222'
            assert call_args[1]['prefer_thread_local'] is False

    def test_queue_cancel_scheduled_job_sync_integration(self):
        """Test queue_cancel_scheduled_job_sync with real service context."""
        # Mock the underlying async API to avoid network calls
        with patch('naq.queue.sync_api.cancel_scheduled_job') as mock_cancel:
            mock_cancel.return_value = True
            
            result = queue_cancel_scheduled_job_sync("test_job_id")
            
            assert result is True
            mock_cancel.assert_called_once()
            
            # Verify the async function was called with correct parameters
            call_args = mock_cancel.call_args
            assert call_args[0][0] == "test_job_id"
            assert call_args[1]['nats_url'] == 'nats://localhost:4222'
            assert call_args[1]['prefer_thread_local'] is False

    def test_pause_scheduled_job_sync_integration(self):
        """Test pause_scheduled_job_sync with real service context."""
        # Mock the underlying async API to avoid network calls
        with patch('naq.queue.sync_api.pause_scheduled_job') as mock_pause:
            mock_pause.return_value = True
            
            result = pause_scheduled_job_sync("test_job_id")
            
            assert result is True
            mock_pause.assert_called_once()
            
            # Verify the async function was called with correct parameters
            call_args = mock_pause.call_args
            assert call_args[0][0] == "test_job_id"
            assert call_args[1]['nats_url'] == 'nats://localhost:4222'
            assert call_args[1]['prefer_thread_local'] is False

    def test_resume_scheduled_job_sync_integration(self):
        """Test resume_scheduled_job_sync with real service context."""
        # Mock the underlying async API to avoid network calls
        with patch('naq.queue.sync_api.resume_scheduled_job') as mock_resume:
            mock_resume.return_value = True
            
            result = resume_scheduled_job_sync("test_job_id")
            
            assert result is True
            mock_resume.assert_called_once()
            
            # Verify the async function was called with correct parameters
            call_args = mock_resume.call_args
            assert call_args[0][0] == "test_job_id"
            assert call_args[1]['nats_url'] == 'nats://localhost:4222'
            assert call_args[1]['prefer_thread_local'] is False

    def test_modify_scheduled_job_sync_integration(self):
        """Test modify_scheduled_job_sync with real service context."""
        # Mock the underlying async API to avoid network calls
        with patch('naq.queue.sync_api.modify_scheduled_job') as mock_modify:
            mock_modify.return_value = True
            
            result = modify_scheduled_job_sync("test_job_id", timeout=60)
            
            assert result is True
            mock_modify.assert_called_once()
            
            # Verify the async function was called with correct parameters
            call_args = mock_modify.call_args
            assert call_args[0][0] == "test_job_id"
            assert call_args[1]['nats_url'] == 'nats://localhost:4222'
            assert call_args[1]['prefer_thread_local'] is False
            assert call_args[1]['timeout'] == 60

    def test_close_sync_connections_integration(self):
        """Test close_sync_connections with real service context."""
        # Mock the underlying async API to avoid network calls
        with patch('naq.queue.sync_api.run_with_service_context') as mock_run:
            close_sync_connections()
            
            mock_run.assert_called_once()
            
            # Verify the async function was called with correct parameters
            call_args = mock_run.call_args
            assert call_args[1]['nats_url'] == 'nats://localhost:4222'
            assert call_args[1]['logger_name'] == 'naq.queue.sync_api.close_sync_connections'


class TestSyncApiWithCustomConfig:
    """Integration tests for synchronous API with custom configuration."""

    def test_sync_api_with_custom_config(self):
        """Test that sync APIs work with custom configuration."""
        def test_func(x):
            return x * 2
        
        custom_nats_url = "nats://custom:4222"
        custom_config = GlobalServiceConfig(
            nats_url=custom_nats_url,
            connection_timeout=30,
            request_timeout=10,
        )
        
        # Mock the underlying async API to avoid network calls
        with patch('naq.sync_api.enqueue') as mock_enqueue:
            mock_job = MagicMock(spec=Job)
            mock_job.job_id = "test_job_id"
            mock_enqueue.return_value = mock_job
            
            result = enqueue_sync(
                test_func, 
                5, 
                nats_url=custom_nats_url,
                config=custom_config
            )
            
            assert result == mock_job
            mock_enqueue.assert_called_once()
            
            # Verify the async function was called with correct parameters
            call_args = mock_enqueue.call_args
            assert call_args[1]['nats_url'] == custom_nats_url
            assert call_args[1]['config'].nats_url == custom_nats_url
            assert call_args[1]['config'].connection_timeout == 30
            assert call_args[1]['config'].request_timeout == 10

    def test_queue_sync_api_with_custom_config(self):
        """Test that queue sync APIs work with custom configuration."""
        def test_func(x):
            return x * 2
        
        custom_nats_url = "nats://custom:4222"
        custom_config = GlobalServiceConfig(
            nats_url=custom_nats_url,
            connection_timeout=30,
            request_timeout=10,
        )
        
        # Mock the underlying async API to avoid network calls
        with patch('naq.queue.sync_api.enqueue') as mock_enqueue:
            mock_job = MagicMock(spec=Job)
            mock_job.job_id = "test_job_id"
            mock_enqueue.return_value = mock_job
            
            result = enqueue_sync(
                test_func, 
                5, 
                nats_url=custom_nats_url,
                config=custom_config
            )
            
            assert result == mock_job
            mock_enqueue.assert_called_once()
            
            # Verify the async function was called with correct parameters
            call_args = mock_enqueue.call_args
            assert call_args[1]['nats_url'] == custom_nats_url
            assert call_args[1]['config'].nats_url == custom_nats_url
            assert call_args[1]['config'].connection_timeout == 30
            assert call_args[1]['config'].request_timeout == 10


class TestSyncApiErrorHandling:
    """Integration tests for synchronous API error handling."""

    def test_sync_api_error_propagation(self):
        """Test that errors are properly propagated through sync APIs."""
        def test_func(x):
            return x * 2
        
        # Mock the underlying async API to raise an error
        with patch('naq.sync_api.enqueue') as mock_enqueue:
            mock_enqueue.side_effect = RuntimeError("Service error")
            
            with pytest.raises(RuntimeError, match="Service error"):
                enqueue_sync(test_func, 5)

    def test_queue_sync_api_error_propagation(self):
        """Test that errors are properly propagated through queue sync APIs."""
        def test_func(x):
            return x * 2
        
        # Mock the underlying async API to raise an error
        with patch('naq.queue.sync_api.enqueue') as mock_enqueue:
            mock_enqueue.side_effect = RuntimeError("Service error")
            
            with pytest.raises(RuntimeError, match="Service error"):
                enqueue_sync(test_func, 5)

    def test_sync_api_service_initialization_error(self):
        """Test that service initialization errors are handled properly."""
        def test_func(x):
            return x * 2
        
        # Mock the service context to raise an error during initialization
        with patch('naq.sync_api.run_with_service_context') as mock_run:
            mock_run.side_effect = RuntimeError("Service initialization failed")
            
            with pytest.raises(RuntimeError, match="Service initialization failed"):
                enqueue_sync(test_func, 5)

    def test_queue_sync_api_service_initialization_error(self):
        """Test that service initialization errors are handled properly in queue sync APIs."""
        def test_func(x):
            return x * 2
        
        # Mock the service context to raise an error during initialization
        with patch('naq.queue.sync_api.run_with_service_context') as mock_run:
            mock_run.side_effect = RuntimeError("Service initialization failed")
            
            with pytest.raises(RuntimeError, match="Service initialization failed"):
                enqueue_sync(test_func, 5)