"""Unit tests for synchronous API wrappers."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timedelta

from naq.sync_api import (
    enqueue_job_sync,
    enqueue_at_sync,
    enqueue_in_sync,
    purge_queue_sync,
    cancel_scheduled_job_sync,
    list_workers_sync,
)
from naq.models.jobs import Job
from naq.services.config import create_global_config, GlobalServiceConfig


class TestEnqueueJobSync:
    """Test cases for enqueue_job_sync function."""

    def test_enqueue_job_sync_basic(self):
        """Test basic enqueue_job_sync functionality."""
        def test_func(x):
            return x * 2
        
        mock_job = MagicMock(spec=Job)
        mock_job.job_id = "test_job_id"
        
        with patch('naq.sync_api.run_with_service_context') as mock_run:
            mock_run.return_value = mock_job
            
            result = enqueue_job_sync(test_func, 5)
            
            assert result == mock_job
            mock_run.assert_called_once()
            
            # Verify the async function was called correctly
            call_args = mock_run.call_args
            assert call_args[0][0].__name__ == '_enqueue_with_services'
            assert call_args[1]['nats_url'] == 'nats://localhost:4222'

    def test_enqueue_job_sync_with_custom_params(self):
        """Test enqueue_job_sync with custom parameters."""
        def test_func(x):
            return x * 2
        
        mock_job = MagicMock(spec=Job)
        mock_job.job_id = "test_job_id"
        
        custom_nats_url = "nats://custom:4222"
        custom_queue = "test_queue"
        custom_config = GlobalServiceConfig(nats_url=custom_nats_url)
        
        with patch('naq.sync_api.run_with_service_context') as mock_run:
            mock_run.return_value = mock_job
            
            result = enqueue_job_sync(
                test_func, 
                5, 
                queue_name=custom_queue,
                nats_url=custom_nats_url,
                max_retries=3,
                timeout=60,
                config=custom_config
            )
            
            assert result == mock_job
            mock_run.assert_called_once()
            
            # Verify the async function was called with correct parameters
            call_args = mock_run.call_args
            assert call_args[1]['nats_url'] == custom_nats_url
            assert call_args[1]['global_config'] == custom_config

    def test_enqueue_job_sync_error_handling(self):
        """Test enqueue_job_sync error handling."""
        def test_func(x):
            return x * 2
        
        with patch('naq.sync_api.run_with_service_context') as mock_run:
            mock_run.side_effect = ValueError("Test error")
            
            with pytest.raises(ValueError, match="Test error"):
                enqueue_job_sync(test_func, 5)

    def test_enqueue_job_sync_validation(self):
        """Test enqueue_job_sync parameter validation."""
        # Test with invalid function
        with pytest.raises(ValueError, match="Parameter 'func' must be of type"):
            enqueue_job_sync("not_a_function", 5)
        
        # Test with invalid queue_name
        def test_func(x):
            return x * 2
        
        with pytest.raises(ValueError, match="Parameter 'queue_name' must be of type"):
            enqueue_job_sync(test_func, 5, queue_name=123)
        
        # Test with invalid nats_url
        with pytest.raises(ValueError, match="Parameter 'nats_url' must be of type"):
            enqueue_job_sync(test_func, 5, nats_url=123)


class TestEnqueueAtSync:
    """Test cases for enqueue_at_sync function."""

    def test_enqueue_at_sync_basic(self):
        """Test basic enqueue_at_sync functionality."""
        def test_func(x):
            return x * 2
        
        scheduled_time = datetime.now() + timedelta(hours=1)
        mock_job = MagicMock(spec=Job)
        mock_job.job_id = "test_job_id"
        
        with patch('naq.sync_api.run_with_service_context') as mock_run:
            mock_run.return_value = mock_job
            
            result = enqueue_at_sync(scheduled_time, test_func, 5)
            
            assert result == mock_job
            mock_run.assert_called_once()
            
            # Verify the async function was called correctly
            call_args = mock_run.call_args
            assert call_args[0][0].__name__ == '_enqueue_at_with_services'

    def test_enqueue_at_sync_with_custom_params(self):
        """Test enqueue_at_sync with custom parameters."""
        def test_func(x):
            return x * 2
        
        scheduled_time = datetime.now() + timedelta(hours=1)
        mock_job = MagicMock(spec=Job)
        mock_job.job_id = "test_job_id"
        
        custom_nats_url = "nats://custom:4222"
        custom_queue = "test_queue"
        custom_config = GlobalServiceConfig(nats_url=custom_nats_url)
        
        with patch('naq.sync_api.run_with_service_context') as mock_run:
            mock_run.return_value = mock_job
            
            result = enqueue_at_sync(
                scheduled_time,
                test_func, 
                5, 
                queue_name=custom_queue,
                nats_url=custom_nats_url,
                max_retries=3,
                timeout=60,
                config=custom_config
            )
            
            assert result == mock_job
            mock_run.assert_called_once()
            
            # Verify the async function was called with correct parameters
            call_args = mock_run.call_args
            assert call_args[1]['nats_url'] == custom_nats_url
            assert call_args[1]['global_config'] == custom_config

    def test_enqueue_at_sync_validation(self):
        """Test enqueue_at_sync parameter validation."""
        def test_func(x):
            return x * 2
        
        # Test with invalid datetime
        with pytest.raises(ValueError, match="Parameter 'dt' must be of type"):
            enqueue_at_sync("not_a_datetime", test_func, 5)
        
        # Test with invalid function
        scheduled_time = datetime.now() + timedelta(hours=1)
        with pytest.raises(ValueError, match="Parameter 'func' must be of type"):
            enqueue_at_sync(scheduled_time, "not_a_function", 5)


class TestEnqueueInSync:
    """Test cases for enqueue_in_sync function."""

    def test_enqueue_in_sync_basic(self):
        """Test basic enqueue_in_sync functionality."""
        def test_func(x):
            return x * 2
        
        delay = timedelta(hours=1)
        mock_job = MagicMock(spec=Job)
        mock_job.job_id = "test_job_id"
        
        with patch('naq.sync_api.run_with_service_context') as mock_run:
            mock_run.return_value = mock_job
            
            result = enqueue_in_sync(delay, test_func, 5)
            
            assert result == mock_job
            mock_run.assert_called_once()
            
            # Verify the async function was called correctly
            call_args = mock_run.call_args
            assert call_args[0][0].__name__ == '_enqueue_in_with_services'

    def test_enqueue_in_sync_with_custom_params(self):
        """Test enqueue_in_sync with custom parameters."""
        def test_func(x):
            return x * 2
        
        delay = timedelta(hours=1)
        mock_job = MagicMock(spec=Job)
        mock_job.job_id = "test_job_id"
        
        custom_nats_url = "nats://custom:4222"
        custom_queue = "test_queue"
        custom_config = GlobalServiceConfig(nats_url=custom_nats_url)
        
        with patch('naq.sync_api.run_with_service_context') as mock_run:
            mock_run.return_value = mock_job
            
            result = enqueue_in_sync(
                delay,
                test_func, 
                5, 
                queue_name=custom_queue,
                nats_url=custom_nats_url,
                max_retries=3,
                timeout=60,
                config=custom_config
            )
            
            assert result == mock_job
            mock_run.assert_called_once()
            
            # Verify the async function was called with correct parameters
            call_args = mock_run.call_args
            assert call_args[1]['nats_url'] == custom_nats_url
            assert call_args[1]['global_config'] == custom_config

    def test_enqueue_in_sync_validation(self):
        """Test enqueue_in_sync parameter validation."""
        def test_func(x):
            return x * 2
        
        # Test with invalid timedelta
        with pytest.raises(ValueError, match="Parameter 'delta' must be of type"):
            enqueue_in_sync("not_a_timedelta", test_func, 5)
        
        # Test with invalid function
        delay = timedelta(hours=1)
        with pytest.raises(ValueError, match="Parameter 'func' must be of type"):
            enqueue_in_sync(delay, "not_a_function", 5)


class TestPurgeQueueSync:
    """Test cases for purge_queue_sync function."""

    def test_purge_queue_sync_basic(self):
        """Test basic purge_queue_sync functionality."""
        with patch('naq.sync_api.run_with_service_context') as mock_run:
            mock_run.return_value = 5
            
            result = purge_queue_sync()
            
            assert result == 5
            mock_run.assert_called_once()
            
            # Verify the async function was called correctly
            call_args = mock_run.call_args
            assert call_args[0][0].__name__ == '_purge_with_services'

    def test_purge_queue_sync_with_custom_params(self):
        """Test purge_queue_sync with custom parameters."""
        custom_nats_url = "nats://custom:4222"
        custom_queue = "test_queue"
        custom_config = GlobalServiceConfig(nats_url=custom_nats_url)
        
        with patch('naq.sync_api.run_with_service_context') as mock_run:
            mock_run.return_value = 5
            
            result = purge_queue_sync(
                queue_name=custom_queue,
                nats_url=custom_nats_url,
                config=custom_config
            )
            
            assert result == 5
            mock_run.assert_called_once()
            
            # Verify the async function was called with correct parameters
            call_args = mock_run.call_args
            assert call_args[1]['nats_url'] == custom_nats_url
            assert call_args[1]['global_config'] == custom_config

    def test_purge_queue_sync_validation(self):
        """Test purge_queue_sync parameter validation."""
        # Test with invalid queue_name
        with pytest.raises(ValueError, match="Parameter 'queue_name' must be of type"):
            purge_queue_sync(queue_name=123)
        
        # Test with invalid nats_url
        with pytest.raises(ValueError, match="Parameter 'nats_url' must be of type"):
            purge_queue_sync(nats_url=123)


class TestCancelScheduledJobSync:
    """Test cases for cancel_scheduled_job_sync function."""

    def test_cancel_scheduled_job_sync_basic(self):
        """Test basic cancel_scheduled_job_sync functionality."""
        with patch('naq.sync_api.run_with_service_context') as mock_run:
            mock_run.return_value = True
            
            result = cancel_scheduled_job_sync("test_job_id")
            
            assert result is True
            mock_run.assert_called_once()
            
            # Verify the async function was called correctly
            call_args = mock_run.call_args
            assert call_args[0][0].__name__ == '_cancel_with_services'

    def test_cancel_scheduled_job_sync_with_custom_params(self):
        """Test cancel_scheduled_job_sync with custom parameters."""
        custom_nats_url = "nats://custom:4222"
        custom_config = GlobalServiceConfig(nats_url=custom_nats_url)
        
        with patch('naq.sync_api.run_with_service_context') as mock_run:
            mock_run.return_value = True
            
            result = cancel_scheduled_job_sync(
                "test_job_id",
                nats_url=custom_nats_url,
                config=custom_config
            )
            
            assert result is True
            mock_run.assert_called_once()
            
            # Verify the async function was called with correct parameters
            call_args = mock_run.call_args
            assert call_args[1]['nats_url'] == custom_nats_url
            assert call_args[1]['global_config'] == custom_config

    def test_cancel_scheduled_job_sync_validation(self):
        """Test cancel_scheduled_job_sync parameter validation."""
        # Test with invalid job_id
        with pytest.raises(ValueError, match="Parameter 'job_id' must be of type"):
            cancel_scheduled_job_sync(123)
        
        # Test with invalid nats_url
        with pytest.raises(ValueError, match="Parameter 'nats_url' must be of type"):
            cancel_scheduled_job_sync("test_job_id", nats_url=123)


class TestListWorkersSync:
    """Test cases for list_workers_sync function."""

    def test_list_workers_sync_basic(self):
        """Test basic list_workers_sync functionality."""
        mock_workers = [
            {"worker_id": "worker1", "status": "active"},
            {"worker_id": "worker2", "status": "idle"}
        ]
        
        with patch('naq.sync_api.run_with_service_context') as mock_run:
            mock_run.return_value = mock_workers
            
            result = list_workers_sync()
            
            assert result == mock_workers
            mock_run.assert_called_once()
            
            # Verify the async function was called correctly
            call_args = mock_run.call_args
            assert call_args[0][0].__name__ == '_list_with_services'

    def test_list_workers_sync_with_custom_params(self):
        """Test list_workers_sync with custom parameters."""
        custom_nats_url = "nats://custom:4222"
        custom_config = GlobalServiceConfig(nats_url=custom_nats_url)
        
        mock_workers = [
            {"worker_id": "worker1", "status": "active"},
            {"worker_id": "worker2", "status": "idle"}
        ]
        
        with patch('naq.sync_api.run_with_service_context') as mock_run:
            mock_run.return_value = mock_workers
            
            result = list_workers_sync(
                nats_url=custom_nats_url,
                config=custom_config
            )
            
            assert result == mock_workers
            mock_run.assert_called_once()
            
            # Verify the async function was called with correct parameters
            call_args = mock_run.call_args
            assert call_args[1]['nats_url'] == custom_nats_url
            assert call_args[1]['global_config'] == custom_config

    def test_list_workers_sync_validation(self):
        """Test list_workers_sync parameter validation."""
        # Test with invalid nats_url
        with pytest.raises(ValueError, match="Parameter 'nats_url' must be of type"):
            list_workers_sync(nats_url=123)


class TestSyncApiIntegration:
    """Integration tests for synchronous API wrappers."""

    def test_sync_api_with_real_service_context(self):
        """Test that sync API functions properly use service context."""
        def test_func(x):
            return x * 2
        
        mock_job = MagicMock(spec=Job)
        mock_job.job_id = "test_job_id"
        
        with patch('naq.sync_api.run_with_service_context') as mock_run:
            mock_run.return_value = mock_job
            
            # Test multiple sync API functions
            enqueue_job_sync(test_func, 5)
            enqueue_at_sync(datetime.now() + timedelta(hours=1), test_func, 5)
            enqueue_in_sync(timedelta(hours=1), test_func, 5)
            purge_queue_sync()
            cancel_scheduled_job_sync("test_job_id")
            list_workers_sync()
            
            # Verify all functions called run_with_service_context
            assert mock_run.call_count == 6

    def test_sync_api_error_propagation(self):
        """Test that errors are properly propagated through sync API functions."""
        def test_func(x):
            return x * 2
        
        with patch('naq.sync_api.run_with_service_context') as mock_run:
            mock_run.side_effect = RuntimeError("Service error")
            
            # Test that the same error is propagated
            with pytest.raises(RuntimeError, match="Service error"):
                enqueue_job_sync(test_func, 5)
            
            with pytest.raises(RuntimeError, match="Service error"):
                enqueue_at_sync(datetime.now() + timedelta(hours=1), test_func, 5)
            
            with pytest.raises(RuntimeError, match="Service error"):
                enqueue_in_sync(timedelta(hours=1), test_func, 5)
            
            with pytest.raises(RuntimeError, match="Service error"):
                purge_queue_sync()
            
            with pytest.raises(RuntimeError, match="Service error"):
                cancel_scheduled_job_sync("test_job_id")
            
            with pytest.raises(RuntimeError, match="Service error"):
                list_workers_sync()