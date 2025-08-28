"""Tests for the SyncClient implementation."""

import pytest
from datetime import datetime, timedelta
from unittest.mock import patch, AsyncMock

from naq.client import SyncClient
from naq.models.jobs import Job


class TestSyncClient:
    """Test cases for the SyncClient class."""

    def test_sync_client_initialization(self):
        """Test that SyncClient can be initialized correctly."""
        client = SyncClient()
        assert client.nats_url is not None
        assert client.config is None
        assert client._jobs == []

    def test_sync_client_context_manager(self):
        """Test that SyncClient works as a context manager."""
        with SyncClient() as client:
            assert isinstance(client, SyncClient)

    @patch('naq.client.enqueue_sync')
    def test_sync_client_enqueue(self, mock_enqueue_sync):
        """Test that SyncClient.enqueue calls enqueue_sync correctly."""
        # Setup mock
        mock_job = AsyncMock(spec=Job)
        mock_enqueue_sync.return_value = mock_job

        # Test
        with SyncClient() as client:
            job = client.enqueue(lambda x: x, 1, queue_name="test_queue")
            
            # Assertions while context manager is active
            assert job == mock_job
            assert len(client.jobs) == 1
            assert client.jobs[0] == mock_job

        mock_enqueue_sync.assert_called_once()

    @patch('naq.client.enqueue_at_sync')
    def test_sync_client_enqueue_at(self, mock_enqueue_at_sync):
        """Test that SyncClient.enqueue_at calls enqueue_at_sync correctly."""
        # Setup mock
        mock_job = AsyncMock(spec=Job)
        mock_enqueue_at_sync.return_value = mock_job
        future_time = datetime.now() + timedelta(hours=1)

        # Test
        with SyncClient() as client:
            job = client.enqueue_at(future_time, lambda x: x, 1)
            
            # Assertions while context manager is active
            assert job == mock_job
            assert len(client.jobs) == 1

        mock_enqueue_at_sync.assert_called_once()

    @patch('naq.client.enqueue_in_sync')
    def test_sync_client_enqueue_in(self, mock_enqueue_in_sync):
        """Test that SyncClient.enqueue_in calls enqueue_in_sync correctly."""
        # Setup mock
        mock_job = AsyncMock(spec=Job)
        mock_enqueue_in_sync.return_value = mock_job
        delay = timedelta(minutes=30)

        # Test
        with SyncClient() as client:
            job = client.enqueue_in(delay, lambda x: x, 1)
            
            # Assertions while context manager is active
            assert job == mock_job
            assert len(client.jobs) == 1

        mock_enqueue_in_sync.assert_called_once()

    @patch('naq.client.purge_queue_sync')
    def test_sync_client_purge_queue(self, mock_purge_queue_sync):
        """Test that SyncClient.purge_queue calls purge_queue_sync correctly."""
        # Setup mock
        mock_purge_queue_sync.return_value = 5

        # Test
        with SyncClient() as client:
            count = client.purge_queue("test_queue")

        # Assertions
        assert count == 5
        mock_purge_queue_sync.assert_called_once_with(
            queue_name="test_queue",
            nats_url=client.nats_url,
            config=client.config,
        )