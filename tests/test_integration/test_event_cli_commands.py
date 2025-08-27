"""Integration tests for CLI event commands."""

import json
import time
from unittest.mock import patch, MagicMock, AsyncMock
import pytest
import anyio

from naq.cli.event_commands import (
    EventCommandHandler,
    display_event,
    display_event_table,
    display_stats_table,
    display_worker_table
)
from naq.models.events import JobEvent, WorkerEvent
from naq.models.enums import JobEventType, WORKER_STATUS, WorkerEventType
from naq.exceptions import NaqConnectionError


class TestEventCommandHandler:
    """Test the EventCommandHandler class."""

    def test_initialization(self):
        """Test EventCommandHandler initialization."""
        handler = EventCommandHandler()
        assert handler.console is not None
        assert handler.structured_logger is not None
        assert handler.service_manager is None
        assert handler.event_service is None

    def test_validate_common_parameters(self):
        """Test EventCommandHandler.validate_common_parameters method."""
        handler = EventCommandHandler()
        
        # Test valid parameters
        handler.validate_common_parameters(
            nats_url="nats://localhost:4222",
            log_level="INFO",
            limit=100,
            worker_id="test_worker"
        )
        
        # Test invalid nats_url
        with pytest.raises(Exception):  # Should raise ValidationError
            handler.validate_common_parameters(nats_url="invalid_url")
        
        # Test invalid log_level
        with pytest.raises(Exception):  # Should raise ValidationError
            handler.validate_common_parameters(
                nats_url="nats://localhost:4222",
                log_level="INVALID"
            )
        
        # Test invalid limit
        with pytest.raises(Exception):  # Should raise ValidationError
            handler.validate_common_parameters(
                nats_url="nats://localhost:4222",
                limit=0
            )


class TestDisplayFunctions:
    """Test the display functions."""

    def test_display_event(self):
        """Test display_event function with different formats."""
        # Create test event
        event = JobEvent(
            job_id="test_job_1",
            event_type=JobEventType.ENQUEUED,
            timestamp=time.time(),
            worker_id="worker_1",
            queue_name="test_queue",
            message="Test job created"
        )
        
        # Test table format
        with patch("rich.console.Console.print") as mock_print:
            display_event(event, format_type="table")
            assert mock_print.called
            
        # Test json format
        with patch("rich.console.Console.print") as mock_print:
            display_event(event, format_type="json")
            assert mock_print.called
            
        # Test raw format
        with patch("rich.console.Console.print") as mock_print:
            display_event(event, format_type="raw")
            assert mock_print.called

    def test_display_event_table(self):
        """Test display_event_table function."""
        # Create test events
        events = [
            JobEvent(
                job_id="test_job_1",
                event_type=JobEventType.ENQUEUED,
                timestamp=time.time(),
                worker_id="worker_1",
                queue_name="test_queue",
                message="Test job created"
            ),
            WorkerEvent(
                worker_id="worker_1",
                event_type=WorkerEventType.STARTED,
                timestamp=time.time(),
                queue_names=["test_queue"],
                message="Worker started"
            )
        ]
        
        # Mock console.print
        with patch("rich.console.Console.print") as mock_print:
            display_event_table(events)
            
            # Verify that print was called
            assert mock_print.called

    def test_display_stats_table(self):
        """Test display_stats_table function."""
        # Create test stats
        stats_data = {
            "total_events": 100,
            "job_events": 80,
            "worker_events": 20,
            "events_by_type": {
                "JOB_CREATED": 30,
                "JOB_STARTED": 25,
                "JOB_COMPLETED": 20,
                "JOB_FAILED": 5
            },
            "events_by_hour": {
                "2024-01-01T10:00:00": 10,
                "2024-01-01T11:00:00": 15
            }
        }
        
        # Mock console.print
        with patch("naq.cli.event_commands.Console.print") as mock_print:
            display_stats_table(stats_data)
            
            # Verify that print was called
            assert mock_print.called

    def test_display_worker_table(self):
        """Test display_worker_table function."""
        # Create test worker data
        workers = [
            {
                "worker_id": "worker_1",
                "status": "idle",
                "queues": ["test_queue"],
                "current_job_id": None,
                "last_heartbeat_utc": time.time()
            },
            {
                "worker_id": "worker_2",
                "status": "busy",
                "queues": ["test_queue"],
                "current_job_id": "job_123",
                "last_heartbeat_utc": time.time()
            }
        ]
        
        # Mock console.print
        with patch("naq.cli.event_commands.Console.print") as mock_print:
            display_worker_table(workers)
            
            # Verify that print was called
            assert mock_print.called

    def test_display_event_table_empty(self):
        """Test display_event_table function with empty events."""
        # Mock console.print
        with patch("naq.cli.event_commands.Console.print") as mock_print:
            display_event_table([])
            
            # Verify that "No events found" message was printed
            assert mock_print.called
            assert "No events found" in str(mock_print.call_args)

    def test_display_worker_table_empty(self):
        """Test display_worker_table function with empty workers."""
        # Mock console.print
        with patch("naq.cli.event_commands.Console.print") as mock_print:
            display_worker_table([])
            
            # Verify that "No workers found" message was printed
            assert mock_print.called
            assert "No workers found" in str(mock_print.call_args)


class TestErrorHandling:
    """Test error handling in event commands."""

    @pytest.mark.anyio
    async def test_event_command_handler_error_handling(self):
        """Test EventCommandHandler error handling."""
        handler = EventCommandHandler()
        
        # Test with invalid parameters that should raise exceptions
        with pytest.raises(Exception):
            handler.validate_common_parameters(nats_url="")
        
        with pytest.raises(Exception):
            handler.validate_common_parameters(
                nats_url="nats://localhost:4222",
                log_level="INVALID_LEVEL"
            )
        
        with pytest.raises(Exception):
            handler.validate_common_parameters(
                nats_url="nats://localhost:4222",
                limit=-1
            )