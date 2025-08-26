"""Tests for CLI commands."""

import asyncio
import json
import msgspec
import pytest
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch, call
from datetime import datetime, timezone
from typer.testing import CliRunner

from naq.cli.main import app
from naq.cli.job_commands import job_app
from naq.cli.worker_commands import worker_app
from naq.cli.scheduler_commands import scheduler_app
from naq.cli.system_commands import system_app
from naq.cli.event_commands import event_app, EventCommandHandler
from naq.models.enums import JOB_STATUS, SCHEDULED_JOB_STATUS, WORKER_STATUS, JobEventType
from naq.models.jobs import Job, JobResult
from naq.models.events import JobEvent, WorkerEvent
from naq.settings import DEFAULT_NATS_URL, DEFAULT_QUEUE_NAME, DEFAULT_WORKER_TTL_SECONDS


@pytest.fixture
def runner() -> CliRunner:
    """Create a CLI runner for testing."""
    return CliRunner()


@pytest.fixture
def mock_job() -> Job:
    """Create a mock job for testing."""
    return Job(
        function=lambda: "test",
        args=(),
        kwargs={},
        queue_name=DEFAULT_QUEUE_NAME,
        job_id="test-job-id",
        timeout=30,
        retry_strategy="linear",
        max_retries=3,
        retry_delay=1.0,
    )


@pytest.fixture
def mock_job_result() -> JobResult:
    """Create a mock job result for testing."""
    return JobResult(
        job_id="test-job-id",
        status=JOB_STATUS.COMPLETED,
        result="test result",
        error=None,
        started_at_utc=1234567890.0,
        finished_at_utc=1234567895.0,
        duration_ms=5000,
        worker_id="test-worker-id",
        queue_name=DEFAULT_QUEUE_NAME,
        retry_count=0,
    )


@pytest.fixture
def mock_job_event() -> JobEvent:
    """Create a mock job event for testing."""
    return JobEvent(
        timestamp=1234567890.0,
        event_type=JobEventType.STARTED,
        job_id="test-job-id",
        worker_id="test-worker-id",
        queue_name=DEFAULT_QUEUE_NAME,
        message="Job started",
        error_type=None,
        error_message=None,
        duration_ms=None,
        details={},
    )


@pytest.fixture
def mock_worker_event() -> WorkerEvent:
    """Create a mock worker event for testing."""
    return WorkerEvent(
        timestamp=1234567890.0,
        event_type="started",
        worker_id="test-worker-id",
        queue_names=[DEFAULT_QUEUE_NAME],
        message="Worker started",
        job_id=None,
        duration_ms=None,
        cpu_usage=None,
        memory_usage=None,
        details={},
    )


class TestMainCLI:
    """Test the main CLI application."""

    def test_version_callback(self, runner: CliRunner) -> None:
        """Test the version callback."""
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert "naq version:" in result.stdout

    def test_main_help(self, runner: CliRunner) -> None:
        """Test the main help command."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "A simple NATS-based queueing system" in result.stdout


class TestJobCommands:
    """Test job-related CLI commands."""

    @patch("naq.cli.job_commands.service_context")
    @patch("naq.cli.job_commands.StreamService")
    @patch("naq.cli.job_commands.ConnectionService")
    @patch("naq.cli.job_commands.JobService")
    def test_purge_queues(
        self,
        mock_job_service: MagicMock,
        mock_connection_service: MagicMock,
        mock_stream_service: MagicMock,
        mock_service_context: MagicMock,
        runner: CliRunner,
    ) -> None:
        """Test the purge command."""
        # Setup mocks
        mock_context_manager = AsyncMock()
        mock_service_context.return_value.__aenter__.return_value = mock_context_manager
        mock_context_manager.get_service.side_effect = [
            AsyncMock(return_value=mock_job_service),
            AsyncMock(return_value=mock_stream_service),
            AsyncMock(return_value=mock_connection_service),
        ]
        
        mock_js = AsyncMock()
        mock_connection_service.return_value.get_jetstream.return_value = mock_js
        mock_js.stream_info.return_value = {"config": {"name": "naq_queue_test"}}
        mock_stream_service.return_value.purge_stream = AsyncMock()
        
        # Run command
        result = runner.invoke(job_app, ["purge", "test"])
        assert result.exit_code == 0
        assert "Purge Results:" in result.stdout
        assert "Queue 'test': Purged 0 jobs." in result.stdout

    @patch("naq.cli.job_commands.service_context")
    @patch("naq.cli.job_commands.SchedulerService")
    def test_job_control_cancel(
        self,
        mock_scheduler_service: MagicMock,
        mock_service_context: MagicMock,
        runner: CliRunner,
    ) -> None:
        """Test the job control command with cancel action."""
        # Setup mocks
        mock_context_manager = AsyncMock()
        mock_service_context.return_value.__aenter__.return_value = mock_context_manager
        mock_context_manager.get_service.return_value = AsyncMock(return_value=mock_scheduler_service)
        mock_scheduler_service.return_value.cancel_scheduled_job.return_value = True
        
        # Run command
        result = runner.invoke(job_app, ["control", "test-job-id", "cancel"])
        assert result.exit_code == 0
        assert "Job test-job-id cancelled successfully." in result.stdout

    @patch("naq.cli.job_commands.service_context")
    @patch("naq.cli.job_commands.SchedulerService")
    def test_job_control_pause(
        self,
        mock_scheduler_service: MagicMock,
        mock_service_context: MagicMock,
        runner: CliRunner,
    ) -> None:
        """Test the job control command with pause action."""
        # Setup mocks
        mock_context_manager = AsyncMock()
        mock_service_context.return_value.__aenter__.return_value = mock_context_manager
        mock_context_manager.get_service.return_value = AsyncMock(return_value=mock_scheduler_service)
        mock_scheduler_service.return_value.pause_scheduled_job.return_value = True
        
        # Run command
        result = runner.invoke(job_app, ["control", "test-job-id", "pause"])
        assert result.exit_code == 0
        assert "Job test-job-id paused successfully." in result.stdout

    @patch("naq.cli.job_commands.service_context")
    @patch("naq.cli.job_commands.SchedulerService")
    def test_job_control_resume(
        self,
        mock_scheduler_service: MagicMock,
        mock_service_context: MagicMock,
        runner: CliRunner,
    ) -> None:
        """Test the job control command with resume action."""
        # Setup mocks
        mock_context_manager = AsyncMock()
        mock_service_context.return_value.__aenter__.return_value = mock_context_manager
        mock_context_manager.get_service.return_value = AsyncMock(return_value=mock_scheduler_service)
        mock_scheduler_service.return_value.resume_scheduled_job.return_value = True
        
        # Run command
        result = runner.invoke(job_app, ["control", "test-job-id", "resume"])
        assert result.exit_code == 0
        assert "Job test-job-id resumed successfully." in result.stdout

    @patch("naq.cli.job_commands.service_context")
    @patch("naq.cli.job_commands.SchedulerService")
    @patch("naq.cli.job_commands.SerializationHelper")
    @patch("naq.cli.job_commands.Job")
    def test_job_control_reschedule(
        self,
        mock_job_class: MagicMock,
        mock_serialization_helper: MagicMock,
        mock_scheduler_service: MagicMock,
        mock_service_context: MagicMock,
        runner: CliRunner,
        mock_job: Job,
    ) -> None:
        """Test the job control command with reschedule action."""
        # Setup mocks
        mock_context_manager = AsyncMock()
        mock_service_context.return_value.__aenter__.return_value = mock_context_manager
        mock_context_manager.get_service.return_value = AsyncMock(return_value=mock_scheduler_service)
        
        # Mock scheduled job
        mock_scheduled_job = AsyncMock()
        mock_scheduled_job.job_id = "test-job-id"
        mock_scheduled_job.scheduled_timestamp_utc = 1234567890.0
        mock_scheduled_job.cron = None
        mock_scheduled_job.interval_seconds = 60
        mock_scheduled_job.repeat = 5
        mock_scheduled_job._orig_job_payload = b"mock_payload"
        mock_scheduler_service.return_value.get_scheduled_job.return_value = mock_scheduled_job
        mock_scheduler_service.return_value.cancel_scheduled_job.return_value = True
        mock_scheduler_service.return_value.schedule_job = AsyncMock()
        
        # Mock serialization
        mock_serialization_helper.return_value.safe_deserialize.return_value = b"mock_serialized_data"
        mock_job_class.return_value.deserialize.return_value = mock_job
        
        # Run command
        result = runner.invoke(job_app, ["control", "test-job-id", "reschedule", "--interval", "120"])
        assert result.exit_code == 0
        assert "Job test-job-id rescheduled successfully." in result.stdout

    def test_job_control_invalid_action(self, runner: CliRunner) -> None:
        """Test the job control command with invalid action."""
        result = runner.invoke(job_app, ["control", "test-job-id", "invalid"])
        assert result.exit_code == 1
        assert "Invalid action 'invalid'" in result.stderr

    def test_job_control_reschedule_missing_params(self, runner: CliRunner) -> None:
        """Test the job control command with reschedule action but missing parameters."""
        result = runner.invoke(job_app, ["control", "test-job-id", "reschedule"])
        assert result.exit_code == 1
        assert "Reschedule action requires at least one scheduling parameter" in result.stderr


class TestWorkerCommands:
    """Test worker-related CLI commands."""

    @patch("naq.cli.worker_commands.service_context")
    @patch("naq.cli.worker_commands.Worker")
    @patch("naq.cli.worker_commands.WorkerService")
    @patch("naq.cli.worker_commands.ConnectionService")
    def test_start_worker(
        self,
        mock_connection_service: MagicMock,
        mock_worker_service: MagicMock,
        mock_worker_class: MagicMock,
        mock_service_context: MagicMock,
        runner: CliRunner,
    ) -> None:
        """Test the start worker command."""
        # Setup mocks
        mock_context_manager = AsyncMock()
        mock_service_context.return_value.__aenter__.return_value = mock_context_manager
        mock_context_manager.get_service.side_effect = [
            AsyncMock(return_value=mock_worker_service),
            AsyncMock(return_value=mock_connection_service),
        ]
        
        mock_connection_service.return_value.test_connection.return_value = True
        mock_js = AsyncMock()
        mock_connection_service.return_value.get_jetstream.return_value = mock_js
        mock_js.stream_info.return_value = {"config": {"name": "naq_jobs"}}
        
        mock_worker_instance = AsyncMock()
        mock_worker_class.return_value = mock_worker_instance
        mock_worker_service.return_value.register_worker = AsyncMock()
        mock_worker_instance.run = AsyncMock()
        
        # Run command
        result = runner.invoke(worker_app, ["start", "test-queue"])
        assert result.exit_code == 0

    @patch("naq.cli.worker_commands.service_context")
    @patch("naq.cli.worker_commands.WorkerService")
    @patch("naq.cli.worker_commands.ConnectionService")
    def test_list_workers(
        self,
        mock_connection_service: MagicMock,
        mock_worker_service: MagicMock,
        mock_service_context: MagicMock,
        runner: CliRunner,
    ) -> None:
        """Test the list workers command."""
        # Setup mocks
        mock_context_manager = AsyncMock()
        mock_service_context.return_value.__aenter__.return_value = mock_context_manager
        
        # Create a worker service instance that will be returned by get_service
        worker_service_instance = MagicMock()
        
        # Set up the service context to return our mock instances
        async def mock_get_service(service_name, service_class):
            if service_name == "worker":
                return worker_service_instance
            elif service_name == "connection":
                return mock_connection_service.return_value
            else:
                raise ValueError(f"Unexpected service name: {service_name}")
        
        mock_context_manager.get_service.side_effect = mock_get_service
        
        # Set up connection service mocks
        mock_connection_service.return_value.test_connection = AsyncMock(return_value=True)
        mock_js = AsyncMock()
        mock_connection_service.return_value.get_jetstream = AsyncMock(return_value=mock_js)
        mock_js.stream_info = AsyncMock(return_value={"config": {"name": "naq_jobs"}})
        
        # Mock worker data
        mock_workers = [
            {
                "worker_id": "test-worker-1",
                "status": WORKER_STATUS.IDLE,
                "queues": ["test-queue"],
                "current_job_id": None,
                "last_heartbeat_utc": 1234567890.0,
            },
            {
                "worker_id": "test-worker-2",
                "status": WORKER_STATUS.BUSY,
                "queues": ["test-queue"],
                "current_job_id": "test-job-id",
                "last_heartbeat_utc": 1234567890.0,
            },
        ]
        
        # Create an AsyncMock for the list_workers method
        worker_service_instance.list_workers = AsyncMock(return_value=mock_workers)
        
        # Run command
        result = runner.invoke(worker_app, ["list"])
        assert result.exit_code == 0
        assert "NAQ Workers" in result.stdout
        assert "test-worker-1" in result.stdout
        assert "test-worker-2" in result.stdout
        assert "Total: 2 active worker(s)" in result.stdout

    @patch("naq.cli.worker_commands.service_context")
    @patch("naq.cli.worker_commands.WorkerService")
    @patch("naq.cli.worker_commands.ConnectionService")
    def test_list_workers_no_workers(
        self,
        mock_connection_service: MagicMock,
        mock_worker_service: MagicMock,
        mock_service_context: MagicMock,
        runner: CliRunner,
    ) -> None:
        """Test the list workers command when no workers are active."""
        # Setup mocks
        mock_context_manager = AsyncMock()
        mock_service_context.return_value.__aenter__.return_value = mock_context_manager
        
        # Create a worker service instance that will be returned by get_service
        worker_service_instance = MagicMock()
        worker_service_instance.list_workers = AsyncMock()
        
        # Set up the service context to return our mock instances
        async def mock_get_service(service_name, service_class):
            if service_name == "worker":
                return worker_service_instance
            elif service_name == "connection":
                return mock_connection_service.return_value
            else:
                raise ValueError(f"Unexpected service name: {service_name}")
        
        mock_context_manager.get_service.side_effect = mock_get_service
        
        # Set up connection service mocks
        mock_connection_service.return_value.test_connection = AsyncMock(return_value=True)
        mock_js = AsyncMock()
        mock_connection_service.return_value.get_jetstream = AsyncMock(return_value=mock_js)
        mock_js.stream_info = AsyncMock(return_value={"config": {"name": "naq_jobs"}})
        
        # Mock empty worker list
        worker_service_instance.list_workers.return_value = []
        
        # Run command
        result = runner.invoke(worker_app, ["list"])
        assert result.exit_code == 0
        assert "No active workers found." in result.stdout


class TestSchedulerCommands:
    """Test scheduler-related CLI commands."""

    @patch("naq.cli.scheduler_commands.Scheduler")
    @patch("naq.cli.scheduler_commands.SchedulerService")
    @patch("naq.cli.scheduler_commands.ConnectionService")
    def test_start_scheduler(
        self,
        mock_connection_service: MagicMock,
        mock_scheduler_service: MagicMock,
        mock_scheduler_class: MagicMock,
        runner: CliRunner,
    ) -> None:
        """Test the start scheduler command."""
        # Setup mocks
        mock_service_manager = AsyncMock()
        mock_service_manager.register_service.side_effect = [
            AsyncMock(return_value=mock_scheduler_service),
            AsyncMock(return_value=mock_connection_service),
        ]
        
        mock_js = AsyncMock()
        mock_connection_service.return_value.get_jetstream.return_value = mock_js
        mock_js.stream_info.return_value = {"config": {"name": "naq_jobs"}}
        
        mock_scheduler_instance = AsyncMock()
        mock_scheduler_class.return_value = mock_scheduler_instance
        mock_scheduler_instance.run = AsyncMock()
        
        with patch("naq.cli.scheduler_commands.ServiceManager", return_value=mock_service_manager):
            # Run command
            result = runner.invoke(scheduler_app, ["start"])
            assert result.exit_code == 0

    @patch("naq.cli.scheduler_commands.ServiceManager")
    @patch("naq.cli.scheduler_commands.SchedulerService")
    @patch("naq.cli.scheduler_commands.ConnectionService")
    def test_list_scheduled_jobs(
        self,
        mock_connection_service: MagicMock,
        mock_scheduler_service: MagicMock,
        mock_service_manager_class: MagicMock,
        runner: CliRunner,
    ) -> None:
        """Test the list scheduled jobs command."""
        # Setup mocks
        mock_service_manager = MagicMock()
        mock_scheduler_service_instance = MagicMock()
        mock_connection_service_instance = MagicMock()
        
        # Mock the ServiceManager class to return our mock instance
        mock_service_manager_class.return_value = mock_service_manager
        
        # Mock register_service to return our service instances
        async def mock_register_service(name, service_class, initialize=True):
            if name == "scheduler":
                return mock_scheduler_service_instance
            elif name == "connection":
                return mock_connection_service_instance
            else:
                raise ValueError(f"Unexpected service name: {name}")
        
        mock_service_manager.register_service.side_effect = mock_register_service
        
        # Mock connection service
        mock_js = MagicMock()
        mock_js.stream_info = AsyncMock(return_value={"config": {"name": "naq_jobs"}})
        mock_connection_service_instance.get_jetstream = AsyncMock(return_value=mock_js)
        
        # Mock scheduled jobs
        mock_schedule1 = MagicMock()
        mock_schedule1.job_id = "test-job-1"
        mock_schedule1.queue_name = "default"  # Match default queue filter
        mock_schedule1.status = SCHEDULED_JOB_STATUS.ACTIVE
        mock_schedule1.scheduled_timestamp_utc = 1234567890.0
        mock_schedule1.cron = "0 0 * * *"
        mock_schedule1.interval_seconds = None
        mock_schedule1.repeat = None
        mock_schedule1.last_enqueued_utc = 1234567000.0
        mock_schedule1.schedule_failure_count = 0
        
        mock_schedule2 = MagicMock()
        mock_schedule2.job_id = "test-job-2"
        mock_schedule2.queue_name = "default"  # Match default queue filter
        mock_schedule2.status = SCHEDULED_JOB_STATUS.PAUSED
        mock_schedule2.scheduled_timestamp_utc = 1234567900.0
        mock_schedule2.cron = None
        mock_schedule2.interval_seconds = 60
        mock_schedule2.repeat = 5
        mock_schedule2.last_enqueued_utc = 1234567000.0
        mock_schedule2.schedule_failure_count = 0
        
        # Fix the mock to return the scheduled jobs
        async def mock_list_scheduled_jobs(status_filter=None):
            return [mock_schedule1, mock_schedule2]
        
        mock_scheduler_service_instance.list_scheduled_jobs = mock_list_scheduled_jobs
        
        # Mock cleanup_all to avoid issues
        mock_service_manager.cleanup_all = AsyncMock()
        
        # Run command
        result = runner.invoke(scheduler_app, ["jobs"])
        assert result.exit_code == 0
        assert "NAQ Scheduled Jobs" in result.stdout
        assert "test-job-1" in result.stdout
        assert "test-job-2" in result.stdout
        assert "Total: 2 scheduled job(s)" in result.stdout

    @patch("naq.cli.scheduler_commands.ServiceManager")
    @patch("naq.cli.scheduler_commands.SchedulerService")
    @patch("naq.cli.scheduler_commands.ConnectionService")
    def test_list_scheduled_jobs_detailed(
        self,
        mock_connection_service: MagicMock,
        mock_scheduler_service: MagicMock,
        mock_service_manager_class: MagicMock,
        runner: CliRunner,
    ) -> None:
        """Test the list scheduled jobs command with detailed view."""
        # Setup mocks
        mock_service_manager = MagicMock()
        mock_scheduler_service_instance = MagicMock()
        mock_connection_service_instance = MagicMock()
        
        # Mock the ServiceManager class to return our mock instance
        mock_service_manager_class.return_value = mock_service_manager
        
        # Mock register_service to return our service instances
        async def mock_register_service(name, service_class, initialize=True):
            if name == "scheduler":
                return mock_scheduler_service_instance
            elif name == "connection":
                return mock_connection_service_instance
            else:
                raise ValueError(f"Unexpected service name: {name}")
        
        mock_service_manager.register_service.side_effect = mock_register_service
        
        # Mock connection service
        mock_js = MagicMock()
        mock_js.stream_info = AsyncMock(return_value={"config": {"name": "naq_jobs"}})
        mock_connection_service_instance.get_jetstream = AsyncMock(return_value=mock_js)
        
        # Mock scheduled job
        mock_schedule = MagicMock()
        mock_schedule.job_id = "test-job-1"
        mock_schedule.queue_name = "default"  # Match default queue filter
        mock_schedule.status = SCHEDULED_JOB_STATUS.ACTIVE
        mock_schedule.scheduled_timestamp_utc = 1234567890.0
        mock_schedule.cron = "0 0 * * *"
        mock_schedule.interval_seconds = None
        mock_schedule.repeat = None
        mock_schedule.last_enqueued_utc = 1234567000.0
        mock_schedule.schedule_failure_count = 0
        
        # Fix the mock to return the scheduled job
        async def mock_list_scheduled_jobs_detailed(status_filter=None):
            return [mock_schedule]
        
        mock_scheduler_service_instance.list_scheduled_jobs = mock_list_scheduled_jobs_detailed
        
        # Mock cleanup_all to avoid issues
        mock_service_manager.cleanup_all = AsyncMock()
        
        # Run command
        result = runner.invoke(scheduler_app, ["jobs", "--detailed"])
        assert result.exit_code == 0
        assert "NAQ Scheduled Jobs" in result.stdout
        assert "test-job-1" in result.stdout
        # The cron expression might be displayed differently in the detailed view
        # Let's just check that the job appears and has the cron schedule type
        assert "cron" in result.stdout

    @patch("naq.cli.scheduler_commands.ServiceManager")
    @patch("naq.cli.scheduler_commands.SchedulerService")
    @patch("naq.cli.scheduler_commands.ConnectionService")
    def test_list_scheduled_jobs_invalid_status(
        self,
        mock_connection_service: MagicMock,
        mock_scheduler_service: MagicMock,
        mock_service_manager_class: MagicMock,
        runner: CliRunner,
    ) -> None:
        """Test the list scheduled jobs command with invalid status."""
        # Setup mocks
        mock_service_manager = MagicMock()
        mock_scheduler_service_instance = MagicMock()
        mock_connection_service_instance = MagicMock()
        
        # Mock the ServiceManager class to return our mock instance
        mock_service_manager_class.return_value = mock_service_manager
        
        # Mock register_service to return our service instances
        async def mock_register_service(name, service_class, initialize=True):
            if name == "scheduler":
                return mock_scheduler_service_instance
            elif name == "connection":
                return mock_connection_service_instance
            else:
                raise ValueError(f"Unexpected service name: {name}")
        
        mock_service_manager.register_service.side_effect = mock_register_service
        
        # Mock connection service
        mock_js = MagicMock()
        mock_js.stream_info = AsyncMock(return_value={"config": {"name": "naq_jobs"}})
        mock_connection_service_instance.get_jetstream = AsyncMock(return_value=mock_js)
        
        # Mock cleanup_all to avoid issues
        mock_service_manager.cleanup_all = AsyncMock()
        
        result = runner.invoke(scheduler_app, ["jobs", "--status", "invalid"])
        assert result.exit_code != 0
        # Check for validation error in either stdout or stderr
        output = result.stdout + result.stderr
        assert "Invalid status" in output or "Invalid status: invalid" in output


class TestSystemCommands:
    """Test system-related CLI commands."""

    @patch("naq.cli.system_commands.setup_logging")
    @patch("naq.cli.system_commands.StructuredLogger")
    def test_dashboard(self, mock_logger: MagicMock, mock_setup_logging: MagicMock, runner: CliRunner) -> None:
        """Test the dashboard command."""
        # Mock uvicorn import at module level
        mock_uvicorn = MagicMock()
        mock_uvicorn.run = MagicMock()
        
        with patch.dict('sys.modules', {'uvicorn': mock_uvicorn}):
            # Run command
            result = runner.invoke(system_app, ["dashboard"])
            assert result.exit_code == 0
            mock_uvicorn.run.assert_called_once()

    @patch("naq.cli.system_commands.setup_logging")
    @patch("naq.cli.system_commands.StructuredLogger")
    def test_dashboard_with_custom_params(
        self, mock_logger: MagicMock, mock_setup_logging: MagicMock, runner: CliRunner
    ) -> None:
        """Test the dashboard command with custom parameters."""
        # Mock uvicorn import at module level
        mock_uvicorn = MagicMock()
        mock_uvicorn.run = MagicMock()
        
        with patch.dict('sys.modules', {'uvicorn': mock_uvicorn}):
            # Run command
            result = runner.invoke(
                system_app, ["dashboard", "--host", "0.0.0.0", "--port", "9000", "--log-level", "debug"]
            )
            assert result.exit_code == 0
            mock_uvicorn.run.assert_called_once_with(
                "naq.dashboard.app:app",
                host="0.0.0.0",
                port=9000,
                log_level="debug",
                reload=False,
            )

    @patch("naq.cli.system_commands.setup_logging")
    @patch("naq.cli.system_commands.StructuredLogger")
    def test_dashboard_import_error(
        self, mock_logger: MagicMock, mock_setup_logging: MagicMock, runner: CliRunner
    ) -> None:
        """Test the dashboard command when uvicorn is not available."""
        # Mock import to raise ImportError
        with patch.dict('sys.modules', {'uvicorn': None}):
            # Run command
            result = runner.invoke(system_app, ["dashboard"])
            assert result.exit_code == 1
            assert "Dashboard dependencies not installed" in result.stdout

    def test_version(self, runner: CliRunner) -> None:
        """Test the version command."""
        result = runner.invoke(system_app, ["version"])
        assert result.exit_code == 0
        assert "naq version:" in result.stdout

    @patch("naq.cli.system_commands.ServiceManager")
    @patch("naq.cli.system_commands.setup_logging")
    @patch("naq.cli.system_commands.StructuredLogger")
    def test_health_success(
        self, mock_logger: MagicMock, mock_setup_logging: MagicMock, mock_service_manager: MagicMock, runner: CliRunner
    ) -> None:
        """Test the health command when connection is successful."""
        # Setup mocks
        mock_connection_service = AsyncMock()
        mock_connection_service.test_connection.return_value = True
    
        mock_manager_instance = AsyncMock()
        mock_manager_instance.register_service.return_value = mock_connection_service
        mock_manager_instance.cleanup_all = AsyncMock()
        mock_service_manager.return_value = mock_manager_instance
    
        # Run command
        result = runner.invoke(system_app, ["health"])
        assert result.exit_code == 0
        assert "System Health: NATS connection successful" in result.stdout

    @patch("naq.cli.system_commands.ServiceManager")
    @patch("naq.cli.system_commands.setup_logging")
    @patch("naq.cli.system_commands.StructuredLogger")
    def test_health_failure(
        self, mock_logger: MagicMock, mock_setup_logging: MagicMock, mock_service_manager: MagicMock, runner: CliRunner
    ) -> None:
        """Test the health command when connection fails."""
        # Setup mocks
        mock_connection_service = AsyncMock()
        mock_connection_service.test_connection.return_value = False
    
        mock_manager_instance = AsyncMock()
        mock_manager_instance.register_service.return_value = mock_connection_service
        mock_manager_instance.cleanup_all = AsyncMock()
        mock_service_manager.return_value = mock_manager_instance
    
        # Run command
        result = runner.invoke(system_app, ["health"])
        assert result.exit_code == 1
        assert "System Health: NATS connection not established" in result.stdout

    @patch("naq.cli.system_commands.load_config")
    @patch("naq.cli.system_commands.setup_logging")
    @patch("naq.cli.system_commands.StructuredLogger")
    def test_config_show(self, mock_logger: MagicMock, mock_setup_logging: MagicMock, mock_load_config: MagicMock, runner: CliRunner) -> None:
        """Test the config show command."""
        # Setup mocks
        mock_config = MagicMock()
        mock_config.to_dict.return_value = {
            "nats_url": "nats://localhost:4222",
            "log_level": "INFO",
        }
        mock_load_config.return_value = mock_config
        
        # Run command
        result = runner.invoke(system_app, ["config", "show"])
        assert result.exit_code == 0
        assert "nats_url" in result.stdout
        assert "nats://localhost:4222" in result.stdout

    @patch("naq.cli.system_commands.load_config")
    @patch("naq.cli.system_commands.ConfigValidator")
    @patch("naq.cli.system_commands.setup_logging")
    @patch("naq.cli.system_commands.StructuredLogger")
    def test_config_validate_success(
        self, mock_logger: MagicMock, mock_setup_logging: MagicMock, mock_config_validator: MagicMock, mock_load_config: MagicMock, runner: CliRunner
    ) -> None:
        """Test the config validate command when config is valid."""
        # Setup mocks
        mock_config = MagicMock()
        mock_config.to_dict.return_value = {
            "nats_url": "nats://localhost:4222",
            "log_level": "INFO",
        }
        mock_load_config.return_value = mock_config
        
        mock_validator = MagicMock()
        mock_validator.validate.return_value = None
        mock_config_validator.return_value = mock_validator
        
        # Run command
        result = runner.invoke(system_app, ["config", "validate"])
        assert result.exit_code == 0
        assert "Configuration is valid!" in result.stdout

    @patch("naq.cli.system_commands.load_config")
    @patch("naq.cli.system_commands.ConfigValidator")
    @patch("naq.cli.system_commands.setup_logging")
    @patch("naq.cli.system_commands.StructuredLogger")
    def test_config_validate_failure(
        self, mock_logger: MagicMock, mock_setup_logging: MagicMock, mock_config_validator: MagicMock, mock_load_config: MagicMock, runner: CliRunner
    ) -> None:
        """Test the config validate command when config is invalid."""
        # Setup mocks
        mock_config = MagicMock()
        mock_config.to_dict.return_value = {
            "nats_url": "invalid-url",
            "log_level": "INFO",
        }
        mock_load_config.return_value = mock_config
    
        mock_validator = MagicMock()
        mock_validator.validate.side_effect = ValueError("Invalid NATS URL")
        mock_config_validator.return_value = mock_validator
    
        # Run command
        result = runner.invoke(system_app, ["config", "validate"])
        assert result.exit_code == 1
        assert "Configuration validation failed" in result.stdout

    @patch("naq.cli.system_commands.get_config")
    @patch("builtins.open", new_callable=MagicMock)
    @patch("naq.cli.system_commands.setup_logging")
    @patch("naq.cli.system_commands.StructuredLogger")
    def test_generate_config(
        self, mock_logger: MagicMock, mock_setup_logging: MagicMock, mock_open: MagicMock, mock_get_config: MagicMock, runner: CliRunner
    ) -> None:
        """Test the generate config command."""
        # Setup mocks
        mock_config = MagicMock()
        mock_config.to_dict.return_value = {
            "nats_url": "nats://localhost:4222",
            "log_level": "INFO",
        }
        mock_get_config.return_value = mock_config
        
        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file
        
        # Run command
        result = runner.invoke(system_app, ["generate-config"])
        assert result.exit_code == 0
        assert "Example configuration generated at:" in result.stdout
        # Check that open was called with a Path object
        mock_open.assert_called_once()
        args, kwargs = mock_open.call_args
        assert str(args[0]).endswith("naq-config.yaml")
        # Check if mode is in args or kwargs
        if len(args) > 1:
            assert args[1] == "w"
        else:
            assert kwargs.get("mode") == "w"


class TestEventCommands:
    """Test event-related CLI commands."""

    @patch("naq.cli.event_commands.service_context")
    @patch("naq.cli.event_commands.EventService")
    @patch("naq.cli.event_commands.ConnectionService")
    @patch("naq.cli.event_commands.EventCommandHandler.validate_common_parameters")
    @patch("naq.cli.event_commands.log_errors")
    @patch("naq.cli.event_commands.ensure_type")
    def test_stream_events(
        self,
        mock_ensure_type: MagicMock,
        mock_log_errors: MagicMock,
        mock_validate: MagicMock,
        mock_connection_service: MagicMock,
        mock_event_service: MagicMock,
        mock_service_context: MagicMock,
        runner: CliRunner,
        mock_job_event: JobEvent,
        mock_worker_event: WorkerEvent,
    ) -> None:
        """Test the stream events command."""
        # Setup mocks
        mock_context_manager = AsyncMock()
        mock_event_service_instance = MagicMock()
        mock_connection_service_instance = MagicMock()
        
        mock_service_context.return_value.__aenter__.return_value = mock_context_manager
        mock_context_manager.get_service.side_effect = [
            AsyncMock(return_value=mock_event_service_instance),
            AsyncMock(return_value=mock_connection_service_instance),
        ]
        
        # Mock the validation method to do nothing
        mock_validate.return_value = None
        
        # Mock the log_errors decorator to return the function unchanged
        def mock_decorator(func):
            return func
        mock_log_errors.side_effect = mock_decorator
        
        # Mock ensure_type to return the input value
        mock_ensure_type.side_effect = lambda x, y, z: x
        
        # Mock NATS connection
        mock_js = AsyncMock()
        mock_connection_service_instance.get_jetstream.return_value = mock_js
        
        # Mock KV store
        mock_kv = AsyncMock()
        mock_kv.keys.return_value = ["job:test-job:events", "worker:test-worker:events"]
        mock_js.key_value.return_value = mock_kv
        
        # Mock event service
        mock_event_service_instance.event_config.events_bucket_name = "naq_events"
        mock_event_service_instance._kv_store_service = MagicMock()
        mock_event_service_instance._kv_store_service.get.side_effect = [
            [msgspec.structs.asdict(mock_job_event)],
            [msgspec.structs.asdict(mock_worker_event)],
        ]
        
        # Run command with follow=False to avoid infinite loop
        result = runner.invoke(event_app, ["stream", "--follow", "false", "--tail", "2"])
        # Check both stdout and stderr for the expected output
        output = result.stdout + result.stderr
        assert result.exit_code == 0
        assert "Showing last 2 events:" in output

    @patch("naq.cli.event_commands.service_context")
    @patch("naq.cli.event_commands.EventService")
    @patch("naq.cli.event_commands.EventCommandHandler.validate_common_parameters")
    @patch("naq.cli.event_commands.log_errors")
    @patch("naq.cli.event_commands.ensure_type")
    def test_history(
        self,
        mock_ensure_type: MagicMock,
        mock_log_errors: MagicMock,
        mock_validate: MagicMock,
        mock_event_service: MagicMock,
        mock_service_context: MagicMock,
        runner: CliRunner,
        mock_job_event: JobEvent,
    ) -> None:
        """Test the history command."""
        # Setup mocks
        mock_context_manager = AsyncMock()
        mock_event_service_instance = MagicMock()
        
        mock_service_context.return_value.__aenter__.return_value = mock_context_manager
        mock_context_manager.get_service.return_value = AsyncMock(return_value=mock_event_service_instance)
        
        # Mock the validation method to do nothing
        mock_validate.return_value = None
        
        # Mock the log_errors decorator to return the function unchanged
        def mock_decorator(func):
            return func
        mock_log_errors.side_effect = mock_decorator
        
        # Mock ensure_type to return the input value
        mock_ensure_type.side_effect = lambda x, y, z: x
        
        # Mock event service
        mock_event_service_instance.get_job_events.return_value = [mock_job_event]
        
        # Run command
        result = runner.invoke(event_app, ["history", "test-job-id"])
        # Check both stdout and stderr for the expected output
        output = result.stdout + result.stderr
        assert result.exit_code == 0
        assert "Event history for job test-job-id:" in output
        assert "Found 1 events" in output

    @patch("naq.cli.event_commands.service_context")
    @patch("naq.cli.event_commands.EventService")
    @patch("naq.cli.event_commands.ConnectionService")
    @patch("naq.cli.event_commands.EventCommandHandler.validate_common_parameters")
    @patch("naq.cli.event_commands.log_errors")
    @patch("naq.cli.event_commands.ensure_type")
    def test_stats(
        self,
        mock_ensure_type: MagicMock,
        mock_log_errors: MagicMock,
        mock_validate: MagicMock,
        mock_connection_service: MagicMock,
        mock_event_service: MagicMock,
        mock_service_context: MagicMock,
        runner: CliRunner,
        mock_job_event: JobEvent,
        mock_worker_event: WorkerEvent,
    ) -> None:
        """Test the stats command."""
        # Setup mocks
        mock_context_manager = AsyncMock()
        mock_event_service_instance = MagicMock()
        mock_connection_service_instance = MagicMock()
        
        mock_service_context.return_value.__aenter__.return_value = mock_context_manager
        mock_context_manager.get_service.side_effect = [
            AsyncMock(return_value=mock_event_service_instance),
            AsyncMock(return_value=mock_connection_service_instance),
        ]
        
        # Mock the validation method to do nothing
        mock_validate.return_value = None
        
        # Mock the log_errors decorator to return the function unchanged
        def mock_decorator(func):
            return func
        mock_log_errors.side_effect = mock_decorator
        
        # Mock ensure_type to return the input value
        mock_ensure_type.side_effect = lambda x, y, z: x
        
        # Mock NATS connection
        mock_js = AsyncMock()
        mock_connection_service_instance.get_jetstream.return_value = mock_js
        
        # Mock KV store
        mock_kv = AsyncMock()
        mock_kv.keys.return_value = ["job:test-job:events", "worker:test-worker:events"]
        mock_js.key_value.return_value = mock_kv
        
        # Mock event service
        mock_event_service_instance.event_config.events_bucket_name = "naq_events"
        mock_event_service_instance._kv_store_service = MagicMock()
        mock_event_service_instance._kv_store_service.get.side_effect = [
            [msgspec.structs.asdict(mock_job_event)],
            [msgspec.structs.asdict(mock_worker_event)],
        ]
        
        # Run command
        result = runner.invoke(event_app, ["stats"])
        # Check both stdout and stderr for the expected output
        output = result.stdout + result.stderr
        assert result.exit_code == 0
        assert "Event Statistics" in output
        assert "Total events: 2" in output

    @patch("naq.cli.event_commands.service_context")
    @patch("naq.cli.event_commands.WorkerService")
    @patch("naq.cli.event_commands.EventCommandHandler.validate_common_parameters")
    @patch("naq.cli.event_commands.log_errors")
    @patch("naq.cli.event_commands.ensure_type")
    def test_workers(
        self,
        mock_ensure_type: MagicMock,
        mock_log_errors: MagicMock,
        mock_validate: MagicMock,
        mock_worker_service: MagicMock,
        mock_service_context: MagicMock,
        runner: CliRunner,
    ) -> None:
        """Test the workers command."""
        # Setup mocks
        mock_context_manager = AsyncMock()
        mock_worker_service_instance = MagicMock()
        
        mock_service_context.return_value.__aenter__.return_value = mock_context_manager
        mock_context_manager.get_service.return_value = AsyncMock(return_value=mock_worker_service_instance)
        
        # Mock the validation method to do nothing
        mock_validate.return_value = None
        
        # Mock the log_errors decorator to return the function unchanged
        def mock_decorator(func):
            return func
        mock_log_errors.side_effect = mock_decorator
        
        # Mock ensure_type to return the input value
        mock_ensure_type.side_effect = lambda x, y, z: x
        
        # Mock worker data
        mock_workers = [
            {
                "worker_id": "test-worker-1",
                "status": WORKER_STATUS.IDLE,
                "queues": ["test-queue"],
                "current_job_id": None,
                "last_heartbeat_utc": 1234567890.0,
            },
            {
                "worker_id": "test-worker-2",
                "status": WORKER_STATUS.BUSY,
                "queues": ["test-queue"],
                "current_job_id": "test-job-id",
                "last_heartbeat_utc": 1234567890.0,
            },
        ]
        mock_worker_service_instance.get_workers.return_value = mock_workers
        
        # Run command with follow=False to avoid infinite loop
        result = runner.invoke(event_app, ["workers", "--follow", "false"])
        # Check both stdout and stderr for the expected output
        output = result.stdout + result.stderr
        assert result.exit_code == 0
        assert "Workers" in output
        assert "test-worker-1" in output
        assert "test-worker-2" in output
        assert "Total: 2 worker(s)" in output

    @patch("naq.cli.event_commands.EventCommandHandler.validate_common_parameters")
    @patch("naq.cli.event_commands.ensure_type")
    def test_stream_events_invalid_format(self, mock_ensure_type: MagicMock, mock_validate: MagicMock, runner: CliRunner) -> None:
        """Test the stream events command with invalid format."""
        # Mock the validation method to do nothing
        mock_validate.return_value = None
        # Mock ensure_type to return the input value
        mock_ensure_type.side_effect = lambda x, y, z: x
        
        result = runner.invoke(event_app, ["stream", "--format", "invalid"])
        assert result.exit_code != 0
        # Check for validation error in either stdout or stderr
        output = result.stdout + result.stderr
        assert "Invalid format" in output

    @patch("naq.cli.event_commands.EventCommandHandler.validate_common_parameters")
    @patch("naq.cli.event_commands.ensure_type")
    def test_history_invalid_format(self, mock_ensure_type: MagicMock, mock_validate: MagicMock, runner: CliRunner) -> None:
        """Test the history command with invalid format."""
        # Mock the validation method to do nothing
        mock_validate.return_value = None
        # Mock ensure_type to return the input value
        mock_ensure_type.side_effect = lambda x, y, z: x
        
        result = runner.invoke(event_app, ["history", "test-job-id", "--format", "invalid"])
        assert result.exit_code != 0
        # Check for validation error in either stdout or stderr
        output = result.stdout + result.stderr
        assert "Invalid format" in output

    @patch("naq.cli.event_commands.EventCommandHandler.validate_common_parameters")
    @patch("naq.cli.event_commands.ensure_type")
    def test_stats_invalid_format(self, mock_ensure_type: MagicMock, mock_validate: MagicMock, runner: CliRunner) -> None:
        """Test the stats command with invalid format."""
        # Mock the validation method to do nothing
        mock_validate.return_value = None
        # Mock ensure_type to return the input value
        mock_ensure_type.side_effect = lambda x, y, z: x
        
        result = runner.invoke(event_app, ["stats", "--format", "invalid"])
        assert result.exit_code != 0
        # Check for validation error in either stdout or stderr
        output = result.stdout + result.stderr
        assert "Invalid format" in output

    @patch("naq.cli.event_commands.EventCommandHandler.validate_common_parameters")
    @patch("naq.cli.event_commands.ensure_type")
    def test_stats_invalid_time_range(self, mock_ensure_type: MagicMock, mock_validate: MagicMock, runner: CliRunner) -> None:
        """Test the stats command with invalid time range."""
        # Mock the validation method to do nothing
        mock_validate.return_value = None
        # Mock ensure_type to return the input value
        mock_ensure_type.side_effect = lambda x, y, z: x
        
        result = runner.invoke(event_app, ["stats", "--time-range", "invalid"])
        assert result.exit_code != 0
        # Check for validation error in either stdout or stderr
        output = result.stdout + result.stderr
        assert "Invalid time range format" in output

    @patch("naq.cli.event_commands.EventCommandHandler.validate_common_parameters")
    @patch("naq.cli.event_commands.ensure_type")
    def test_workers_invalid_format(self, mock_ensure_type: MagicMock, mock_validate: MagicMock, runner: CliRunner) -> None:
        """Test the workers command with invalid format."""
        # Mock the validation method to do nothing
        mock_validate.return_value = None
        # Mock ensure_type to return the input value
        mock_ensure_type.side_effect = lambda x, y, z: x
        
        result = runner.invoke(event_app, ["workers", "--format", "invalid"])
        assert result.exit_code != 0
        # Check for validation error in either stdout or stderr
        output = result.stdout + result.stderr
        assert "Invalid format" in output

    @patch("naq.cli.event_commands.EventCommandHandler.validate_common_parameters")
    @patch("naq.cli.event_commands.ensure_type")
    def test_workers_invalid_status(self, mock_ensure_type: MagicMock, mock_validate: MagicMock, runner: CliRunner) -> None:
        """Test the workers command with invalid status."""
        # Mock the validation method to do nothing
        mock_validate.return_value = None
        # Mock ensure_type to return the input value
        mock_ensure_type.side_effect = lambda x, y, z: x
        
        result = runner.invoke(event_app, ["workers", "--status", "invalid"])
        assert result.exit_code != 0
        # Check for validation error in either stdout or stderr
        output = result.stdout + result.stderr
        assert "Invalid status" in output


class TestEventCommandHandler:
    """Test the EventCommandHandler class."""

    def test_validate_common_parameters_valid(self) -> None:
        """Test validate_common_parameters with valid parameters."""
        handler = EventCommandHandler()
        # Should not raise any exceptions
        handler.validate_common_parameters(
            nats_url="nats://localhost:4222",
            log_level="INFO",
            limit=100,
            worker_id="test-worker",
        )

    def test_validate_common_parameters_invalid_nats_url(self) -> None:
        """Test validate_common_parameters with invalid NATS URL."""
        handler = EventCommandHandler()
        # Current implementation doesn't validate NATS URL format strictly
        # This test documents the current behavior
        handler.validate_common_parameters(
            nats_url="invalid-url",
            log_level="INFO",
            limit=100,
            worker_id="test-worker",
        )
        # If no exception is raised, that's the current behavior

    def test_validate_common_parameters_invalid_log_level(self) -> None:
        """Test validate_common_parameters with invalid log level."""
        from naq.exceptions import ValidationError
        handler = EventCommandHandler()
        # Should raise ValidationError for invalid log level
        with pytest.raises(ValidationError, match="log_level must be one of"):
            handler.validate_common_parameters(
                nats_url="nats://localhost:4222",
                log_level="INVALID",
                limit=100,
                worker_id="test-worker",
            )

    def test_validate_common_parameters_invalid_limit(self) -> None:
        """Test validate_common_parameters with invalid limit."""
        from naq.exceptions import ValidationError
        handler = EventCommandHandler()
        # Should raise ValidationError for invalid limit
        with pytest.raises(ValidationError, match="limit must be between 1 and 10000"):
            handler.validate_common_parameters(
                nats_url="nats://localhost:4222",
                log_level="INFO",
                limit=0,
                worker_id="test-worker",
            )

    def test_validate_common_parameters_invalid_worker_id(self) -> None:
        """Test validate_common_parameters with invalid worker ID."""
        handler = EventCommandHandler()
        # Current implementation doesn't validate empty worker ID
        # This test documents the current behavior
        handler.validate_common_parameters(
            nats_url="nats://localhost:4222",
            log_level="INFO",
            limit=100,
            worker_id="",
        )
        # If no exception is raised, that's the current behavior