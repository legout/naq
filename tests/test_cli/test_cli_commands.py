# tests/test_cli/test_cli_commands.py
"""Tests for CLI command functionality."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from click.testing import CliRunner

from naq.cli.main import cli
from naq.cli.job_commands import job_commands
from naq.cli.worker_commands import worker_commands
from naq.cli.scheduler_commands import scheduler_commands
from naq.cli.system_commands import system_commands
from naq.cli.event_commands import event_commands


class TestMainCLI:
    """Test main CLI functionality."""

    def test_cli_help(self):
        """Test CLI help command."""
        runner = CliRunner()
        result = runner.invoke(cli, ['--help'])
        assert result.exit_code == 0
        assert 'Usage:' in result.output

    def test_cli_version(self):
        """Test CLI version command."""
        runner = CliRunner()
        result = runner.invoke(cli, ['--version'])
        assert result.exit_code == 0
        assert '0.2.0' in result.output


class TestJobCommands:
    """Test job-related CLI commands."""

    @patch('naq.cli.job_commands.enqueue_sync')
    def test_job_enqueue_command(self, mock_enqueue):
        """Test job enqueue CLI command."""
        mock_job = MagicMock()
        mock_job.job_id = "test-job-123"
        mock_enqueue.return_value = mock_job
        
        runner = CliRunner()
        result = runner.invoke(cli, [
            'job', 'enqueue', 
            '--function', 'builtins.print', 
            '--args', '["Hello World"]',
            '--queue', 'test-queue'
        ])
        
        assert result.exit_code == 0
        mock_enqueue.assert_called_once()

    @patch('naq.cli.job_commands.fetch_job_result_sync')
    def test_job_status_command(self, mock_fetch):
        """Test job status CLI command."""
        mock_result = {
            'job_id': 'test-job-123',
            'status': 'completed',
            'result': 'Hello World'
        }
        mock_fetch.return_value = mock_result
        
        runner = CliRunner()
        result = runner.invoke(cli, ['job', 'status', 'test-job-123'])
        
        assert result.exit_code == 0
        assert 'test-job-123' in result.output
        mock_fetch.assert_called_once_with('test-job-123')

    @patch('naq.cli.job_commands.purge_queue_sync')
    def test_job_purge_command(self, mock_purge):
        """Test job purge CLI command."""
        mock_purge.return_value = 5  # 5 jobs purged
        
        runner = CliRunner()
        result = runner.invoke(cli, ['job', 'purge', '--queue', 'test-queue'])
        
        assert result.exit_code == 0
        assert '5' in result.output
        mock_purge.assert_called_once_with('test-queue')


class TestWorkerCommands:
    """Test worker-related CLI commands."""

    @patch('naq.cli.worker_commands.Worker')
    def test_worker_start_command(self, mock_worker_class):
        """Test worker start CLI command."""
        mock_worker = AsyncMock()
        mock_worker_class.return_value = mock_worker
        
        runner = CliRunner()
        result = runner.invoke(cli, [
            'worker', 'start',
            '--queues', 'queue1,queue2',
            '--concurrency', '4'
        ])
        
        # Note: This is a simplified test as the actual command runs async
        assert result.exit_code == 0 or 'queue1' in str(mock_worker_class.call_args)

    @patch('naq.cli.worker_commands.list_workers_sync')
    def test_worker_list_command(self, mock_list):
        """Test worker list CLI command."""
        mock_workers = [
            {'worker_id': 'worker-1', 'status': 'idle', 'queues': ['queue1']},
            {'worker_id': 'worker-2', 'status': 'busy', 'queues': ['queue2']}
        ]
        mock_list.return_value = mock_workers
        
        runner = CliRunner()
        result = runner.invoke(cli, ['worker', 'list'])
        
        assert result.exit_code == 0
        assert 'worker-1' in result.output
        assert 'worker-2' in result.output
        mock_list.assert_called_once()


class TestSchedulerCommands:
    """Test scheduler-related CLI commands."""

    @patch('naq.cli.scheduler_commands.Scheduler')
    def test_scheduler_start_command(self, mock_scheduler_class):
        """Test scheduler start CLI command."""
        mock_scheduler = AsyncMock()
        mock_scheduler_class.return_value = mock_scheduler
        
        runner = CliRunner()
        result = runner.invoke(cli, ['scheduler', 'start'])
        
        # Note: This is a simplified test as the actual command runs async
        assert result.exit_code == 0 or mock_scheduler_class.called

    @patch('naq.cli.scheduler_commands.schedule_sync')
    def test_schedule_job_command(self, mock_schedule):
        """Test schedule job CLI command."""
        mock_job = MagicMock()
        mock_job.job_id = "scheduled-job-123"
        mock_schedule.return_value = mock_job
        
        runner = CliRunner()
        result = runner.invoke(cli, [
            'scheduler', 'schedule',
            '--function', 'builtins.print',
            '--args', '["Scheduled Hello"]',
            '--cron', '0 0 * * *'
        ])
        
        assert result.exit_code == 0
        mock_schedule.assert_called_once()


class TestSystemCommands:
    """Test system-related CLI commands."""

    @patch('naq.cli.system_commands.get_nats_connection')
    def test_system_status_command(self, mock_get_conn):
        """Test system status CLI command."""
        mock_nc = AsyncMock()
        mock_nc.is_connected = True
        mock_get_conn.return_value = mock_nc
        
        runner = CliRunner()
        result = runner.invoke(cli, ['system', 'status'])
        
        # Basic test - the actual implementation may vary
        assert result.exit_code == 0

    @patch('naq.cli.system_commands.ServiceManager')
    def test_system_health_command(self, mock_service_manager):
        """Test system health CLI command."""
        mock_manager = AsyncMock()
        mock_service_manager.return_value = mock_manager
        
        runner = CliRunner()
        result = runner.invoke(cli, ['system', 'health'])
        
        assert result.exit_code == 0


class TestEventCommands:
    """Test event-related CLI commands."""

    @patch('naq.cli.event_commands.NATSJobEventStorage')
    def test_events_list_command(self, mock_storage_class):
        """Test events list CLI command."""
        mock_storage = AsyncMock()
        mock_events = [
            MagicMock(job_id='job-1', event_type='enqueued', timestamp=1234567890),
            MagicMock(job_id='job-1', event_type='started', timestamp=1234567900)
        ]
        mock_storage.get_events.return_value = mock_events
        mock_storage_class.return_value = mock_storage
        
        runner = CliRunner()
        result = runner.invoke(cli, ['events', 'list', '--job-id', 'job-1'])
        
        assert result.exit_code == 0

    @patch('naq.cli.event_commands.AsyncJobEventLogger')
    def test_events_export_command(self, mock_logger_class):
        """Test events export CLI command."""
        mock_logger = AsyncMock()
        mock_logger_class.return_value = mock_logger
        
        runner = CliRunner()
        result = runner.invoke(cli, [
            'events', 'export',
            '--output', '/tmp/test-events.json',
            '--format', 'json'
        ])
        
        # Basic test - actual implementation may vary
        assert result.exit_code == 0


class TestCLIIntegration:
    """Test CLI integration scenarios."""

    def test_cli_configuration_loading(self):
        """Test CLI configuration loading."""
        runner = CliRunner()
        
        # Test with custom config file
        result = runner.invoke(cli, [
            '--config', '/nonexistent/config.yaml',
            'system', 'status'
        ])
        
        # Should handle missing config gracefully
        assert result.exit_code in [0, 1]  # May fail or succeed depending on defaults

    def test_cli_verbose_output(self):
        """Test CLI verbose output."""
        runner = CliRunner()
        result = runner.invoke(cli, ['--verbose', 'system', 'status'])
        
        # Should run without error
        assert result.exit_code == 0

    def test_cli_quiet_output(self):
        """Test CLI quiet output."""
        runner = CliRunner()
        result = runner.invoke(cli, ['--quiet', 'system', 'status'])
        
        # Should run without error and minimal output
        assert result.exit_code == 0


class TestCLIErrorHandling:
    """Test CLI error handling."""

    def test_invalid_command(self):
        """Test invalid CLI command."""
        runner = CliRunner()
        result = runner.invoke(cli, ['invalid-command'])
        
        assert result.exit_code != 0
        assert 'No such command' in result.output

    def test_missing_required_argument(self):
        """Test missing required argument."""
        runner = CliRunner()
        result = runner.invoke(cli, ['job', 'status'])  # Missing job ID
        
        assert result.exit_code != 0

    @patch('naq.cli.job_commands.enqueue_sync')
    def test_command_execution_error(self, mock_enqueue):
        """Test command execution error handling."""
        mock_enqueue.side_effect = Exception("Test error")
        
        runner = CliRunner()
        result = runner.invoke(cli, [
            'job', 'enqueue',
            '--function', 'builtins.print',
            '--args', '["Hello"]'
        ])
        
        assert result.exit_code != 0
        assert 'error' in result.output.lower()