# tests/test_compatibility/test_imports.py
"""Tests for backward compatibility of imports."""

import pytest


class TestImportCompatibility:
    """Test that all legacy import patterns continue to work."""

    def test_main_imports(self):
        """Test main package imports still work."""
        # These should all work without errors
        from naq import Queue, Worker, Job, enqueue, enqueue_sync
        from naq import JOB_STATUS, JobEvent, DEFAULT_NATS_URL
        
        assert Queue is not None
        assert Worker is not None
        assert Job is not None
        assert callable(enqueue)
        assert callable(enqueue_sync)
        assert JOB_STATUS is not None
        assert JobEvent is not None
        assert DEFAULT_NATS_URL is not None

    def test_model_imports(self):
        """Test model imports still work."""
        from naq.models import Job, JOB_STATUS, JobEvent, JobResult
        
        assert Job is not None
        assert JOB_STATUS is not None
        assert JobEvent is not None
        assert JobResult is not None

    def test_event_imports(self):
        """Test event system imports still work."""
        from naq.events import AsyncJobEventLogger, JobEventType
        
        assert AsyncJobEventLogger is not None
        assert JobEventType is not None

    def test_queue_imports(self):
        """Test queue-related imports still work."""
        from naq.queue import Queue, enqueue, enqueue_sync
        
        assert Queue is not None
        assert callable(enqueue)
        assert callable(enqueue_sync)

    def test_worker_imports(self):
        """Test worker imports still work."""
        from naq.worker import Worker
        
        assert Worker is not None

    def test_scheduler_imports(self):
        """Test scheduler imports still work."""
        from naq.scheduler import Scheduler
        
        assert Scheduler is not None

    def test_results_imports(self):
        """Test results imports still work."""
        from naq.results import Results
        
        assert Results is not None

    def test_settings_imports(self):
        """Test settings imports still work."""
        from naq.settings import DEFAULT_NATS_URL, DEFAULT_QUEUE_NAME
        
        assert DEFAULT_NATS_URL is not None
        assert DEFAULT_QUEUE_NAME is not None

    def test_exception_imports(self):
        """Test exception imports still work."""
        from naq.exceptions import (
            NaqException, 
            ConfigurationError, 
            JobNotFoundError
        )
        
        assert NaqException is not None
        assert ConfigurationError is not None
        assert JobNotFoundError is not None

    def test_utility_imports(self):
        """Test utility imports still work."""
        from naq.utils import setup_logging, run_async_from_sync
        
        assert callable(setup_logging)
        assert callable(run_async_from_sync)

    def test_new_modular_imports_available(self):
        """Test new modular imports are available."""
        # Test new import structure works
        from naq.models.jobs import Job
        from naq.models.enums import JOB_STATUS
        from naq.models.events import JobEvent
        from naq.queue.core import Queue
        from naq.worker.core import Worker
        from naq.services import ServiceManager
        
        assert Job is not None
        assert JOB_STATUS is not None
        assert JobEvent is not None
        assert Queue is not None
        assert Worker is not None
        assert ServiceManager is not None

    def test_import_equivalence(self):
        """Test that old and new imports refer to same objects."""
        # Legacy imports
        from naq import Job as LegacyJob
        from naq import JOB_STATUS as LegacyStatus
        
        # New imports
        from naq.models.jobs import Job as NewJob
        from naq.models.enums import JOB_STATUS as NewStatus
        
        # Should be the same objects
        assert LegacyJob is NewJob
        assert LegacyStatus is NewStatus

    def test_configuration_imports(self):
        """Test configuration system imports work."""
        from naq.config import load_config, get_config
        
        assert callable(load_config)
        assert callable(get_config)

    def test_service_imports(self):
        """Test service layer imports work."""
        from naq.services import ServiceManager
        from naq.services.connection import ConnectionService
        from naq.services.jobs import JobService
        
        assert ServiceManager is not None
        assert ConnectionService is not None
        assert JobService is not None

    def test_legacy_convenience_functions(self):
        """Test legacy convenience functions still work."""
        from naq import (
            fetch_job_result,
            fetch_job_result_sync,
            list_workers,
            list_workers_sync
        )
        
        assert callable(fetch_job_result)
        assert callable(fetch_job_result_sync)
        assert callable(list_workers)
        assert callable(list_workers_sync)

    def test_enums_compatibility(self):
        """Test enum imports and values compatibility."""
        from naq.models import JOB_STATUS, JobEventType, WorkerEventType
        
        # Test that enums have expected values
        assert hasattr(JOB_STATUS, 'PENDING')
        assert hasattr(JOB_STATUS, 'RUNNING')
        assert hasattr(JOB_STATUS, 'COMPLETED')
        assert hasattr(JOB_STATUS, 'FAILED')
        
        assert hasattr(JobEventType, 'ENQUEUED')
        assert hasattr(JobEventType, 'STARTED')
        assert hasattr(JobEventType, 'COMPLETED')
        
        assert hasattr(WorkerEventType, 'WORKER_STARTED')
        assert hasattr(WorkerEventType, 'WORKER_STOPPED')

    def test_mixed_import_patterns(self):
        """Test that mixed old and new import patterns work together."""
        # Mix legacy and new imports
        from naq import enqueue_sync  # Legacy
        from naq.models.jobs import Job  # New
        from naq.worker import Worker  # Legacy
        from naq.services import ServiceManager  # New
        
        # All should work together
        assert callable(enqueue_sync)
        assert Job is not None
        assert Worker is not None
        assert ServiceManager is not None

    def test_star_imports_discouraged_but_work(self):
        """Test that star imports work but are discouraged."""
        # This tests that __all__ is properly defined
        import naq
        
        # Should have main components available
        assert hasattr(naq, 'Queue')
        assert hasattr(naq, 'Worker')
        assert hasattr(naq, 'Job')
        assert hasattr(naq, 'enqueue')
        assert hasattr(naq, 'enqueue_sync')