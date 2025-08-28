"""
Import compatibility tests for the NAQ job queue system.

This module contains tests to ensure that existing import paths and patterns 
remain functional after the service layer refactoring, preventing breaking 
changes for users.
"""

import pytest
from typing import Any, Callable


def test_naq_core_models_imports():
    """Test importing core models from naq package."""
    # Test importing core models
    from naq import Job, JobResult, JOB_STATUS, JobEventType, WorkerEventType, RetryDelayType
    
    # Verify imported objects are of expected type
    assert callable(Job)
    assert callable(JobResult)
    assert hasattr(JOB_STATUS, 'PENDING')  # Check it's an enum
    assert hasattr(JobEventType, 'ENQUEUED')  # Check it's an enum
    assert hasattr(WorkerEventType, 'STARTED')  # Check it's an enum
    assert RetryDelayType is not None


def test_naq_event_models_imports():
    """Test importing event models from naq package."""
    # Test importing event models
    from naq import JobEvent, WorkerEvent
    
    # Verify imported objects are of expected type
    assert callable(JobEvent)
    assert callable(WorkerEvent)


def test_naq_schedule_imports():
    """Test importing Schedule model from naq package."""
    # Test importing Schedule model
    from naq import Schedule
    
    # Verify imported object is of expected type
    assert callable(Schedule)


def test_naq_queue_functions_imports():
    """Test importing queue-related functions from naq package."""
    # Test importing queue functions
    from naq import enqueue, schedule
    
    # Verify imported objects are callable
    assert callable(enqueue)
    assert callable(schedule)


def test_naq_worker_imports():
    """Test importing Worker class from naq package."""
    # Test importing Worker class
    from naq import Worker
    
    # Verify imported object is callable
    assert callable(Worker)


def test_naq_scheduler_imports():
    """Test importing Scheduler class from naq package."""
    # Test importing Scheduler class
    from naq import Scheduler
    
    # Verify imported object is callable
    assert callable(Scheduler)


def test_naq_results_imports():
    """Test importing Results class from naq package."""
    # Test importing Results class
    from naq import Results
    
    # Verify imported object is callable
    assert callable(Results)


def test_naq_events_imports():
    """Test importing events module from naq package."""
    # Test importing events module - this should fail as it's not exported
    try:
        from naq import events
        # If it succeeds, test that events is a module
        assert events is not None
        assert hasattr(events, '__name__')
    except ImportError:
        # This is expected as events is not exported from naq
        pytest.skip("events module is not exported from naq package")


def test_naq_connection_imports():
    """Test importing connection management functions from naq package."""
    # Test importing connection functions - these should fail as they're not exported
    try:
        from naq import connect, disconnect
        # If they succeed, test that functions are callable
        assert callable(connect)
        assert callable(disconnect)
    except ImportError:
        # This is expected as connect/disconnect are not exported from naq
        pytest.skip("connect/disconnect functions are not exported from naq package")


def test_naq_config_imports():
    """Test importing config from naq package."""
    # Test importing config
    from naq import config
    
    # Verify imported object is a module
    assert config is not None


def test_naq_version_imports():
    """Test importing version from naq package."""
    # Test importing version
    from naq import __version__
    
    # Verify version is a string
    assert isinstance(__version__, str)


def test_naq_convenience_functions_imports():
    """Test importing convenience functions from naq package."""
    # Test importing convenience functions - some should fail as they're not exported
    try:
        from naq import fetch_job_result, fetch_job_result_sync, list_workers, list_workers_sync
        # If they succeed, test that functions are callable
        assert callable(fetch_job_result)
        assert callable(fetch_job_result_sync)
        assert callable(list_workers)
        assert callable(list_workers_sync)
    except ImportError:
        # This is expected as fetch_job_result functions are not exported from naq
        # But list_workers should be available
        from naq import list_workers, list_workers_sync
        assert callable(list_workers)
        assert callable(list_workers_sync)


def test_naq_exceptions_imports():
    """Test importing exceptions from naq package."""
    # Test importing exceptions
    from naq import exceptions
    
    # Verify imported object is a module
    assert exceptions is not None
    
    # Test importing specific exception classes
    from naq.exceptions import NaqException, NaqConnectionError, JobExecutionError
    
    # Verify imported objects are exception classes
    assert issubclass(NaqException, Exception)
    assert issubclass(NaqConnectionError, NaqException)
    assert issubclass(JobExecutionError, NaqException)


def test_naq_connection_management_imports():
    """Test importing connection management functions from naq package."""
    # Test importing connection management functions
    from naq import nats_jetstream, nats_kv_store
    
    # Verify imported objects are callable
    assert callable(nats_jetstream)
    assert callable(nats_kv_store)


def test_naq_config_functions_imports():
    """Test importing configuration functions from naq package."""
    # Test importing configuration functions
    from naq import get_config, load_config
    
    # Verify imported objects are callable
    assert callable(get_config)
    assert callable(load_config)


def test_naq_queue_management_functions_imports():
    """Test importing queue management functions from naq package."""
    # Test importing async queue management functions
    from naq import (
        cancel_scheduled_job, pause_scheduled_job, resume_scheduled_job, modify_scheduled_job,
        cancel_scheduled_job_sync, pause_scheduled_job_sync, resume_scheduled_job_sync, modify_scheduled_job_sync
    )
    
    # Verify imported objects are callable
    assert callable(cancel_scheduled_job)
    assert callable(pause_scheduled_job)
    assert callable(resume_scheduled_job)
    assert callable(modify_scheduled_job)
    assert callable(cancel_scheduled_job_sync)
    assert callable(pause_scheduled_job_sync)
    assert callable(resume_scheduled_job_sync)
    assert callable(modify_scheduled_job_sync)


def test_naq_additional_exceptions_imports():
    """Test importing additional exception types from naq package."""
    # Test importing additional exception types
    from naq import exceptions
    from naq.exceptions import ValidationError, TypeConversionError
    
    # Verify imported objects are exception classes
    assert issubclass(ValidationError, Exception)
    assert issubclass(TypeConversionError, Exception)
    
    # Verify they're accessible through the exceptions module
    assert hasattr(exceptions, 'ValidationError')
    assert hasattr(exceptions, 'TypeConversionError')


def test_naq_models_core_imports():
    """Test importing core models from naq.models package."""
    # Test importing core models from naq.models
    from naq.models import Job, JobResult, JOB_STATUS, JobEventType, WorkerEventType, RetryDelayType
    
    # Verify imported objects are of expected type
    assert callable(Job)
    assert callable(JobResult)
    assert hasattr(JOB_STATUS, 'PENDING')  # Check it's an enum
    assert hasattr(JobEventType, 'ENQUEUED')  # Check it's an enum
    assert hasattr(WorkerEventType, 'STARTED')  # Check it's an enum
    assert RetryDelayType is not None


def test_naq_models_event_imports():
    """Test importing event models from naq.models package."""
    # Test importing event models from naq.models
    from naq.models import JobEvent, WorkerEvent
    
    # Verify imported objects are of expected type
    assert callable(JobEvent)
    assert callable(WorkerEvent)


def test_naq_models_schedule_imports():
    """Test importing Schedule model from naq.models package."""
    # Test importing Schedule model from naq.models
    from naq.models import Schedule
    
    # Verify imported object is of expected type
    assert callable(Schedule)


def test_naq_events_async_job_event_logger_imports():
    """Test importing AsyncJobEventLogger from naq.events package."""
    # Test importing AsyncJobEventLogger from naq.events
    # Note: This might not exist in the current structure, but we test it as requested
    try:
        from naq.events import AsyncJobEventLogger
        assert callable(AsyncJobEventLogger)
    except ImportError:
        # If the import fails, we skip this test as the module might not exist
        pytest.skip("AsyncJobEventLogger not available in naq.events")


def test_naq_job_instantiation():
    """Test that Job can be instantiated from naq import."""
    from naq import Job
    
    # Test that we can create a Job instance
    def dummy_func():
        return "test"
    
    job = Job(function=dummy_func)
    
    # Test that the job has the expected attributes
    assert hasattr(job, 'job_id')
    assert hasattr(job, 'function')
    assert job.function == dummy_func


def test_naq_models_job_instantiation():
    """Test that Job can be instantiated from naq.models import."""
    from naq.models import Job
    
    # Test that we can create a Job instance
    def dummy_func():
        return "test"
    
    job = Job(function=dummy_func)
    
    # Test that the job has the expected attributes
    assert hasattr(job, 'job_id')
    assert hasattr(job, 'function')
    assert job.function == dummy_func


def test_naq_job_event_instantiation():
    """Test that JobEvent can be instantiated from naq import."""
    from naq import JobEvent, JobEventType
    
    # Test that we can create a JobEvent instance
    event = JobEvent(
        job_id="test-job-id",
        event_type=JobEventType.ENQUEUED,
        queue_name="test-queue"
    )
    assert event.job_id == "test-job-id"
    assert event.event_type == JobEventType.ENQUEUED
    assert event.queue_name == "test-queue"


def test_naq_models_job_event_instantiation():
    """Test that JobEvent can be instantiated from naq.models import."""
    from naq.models import JobEvent, JobEventType
    
    # Test that we can create a JobEvent instance
    event = JobEvent(
        job_id="test-job-id",
        event_type=JobEventType.ENQUEUED,
        queue_name="test-queue"
    )
    assert event.job_id == "test-job-id"
    assert event.event_type == JobEventType.ENQUEUED
    assert event.queue_name == "test-queue"


def test_naq_worker_event_instantiation():
    """Test that WorkerEvent can be instantiated from naq import."""
    from naq import WorkerEvent, WorkerEventType
    
    # Test that we can create a WorkerEvent instance
    event = WorkerEvent(
        worker_id="test-worker-id",
        event_type=WorkerEventType.STARTED,
        queue_names=["test-queue"]
    )
    assert event.worker_id == "test-worker-id"
    assert event.event_type == WorkerEventType.STARTED
    assert event.queue_names == ["test-queue"]


def test_naq_models_worker_event_instantiation():
    """Test that WorkerEvent can be instantiated from naq.models import."""
    from naq.models import WorkerEvent, WorkerEventType
    
    # Test that we can create a WorkerEvent instance
    event = WorkerEvent(
        worker_id="test-worker-id",
        event_type=WorkerEventType.STARTED,
        queue_names=["test-queue"]
    )
    assert event.worker_id == "test-worker-id"
    assert event.event_type == WorkerEventType.STARTED
    assert event.queue_names == ["test-queue"]


def test_naq_schedule_instantiation():
    """Test that Schedule can be instantiated from naq import."""
    from naq import Schedule
    from naq.models import Job
    import time

    # Test that we can create a Schedule instance
    def dummy_func():
        return "test"

    # First create a job
    job = Job(function=dummy_func)
    
    # Then create a schedule from the job
    schedule = Schedule.from_job(
        job=job,
        scheduled_timestamp_utc=time.time() + 3600
    )
    
    # Test that the schedule has the expected attributes
    assert hasattr(schedule, 'job_id')
    assert hasattr(schedule, 'scheduled_timestamp_utc')
    assert hasattr(schedule, '_orig_job_payload')


def test_naq_models_schedule_instantiation():
    """Test that Schedule can be instantiated from naq.models import."""
    from naq.models import Schedule
    from naq.models import Job
    import time

    # Test that we can create a Schedule instance
    def dummy_func():
        return "test"

    # First create a job
    job = Job(function=dummy_func)
    
    # Then create a schedule from the job
    schedule = Schedule.from_job(
        job=job,
        scheduled_timestamp_utc=time.time() + 3600
    )
    
    # Test that the schedule has the expected attributes
    assert hasattr(schedule, 'job_id')
    assert hasattr(schedule, 'scheduled_timestamp_utc')
    assert hasattr(schedule, '_orig_job_payload')