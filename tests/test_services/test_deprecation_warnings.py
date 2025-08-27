"""Test deprecation warnings for legacy import paths."""

import warnings
import pytest
from naq import Job, Queue, Worker


def test_job_deprecation_warning():
    """Test that importing Job from the top level emits a deprecation warning."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        
        # This should trigger a deprecation warning
        job = Job(function=lambda: None, args=(), kwargs={})
        
        # Check that a warning was issued
        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)
        assert "naq.models.jobs.Job" in str(w[0].message)
        assert "will be removed in version 1.0.0" in str(w[0].message)


def test_queue_deprecation_warning():
    """Test that importing Queue from the top level emits a deprecation warning."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        
        # This should trigger a deprecation warning
        queue = Queue(name="test_queue")
        
        # Check that a warning was issued
        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)
        assert "naq.queue.core.Queue" in str(w[0].message)
        assert "will be removed in version 1.0.0" in str(w[0].message)


def test_worker_deprecation_warning():
    """Test that importing Worker from the top level emits a deprecation warning."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        
        # This should trigger a deprecation warning
        worker = Worker(queues="test_queue")
        
        # Check that a warning was issued
        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)
        assert "naq.worker.core.Worker" in str(w[0].message)
        assert "will be removed in version 1.0.0" in str(w[0].message)


def test_modular_imports_no_warning():
    """Test that importing from the new modular paths doesn't emit warnings."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        
        # These should not trigger warnings
        from naq.models.jobs import Job as ModularJob
        from naq.queue.core import Queue as ModularQueue
        from naq.worker.core import Worker as ModularWorker
        
        job = ModularJob(function=lambda: None, args=(), kwargs={})
        queue = ModularQueue(name="test_queue")
        worker = ModularWorker(queues="test_queue")
        
        # Check that no warnings were issued
        assert len(w) == 0