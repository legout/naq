"""Worker module.

This module provides the main Worker class and related components for processing
jobs from NATS queues. The module has been refactored to separate concerns into
dedicated submodules.
"""

from .core import Worker
from .failed import FailedJobHandler
from .jobs import JobStatusManager
from .status import WorkerStatusManager

__all__ = [
    "Worker",
    "JobStatusManager",
    "FailedJobHandler",
    "WorkerStatusManager",
]
