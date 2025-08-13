"""Worker module.

This module provides the main Worker class and related components for processing
jobs from NATS queues. The module has been refactored to separate concerns into
dedicated submodules.
"""

from .core import Worker
from .jobs import JobStatusManager
from .failed import FailedJobHandler
from .status import WorkerStatusManager

__all__ = [
    "Worker",
    "JobStatusManager",
    "FailedJobHandler",
    "WorkerStatusManager",
]
