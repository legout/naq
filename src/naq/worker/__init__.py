# src/naq/worker/__init__.py
"""
NAQ Worker Package

This package provides worker functionality for NAQ, organized into focused modules:
- core: Main Worker class
- status: WorkerStatusManager for worker status and heartbeat management
- jobs: JobStatusManager for job status tracking and dependency resolution
- failed: FailedJobHandler for failed job processing and retry logic

All public APIs are re-exported from this __init__.py for backward compatibility.
"""

# Import main Worker class
from .core import Worker

# Import manager classes (if they need to be public)
from .status import WorkerStatusManager
from .jobs import JobStatusManager  
from .failed import FailedJobHandler

__all__ = [
    "Worker",
    "WorkerStatusManager", 
    "JobStatusManager",
    "FailedJobHandler",
]