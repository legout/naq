# src/naq/job.py
"""
Job execution module.

This module provides the Job class for executing jobs, which has been moved to
src/naq/models/jobs.py for better organization.
"""

from .models.jobs import Job

__all__ = ["Job"]
