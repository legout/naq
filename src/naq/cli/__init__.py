# src/naq/cli/__init__.py
"""
NAQ CLI Package

This package contains the command-line interface for NAQ, organized into
focused command modules for better maintainability.

Command modules:
- main: Main CLI app, version handling, and shared utilities
- worker_commands: Worker management (start, list, events)
- job_commands: Job and queue management (purge, control)
- scheduler_commands: Scheduler management (start, list scheduled jobs)
- event_commands: Event monitoring (stream, history, stats)  
- system_commands: System utilities (dashboard, config)
"""

from .main import app, main

__all__ = ["app", "main"]