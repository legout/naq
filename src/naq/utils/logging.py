"""
Logging utilities for NAQ.

This module provides centralized logging configuration and utilities
for the NAQ job queue system using loguru.
"""

import sys
import time
import functools
from typing import Optional, Callable, Any, Dict
from contextlib import contextmanager

from loguru import logger

from ..settings import LOG_LEVEL, LOG_TO_FILE_ENABLED, LOG_FILE_PATH


def setup_logging(level: str | None = None) -> None:
    """Configures logging based on environment variables or provided level string using loguru."""
    logger.remove()  # Remove all existing handlers

    # Determine the effective log level
    # CLI argument takes precedence over environment variable
    effective_level = level.upper() if level else LOG_LEVEL

    # Add stdout handler
    logger.add(
        sys.stdout,
        level=effective_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        colorize=True,
    )

    # Add file handler if enabled
    if LOG_TO_FILE_ENABLED:
        logger.add(
            LOG_FILE_PATH,
            level=effective_level,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            rotation="10 MB",
            retention="1 week",  # Keep logs for 1 week
            compression="zip",  # Compress rotated logs
        )
    # Optionally silence overly verbose libraries if needed
    # logging.getLogger("nats").setLevel(logging.WARNING)


def get_logger(name: str = None):
    """
    Get a logger instance with the specified name.
    
    Args:
        name: Optional name for the logger. If None, returns the root logger.
        
    Returns:
        Logger instance.
    """
    if name:
        return logger.bind(name=name)
    return logger


def log_function_call(func: Callable = None, *, level: str = "DEBUG"):
    """
    Decorator to log function calls with arguments and return values.
    
    Args:
        func: Function to decorate
        level: Log level to use
        
    Returns:
        Decorated function
    """
    def decorator(f: Callable) -> Callable:
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            logger_ = get_logger(f.__name__)
            getattr(logger_, level.lower())(
                f"Calling {f.__name__} with args={args}, kwargs={kwargs}"
            )
            try:
                result = f(*args, **kwargs)
                getattr(logger_, level.lower())(
                    f"{f.__name__} returned {result}"
                )
                return result
            except Exception as e:
                getattr(logger_, level.lower())(
                    f"{f.__name__} raised {type(e).__name__}: {e}"
                )
                raise
        return wrapper
    
    if func is None:
        return decorator
    return decorator(func)


def log_performance(func: Callable = None, *, level: str = "INFO"):
    """
    Decorator to log function execution time.
    
    Args:
        func: Function to decorate
        level: Log level to use
        
    Returns:
        Decorated function
    """
    def decorator(f: Callable) -> Callable:
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            logger_ = get_logger(f.__name__)
            start_time = time.time()
            try:
                result = f(*args, **kwargs)
                end_time = time.time()
                getattr(logger_, level.lower())(
                    f"{f.__name__} completed in {end_time - start_time:.4f} seconds"
                )
                return result
            except Exception as e:
                end_time = time.time()
                getattr(logger_, level.lower())(
                    f"{f.__name__} failed after {end_time - start_time:.4f} seconds: {e}"
                )
                raise
        return wrapper
    
    if func is None:
        return decorator
    return decorator(func)


def log_errors(func: Callable = None, *, level: str = "ERROR", reraise: bool = True):
    """
    Decorator to log function errors.
    
    Args:
        func: Function to decorate
        level: Log level to use
        reraise: Whether to re-raise the exception
        
    Returns:
        Decorated function
    """
    def decorator(f: Callable) -> Callable:
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            logger_ = get_logger(f.__name__)
            try:
                return f(*args, **kwargs)
            except Exception as e:
                getattr(logger_, level.lower())(
                    f"{f.__name__} raised {type(e).__name__}: {e}",
                    exc_info=True
                )
                if reraise:
                    raise
                return None
        return wrapper
    
    if func is None:
        return decorator
    return decorator(func)


@contextmanager
def LogContext(**context: Dict[str, Any]):
    """
    Context manager for adding structured context to log messages.
    
    Args:
        **context: Context key-value pairs to add to log messages
    """
    logger_ = get_logger()
    with logger_.contextualize(**context):
        try:
            yield
        finally:
            pass


class PerformanceTimer:
    """
    A timer for measuring and logging performance metrics.
    """
    
    def __init__(self, name: str = None, auto_log: bool = True, level: str = "INFO"):
        """
        Initialize the performance timer.
        
        Args:
            name: Name for the timer (used in log messages)
            auto_log: Whether to automatically log when stopped
            level: Log level for auto-logging
        """
        self.name = name or "PerformanceTimer"
        self.auto_log = auto_log
        self.level = level
        self.start_time = None
        self.end_time = None
        self.elapsed = None
    
    def start(self) -> None:
        """Start the timer."""
        self.start_time = time.time()
        self.end_time = None
        self.elapsed = None
    
    def stop(self) -> float:
        """
        Stop the timer and return elapsed time.
        
        Returns:
            Elapsed time in seconds
        """
        if self.start_time is None:
            raise RuntimeError("Timer not started")
        
        self.end_time = time.time()
        self.elapsed = self.end_time - self.start_time
        
        if self.auto_log:
            logger_ = get_logger(self.name)
            getattr(logger_, self.level.lower())(
                f"Timer '{self.name}' completed in {self.elapsed:.4f} seconds"
            )
        
        return self.elapsed
    
    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop()
    
    def __str__(self) -> str:
        """String representation."""
        if self.elapsed is None:
            return f"{self.name} (not started)"
        return f"{self.name}: {self.elapsed:.4f}s"


def add_log_handler(
    sink,
    level: str = "INFO",
    format: str = None,
    filter: str = None,
    **kwargs
):
    """
    Add a custom log handler.
    
    Args:
        sink: The log sink (file path, callable, etc.)
        level: Log level for this handler
        format: Optional custom format string
        filter: Optional filter for this handler
        **kwargs: Additional arguments passed to logger.add()
        
    Returns:
        The handler ID that can be used to remove the handler later.
    """
    if format is None:
        format = "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}"
    
    return logger.add(
        sink,
        level=level,
        format=format,
        filter=filter,
        **kwargs
    )


def remove_log_handler(handler_id: int) -> None:
    """
    Remove a log handler by its ID.
    
    Args:
        handler_id: The ID of the handler to remove.
    """
    logger.remove(handler_id)


def set_log_level(level: str) -> None:
    """
    Set the global log level.
    
    Args:
        level: The log level to set (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    """
    # Remove all handlers and re-add them with the new level
    logger.remove()
    setup_logging(level)


def disable_library_logging(library_name: str, level: str = "WARNING") -> None:
    """
    Disable or reduce logging for a specific library.
    
    Args:
        library_name: Name of the library to silence
        level: Level to set for the library (default: WARNING)
    """
    # This is a placeholder - in loguru, you would typically use filters
    # to control logging from specific libraries
    pass


class StructuredLogger:
    """
    A logger that adds structured context to log messages.
    """
    
    def __init__(self, name: str = None, **context):
        """
        Initialize the structured logger.
        
        Args:
            name: Optional name for the logger
            **context: Default context fields to include in all log messages
        """
        self.name = name
        self.context = context or {}
    
    def bind(self, **additional_context):
        """
        Create a new logger with additional context.
        
        Args:
            **additional_context: Additional context fields
            
        Returns:
            New StructuredLogger instance with combined context
        """
        new_context = self.context.copy()
        new_context.update(additional_context)
        return StructuredLogger(self.name, **new_context)
    
    def _log(self, level: str, message: str, **extra_context):
        """Internal logging method."""
        context = self.context.copy()
        context.update(extra_context)
        
        log_entry = {"message": message, **context}
        
        if self.name:
            bound_logger = logger.bind(name=self.name)
        else:
            bound_logger = logger
            
        getattr(bound_logger, level.lower())(log_entry)
    
    def debug(self, message: str, **context):
        """Log a debug message."""
        self._log("DEBUG", message, **context)
    
    def info(self, message: str, **context):
        """Log an info message."""
        self._log("INFO", message, **context)
    
    def warning(self, message: str, **context):
        """Log a warning message."""
        self._log("WARNING", message, **context)
    
    def error(self, message: str, **context):
        """Log an error message."""
        self._log("ERROR", message, **context)
    
    def critical(self, message: str, **context):
        """Log a critical message."""
        self._log("CRITICAL", message, **context)