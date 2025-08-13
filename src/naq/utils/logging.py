# src/naq/utils/logging.py
"""
Structured logging utilities for NAQ.

This module provides structured logging with consistent formatting,
context management, and performance tracking.
"""

import json
import time
import logging
import threading
from typing import Dict, Any, Optional, Union
from contextlib import contextmanager
from dataclasses import dataclass, asdict
import uuid

from loguru import logger as loguru_logger


@dataclass
class LogContext:
    """Structured log context."""
    operation: str
    operation_id: str
    start_time: float
    extra_fields: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


class StructuredLogger:
    """
    Structured logging with consistent formatting and context management.
    
    Provides structured logging with automatic context injection and
    performance tracking capabilities.
    """
    
    def __init__(
        self, 
        name: str, 
        extra_fields: Optional[Dict[str, Any]] = None,
        use_loguru: bool = True
    ):
        """
        Initialize structured logger.
        
        Args:
            name: Logger name
            extra_fields: Default extra fields for all log messages
            use_loguru: Whether to use loguru (default) or standard logging
        """
        self.name = name
        self.extra_fields = extra_fields or {}
        self.use_loguru = use_loguru
        
        if use_loguru:
            self.logger = loguru_logger.bind(logger_name=name, **self.extra_fields)
        else:
            self.logger = logging.getLogger(name)
        
        self._contexts = {}  # Thread-local contexts
    
    def _get_context(self) -> Dict[str, Any]:
        """Get current thread's context."""
        thread_id = threading.get_ident()
        return self._contexts.get(thread_id, {})
    
    def _set_context(self, context: Dict[str, Any]):
        """Set current thread's context."""
        thread_id = threading.get_ident()
        self._contexts[thread_id] = context
    
    def _clear_context(self):
        """Clear current thread's context."""
        thread_id = threading.get_ident()
        self._contexts.pop(thread_id, None)
    
    def _log_with_context(
        self, 
        level: Union[int, str], 
        message: str, 
        **kwargs
    ):
        """Log with structured context."""
        # Merge all context: defaults + thread context + call-specific
        context = {
            **self.extra_fields,
            **self._get_context(),
            **kwargs
        }
        
        if self.use_loguru:
            # Use loguru's structured logging
            if isinstance(level, str):
                level = level.upper()
            
            self.logger.bind(**context).log(level, message)
        else:
            # Use standard logging with extra fields
            if isinstance(level, str):
                level = getattr(logging, level.upper())
            
            self.logger.log(level, message, extra=context)
    
    def debug(self, message: str, **kwargs):
        """Log debug message."""
        self._log_with_context("DEBUG", message, **kwargs)
    
    def info(self, message: str, **kwargs):
        """Log info message."""
        self._log_with_context("INFO", message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log warning message."""
        self._log_with_context("WARNING", message, **kwargs)
    
    def error(self, message: str, **kwargs):
        """Log error message."""
        self._log_with_context("ERROR", message, **kwargs)
    
    def critical(self, message: str, **kwargs):
        """Log critical message."""
        self._log_with_context("CRITICAL", message, **kwargs)
    
    @contextmanager
    def operation_context(
        self, 
        operation_name: str, 
        **context_data
    ):
        """
        Context manager for operation logging with timing.
        
        Args:
            operation_name: Name of the operation
            **context_data: Additional context data
            
        Usage:
            with logger.operation_context("database_query", table="users"):
                # Operation is automatically timed and logged
                result = db.query("SELECT * FROM users")
                
            # Logs operation start, end, and duration
        """
        operation_id = str(uuid.uuid4())[:8]
        start_time = time.perf_counter()
        
        # Build context
        operation_context = {
            'operation': operation_name,
            'operation_id': operation_id,
            'start_time': start_time,
            **context_data
        }
        
        # Set thread-local context
        old_context = self._get_context()
        merged_context = {**old_context, **operation_context}
        self._set_context(merged_context)
        
        self.info(f"Starting {operation_name}")
        
        try:
            yield operation_id
            
            duration_ms = (time.perf_counter() - start_time) * 1000
            self.info(
                f"Completed {operation_name}",
                duration_ms=duration_ms,
                status="success"
            )
            
        except Exception as e:
            duration_ms = (time.perf_counter() - start_time) * 1000
            self.error(
                f"Failed {operation_name}: {e}",
                error_type=type(e).__name__,
                error_message=str(e),
                duration_ms=duration_ms,
                status="error"
            )
            raise
        
        finally:
            # Restore previous context
            self._set_context(old_context)
    
    @contextmanager
    def context(self, **context_data):
        """
        Add temporary context to all log messages.
        
        Args:
            **context_data: Context data to add
            
        Usage:
            with logger.context(user_id=123, request_id="abc"):
                logger.info("User logged in")  # Includes user_id and request_id
        """
        old_context = self._get_context()
        merged_context = {**old_context, **context_data}
        self._set_context(merged_context)
        
        try:
            yield
        finally:
            self._set_context(old_context)
    
    def with_context(self, **context_data) -> 'StructuredLogger':
        """
        Create a new logger with additional context.
        
        Args:
            **context_data: Context data to add
            
        Returns:
            New logger instance with added context
            
        Usage:
            user_logger = logger.with_context(user_id=123)
            user_logger.info("User action")  # Includes user_id in all messages
        """
        new_extra_fields = {**self.extra_fields, **context_data}
        return StructuredLogger(
            self.name,
            new_extra_fields,
            self.use_loguru
        )


class JSONFormatter(logging.Formatter):
    """
    JSON formatter for structured logging with standard logging.
    
    Formats log records as JSON with consistent field structure.
    """
    
    def __init__(self, include_extra: bool = True):
        """
        Initialize JSON formatter.
        
        Args:
            include_extra: Whether to include extra fields from log records
        """
        super().__init__()
        self.include_extra = include_extra
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_entry = {
            'timestamp': record.created,
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
            'thread_id': record.thread,
            'process_id': record.process
        }
        
        # Add extra fields if enabled
        if self.include_extra:
            for key, value in record.__dict__.items():
                # Skip standard fields and private fields
                if key not in log_entry and not key.startswith('_'):
                    try:
                        # Ensure value is JSON serializable
                        json.dumps(value)
                        log_entry[key] = value
                    except (TypeError, ValueError):
                        # Convert non-serializable values to string
                        log_entry[key] = str(value)
        
        # Add exception info if present
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)
        
        # Add stack info if present
        if record.stack_info:
            log_entry['stack_info'] = record.stack_info
        
        return json.dumps(log_entry, default=str, ensure_ascii=False)


def setup_structured_logging(
    level: str = "INFO",
    format_type: str = "json",
    extra_fields: Optional[Dict[str, Any]] = None,
    logger_name: str = "naq"
) -> StructuredLogger:
    """
    Setup structured logging configuration.
    
    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format_type: Format type ("json" or "text")
        extra_fields: Default extra fields for all messages
        logger_name: Base logger name
        
    Returns:
        Configured StructuredLogger instance
        
    Usage:
        logger = setup_structured_logging(
            level="DEBUG",
            format_type="json",
            extra_fields={"service": "naq", "version": "1.0.0"}
        )
    """
    # Configure standard logging
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create new handler
    handler = logging.StreamHandler()
    
    if format_type == "json":
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
    
    root_logger.addHandler(handler)
    
    # Return structured logger
    return StructuredLogger(
        logger_name, 
        extra_fields, 
        use_loguru=False  # Use standard logging when explicitly configured
    )


class LogMetrics:
    """
    Track logging metrics and statistics.
    
    Useful for monitoring application health and debugging.
    """
    
    def __init__(self):
        """Initialize metrics collector."""
        self.metrics = {
            'total_logs': 0,
            'logs_by_level': {
                'DEBUG': 0,
                'INFO': 0,
                'WARNING': 0,
                'ERROR': 0,
                'CRITICAL': 0
            },
            'logs_by_logger': {},
            'operations': {},
            'start_time': time.time()
        }
        self._lock = threading.Lock()
    
    def record_log(
        self, 
        level: str, 
        logger_name: str, 
        operation: Optional[str] = None,
        duration_ms: Optional[float] = None
    ):
        """
        Record a log event.
        
        Args:
            level: Log level
            logger_name: Logger name
            operation: Operation name (if any)
            duration_ms: Operation duration (if any)
        """
        with self._lock:
            self.metrics['total_logs'] += 1
            
            if level in self.metrics['logs_by_level']:
                self.metrics['logs_by_level'][level] += 1
            
            if logger_name not in self.metrics['logs_by_logger']:
                self.metrics['logs_by_logger'][logger_name] = 0
            self.metrics['logs_by_logger'][logger_name] += 1
            
            if operation:
                if operation not in self.metrics['operations']:
                    self.metrics['operations'][operation] = {
                        'count': 0,
                        'total_duration_ms': 0,
                        'avg_duration_ms': 0
                    }
                
                op_stats = self.metrics['operations'][operation]
                op_stats['count'] += 1
                
                if duration_ms is not None:
                    op_stats['total_duration_ms'] += duration_ms
                    op_stats['avg_duration_ms'] = (
                        op_stats['total_duration_ms'] / op_stats['count']
                    )
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics."""
        with self._lock:
            metrics = self.metrics.copy()
            metrics['uptime_seconds'] = time.time() - metrics['start_time']
            return metrics
    
    def reset_metrics(self):
        """Reset all metrics."""
        with self._lock:
            self.metrics = {
                'total_logs': 0,
                'logs_by_level': {
                    'DEBUG': 0,
                    'INFO': 0,
                    'WARNING': 0,
                    'ERROR': 0,
                    'CRITICAL': 0
                },
                'logs_by_logger': {},
                'operations': {},
                'start_time': time.time()
            }


# Global metrics collector
_log_metrics = LogMetrics()


def get_log_metrics() -> LogMetrics:
    """Get the global log metrics collector."""
    return _log_metrics


class MetricsHandler(logging.Handler):
    """
    Logging handler that collects metrics.
    
    Can be added to any logger to automatically track metrics.
    """
    
    def __init__(self, metrics_collector: Optional[LogMetrics] = None):
        """
        Initialize metrics handler.
        
        Args:
            metrics_collector: Metrics collector to use (defaults to global)
        """
        super().__init__()
        self.metrics = metrics_collector or _log_metrics
    
    def emit(self, record: logging.LogRecord):
        """Handle a log record by updating metrics."""
        try:
            # Extract operation info from record
            operation = getattr(record, 'operation', None)
            duration_ms = getattr(record, 'duration_ms', None)
            
            self.metrics.record_log(
                record.levelname,
                record.name,
                operation,
                duration_ms
            )
        except Exception:
            # Don't let metrics collection break logging
            pass


# Backward compatibility alias for tests
setup_logging = setup_structured_logging


def get_logger(name: str, **kwargs) -> StructuredLogger:
    """
    Get a structured logger instance for backward compatibility.
    
    Args:
        name: Logger name
        **kwargs: Additional arguments
        
    Returns:
        StructuredLogger instance
    """
    return StructuredLogger(name, **kwargs)