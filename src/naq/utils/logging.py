"""Logging utilities for NAQ.

This module contains common logging utilities used throughout the NAQ codebase.
"""

import json
import logging
import sys
import time
from contextlib import contextmanager
from typing import Any, Dict, Iterator, Optional, Union

from loguru import logger


def setup_logging(
    level: str = "INFO",
    format_string: Optional[str] = None,
    enable_file: bool = False,
    file_path: Optional[str] = None,
) -> None:
    """Set up logging configuration for NAQ.

    This function configures the loguru logger with appropriate settings
    for the NAQ application. It can configure both console and file logging.

    Args:
        level: The logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
            Defaults to "INFO".
        format_string: Custom format string for log messages. If None,
            uses the default NAQ format.
        enable_file: Whether to enable file logging. Defaults to False.
        file_path: Path to the log file. If None and enable_file is True,
            uses "naq.log" in the current directory.

    Examples:
        Basic setup with default settings:

        ```python
        from naq.utils import setup_logging
        setup_logging()
        ```

        Setup with custom level and file logging:

        ```python
        from naq.utils import setup_logging
        setup_logging(
            level="DEBUG",
            enable_file=True,
            file_path="my_app.log"
        )
        ```
    """
    # Remove default handler
    logger.remove()

    # Default format
    if format_string is None:
        format_string = (
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
            "<level>{message}</level>"
        )

    # Add console handler
    logger.add(
        sys.stderr,
        format=format_string,
        level=level,
        colorize=True,
    )

    # Add file handler if requested
    if enable_file:
        log_file = file_path or "naq.log"
        logger.add(
            log_file,
            format=format_string,
            level=level,
            rotation="10 MB",
            retention="1 week",
            compression="gz",
        )


class StructuredLogger:
    """A logger wrapper that provides structured logging capabilities.

    This class wraps a standard Python logging.Logger instance and provides
    methods for logging with structured context data.
    """

    def __init__(self, name: str, level: Union[int, str] = logging.INFO) -> None:
        """Initialize the StructuredLogger.

        Args:
            name: The name of the logger.
            level: The logging level, either as a string or logging level constant.
        """
        self._logger = logging.getLogger(name)
        self._logger.setLevel(level)

        # Add a default handler if none exists
        if not self._logger.handlers:
            handler = logging.StreamHandler(sys.stderr)
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            self._logger.addHandler(handler)

    def _log_with_context(self, level: int, message: str, **kwargs: Any) -> None:
        """Log a message with structured context.

        Args:
            level: The logging level.
            message: The log message.
            **kwargs: Additional structured data to include in the log.
        """
        # Create a copy of kwargs to avoid modifying the original
        context = kwargs.copy()

        # Format the message with structured context
        if context:
            formatted_message = f"{message} | Context: {context}"
        else:
            formatted_message = message

        # Log with the formatted message
        self._logger.log(level, formatted_message)

    def info(self, message: str, **kwargs: Any) -> None:
        """Log an info message with structured context.

        Args:
            message: The log message.
            **kwargs: Additional structured data to include in the log.
        """
        self._log_with_context(logging.INFO, message, **kwargs)

    def error(self, message: str, **kwargs: Any) -> None:
        """Log an error message with structured context.

        Args:
            message: The log message.
            **kwargs: Additional structured data to include in the log.
        """
        self._log_with_context(logging.ERROR, message, **kwargs)

    def debug(self, message: str, **kwargs: Any) -> None:
        """Log a debug message with structured context.

        Args:
            message: The log message.
            **kwargs: Additional structured data to include in the log.
        """
        self._log_with_context(logging.DEBUG, message, **kwargs)

    def warning(self, message: str, **kwargs: Any) -> None:
        """Log a warning message with structured context.

        Args:
            message: The log message.
            **kwargs: Additional structured data to include in the log.
        """
        self._log_with_context(logging.WARNING, message, **kwargs)

    def critical(self, message: str, **kwargs: Any) -> None:
        """Log a critical message with structured context.

        Args:
            message: The log message.
            **kwargs: Additional structured data to include in the log.
        """
        self._log_with_context(logging.CRITICAL, message, **kwargs)

    @contextmanager
    def operation_context(self, operation_name: str, **context: Any) -> Iterator[None]:
        """Context manager for logging operation start, completion, and failures.

        This context manager logs the start of an operation, its completion,
        and any failures that occur during the operation. It also includes
        timing information.

        Args:
            operation_name: The name of the operation.
            **context: Additional contextual information to include in logs.

        Yields:
            None

        Example:
            ```python
            logger = StructuredLogger("my_app")

            with logger.operation_context("data_processing", batch_id="123"):
                # Process data here
                process_data()
            ```
        """
        start_time = time.perf_counter()

        # Log operation start
        self.info(
            f"Starting operation: {operation_name}",
            operation=operation_name,
            status="started",
            **context,
        )

        try:
            yield
            # Calculate duration
            duration = time.perf_counter() - start_time

            # Log successful completion
            self.info(
                f"Completed operation: {operation_name}",
                operation=operation_name,
                status="completed",
                duration_seconds=duration,
                **context,
            )
        except Exception as e:
            # Calculate duration
            duration = time.perf_counter() - start_time

            # Log failure
            self.error(
                f"Failed operation: {operation_name}",
                operation=operation_name,
                status="failed",
                duration_seconds=duration,
                error=str(e),
                error_type=type(e).__name__,
                **context,
            )
            raise


class JSONFormatter(logging.Formatter):
    """A JSON formatter for logging records.

    This formatter converts LogRecord objects into JSON strings, including
    standard attributes and any extra fields passed to the logger.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Format a LogRecord as a JSON string.

        Args:
            record: The LogRecord to format.

        Returns:
            A JSON string representation of the LogRecord.
        """
        # Create a dictionary with standard LogRecord attributes
        log_data = {
            "levelname": record.levelname,
            "name": record.name,
            "asctime": self.formatTime(record),
            "message": record.getMessage(),
        }

        # Add exception information if present
        if record.exc_info:
            log_data["exc_info"] = self.formatException(record.exc_info)

        # Add any extra fields passed to the logger
        # Filter out standard LogRecord attributes
        standard_attributes = {
            "args",
            "asctime",
            "created",
            "exc_info",
            "exc_text",
            "filename",
            "funcName",
            "levelname",
            "levelno",
            "lineno",
            "module",
            "msecs",
            "message",
            "msg",
            "name",
            "pathname",
            "process",
            "processName",
            "relativeCreated",
            "stack_info",
            "thread",
            "threadName",
        }

        for key, value in record.__dict__.items():
            if key not in standard_attributes:
                log_data[key] = value

        # Convert to JSON string
        return json.dumps(log_data, default=str)


def setup_structured_logging(
    level: Union[int, str] = logging.INFO,
    format_type: str = "text",
    extra_fields: Optional[Dict[str, Any]] = None,
) -> StructuredLogger:
    """Set up structured logging for the application.

    This function configures the root logger with structured logging capabilities.
    It removes existing handlers and adds a new handler with the specified format.

    Args:
        level: The logging level, either as a string or logging level constant.
            Defaults to logging.INFO.
        format_type: The format type for log messages. Either "json" or "text".
            Defaults to "text".
        extra_fields: Additional fields to include in all log messages.
            Defaults to None.

    Returns:
        A StructuredLogger instance wrapping the configured root logger.

    Raises:
        ValueError: If format_type is not "json" or "text".

    Examples:
        Set up structured logging with JSON format:

        ```python
        from naq.utils.logging import setup_structured_logging
        logger = setup_structured_logging(level="DEBUG", format_type="json")
        logger.info("Application started", app_name="my_app")
        ```

        Set up structured logging with text format:

        ```python
        from naq.utils.logging import setup_structured_logging
        logger = setup_structured_logging(level="INFO", format_type="text")
        logger.info("Application started")
        ```
    """
    # Validate format_type
    if format_type not in ("json", "text"):
        raise ValueError(f"format_type must be 'json' or 'text', got '{format_type}'")

    # Get the root logger
    root_logger = logging.getLogger()

    # Set the logging level
    root_logger.setLevel(level)

    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Create a new stream handler
    handler = logging.StreamHandler(sys.stderr)

    # Set the formatter based on format_type
    if format_type == "json":
        formatter = JSONFormatter()
    else:  # format_type == "text"
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

    handler.setFormatter(formatter)

    # Add the handler to the root logger
    root_logger.addHandler(handler)

    # Create and return a StructuredLogger
    return StructuredLogger("root", level)
