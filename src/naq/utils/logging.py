"""
Logging utilities for NAQ

This module provides structured logging capabilities using loguru.
"""

import os
import sys
from typing import Any, Dict, Optional

from loguru import logger


class StructuredLogger:
    """
    Structured logger for NAQ components.
    
    This class provides a consistent interface for structured logging
    throughout the NAQ library, using loguru under the hood.
    """
    
    def __init__(self, name: str, **extra_fields: Any) -> None:
        """
        Initialize the structured logger.
        
        Args:
            name: Name of the logger.
            **extra_fields: Additional fields to include in all log messages.
        """
        self.name = name
        self.extra_fields = extra_fields or {}
    
    def _log(
        self,
        level: str,
        message: str,
        **fields: Any,
    ) -> None:
        """
        Log a message with structured fields.
        
        Args:
            level: Log level (debug, info, warning, error, critical).
            message: Log message.
            **fields: Additional fields to include in this log message.
        """
        # Combine default extra fields with message-specific fields
        all_fields = {**self.extra_fields, **fields}
        all_fields["logger"] = self.name
        
        # Log with structured fields
        logger.bind(**all_fields).log(level, message)
    
    def debug(self, message: str, **fields: Any) -> None:
        """Log a debug message."""
        self._log("DEBUG", message, **fields)
    
    def info(self, message: str, **fields: Any) -> None:
        """Log an info message."""
        self._log("INFO", message, **fields)
    
    def warning(self, message: str, **fields: Any) -> None:
        """Log a warning message."""
        self._log("WARNING", message, **fields)
    
    def error(self, message: str, **fields: Any) -> None:
        """Log an error message."""
        self._log("ERROR", message, **fields)
    
    def critical(self, message: str, **fields: Any) -> None:
        """Log a critical message."""
        self._log("CRITICAL", message, **fields)
    
    def exception(self, message: str, **fields: Any) -> None:
        """Log an exception message with traceback."""
        all_fields = {**self.extra_fields, **fields}
        all_fields["logger"] = self.name
        logger.bind(**all_fields).exception(message)


def get_logger(name: str, **extra_fields: Any) -> StructuredLogger:
    """
    Get a structured logger instance.
    
    Args:
        name: Name of the logger.
        **extra_fields: Additional fields to include in all log messages.
        
    Returns:
        StructuredLogger: A structured logger instance.
    """
    return StructuredLogger(name, **extra_fields)


def setup_logging() -> None:
    """
    Set up logging configuration for NAQ.
    
    This function configures loguru logger with appropriate settings
    for the NAQ library.
    """
    # Remove default handler
    logger.remove()
    
    # Add stderr handler with appropriate format
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level=os.getenv("NAQ_LOG_LEVEL", "INFO"),
    )