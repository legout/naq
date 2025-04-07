# src/naq/exceptions.py


class NaqException(Exception):
    """Base exception class for naq."""

    pass


class ConnectionError(NaqException):
    """Raised when there's an issue connecting to NATS."""

    pass


class ConfigurationError(NaqException):
    """Raised for configuration issues."""

    pass


class SerializationError(NaqException):
    """Raised when job serialization/deserialization fails."""

    pass


class JobExecutionError(NaqException):
    """Raised when a job fails during execution."""

    pass


class JobNotFoundError(NaqException):
    """Raised when a job is not found in the queue."""

    pass


class NaqConnectionError(NaqException):
    """Raised when NATS connection fails."""

    pass
