# src/naq/exceptions.py


class NaqException(Exception):
    """Base exception class for naq."""

    pass


# Note: ConnectionError has been removed. Use NaqConnectionError for all NATS connection-related errors.


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
    """Raised when there is a connection-related error with NATS."""

    pass
