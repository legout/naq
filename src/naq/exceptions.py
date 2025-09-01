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


class ValidationError(NaqException):
    """Raised when validation of a parameter or value fails."""

    pass


class TypeConversionError(NaqException):
    """Raised when type conversion fails in ensure_type."""

    pass


class LeaderElectionError(NaqException):
    """Raised when there is an error in the leader election process."""

    pass


class LockAcquisitionError(LeaderElectionError):
    """Raised when there is an error acquiring the leader lock."""

    pass


class LockRenewalError(LeaderElectionError):
    """Raised when there is an error renewing the leader lock."""

    pass


class LockReleaseError(LeaderElectionError):
    """Raised when there is an error releasing the leader lock."""

    pass


class LockTimeoutError(LeaderElectionError):
    """Raised when a lock operation times out."""

    pass


class LockConflictError(LeaderElectionError):
    """Raised when there is a conflict in lock operations (e.g., concurrent modifications)."""

    pass


class LockDataError(LeaderElectionError):
    """Raised when there is an error with lock data (e.g., serialization, validation)."""

    pass


class LockUpdateError(LeaderElectionError):
    """Raised when there is an error updating the leader lock."""

    pass



class JobResultError(NaqException):
    """Raised when there is an error with job results."""

    pass


class JobResultNotFound(JobResultError):
    """Raised when a job result is not found."""

    pass


class JobResultSerializationError(JobResultError):
    """Raised when job result serialization/deserialization fails."""

    pass


class JobResultStorageError(JobResultError):
    """Raised when job result storage operation fails."""

    pass