# src/naq/utils/job_serialization.py
"""
Utility functions for job serialization to avoid circular imports.

This module provides serialization functions for Job objects to avoid circular
import dependencies between the models.jobs and serializers modules.
"""

from typing import Any, Dict, List, Optional, Tuple

from ..models.enums import JOB_STATUS
from ..models.jobs import Job, JobResult
from ..serializers import get_serializer


def serialize_job(job: Job) -> bytes:
    """
    Serializes the job data for sending over NATS.

    Args:
        job: The job to serialize

    Returns:
        bytes: Serialized job data suitable for transmission over NATS

    This method uses the configured serializer (pickle or JSON) to convert
    the job object into a byte representation that can be sent over NATS.
    """
    serializer = get_serializer()
    return serializer.serialize_job(job)


def deserialize_job(data: bytes) -> Job:
    """
    Deserializes job data received from NATS.

    Args:
        data: Byte data containing the serialized job information

    Returns:
        Job: A fully reconstructed Job object

    This method uses the configured serializer to reconstruct a Job object
    from its serialized byte representation.
    """
    serializer = get_serializer()
    return serializer.deserialize_job(data)


def serialize_failed_job(job: Job) -> bytes:
    """
    Serializes job data including error info for the failed queue.

    Args:
        job: The job to serialize

    Returns:
        bytes: Serialized job data including error information

    This method is used when a job fails and needs to be sent to the
    failed job queue for later analysis or retry.
    """
    serializer = get_serializer()
    return serializer.serialize_failed_job(job)


def serialize_result(
    result: Any,
    status: JOB_STATUS,
    error: Optional[str] = None,
    traceback_str: Optional[str] = None,
) -> bytes:
    """
    Serializes job result data.

    Args:
        result: The result value from the job execution
        status: The final status of the job
        error: Optional error message if the job failed
        traceback_str: Optional traceback string if the job failed

    Returns:
        bytes: Serialized result data

    This method serializes the result of job execution, including any
    error information, for storage in the result backend.
    """
    serializer = get_serializer()
    return serializer.serialize_result(result, status, error, traceback_str)


def deserialize_result(data: bytes) -> Dict[str, Any]:
    """
    Deserializes job result data.

    Args:
        data: Byte data containing the serialized result information

    Returns:
        Dict[str, Any]: A dictionary containing the result data including
        status, result value, error message, and traceback if applicable

    This method reconstructs result data from its serialized representation
    for use by clients fetching job results.
    """
    serializer = get_serializer()
    return serializer.deserialize_result(data)
