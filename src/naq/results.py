# src/naq/results.py
from typing import Any, Dict, List, Optional

from nats.js import JetStreamContext
from nats.js.errors import KeyNotFoundError

from .connection_utils import nats_jetstream
from .exceptions import JobNotFoundError, NaqException
from .models.jobs import Job
from .settings import DEFAULT_NATS_URL, DEFAULT_RESULT_TTL_SECONDS, RESULT_KV_NAME


class Results:
    """
    Manages job results stored in NATS Key-Value store.
    
    This class encapsulates all operations related to storing, retrieving,
    and managing job results in the NATS KV store.
    """

    def __init__(self, nats_url: str = DEFAULT_NATS_URL):
        """
        Initialize the Results manager.

        Args:
            nats_url: NATS server URL. Defaults to DEFAULT_NATS_URL.
        """
        self.nats_url = nats_url

    async def add_job_result(
        self, 
        job_id: str, 
        result_data: Dict[str, Any], 
        result_ttl: Optional[int] = None
    ) -> None:
        """
        Store a job result in the KV store.

        Args:
            job_id: The ID of the job.
            result_data: The result data to store.
            result_ttl: Time-to-live for the result in seconds. 
                       Defaults to DEFAULT_RESULT_TTL_SECONDS.

        Raises:
            NaqException: If storing the result fails.
        """
        try:
            config = {'nats_url': self.nats_url}
            async with nats_jetstream(config) as (conn, js):
                kv = await js.key_value(bucket=RESULT_KV_NAME)
                
                # Serialize the result data
                serialized_result = Job.serialize_result(
                    result=result_data.get("result"),
                    status=result_data.get("status"),
                    error=result_data.get("error"),
                    traceback_str=result_data.get("traceback")
                )
                
                # Set TTL (default to settings value if not provided)
                ttl = result_ttl if result_ttl is not None else DEFAULT_RESULT_TTL_SECONDS
                
                # Store the result with TTL
                await kv.put(job_id, serialized_result, ttl=ttl)
                
        except Exception as e:
            raise NaqException(f"Failed to store result for job {job_id}: {e}") from e

    async def fetch_job_result(self, job_id: str) -> Dict[str, Any]:
        """
        Fetch a specific job result from the KV store.

        Args:
            job_id: The ID of the job.

        Returns:
            The job result data as a dictionary.

        Raises:
            JobNotFoundError: If the job result is not found.
            NaqException: If fetching the result fails.
        """
        try:
            config = {'nats_url': self.nats_url}
            async with nats_jetstream(config) as (conn, js):
                kv = await js.key_value(bucket=RESULT_KV_NAME)
                
                try:
                    entry = await kv.get(job_id)
                    result_data = Job.deserialize_result(entry.value)
                    return result_data
                except KeyNotFoundError:
                    raise JobNotFoundError(
                        f"Result for job {job_id} not found. It may not have completed, "
                        f"failed, or the result expired."
                    ) from None
                    
        except Exception as e:
            if isinstance(e, JobNotFoundError):
                raise
            raise NaqException(f"Failed to fetch result for job {job_id}: {e}") from e

    async def list_all_job_results(self) -> List[str]:
        """
        List all job IDs for which results are stored.

        Returns:
            A list of job IDs that have results stored.

        Raises:
            NaqException: If listing the results fails.
        """
        try:
            config = {'nats_url': self.nats_url}
            async with nats_jetstream(config) as (conn, js):
                kv = await js.key_value(bucket=RESULT_KV_NAME)
                
                # Get all keys in the KV store
                keys = await kv.keys()
                return list(keys)
                
        except Exception as e:
            raise NaqException(f"Failed to list job results: {e}") from e

    async def purge_all_job_results(self) -> None:
        """
        Delete all job results from the KV store.

        Raises:
            NaqException: If purging the results fails.
        """
        try:
            config = {'nats_url': self.nats_url}
            async with nats_jetstream(config) as (conn, js):
                kv = await js.key_value(bucket=RESULT_KV_NAME)
                
                # Get all keys and delete them
                keys = await kv.keys()
                for key in keys:
                    await kv.delete(key)
                    
        except Exception as e:
            raise NaqException(f"Failed to purge job results: {e}") from e

    async def delete_job_result(self, job_id: str) -> None:
        """
        Delete a specific job result from the KV store.

        Args:
            job_id: The ID of the job result to delete.

        Raises:
            NaqException: If deleting the result fails.
        """
        try:
            config = {'nats_url': self.nats_url}
            async with nats_jetstream(config) as (conn, js):
                kv = await js.key_value(bucket=RESULT_KV_NAME)
                
                try:
                    await kv.delete(job_id)
                except KeyNotFoundError:
                    # If the key doesn't exist, we don't need to raise an error
                    # as the end result is the same - the key doesn't exist
                    pass
                    
        except Exception as e:
            raise NaqException(f"Failed to delete result for job {job_id}: {e}") from e