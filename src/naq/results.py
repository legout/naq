# src/naq/results.py
from typing import Any, Dict, List, Optional

from loguru import logger
from nats.js.errors import KeyNotFoundError

from .connection import (
    nats_kv_store,
)
from .services.config import create_global_config
from .exceptions import JobNotFoundError, NaqException
from .models.jobs import Job, JobResult
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

    def _get_kv_store_context(self):
        """
        Helper method to create a configured NATS KV store context manager.
        This encapsulates the common logic for setting up NATS configuration
        and obtaining the KV store context for RESULT_KV_NAME.
        """
        config = create_global_config()
        config.nats_url = self.nats_url
        try:
            logger.debug(
                "Preparing NATS KV store context",
                bucket_name=RESULT_KV_NAME,
                nats_url=config.nats_url
            )
            return nats_kv_store(RESULT_KV_NAME, config)
        except ConnectionError as e:
            logger.error(
                f"Connection error while preparing KV store context for {RESULT_KV_NAME}: {e}",
                exc_info=True
            )
            raise NaqException(f"Failed to connect to NATS server at {self.nats_url}: {e}") from e
        except Exception as e:
            logger.error(
                f"Failed to prepare KV store context for {RESULT_KV_NAME}: {e}",
                exc_info=True
            )
            raise NaqException(f"Failed to prepare KV store context: {e}") from e

    async def add_job_result(
        self, job_id: str, result_data: Dict[str, Any], result_ttl: Optional[int] = None
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
            ValueError: If job_id is empty or result_data is invalid.
        """
        # Validate inputs
        if not job_id or not isinstance(job_id, str):
            logger.error("Invalid job_id provided", job_id=job_id, type=type(job_id).__name__)
            raise ValueError("job_id must be a non-empty string")
            
        if not result_data or not isinstance(result_data, dict):
            logger.error("Invalid result_data provided", result_data=result_data, type=type(result_data).__name__)
            raise ValueError("result_data must be a non-empty dictionary")
            
        if "status" not in result_data:
            logger.error("Missing required field in result_data", field="status", result_data=result_data)
            raise ValueError("result_data must contain a 'status' field")
        
        logger.debug("Attempting to add job result", job_id=job_id, result_ttl=result_ttl)
        try:
            # Use the helper method to get the configured KV store context
            async with self._get_kv_store_context() as kv:
                # Create a JobResult object for efficient serialization
                job_result = JobResult(
                    job_id=job_id,
                    status=result_data.get("status", ""),
                    result=result_data.get("result"),
                    error=result_data.get("error"),
                    traceback=result_data.get("traceback"),
                    start_time=result_data.get("start_time", 0.0),
                    finish_time=result_data.get("finish_time", 0.0),
                )
                
                # Serialize the JobResult object
                serialized_result = Job.serialize_result(
                    result=job_result.result,
                    status=result_data.get("status"),
                    error=job_result.error,
                    traceback_str=job_result.traceback,
                )

                # Set TTL (default to settings value if not provided)
                ttl = (
                    result_ttl if result_ttl is not None else DEFAULT_RESULT_TTL_SECONDS
                )
                
                # Validate TTL
                if ttl is not None and ttl < 0:
                    logger.warning("Invalid TTL provided, using default", provided_ttl=ttl, default_ttl=DEFAULT_RESULT_TTL_SECONDS)
                    ttl = DEFAULT_RESULT_TTL_SECONDS

                # Store the result with TTL
                await kv.put(job_id, serialized_result, ttl=ttl)
                logger.success("Job result added successfully", job_id=job_id)

        except ConnectionError as e:
            logger.error("Connection error while adding job result", job_id=job_id, error=str(e), exc_info=True)
            raise NaqException(f"Failed to connect to NATS server while storing result for job {job_id}: {e}") from e
        except ValueError as e:
            # Re-raise ValueError as is since it's a client error
            raise
        except Exception as e:
            logger.error("Failed to add job result", job_id=job_id, error=str(e), exc_info=True)
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
            ValueError: If job_id is empty or invalid.
        """
        # Validate input
        if not job_id or not isinstance(job_id, str):
            logger.error("Invalid job_id provided", job_id=job_id, type=type(job_id).__name__)
            raise ValueError("job_id must be a non-empty string")
            
        logger.debug("Attempting to fetch job result", job_id=job_id)
        try:
            # Use the helper method to get the configured KV store context
            async with self._get_kv_store_context() as kv:
                try:
                    entry = await kv.get(job_id)
                    result_data = Job.deserialize_result(entry.value)
                    
                    # Create a JobResult object from the deserialized data
                    job_result = JobResult(
                        job_id=job_id,
                        status=result_data.get("status", ""),
                        result=result_data.get("result"),
                        error=result_data.get("error"),
                        traceback=result_data.get("traceback"),
                        start_time=result_data.get("start_time", 0.0),
                        finish_time=result_data.get("finish_time", 0.0),
                    )
                    
                    # Convert JobResult back to dictionary for API compatibility
                    result_dict = {
                        "job_id": job_result.job_id,
                        "status": job_result.status,
                        "result": job_result.result,
                        "error": job_result.error,
                        "traceback": job_result.traceback,
                        "start_time": job_result.start_time,
                        "finish_time": job_result.finish_time,
                        "duration_ms": job_result.duration_ms,
                    }
                    
                    logger.success("Job result fetched successfully", job_id=job_id)
                    return result_dict
                except KeyNotFoundError:
                    logger.warning("Job result not found", job_id=job_id)
                    raise JobNotFoundError(
                        f"Result for job {job_id} not found. It may not have completed, "
                        f"failed, or the result expired."
                    ) from None
                except Exception as e:
                    logger.error("Error processing job result data", job_id=job_id, error=str(e), exc_info=True)
                    raise NaqException(f"Failed to process result data for job {job_id}: {e}") from e

        except ConnectionError as e:
            logger.error("Connection error while fetching job result", job_id=job_id, error=str(e), exc_info=True)
            raise NaqException(f"Failed to connect to NATS server while fetching result for job {job_id}: {e}") from e
        except ValueError as e:
            # Re-raise ValueError as is since it's a client error
            raise
        except JobNotFoundError as e:
            # Re-raise JobNotFoundError as is
            raise
        except Exception as e:
            logger.error("Failed to fetch job result", job_id=job_id, error=str(e), exc_info=True)
            raise NaqException(f"Failed to fetch result for job {job_id}: {e}") from e

    async def list_all_job_results(self) -> List[str]:
        """
        List all job IDs for which results are stored.

        Returns:
            A list of job IDs that have results stored.

        Raises:
            NaqException: If listing the results fails.
        """
        logger.debug("Attempting to list all job results")
        try:
            # Use the helper method to get the configured KV store context
            async with self._get_kv_store_context() as kv:
                # Get all keys in the KV store
                keys = await kv.keys()
                job_ids = list(keys)
                logger.success("Listed all job results successfully", count=len(job_ids))
                return job_ids

        except ConnectionError as e:
            logger.error("Connection error while listing job results", error=str(e), exc_info=True)
            raise NaqException(f"Failed to connect to NATS server while listing job results: {e}") from e
        except Exception as e:
            logger.error("Failed to list job results", error=str(e), exc_info=True)
            raise NaqException(f"Failed to list job results: {e}") from e

    async def purge_all_job_results(self) -> None:
        """
        Delete all job results from the KV store.

        Raises:
            NaqException: If purging the results fails.
        """
        logger.debug("Attempting to purge all job results")
        try:
            # Use the helper method to get the configured KV store context
            async with self._get_kv_store_context() as kv:
                # Get all keys and delete them
                keys = await kv.keys()
                deleted_count = 0
                for key in keys:
                    try:
                        await kv.delete(key)
                        deleted_count += 1
                    except Exception as delete_error:
                        logger.warning("Failed to delete individual job result during purge",
                                     key=key, error=str(delete_error))
                        # Continue with other keys even if one fails
                logger.success("Purged all job results successfully", count=deleted_count)

        except ConnectionError as e:
            logger.error("Connection error while purging job results", error=str(e), exc_info=True)
            raise NaqException(f"Failed to connect to NATS server while purging job results: {e}") from e
        except Exception as e:
            logger.error("Failed to purge job results", error=str(e), exc_info=True)
            raise NaqException(f"Failed to purge job results: {e}") from e

    async def delete_job_result(self, job_id: str) -> None:
        """
        Delete a specific job result from the KV store.

        Args:
            job_id: The ID of the job result to delete.

        Raises:
            NaqException: If deleting the result fails.
            ValueError: If job_id is empty or invalid.
        """
        # Validate input
        if not job_id or not isinstance(job_id, str):
            logger.error("Invalid job_id provided", job_id=job_id, type=type(job_id).__name__)
            raise ValueError("job_id must be a non-empty string")
            
        logger.debug("Attempting to delete job result", job_id=job_id)
        try:
            # Use the helper method to get the configured KV store context
            async with self._get_kv_store_context() as kv:
                try:
                    await kv.delete(job_id)
                    logger.success("Job result deleted successfully", job_id=job_id)
                except KeyNotFoundError:
                    # If the key doesn't exist, we don't need to raise an error
                    # as the end result is the same - the key doesn't exist
                    logger.info("Job result not found for deletion, already deleted", job_id=job_id)
                    pass

        except ConnectionError as e:
            logger.error("Connection error while deleting job result", job_id=job_id, error=str(e), exc_info=True)
            raise NaqException(f"Failed to connect to NATS server while deleting result for job {job_id}: {e}") from e
        except ValueError as e:
            # Re-raise ValueError as is since it's a client error
            raise
        except Exception as e:
            logger.error("Failed to delete job result", job_id=job_id, error=str(e), exc_info=True)
            raise NaqException(f"Failed to delete result for job {job_id}: {e}") from e
