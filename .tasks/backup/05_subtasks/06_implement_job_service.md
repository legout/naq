# Subtask 05.06: Implement JobService

## Overview
Implement the JobService to centralize job execution, result storage, and lifecycle management across the codebase.

## Objectives
- Centralize job execution and lifecycle management
- Integrate event logging for comprehensive tracking
- Provide result storage and retrieval capabilities
- Implement failure handling and retry logic
- Add performance metrics collection
- Integrate with ConnectionService, KVStoreService, and EventService

## Implementation Steps

### 1. Implement JobService Class
Create the JobService that inherits from BaseService:

```python
import asyncio
import logging
import time
from typing import Dict, List, Optional, Any, Union, AsyncIterator
from contextlib import asynccontextmanager

from .base import BaseService, ServiceError
from .connection import ConnectionService
from .kv_stores import KVStoreService
from .events import EventService
from naq.job import Job, JobResult, JobStatus
from naq.exceptions import JobExecutionError

class JobService(BaseService):
    """Job execution and lifecycle management service."""
    
    def __init__(self, config: Dict[str, Any], 
                 connection_service: ConnectionService,
                 kv_service: KVStoreService,
                 event_service: EventService):
        super().__init__(config)
        self.connection_service = connection_service
        self.kv_service = kv_service
        self.event_service = event_service
        self._logger = logging.getLogger(__name__)
        self._job_metrics: Dict[str, Dict[str, Any]] = {}
        self._max_job_history = config.get("max_job_history", 1000)
        
    async def _do_initialize(self) -> None:
        """Initialize job service."""
        self._logger.info("Initializing JobService")
        
    async def _do_cleanup(self) -> None:
        """Cleanup job service."""
        self._logger.info("Cleaning up JobService")
        self._job_metrics.clear()
        
    async def execute_job(self, job: Job) -> JobResult:
        """Execute job with full lifecycle management.
        
        Args:
            job: Job to execute
            
        Returns:
            JobResult with execution outcome
        """
        job_id = job.job_id
        start_time = time.time()
        
        try:
            # Log job started event
            await self.event_service.log_job_started(
                job_id=job_id,
                worker_id=self.config.get("worker_id", "unknown"),
                queue=job.queue
            )
            
            # Update job status to running
            await self._update_job_status(job_id, JobStatus.RUNNING)
            
            # Execute the job
            result = await self._execute_job_function(job)
            
            # Store result
            await self.store_result(job_id, result)
            
            # Log job completed event
            await self.event_service.log_event({
                'type': 'job_completed',
                'job_id': job_id,
                'queue': job.queue,
                'duration': time.time() - start_time,
                'success': True
            })
            
            # Update job status to completed
            await self._update_job_status(job_id, JobStatus.COMPLETED)
            
            # Record metrics
            self._record_job_metrics(job_id, start_time, True)
            
            return result
            
        except Exception as e:
            # Handle job failure
            await self.handle_job_failure(job, e)
            
            # Log job failed event
            await self.event_service.log_event({
                'type': 'job_failed',
                'job_id': job_id,
                'queue': job.queue,
                'duration': time.time() - start_time,
                'error': str(e)
            })
            
            # Update job status to failed
            await self._update_job_status(job_id, JobStatus.FAILED)
            
            # Record metrics
            self._record_job_metrics(job_id, start_time, False)
            
            raise JobExecutionError(f"Job {job_id} failed: {e}")
            
    async def _execute_job_function(self, job: Job) -> JobResult:
        """Execute the actual job function."""
        try:
            # Execute the job function
            if asyncio.iscoroutinefunction(job.func):
                result = await job.func(*job.args, **job.kwargs)
            else:
                result = job.func(*job.args, **job.kwargs)
                
            return JobResult(
                job_id=job.job_id,
                success=True,
                result=result,
                error=None,
                execution_time=time.time() - job.created_at
            )
            
        except Exception as e:
            return JobResult(
                job_id=job.job_id,
                success=False,
                result=None,
                error=str(e),
                execution_time=time.time() - job.created_at
            )
            
    async def store_result(self, job_id: str, result: JobResult) -> None:
        """Store job result with event logging.
        
        Args:
            job_id: Job ID
            result: Job result to store
        """
        try:
            # Store result in KV store
            await self.kv_service.put(
                bucket="job_results",
                key=job_id,
                value=result.dict(),
                ttl=self.config.get("result_ttl", 3600)  # 1 hour default
            )
            
            self._logger.debug(f"Stored result for job {job_id}")
            
        except Exception as e:
            raise ServiceError(f"Failed to store result for job {job_id}: {e}")
            
    async def get_result(self, job_id: str) -> Optional[JobResult]:
        """Get job result.
        
        Args:
            job_id: Job ID
            
        Returns:
            JobResult if found, None otherwise
        """
        try:
            result_data = await self.kv_service.get_json("job_results", job_id)
            if result_data:
                return JobResult(**result_data)
            return None
            
        except Exception as e:
            raise ServiceError(f"Failed to get result for job {job_id}: {e}")
            
    async def handle_job_failure(self, job: Job, error: Exception) -> None:
        """Handle job failure with retry logic.
        
        Args:
            job: Failed job
            error: Exception that caused failure
        """
        try:
            # Check if job should be retried
            if await self._should_retry_job(job, error):
                await self._schedule_retry(job, error)
            else:
                # Permanent failure - store error details
                await self.kv_service.put(
                    bucket="job_failures",
                    key=job.job_id,
                    value={
                        'job_id': job.job_id,
                        'error': str(error),
                        'timestamp': time.time(),
                        'attempts': job.attempts + 1,
                        'max_attempts': job.max_attempts
                    }
                )
                
        except Exception as e:
            self._logger.error(f"Error handling job failure for {job.job_id}: {e}")
            
    async def _should_retry_job(self, job: Job, error: Exception) -> bool:
        """Determine if job should be retried."""
        # Check if job has reached max attempts
        if job.attempts >= job.max_attempts:
            return False
            
        # Check error type for retry eligibility
        retryable_errors = self.config.get("retryable_errors", [
            "ConnectionError",
            "TimeoutError",
            "TemporaryError"
        ])
        
        error_type = type(error).__name__
        return error_type in retryable_errors
        
    async def _schedule_retry(self, job: Job, error: Exception) -> None:
        """Schedule job retry."""
        retry_delay = self._calculate_retry_delay(job.attempts)
        
        # Update job with retry information
        job.attempts += 1
        job.next_attempt_at = time.time() + retry_delay
        
        # Store retry information
        await self.kv_service.put(
            bucket="job_retries",
            key=job.job_id,
            value={
                'job_id': job.job_id,
                'attempts': job.attempts,
                'next_attempt_at': job.next_attempt_at,
                'error': str(error)
            }
        )
        
        self._logger.info(f"Scheduled retry for job {job.job_id} in {retry_delay}s")
        
    def _calculate_retry_delay(self, attempt: int) -> float:
        """Calculate retry delay with exponential backoff."""
        base_delay = self.config.get("base_retry_delay", 1.0)
        max_delay = self.config.get("max_retry_delay", 300.0)
        multiplier = self.config.get("retry_multiplier", 2.0)
        
        delay = base_delay * (multiplier ** attempt)
        return min(delay, max_delay)
        
    async def _update_job_status(self, job_id: str, status: JobStatus) -> None:
        """Update job status in KV store."""
        try:
            await self.kv_service.put(
                bucket="job_status",
                key=job_id,
                value={
                    'job_id': job_id,
                    'status': status.value,
                    'timestamp': time.time()
                }
            )
            
        except Exception as e:
            self._logger.error(f"Failed to update status for job {job_id}: {e}")
            
    def _record_job_metrics(self, job_id: str, start_time: float, success: bool) -> None:
        """Record job execution metrics."""
        execution_time = time.time() - start_time
        
        if job_id not in self._job_metrics:
            self._job_metrics[job_id] = {
                'executions': 0,
                'success_count': 0,
                'failure_count': 0,
                'total_time': 0,
                'avg_time': 0
            }
            
        metrics = self._job_metrics[job_id]
        metrics['executions'] += 1
        metrics['total_time'] += execution_time
        
        if success:
            metrics['success_count'] += 1
        else:
            metrics['failure_count'] += 1
            
        metrics['avg_time'] = metrics['total_time'] / metrics['executions']
        
        # Clean up old metrics if needed
        if len(self._job_metrics) > self._max_job_history:
            oldest_key = min(self._job_metrics.keys())
            del self._job_metrics[oldest_key]
            
    async def get_job_metrics(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job execution metrics.
        
        Args:
            job_id: Job ID
            
        Returns:
            Job metrics if available
        """
        return self._job_metrics.get(job_id)
        
    async def get_pending_retries(self) -> List[Dict[str, Any]]:
        """Get list of pending job retries.
        
        Returns:
            List of retry information
        """
        try:
            retry_keys = await self.kv_service.get_json("job_retries", "keys") or []
            now = time.time()
            
            pending_retries = []
            for key in retry_keys:
                retry_info = await self.kv_service.get_json("job_retries", key)
                if retry_info and retry_info.get('next_attempt_at', 0) <= now:
                    pending_retries.append(retry_info)
                    
            return pending_retries
            
        except Exception as e:
            self._logger.error(f"Failed to get pending retries: {e}")
            return []
            
    async def cleanup_old_results(self, max_age: int = 86400) -> None:
        """Clean up old job results and failures.
        
        Args:
            max_age: Maximum age in seconds to keep results
        """
        try:
            cutoff_time = time.time() - max_age
            
            # Clean up old results
            result_keys = await self.kv_service.get_json("job_results", "keys") or []
            for key in result_keys:
                try:
                    result_data = await self.kv_service.get_json("job_results", key)
                    if result_data and result_data.get('timestamp', 0) < cutoff_time:
                        await self.kv_service.delete("job_results", key)
                except Exception:
                    pass
                    
            # Clean up old failures
            failure_keys = await self.kv_service.get_json("job_failures", "keys") or []
            for key in failure_keys:
                try:
                    failure_data = await self.kv_service.get_json("job_failures", key)
                    if failure_data and failure_data.get('timestamp', 0) < cutoff_time:
                        await self.kv_service.delete("job_failures", key)
                except Exception:
                    pass
                    
            self._logger.info(f"Cleaned up job results older than {max_age}s")
            
        except Exception as e:
            self._logger.error(f"Failed to cleanup old results: {e}")
```

### 2. Update Service Registration
Register the JobService in the service system:

```python
# In __init__.py or base.py
from .jobs import JobService

# Register the service
ServiceManager.register_service("jobs", JobService)
```

### 3. Add Job Queue Integration
Implement job queue integration:

```python
async def submit_job(self, job: Job, queue: str = "default") -> str:
    """Submit job to queue.
    
    Args:
        job: Job to submit
        queue: Queue name
        
    Returns:
        Job ID
    """
    try:
        # Store job in KV store
        await self.kv_service.put(
            bucket="job_queue",
            key=job.job_id,
            value=job.dict(),
            ttl=self.config.get("job_ttl", 3600)
        )
        
        # Log job submitted event
        await self.event_service.log_event({
            'type': 'job_submitted',
            'job_id': job.job_id,
            'queue': queue,
            'function': job.func.__name__ if hasattr(job.func, '__name__') else str(job.func)
        })
        
        self._logger.info(f"Submitted job {job.job_id} to queue {queue}")
        return job.job_id
        
    except Exception as e:
        raise ServiceError(f"Failed to submit job {job.job_id}: {e}")
```

### 4. Add Job Monitoring
Implement job monitoring capabilities:

```python
async def monitor_jobs(self, interval: float = 60.0) -> AsyncIterator[Dict[str, Any]]:
    """Monitor job execution periodically.
    
    Args:
        interval: Monitoring interval in seconds
        
    Yields:
        Job monitoring data
    """
    while True:
        try:
            # Get job queue status
            queue_keys = await self.kv_service.get_json("job_queue", "keys") or []
            
            # Get job status
            status_keys = await self.kv_service.get_json("job_status", "keys") or []
            
            # Get job results
            result_keys = await self.kv_service.get_json("job_results", "keys") or []
            
            monitoring_data = {
                'timestamp': time.time(),
                'queue_size': len(queue_keys),
                'active_jobs': len(status_keys),
                'completed_jobs': len(result_keys),
                'pending_retries': len(await self.get_pending_retries()),
                'metrics': self._job_metrics
            }
            
            yield monitoring_data
            await asyncio.sleep(interval)
            
        except Exception as e:
            self._logger.error(f"Error in job monitoring: {e}")
            await asyncio.sleep(interval)
```

### 5. Update Usage Examples
Create usage examples showing the new pattern:

```python
# Old pattern (to be replaced)
async def old_job_pattern():
    nc = await get_nats_connection(nats_url)
    try:
        js = await get_jetstream_context(nc)
        kv = await js.key_value("job_results")
        
        # Execute job
        result = await job_func(*args, **kwargs)
        
        # Store result
        await kv.put(job_id, result.encode())
        
        # Log event
        await js.publish("job_events", json.dumps({
            'type': 'job_completed',
            'job_id': job_id
        }).encode())
        
    finally:
        await close_nats_connection(nc)

# New pattern
async def new_job_pattern():
    async with ServiceManager(config) as services:
        job_service = await services.get_service(JobService)
        event_service = await services.get_service(EventService)
        
        # Create job
        job = Job(
            job_id="unique_job_id",
            func=job_function,
            args=[arg1, arg2],
            kwargs={'param1': 'value1'},
            max_attempts=3
        )
        
        # Execute job with full lifecycle management
        result = await job_service.execute_job(job)
        
        # Events are automatically logged by JobService
        # Result is automatically stored
        # Metrics are automatically collected
        
        # Get job metrics
        metrics = await job_service.get_job_metrics(job.job_id)
        
        # Monitor jobs
        async for monitoring_data in job_service.monitor_jobs():
            print(f"Queue size: {monitoring_data['queue_size']}")
```

## Success Criteria
- [ ] JobService implemented with complete job lifecycle management
- [ ] Integrated event logging working correctly
- [ ] Result storage and retrieval functional
- [ ] Failure handling and retry logic implemented
- [ ] Performance metrics collection working
- [ ] Integration with ConnectionService, KVStoreService, and EventService successful
- [ ] Job queue integration functional
- [ ] All existing job functionality preserved

## Dependencies
- **Depends on**: Subtasks 05.03-05.05 (Core services)
- **Prepares for**: Subtask 05.07 (Implement EventService)

## Estimated Time
- **Implementation**: 4-5 hours
- **Testing**: 2 hours
- **Total**: 6-7 hours