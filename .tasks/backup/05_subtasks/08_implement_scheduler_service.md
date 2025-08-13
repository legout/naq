# Subtask 05.08: Implement SchedulerService

## Overview
Implement the SchedulerService to centralize scheduler operations and scheduled job management across the codebase.

## Objectives
- Centralize all scheduler operations through SchedulerService
- Implement scheduled job lifecycle management
- Add due job detection and triggering capabilities
- Support schedule modification operations
- Include leader election support for distributed scheduling
- Integrate with ConnectionService, KVStoreService, and EventService
- Ensure consistent scheduling behavior across the system

## Implementation Steps

### 1. Implement SchedulerService Class
Create the SchedulerService that inherits from BaseService:

```python
import asyncio
import json
import logging
import time
import uuid
from typing import Dict, List, Optional, Any, AsyncIterator
from datetime import datetime, timedelta
from contextlib import asynccontextmanager

from .base import BaseService, ServiceError
from .connection import ConnectionService
from .kv_stores import KVStoreService
from .events import EventService
from naq.job import Job, JobStatus
from naq.exceptions import SchedulerError

class SchedulerService(BaseService):
    """Scheduler operations service."""
    
    def __init__(self, config: Dict[str, Any],
                 connection_service: ConnectionService, 
                 kv_service: KVStoreService,
                 event_service: EventService):
        super().__init__(config)
        self.connection_service = connection_service
        self.kv_service = kv_service
        self.event_service = event_service
        self._logger = logging.getLogger(__name__)
        self._scheduled_jobs: Dict[str, Dict[str, Any]] = {}
        self._leader_lock = asyncio.Lock()
        self._is_leader = False
        self._leader_check_interval = config.get("leader_check_interval", 30.0)
        self._leader_heartbeat_interval = config.get("leader_heartbeat_interval", 10.0)
        self._leader_task: Optional[asyncio.Task] = None
        self._scheduler_task: Optional[asyncio.Task] = None
        
    async def _do_initialize(self) -> None:
        """Initialize scheduler service."""
        self._logger.info("Initializing SchedulerService")
        
        # Ensure scheduler streams exist
        await self._ensure_scheduler_streams()
        
        # Start leader election
        await self._start_leader_election()
        
        # Start scheduler loop
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())
        
    async def _do_cleanup(self) -> None:
        """Cleanup scheduler service."""
        self._logger.info("Cleaning up SchedulerService")
        
        # Stop scheduler loop
        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass
                
        # Stop leader election
        if self._leader_task:
            self._leader_task.cancel()
            try:
                await self._leader_task
            except asyncio.CancelledError:
                pass
                
        # Cleanup scheduled jobs
        self._scheduled_jobs.clear()
        
    async def _ensure_scheduler_streams(self) -> None:
        """Ensure scheduler-related streams exist."""
        # Scheduled jobs stream
        await self.connection_service.stream_service.ensure_stream(
            name="scheduled_jobs",
            subjects=["scheduled_jobs.>"],
            retention="limits",
            max_msgs=1000000
        )
        
        # Scheduler events stream
        await self.connection_service.stream_service.ensure_stream(
            name="scheduler_events",
            subjects=["scheduler_events.>"],
            retention="limits",
            max_msgs=100000
        )
        
    async def _start_leader_election(self) -> None:
        """Start leader election process."""
        self._leader_task = asyncio.create_task(self._leader_election_loop())
        
    async def _leader_election_loop(self) -> None:
        """Leader election background loop."""
        while True:
            try:
                await asyncio.sleep(self._leader_check_interval)
                
                # Check if we should be leader
                should_be_leader = await self._should_be_leader()
                
                if should_be_leader and not self._is_leader:
                    await self._become_leader()
                elif not should_be_leader and self._is_leader:
                    await self._relinquish_leadership()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._logger.error(f"Error in leader election loop: {e}")
                
    async def _should_be_leader(self) -> bool:
        """Determine if this instance should be leader."""
        try:
            # Get current leader from KV store
            leader_info = await self.kv_service.get_json("scheduler_leader", "current")
            
            if not leader_info:
                return True
                
            # Check if current leader is still alive
            last_heartbeat = leader_info.get('last_heartbeat', 0)
            heartbeat_timeout = self._leader_heartbeat_interval * 2
            
            if time.time() - last_heartbeat > heartbeat_timeout:
                return True
                
            # Check if we have higher priority
            our_priority = self.config.get("priority", 1)
            leader_priority = leader_info.get('priority', 1)
            
            return our_priority > leader_priority
            
        except Exception as e:
            self._logger.error(f"Error checking leadership: {e}")
            return False
            
    async def _become_leader(self) -> None:
        """Become the scheduler leader."""
        async with self._leader_lock:
            if self._is_leader:
                return
                
            try:
                # Register as leader
                leader_info = {
                    'instance_id': self.config.get("instance_id", str(uuid.uuid4())),
                    'priority': self.config.get("priority", 1),
                    'last_heartbeat': time.time(),
                    'became_leader_at': time.time()
                }
                
                await self.kv_service.put(
                    bucket="scheduler_leader",
                    key="current",
                    value=leader_info
                )
                
                self._is_leader = True
                self._logger.info("Became scheduler leader")
                
                # Log leadership event
                await self.event_service.log_event({
                    'type': 'scheduler_became_leader',
                    'instance_id': leader_info['instance_id'],
                    'timestamp': datetime.utcnow().isoformat()
                })
                
                # Start heartbeat task
                asyncio.create_task(self._leader_heartbeat_loop())
                
            except Exception as e:
                self._logger.error(f"Error becoming leader: {e}")
                
    async def _relinquish_leadership(self) -> None:
        """Relinquish scheduler leadership."""
        async with self._leader_lock:
            if not self._is_leader:
                return
                
            try:
                # Remove leader registration
                await self.kv_service.delete("scheduler_leader", "current")
                
                self._is_leader = False
                self._logger.info("Relinquished scheduler leadership")
                
                # Log leadership event
                await self.event_service.log_event({
                    'type': 'scheduler_relinquished_leadership',
                    'instance_id': self.config.get("instance_id", "unknown"),
                    'timestamp': datetime.utcnow().isoformat()
                })
                
            except Exception as e:
                self._logger.error(f"Error relinquishing leadership: {e}")
                
    async def _leader_heartbeat_loop(self) -> None:
        """Leader heartbeat background loop."""
        while self._is_leader:
            try:
                await asyncio.sleep(self._leader_heartbeat_interval)
                
                # Update heartbeat
                leader_info = {
                    'instance_id': self.config.get("instance_id", str(uuid.uuid4())),
                    'priority': self.config.get("priority", 1),
                    'last_heartbeat': time.time(),
                    'became_leader_at': time.time()
                }
                
                await self.kv_service.put(
                    bucket="scheduler_leader",
                    key="current",
                    value=leader_info
                )
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._logger.error(f"Error in leader heartbeat: {e}")
                
    async def schedule_job(self, job: Job, schedule: Dict[str, Any]) -> str:
        """Schedule job for future execution.
        
        Args:
            job: Job to schedule
            schedule: Schedule configuration
            
        Returns:
            Schedule ID
        """
        try:
            schedule_id = str(uuid.uuid4())
            
            # Create schedule entry
            schedule_entry = {
                'schedule_id': schedule_id,
                'job': job.dict(),
                'schedule': schedule,
                'created_at': time.time(),
                'next_run_at': self._calculate_next_run(schedule),
                'status': 'active',
                'runs': 0,
                'max_runs': schedule.get('max_runs', -1),  # -1 for unlimited
                'last_run_at': None,
                'error_count': 0
            }
            
            # Store schedule in KV store
            await self.kv_service.put(
                bucket="scheduler_schedules",
                key=schedule_id,
                value=schedule_entry
            )
            
            # Add to in-memory cache
            self._scheduled_jobs[schedule_id] = schedule_entry
            
            # Log scheduling event
            await self.event_service.log_event({
                'type': 'job_scheduled',
                'schedule_id': schedule_id,
                'job_id': job.job_id,
                'schedule': schedule,
                'timestamp': datetime.utcnow().isoformat()
            })
            
            self._logger.info(f"Scheduled job {job.job_id} with schedule {schedule_id}")
            return schedule_id
            
        except Exception as e:
            raise ServiceError(f"Failed to schedule job {job.job_id}: {e}")
            
    def _calculate_next_run(self, schedule: Dict[str, Any]) -> float:
        """Calculate next run time based on schedule."""
        now = time.time()
        
        schedule_type = schedule.get('type', 'once')
        
        if schedule_type == 'once':
            # One-time schedule
            run_at = schedule.get('run_at')
            if isinstance(run_at, str):
                # Parse ISO format datetime
                run_at = datetime.fromisoformat(run_at).timestamp()
            return float(run_at)
            
        elif schedule_type == 'interval':
            # Interval schedule
            interval = schedule.get('interval', 3600)  # 1 hour default
            return now + interval
            
        elif schedule_type == 'cron':
            # Cron-like schedule (simplified)
            cron_expr = schedule.get('cron', '0 * * * *')  # Every hour default
            return self._parse_cron_next_run(cron_expr, now)
            
        else:
            raise ValueError(f"Unsupported schedule type: {schedule_type}")
            
    def _parse_cron_next_run(self, cron_expr: str, now: float) -> float:
        """Parse cron expression and calculate next run time (simplified)."""
        # This is a simplified cron parser
        # In production, use a proper cron library like croniter
        
        parts = cron_expr.split()
        if len(parts) != 5:
            raise ValueError(f"Invalid cron expression: {cron_expr}")
            
        # For now, just return next hour
        return now + 3600
        
    async def trigger_due_jobs(self) -> List[str]:
        """Find and trigger jobs that are due.
        
        Returns:
            List of triggered job IDs
        """
        if not self._is_leader:
            return []
            
        try:
            now = time.time()
            triggered_jobs = []
            
            # Check all scheduled jobs
            schedule_keys = await self.kv_service.get_json("scheduler_schedules", "keys") or []
            
            for schedule_id in schedule_keys:
                try:
                    schedule_entry = await self.kv_service.get_json("scheduler_schedules", schedule_id)
                    if not schedule_entry:
                        continue
                        
                    # Check if schedule is active and due
                    if (schedule_entry.get('status') == 'active' and 
                        schedule_entry.get('next_run_at', 0) <= now):
                        
                        # Check max runs limit
                        max_runs = schedule_entry.get('max_runs', -1)
                        if max_runs > 0 and schedule_entry.get('runs', 0) >= max_runs:
                            await self._deactivate_schedule(schedule_id)
                            continue
                            
                        # Trigger the job
                        job_id = await self._trigger_scheduled_job(schedule_entry)
                        triggered_jobs.append(job_id)
                        
                        # Update schedule
                        await self._update_schedule_after_run(schedule_id)
                        
                except Exception as e:
                    self._logger.error(f"Error processing schedule {schedule_id}: {e}")
                    
            return triggered_jobs
            
        except Exception as e:
            self._logger.error(f"Error triggering due jobs: {e}")
            return []
            
    async def _trigger_scheduled_job(self, schedule_entry: Dict[str, Any]) -> str:
        """Trigger a scheduled job."""
        try:
            job_data = schedule_entry['job']
            job = Job(**job_data)
            
            # Store job in queue for execution
            await self.kv_service.put(
                bucket="job_queue",
                key=job.job_id,
                value=job.dict(),
                ttl=self.config.get("job_ttl", 3600)
            )
            
            # Log trigger event
            await self.event_service.log_event({
                'type': 'scheduled_job_triggered',
                'schedule_id': schedule_entry['schedule_id'],
                'job_id': job.job_id,
                'timestamp': datetime.utcnow().isoformat()
            })
            
            self._logger.info(f"Triggered scheduled job {job.job_id}")
            return job.job_id
            
        except Exception as e:
            self._logger.error(f"Error triggering scheduled job: {e}")
            raise
            
    async def _update_schedule_after_run(self, schedule_id: str) -> None:
        """Update schedule after job execution."""
        try:
            schedule_entry = await self.kv_service.get_json("scheduler_schedules", schedule_id)
            if not schedule_entry:
                return
                
            # Update run statistics
            schedule_entry['runs'] = schedule_entry.get('runs', 0) + 1
            schedule_entry['last_run_at'] = time.time()
            
            # Calculate next run time
            if schedule_entry.get('max_runs', -1) > 0 and schedule_entry['runs'] >= schedule_entry['max_runs']:
                schedule_entry['status'] = 'completed'
            else:
                schedule_entry['next_run_at'] = self._calculate_next_run(schedule_entry['schedule'])
                
            # Store updated schedule
            await self.kv_service.put(
                bucket="scheduler_schedules",
                key=schedule_id,
                value=schedule_entry
            )
            
            # Update in-memory cache
            if schedule_id in self._scheduled_jobs:
                self._scheduled_jobs[schedule_id] = schedule_entry
                
        except Exception as e:
            self._logger.error(f"Error updating schedule {schedule_id}: {e}")
            
    async def cancel_scheduled_job(self, schedule_id: str) -> bool:
        """Cancel scheduled job.
        
        Args:
            schedule_id: Schedule ID to cancel
            
        Returns:
            True if cancelled, False if not found
        """
        try:
            # Get schedule entry
            schedule_entry = await self.kv_service.get_json("scheduler_schedules", schedule_id)
            if not schedule_entry:
                return False
                
            # Mark as cancelled
            schedule_entry['status'] = 'cancelled'
            schedule_entry['cancelled_at'] = time.time()
            
            # Store updated schedule
            await self.kv_service.put(
                bucket="scheduler_schedules",
                key=schedule_id,
                value=schedule_entry
            )
            
            # Update in-memory cache
            if schedule_id in self._scheduled_jobs:
                self._scheduled_jobs[schedule_id] = schedule_entry
                
            # Log cancellation event
            await self.event_service.log_event({
                'type': 'schedule_cancelled',
                'schedule_id': schedule_id,
                'timestamp': datetime.utcnow().isoformat()
            })
            
            self._logger.info(f"Cancelled scheduled job with schedule {schedule_id}")
            return True
            
        except Exception as e:
            self._logger.error(f"Error cancelling schedule {schedule_id}: {e}")
            return False
            
    async def pause_scheduled_job(self, schedule_id: str) -> bool:
        """Pause scheduled job.
        
        Args:
            schedule_id: Schedule ID to pause
            
        Returns:
            True if paused, False if not found
        """
        try:
            # Get schedule entry
            schedule_entry = await self.kv_service.get_json("scheduler_schedules", schedule_id)
            if not schedule_entry:
                return False
                
            # Mark as paused
            schedule_entry['status'] = 'paused'
            schedule_entry['paused_at'] = time.time()
            
            # Store updated schedule
            await self.kv_service.put(
                bucket="scheduler_schedules",
                key=schedule_id,
                value=schedule_entry
            )
            
            # Update in-memory cache
            if schedule_id in self._scheduled_jobs:
                self._scheduled_jobs[schedule_id] = schedule_entry
                
            # Log pause event
            await self.event_service.log_event({
                'type': 'schedule_paused',
                'schedule_id': schedule_id,
                'timestamp': datetime.utcnow().isoformat()
            })
            
            self._logger.info(f"Paused scheduled job with schedule {schedule_id}")
            return True
            
        except Exception as e:
            self._logger.error(f"Error pausing schedule {schedule_id}: {e}")
            return False
            
    async def resume_scheduled_job(self, schedule_id: str) -> bool:
        """Resume paused scheduled job.
        
        Args:
            schedule_id: Schedule ID to resume
            
        Returns:
            True if resumed, False if not found
        """
        try:
            # Get schedule entry
            schedule_entry = await self.kv_service.get_json("scheduler_schedules", schedule_id)
            if not schedule_entry:
                return False
                
            # Check if it's paused
            if schedule_entry.get('status') != 'paused':
                return False
                
            # Mark as active and recalculate next run
            schedule_entry['status'] = 'active'
            schedule_entry['resumed_at'] = time.time()
            schedule_entry['next_run_at'] = self._calculate_next_run(schedule_entry['schedule'])
            
            # Store updated schedule
            await self.kv_service.put(
                bucket="scheduler_schedules",
                key=schedule_id,
                value=schedule_entry
            )
            
            # Update in-memory cache
            if schedule_id in self._scheduled_jobs:
                self._scheduled_jobs[schedule_id] = schedule_entry
                
            # Log resume event
            await self.event_service.log_event({
                'type': 'schedule_resumed',
                'schedule_id': schedule_id,
                'timestamp': datetime.utcnow().isoformat()
            })
            
            self._logger.info(f"Resumed scheduled job with schedule {schedule_id}")
            return True
            
        except Exception as e:
            self._logger.error(f"Error resuming schedule {schedule_id}: {e}")
            return False
            
    async def _deactivate_schedule(self, schedule_id: str) -> None:
        """Deactivate a schedule."""
        try:
            schedule_entry = await self.kv_service.get_json("scheduler_schedules", schedule_id)
            if not schedule_entry:
                return
                
            schedule_entry['status'] = 'completed'
            schedule_entry['completed_at'] = time.time()
            
            await self.kv_service.put(
                bucket="scheduler_schedules",
                key=schedule_id,
                value=schedule_entry
            )
            
            if schedule_id in self._scheduled_jobs:
                del self._scheduled_jobs[schedule_id]
                
        except Exception as e:
            self._logger.error(f"Error deactivating schedule {schedule_id}: {e}")
            
    async def _scheduler_loop(self) -> None:
        """Main scheduler loop."""
        while True:
            try:
                await asyncio.sleep(60.0)  # Check every minute
                
                # Trigger due jobs
                triggered_jobs = await self.trigger_due_jobs()
                
                if triggered_jobs:
                    self._logger.info(f"Triggered {len(triggered_jobs)} due jobs")
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._logger.error(f"Error in scheduler loop: {e}")
```

### 2. Update Service Registration
Register the SchedulerService in the service system:

```python
# In __init__.py or base.py
from .scheduler import SchedulerService

# Register the service
ServiceManager.register_service("scheduler", SchedulerService)
```

### 3. Add Schedule Management
Enhance schedule management capabilities:

```python
async def list_schedules(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
    """List all schedules.
    
    Args:
        status: Filter by status (active, paused, completed, cancelled)
        
    Returns:
        List of schedule entries
    """
    try:
        schedule_keys = await self.kv_service.get_json("scheduler_schedules", "keys") or []
        schedules = []
        
        for schedule_id in schedule_keys:
            schedule_entry = await self.kv_service.get_json("scheduler_schedules", schedule_id)
            if not schedule_entry:
                continue
                
            if status and schedule_entry.get('status') != status:
                continue
                
            schedules.append(schedule_entry)
            
        return sorted(schedules, key=lambda x: x.get('created_at', 0), reverse=True)
        
    except Exception as e:
        raise ServiceError(f"Failed to list schedules: {e}")
        
async def get_schedule(self, schedule_id: str) -> Optional[Dict[str, Any]]:
    """Get schedule by ID.
    
    Args:
        schedule_id: Schedule ID
        
    Returns:
        Schedule entry if found
    """
    try:
        return await self.kv_service.get_json("scheduler_schedules", schedule_id)
    except Exception as e:
        raise ServiceError(f"Failed to get schedule {schedule_id}: {e}")
```

### 4. Add Schedule Monitoring
Implement schedule monitoring:

```python
async def monitor_schedules(self, interval: float = 60.0) -> AsyncIterator[Dict[str, Any]]:
    """Monitor scheduler status.
    
    Args:
        interval: Monitoring interval in seconds
        
    Yields:
        Scheduler monitoring data
    """
    while True:
        try:
            # Get leader info
            leader_info = await self.kv_service.get_json("scheduler_leader", "current")
            
            # Get schedule statistics
            schedule_keys = await self.kv_service.get_json("scheduler_schedules", "keys") or []
            
            stats = {
                'timestamp': time.time(),
                'is_leader': self._is_leader,
                'leader_info': leader_info,
                'total_schedules': len(schedule_keys),
                'active_schedules': 0,
                'paused_schedules': 0,
                'completed_schedules': 0,
                'cancelled_schedules': 0
            }
            
            # Count schedules by status
            for schedule_id in schedule_keys:
                try:
                    schedule_entry = await self.kv_service.get_json("scheduler_schedules", schedule_id)
                    if schedule_entry:
                        status = schedule_entry.get('status', 'unknown')
                        stats[f'{status}_schedules'] = stats.get(f'{status}_schedules', 0) + 1
                except Exception:
                    pass
                    
            yield stats
            await asyncio.sleep(interval)
            
        except Exception as e:
            self._logger.error(f"Error in schedule monitoring: {e}")
            await asyncio.sleep(interval)
```

### 5. Update Usage Examples
Create usage examples showing the new pattern:

```python
# Old pattern (to be replaced)
async def old_scheduler_pattern():
    nc = await get_nats_connection(nats_url)
    try:
        js = await get_jetstream_context(nc)
        kv = await js.key_value("scheduler")
        
        # Schedule job
        await kv.put("schedule_1", json.dumps({
            'job': job_dict,
            'schedule': {'type': 'interval', 'interval': 3600},
            'next_run': time.time() + 3600
        }).encode())
        
        # Check due jobs
        schedules = await kv.get("schedules")
        for schedule in schedules:
            if schedule['next_run'] <= time.time():
                # Trigger job
                await js.publish("job_queue", json.dumps(schedule['job']).encode())
                
    finally:
        await close_nats_connection(nc)

# New pattern
async def new_scheduler_pattern():
    async with ServiceManager(config) as services:
        scheduler_service = await services.get_service(SchedulerService)
        
        # Schedule job
        job = Job(
            job_id="scheduled_job_1",
            func=job_function,
            args=[arg1, arg2]
        )
        
        schedule_id = await scheduler_service.schedule_job(
            job=job,
            schedule={
                'type': 'interval',
                'interval': 3600,  # Every hour
                'max_runs': 10     # Run 10 times max
            }
        )
        
        # Trigger due jobs (only if leader)
        if scheduler_service._is_leader:
            triggered_jobs = await scheduler_service.trigger_due_jobs()
            print(f"Triggered {len(triggered_jobs)} jobs")
            
        # Manage schedules
        await scheduler_service.pause_scheduled_job(schedule_id)
        await scheduler_service.resume_scheduled_job(schedule_id)
        await scheduler_service.cancel_scheduled_job(schedule_id)
        
        # Monitor scheduler
        async for monitoring_data in scheduler_service.monitor_schedules():
            print(f"Active schedules: {monitoring_data['active_schedules']}")
            print(f"Is leader: {monitoring_data['is_leader']}")
```

## Success Criteria
- [ ] SchedulerService implemented with complete scheduling functionality
- [ ] Scheduled job lifecycle management working
- [ ] Due job detection and triggering functional
- [ ] Schedule modification operations implemented
- [ ] Leader election support working
- [ ] Integration with ConnectionService, KVStoreService, and EventService successful
- [ ] Scheduler monitoring capabilities added
- [ ] All existing scheduler functionality preserved

## Dependencies
- **Depends on**: Subtasks 05.03-05.07 (All previous services)
- **Prepares for**: Service integration in subsequent tasks

## Estimated Time
- **Implementation**: 4-5 hours
- **Testing**: 2 hours
- **Total**: 6-7 hours