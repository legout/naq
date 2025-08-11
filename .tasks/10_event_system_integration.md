# Task 10: Complete Event System Integration

## Overview
Complete the integration of NAQ's event-driven state management system throughout the codebase, ensuring all job lifecycle events, worker events, and scheduler events are properly logged and can be monitored in real-time.

## Current State
- Event models and infrastructure exist (JobEvent, WorkerEvent, EventService)
- Event logging and processing components implemented  
- Service layer available for consistent event handling
- CLI commands for event monitoring partially implemented
- Event logging not fully integrated across all components

## Goals
- Complete event logging integration across all NAQ components
- Implement comprehensive CLI commands for event monitoring
- Ensure event system performance doesn't impact job processing
- Provide real-time monitoring and analytics capabilities
- Enable custom event processing and integration

## Event Integration Architecture

### Event Flow
```
Job/Worker/Scheduler Operations
    ↓
EventService (via ServiceManager)
    ↓  
AsyncJobEventLogger (batched, non-blocking)
    ↓
NATSJobEventStorage (JetStream)
    ↓
Event Consumers (CLI, Custom Processors)
```

### Event Categories to Integrate

1. **Job Lifecycle Events**
   - Job enqueued, started, completed, failed
   - Job retry scheduled, cancelled, paused
   - Job dependency resolution

2. **Worker Lifecycle Events** 
   - Worker started, stopped, idle, busy
   - Worker heartbeats, errors, status changes
   - Worker concurrency and performance metrics

3. **Scheduler Events**
   - Scheduled job created, triggered, modified
   - Schedule paused, resumed, cancelled
   - Scheduler leader election events

4. **System Events**
   - Configuration changes, system startup/shutdown
   - Performance metrics, resource usage
   - Error conditions and recovery

## Detailed Integration Plan

### 1. Complete Job Event Integration

**Integration Points:**
- Queue operations (enqueue, schedule, cancel)
- Worker job processing (start, complete, fail)
- Job retry and dependency resolution
- Result storage and retrieval

**Implementation:**
```python
# In JobService
class JobService(BaseService):
    def __init__(self, config: NAQConfig, event_service: EventService):
        super().__init__(config)
        self.event_service = event_service
    
    async def enqueue_job(self, job: Job, queue_name: str) -> str:
        """Enqueue job with event logging."""
        # Store job
        job_id = await self._store_job(job, queue_name)
        
        # Log enqueue event
        await self.event_service.log_job_enqueued(
            job_id=job_id,
            queue_name=queue_name,
            function_name=job.function.__name__,
            args_count=len(job.args),
            kwargs_count=len(job.kwargs)
        )
        
        return job_id
    
    async def execute_job(self, job: Job, worker_id: str) -> JobResult:
        """Execute job with complete event logging."""
        start_time = time.time()
        
        # Log job started
        await self.event_service.log_job_started(
            job_id=job.job_id,
            worker_id=worker_id,
            queue_name=job.queue_name
        )
        
        try:
            # Execute job
            result = await job.execute()
            
            # Calculate duration
            duration_ms = int((time.time() - start_time) * 1000)
            
            # Log job completed
            await self.event_service.log_job_completed(
                job_id=job.job_id,
                worker_id=worker_id,
                queue_name=job.queue_name,
                duration_ms=duration_ms,
                result_size=len(str(result)) if result else 0
            )
            
            return JobResult(
                job_id=job.job_id,
                status=JOB_STATUS.COMPLETED.value,
                result=result,
                start_time=start_time,
                finish_time=time.time()
            )
            
        except Exception as e:
            # Calculate duration  
            duration_ms = int((time.time() - start_time) * 1000)
            
            # Log job failed
            await self.event_service.log_job_failed(
                job_id=job.job_id,
                worker_id=worker_id,
                queue_name=job.queue_name,
                error_type=type(e).__name__,
                error_message=str(e),
                duration_ms=duration_ms
            )
            
            # Handle retries
            if job.should_retry(e):
                retry_delay = job.get_next_retry_delay()
                job.increment_retry_count()
                
                # Log retry scheduled
                await self.event_service.log_job_retry_scheduled(
                    job_id=job.job_id,
                    worker_id=worker_id,
                    queue_name=job.queue_name,
                    retry_count=job.retry_count,
                    retry_delay=retry_delay
                )
                
                # Schedule retry
                await self._schedule_retry(job, retry_delay)
            
            return JobResult(
                job_id=job.job_id,
                status=JOB_STATUS.FAILED.value,
                error=str(e),
                traceback=traceback.format_exc(),
                start_time=start_time,
                finish_time=time.time()
            )
```

### 2. Complete Worker Event Integration

**Integration Points:**
- Worker startup and shutdown
- Worker heartbeat and status updates
- Job processing state changes
- Worker error conditions

**Implementation:**
```python
# In Worker class
class Worker:
    async def start(self) -> None:
        """Start worker with event logging."""
        # Log worker started event
        await self.event_service.log_worker_started(
            worker_id=self.worker_id,
            hostname=socket.gethostname(),
            pid=os.getpid(),
            queue_names=self.queue_names,
            concurrency_limit=self.concurrency
        )
        
        # Start heartbeat task
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        
        # Start job processing
        await self._run_worker()
    
    async def _heartbeat_loop(self) -> None:
        """Worker heartbeat with event logging."""
        while self._running:
            try:
                # Log heartbeat event
                await self.event_service.log_worker_heartbeat(
                    worker_id=self.worker_id,
                    hostname=socket.gethostname(),
                    pid=os.getpid(),
                    active_jobs=self._active_jobs_count,
                    concurrency_limit=self.concurrency,
                    current_job_id=self._current_job_id
                )
                
                await asyncio.sleep(self.heartbeat_interval)
                
            except Exception as e:
                # Log worker error
                await self.event_service.log_worker_error(
                    worker_id=self.worker_id,
                    hostname=socket.gethostname(),
                    pid=os.getpid(),
                    error_type=type(e).__name__,
                    error_message=str(e)
                )
                
                await asyncio.sleep(5)  # Brief pause before retry
    
    async def _process_message(self, msg) -> None:
        """Process job message with status events."""
        job = Job.deserialize(msg.data)
        
        # Update worker status to busy
        self._active_jobs_count += 1
        self._current_job_id = job.job_id
        
        # Log worker busy event
        await self.event_service.log_worker_busy(
            worker_id=self.worker_id,
            hostname=socket.gethostname(),
            pid=os.getpid(),
            current_job_id=job.job_id
        )
        
        try:
            # Process job (JobService handles job events)
            job_service = await self.services.get_service(JobService)
            result = await job_service.execute_job(job, self.worker_id)
            
        finally:
            # Update worker status to idle  
            self._active_jobs_count -= 1
            self._current_job_id = None
            
            # Log worker idle event (if no other jobs)
            if self._active_jobs_count == 0:
                await self.event_service.log_worker_idle(
                    worker_id=self.worker_id,
                    hostname=socket.gethostname(),
                    pid=os.getpid()
                )
    
    async def stop(self) -> None:
        """Stop worker with event logging."""
        self._running = False
        
        # Cancel heartbeat task
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        
        # Wait for active jobs to complete
        while self._active_jobs_count > 0:
            await asyncio.sleep(0.1)
        
        # Log worker stopped event
        await self.event_service.log_worker_stopped(
            worker_id=self.worker_id,
            hostname=socket.gethostname(),
            pid=os.getpid()
        )
```

### 3. Complete Scheduler Event Integration

**Integration Points:**
- Scheduled job creation and management
- Job trigger events when schedules fire
- Schedule management operations (pause, resume, cancel)
- Leader election events

**Implementation:**
```python
# In SchedulerService
class SchedulerService(BaseService):
    async def schedule_job(
        self, 
        job: Job, 
        schedule_config: Dict[str, Any]
    ) -> str:
        """Schedule job with event logging."""
        schedule = Schedule(
            job_id=job.job_id,
            scheduled_timestamp_utc=schedule_config['scheduled_time'],
            status=SCHEDULED_JOB_STATUS.ACTIVE.value,
            cron=schedule_config.get('cron'),
            interval_seconds=schedule_config.get('interval'),
            repeat=schedule_config.get('repeat')
        )
        
        # Store schedule
        await self._store_schedule(schedule)
        
        # Log schedule created event
        await self.event_service.log_job_scheduled(
            job_id=job.job_id,
            queue_name=job.queue_name,
            scheduled_timestamp=schedule.scheduled_timestamp_utc,
            cron=schedule.cron,
            interval=schedule.interval_seconds,
            repeat=schedule.repeat
        )
        
        return job.job_id
    
    async def trigger_due_jobs(self) -> List[str]:
        """Find and trigger due jobs with event logging."""
        current_time = time.time()
        due_schedules = await self._get_due_schedules(current_time)
        
        triggered_jobs = []
        
        for schedule in due_schedules:
            try:
                # Deserialize job
                job = await self._get_scheduled_job(schedule.job_id)
                
                # Log schedule triggered event
                await self.event_service.log_schedule_triggered(
                    job_id=schedule.job_id,
                    queue_name=job.queue_name,
                    scheduled_time=schedule.scheduled_timestamp_utc,
                    actual_time=current_time
                )
                
                # Enqueue job
                job_service = await self.services.get_service(JobService)
                await job_service.enqueue_job(job, job.queue_name)
                
                triggered_jobs.append(schedule.job_id)
                
                # Update schedule for recurring jobs
                if schedule.cron or schedule.interval_seconds:
                    next_time = self._calculate_next_run_time(schedule)
                    if next_time:
                        schedule.scheduled_timestamp_utc = next_time
                        await self._update_schedule(schedule)
                
            except Exception as e:
                # Log scheduler error
                await self.event_service.log_scheduler_error(
                    schedule_id=schedule.job_id,
                    error_type=type(e).__name__,
                    error_message=str(e)
                )
        
        return triggered_jobs
```

### 4. Enhanced CLI Event Commands

**Complete CLI Event Monitoring:**
```python
# In cli/event_commands.py
import asyncio
from rich.live import Live
from rich.table import Table
from rich.console import Console

@event_app.command()
def stream(
    job_id: Optional[str] = typer.Option(None, "--job-id", help="Filter by job ID"),
    event_type: Optional[str] = typer.Option(None, "--event-type", help="Filter by event type"),
    queue: Optional[str] = typer.Option(None, "--queue", help="Filter by queue name"),
    worker: Optional[str] = typer.Option(None, "--worker", help="Filter by worker ID"),
    format: str = typer.Option("table", "--format", help="Output format: table, json, raw"),
    follow: bool = typer.Option(True, "--follow/--no-follow", help="Follow new events"),
    tail: int = typer.Option(10, "--tail", help="Show last N events"),
):
    """Stream job events in real-time."""
    
    async def stream_events():
        config = get_config()
        async with ServiceManager(config) as services:
            event_service = await services.get_service(EventService)
            
            # Get historical events if requested
            if tail > 0:
                filters = {
                    'job_id': job_id,
                    'event_type': event_type,
                    'queue_name': queue,
                    'worker_id': worker
                }
                
                historical = await event_service.get_recent_events(
                    limit=tail, 
                    filters={k: v for k, v in filters.items() if v is not None}
                )
                
                for event in historical:
                    display_event(event, format)
            
            # Stream live events if following
            if follow:
                async for event in event_service.stream_events(
                    job_id=job_id,
                    event_type=event_type,
                    queue_name=queue,
                    worker_id=worker
                ):
                    display_event(event, format)
    
    asyncio.run(stream_events())

@event_app.command() 
def history(
    job_id: str = typer.Argument(help="Job ID to get history for"),
    format: str = typer.Option("table", "--format", help="Output format"),
):
    """Get complete event history for a job."""
    
    async def get_history():
        config = get_config()
        async with ServiceManager(config) as services:
            event_service = await services.get_service(EventService)
            events = await event_service.get_job_event_history(job_id)
            
            if format == "table":
                display_event_table(events)
            elif format == "json":
                console.print_json([event.to_dict() for event in events])
            else:
                for event in events:
                    console.print(f"{event.timestamp} {event.event_type} {event.message}")
    
    asyncio.run(get_history())

@event_app.command()
def stats(
    hours: int = typer.Option(24, "--hours", help="Time window in hours"),
    by_queue: bool = typer.Option(False, "--by-queue", help="Group by queue"),
    by_worker: bool = typer.Option(False, "--by-worker", help="Group by worker"),
):
    """Show event statistics and analytics."""
    
    async def show_stats():
        config = get_config()
        async with ServiceManager(config) as services:
            event_service = await services.get_service(EventService)
            
            # Get event statistics
            stats = await event_service.get_event_statistics(
                hours=hours,
                group_by_queue=by_queue,
                group_by_worker=by_worker
            )
            
            # Display statistics table
            display_stats_table(stats)
    
    asyncio.run(show_stats())

@event_app.command()
def workers(
    format: str = typer.Option("table", "--format", help="Output format"),
    show_inactive: bool = typer.Option(False, "--show-inactive", help="Include inactive workers"),
):
    """Monitor worker events and status."""
    
    async def monitor_workers():
        config = get_config()
        async with ServiceManager(config) as services:
            event_service = await services.get_service(EventService)
            
            # Get worker status from recent events
            worker_status = await event_service.get_worker_status(
                include_inactive=show_inactive
            )
            
            if format == "table":
                display_worker_table(worker_status)
            else:
                console.print_json(worker_status)
    
    asyncio.run(monitor_workers())

def display_event(event: JobEvent, format: str) -> None:
    """Display single event in specified format."""
    if format == "json":
        console.print_json(event.to_dict())
    elif format == "raw":
        console.print(f"{event.timestamp} {event.job_id} {event.event_type} {event.message}")
    else:  # table
        table = Table()
        table.add_column("Time")
        table.add_column("Job ID") 
        table.add_column("Event")
        table.add_column("Queue")
        table.add_column("Worker")
        table.add_column("Message")
        
        table.add_row(
            datetime.fromtimestamp(event.timestamp).strftime("%H:%M:%S"),
            event.job_id[:8],
            event.event_type,
            event.queue_name or "-",
            event.worker_id or "-", 
            event.message or ""
        )
        
        console.print(table)
```

### 5. Event Performance Optimization

**Ensure Event Logging Doesn't Impact Performance:**
```python
# In EventService
class EventService(BaseService):
    def __init__(self, config: NAQConfig):
        super().__init__(config)
        self.enabled = config.events.enabled
        self._logger: Optional[AsyncJobEventLogger] = None
        self._background_task: Optional[asyncio.Task] = None
    
    async def log_event(self, event: JobEvent) -> None:
        """Non-blocking event logging."""
        if not self.enabled:
            return
            
        # Quick return if event logging disabled
        if not self._logger:
            return
            
        # Log event asynchronously (non-blocking)
        await self._logger.log_event(event)
    
    async def _initialize_logger(self) -> None:
        """Initialize event logger with performance optimization."""
        if not self.enabled:
            return
            
        self._logger = AsyncJobEventLogger(
            batch_size=self.config.events.batch_size,
            flush_interval=self.config.events.flush_interval,
            max_buffer_size=self.config.events.max_buffer_size
        )
        
        await self._logger.start()
        
        # Start background flush task for guaranteed delivery
        self._background_task = asyncio.create_task(self._background_flush())
    
    async def _background_flush(self) -> None:
        """Background task to ensure event delivery."""
        while self.enabled and self._logger:
            try:
                await asyncio.sleep(self.config.events.flush_interval * 2)
                if self._logger:
                    await self._logger.flush()
            except Exception as e:
                logger.warning(f"Background event flush error: {e}")
```

### 6. Custom Event Processing Support

**Enable Custom Event Processors:**
```python
# In EventService - Custom processor registration
class EventService(BaseService):
    def __init__(self, config: NAQConfig):
        super().__init__(config)
        self._custom_processors: List[Callable[[JobEvent], Awaitable[None]]] = []
    
    def register_event_processor(
        self, 
        processor: Callable[[JobEvent], Awaitable[None]]
    ) -> None:
        """Register custom event processor."""
        self._custom_processors.append(processor)
    
    async def log_event(self, event: JobEvent) -> None:
        """Log event and notify custom processors."""
        # Standard event logging
        if self._logger:
            await self._logger.log_event(event)
        
        # Notify custom processors (non-blocking)
        for processor in self._custom_processors:
            asyncio.create_task(self._safe_process_event(processor, event))
    
    async def _safe_process_event(
        self, 
        processor: Callable[[JobEvent], Awaitable[None]], 
        event: JobEvent
    ) -> None:
        """Safely execute custom event processor."""
        try:
            await processor(event)
        except Exception as e:
            logger.warning(f"Custom event processor error: {e}")

# Usage example
async def custom_alert_processor(event: JobEvent) -> None:
    """Custom processor for job failure alerts."""
    if event.event_type == JobEventType.FAILED:
        if event.duration_ms and event.duration_ms > 60000:  # > 1 minute
            await send_alert(f"Long-running job failed: {event.job_id}")

# Register processor
event_service.register_event_processor(custom_alert_processor)
```

## Testing Strategy

### Event Integration Tests
```python
async def test_job_events_logged():
    """Test that job execution logs all expected events."""
    config = get_test_config()
    
    async with ServiceManager(config) as services:
        job_service = await services.get_service(JobService)
        event_service = await services.get_service(EventService)
        
        # Mock event processor to capture events
        captured_events = []
        
        async def capture_events(event):
            captured_events.append(event)
        
        event_service.register_event_processor(capture_events)
        
        # Execute job
        job = Job(function=dummy_task, args=("test",))
        result = await job_service.execute_job(job, "test-worker")
        
        # Verify events were logged
        assert len(captured_events) >= 2  # started + completed/failed
        assert captured_events[0].event_type == JobEventType.STARTED
        assert captured_events[-1].event_type in [JobEventType.COMPLETED, JobEventType.FAILED]

async def test_worker_events_logged():
    """Test that worker lifecycle logs events."""
    # Similar test for worker events

async def test_event_performance():
    """Test that event logging doesn't impact performance.""" 
    # Performance test comparing with/without events
```

## Files to Update

### Core Integration
- `src/naq/services/jobs.py` - Complete job event logging
- `src/naq/services/events.py` - Enhance EventService capabilities  
- `src/naq/worker/core.py` - Complete worker event integration
- `src/naq/services/scheduler.py` - Complete scheduler event integration

### CLI Commands  
- `src/naq/cli/event_commands.py` - Complete event monitoring commands
- `src/naq/cli/worker_commands.py` - Add worker event monitoring
- `src/naq/cli/job_commands.py` - Add job event monitoring

### Event System
- `src/naq/events/logger.py` - Performance optimizations
- `src/naq/events/processor.py` - Custom processor support
- `src/naq/events/storage.py` - Query optimizations

### Configuration
- Add event-specific configuration options
- Event system performance tuning settings

## Success Criteria

### Functional Requirements
- [ ] All job lifecycle events logged consistently
- [ ] All worker lifecycle events logged consistently  
- [ ] All scheduler events logged consistently
- [ ] CLI event commands work with real-time streaming
- [ ] Event history queries work correctly
- [ ] Custom event processors supported

### Performance Requirements  
- [ ] Event logging adds <5% overhead to job processing
- [ ] Event streaming supports >1000 events/second
- [ ] Event queries return results in <100ms
- [ ] Memory usage for event buffering bounded
- [ ] No event logging failures impact job processing

### Quality Requirements
- [ ] Event data consistent and complete
- [ ] Event timestamps accurate across distributed system
- [ ] Event filtering and querying robust
- [ ] Error handling prevents event logging failures
- [ ] Documentation complete for event system

## Dependencies
- **Depends on**: 
  - Task 05 (Service Layer) - EventService implementation
  - Task 09 (Service Integration) - Services available in components
- **Blocks**: None - completes event system implementation

## Estimated Time
- **Job Event Integration**: 10-12 hours
- **Worker Event Integration**: 8-10 hours
- **Scheduler Event Integration**: 6-8 hours
- **CLI Commands**: 12-15 hours  
- **Performance Optimization**: 8-10 hours
- **Testing**: 10-12 hours
- **Total**: 54-67 hours