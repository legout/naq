"""Scheduler-related CLI commands for naq."""

import asyncio
import datetime
from datetime import timezone
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from ..settings import DEFAULT_NATS_URL
from ..models.enums import SCHEDULED_JOB_STATUS
from ..scheduler import Scheduler
from ..services.base import ServiceManager, ServiceConfig
from ..services.scheduler import SchedulerService
from ..services.connection import ConnectionService
from ..services.config import GlobalServiceConfig
from ..utils import setup_logging
from ..utils.decorators import timing, log_errors
from ..utils.logging import StructuredLogger
from ..utils.nats_helpers import build_subject, stream_exists
from ..utils.serialization import SerializationHelper
from ..utils.validation import ensure_type, validate_parameter

# Create a Typer instance for scheduler commands
scheduler_app = typer.Typer(
    name="scheduler",
    help="Scheduler management commands",
    add_completion=False,
)


@scheduler_app.command("start")
@timing()
@log_errors()
def start_scheduler(
    nats_url: str = typer.Option(
        DEFAULT_NATS_URL,
        "--nats-url",
        "-u",
        help="URL of the NATS server.",
        envvar="NAQ_NATS_URL",
    ),
    poll_interval: float = typer.Option(
        1.0,
        "--poll-interval",
        "-p",
        min=0.1,
        help="Interval in seconds between checks for due jobs.",
    ),
    instance_id: Optional[str] = typer.Option(
        None,
        "--instance-id",
        "-i",
        help="Optional unique ID for this scheduler instance (for high availability).",
    ),
    disable_ha: bool = typer.Option(
        False,
        "--disable-ha",
        help="Disable high availability mode (leader election).",
    ),
    log_level: Optional[str] = typer.Option(
        None,
        "--log-level",
        "-l",
        help=(
            "Set logging level (e.g., DEBUG, INFO, WARNING, ERROR). "
            "Defaults to NAQ_LOG_LEVEL env var or CRITICAL."
        ),
    ),
) -> None:
    """
    Starts a naq scheduler process to execute scheduled jobs at their specified times.

    In high availability mode (default), multiple scheduler instances can be run
    simultaneously and they will coordinate using leader election to ensure jobs
    are only processed once.
    """
    # Validate parameters
    validate_parameter(nats_url, "nats_url", not_none=True)
    validate_parameter(poll_interval, "poll_interval", min_value=0.1)
    ensure_type(log_level, (str, type(None)), "log_level", convert=False)
    ensure_type(instance_id, (str, type(None)), "instance_id", convert=False)
    
    setup_logging(log_level if log_level else None)
    enable_ha = not disable_ha

    # Use structured logger
    structured_logger = StructuredLogger("scheduler")
    structured_logger.info(
        f"Starting scheduler{f' instance {instance_id}' if instance_id else ''}",
        nats_url=nats_url,
        poll_interval=poll_interval,
        high_availability_enabled=enable_ha,
        instance_id=instance_id
    )

    async def _run_scheduler():
        with structured_logger.operation_context(
            "scheduler_run",
            nats_url=nats_url,
            poll_interval=poll_interval,
            instance_id=instance_id,
            enable_ha=enable_ha
        ):
            # Create global config with NATS URL and custom settings
            config = GlobalServiceConfig()
            config.nats_url = nats_url
            
            # Use serialize_with_metadata for configuration data
            config_data = {
                "log_level": log_level,
                "poll_interval": poll_interval,
                "instance_id": instance_id,
                "enable_ha": enable_ha,
            }
            
            try:
                # Serialize configuration with metadata
                serialized_config = SerializationHelper.serialize_with_metadata(
                    config_data,
                    serializer="json",
                    metadata={"source": "scheduler_cli", "version": "1.0"}
                )
                structured_logger.debug(
                    "Configuration serialized with metadata",
                    operation="scheduler_run",
                    status="config_serialized"
                )
            except Exception as e:
                structured_logger.warning(
                    f"Failed to serialize configuration with metadata: {e}",
                    operation="scheduler_run",
                    status="config_serialization_warning",
                    error=str(e)
                )
                # Fall back to direct assignment
                config.custom_settings.update(config_data)
            else:
                # Update config with the original data (serialization was for demonstration)
                config.custom_settings.update(config_data)

            try:
                # Create service manager with configuration
                service_manager = ServiceManager(
                    config=ServiceConfig(
                        nats_url=nats_url,
                        custom_settings={
                            "log_level": log_level,
                            "poll_interval": poll_interval,
                            "instance_id": instance_id,
                            "enable_ha": enable_ha,
                        },
                    )
                )

                # Register required services
                scheduler_service = await service_manager.register_service(
                    "scheduler", SchedulerService, initialize=True
                )
                
                # Get connection service for NATS operations
                connection_service = await service_manager.register_service(
                    "connection", ConnectionService, initialize=True
                )

                # Check if the required stream exists using nats_helpers
                js = await connection_service.get_jetstream()
                stream_name = "naq_jobs"
                if await stream_exists(js=js, stream_name=stream_name):
                    structured_logger.debug(
                        f"Stream '{stream_name}' exists",
                        operation="scheduler_run",
                        status="stream_check"
                    )
                else:
                    structured_logger.warning(
                        f"Stream '{stream_name}' does not exist",
                        operation="scheduler_run",
                        status="stream_check"
                    )
                
                # Build subject for scheduler operations
                scheduler_subject = build_subject("naq", "scheduler", instance_id or "default")
                structured_logger.debug(
                    f"Using scheduler subject: {scheduler_subject}",
                    operation="scheduler_run",
                    status="subject_built"
                )

                # Create and run scheduler with services
                s = Scheduler(
                    nats_url=nats_url,
                    poll_interval=poll_interval,
                    instance_id=instance_id,
                    enable_ha=enable_ha,
                    connection_service=connection_service,
                    scheduler_service=scheduler_service,
                )
                await s.run()

            except KeyboardInterrupt:
                structured_logger.info(
                    "Scheduler interrupted by user (KeyboardInterrupt). Shutting down.",
                    operation="scheduler_run",
                    status="interrupted"
                )
            except Exception as e:
                structured_logger.error(
                    f"Scheduler failed unexpectedly: {e}",
                    operation="scheduler_run",
                    status="failed",
                    error=str(e)
                )
                raise typer.Exit(code=1)
            finally:
                structured_logger.info(
                    "Scheduler process finished.",
                    operation="scheduler_run",
                    status="finished"
                )
                if "service_manager" in locals():
                    await service_manager.cleanup_all()

    # Run the async function
    asyncio.run(_run_scheduler())


@scheduler_app.command("jobs")
@timing()
@log_errors()
def list_scheduled_jobs(
    nats_url: str = typer.Option(
        DEFAULT_NATS_URL,
        "--nats-url",
        "-u",
        help="URL of the NATS server.",
        envvar="NAQ_NATS_URL",
    ),
    status: Optional[str] = typer.Option(
        None,
        "--status",
        "-s",
        help=(
            f"Filter by job status: '{SCHEDULED_JOB_STATUS.ACTIVE}', "
            f"'{SCHEDULED_JOB_STATUS.PAUSED}', or '{SCHEDULED_JOB_STATUS.FAILED}'"
        ),
    ),
    job_id: Optional[str] = typer.Option(
        None,
        "--job-id",
        "-j",
        help="Filter by job ID",
    ),
    queue: str = typer.Option(
        "default",
        "--queue",
        "-q",
        help="Filter by queue name",
    ),
    detailed: bool = typer.Option(
        False,
        "--detailed",
        "-d",
        help="Show detailed job information",
    ),
    log_level: Optional[str] = typer.Option(
        None,
        "--log-level",
        "-l",
        help=(
            "Set logging level (e.g., DEBUG, INFO, WARNING, ERROR). "
            "Defaults to NAQ_LOG_LEVEL env var or CRITICAL."
        ),
    ),
) -> None:
    """
    Lists all scheduled jobs with their status and next run time.
    """
    # Validate parameters
    validate_parameter(nats_url, "nats_url", not_none=True)
    validate_parameter(queue, "queue", not_none=True)
    ensure_type(status, (str, type(None)), "status", convert=False)
    ensure_type(job_id, (str, type(None)), "job_id", convert=False)
    ensure_type(detailed, bool, "detailed", convert=False)
    ensure_type(log_level, (str, type(None)), "log_level", convert=False)
    
    setup_logging(log_level if log_level else None)
    
    # Use structured logger
    structured_logger = StructuredLogger("scheduler_jobs")
    structured_logger.info(
        f"Listing scheduled jobs from NATS at {nats_url}",
        nats_url=nats_url,
        status_filter=status,
        job_id_filter=job_id,
        queue_filter=queue,
        detailed_view=detailed
    )
    console = Console()

    async def _list_scheduled_jobs_async():
        with structured_logger.operation_context(
            "list_scheduled_jobs",
            nats_url=nats_url,
            status_filter=status,
            job_id_filter=job_id,
            queue_filter=queue,
            detailed_view=detailed
        ):
            # Create global config with NATS URL and custom settings
            config = GlobalServiceConfig()
            config.nats_url = nats_url
            config.custom_settings.update({"log_level": log_level})

            try:
                # Create service manager with configuration
                service_manager = ServiceManager(
                    config=ServiceConfig(
                        nats_url=nats_url, custom_settings={"log_level": log_level}
                    )
                )

                # Register required services
                scheduler_service = await service_manager.register_service(
                    "scheduler", SchedulerService, initialize=True
                )
                
                # Get connection service for NATS operations
                connection_service = await service_manager.register_service(
                    "connection", ConnectionService, initialize=True
                )

                # Check if the required stream exists using nats_helpers
                js = await connection_service.get_jetstream()
                stream_name = "naq_jobs"
                if await stream_exists(js=js, stream_name=stream_name):
                    structured_logger.debug(
                        f"Stream '{stream_name}' exists",
                        operation="list_scheduled_jobs",
                        status="stream_check"
                    )
                else:
                    structured_logger.warning(
                        f"Stream '{stream_name}' does not exist",
                        operation="list_scheduled_jobs",
                        status="stream_check"
                    )
                
                # Build subject for scheduler operations
                scheduler_subject = build_subject("naq", "scheduler", "jobs")
                structured_logger.debug(
                    f"Using scheduler subject: {scheduler_subject}",
                    operation="list_scheduled_jobs",
                    status="subject_built"
                )

                # Parse status filter
                status_filter = None
                if status:
                    try:
                        # Validate status parameter
                        validate_parameter(
                            status,
                            "status",
                            not_none=True,
                            custom_validator=lambda x: x in [
                                SCHEDULED_JOB_STATUS.ACTIVE,
                                SCHEDULED_JOB_STATUS.PAUSED,
                                SCHEDULED_JOB_STATUS.FAILED
                            ]
                        )
                        status_filter = SCHEDULED_JOB_STATUS(status)
                    except ValueError as e:
                        structured_logger.error(
                            f"Invalid status filter: {status}",
                            operation="list_scheduled_jobs",
                            status="validation_error",
                            invalid_status=status,
                            error=str(e)
                        )
                        console.print(f"[red]Invalid status: {status}[/red]")
                        return

                # Get scheduled jobs using the service
                try:
                    jobs_data = []
                    schedules = await scheduler_service.list_scheduled_jobs(
                        status_filter
                    )

                    for schedule in schedules:
                        # Convert schedule to job data format for compatibility
                        job_data = {
                            "job_id": schedule.job_id,
                            "queue_name": schedule.queue_name,
                            "status": schedule.status,
                            "scheduled_timestamp_utc": schedule.scheduled_timestamp_utc,
                            "cron": schedule.cron,
                            "interval_seconds": schedule.interval_seconds,
                            "repeat": schedule.repeat,
                            "last_enqueued_utc": schedule.last_enqueued_utc,
                            "schedule_failure_count": schedule.schedule_failure_count,
                        }

                        # Apply filters
                        if job_id and job_id != schedule.job_id:
                            continue
                        if queue and schedule.queue_name != queue:
                            continue

                        # Use SerializationHelper to serialize job data with metadata
                        try:
                            serialized_job = SerializationHelper.safe_serialize(
                                job_data,
                                serializer="json",
                                fallback_serializer="pickle"
                            )
                            # Deserialize to get the original data back
                            deserialized_job = SerializationHelper.safe_deserialize(
                                serialized_job,
                                serializer="json",
                                expected_type=dict
                            )
                            jobs_data.append(deserialized_job)
                        except Exception as e:
                            structured_logger.warning(
                                f"Failed to serialize/deserialize job data: {e}",
                                operation="list_scheduled_jobs",
                                status="serialization_warning",
                                job_id=schedule.job_id,
                                error=str(e)
                            )
                            # Fall back to using the original job data
                            jobs_data.append(job_data)

                except Exception as e:
                    structured_logger.error(
                        f"Failed to list scheduled jobs: {e}",
                        operation="list_scheduled_jobs",
                        status="error",
                        error=str(e)
                    )
                    console.print(
                        "[yellow]No scheduled jobs found or cannot access "
                        "job store.[/yellow]"
                    )
                    return

                jobs_data.sort(key=lambda j: j.get("scheduled_timestamp_utc", 0))

                if detailed:
                    table = Table(
                        title="NAQ Scheduled Jobs",
                        show_header=True,
                        header_style="bold cyan",
                    )
                    table.add_column("JOB ID", style="dim", width=36)
                    table.add_column("QUEUE", width=15)
                    table.add_column("STATUS", width=10)
                    table.add_column("NEXT RUN", width=25)
                    table.add_column("SCHEDULE TYPE", width=15)
                    table.add_column("REPEATS LEFT", width=12)
                    table.add_column("DETAILS")
                else:
                    table = Table(
                        title="NAQ Scheduled Jobs",
                        show_header=True,
                        header_style="bold cyan",
                    )
                    table.add_column("JOB ID", style="dim", width=36)
                    table.add_column("QUEUE", width=15)
                    table.add_column("STATUS", width=10)
                    table.add_column("NEXT RUN", width=25)
                    table.add_column("SCHEDULE TYPE", width=15)

                for job in jobs_data:
                    job_id_local = job.get("job_id", "unknown")
                    queue_name = job.get("queue_name", "unknown")
                    current_job_status = job.get("status", SCHEDULED_JOB_STATUS.ACTIVE)

                    status_style = "green"
                    if current_job_status == SCHEDULED_JOB_STATUS.PAUSED:
                        status_style = "yellow"
                    elif current_job_status == SCHEDULED_JOB_STATUS.FAILED:
                        status_style = "red"

                    next_run_ts = job.get("scheduled_timestamp_utc")
                    if next_run_ts:
                        next_run = datetime.datetime.fromtimestamp(
                            next_run_ts, timezone.utc
                        ).strftime("%Y-%m-%d %H:%M:%S UTC")
                    else:
                        next_run = "unknown"

                    if job.get("cron"):
                        schedule_type = "cron"
                    elif job.get("interval_seconds"):
                        schedule_type = "interval"
                    else:
                        schedule_type = "one-time"

                    if detailed:
                        repeats = (
                            "infinite"
                            if job.get("repeat") is None
                            else str(job.get("repeat", 0))
                        )
                        details = []
                        if job.get("cron"):
                            details.append(f"cron='{job.get('cron')}'")
                        if job.get("interval_seconds"):
                            details.append(f"interval={job.get('interval_seconds')}s")
                        if job.get("schedule_failure_count", 0) > 0:
                            details.append(f"failures={job.get('schedule_failure_count')}")
                        if job.get("last_enqueued_utc"):
                            last_run = datetime.datetime.fromtimestamp(
                                job.get("last_enqueued_utc"), timezone.utc
                            ).strftime("%Y-%m-%d %H:%M:%S UTC")
                            details.append(f"last_run={last_run}")

                        details_str = ", ".join(details)
                        table.add_row(
                            job_id_local,
                            queue_name,
                            f"[{status_style}]{current_job_status}[/{status_style}]",
                            next_run,
                            schedule_type,
                            repeats,
                            details_str,
                        )
                    else:
                        table.add_row(
                            job_id_local,
                            queue_name,
                            f"[{status_style}]{current_job_status}[/{status_style}]",
                            next_run,
                            schedule_type,
                        )

                console.print(table)
                console.print(f"\n[bold]Total:[/bold] {len(jobs_data)} scheduled job(s)")
            except Exception as e:
                structured_logger.error(
                    f"Error listing scheduled jobs: {e}",
                    operation="list_scheduled_jobs",
                    status="error",
                    error=str(e)
                )
                console.print(f"[red]Error listing scheduled jobs: {str(e)}[/red]")
            finally:
                if "service_manager" in locals():
                    await service_manager.cleanup_all()

    # Run the async routine
    asyncio.run(_list_scheduled_jobs_async())
