import typer
import asyncio
import datetime
import nats.js.errors
from loguru import logger
from typing import Optional, cast
from datetime import timezone

import cloudpickle
from rich.table import Table

from rich.console import Console

# Create a shared console instance
console = Console()
from ..settings import DEFAULT_NATS_URL, SCHEDULED_JOB_STATUS, SCHEDULED_JOBS_KV_NAME
from ..utils import setup_logging
from ..scheduler import Scheduler
from ..services import ServiceManager, ConnectionService, KVStoreService
from ..services.scheduler import SchedulerService
from ..services.events import EventService

# Create scheduler app
scheduler_app = typer.Typer(help="Scheduler and scheduled job commands")


@scheduler_app.command("start")
def start(
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
        help="Set logging level (e.g., DEBUG, INFO, WARNING, ERROR). Defaults to NAQ_LOG_LEVEL env var or CRITICAL.",
    ),
):
    """
    Starts a naq scheduler process to execute scheduled jobs at their specified times.

    In high availability mode (default), multiple scheduler instances can be run simultaneously
    and they will coordinate using leader election to ensure jobs are only processed once.
    """
    setup_logging(log_level if log_level else None)
    enable_ha = not disable_ha

    logger.info(
        f"Starting scheduler{f' instance {instance_id}' if instance_id else ''}"
    )
    logger.info(f"NATS URL: {nats_url}")
    logger.info(f"Poll interval: {poll_interval}s")
    logger.info(f"High availability mode: {'enabled' if enable_ha else 'disabled'}")

    s = Scheduler(
        nats_url=nats_url,
        poll_interval=poll_interval,
        instance_id=instance_id,
        enable_ha=enable_ha,
    )
    try:
        asyncio.run(s.run())
    except KeyboardInterrupt:
        logger.info("Scheduler interrupted by user (KeyboardInterrupt). Shutting down.")
    except Exception as e:
        logger.exception(f"Scheduler failed unexpectedly: {e}")
        raise typer.Exit(code=1)
    finally:
        logger.info("Scheduler process finished.")


@scheduler_app.command("jobs")
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
        help=f"Filter by job status: '{SCHEDULED_JOB_STATUS.ACTIVE}', '{SCHEDULED_JOB_STATUS.PAUSED}', or '{SCHEDULED_JOB_STATUS.FAILED}'",
    ),
    job_id: Optional[str] = typer.Option(
        None,
        "--job-id",
        "-j",
        help="Filter by job ID",
    ),
    queue: str = typer.Option(
        DEFAULT_NATS_URL,
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
        help="Set logging level (e.g., DEBUG, INFO, WARNING, ERROR). Defaults to NAQ_LOG_LEVEL env var or CRITICAL.",
    ),
):
    """
    Lists all scheduled jobs with their status and next run time.
    """
    setup_logging(log_level if log_level else None)
    logger.info(f"Listing scheduled jobs from NATS at {nats_url}")

    async def _list_scheduled_jobs_async():
        config = {
            'nats': {
                'url': nats_url,
            },
            'scheduler': {
                'poll_interval': 1.0,
            },
            'events': {
                'nats_url': nats_url,
                'stream_name': 'NAQ_JOB_EVENTS',
                'subject_prefix': 'naq.jobs.events',
                'batch_size': 100,
                'flush_interval': 5.0,
                'max_buffer_size': 10000,
            }
        }
        
        try:
            async with ServiceManager(config) as services:
                connection_service = cast(ConnectionService, await services.get_service(ConnectionService))
                kv_store_service = cast(KVStoreService, await services.get_service(KVStoreService))
                event_service = cast(EventService, await services.get_service(EventService))
                
                # Create scheduler service with dependencies
                scheduler_service = SchedulerService(config, connection_service, kv_store_service, event_service)
                
                # Get scheduled jobs using scheduler service
                jobs_data = await scheduler_service.list_scheduled_jobs(status=status)
                
                # Filter by job_id if specified
                if job_id:
                    jobs_data = [job for job in jobs_data if job.get("job_id") == job_id]
                
                # Filter by queue if specified
                if queue and queue != DEFAULT_NATS_URL:  # DEFAULT_NATS_URL is the default value for queue parameter
                    jobs_data = [job for job in jobs_data if job.get("queue_name") == queue]

                if not jobs_data:
                    console.print("[yellow]No scheduled jobs found.[/yellow]")
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
                        last_enqueued_utc = job.get("last_enqueued_utc")
                        if last_enqueued_utc is not None:
                            last_run = datetime.datetime.fromtimestamp(
                                last_enqueued_utc, timezone.utc
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
            logger.exception(f"Error listing scheduled jobs: {e}")
            console.print(f"[red]Error listing scheduled jobs: {str(e)}[/red]")

    # Run the async routine
    asyncio.run(_list_scheduled_jobs_async())