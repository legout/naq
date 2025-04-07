import asyncio
import datetime
import time
from datetime import timezone
from typing import List, Optional  # , Dict, Any

import cloudpickle
import typer
import uvicorn  # Use uvicorn to run Sanic
from loguru import logger

# from naq.dashboard.app import app as dashboard_app

from . import __version__
from .connection import (
    close_nats_connection,
    get_jetstream_context,
    get_nats_connection,
)
from .queue import Queue
from .scheduler import Scheduler
from .settings import DEFAULT_WORKER_TTL_SECONDS  # Import statuses
from .settings import (  # WORKER_STATUS_IDLE,; WORKER_STATUS_STARTING,; WORKER_STATUS_STOPPING,
    DEFAULT_NATS_URL,
    SCHEDULED_JOB_STATUS_ACTIVE,
    SCHEDULED_JOB_STATUS_FAILED,
    SCHEDULED_JOB_STATUS_PAUSED,
    SCHEDULED_JOBS_KV_NAME,
    WORKER_STATUS_BUSY,
)
from .utils import setup_logging
from .worker import Worker

# import os  # Import os for environment variables


app = typer.Typer(
    name="naq",
    help="A simple NATS-based queueing system, similar to RQ.",
    add_completion=False,
)

# --- Helper Functions ---


def version_callback(value: bool):
    if value:
        print(f"naq version: {__version__}")
        raise typer.Exit()


# --- CLI Commands ---


@app.command()
def worker(
    queues: List[str] = typer.Argument(
        ..., help="The names of the queues to listen to."
    ),
    nats_url: Optional[str] = typer.Option(
        DEFAULT_NATS_URL,
        "--nats-url",
        "-u",
        help="URL of the NATS server.",
        envvar="NAQ_NATS_URL",  # Allow setting via env var
    ),
    concurrency: int = typer.Option(
        10,
        "--concurrency",
        "-c",
        min=1,
        help="Maximum number of concurrent jobs to process.",
    ),
    name: Optional[str] = typer.Option(
        None,
        "--name",
        "-n",
        help="Optional name for this worker instance.",
    ),
    log_level: str = typer.Option(
        "INFO",
        "--log-level",
        "-l",
        help="Set logging level (e.g., DEBUG, INFO, WARNING, ERROR).",
    ),
):
    """
    Starts a naq worker process to listen for and execute jobs on the specified queues.
    """
    setup_logging(log_level)
    # Use loguru directly
    logger.info(f"Starting worker '{name or 'default'}' for queues: {queues}")
    logger.info(f"NATS URL: {nats_url}")
    logger.info(f"Concurrency: {concurrency}")

    w = Worker(
        queues=queues,
        nats_url=nats_url,
        concurrency=concurrency,
        worker_name=name,
    )
    try:
        asyncio.run(w.run())
    except KeyboardInterrupt:
        logger.info("Worker interrupted by user (KeyboardInterrupt). Shutting down.")
    except Exception as e:
        logger.exception(
            f"Worker failed unexpectedly: {e}"
        )  # Use logger.exception for stack trace
        raise typer.Exit(code=1)
    finally:
        logger.info("Worker process finished.")


@app.command()
def purge(
    queues: List[str] = typer.Argument(..., help="The names of the queues to purge."),
    nats_url: Optional[str] = typer.Option(
        DEFAULT_NATS_URL,
        "--nats-url",
        "-u",
        help="URL of the NATS server.",
        envvar="NAQ_NATS_URL",
    ),
    log_level: str = typer.Option(
        "INFO",
        "--log-level",
        "-l",
        help="Set logging level (e.g., DEBUG, INFO, WARNING, ERROR).",
    ),
):
    """
    Removes all jobs from the specified queues.
    """
    setup_logging(log_level)
    logger.info(f"Attempting to purge queues: {queues}")
    logger.info(f"Using NATS URL: {nats_url}")

    async def _purge_queues():
        total_purged = 0
        for queue_name in queues:
            try:
                q = Queue(name=queue_name, nats_url=nats_url)
                purged_count = await q.purge()
                logger.info(
                    f"Successfully purged queue '{queue_name}'. Removed {purged_count} jobs."
                )
                total_purged += purged_count
            except Exception as e:
                logger.error(f"Failed to purge queue '{queue_name}': {e}")
                # Optionally exit or continue with other queues
        logger.info(f"Purge operation finished. Total jobs removed: {total_purged}")
        # Close connection if opened by Queue instances
        await close_nats_connection()

    try:
        asyncio.run(_purge_queues())
    except Exception as e:
        logger.exception(f"Purge command failed unexpectedly: {e}")
        raise typer.Exit(code=1)
    finally:
        logger.info("Purge process finished.")


@app.command()
def scheduler(
    nats_url: Optional[str] = typer.Option(
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
    log_level: str = typer.Option(
        "INFO",
        "--log-level",
        "-l",
        help="Set logging level (e.g., DEBUG, INFO, WARNING, ERROR).",
    ),
):
    """
    Starts a naq scheduler process to execute scheduled jobs at their specified times.

    In high availability mode (default), multiple scheduler instances can be run simultaneously
    and they will coordinate using leader election to ensure jobs are only processed once.
    """
    setup_logging(log_level)
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


@app.command("list-scheduled")
def list_scheduled_jobs(
    nats_url: Optional[str] = typer.Option(
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
        help=f"Filter by job status: '{SCHEDULED_JOB_STATUS_ACTIVE}', '{SCHEDULED_JOB_STATUS_PAUSED}', or '{SCHEDULED_JOB_STATUS_FAILED}'",
    ),
    job_id: Optional[str] = typer.Option(
        None,
        "--job-id",
        "-j",
        help="Filter by job ID",
    ),
    queue: Optional[str] = typer.Option(
        None,
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
    log_level: str = typer.Option(
        "INFO",
        "--log-level",
        "-l",
        help="Set logging level (e.g., DEBUG, INFO, WARNING, ERROR).",
    ),
):
    """
    Lists all scheduled jobs with their status and next run time.
    """
    setup_logging(log_level)
    logger.info(f"Listing scheduled jobs from NATS at {nats_url}")

    async def _list_scheduled_jobs():
        try:
            nc = await get_nats_connection(url=nats_url)
            js = await get_jetstream_context(nc=nc)

            try:
                kv = await js.key_value(bucket=SCHEDULED_JOBS_KV_NAME)
            except Exception as e:
                logger.error(
                    f"Failed to access KV store '{SCHEDULED_JOBS_KV_NAME}': {e}"
                )
                print("No scheduled jobs found or cannot access job store.")
                return

            # Get all keys
            keys = await kv.keys()
            if not keys:
                print("No scheduled jobs found.")
                return

            jobs_data = []

            # Process each job
            for key_bytes in keys:
                key = key_bytes.decode("utf-8")

                # If filtering by job_id, skip non-matching jobs
                if job_id and job_id != key:
                    continue

                try:
                    entry = await kv.get(key_bytes)
                    if not entry:
                        continue

                    job_data = cloudpickle.loads(entry.value)

                    # Apply filters
                    if status and job_data.get("status") != status:
                        continue
                    if queue and job_data.get("queue_name") != queue:
                        continue

                    # Add to results
                    jobs_data.append(job_data)
                except Exception as e:
                    logger.error(f"Error processing job '{key}': {e}")
                    continue

            # Sort by next run time
            jobs_data.sort(key=lambda j: j.get("scheduled_timestamp_utc", 0))

            # Print header
            if detailed:
                print(
                    f"{'JOB ID':<36} | {'QUEUE':<15} | {'STATUS':<10} | {'NEXT RUN':<25} | {'SCHEDULE TYPE':<15} | {'REPEATS LEFT':<12} | DETAILS"
                )
                print("-" * 130)
            else:
                print(
                    f"{'JOB ID':<36} | {'QUEUE':<15} | {'STATUS':<10} | {'NEXT RUN':<25} | {'SCHEDULE TYPE':<15}"
                )
                print("-" * 100)

            # Print each job
            for job in jobs_data:
                job_id = job.get("job_id", "unknown")
                queue_name = job.get("queue_name", "unknown")
                status = job.get("status", SCHEDULED_JOB_STATUS_ACTIVE)

                # Format next run time
                next_run_ts = job.get("scheduled_timestamp_utc")
                if next_run_ts:
                    next_run = datetime.datetime.fromtimestamp(
                        next_run_ts, timezone.utc
                    ).strftime("%Y-%m-%d %H:%M:%S UTC")
                else:
                    next_run = "unknown"

                # Determine schedule type
                if job.get("cron"):
                    schedule_type = "cron"
                elif job.get("interval_seconds"):
                    schedule_type = "interval"
                else:
                    schedule_type = "one-time"

                # Basic output
                if detailed:
                    repeats = (
                        "infinite"
                        if job.get("repeat") is None
                        else str(job.get("repeat", 0))
                    )

                    # Determine additional details
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
                    print(
                        f"{job_id:<36} | {queue_name:<15} | {status:<10} | {next_run:<25} | {schedule_type:<15} | {repeats:<12} | {details_str}"
                    )
                else:
                    print(
                        f"{job_id:<36} | {queue_name:<15} | {status:<10} | {next_run:<25} | {schedule_type:<15}"
                    )

            # Print summary
            print(f"\nTotal: {len(jobs_data)} scheduled job(s)")

        except Exception as e:
            logger.exception(f"Error listing scheduled jobs: {e}")
        finally:
            await close_nats_connection()

    try:
        asyncio.run(_list_scheduled_jobs())
    except Exception as e:
        logger.exception(f"Command failed unexpectedly: {e}")
        raise typer.Exit(code=1)


@app.command("job-control")
def job_control(
    job_id: str = typer.Argument(..., help="The ID of the scheduled job to control"),
    action: str = typer.Argument(
        ...,
        help="Action to perform: 'cancel', 'pause', 'resume', or 'reschedule'",
        show_choices=True,
    ),
    nats_url: Optional[str] = typer.Option(
        DEFAULT_NATS_URL,
        "--nats-url",
        "-u",
        help="URL of the NATS server.",
        envvar="NAQ_NATS_URL",
    ),
    cron: Optional[str] = typer.Option(
        None,
        "--cron",
        help="New cron expression for reschedule action",
    ),
    interval: Optional[float] = typer.Option(
        None,
        "--interval",
        help="New interval in seconds for reschedule action",
    ),
    repeat: Optional[int] = typer.Option(
        None,
        "--repeat",
        help="New repeat count for reschedule action",
    ),
    next_run: Optional[str] = typer.Option(
        None,
        "--next-run",
        help="Next run time (ISO format, e.g. '2023-01-01T12:00:00Z') for reschedule action",
    ),
    log_level: str = typer.Option(
        "INFO",
        "--log-level",
        "-l",
        help="Set logging level (e.g., DEBUG, INFO, WARNING, ERROR).",
    ),
):
    """
    Controls scheduled jobs: cancel, pause, resume, or modify scheduling parameters.
    """
    setup_logging(log_level)

    # Validate action
    if action not in ["cancel", "pause", "resume", "reschedule"]:
        logger.error(
            f"Invalid action '{action}'. Must be one of: cancel, pause, resume, reschedule"
        )
        raise typer.Exit(code=1)

    # Validate parameters for reschedule
    if action == "reschedule":
        if not any([cron, interval, repeat, next_run]):
            logger.error(
                "Reschedule action requires at least one scheduling parameter: --cron, --interval, --repeat, or --next-run"
            )
            raise typer.Exit(code=1)
        if cron and interval:
            logger.error(
                "Cannot specify both --cron and --interval. Choose one scheduling method."
            )
            raise typer.Exit(code=1)

    logger.info(f"Performing {action} on job {job_id}")

    async def _control_job():
        try:
            q = Queue(nats_url=nats_url)

            # Process according to action
            if action == "cancel":
                result = await q.cancel_scheduled_job(job_id)
                if result:
                    print(f"Job {job_id} cancelled successfully.")
                else:
                    print(f"Job {job_id} not found or already cancelled.")

            elif action == "pause":
                result = await q.pause_scheduled_job(job_id)
                if result:
                    print(f"Job {job_id} paused successfully.")
                else:
                    print(
                        f"Failed to pause job {job_id}. Job might not exist or was already paused."
                    )

            elif action == "resume":
                result = await q.resume_scheduled_job(job_id)
                if result:
                    print(f"Job {job_id} resumed successfully.")
                else:
                    print(
                        f"Failed to resume job {job_id}. Job might not exist or was not paused."
                    )

            elif action == "reschedule":
                # Build update dict
                updates = {}

                if cron:
                    updates["cron"] = cron
                if interval is not None:
                    updates["interval"] = interval
                if repeat is not None:
                    updates["repeat"] = repeat
                if next_run:
                    try:
                        # Parse ISO format
                        next_run_dt = datetime.datetime.fromisoformat(
                            next_run.replace("Z", "+00:00")
                        )
                        # Convert to UTC timestamp
                        updates["scheduled_timestamp_utc"] = next_run_dt.timestamp()
                    except ValueError as e:
                        logger.error(
                            f"Invalid next_run format: {e}. Use ISO format (e.g., '2023-01-01T12:00:00Z')"
                        )
                        raise typer.Exit(code=1)

                result = await q.modify_scheduled_job(job_id, **updates)
                if result:
                    print(f"Job {job_id} rescheduled successfully.")
                else:
                    print(f"Failed to reschedule job {job_id}. Job might not exist.")

        except Exception as e:
            logger.exception(f"Error controlling job {job_id}: {e}")
        finally:
            await close_nats_connection()

    try:
        asyncio.run(_control_job())
    except Exception as e:
        logger.exception(f"Command failed unexpectedly: {e}")
        raise typer.Exit(code=1)


@app.command("list-workers")
def list_workers_command(
    nats_url: Optional[str] = typer.Option(
        DEFAULT_NATS_URL,
        "--nats-url",
        "-u",
        help="URL of the NATS server.",
        envvar="NAQ_NATS_URL",
    ),
    log_level: str = typer.Option(
        "INFO",
        "--log-level",
        "-l",
        help="Set logging level (e.g., DEBUG, INFO, WARNING, ERROR).",
    ),
):
    """
    Lists all currently active workers registered in the system.
    """
    setup_logging(log_level)
    logger.info(f"Listing active workers from NATS at {nats_url}")

    async def _list_workers():
        try:
            workers = await Worker.list_workers(nats_url=nats_url)
            if not workers:
                print("No active workers found.")
                return

            # Sort workers by ID for consistent output
            workers.sort(key=lambda w: w.get("worker_id", ""))

            # Print header
            print(
                f"{'WORKER ID':<45} | {'STATUS':<10} | {'QUEUES':<30} | {'CURRENT JOB':<37} | {'LAST HEARTBEAT':<25}"
            )
            print("-" * 155)

            # Print worker info
            now = time.time()
            for worker in workers:
                worker_id = worker.get("worker_id", "unknown")
                status = worker.get("status", "?")
                queues = ", ".join(worker.get("queues", []))
                current_job = (
                    worker.get("current_job_id", "-")
                    if status == WORKER_STATUS_BUSY
                    else "-"
                )
                last_hb_ts = worker.get("last_heartbeat_utc")
                if last_hb_ts:
                    hb_dt = datetime.datetime.fromtimestamp(last_hb_ts, timezone.utc)
                    hb_str = hb_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
                    # Check if heartbeat is recent
                    if now - last_hb_ts > DEFAULT_WORKER_TTL_SECONDS:
                        hb_str += " (STALE)"
                else:
                    hb_str = "never"

                print(
                    f"{worker_id:<45} | {status:<10} | {queues:<30} | {current_job:<37} | {hb_str:<25}"
                )

            print(f"\nTotal: {len(workers)} active worker(s)")

        except Exception as e:
            logger.exception(f"Error listing workers: {e}")
        finally:
            await close_nats_connection()

    try:
        asyncio.run(_list_workers())
    except Exception as e:
        logger.exception(f"Command failed unexpectedly: {e}")
        raise typer.Exit(code=1)


# --- Dashboard Command (Optional) ---
# try:
# Only define the command if dashboard dependencies are installed


@app.command()
def dashboard(
    host: str = typer.Option(
        "127.0.0.1",
        "--host",
        "-h",
        help="Host to bind the dashboard server to.",
        envvar="NAQ_DASHBOARD_HOST",
    ),
    port: int = typer.Option(
        8080,
        "--port",
        "-p",
        help="Port to run the dashboard server on.",
        envvar="NAQ_DASHBOARD_PORT",
    ),
    log_level: str = typer.Option(
        "INFO",
        "--log-level",
        "-l",
        help="Set logging level for the dashboard server.",
    ),
    # Add NATS URL option if dashboard needs direct NATS access later
    # nats_url: Optional[str] = typer.Option(...)
):
    """
    Starts the NAQ web dashboard (requires 'dashboard' extras).
    """
    setup_logging(log_level)  # Setup naq logging if needed
    logger.info(f"Starting NAQ Dashboard server on http://{host}:{port}")
    logger.info("Ensure NATS server is running and accessible.")

    # Configure uvicorn logging level based on input
    uvicorn_log_level = log_level.lower()

    # Run Sanic app using uvicorn
    uvicorn.run(
        "naq.dashboard.app:app",  # Path to the Sanic app instance
        host=host,
        port=port,
        log_level=uvicorn_log_level,
        reload=False,  # Disable auto-reload for production-like command
        # workers=1 # Can configure workers if needed
    )


# except ImportError:
#     @app.command()
#     def dashboard():
#         """
#         Starts the NAQ web dashboard (requires 'dashboard' extras).
#         """
#         print("Error: Dashboard dependencies not installed.")
#         print("Please run: pip install naq[dashboard]")
#         raise typer.Exit(code=1)


# --- Version Option ---


@app.callback()
def main(
    version: Optional[bool] = typer.Option(
        None,
        "--version",
        callback=version_callback,
        is_eager=True,
        help="Show the application's version and exit.",
    ),
):
    """
    naq CLI entry point.
    """
    pass


# --- Main Execution Guard ---
# (Typer handles this implicitly when run as a script via the entry point)
