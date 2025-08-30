"""Job and queue-related CLI commands for naq."""

import asyncio
from typing import List, Optional

import typer
from rich.panel import Panel

from ..settings import DEFAULT_NATS_URL
from ..services.jobs import JobService
from ..services.scheduler import SchedulerService
from ..services.streams import StreamService
from ..services.connection import ConnectionService
from ..services.config import GlobalServiceConfig
from ..service_context import service_context
from ..utils import setup_logging
from ..utils.decorators import log_errors, timing
from ..utils.validation import validate_parameter
from ..utils.serialization import SerializationHelper
from ..utils.nats_helpers import build_subject, stream_exists
from ..exceptions import ValidationError, SerializationError
from ..models.jobs import Job
from .cli_base import BaseCLICommand


class JobCommandHandler(BaseCLICommand):
    """Base class for job command handlers with common functionality."""

    def __init__(self) -> None:
        """Initialize the JobCommandHandler."""
        super().__init__("naq.cli.job")


# Create a Typer instance for job commands
job_app = typer.Typer(
    name="job",
    help="Job and queue management commands",
    add_completion=False,
)


@job_app.command("purge")
def purge(
    queues: List[str] = typer.Argument(..., help="The names of the queues to purge."),
    nats_url: str = typer.Option(
        DEFAULT_NATS_URL,
        "--nats-url",
        "-u",
        help="URL of the NATS server.",
        envvar="NAQ_NATS_URL",
    ),
    log_level: Optional[str] = typer.Option(
        None,  # Set default log level to None to use env var
        "--log-level",
        "-l",
        help=(
            "Set logging level (e.g., DEBUG, INFO, WARNING, ERROR). "
            "Defaults to NAQ_LOG_LEVEL env var or CRITICAL."
        ),
    ),
) -> None:
    """
    Removes all jobs from the specified queues.
    """
    handler = JobCommandHandler()
    handler.setup_logging(log_level if log_level else "INFO")

    @log_errors()
    @timing()
    async def _purge_queues():
        # Create global config with NATS URL and custom settings
        config = GlobalServiceConfig()
        config.nats_url = nats_url
        config.custom_settings.update({"log_level": log_level})

        try:
            # Use service context for short-lived operation
            async with service_context(
                nats_url=nats_url,
                custom_settings={"log_level": log_level},
                logger_name="naq.cli.job_commands.purge",
            ) as service_manager:
                # Get required services
                await service_manager.get_service("job", JobService)
                stream_service = await service_manager.get_service(
                    "stream", StreamService
                )
                connection_service = await service_manager.get_service(
                    "connection", ConnectionService
                )

            handler.structured_logger.info(
                f"Attempting to purge queues: {queues}",
                operation="purge_queues",
                queues=queues,
                nats_url=nats_url,
            )

            # Use services to purge queues
            from ..settings import NAQ_PREFIX

            results = {}
            total_purged = 0
            for queue_name in queues:
                try:
                    # Validate queue name
                    validate_parameter(
                        queue_name,
                        "queue_name",
                        not_none=True,
                        regex_pattern=r"^[a-zA-Z0-9_-]+$",
                    )

                    # Use JetStream context from the context manager
                    stream_name = build_subject(NAQ_PREFIX, "queue", queue_name)

                    # Check if stream exists before purging
                    js = await connection_service.get_jetstream()
                    if await stream_exists(js, stream_name=stream_name):
                        # Purge the stream
                        await stream_service.purge_stream(stream_name)
                        purged_count = 0  # NATS doesn't return count for purge

                        results[queue_name] = {
                            "status": "success",
                            "count": purged_count,
                        }
                        total_purged += purged_count

                        handler.structured_logger.info(
                            f"Successfully purged queue: {queue_name}",
                            operation="purge_queue",
                            queue_name=queue_name,
                            stream_name=stream_name,
                            status="success",
                        )
                    else:
                        results[queue_name] = {
                            "status": "error",
                            "error": f"Stream '{stream_name}' does not exist",
                        }
                        handler.structured_logger.warning(
                            f"Stream does not exist for queue: {queue_name}",
                            operation="purge_queue",
                            queue_name=queue_name,
                            stream_name=stream_name,
                            status="stream_not_found",
                        )
                except Exception as e:
                    results[queue_name] = {"status": "error", "error": str(e)}
                    handler.structured_logger.error(
                        f"Failed to purge queue '{queue_name}': {e}",
                        operation="purge_queue",
                        queue_name=queue_name,
                        error=str(e),
                        error_type=type(e).__name__,
                    )

            # --- Report Results using Rich ---
            success_count = sum(1 for r in results.values() if r["status"] == "success")
            error_count = len(results) - success_count

            handler.console.print("\n[bold]Purge Results:[/bold]")
            for name, result in results.items():
                if result["status"] == "success":
                    handler.console.print(
                        f"  - [green]Queue '{name}': "
                        f"Purged {result['count']} jobs.[/green]"
                    )
                else:
                    handler.console.print(
                        f"  - [red]Queue '{name}': Failed - {result['error']}[/red]"
                    )

            # --- Summary Panel ---
            summary_color = (
                "green"
                if error_count == 0
                else ("yellow" if success_count > 0 else "red")
            )
            summary_text = (
                f"Total jobs removed: {total_purged}\n"
                f"Queues processed: {len(results)}\n"
                f"Successful purges: {success_count}\n"
                f"Failed purges: {error_count}"
            )
            handler.console.print(
                Panel(
                    summary_text,
                    title="Purge Summary",
                    style=summary_color,
                    expand=False,
                )
            )
            # --- End Reporting ---

        except Exception as e:
            handler.structured_logger.error(
                f"Failed to purge queues: {e}",
                operation="purge_queues",
                error=str(e),
                error_type=type(e).__name__,
            )
            handler.console.print(f"[red]Error: {str(e)}[/red]")
        # Service context automatically handles cleanup

    # Run the async function
    asyncio.run(_purge_queues())


@job_app.command("control")
def job_control(
    job_id: str = typer.Argument(..., help="The ID of the scheduled job to control"),
    action: str = typer.Argument(
        ...,
        help="Action to perform: 'cancel', 'pause', 'resume', or 'reschedule'",
        show_choices=True,
    ),
    nats_url: str = typer.Option(
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
        help="Next run time (ISO format, e.g. '2023-01-01T12:00:00Z') "
        "for reschedule action",
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
    Controls scheduled jobs: cancel, pause, resume, or modify scheduling parameters.
    """
    import datetime
    from rich.text import Text

    handler = JobCommandHandler()
    handler.setup_logging(log_level if log_level else "INFO")

    # Validate action
    if action not in ["cancel", "pause", "resume", "reschedule"]:
        error_msg = f"Invalid action '{action}'. Must be one of: cancel, pause, resume, reschedule"
        handler.structured_logger.error(
            error_msg,
            operation="job_control",
            action=action,
            error_type="invalid_action",
        )
        typer.echo(f"Error: {error_msg}", err=True)
        raise typer.Exit(code=1)

    # Validate parameters for reschedule
    if action == "reschedule":
        if not any([cron, interval, repeat, next_run]):
            error_msg = "Reschedule action requires at least one scheduling parameter: --cron, --interval, --repeat, or --next-run"
            handler.structured_logger.error(
                error_msg,
                operation="job_control",
                action=action,
                error_type="missing_parameters",
            )
            typer.echo(f"Error: {error_msg}", err=True)
            raise typer.Exit(code=1)
        if cron and interval:
            handler.structured_logger.error(
                "Cannot specify both --cron and --interval. "
                "Choose one scheduling method.",
                operation="job_control",
                action=action,
                error_type="conflicting_parameters",
            )
            raise typer.Exit(code=1)

    @log_errors()
    @timing()
    async def _control_job():
        # Create global config with NATS URL and custom settings
        config = GlobalServiceConfig()
        config.nats_url = nats_url
        config.custom_settings.update({"log_level": log_level})

        try:
            # Use service context for short-lived operation
            async with service_context(
                nats_url=nats_url,
                custom_settings={"log_level": log_level},
                logger_name="naq.cli.job_commands.control",
            ) as service_manager:
                # Get required services
                scheduler_service = await service_manager.get_service(
                    "scheduler", SchedulerService
                )

            # Validate job_id
            validate_parameter(
                job_id, "job_id", not_none=True, regex_pattern=r"^[a-zA-Z0-9_-]+$"
            )

            handler.structured_logger.info(
                f"Performing {action} on job {job_id}",
                operation="job_control",
                action=action,
                job_id=job_id,
            )

            try:
                if action == "cancel":
                    result = await scheduler_service.cancel_scheduled_job(job_id)
                    if result:
                        handler.console.print(
                            f"[green]Job {job_id} cancelled successfully.[/green]"
                        )
                        handler.structured_logger.info(
                            f"Job {job_id} cancelled successfully",
                            operation="job_control",
                            action=action,
                            job_id=job_id,
                            status="success",
                        )
                    else:
                        handler.console.print(
                            f"[yellow]Job {job_id} not found or "
                            "already cancelled.[/yellow]"
                        )
                        handler.structured_logger.warning(
                            f"Job {job_id} not found or already cancelled",
                            operation="job_control",
                            action=action,
                            job_id=job_id,
                            status="not_found",
                        )

                elif action == "pause":
                    result = await scheduler_service.pause_scheduled_job(job_id)
                    if result:
                        handler.console.print(
                            f"[green]Job {job_id} paused successfully.[/green]"
                        )
                        handler.structured_logger.info(
                            f"Job {job_id} paused successfully",
                            operation="job_control",
                            action=action,
                            job_id=job_id,
                            status="success",
                        )
                    else:
                        handler.console.print(
                            f"[yellow]Failed to pause job {job_id}. "
                            "Job might not exist or was already paused.[/yellow]"
                        )
                        handler.structured_logger.warning(
                            f"Failed to pause job {job_id}",
                            operation="job_control",
                            action=action,
                            job_id=job_id,
                            status="failed",
                        )

                elif action == "resume":
                    result = await scheduler_service.resume_scheduled_job(job_id)
                    if result:
                        handler.console.print(
                            f"[green]Job {job_id} resumed successfully.[/green]"
                        )
                        handler.structured_logger.info(
                            f"Job {job_id} resumed successfully",
                            operation="job_control",
                            action=action,
                            job_id=job_id,
                            status="success",
                        )
                    else:
                        handler.console.print(
                            f"[yellow]Failed to resume job {job_id}. "
                            "Job might not exist or was not paused.[/yellow]"
                        )
                        handler.structured_logger.warning(
                            f"Failed to resume job {job_id}",
                            operation="job_control",
                            action=action,
                            job_id=job_id,
                            status="failed",
                        )

                elif action == "reschedule":
                    # For reschedule, we need to get the current job, cancel it,
                    # and reschedule
                    current_job = await scheduler_service.get_scheduled_job(job_id)
                    if current_job is None:
                        handler.console.print(
                            f"[yellow]Job {job_id} not found.[/yellow]"
                        )
                        handler.structured_logger.warning(
                            f"Job {job_id} not found for reschedule",
                            operation="job_control",
                            action=action,
                            job_id=job_id,
                            status="not_found",
                        )
                        return

                    # Cancel the current job
                    await scheduler_service.cancel_scheduled_job(job_id)

                    # Create new job with updated parameters
                    # Deserialize the original job using SerializationHelper
                    try:
                        original_job_data = SerializationHelper.safe_deserialize(
                            current_job._orig_job_payload, serializer="pickle"
                        )
                        new_job = Job.deserialize(original_job_data)
                    except SerializationError as e:
                        handler.structured_logger.error(
                            f"Failed to deserialize job {job_id}: {e}",
                            operation="job_control",
                            action=action,
                            job_id=job_id,
                            error=str(e),
                            error_type="deserialization_error",
                        )
                        handler.console.print(
                            f"[red]Error deserializing job: {str(e)}[/red]"
                        )
                        return

                    # Calculate new scheduled timestamp
                    scheduled_timestamp = current_job.scheduled_timestamp_utc
                    if next_run:
                        try:
                            # Validate next_run format
                            validate_parameter(
                                next_run,
                                "next_run",
                                not_none=True,
                                regex_pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?$",
                            )

                            next_run_dt = datetime.datetime.fromisoformat(
                                next_run.replace("Z", "+00:00")
                            )
                            scheduled_timestamp = next_run_dt.timestamp()
                        except (ValueError, ValidationError) as e:
                            handler.structured_logger.error(
                                f"Invalid next_run format: {e}. Use ISO format "
                                "(e.g., '2023-01-01T12:00:00Z')",
                                operation="job_control",
                                action=action,
                                job_id=job_id,
                                next_run=next_run,
                                error=str(e),
                                error_type="invalid_format",
                            )
                            raise typer.Exit(code=1)

                    # Reschedule with new parameters
                    await scheduler_service.schedule_job(
                        job=new_job,
                        scheduled_timestamp=scheduled_timestamp,
                        cron=cron or current_job.cron,
                        interval_seconds=interval or current_job.interval_seconds,
                        repeat=repeat or current_job.repeat,
                    )

                    handler.console.print(
                        f"[green]Job {job_id} rescheduled successfully.[/green]"
                    )
                    handler.structured_logger.info(
                        f"Job {job_id} rescheduled successfully",
                        operation="job_control",
                        action=action,
                        job_id=job_id,
                        status="success",
                    )
                    change_summary = []
                    if cron:
                        change_summary.append(f"cron='{cron}'")
                    if interval is not None:
                        change_summary.append(f"interval={interval}s")
                    if repeat is not None:
                        change_summary.append(f"repeat={repeat}")
                    if next_run:
                        change_summary.append(f"next_run={next_run}")

                    if change_summary:
                        handler.console.print(
                            Panel(
                                Text(
                                    "\n".join(
                                        f"• {change}" for change in change_summary
                                    )
                                ),
                                title="Applied Changes",
                                expand=False,
                            )
                        )
            except Exception as e:
                handler.structured_logger.error(
                    f"Error controlling job {job_id}: {e}",
                    operation="job_control",
                    action=action,
                    job_id=job_id,
                    error=str(e),
                    error_type=type(e).__name__,
                )
                handler.console.print(f"[red]Error: {str(e)}[/red]")

        except Exception as e:
            handler.structured_logger.error(
                f"Failed to control job: {e}",
                operation="job_control",
                action=action,
                job_id=job_id,
                error=str(e),
                error_type=type(e).__name__,
            )
            handler.console.print(f"[red]Error: {str(e)}[/red]")
        # Service context automatically handles cleanup

    # Run the async function
    asyncio.run(_control_job())
