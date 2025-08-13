import typer
from loguru import logger
from typing import List, Optional, cast

from rich.console import Console

# Create a shared console instance
console = Console()
from ..settings import DEFAULT_NATS_URL
from ..utils import setup_logging
from ..services import ServiceManager, ConnectionService

# Create job app
job_app = typer.Typer(help="Job control and queue management commands")


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
        help="Set logging level (e.g., DEBUG, INFO, WARNING, ERROR). Defaults to NAQ_LOG_LEVEL env var or CRITICAL.",
    ),
):
    """
    Removes all jobs from the specified queues.
    """
    setup_logging(log_level if log_level else None)
    logger.info(f"Attempting to purge queues: {queues}")
    logger.info(f"Using NATS URL: {nats_url}")

    async def _purge_queues_async():
        config = {
            'nats': {
                'url': nats_url,
            }
        }
        
        try:
            async with ServiceManager(config) as services:
                connection_service = cast(ConnectionService, await services.get_service(ConnectionService))
                
                # Import async purge function
                from ..queue import purge_queue
                
                results = {}
                total_purged = 0
                for queue_name in queues:
                    try:
                        # Use connection service to get connection
                        await connection_service.get_connection(nats_url)
                        purged_count = await purge_queue(queue_name=queue_name, nats_url=nats_url)
                        results[queue_name] = {"status": "success", "count": purged_count}
                        total_purged += purged_count
                    except Exception as e:
                        results[queue_name] = {"status": "error", "error": str(e)}
                        logger.error(f"Failed to purge queue '{queue_name}': {e}")
                
                return results
                
        except Exception as e:
            logger.exception(f"Error setting up service manager: {e}")
            console.print(f"[red]Error setting up services: {str(e)}[/red]")
            return {}

    # Run the async routine
    import asyncio
    results = asyncio.run(_purge_queues_async())
    
    # Calculate total purged from results
    total_purged = sum(r.get("count", 0) for r in results.values() if r.get("status") == "success")

    # --- Report Results using Rich ---
    success_count = sum(1 for r in results.values() if r["status"] == "success")
    error_count = len(results) - success_count

    console.print("\n[bold]Purge Results:[/bold]")
    for name, result in results.items():
        if result["status"] == "success":
            console.print(
                f"  - [green]Queue '{name}': Purged {result['count']} jobs.[/green]"
            )
        else:
            console.print(f"  - [red]Queue '{name}': Failed - {result['error']}[/red]")

    # --- Summary Panel ---
    from rich.panel import Panel

    summary_color = (
        "green" if error_count == 0 else ("yellow" if success_count > 0 else "red")
    )
    summary_text = f"Total jobs removed: {total_purged}\nQueues processed: {len(results)}\nSuccessful purges: {success_count}\nFailed purges: {error_count}"
    console.print(
        Panel(summary_text, title="Purge Summary", style=summary_color, expand=False)
    )
    # --- End Reporting ---


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
        help="Next run time (ISO format, e.g. '2023-01-01T12:00:00Z') for reschedule action",
    ),
    log_level: Optional[str] = typer.Option(
        None,
        "--log-level",
        "-l",
        help="Set logging level (e.g., DEBUG, INFO, WARNING, ERROR). Defaults to NAQ_LOG_LEVEL env var or CRITICAL.",
    ),
):
    """
    Controls scheduled jobs: cancel, pause, resume, or modify scheduling parameters.
    """
    setup_logging(log_level if log_level else None)

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

    async def _control_job_async():
        config = {
            'nats': {
                'url': nats_url,
            }
        }
        
        try:
            async with ServiceManager(config) as services:
                connection_service = cast(ConnectionService, await services.get_service(ConnectionService))
                
                # Import async job control functions
                from ..queue import (
                    cancel_scheduled_job,
                    pause_scheduled_job,
                    resume_scheduled_job,
                    modify_scheduled_job,
                )

                # Use connection service to get connection
                await connection_service.get_connection(nats_url)
                
                if action == "cancel":
                    result = await cancel_scheduled_job(job_id, nats_url=nats_url)
                    if result:
                        console.print(f"[green]Job {job_id} cancelled successfully.[/green]")
                    else:
                        console.print(
                            f"[yellow]Job {job_id} not found or already cancelled.[/yellow]"
                        )

                elif action == "pause":
                    result = await pause_scheduled_job(job_id, nats_url=nats_url)
                    if result:
                        console.print(f"[green]Job {job_id} paused successfully.[/green]")
                    else:
                        console.print(
                            f"[yellow]Failed to pause job {job_id}. Job might not exist or was already paused.[/yellow]"
                        )

                elif action == "resume":
                    result = await resume_scheduled_job(job_id, nats_url=nats_url)
                    if result:
                        console.print(f"[green]Job {job_id} resumed successfully.[/green]")
                    else:
                        console.print(
                            f"[yellow]Failed to resume job {job_id}. Job might not exist or was not paused.[/yellow]"
                        )

                elif action == "reschedule":
                    updates = {}
                    if cron:
                        updates["cron"] = cron
                    if interval is not None:
                        updates["interval"] = interval
                    if repeat is not None:
                        updates["repeat"] = repeat
                    if next_run:
                        try:
                            import datetime
                            next_run_dt = datetime.datetime.fromisoformat(
                                next_run.replace("Z", "+00:00")
                            )
                            updates["scheduled_timestamp_utc"] = next_run_dt.timestamp()
                        except ValueError as e:
                            logger.error(
                                f"Invalid next_run format: {e}. Use ISO format (e.g., '2023-01-01T12:00:00Z')"
                            )
                            raise typer.Exit(code=1)

                    result = await modify_scheduled_job(job_id, nats_url=nats_url, **updates)
                    if result:
                        console.print(f"[green]Job {job_id} rescheduled successfully.[/green]")

                        from rich.panel import Panel
                        from rich.text import Text

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
                            console.print(
                                Panel(
                                    Text("\n".join(f"• {change}" for change in change_summary)),
                                    title="Applied Changes",
                                    expand=False,
                                )
                            )
                    else:
                        console.print(
                            f"[yellow]Failed to reschedule job {job_id}. Job might not exist.[/yellow]"
                        )
                    
        except Exception as e:
            logger.exception(f"Error controlling job {job_id}: {e}")
            console.print(f"[red]Error: {str(e)}[/red]")

    # Run the async routine
    import asyncio
    asyncio.run(_control_job_async())