# src/naq/cli/worker_commands.py
"""
Worker management commands for the NAQ CLI.

This module contains commands for starting workers, listing active workers,
and monitoring worker events.
"""

import asyncio
import datetime
import time
from datetime import timezone
from typing import List, Optional

import typer
from loguru import logger

from .main import console
from ..settings import (
    DEFAULT_NATS_URL,
    DEFAULT_QUEUE_NAME,
    DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_WORKER_TTL_SECONDS,
    WORKER_STATUS,
)
from ..utils import setup_logging
from ..worker import Worker


# Create worker command group
worker_app = typer.Typer(help="Worker management commands")


@worker_app.command()
def start(
    queues: List[str] = typer.Argument(
        default=None,
        help="The names of the queues to listen to. Defaults to the configured default queue.",
    ),
    nats_url: str = typer.Option(
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
    module_paths: Optional[List[str]] = typer.Option(
        None,
        "--module-path",
        "-m",
        help="Additional paths to add to sys.path for module imports. Can be specified multiple times.",
    ),
    log_level: Optional[str] = typer.Option(
        None,
        "--log-level",
        "-l",
        help="Set logging level (e.g., DEBUG, INFO, WARNING, ERROR). Defaults to NAQ_LOG_LEVEL env var or CRITICAL.",
    ),
):
    """
    Start a NAQ worker process to listen for and execute jobs on the specified queues.
    """
    setup_logging(log_level if log_level else None)

    # If no queues are provided, let the Worker class handle the default
    if queues is None:
        queues = []

    # Use loguru directly
    logger.info(
        f"Starting worker '{name or 'default'}' for queues: {queues if queues else [DEFAULT_QUEUE_NAME]}"
    )
    logger.info(f"NATS URL: {nats_url}")
    logger.info(f"Concurrency: {concurrency}")

    # Create configuration
    config = {
        'nats_url': nats_url,
        'concurrency': concurrency,
        'heartbeat_interval': DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS,
        'worker_ttl': DEFAULT_WORKER_TTL_SECONDS,
    }

    w = Worker(
        queues=queues,
        config=config,
        worker_name=name,
        module_paths=module_paths,
    )
    try:
        # Use synchronous interface backed by AnyIO BlockingPortal
        w.run_sync()
    except KeyboardInterrupt:
        logger.info("Worker interrupted by user (KeyboardInterrupt). Shutting down.")
    except Exception as e:
        logger.exception(
            f"Worker failed unexpectedly: {e}"
        )  # Use logger.exception for stack trace
        raise typer.Exit(code=1)
    finally:
        logger.info("Worker process finished.")


@worker_app.command("list")
def list_workers(
    nats_url: str = typer.Option(
        DEFAULT_NATS_URL,
        "--nats-url",
        "-u",
        help="URL of the NATS server.",
        envvar="NAQ_NATS_URL",
    ),
    log_level: Optional[str] = typer.Option(
        None,
        "--log-level",
        "-l",
        help="Set logging level (e.g., DEBUG, INFO, WARNING, ERROR). Defaults to NAQ_LOG_LEVEL env var or CRITICAL.",
    ),
):
    """
    List all currently active workers registered in the system.
    """
    from rich.table import Table
    
    setup_logging(log_level if log_level else None)
    logger.info(f"Listing active workers from NATS at {nats_url}")

    async def _list_workers_async():
        from ..worker.status import WorkerStatusManager
        return await WorkerStatusManager.list_workers(nats_url=nats_url)
    
    try:
        # Use async interface to list workers
        workers = asyncio.run(_list_workers_async())
        if not workers:
            console.print("[yellow]No active workers found.[/yellow]")
            return

        # Sort workers by ID for consistent output
        workers.sort(key=lambda w: w.get("worker_id", ""))

        table = Table(title="NAQ Workers", show_header=True, header_style="bold cyan")

        # Add columns
        table.add_column("WORKER ID", style="dim", width=45)
        table.add_column("STATUS", width=10)
        table.add_column("QUEUES", width=30)
        table.add_column("CURRENT JOB", width=37)
        table.add_column("LAST HEARTBEAT", width=25)

        # Add rows to the table
        now = time.time()
        for worker in workers:
            worker_id = worker.get("worker_id", "unknown")
            status = worker.get("status", "?")

            # Determine status style
            status_style = "green"
            if status == "busy":
                status_style = "yellow"
            elif status in ["stopping", "starting"]:
                status_style = "blue"

            queues = ", ".join(worker.get("queues", []))
            current_job = (
                worker.get("current_job_id", "-")
                if status == WORKER_STATUS.BUSY
                else "-"
            )

            # Format last heartbeat
            last_hb_ts = worker.get("last_heartbeat_utc")
            if last_hb_ts:
                hb_dt = datetime.datetime.fromtimestamp(last_hb_ts, timezone.utc)
                hb_str = hb_dt.strftime("%Y-%m-%d %H:%M:%S UTC")

                # Check if heartbeat is stale
                if now - last_hb_ts > DEFAULT_WORKER_TTL_SECONDS:
                    hb_str = f"[red]{hb_str} (STALE)[/red]"
            else:
                hb_str = "[italic]never[/italic]"

            # Add row to table
            table.add_row(
                worker_id,
                f"[{status_style}]{status}[/{status_style}]",
                queues,
                current_job,
                hb_str,
            )

        # Print the table
        console.print(table)
        console.print(f"\n[bold]Total:[/bold] {len(workers)} active worker(s)")

    except Exception as e:
        logger.exception(f"Error listing workers: {e}")
        console.print(f"[red]Error listing workers: {str(e)}[/red]")
    # No cleanup needed for service-based approach


@worker_app.command("events") 
def worker_events(
    nats_url: str = typer.Option(
        DEFAULT_NATS_URL,
        "--nats-url", 
        "-u",
        help="URL of the NATS server.",
        envvar="NAQ_NATS_URL",
    ),
    worker_id: Optional[str] = typer.Option(
        None,
        "--worker-id",
        "-w",
        help="Filter events for a specific worker.",
    ),
    format_output: str = typer.Option(
        "table",
        "--format",
        "-f", 
        help="Output format: table, json, or raw.",
    ),
):
    """
    Monitor worker lifecycle events in real-time.
    
    This command shows worker start/stop, status changes, and heartbeat events.
    """
    import json
    from rich.table import Table
    from rich.panel import Panel
    
    from ..events.processor import AsyncJobEventProcessor
    from ..models import JobEventType, WorkerEventType
    
    async def run_worker_events_monitor():
        """Run the worker events monitor."""
        processor = AsyncJobEventProcessor(nats_url=nats_url)
        
        # Create a handler to display worker events
        def display_worker_event(event):
            # Filter for worker-related events
            if not (hasattr(event, 'details') and event.details and 
                   event.details.get('event_category') == 'worker'):
                return
                
            if format_output == "json":
                console.print(json.dumps(event.to_dict(), indent=2))
            elif format_output == "raw":
                console.print(f"{event.timestamp:.2f} {event.worker_id} {event.event_type.value} {event.message or ''}")
            else:  # table format
                table = Table(show_header=True, header_style="bold cyan")
                table.add_column("Time", style="dim")
                table.add_column("Worker ID", style="cyan")
                table.add_column("Event", style="green")
                table.add_column("Hostname", style="blue")
                table.add_column("Status", style="yellow")
                table.add_column("Message")
                
                # Format timestamp
                dt = datetime.datetime.fromtimestamp(event.timestamp, tz=timezone.utc)
                time_str = dt.strftime("%H:%M:%S.%f")[:-3]
                
                # Extract worker details
                hostname = event.details.get('hostname', '-') if event.details else '-'
                status_info = ""
                if event.details:
                    if event.details.get('active_jobs') is not None:
                        active = event.details.get('active_jobs', 0)
                        limit = event.details.get('concurrency_limit', 0)
                        status_info = f"{active}/{limit} jobs"
                
                table.add_row(
                    time_str,
                    event.worker_id[:12] + "..." if len(event.worker_id) > 15 else event.worker_id,
                    event.event_type.value,
                    hostname,
                    status_info,
                    event.message or "-"
                )
                console.print(table)
        
        # Register the display handler
        processor.add_global_handler(display_worker_event)
        
        try:
            # Start the processor
            await processor.start()
            
            console.print(Panel(
                "[bold cyan]NAQ Worker Events Monitor Started[/bold cyan]\n"
                f"NATS URL: {nats_url}\n"
                f"Worker Filter: {worker_id or 'all'}\n"
                f"Format: {format_output}\n\n"
                "[dim]Press Ctrl+C to stop[/dim]",
                title="Worker Events Monitor",
                border_style="cyan"
            ))
            
            # Stream all events and filter for worker events in the handler
            async for event in processor.stream_job_events():
                # The display_worker_event handler will filter and display relevant events
                pass
                
        except KeyboardInterrupt:
            console.print("\n[yellow]Worker event monitoring stopped by user[/yellow]")
        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")
            raise typer.Exit(code=1)
        finally:
            await processor.stop()
    
    # Run the async function
    asyncio.run(run_worker_events_monitor())


# Backward compatibility alias for tests
worker_commands = worker_app