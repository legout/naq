"""
Worker CLI commands.

This module contains all CLI commands related to worker management.
"""

import asyncio
import datetime
import time
from datetime import timezone
from typing import List, Optional

import typer
from loguru import logger
from rich.console import Console
from rich.table import Table

from ..connection import close_nats_connection
from ..settings import (
    DEFAULT_NATS_URL,
    DEFAULT_QUEUE_NAME,
    DEFAULT_WORKER_TTL_SECONDS,
    WORKER_STATUS,
)
from ..utils import setup_logging
from ..worker import Worker

worker_app = typer.Typer(help="Worker management commands")
console = Console()


@worker_app.command("start")
def worker(
    queues: List[str] = typer.Argument(
        default=None,
        help=(
            "The names of the queues to listen to. Defaults to the configured "
            "default queue."
        ),
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
        help=(
            "Additional paths to add to sys.path for module imports. Can be "
            "specified multiple times."
        ),
    ),
    log_level: Optional[str] = typer.Option(
        None,
        "--log-level",
        "-l",
        help=(
            "Set logging level (e.g., DEBUG, INFO, WARNING, ERROR). Defaults to "
            "NAQ_LOG_LEVEL env var or CRITICAL."
        ),
    ),
):
    """
    Starts a naq worker process to listen for and execute jobs on the specified queues.
    """
    setup_logging(log_level if log_level else None)

    # If no queues are provided, let the Worker class handle the default
    if queues is None:
        queues = []

    # Use loguru directly
    logger.info(
        f"Starting worker '{name or 'default'}' for queues: "
        f"{queues if queues else [DEFAULT_QUEUE_NAME]}"
    )
    logger.info(f"NATS URL: {nats_url}")
    logger.info(f"Concurrency: {concurrency}")

    w = Worker(
        queues=queues,
        nats_url=nats_url,
        concurrency=concurrency,
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
def list_workers_command(
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
        help=(
            "Set logging level (e.g., DEBUG, INFO, WARNING, ERROR). Defaults to "
            "NAQ_LOG_LEVEL env var or CRITICAL."
        ),
    ),
):
    """
    Lists all currently active workers registered in the system.
    """
    setup_logging(log_level if log_level else None)
    logger.info(f"Listing active workers from NATS at {nats_url}")

    try:
        # Use synchronous interface to list workers
        workers = Worker.list_workers_sync(nats_url=nats_url)
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
    finally:
        # Close any NATS connection if opened by the sync wrapper
        try:
            asyncio.run(close_nats_connection())
        except Exception:
            pass
