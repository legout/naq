"""Worker-related CLI commands for naq."""

import asyncio
from typing import List, Optional

import typer
from loguru import logger

from ..settings import DEFAULT_NATS_URL, DEFAULT_QUEUE_NAME
from ..utils import setup_logging
from ..worker import Worker
from ..services import ServiceManager, WorkerService, ServiceConfig
from ..services.config import GlobalServiceConfig
from ..connection.context_managers import nats_connection

# Create a Typer instance for worker commands
worker_app = typer.Typer(
    name="worker",
    help="Worker-related commands",
    add_completion=False,
)


@worker_app.command("start")
def start_worker(
    queues: List[str] = typer.Argument(
        default=None,
        help=(
            "The names of the queues to listen to. "
            "Defaults to the configured default queue."
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
            "Additional paths to add to sys.path for module imports. "
            "Can be specified multiple times."
        ),
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

    async def _run_worker():
        # Create global config with NATS URL and custom settings
        config = GlobalServiceConfig()
        config.nats_url = nats_url
        config.custom_settings.update({
            "log_level": log_level,
            "concurrency": concurrency,
            "worker_name": name,
            "module_paths": module_paths,
        })
        
        try:
            # Use the new context manager for NATS connection
            async with nats_connection(config) as nc:
                # Create service manager with configuration
                service_manager = ServiceManager(
                    config=ServiceConfig(
                        nats_url=nats_url,
                        custom_settings={
                            "log_level": log_level,
                            "concurrency": concurrency,
                            "worker_name": name,
                            "module_paths": module_paths,
                        },
                    )
                )

                # Register required services
                worker_service = await service_manager.register_service(
                    "worker", WorkerService, initialize=True
                )

                # Create and run worker
                w = Worker(
                    queues=queues,
                    nats_url=nats_url,
                    concurrency=concurrency,
                    worker_name=name,
                    module_paths=module_paths,
                    connection_service=None,  # Not needed with context manager
                    worker_service=worker_service,
                )
                
                # Register the worker with the service
                await worker_service.register_worker(w)
                
                await w.run()

        except KeyboardInterrupt:
            logger.info(
                "Worker interrupted by user (KeyboardInterrupt). Shutting down."
            )
        except Exception as e:
            logger.exception(
                f"Worker failed unexpectedly: {e}"
            )  # Use logger.exception for stack trace
            raise typer.Exit(code=1)
        finally:
            logger.info("Worker process finished.")
            if "service_manager" in locals():
                await service_manager.cleanup_all()

    # Run the async function
    asyncio.run(_run_worker())


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
        help=(
            "Set logging level (e.g., DEBUG, INFO, WARNING, ERROR). "
            "Defaults to NAQ_LOG_LEVEL env var or CRITICAL."
        ),
    ),
) -> None:
    """
    Lists all currently active workers registered in the system.
    """
    import datetime
    import time
    from datetime import timezone
    from rich.console import Console
    from rich.table import Table

    from ..settings import DEFAULT_WORKER_TTL_SECONDS, WORKER_STATUS

    setup_logging(log_level if log_level else None)
    logger.info(f"Listing active workers from NATS at {nats_url}")

    console = Console()

    async def _list_workers():
        # Create global config with NATS URL and custom settings
        config = GlobalServiceConfig()
        config.nats_url = nats_url
        config.custom_settings.update({"log_level": log_level})
        
        try:
            # Use the new context manager for NATS connection
            async with nats_connection(config) as nc:
                # Create service manager with configuration
                service_manager = ServiceManager(
                    config=ServiceConfig(
                        nats_url=nats_url, custom_settings={"log_level": log_level}
                    )
                )

                # Register required services
                worker_service = await service_manager.register_service(
                    "worker", WorkerService, initialize=True
                )

                # Use worker service to list workers
                workers = await worker_service.list_workers()
            if not workers:
                console.print("[yellow]No active workers found.[/yellow]")
                return

            # Sort workers by ID for consistent output
            workers.sort(key=lambda w: w.get("worker_id", ""))

            table = Table(
                title="NAQ Workers", show_header=True, header_style="bold cyan"
            )

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
            if "service_manager" in locals():
                await service_manager.cleanup_all()

    # Run the async function
    asyncio.run(_list_workers())
