"""Worker-related CLI commands for naq."""

import asyncio
from typing import List, Optional

import typer

from ..config import DEFAULT_NATS_URL, DEFAULT_QUEUE_NAME
from ..utils import setup_logging
from ..utils.decorators import timing, log_errors
from ..utils.validation import validate_parameter, ensure_type
from ..utils.nats_helpers import stream_exists
from ..utils.serialization import serialize_with_metadata
from ..worker import Worker
from ..services.worker import WorkerService
from ..services.connection import ConnectionService
from ..services.config import GlobalServiceConfig
from ..service_context import service_context
from .cli_base import BaseCLICommand


class WorkerCommandHandler(BaseCLICommand):
    """Base class for worker command handlers with common functionality."""

    def __init__(self) -> None:
        """Initialize the WorkerCommandHandler."""
        super().__init__("naq.cli.worker")


# Create a Typer instance for worker commands
worker_app = typer.Typer(
    name="worker",
    help="Worker-related commands",
    add_completion=False,
)


@worker_app.command("start")
@timing(threshold_ms=1000)
@log_errors()
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
    handler = WorkerCommandHandler()
    handler.setup_logging(log_level if log_level else "CRITICAL")

    # Validate and convert parameters
    queues = ensure_type(queues, list, "queues", convert=True) or []
    nats_url = ensure_type(nats_url, str, "nats_url")
    concurrency = ensure_type(concurrency, int, "concurrency")
    name = ensure_type(name, str, "name", convert=True) if name is not None else None
    module_paths = (
        ensure_type(module_paths, (list, type(None)), "module_paths", convert=True)
        or []
    )
    log_level = (
        ensure_type(log_level, str, "log_level", convert=True)
        if log_level is not None
        else None
    )

    # Validate parameter constraints
    validate_parameter(concurrency, "concurrency", not_none=True, min_value=1)
    validate_parameter(nats_url, "nats_url", not_none=True)

    # Use structured logging
    handler.structured_logger.info(
        "Starting worker",
        worker_name=name or "default",
        queues=queues if queues else [DEFAULT_QUEUE_NAME],
        nats_url=nats_url,
        concurrency=concurrency,
    )

    # Serialize worker configuration with metadata
    worker_config = {
        "worker_name": name or "default",
        "queues": queues if queues else [DEFAULT_QUEUE_NAME],
        "nats_url": nats_url,
        "concurrency": concurrency,
        "module_paths": module_paths,
        "log_level": log_level,
    }

    # Serialize configuration with metadata for potential persistence or transmission
    serialized_config = serialize_with_metadata(
        worker_config,
        serializer="json",
        metadata={
            "component": "worker_commands",
            "action": "start_worker",
            "timestamp": None,  # Will be set in async context
        },
    )

    handler.structured_logger.debug(
        "Worker configuration serialized", config_size=len(str(serialized_config))
    )

    async def _run_worker():
        # Create global config with NATS URL and custom settings
        config = GlobalServiceConfig()
        config.nats_url = nats_url
        config.custom_settings.update(
            {
                "log_level": log_level,
                "concurrency": concurrency,
                "worker_name": name,
                "module_paths": module_paths,
            }
        )

        try:
            # Use service context for short-lived operation
            async with service_context(
                nats_url=nats_url,
                custom_settings={
                    "log_level": log_level,
                    "concurrency": concurrency,
                    "worker_name": name,
                    "module_paths": module_paths,
                },
                logger_name="naq.cli.worker_commands.start",
            ) as service_manager:
                # Get required services
                worker_service = await service_manager.get_service(
                    "worker", WorkerService
                )
                connection_service = await service_manager.get_service(
                    "connection", ConnectionService
                )

            # Test NATS connection before proceeding
            is_connected = await connection_service.test_connection()
            if not is_connected:
                handler.structured_logger.error(
                    "Failed to establish NATS connection", nats_url=nats_url
                )
                raise typer.Exit(code=1)

            # Check if the required stream exists
            stream_name = "naq_jobs"
            js = await connection_service.get_jetstream()
            stream_available = await stream_exists(js=js, stream_name=stream_name)
            if not stream_available:
                handler.structured_logger.error(
                    "Required JetStream stream not found", stream_name=stream_name
                )
                raise typer.Exit(code=1)

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
            handler.structured_logger.info(
                "Worker interrupted by user. Shutting down.", reason="KeyboardInterrupt"
            )
        except Exception as e:
            handler.structured_logger.error(
                "Worker failed unexpectedly", error=str(e), error_type=type(e).__name__
            )
            raise typer.Exit(code=1)
        finally:
            handler.structured_logger.info("Worker process finished.")
            # Service context automatically handles cleanup

    # Run the async function
    asyncio.run(_run_worker())


@worker_app.command("list")
@timing(threshold_ms=500)
@log_errors()
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
    from rich.table import Table

    from ..config import DEFAULT_WORKER_TTL_SECONDS

    handler = WorkerCommandHandler()
    handler.setup_logging(log_level if log_level else "CRITICAL")

    # Validate and convert parameters
    nats_url = ensure_type(nats_url, str, "nats_url")
    log_level = (
        ensure_type(log_level, str, "log_level", convert=True)
        if log_level is not None
        else None
    )

    # Validate parameter constraints
    validate_parameter(nats_url, "nats_url", not_none=True)

    handler.structured_logger.info("Listing active workers", nats_url=nats_url)

    # Serialize list request with metadata
    list_request = {"nats_url": nats_url, "log_level": log_level}

    # Serialize request with metadata for potential persistence or transmission
    serialized_request = serialize_with_metadata(
        list_request,
        serializer="json",
        metadata={
            "component": "worker_commands",
            "action": "list_workers",
            "timestamp": None,  # Will be set in async context
        },
    )

    handler.structured_logger.debug(
        "List workers request serialized", request_size=len(str(serialized_request))
    )

    async def _list_workers():
        # Create global config with NATS URL and custom settings
        config = GlobalServiceConfig()
        config.nats_url = nats_url
        config.custom_settings.update({"log_level": log_level})

        try:
            # Use service context for short-lived operation
            async with service_context(
                nats_url=nats_url,
                custom_settings={"log_level": log_level},
                logger_name="naq.cli.worker_commands.list",
            ) as service_manager:
                # Get required services
                worker_service = await service_manager.get_service(
                    "worker", WorkerService
                )
                connection_service = await service_manager.get_service(
                    "connection", ConnectionService
                )

            # Test NATS connection before proceeding
            is_connected = await connection_service.test_connection()
            if not is_connected:
                handler.structured_logger.error(
                    "Failed to establish NATS connection", nats_url=nats_url
                )
                raise typer.Exit(code=1)

            # Check if the required stream exists
            stream_name = "naq_jobs"
            js = await connection_service.get_jetstream()
            stream_available = await stream_exists(js=js, stream_name=stream_name)
            if not stream_available:
                handler.structured_logger.error(
                    "Required JetStream stream not found", stream_name=stream_name
                )
                raise typer.Exit(code=1)

            # Use worker service to list workers
            workers = await worker_service.list_workers()
            if not workers:
                handler.console.print("[yellow]No active workers found.[/yellow]")
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

                # Convert status to string if it's an enum
                if hasattr(status, "value"):
                    status = status.value

                # Determine status style
                status_style = "green"
                if status == "busy":
                    status_style = "yellow"
                elif status in ["stopping", "starting"]:
                    status_style = "blue"

                queues = ", ".join(worker.get("queues", []))
                current_job = (
                    worker.get("current_job_id", "-") if status == "busy" else "-"
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
            handler.console.print(table)
            handler.console.print(
                f"\n[bold]Total:[/bold] {len(workers)} active worker(s)"
            )

        except Exception as e:
            handler.structured_logger.error(
                "Error listing workers", error=str(e), error_type=type(e).__name__
            )
            handler.console.print(f"[red]Error listing workers: {str(e)}[/red]")
        # Service context automatically handles cleanup

    # Run the async function
    asyncio.run(_list_workers())
