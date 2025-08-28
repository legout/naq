"""Event monitoring CLI commands for naq."""

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, Union

import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.live import Live
from rich.layout import Layout

from ..settings import DEFAULT_NATS_URL
from ..services.base import ServiceManager, ServiceConfig
from ..services.events import EventService
from ..services.connection import ConnectionService
from ..services.worker import WorkerService
from ..services.config import GlobalServiceConfig
from ..service_context import service_context
from ..utils import setup_logging
from ..utils.validation import validate_parameter, ensure_type
from ..utils.nats_helpers import build_subject, stream_exists
from ..utils.decorators import timing, log_errors
from ..utils.logging import StructuredLogger
from ..utils.serialization import SerializationHelper
from ..exceptions import NaqConnectionError
from ..models.events import JobEvent, WorkerEvent
from ..models.enums import JobEventType


class EventCommandHandler:
    """Base class for event command handlers with common functionality."""

    def __init__(self) -> None:
        """Initialize the EventCommandHandler."""
        self.console = Console()
        self.structured_logger = StructuredLogger("naq.events")
        self.service_manager: Optional[ServiceManager] = None
        self.event_service: Optional[EventService] = None

    def validate_common_parameters(
        self,
        nats_url: str,
        log_level: Optional[str] = None,
        limit: Optional[int] = None,
        worker_id: Optional[str] = None,
    ) -> None:
        """Validate common parameters used across event commands.

        Args:
            nats_url: URL of the NATS server.
            log_level: Logging level (e.g., DEBUG, INFO, WARNING, ERROR).
            limit: Maximum number of events to display.
            worker_id: ID of the worker to monitor events for.

        Raises:
            ValidationError: If any parameter validation fails.
        """
        # Validate nats_url
        validate_parameter(
            nats_url,
            "nats_url",
            not_none=True,
            regex_pattern=r"^(nats://)?[a-zA-Z0-9][a-zA-Z0-9.-]*[a-zA-Z0-9](:[0-9]+)?(,[a-zA-Z0-9][a-zA-Z0-9.-]*[a-zA-Z0-9](:[0-9]+)?)*$|^(nats://)?[a-zA-Z0-9](:[0-9]+)?$",
            error_message="Invalid NATS URL format",
        )

        # Validate log_level if provided
        if log_level is not None:
            validate_parameter(
                log_level,
                "log_level",
                regex_pattern=r"^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$",
                error_message="log_level must be one of: DEBUG, INFO, WARNING, ERROR, CRITICAL",
            )

        # Validate limit if provided
        if limit is not None:
            validate_parameter(
                limit,
                "limit",
                min_value=1,
                max_value=10000,
                error_message="limit must be between 1 and 10000",
            )

        # Validate worker_id if provided
        if worker_id is not None:
            validate_parameter(
                worker_id,
                "worker_id",
                not_none=True,
                error_message="worker_id cannot be empty",
            )

    async def setup_services(
        self,
        nats_url: str,
        log_level: Optional[str] = None,
        custom_settings: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Set up common services for event commands.

        Args:
            nats_url: URL of the NATS server.
            log_level: Logging level (e.g., DEBUG, INFO, WARNING, ERROR).
            custom_settings: Additional custom settings for service configuration.

        Raises:
            NaqConnectionError: If connection to NATS fails.
        """
        # Create global config with NATS URL and custom settings
        config = GlobalServiceConfig()
        config.nats_url = nats_url

        # Set up custom settings
        settings = {"log_level": log_level}
        if custom_settings:
            settings.update(custom_settings)
        config.custom_settings.update(settings)

        try:
            # Serialize configuration for logging and persistence
            serialized_settings = SerializationHelper.safe_serialize(
                settings, serializer="json"
            )

            # Build NATS subjects using the helper
            events_subject = build_subject("naq", "events")
            monitoring_subject = build_subject("naq", "events", "monitoring")

            self.structured_logger.info(
                "Setting up event services",
                nats_url=nats_url,
                settings_size=len(str(serialized_settings)),
                events_subject=events_subject,
                monitoring_subject=monitoring_subject,
            )

            # Create service manager with configuration
            service_config = ServiceConfig(nats_url=nats_url, custom_settings=settings)

            # Serialize service configuration
            serialized_service_config = SerializationHelper.safe_serialize(
                service_config.__dict__, serializer="json"
            )

            self.structured_logger.debug(
                "Service configuration serialized",
                config_size=len(str(serialized_service_config)),
            )

            self.service_manager = ServiceManager(config=service_config)

            # Register required services
            connection_service = await self.service_manager.register_service(
                "connection", ConnectionService, initialize=True
            )

            event_service_config = ServiceConfig(
                custom_settings={"enable_event_logging": True}
            )

            # Serialize event service configuration
            serialized_event_config = SerializationHelper.safe_serialize(
                event_service_config.__dict__, serializer="json"
            )

            self.structured_logger.debug(
                "Event service configuration serialized",
                config_size=len(str(serialized_event_config)),
            )

            self.event_service = await self.service_manager.register_service(
                "events",
                EventService,
                config=event_service_config,
                initialize=True,
            )

            # Check if required streams exist using the helper
            nc = await connection_service.get_connection()
            events_stream_exists = await stream_exists(nc=nc, stream_name="naq_events")

            if not events_stream_exists:
                self.structured_logger.warning(
                    "Events stream does not exist", stream_name="naq_events"
                )

            self.structured_logger.info(
                "Event services initialized",
                nats_url=nats_url,
                log_level=log_level,
                events_stream_exists=events_stream_exists,
            )

        except Exception as e:
            error_msg = f"Failed to set up services: {str(e)}"
            self.structured_logger.error(
                error_msg, nats_url=nats_url, error_type=type(e).__name__
            )
            self.console.print(f"[red]Error: {str(e)}[/red]")
            raise NaqConnectionError(error_msg) from e

    async def cleanup_services(self) -> None:
        """Clean up services."""
        if self.service_manager:
            await self.service_manager.cleanup_all()
            self.structured_logger.info("Services cleaned up")


# Helper functions for displaying events and statistics
def display_event(
    event: Union[JobEvent, WorkerEvent],
    format_type: str = "table",
    console: Optional[Console] = None,
) -> None:
    """
    Display a single event in the specified format.

    Args:
        event: The event to display (JobEvent or WorkerEvent)
        format_type: Output format ('table', 'json', 'raw')
        console: Rich console instance (creates one if not provided)
    """
    if console is None:
        console = Console()

    if format_type == "json":
        # Convert event to dictionary for JSON serialization
        event_dict = {
            "timestamp": event.timestamp,
            "event_type": event.event_type.value,
        }

        if isinstance(event, JobEvent):
            event_dict.update(
                {
                    "job_id": event.job_id,
                    "worker_id": event.worker_id,
                    "queue_name": event.queue_name,
                    "message": event.message,
                    "error_type": event.error_type,
                    "error_message": event.error_message,
                    "duration_ms": event.duration_ms,
                    "details": event.details,
                }
            )
        elif isinstance(event, WorkerEvent):
            event_dict.update(
                {
                    "worker_id": event.worker_id,
                    "queue_names": event.queue_names,
                    "message": event.message,
                    "job_id": event.job_id,
                    "duration_ms": event.duration_ms,
                    "cpu_usage": event.cpu_usage,
                    "memory_usage": event.memory_usage,
                    "details": event.details,
                }
            )

        console.print(json.dumps(event_dict, indent=2, default=str))

    elif format_type == "raw":
        # Display event as raw text
        if isinstance(event, JobEvent):
            console.print(
                f"[{datetime.fromtimestamp(event.timestamp, timezone.utc)}] "
                f"JobEvent: {event.event_type.value} | "
                f"Job: {event.job_id} | "
                f"Worker: {event.worker_id or 'N/A'} | "
                f"Queue: {event.queue_name or 'N/A'}"
            )
            if event.message:
                console.print(f"  Message: {event.message}")
            if event.error_type:
                console.print(f"  Error: {event.error_type}: {event.error_message}")
        elif isinstance(event, WorkerEvent):
            console.print(
                f"[{datetime.fromtimestamp(event.timestamp, timezone.utc)}] "
                f"WorkerEvent: {event.event_type.value} | "
                f"Worker: {event.worker_id} | "
                f"Queues: {', '.join(event.queue_names or [])}"
            )
            if event.message:
                console.print(f"  Message: {event.message}")
            if event.job_id:
                console.print(f"  Job: {event.job_id}")

    else:  # table format (default)
        if isinstance(event, JobEvent):
            console.print(
                f"[{datetime.fromtimestamp(event.timestamp, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}] "
                f"[cyan]{event.event_type.value}[/cyan] | "
                f"Job: [bold]{event.job_id}[/bold] | "
                f"Worker: {event.worker_id or 'N/A'} | "
                f"Queue: {event.queue_name or 'N/A'}"
            )
            if event.message:
                console.print(f"  └─ {event.message}")
            if event.error_type:
                console.print(
                    f"  └─ [red]Error: {event.error_type}: {event.error_message}[/red]"
                )
        elif isinstance(event, WorkerEvent):
            console.print(
                f"[{datetime.fromtimestamp(event.timestamp, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}] "
                f"[green]{event.event_type.value}[/green] | "
                f"Worker: [bold]{event.worker_id}[/bold] | "
                f"Queues: {', '.join(event.queue_names or [])}"
            )
            if event.message:
                console.print(f"  └─ {event.message}")
            if event.job_id:
                console.print(f"  └─ Job: {event.job_id}")


def display_event_table(
    events: List[Union[JobEvent, WorkerEvent]],
    console: Optional[Console] = None,
    title: str = "Events",
) -> None:
    """
    Display a list of events in a table format.

    Args:
        events: List of events to display
        console: Rich console instance (creates one if not provided)
        title: Title for the table
    """
    if console is None:
        console = Console()

    if not events:
        console.print("[yellow]No events found.[/yellow]")
        return

    table = Table(title=title, show_header=True, header_style="bold cyan")

    # Add columns
    table.add_column("Timestamp", style="dim", width=20)
    table.add_column("Type", style="bold", width=15)
    table.add_column("ID", style="bold", width=40)
    table.add_column("Details", style="dim")

    # Add rows
    for event in events:
        timestamp = datetime.fromtimestamp(event.timestamp, timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        if isinstance(event, JobEvent):
            event_type = f"[cyan]{event.event_type.value}[/cyan]"
            event_id = f"Job: {event.job_id}"
            details = []
            if event.worker_id:
                details.append(f"Worker: {event.worker_id}")
            if event.queue_name:
                details.append(f"Queue: {event.queue_name}")
            if event.message:
                details.append(f"Msg: {event.message}")
            if event.error_type:
                details.append(f"[red]Error: {event.error_type}[/red]")
        elif isinstance(event, WorkerEvent):
            event_type = f"[green]{event.event_type.value}[/green]"
            event_id = f"Worker: {event.worker_id}"
            details = []
            if event.queue_names:
                details.append(f"Queues: {', '.join(event.queue_names)}")
            if event.message:
                details.append(f"Msg: {event.message}")
            if event.job_id:
                details.append(f"Job: {event.job_id}")

        table.add_row(timestamp, event_type, event_id, " | ".join(details))

    console.print(table)


def display_stats_table(
    stats: Dict[str, Any],
    console: Optional[Console] = None,
    title: str = "Event Statistics",
) -> None:
    """
    Display event statistics in a table format.

    Args:
        stats: Statistics dictionary to display
        console: Rich console instance (creates one if not provided)
        title: Title for the table
    """
    if console is None:
        console = Console()

    table = Table(title=title, show_header=True, header_style="bold cyan")

    # Add columns
    table.add_column("Metric", style="bold", width=30)
    table.add_column("Value", style="dim")

    # Add rows
    for key, value in stats.items():
        if isinstance(value, dict):
            # Handle nested dictionaries
            for sub_key, sub_value in value.items():
                table.add_row(f"{key}.{sub_key}", str(sub_value))
        else:
            table.add_row(key, str(value))

    console.print(table)


def display_worker_table(
    workers: List[Dict[str, Any]],
    console: Optional[Console] = None,
    title: str = "Workers",
) -> None:
    """
    Display worker information in a table format.

    Args:
        workers: List of worker dictionaries to display
        console: Rich console instance (creates one if not provided)
        title: Title for the table
    """
    if console is None:
        console = Console()

    if not workers:
        console.print("[yellow]No workers found.[/yellow]")
        return

    table = Table(title=title, show_header=True, header_style="bold cyan")

    # Add columns
    table.add_column("Worker ID", style="bold", width=40)
    table.add_column("Status", width=12)
    table.add_column("Queues", width=25)
    table.add_column("Current Job", width=35)
    table.add_column("Last Heartbeat", width=20)

    # Add rows
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
        elif status == "idle":
            status_style = "dim"

        queues = ", ".join(worker.get("queues", []))
        current_job = worker.get("current_job_id", "-")

        # Format last heartbeat
        last_hb_ts = worker.get("last_heartbeat_utc")
        if last_hb_ts:
            hb_dt = datetime.fromtimestamp(last_hb_ts, timezone.utc)
            hb_str = hb_dt.strftime("%Y-%m-%d %H:%M:%S")

            # Check if heartbeat is stale (older than 60 seconds)
            if now - last_hb_ts > 60:
                hb_str = f"[red]{hb_str} (STALE)[/red]"
        else:
            hb_str = "[italic]never[/italic]"

        table.add_row(
            worker_id,
            f"[{status_style}]{status}[/{status_style}]",
            queues,
            current_job,
            hb_str,
        )

    console.print(table)
    console.print(f"\n[bold]Total:[/bold] {len(workers)} worker(s)")


# Create a Typer instance for event commands
event_app = typer.Typer(
    name="events",
    help="Event monitoring commands",
    add_completion=False,
)


@event_app.command("stream")
def stream_events(
    job_id: Optional[str] = typer.Option(
        None,
        "--job-id",
        "-j",
        help="Filter events by job ID.",
    ),
    event_type: Optional[str] = typer.Option(
        None,
        "--event-type",
        "-e",
        help="Filter events by event type (e.g., started, completed, failed).",
    ),
    queue: Optional[str] = typer.Option(
        None,
        "--queue",
        "-q",
        help="Filter events by queue name.",
    ),
    worker: Optional[str] = typer.Option(
        None,
        "--worker",
        "-w",
        help="Filter events by worker ID.",
    ),
    format: str = typer.Option(
        "table",
        "--format",
        "-f",
        help="Output format: table, json, or raw.",
        show_choices=True,
    ),
    follow: bool = typer.Option(
        True,
        "--follow",
        help="Follow live events (disable for historical only).",
    ),
    tail: int = typer.Option(
        10,
        "--tail",
        "-t",
        help="Number of historical events to show before following.",
    ),
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
    Stream real-time events from the naq system with filtering options.
    """
    setup_logging(log_level if log_level else "CRITICAL")
    handler = EventCommandHandler()

    # Validate parameters
    handler.validate_common_parameters(nats_url, log_level)

    # Ensure correct types
    nats_url = ensure_type(nats_url, str, "nats_url")
    if log_level is not None:
        log_level = ensure_type(log_level, str, "log_level")
    if job_id is not None:
        job_id = ensure_type(job_id, str, "job_id")
    if event_type is not None:
        event_type = ensure_type(event_type, str, "event_type")
    if queue is not None:
        queue = ensure_type(queue, str, "queue")
    if worker is not None:
        worker = ensure_type(worker, str, "worker")
    format = ensure_type(format, str, "format")
    tail = ensure_type(tail, int, "tail")

    # Validate format
    if format not in ["table", "json", "raw"]:
        error_msg = f"Invalid format: {format}"
        handler.console.print(f"[red]{error_msg}[/red]")
        handler.console.print("[red]Invalid format[/red]")
        raise typer.Exit(code=2)

    # Validate tail
    if tail < 0:
        error_msg = f"Tail must be non-negative: {tail}"
        handler.console.print(f"[red]{error_msg}[/red]")
        raise typer.Exit(code=1)

    @timing
    @log_errors
    async def _stream_events():
        try:
            # Use service context for short-lived operation
            async with service_context(
                nats_url=nats_url,
                custom_settings={"log_level": log_level},
                logger_name="naq.cli.event_commands.stream",
            ) as service_manager:
                # Get required services
                event_service = await service_manager.get_service(
                    "events", EventService
                )
                connection_service = await service_manager.get_service(
                    "connection", ConnectionService
                )

            # Log with structured logger
            handler.structured_logger.info(
                "Streaming events from NATS",
                nats_url=nats_url,
                job_id=job_id,
                event_type=event_type,
                queue=queue,
                worker=worker,
                format=format,
                follow=follow,
                tail=tail,
                operation="event_streaming",
            )

            # Get all job and worker keys from the events bucket
            try:
                # Get connection to NATS
                js = await connection_service.get_jetstream()

                # Get events bucket name from event service config
                events_bucket_name = event_service.event_config.events_bucket_name

                # List all keys in the events bucket
                kv = await js.key_value(events_bucket_name)
                keys = await kv.keys()

                # Filter keys based on criteria
                job_keys = []
                worker_keys = []

                for key in keys:
                    if key.startswith("job:") and key.endswith(":events"):
                        job_keys.append(key)
                    elif key.startswith("worker:") and key.endswith(":events"):
                        worker_keys.append(key)

                # Collect and filter events
                all_events = []

                # Process job events
                for key in job_keys:
                    try:
                        events_data = await event_service._kv_store_service.get(
                            events_bucket_name, key, deserialize=True
                        )
                        if isinstance(events_data, list):
                            for event in events_data:
                                # Convert to JobEvent if it's a dict
                                if isinstance(event, dict):
                                    event = JobEvent(**event)

                                # Apply filters
                                if job_id and event.job_id != job_id:
                                    continue
                                if event_type and event.event_type.value != event_type:
                                    continue
                                if queue and event.queue_name != queue:
                                    continue
                                if worker and event.worker_id != worker:
                                    continue

                                all_events.append(event)
                    except Exception as e:
                        handler.structured_logger.warning(
                            f"Failed to process job events from key {key}: {e}",
                            operation="event_streaming",
                            key=key,
                            error=str(e),
                        )

                # Process worker events
                for key in worker_keys:
                    try:
                        events_data = await event_service._kv_store_service.get(
                            events_bucket_name, key, deserialize=True
                        )
                        if isinstance(events_data, list):
                            for event in events_data:
                                # Convert to WorkerEvent if it's a dict
                                if isinstance(event, dict):
                                    event = WorkerEvent(**event)

                                # Apply filters
                                if job_id and event.job_id != job_id:
                                    continue
                                if event_type and event.event_type.value != event_type:
                                    continue
                                if worker and event.worker_id != worker:
                                    continue

                                all_events.append(event)
                    except Exception as e:
                        handler.structured_logger.warning(
                            f"Failed to process worker events from key {key}: {e}",
                            operation="event_streaming",
                            key=key,
                            error=str(e),
                        )

                # Sort events by timestamp
                all_events.sort(key=lambda e: e.timestamp)

                # Show tail events
                if tail > 0:
                    tail_events = all_events[-tail:]
                    handler.console.print(
                        f"\n[bold]Showing last {len(tail_events)} events:[/bold]"
                    )
                    for event in tail_events:
                        display_event(event, format, handler.console)

                # Follow live events if requested
                if follow:
                    handler.console.print(
                        "\n[bold]Following live events... (Press Ctrl+C to stop)[/bold]"
                    )

                    # Create a layout for live display
                    layout = Layout()
                    layout.split_column(
                        Layout(name="header", size=3),
                        Layout(name="events"),
                    )

                    # Update header
                    layout["header"].update(
                        Panel(
                            f"[bold]Live Event Stream[/bold]\n"
                            f"Filters: job_id={job_id or 'any'}, "
                            f"event_type={event_type or 'any'}, "
                            f"queue={queue or 'any'}, "
                            f"worker={worker or 'any'}",
                            style="blue",
                        )
                    )

                    # Create live display
                    with Live(layout, refresh_per_second=4, screen=True):
                        try:
                            last_timestamp = (
                                max([e.timestamp for e in all_events])
                                if all_events
                                else time.time()
                            )

                            while True:
                                # Check for new events
                                new_events = []

                                # Process job events again
                                for key in job_keys:
                                    try:
                                        events_data = (
                                            await event_service._kv_store_service.get(
                                                events_bucket_name,
                                                key,
                                                deserialize=True,
                                            )
                                        )
                                        if isinstance(events_data, list):
                                            for event in events_data:
                                                # Convert to JobEvent if it's a dict
                                                if isinstance(event, dict):
                                                    event = JobEvent(**event)

                                                # Only include events newer than last_timestamp
                                                if event.timestamp <= last_timestamp:
                                                    continue

                                                # Apply filters
                                                if job_id and event.job_id != job_id:
                                                    continue
                                                if (
                                                    event_type
                                                    and event.event_type.value
                                                    != event_type
                                                ):
                                                    continue
                                                if queue and event.queue_name != queue:
                                                    continue
                                                if worker and event.worker_id != worker:
                                                    continue

                                                new_events.append(event)
                                    except Exception as e:
                                        handler.structured_logger.warning(
                                            f"Failed to process job events from key {key}: {e}",
                                            operation="event_streaming",
                                            key=key,
                                            error=str(e),
                                        )

                                # Process worker events again
                                for key in worker_keys:
                                    try:
                                        events_data = (
                                            await event_service._kv_store_service.get(
                                                events_bucket_name,
                                                key,
                                                deserialize=True,
                                            )
                                        )
                                        if isinstance(events_data, list):
                                            for event in events_data:
                                                # Convert to WorkerEvent if it's a dict
                                                if isinstance(event, dict):
                                                    event = WorkerEvent(**event)

                                                # Only include events newer than last_timestamp
                                                if event.timestamp <= last_timestamp:
                                                    continue

                                                # Apply filters
                                                if job_id and event.job_id != job_id:
                                                    continue
                                                if (
                                                    event_type
                                                    and event.event_type.value
                                                    != event_type
                                                ):
                                                    continue
                                                if worker and event.worker_id != worker:
                                                    continue

                                                new_events.append(event)
                                    except Exception as e:
                                        handler.structured_logger.warning(
                                            f"Failed to process worker events from key {key}: {e}",
                                            operation="event_streaming",
                                            key=key,
                                            error=str(e),
                                        )

                                # Update last_timestamp
                                if new_events:
                                    last_timestamp = max(
                                        [e.timestamp for e in new_events]
                                    )

                                    # Display new events
                                    for event in sorted(
                                        new_events, key=lambda e: e.timestamp
                                    ):
                                        display_event(event, format, handler.console)

                                # Sleep before next check
                                await asyncio.sleep(1)

                        except KeyboardInterrupt:
                            handler.console.print(
                                "\n[yellow]Event streaming stopped by user.[/yellow]"
                            )
                            return
            except Exception as e:
                handler.structured_logger.error(
                    "Failed to stream events",
                    nats_url=nats_url,
                    job_id=job_id,
                    event_type=event_type,
                    queue=queue,
                    worker=worker,
                    error=str(e),
                    error_type=type(e).__name__,
                )
                handler.console.print(f"[red]Error: {str(e)}[/red]")
                raise
            finally:
                # Service context automatically handles cleanup
                pass
        except Exception as e:
            handler.structured_logger.error(
                "Failed to stream events",
                nats_url=nats_url,
                job_id=job_id,
                event_type=event_type,
                queue=queue,
                worker=worker,
                error=str(e),
                error_type=type(e).__name__,
            )
            handler.console.print(f"[red]Error: {str(e)}[/red]")
            raise
        finally:
            # Service context automatically handles cleanup
            pass

    # Run the async function
    asyncio.run(_stream_events())


@event_app.command("history")
def history(
    job_id: str = typer.Argument(
        ...,
        help="Job ID to retrieve event history for.",
    ),
    format: str = typer.Option(
        "table",
        "--format",
        "-f",
        help="Output format: table, json, or raw.",
        show_choices=True,
    ),
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
    Retrieve complete event history for a specific job_id.
    """
    setup_logging(log_level if log_level else "CRITICAL")
    handler = EventCommandHandler()

    # Validate parameters
    handler.validate_common_parameters(nats_url, log_level)

    # Ensure correct types
    nats_url = ensure_type(nats_url, str, "nats_url")
    job_id = ensure_type(job_id, str, "job_id")
    if log_level is not None:
        log_level = ensure_type(log_level, str, "log_level")
    format = ensure_type(format, str, "format")

    # Validate format
    if format not in ["table", "json", "raw"]:
        error_msg = f"Invalid format: {format}"
        handler.console.print(f"[red]{error_msg}[/red]")
        handler.console.print("[red]Invalid format[/red]")
        raise typer.Exit(code=2)

    @timing
    @log_errors
    async def _get_history():
        try:
            # Use service context for short-lived operation
            async with service_context(
                nats_url=nats_url,
                custom_settings={"log_level": log_level},
                logger_name="naq.cli.event_commands.history",
            ) as service_manager:
                # Get required services
                event_service = await service_manager.get_service(
                    "events", EventService
                )

            # Log with structured logger
            handler.structured_logger.info(
                "Retrieving event history for job",
                nats_url=nats_url,
                job_id=job_id,
                format=format,
                operation="event_history",
            )

            # Get job events
            job_events = await event_service.get_job_events(job_id)

            if not job_events:
                handler.console.print(
                    f"[yellow]No events found for job {job_id}[/yellow]"
                )
                return

            # Sort events by timestamp
            job_events.sort(key=lambda e: e.timestamp)

            # Display events
            handler.console.print(f"\n[bold]Event history for job {job_id}:[/bold]")
            handler.console.print(f"Found {len(job_events)} events\n")

            if format == "table":
                display_event_table(job_events, handler.console)
            else:
                for event in job_events:
                    display_event(event, format, handler.console)

        except Exception as e:
            handler.structured_logger.error(
                "Failed to retrieve event history",
                nats_url=nats_url,
                job_id=job_id,
                error=str(e),
                error_type=type(e).__name__,
            )
            handler.console.print(f"[red]Error: {str(e)}[/red]")
            raise
        # Service context automatically handles cleanup

    # Run the async function
    asyncio.run(_get_history())


@event_app.command("stats")
def stats(
    job_id: Optional[str] = typer.Option(
        None,
        "--job-id",
        "-j",
        help="Filter statistics by job ID.",
    ),
    queue: Optional[str] = typer.Option(
        None,
        "--queue",
        "-q",
        help="Filter statistics by queue name.",
    ),
    worker: Optional[str] = typer.Option(
        None,
        "--worker",
        "-w",
        help="Filter statistics by worker ID.",
    ),
    time_range: Optional[str] = typer.Option(
        None,
        "--time-range",
        "-t",
        help="Time range for statistics (e.g., '1h', '24h', '7d', '30d').",
    ),
    format: str = typer.Option(
        "table",
        "--format",
        "-f",
        help="Output format: table, json, or raw.",
        show_choices=True,
    ),
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
    Display statistics about events in the naq system.
    """
    setup_logging(log_level if log_level else "CRITICAL")
    handler = EventCommandHandler()

    # Validate parameters
    handler.validate_common_parameters(nats_url, log_level)

    # Ensure correct types
    nats_url = ensure_type(nats_url, str, "nats_url")
    if log_level is not None:
        log_level = ensure_type(log_level, str, "log_level")
    if job_id is not None:
        job_id = ensure_type(job_id, str, "job_id")
    if queue is not None:
        queue = ensure_type(queue, str, "queue")
    if worker is not None:
        worker = ensure_type(worker, str, "worker")
    if time_range is not None:
        time_range = ensure_type(time_range, str, "time_range")
    format = ensure_type(format, str, "format")

    # Validate format
    if format not in ["table", "json", "raw"]:
        error_msg = f"Invalid format: {format}"
        handler.console.print(f"[red]{error_msg}[/red]")
        handler.console.print("[red]Invalid format[/red]")
        raise typer.Exit(code=2)

    # Parse time range
    time_range_seconds = None
    if time_range:
        try:
            if time_range.endswith("h"):
                time_range_seconds = int(time_range[:-1]) * 3600
            elif time_range.endswith("d"):
                time_range_seconds = int(time_range[:-1]) * 86400
            else:
                handler.console.print("[red]Invalid time range format[/red]")
                handler.console.print("[red]Invalid time range format[/red]")
                raise typer.Exit(code=2)
        except ValueError:
            handler.console.print("[red]Invalid time range format[/red]")
            handler.console.print("[red]Invalid time range format[/red]")
            raise typer.Exit(code=2)

    @timing
    @log_errors
    async def _get_stats():
        try:
            # Use service context for short-lived operation
            async with service_context(
                nats_url=nats_url,
                custom_settings={"log_level": log_level},
                logger_name="naq.cli.event_commands.stats",
            ) as service_manager:
                # Get required services
                event_service = await service_manager.get_service(
                    "events", EventService
                )
                connection_service = await service_manager.get_service(
                    "connection", ConnectionService
                )

            # Log with structured logger
            handler.structured_logger.info(
                "Fetching event statistics from NATS",
                nats_url=nats_url,
                job_id=job_id,
                queue=queue,
                worker=worker,
                time_range=time_range,
                format=format,
                operation="event_stats",
            )

            # Get all job and worker keys from the events bucket
            try:
                # Get connection to NATS
                js = await connection_service.get_jetstream()

                # Get events bucket name from event service config
                events_bucket_name = event_service.event_config.events_bucket_name

                # List all keys in the events bucket
                kv = await js.key_value(events_bucket_name)
                keys = await kv.keys()

                # Filter keys based on criteria
                job_keys = []
                worker_keys = []

                for key in keys:
                    if key.startswith("job:") and key.endswith(":events"):
                        job_keys.append(key)
                    elif key.startswith("worker:") and key.endswith(":events"):
                        worker_keys.append(key)

                # Collect and filter events
                all_events = []
                current_time = time.time()
                time_threshold = (
                    current_time - time_range_seconds if time_range_seconds else 0
                )

                # Process job events
                for key in job_keys:
                    try:
                        events_data = await event_service._kv_store_service.get(
                            events_bucket_name, key, deserialize=True
                        )
                        if isinstance(events_data, list):
                            for event in events_data:
                                # Convert to JobEvent if it's a dict
                                if isinstance(event, dict):
                                    event = JobEvent(**event)

                                # Apply filters
                                if (
                                    time_range_seconds
                                    and event.timestamp < time_threshold
                                ):
                                    continue
                                if job_id and event.job_id != job_id:
                                    continue
                                if queue and event.queue_name != queue:
                                    continue
                                if worker and event.worker_id != worker:
                                    continue

                                all_events.append(event)
                    except Exception as e:
                        handler.structured_logger.warning(
                            f"Failed to process job events from key {key}: {e}",
                            operation="event_stats",
                            key=key,
                            error=str(e),
                        )

                # Process worker events
                for key in worker_keys:
                    try:
                        events_data = await event_service._kv_store_service.get(
                            events_bucket_name, key, deserialize=True
                        )
                        if isinstance(events_data, list):
                            for event in events_data:
                                # Convert to WorkerEvent if it's a dict
                                if isinstance(event, dict):
                                    event = WorkerEvent(**event)

                                # Apply filters
                                if (
                                    time_range_seconds
                                    and event.timestamp < time_threshold
                                ):
                                    continue
                                if job_id and event.job_id != job_id:
                                    continue
                                if worker and event.worker_id != worker:
                                    continue

                                all_events.append(event)
                    except Exception as e:
                        handler.structured_logger.warning(
                            f"Failed to process worker events from key {key}: {e}",
                            operation="event_stats",
                            key=key,
                            error=str(e),
                        )

                # Calculate statistics
                stats = {
                    "total_events": len(all_events),
                    "time_range": time_range or "all",
                    "filters": {
                        "job_id": job_id or "any",
                        "queue": queue or "any",
                        "worker": worker or "any",
                    },
                }

                if all_events:
                    # Separate job and worker events
                    job_events = [e for e in all_events if isinstance(e, JobEvent)]
                    worker_events = [
                        e for e in all_events if isinstance(e, WorkerEvent)
                    ]

                    # Job event statistics
                    if job_events:
                        job_stats = {
                            "total_job_events": len(job_events),
                            "by_event_type": {},
                            "by_queue": {},
                            "by_worker": {},
                            "avg_duration_ms": 0,
                            "success_rate": 0,
                            "error_rate": 0,
                        }

                        # Calculate duration and success/error rates
                        durations = []
                        success_count = 0
                        error_count = 0

                        for event in job_events:
                            # Count by event type
                            event_type = event.event_type.value
                            job_stats["by_event_type"][event_type] = (
                                job_stats["by_event_type"].get(event_type, 0) + 1
                            )

                            # Count by queue
                            if event.queue_name:
                                queue_name = event.queue_name
                                job_stats["by_queue"][queue_name] = (
                                    job_stats["by_queue"].get(queue_name, 0) + 1
                                )

                            # Count by worker
                            if event.worker_id:
                                worker_id = event.worker_id
                                job_stats["by_worker"][worker_id] = (
                                    job_stats["by_worker"].get(worker_id, 0) + 1
                                )

                            # Collect duration and success/error info
                            if event.duration_ms:
                                durations.append(event.duration_ms)

                            if event.event_type == JobEventType.COMPLETED:
                                success_count += 1
                            elif event.event_type == JobEventType.FAILED:
                                error_count += 1

                        # Calculate averages and rates
                        if durations:
                            job_stats["avg_duration_ms"] = sum(durations) / len(
                                durations
                            )

                        total_completed = success_count + error_count
                        if total_completed > 0:
                            job_stats["success_rate"] = (
                                success_count / total_completed
                            ) * 100
                            job_stats["error_rate"] = (
                                error_count / total_completed
                            ) * 100

                        stats["job_events"] = job_stats

                    # Worker event statistics
                    if worker_events:
                        worker_stats = {
                            "total_worker_events": len(worker_events),
                            "by_event_type": {},
                            "by_worker": {},
                            "avg_cpu_usage": 0,
                            "avg_memory_usage": 0,
                        }

                        # Calculate CPU and memory usage
                        cpu_usages = []
                        memory_usages = []

                        for event in worker_events:
                            # Count by event type
                            event_type = event.event_type.value
                            worker_stats["by_event_type"][event_type] = (
                                worker_stats["by_event_type"].get(event_type, 0) + 1
                            )

                            # Count by worker
                            worker_id = event.worker_id
                            worker_stats["by_worker"][worker_id] = (
                                worker_stats["by_worker"].get(worker_id, 0) + 1
                            )

                            # Collect CPU and memory info
                            if event.cpu_usage:
                                cpu_usages.append(event.cpu_usage)

                            if event.memory_usage:
                                memory_usages.append(event.memory_usage)

                        # Calculate averages
                        if cpu_usages:
                            worker_stats["avg_cpu_usage"] = sum(cpu_usages) / len(
                                cpu_usages
                            )

                        if memory_usages:
                            worker_stats["avg_memory_usage"] = sum(memory_usages) / len(
                                memory_usages
                            )

                        stats["worker_events"] = worker_stats

                # Display statistics
                handler.console.print("\n[bold]Event Statistics[/bold]")
                if time_range:
                    handler.console.print(f"Time range: {time_range}")
                if job_id:
                    handler.console.print(f"Job ID: {job_id}")
                if queue:
                    handler.console.print(f"Queue: {queue}")
                if worker:
                    handler.console.print(f"Worker: {worker}")
                handler.console.print("")

                if format == "json":
                    handler.console.print(json.dumps(stats, indent=2, default=str))
                elif format == "raw":
                    # Display statistics as raw text
                    handler.console.print(f"Total events: {stats['total_events']}")
                    if "job_events" in stats:
                        job_stats = stats["job_events"]
                        handler.console.print(
                            f"Job events: {job_stats['total_job_events']}"
                        )
                        handler.console.print(
                            f"Average job duration: {job_stats['avg_duration_ms']:.2f} ms"
                        )
                        handler.console.print(
                            f"Success rate: {job_stats['success_rate']:.2f}%"
                        )
                        handler.console.print(
                            f"Error rate: {job_stats['error_rate']:.2f}%"
                        )
                    if "worker_events" in stats:
                        worker_stats = stats["worker_events"]
                        handler.console.print(
                            f"Worker events: {worker_stats['total_worker_events']}"
                        )
                        handler.console.print(
                            f"Average CPU usage: {worker_stats['avg_cpu_usage']:.2f}%"
                        )
                        handler.console.print(
                            f"Average memory usage: {worker_stats['avg_memory_usage']:.2f}%"
                        )
                else:  # table format
                    display_stats_table(stats, handler.console)
            except Exception as e:
                handler.structured_logger.error(
                    "Failed to fetch event statistics",
                    nats_url=nats_url,
                    job_id=job_id,
                    queue=queue,
                    worker=worker,
                    time_range=time_range,
                    error=str(e),
                    error_type=type(e).__name__,
                )
                handler.console.print(f"[red]Error: {str(e)}[/red]")
                raise
            finally:
                # Service context automatically handles cleanup
                pass
        except Exception as e:
            handler.structured_logger.error(
                "Failed to fetch event statistics",
                nats_url=nats_url,
                job_id=job_id,
                queue=queue,
                worker=worker,
                time_range=time_range,
                error=str(e),
                error_type=type(e).__name__,
            )
            handler.console.print(f"[red]Error: {str(e)}[/red]")
            raise
        finally:
            # Service context automatically handles cleanup
            pass

    # Run the async function
    asyncio.run(_get_stats())


@event_app.command("workers")
def workers(
    worker_id: Optional[str] = typer.Option(
        None,
        "--worker-id",
        "-w",
        help="Filter by specific worker ID.",
    ),
    queue: Optional[str] = typer.Option(
        None,
        "--queue",
        "-q",
        help="Filter by queue name.",
    ),
    status: Optional[str] = typer.Option(
        None,
        "--status",
        "-s",
        help="Filter by worker status (idle, busy, starting, stopping).",
    ),
    format: str = typer.Option(
        "table",
        "--format",
        "-f",
        help="Output format: table, json, or raw.",
        show_choices=True,
    ),
    follow: bool = typer.Option(
        False,
        "--follow",
        help="Follow live worker status updates.",
    ),
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
    Monitor worker events and status.
    """
    setup_logging(log_level if log_level else "CRITICAL")
    handler = EventCommandHandler()

    # Validate parameters
    handler.validate_common_parameters(nats_url, log_level)

    # Ensure correct types
    nats_url = ensure_type(nats_url, str, "nats_url")
    if log_level is not None:
        log_level = ensure_type(log_level, str, "log_level")
    if worker_id is not None:
        worker_id = ensure_type(worker_id, str, "worker_id")
    if queue is not None:
        queue = ensure_type(queue, str, "queue")
    if status is not None:
        status = ensure_type(status, str, "status")
    format = ensure_type(format, str, "format")

    # Validate format
    if format not in ["table", "json", "raw"]:
        error_msg = f"Invalid format: {format}"
        handler.console.print(f"[red]{error_msg}[/red]")
        handler.console.print("[red]Invalid format[/red]")
        raise typer.Exit(code=2)

    # Validate status
    if status is not None and status not in ["idle", "busy", "starting", "stopping"]:
        handler.console.print(f"[red]Invalid status: {status}[/red]")
        handler.console.print("[red]Invalid status[/red]")
        raise typer.Exit(code=2)

    @timing
    @log_errors
    async def _monitor_workers():
        try:
            # Use service context for short-lived operation
            async with service_context(
                nats_url=nats_url,
                custom_settings={"log_level": log_level},
                logger_name="naq.cli.event_commands.workers",
            ) as service_manager:
                # Get required services
                worker_service = await service_manager.get_service(
                    "worker", WorkerService
                )

            # Log with structured logger
            handler.structured_logger.info(
                "Monitoring workers",
                nats_url=nats_url,
                worker_id=worker_id,
                queue=queue,
                status=status,
                format=format,
                follow=follow,
                operation="worker_monitoring",
            )

            # Get worker information
            workers = await worker_service.get_workers()

            # Apply filters
            filtered_workers = []
            for worker in workers:
                # Filter by worker_id
                if worker_id and worker.get("worker_id") != worker_id:
                    continue

                # Filter by queue
                if queue and queue not in worker.get("queues", []):
                    continue

                # Filter by status
                if status and worker.get("status") != status:
                    continue

                filtered_workers.append(worker)

            # Display workers
            if format == "table":
                display_worker_table(filtered_workers, handler.console)
            elif format == "json":
                handler.console.print(
                    json.dumps(filtered_workers, indent=2, default=str)
                )
            else:  # raw format
                for worker in filtered_workers:
                    worker_id_str = worker.get("worker_id", "unknown")
                    status_str = worker.get("status", "?")
                    queues_str = ", ".join(worker.get("queues", []))
                    current_job_str = worker.get("current_job_id", "-")

                    # Format last heartbeat
                    last_hb_ts = worker.get("last_heartbeat_utc")
                    if last_hb_ts:
                        hb_dt = datetime.fromtimestamp(last_hb_ts, timezone.utc)
                        hb_str = hb_dt.strftime("%Y-%m-%d %H:%M:%S")
                    else:
                        hb_str = "never"

                    handler.console.print(
                        f"Worker: {worker_id_str} | "
                        f"Status: {status_str} | "
                        f"Queues: {queues_str} | "
                        f"Current Job: {current_job_str} | "
                        f"Last Heartbeat: {hb_str}"
                    )

            # Follow live updates if requested
            if follow:
                handler.console.print(
                    "\n[bold]Following live worker updates... (Press Ctrl+C to stop)[/bold]"
                )

                # Create a layout for live display
                layout = Layout()
                layout.split_column(
                    Layout(name="header", size=3),
                    Layout(name="workers"),
                )

                # Update header
                layout["header"].update(
                    Panel(
                        f"[bold]Live Worker Monitor[/bold]\n"
                        f"Filters: worker_id={worker_id or 'any'}, "
                        f"queue={queue or 'any'}, "
                        f"status={status or 'any'}",
                        style="blue",
                    )
                )

                # Create live display
                with Live(layout, refresh_per_second=2, screen=True):
                    try:
                        while True:
                            # Get updated worker information
                            updated_workers = await worker_service.get_workers()

                            # Apply filters
                            updated_filtered_workers = []
                            for worker in updated_workers:
                                # Filter by worker_id
                                if worker_id and worker.get("worker_id") != worker_id:
                                    continue

                                # Filter by queue
                                if queue and queue not in worker.get("queues", []):
                                    continue

                                # Filter by status
                                if status and worker.get("status") != status:
                                    continue

                                updated_filtered_workers.append(worker)

                            # Update display
                            if format == "table":
                                # Create a table for the live display
                                table = Table(
                                    title="Workers",
                                    show_header=True,
                                    header_style="bold cyan",
                                )
                                table.add_column("Worker ID", style="bold", width=40)
                                table.add_column("Status", width=12)
                                table.add_column("Queues", width=25)
                                table.add_column("Current Job", width=35)
                                table.add_column("Last Heartbeat", width=20)

                                # Add rows
                                now = time.time()
                                for worker in updated_filtered_workers:
                                    worker_id_str = worker.get("worker_id", "unknown")
                                    status_str = worker.get("status", "?")

                                    # Determine status style
                                    status_style = "green"
                                    if status_str == "busy":
                                        status_style = "yellow"
                                    elif status_str in ["stopping", "starting"]:
                                        status_style = "blue"
                                    elif status_str == "idle":
                                        status_style = "dim"

                                    queues_str = ", ".join(worker.get("queues", []))
                                    current_job_str = worker.get("current_job_id", "-")

                                    # Format last heartbeat
                                    last_hb_ts = worker.get("last_heartbeat_utc")
                                    if last_hb_ts:
                                        hb_dt = datetime.fromtimestamp(
                                            last_hb_ts, timezone.utc
                                        )
                                        hb_str = hb_dt.strftime("%Y-%m-%d %H:%M:%S")

                                        # Check if heartbeat is stale (older than 60 seconds)
                                        if now - last_hb_ts > 60:
                                            hb_str = f"[red]{hb_str} (STALE)[/red]"
                                    else:
                                        hb_str = "[italic]never[/italic]"

                                    table.add_row(
                                        worker_id_str,
                                        f"[{status_style}]{status_str}[/{status_style}]",
                                        queues_str,
                                        current_job_str,
                                        hb_str,
                                    )

                                layout["workers"].update(table)
                                layout["workers"].update(
                                    f"\n[bold]Total:[/bold] {len(updated_filtered_workers)} worker(s)"
                                )
                            else:
                                # For json or raw format, just print the updated data
                                if format == "json":
                                    layout["workers"].update(
                                        json.dumps(
                                            updated_filtered_workers,
                                            indent=2,
                                            default=str,
                                        )
                                    )
                                else:  # raw format
                                    output = ""
                                    for worker in updated_filtered_workers:
                                        worker_id_str = worker.get(
                                            "worker_id", "unknown"
                                        )
                                        status_str = worker.get("status", "?")
                                        queues_str = ", ".join(worker.get("queues", []))
                                        current_job_str = worker.get(
                                            "current_job_id", "-"
                                        )

                                        # Format last heartbeat
                                        last_hb_ts = worker.get("last_heartbeat_utc")
                                        if last_hb_ts:
                                            hb_dt = datetime.fromtimestamp(
                                                last_hb_ts, timezone.utc
                                            )
                                            hb_str = hb_dt.strftime("%Y-%m-%d %H:%M:%S")
                                        else:
                                            hb_str = "never"

                                        output += (
                                            f"Worker: {worker_id_str} | "
                                            f"Status: {status_str} | "
                                            f"Queues: {queues_str} | "
                                            f"Current Job: {current_job_str} | "
                                            f"Last Heartbeat: {hb_str}\n"
                                        )

                                    layout["workers"].update(output)

                            # Sleep before next update
                            await asyncio.sleep(1)

                    except KeyboardInterrupt:
                        handler.console.print(
                            "\n[yellow]Worker monitoring stopped by user.[/yellow]"
                        )
                        return

        except Exception as e:
            handler.structured_logger.error(
                "Failed to monitor workers",
                nats_url=nats_url,
                worker_id=worker_id,
                queue=queue,
                status=status,
                error=str(e),
                error_type=type(e).__name__,
            )
            handler.console.print(f"[red]Error: {str(e)}[/red]")
            raise
        # Service context automatically handles cleanup

    # Run the async function
    asyncio.run(_monitor_workers())
