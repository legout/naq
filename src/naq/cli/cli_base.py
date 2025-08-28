"""Base classes and utilities for NAQ CLI commands.

This module provides common functionality for all NAQ CLI commands,
including service setup, error handling, logging, and Rich console integration.
"""

import asyncio
import json
from typing import Any, Dict, List, Optional, Union

import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from ..settings import DEFAULT_NATS_URL
from ..services.base import ServiceManager, ServiceConfig
from ..services.config import GlobalServiceConfig
from ..service_context import service_context
from ..utils import setup_logging
from ..utils.decorators import timing, log_errors
from ..utils.logging import StructuredLogger
from ..utils.validation import validate_parameter, ensure_type
from ..utils.serialization import SerializationHelper
from ..utils.nats_helpers import build_subject, stream_exists
from ..exceptions import NaqConnectionError
from ..models.events import JobEvent, WorkerEvent
from ..models.enums import JobEventType


class BaseCLICommand:
    """Base class for NAQ CLI commands with common functionality.
    
    This class provides standardized patterns for:
    - Service setup and management
    - Error handling and logging
    - Parameter validation
    - Rich console output
    """

    def __init__(self, logger_name: str = "naq.cli") -> None:
        """Initialize the BaseCLICommand.
        
        Args:
            logger_name: Name for the structured logger.
        """
        self.console = Console()
        self.structured_logger = StructuredLogger(logger_name)
        self.service_manager: Optional[ServiceManager] = None

    def setup_logging(self, log_level: Optional[str] = None) -> None:
        """Set up logging for CLI commands.
        
        Args:
            log_level: Logging level to use.
        """
        setup_logging(log_level if log_level else "CRITICAL")

    def validate_common_parameters(
        self,
        nats_url: str,
        log_level: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Validate common parameters used across CLI commands.
        
        Args:
            nats_url: URL of the NATS server.
            log_level: Logging level (e.g., DEBUG, INFO, WARNING, ERROR).
            **kwargs: Additional parameters to validate.
            
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

        # Validate additional parameters if provided
        for key, value in kwargs.items():
            if value is not None:
                validate_parameter(value, key, not_none=True)

    async def setup_services(
        self,
        nats_url: str,
        log_level: Optional[str] = None,
        custom_settings: Optional[Dict[str, Any]] = None,
    ) -> ServiceManager:
        """Set up common services for CLI commands.
        
        Args:
            nats_url: URL of the NATS server.
            log_level: Logging level (e.g., DEBUG, INFO, WARNING, ERROR).
            custom_settings: Additional custom settings for service configuration.
            
        Returns:
            ServiceManager: Configured service manager.
            
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

            self.structured_logger.info(
                "Setting up CLI services",
                nats_url=nats_url,
                settings_size=len(str(serialized_settings)),
            )

            # Create service manager with configuration
            service_config = ServiceConfig(nats_url=nats_url, custom_settings=settings)
            service_manager = ServiceManager(config=service_config)

            # Register required services
            await service_manager.register_service(
                "connection", "naq.services.connection.ConnectionService", initialize=True
            )

            self.structured_logger.info(
                "CLI services initialized",
                nats_url=nats_url,
                log_level=log_level,
            )

            return service_manager

        except Exception as e:
            error_msg = f"Failed to set up services: {str(e)}"
            self.structured_logger.error(
                error_msg, nats_url=nats_url, error_type=type(e).__name__
            )
            self.console.print(f"[red]Error: {str(e)}[/red]")
            raise NaqConnectionError(error_msg) from e

    async def cleanup_services(self, service_manager: ServiceManager) -> None:
        """Clean up services.
        
        Args:
            service_manager: Service manager to clean up.
        """
        await service_manager.cleanup_all()
        self.structured_logger.info("Services cleaned up")

    def handle_format_validation(self, format_type: str) -> None:
        """Validate output format parameter.
        
        Args:
            format_type: Output format to validate.
            
        Raises:
            typer.Exit: If format is invalid.
        """
        if format_type not in ["table", "json", "raw"]:
            error_msg = f"Invalid format: {format_type}"
            self.console.print(f"[red]{error_msg}[/red]")
            raise typer.Exit(code=2)

    def run_async_command(self, async_func) -> None:
        """Run an async CLI command with standardized error handling.
        
        Args:
            async_func: Async function to run.
        """
        @timing
        @log_errors
        def _run():
            try:
                asyncio.run(async_func())
            except Exception as e:
                self.structured_logger.error(
                    "CLI command failed",
                    error=str(e),
                    error_type=type(e).__name__,
                )
                self.console.print(f"[red]Error: {str(e)}[/red]")
                raise typer.Exit(code=1)

        _run()


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
        from datetime import datetime, timezone
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
        from datetime import datetime, timezone
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
    from datetime import datetime, timezone
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
    title: str = "Statistics",
) -> None:
    """
    Display statistics in a table format.

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
    import time
    from datetime import datetime, timezone

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