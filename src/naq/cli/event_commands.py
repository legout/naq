"""Event monitoring CLI commands for naq."""

import asyncio
from typing import Optional, Dict, Any

import typer
from rich.console import Console

from ..settings import DEFAULT_NATS_URL
from ..services import ServiceManager, EventService, ConnectionService, ServiceConfig
from ..services.config import GlobalServiceConfig
from ..utils import setup_logging
from ..utils.validation import validate_parameter, ensure_type
from ..utils.nats_helpers import build_subject, stream_exists
from ..utils.decorators import timing, log_errors
from ..utils.logging import StructuredLogger
from ..utils.serialization import SerializationHelper
from ..exceptions import NaqConnectionError

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
            regex_pattern=r"^(nats://)?[a-zA-Z0-9.-]+(:[0-9]+)?(,[a-zA-Z0-9.-]+(:[0-9]+)?)*$",
            error_message="Invalid NATS URL format"
        )
        
        # Validate log_level if provided
        if log_level is not None:
            validate_parameter(
                log_level,
                "log_level",
                regex_pattern=r"^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$",
                error_message="log_level must be one of: DEBUG, INFO, WARNING, ERROR, CRITICAL"
            )
        
        # Validate limit if provided
        if limit is not None:
            validate_parameter(
                limit,
                "limit",
                min_value=1,
                max_value=10000,
                error_message="limit must be between 1 and 10000"
            )
        
        # Validate worker_id if provided
        if worker_id is not None:
            validate_parameter(
                worker_id,
                "worker_id",
                not_none=True,
                error_message="worker_id cannot be empty"
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
                settings,
                serializer="json"
            )
            
            # Build NATS subjects using the helper
            events_subject = build_subject("naq", "events")
            monitoring_subject = build_subject("naq", "events", "monitoring")
            
            self.structured_logger.info(
                "Setting up event services",
                nats_url=nats_url,
                settings_size=len(str(serialized_settings)),
                events_subject=events_subject,
                monitoring_subject=monitoring_subject
            )
            
            # Create service manager with configuration
            service_config = ServiceConfig(
                nats_url=nats_url,
                custom_settings=settings
            )
            
            # Serialize service configuration
            serialized_service_config = SerializationHelper.safe_serialize(
                service_config.__dict__,
                serializer="json"
            )
            
            self.structured_logger.debug(
                "Service configuration serialized",
                config_size=len(str(serialized_service_config))
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
                event_service_config.__dict__,
                serializer="json"
            )
            
            self.structured_logger.debug(
                "Event service configuration serialized",
                config_size=len(str(serialized_event_config))
            )
            
            self.event_service = await self.service_manager.register_service(
                "events",
                EventService,
                config=event_service_config,
                initialize=True,
            )
            
            # Check if required streams exist using the helper
            nc = await connection_service.get_connection()
            events_stream_exists = await stream_exists(
                nc=nc,
                stream_name="naq_events"
            )
            
            if not events_stream_exists:
                self.structured_logger.warning(
                    "Events stream does not exist",
                    stream_name="naq_events"
                )
            
            self.structured_logger.info(
                "Event services initialized",
                nats_url=nats_url,
                log_level=log_level,
                events_stream_exists=events_stream_exists
            )
                
        except Exception as e:
            error_msg = f"Failed to set up services: {str(e)}"
            self.structured_logger.error(
                error_msg,
                nats_url=nats_url,
                error_type=type(e).__name__
            )
            self.console.print(f"[red]Error: {str(e)}[/red]")
            raise NaqConnectionError(error_msg) from e
    
    async def cleanup_services(self) -> None:
        """Clean up services."""
        if self.service_manager:
            await self.service_manager.cleanup_all()
            self.structured_logger.info("Services cleaned up")


# Create a Typer instance for event commands
event_app = typer.Typer(
    name="events",
    help="Event monitoring commands",
    add_completion=False,
)


@event_app.command("monitor")
def events(
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
    Monitor real-time events from the naq system.
    """
    setup_logging(log_level if log_level else None)
    handler = EventCommandHandler()
    
    # Validate parameters
    handler.validate_common_parameters(nats_url, log_level)
    
    # Ensure correct types
    nats_url = ensure_type(nats_url, str, "nats_url")
    if log_level is not None:
        log_level = ensure_type(log_level, str, "log_level")

    @timing
    @log_errors
    async def _monitor_events():
        try:
            # Set up services using the handler
            await handler.setup_services(nats_url, log_level)
            
            # Log with structured logger
            handler.structured_logger.info(
                "Monitoring events from NATS",
                nats_url=nats_url,
                operation="event_monitoring"
            )
            
            handler.console.print("[yellow]Event monitoring not yet implemented.[/yellow]")
            handler.console.print(
                "This command will display real-time events from the naq system."
            )

        except Exception as e:
            handler.structured_logger.error(
                "Failed to monitor events",
                nats_url=nats_url,
                error=str(e),
                error_type=type(e).__name__
            )
            handler.console.print(f"[red]Error: {str(e)}[/red]")
            raise
        finally:
            await handler.cleanup_services()

    # Run the async function
    asyncio.run(_monitor_events())


@event_app.command("history")
def event_history(
    nats_url: str = typer.Option(
        DEFAULT_NATS_URL,
        "--nats-url",
        "-u",
        help="URL of the NATS server.",
        envvar="NAQ_NATS_URL",
    ),
    limit: int = typer.Option(
        100,
        "--limit",
        "-n",
        help="Maximum number of events to display.",
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
    Display historical events from the naq system.
    """
    setup_logging(log_level if log_level else None)
    handler = EventCommandHandler()
    
    # Validate parameters
    handler.validate_common_parameters(nats_url, log_level, limit)
    
    # Ensure correct types
    nats_url = ensure_type(nats_url, str, "nats_url")
    limit = ensure_type(limit, int, "limit")
    if log_level is not None:
        log_level = ensure_type(log_level, str, "log_level")

    @timing
    @log_errors
    async def _get_event_history():
        try:
            # Set up services using the handler
            await handler.setup_services(nats_url, log_level)
            
            # Log with structured logger
            handler.structured_logger.info(
                "Fetching event history from NATS",
                nats_url=nats_url,
                limit=limit,
                operation="event_history"
            )
            
            handler.console.print("[yellow]Event history not yet implemented.[/yellow]")
            handler.console.print(
                f"This command will display the last {limit} events "
                "from the naq system."
            )

        except Exception as e:
            handler.structured_logger.error(
                "Failed to fetch event history",
                nats_url=nats_url,
                limit=limit,
                error=str(e),
                error_type=type(e).__name__
            )
            handler.console.print(f"[red]Error: {str(e)}[/red]")
            raise
        finally:
            await handler.cleanup_services()

    # Run the async function
    asyncio.run(_get_event_history())


@event_app.command("stats")
def event_stats(
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
    setup_logging(log_level if log_level else None)
    handler = EventCommandHandler()
    
    # Validate parameters
    handler.validate_common_parameters(nats_url, log_level)
    
    # Ensure correct types
    nats_url = ensure_type(nats_url, str, "nats_url")
    if log_level is not None:
        log_level = ensure_type(log_level, str, "log_level")

    @timing
    @log_errors
    async def _get_event_stats():
        try:
            # Set up services using the handler
            await handler.setup_services(nats_url, log_level)
            
            # Log with structured logger
            handler.structured_logger.info(
                "Fetching event statistics from NATS",
                nats_url=nats_url,
                operation="event_stats"
            )
            
            handler.console.print("[yellow]Event statistics not yet implemented.[/yellow]")
            handler.console.print(
                "This command will display statistics about events in the naq system."
            )

        except Exception as e:
            handler.structured_logger.error(
                "Failed to fetch event statistics",
                nats_url=nats_url,
                error=str(e),
                error_type=type(e).__name__
            )
            handler.console.print(f"[red]Error: {str(e)}[/red]")
            raise
        finally:
            await handler.cleanup_services()

    # Run the async function
    asyncio.run(_get_event_stats())


@event_app.command("worker")
def worker_events(
    worker_id: str = typer.Argument(
        ..., help="The ID of the worker to monitor events for"
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
    Monitor events for a specific worker.
    """
    setup_logging(log_level if log_level else None)
    handler = EventCommandHandler()
    
    # Validate parameters
    handler.validate_common_parameters(nats_url, log_level, worker_id=worker_id)
    
    # Ensure correct types
    worker_id = ensure_type(worker_id, str, "worker_id")
    nats_url = ensure_type(nats_url, str, "nats_url")
    if log_level is not None:
        log_level = ensure_type(log_level, str, "log_level")

    @timing
    @log_errors
    async def _monitor_worker_events():
        try:
            # Set up services using the handler
            await handler.setup_services(nats_url, log_level)
            
            # Log with structured logger
            handler.structured_logger.info(
                "Monitoring events for worker",
                nats_url=nats_url,
                worker_id=worker_id,
                operation="worker_events"
            )
            
            handler.console.print(
                "[yellow]Worker event monitoring not yet implemented.[/yellow]"
            )
            handler.console.print(
                f"This command will display events for worker {worker_id}."
            )

        except Exception as e:
            handler.structured_logger.error(
                "Failed to monitor worker events",
                nats_url=nats_url,
                worker_id=worker_id,
                error=str(e),
                error_type=type(e).__name__
            )
            handler.console.print(f"[red]Error: {str(e)}[/red]")
            raise
        finally:
            await handler.cleanup_services()

    # Run the async function
    asyncio.run(_monitor_worker_events())
