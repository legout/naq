"""System utility commands for the naq CLI."""

import yaml
from pathlib import Path
from typing import Any, Dict, Optional
from rich.console import Console

import typer

from naq import __version__
from naq.utils.logging import setup_logging
from naq.utils.decorators import timing, log_errors
from naq.utils.logging import StructuredLogger
from naq.utils.serialization import SerializationHelper
from naq.services.base import ServiceManager, ServiceConfig
from naq.services.connection import ConnectionService
from naq.settings import DEFAULT_NATS_URL
from naq.config import load_config, get_config, ConfigValidator

# Create a Typer app for system commands
system_app = typer.Typer(
    name="system",
    help="System utility commands for naq.",
    no_args_is_help=True,
)

# Create a Typer app for config commands
config_app = typer.Typer(name="config", help="Manage NAQ configuration.")
system_app.add_typer(config_app)

# Create a shared console instance for Rich output
console = Console()


def version_callback(value: bool):
    """Callback for the version option."""
    if value:
        console.print(f"[cyan]naq[/cyan] version: [bold]{__version__}[/bold]")
        raise typer.Exit()


@system_app.command()
@timing()
@log_errors()
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
    log_level: Optional[str] = typer.Option(
        None,
        "--log-level",
        "-l",
        help=(
            "Set logging level for the dashboard server. "
            "Defaults to NAQ_LOG_LEVEL env var or CRITICAL."
        ),
    ),
) -> None:
    """
    Starts the NAQ web dashboard (requires 'dashboard' extras).
    """
    try:
        import uvicorn  # Use uvicorn to run Sanic
    except ImportError:
        console.print("[red]Error:[/red] Dashboard dependencies not installed.")
        console.print("Please run: [bold cyan]pip install naq[dashboard][/bold cyan]")
        raise typer.Exit(code=1)

    # Create structured logger
    structured_logger = StructuredLogger("naq.cli.system_commands")

    setup_logging(log_level if log_level else None)  # Setup naq logging if needed
    structured_logger.info(
        f"Starting NAQ Dashboard server on http://{host}:{port}",
        host=host,
        port=port,
        log_level=log_level,
    )
    structured_logger.info(
        "Ensure NATS server is running and accessible.", operation="dashboard_startup"
    )

    # Configure uvicorn logging level based on input
    uvicorn_log_level = log_level.lower() if log_level else "critical"

    # Run Sanic app using uvicorn
    uvicorn.run(
        "naq.dashboard.app:app",  # Path to the Sanic app instance
        host=host,
        port=port,
        log_level=uvicorn_log_level,
        reload=False,  # Disable auto-reload for production-like command
        # workers=1 # Can configure workers if needed
    )


@system_app.command()
@timing()
def version() -> None:
    """
    Show the application's version and exit.
    """
    console.print(f"[cyan]naq[/cyan] version: [bold]{__version__}[/bold]")


@system_app.command()
@timing()
@log_errors()
def health(
    nats_url: str = typer.Option(
        DEFAULT_NATS_URL,
        "--nats-url",
        "-u",
        help="URL of the NATS server to check.",
        envvar="NAQ_NATS_URL",
    ),
    timeout: float = typer.Option(
        5.0,
        "--timeout",
        "-t",
        help="Timeout in seconds for the health check.",
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
    Check the health of the NATS connection and naq system.
    """
    # Create structured logger
    structured_logger = StructuredLogger("naq.cli.system_commands")

    setup_logging(log_level if log_level else None)
    structured_logger.info(
        f"Checking health of NATS at {nats_url}",
        nats_url=nats_url,
        timeout=timeout,
        log_level=log_level,
    )

    try:
        # Import here to avoid circular imports
        import asyncio

        async def check_health():
            # Create service manager with configuration
            service_manager = ServiceManager(
                config=ServiceConfig(
                    nats_url=nats_url, custom_settings={"log_level": log_level}
                )
            )

            try:
                # Register required services
                connection_service = await service_manager.register_service(
                    "connection", ConnectionService, initialize=True
                )

                # Use the connection service to test connection
                is_connected = await connection_service.test_connection()
                if is_connected:
                    return True, "NATS connection successful"
                else:
                    return False, "NATS connection not established"
            except Exception as e:
                return False, f"NATS connection failed: {str(e)}"
            finally:
                await service_manager.cleanup_all()

        is_healthy, message = asyncio.run(check_health())

        if is_healthy:
            console.print(f"[green]✓[/green] [bold]System Health:[/bold] {message}")
            structured_logger.info(
                "System health check passed", status="healthy", message=message
            )
        else:
            console.print(f"[red]✗[/red] [bold]System Health:[/bold] {message}")
            structured_logger.error(
                "System health check failed", status="unhealthy", message=message
            )
            raise typer.Exit(code=1)

    except Exception as e:
        structured_logger.error(
            f"Health check failed: {e}",
            error=str(e),
            nats_url=nats_url,
            timeout=timeout,
        )
        console.print(f"[red]✗[/red] [bold]System Health:[/bold] Error: {str(e)}")
        raise typer.Exit(code=1)


@config_app.command("show")
@timing()
@log_errors()
def config_show(
    ctx: typer.Context,
    config_path: Optional[Path] = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to a specific configuration file to load.",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
    ),
) -> None:
    """
    Displays the current effective NAQ configuration.
    """
    # Create structured logger
    structured_logger = StructuredLogger("naq.cli.system_commands")

    try:
        cfg = load_config(
            config_path=str(config_path) if config_path else None, validate=False
        )
        # Use SerializationHelper for JSON serialization
        config_dict = cfg.to_dict()
        serialized_config = SerializationHelper.safe_serialize(config_dict, "json")
        console.print_json(serialized_config)

        structured_logger.info(
            "Configuration displayed successfully",
            config_path=str(config_path) if config_path else "default",
        )
    except Exception as e:
        structured_logger.error(
            f"Error loading configuration: {e}",
            config_path=str(config_path) if config_path else "default",
            error=str(e),
        )
        console.print(f"[bold red]Error loading configuration:[/bold red] {e}")
        raise typer.Exit(code=1)


@config_app.command("validate")
@timing()
@log_errors()
def config_validate(
    ctx: typer.Context,
    config_path: Optional[Path] = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to a specific configuration file to validate.",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
    ),
) -> None:
    """
    Validates the NAQ configuration against its schema.
    """
    # Create structured logger
    structured_logger = StructuredLogger("naq.cli.system_commands")

    try:
        # Use ConfigValidator directly for validation
        config_path_str = str(config_path) if config_path else None
        validator = ConfigValidator()

        # Load configuration without validation first
        cfg = load_config(config_path=config_path_str, validate=False)

        # Convert to dict for validation
        config_dict = cfg.to_dict()

        # Validate using ConfigValidator
        validator.validate(config_dict)

        console.print("[bold green]Configuration is valid![/bold green]")
        structured_logger.info(
            "Configuration validation passed", config_path=config_path_str or "default"
        )
    except Exception as e:
        structured_logger.error(
            f"Configuration validation failed: {e}",
            config_path=str(config_path) if config_path else "default",
            error=str(e),
        )
        console.print(f"[bold red]Configuration validation failed:[/bold red] {e}")
        raise typer.Exit(code=1)


@system_app.command("generate-config")
@timing()
@log_errors()
def generate_config_cmd(
    ctx: typer.Context,
    output: Path = typer.Option(
        "naq-config.yaml",
        "--output",
        "-o",
        help="Output path for the generated configuration file.",
        file_okay=True,
        dir_okay=False,
        writable=True,
        resolve_path=True,
    ),
    environment: str = typer.Option(
        "default",
        "--environment",
        "-e",
        help=(
            "Environment template to generate (e.g., 'default', "
            "'development', 'production')."
        ),
    ),
) -> None:
    """
    Generates an example NAQ configuration file.
    """
    # Create structured logger
    structured_logger = StructuredLogger("naq.cli.system_commands")

    try:
        # Get a default-filled NAQConfig and convert it to dict
        default_config_obj = get_config()
        config_dict = default_config_obj.to_dict()

        # Basic cleanup: remove keys with None values for cleaner YAML
        def clean_dict(d: Dict[str, Any]) -> Dict[str, Any]:
            return {k: v for k, v in d.items() if v is not None}

        cleaned_config = clean_dict(config_dict)

        # Use SerializationHelper for JSON serialization (though we're writing YAML)
        # This ensures consistent serialization approach
        SerializationHelper.safe_serialize(cleaned_config, "json")

        with open(output, "w") as f:
            yaml.dump(cleaned_config, f, sort_keys=False, indent=2)

        console.print(
            f"[bold green]Example configuration generated at:[/bold green] {output}"
        )
        structured_logger.info(
            "Example configuration generated successfully",
            output_path=str(output),
            environment=environment,
        )
    except Exception as e:
        structured_logger.error(
            f"Error generating configuration: {e}",
            output_path=str(output),
            environment=environment,
            error=str(e),
        )
        console.print(f"[bold red]Error generating configuration:[/bold red] {e}")
        raise typer.Exit(code=1)
