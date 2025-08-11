# src/naq/cli/system_commands.py
"""
System utility commands for the NAQ CLI.

This module contains commands for system administration, utilities,
and the web dashboard.
"""

from typing import Optional

import typer
from loguru import logger

from .main import console
from ..utils import setup_logging


# Create system command group
system_app = typer.Typer(help="System administration and utility commands")


@system_app.command()
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
        help="Set logging level for the dashboard server. Defaults to NAQ_LOG_LEVEL env var or CRITICAL.",
    ),
    # Add NATS URL option if dashboard needs direct NATS access later
    # nats_url: Optional[str] = typer.Option(...)
):
    """
    Start the NAQ web dashboard (requires 'dashboard' extras).
    """
    try:
        import uvicorn  # Use uvicorn to run Sanic

        setup_logging(log_level if log_level else None)  # Setup naq logging if needed
        logger.info(f"Starting NAQ Dashboard server on http://{host}:{port}")
        logger.info("Ensure NATS server is running and accessible.")

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
    except ImportError:
        console.print("[red]Error:[/red] Dashboard dependencies not installed.")
        console.print("Please run: [bold cyan]pip install naq[dashboard][/bold cyan]")
        raise typer.Exit(code=1)


@system_app.command()
def config(
    show: bool = typer.Option(
        False,
        "--show",
        help="Show current configuration values.",
    ),
    validate: bool = typer.Option(
        False,
        "--validate",
        help="Validate configuration settings.",
    ),
):
    """
    Show or validate NAQ configuration settings.
    """
    console.print("[yellow]Note:[/yellow] Configuration management commands will be implemented")
    console.print("after the YAML configuration system is added in a future task.")
    
    if show:
        console.print("\n[bold]Current Configuration:[/bold]")
        console.print("This would show current settings from environment variables and config files.")
    
    if validate:
        console.print("\n[bold]Configuration Validation:[/bold]")
        console.print("This would validate all configuration settings and report any issues.")