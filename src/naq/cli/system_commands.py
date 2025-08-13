"""
System CLI commands.

This module contains all CLI commands related to system utilities.
"""

from typing import Optional

import typer
from loguru import logger

from ..utils import setup_logging

system_app = typer.Typer(help="System utility commands")


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
        help=(
            "Set logging level for the dashboard server. Defaults to NAQ_LOG_LEVEL "
            "env var or CRITICAL."
        ),
    ),
    # Add NATS URL option if dashboard needs direct NATS access later
    # nats_url: Optional[str] = typer.Option(...)
):
    """
    Starts the NAQ web dashboard (requires 'dashboard' extras).
    """
    try:
        import uvicorn  # Use uvicorn to run Sanic
    except ImportError:
        typer.echo("[red]Error:[/red] Dashboard dependencies not installed.")
        typer.echo("Please run: [bold cyan]pip install naq[dashboard][/bold cyan]")
        raise typer.Exit(code=1)

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
