import typer
from loguru import logger
from typing import Optional

from rich.console import Console

# Create a shared console instance
console = Console()
from ..settings import DEFAULT_NATS_URL
from ..utils import setup_logging

# Create system app
system_app = typer.Typer(help="System and utility commands")


@system_app.command("dashboard")
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
    Starts the NAQ web dashboard (requires 'dashboard' extras).
    """
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


@system_app.command("version")
def version():
    """
    Show the application's version.
    """
    from .. import __version__
    console.print(f"[cyan]naq[/cyan] version: [bold]{__version__}[/bold]")


@system_app.command("config")
def config(
    log_level: Optional[str] = typer.Option(
        None,
        "--log-level",
        "-l",
        help="Set logging level (e.g., DEBUG, INFO, WARNING, ERROR). Defaults to NAQ_LOG_LEVEL env var or CRITICAL.",
    ),
):
    """
    Show current configuration.
    """
    setup_logging(log_level if log_level else None)
    
    from rich.table import Table
    from ..settings import (
        DEFAULT_NATS_URL,
        DEFAULT_QUEUE_NAME,
        DEFAULT_WORKER_TTL_SECONDS,
        SCHEDULED_JOBS_KV_NAME,
        NAQ_PREFIX,
    )
    
    table = Table(title="NAQ Configuration", show_header=True, header_style="bold cyan")
    table.add_column("Setting", style="cyan")
    table.add_column("Value", style="white")
    
    table.add_row("NATS URL", DEFAULT_NATS_URL)
    table.add_row("Default Queue", DEFAULT_QUEUE_NAME)
    table.add_row("Worker TTL (seconds)", str(DEFAULT_WORKER_TTL_SECONDS))
    table.add_row("Scheduled Jobs KV Store", SCHEDULED_JOBS_KV_NAME)
    table.add_row("NAQ Prefix", NAQ_PREFIX)
    
    console.print(table)