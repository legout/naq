"""System utility commands for the naq CLI."""

import uvicorn
from typing import Optional
from loguru import logger
from rich.console import Console

import typer

from naq import __version__
from naq.utils import setup_logging

# Create a Typer app for system commands
system_app = typer.Typer(
    name="system",
    help="System utility commands for naq.",
    no_args_is_help=True,
)

# Create a shared console instance for Rich output
console = Console()


def version_callback(value: bool):
    """Callback for the version option."""
    if value:
        console.print(f"[cyan]naq[/cyan] version: [bold]{__version__}[/bold]")
        raise typer.Exit()


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
            "Set logging level for the dashboard server. "
            "Defaults to NAQ_LOG_LEVEL env var or CRITICAL."
        ),
    ),
):
    """
    Starts the NAQ web dashboard (requires 'dashboard' extras).
    """
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


@system_app.command()
def version():
    """
    Show the application's version and exit.
    """
    console.print(f"[cyan]naq[/cyan] version: [bold]{__version__}[/bold]")


@system_app.command()
def health(
    nats_url: str = typer.Option(
        "nats://localhost:4222",
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
):
    """
    Check the health of the NATS connection and naq system.
    """
    setup_logging(None)
    logger.info(f"Checking health of NATS at {nats_url}")

    try:
        # Import here to avoid circular imports
        from naq.connection import get_nats_connection
        import asyncio

        async def check_health():
            try:
                nc = await asyncio.wait_for(
                    get_nats_connection(url=nats_url), timeout=timeout
                )
                await nc.close()
                return True, "NATS connection successful"
            except Exception as e:
                return False, f"NATS connection failed: {str(e)}"

        is_healthy, message = asyncio.run(check_health())

        if is_healthy:
            console.print(f"[green]✓[/green] [bold]System Health:[/bold] {message}")
        else:
            console.print(f"[red]✗[/red] [bold]System Health:[/bold] {message}")
            raise typer.Exit(code=1)

    except Exception as e:
        logger.exception(f"Health check failed: {e}")
        console.print(f"[red]✗[/red] [bold]System Health:[/bold] Error: {str(e)}")
        raise typer.Exit(code=1)
