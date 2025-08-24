"""Main CLI application for naq.

This module serves as the main entry point for the naq CLI application.
It initializes the Typer app and registers all sub-commands.
"""

from typing import Optional
import importlib

import typer
from rich.console import Console

from naq import __version__
from naq.utils.decorators import timing, log_errors
from naq.utils.logging import StructuredLogger
from naq.utils.validation import validate_parameter
from naq.utils.error_handling import ErrorHandler, create_error_context

# Create the main Typer application
app = typer.Typer(
    name="naq",
    help="A simple NATS-based queueing system, similar to RQ.",
    add_completion=False,
)

# Create a structured logger for CLI operations
cli_logger = StructuredLogger("naq.cli")

# Create a shared console instance for Rich output (for user-facing messages)
console = Console()


@timing(logger_instance=cli_logger)
@log_errors(logger_instance=cli_logger, reraise=True)
def version_callback(value: bool) -> None:
    """Callback function to display version information."""
    validate_parameter(value, "value", not_none=True)

    if value:
        cli_logger.info(
            "Displaying version information", version=__version__, component="cli"
        )
        # Keep the Rich console for user-facing output
        console.print(f"[cyan]naq[/cyan] version: [bold]{__version__}[/bold]")
        raise typer.Exit()


@timing(logger_instance=cli_logger)
@log_errors(logger_instance=cli_logger, reraise=False)
@app.callback()
def main(
    version: Optional[bool] = typer.Option(
        None,
        "--version",
        callback=version_callback,
        is_eager=True,
        help="Show the application's version and exit.",
    ),
) -> None:
    """
    naq CLI entry point.
    """
    cli_logger.info("NAQ CLI started", component="cli")


def _register_subcommands() -> None:
    """Register all available subcommands with the main CLI app."""
    error_handler = ErrorHandler(logger_instance=cli_logger)

    subcommands = [
        ("worker_commands", "worker", "Worker-related commands"),
        ("job_commands", "job", "Job and queue management commands"),
        ("scheduler_commands", "scheduler", "Scheduler-related commands"),
        ("event_commands", "event", "Event monitoring commands"),
        ("system_commands", "system", "System and utility commands"),
    ]

    for module_name, command_name, help_text in subcommands:
        try:
            # Use importlib instead of __import__ for better control
            full_module_name = (
                f"{'.'.join(__package__.split('.')[:-1])}.cli.{module_name}"
                if __package__
                else f"naq.cli.{module_name}"
            )
            module = importlib.import_module(full_module_name)
            command_app = getattr(module, f"{command_name}_app")
            app.add_typer(command_app, name=command_name, help=help_text)
            cli_logger.info(
                f"Registered {command_name} subcommand",
                component="cli",
                subcommand=command_name,
            )
        except ImportError as e:
            error_context = create_error_context(f"import_{module_name}")
            error_handler.handle_error(e, context=error_context, reraise=False)
        except AttributeError as e:
            error_context = create_error_context(f"register_{command_name}")
            error_handler.handle_error(e, context=error_context, reraise=False)


def initialize_cli() -> None:
    """Initialize the CLI application with proper error handling."""
    try:
        cli_logger.info("Initializing NAQ CLI", component="cli")
        _register_subcommands()
        cli_logger.info("NAQ CLI initialization completed", component="cli")
    except Exception as e:
        error_context = create_error_context("cli_initialization")
        cli_logger.error(
            f"Failed to initialize NAQ CLI: {str(e)}",
            component="cli",
            error_context=error_context,
        )
        raise


# Initialize the CLI
initialize_cli()
