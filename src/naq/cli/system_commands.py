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
    config_path: Optional[str] = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to configuration file.",
    ),
    format_output: str = typer.Option(
        "yaml",
        "--format",
        "-f",
        help="Output format for configuration display (yaml, json).",
    ),
    sources: bool = typer.Option(
        False,
        "--sources",
        help="Show configuration source information.",
    ),
):
    """
    Show, validate, and manage NAQ configuration settings.
    """
    try:
        from ..config import (
            load_config, 
            get_config, 
            get_config_source_info, 
            get_effective_config_dict,
            validate_config_file
        )
        from ..exceptions import ConfigurationError
        import yaml
        import json
        from rich.syntax import Syntax
        from rich.table import Table
        
        # Handle validation mode
        if validate:
            console.print("[bold]Validating Configuration...[/bold]")
            try:
                if config_path:
                    validate_config_file(config_path)
                    console.print(f"[green]✓[/green] Configuration file '{config_path}' is valid")
                else:
                    config = load_config(validate=True)
                    console.print("[green]✓[/green] Current configuration is valid")
            except ConfigurationError as e:
                console.print(f"[red]✗[/red] Configuration validation failed:")
                console.print(str(e))
                raise typer.Exit(code=1)
            except Exception as e:
                console.print(f"[red]✗[/red] Error validating configuration: {e}")
                raise typer.Exit(code=1)
            return
        
        # Handle sources mode
        if sources:
            console.print("[bold]Configuration Sources:[/bold]")
            source_info = get_config_source_info()
            
            table = Table(title="Configuration Source Information")
            table.add_column("Property", style="cyan")
            table.add_column("Value", style="green")
            
            table.add_row("Loaded", str(source_info.get("loaded", False)))
            if source_info.get("config_path"):
                table.add_row("Config File", source_info["config_path"])
            
            table.add_row("Search Paths", "\n".join(source_info.get("search_paths", [])))
            table.add_row("Found Files", "\n".join(source_info.get("found_files", [])))
            
            console.print(table)
            return
        
        # Handle show mode (default)
        if show or not (validate or sources):
            console.print("[bold]Current Configuration:[/bold]")
            
            try:
                # Load configuration
                if config_path:
                    config = load_config(config_path)
                else:
                    config = get_config()
                
                # Get configuration as dictionary
                config_dict = get_effective_config_dict()
                
                # Format output
                if format_output.lower() == "json":
                    formatted = json.dumps(config_dict, indent=2)
                    syntax = Syntax(formatted, "json", theme="monokai", line_numbers=True)
                    console.print(syntax)
                else:  # yaml (default)
                    formatted = yaml.dump(config_dict, default_flow_style=False, sort_keys=True)
                    syntax = Syntax(formatted, "yaml", theme="monokai", line_numbers=True)
                    console.print(syntax)
                
            except ConfigurationError as e:
                console.print(f"[red]Error:[/red] {e}")
                raise typer.Exit(code=1)
            except Exception as e:
                console.print(f"[red]Error loading configuration:[/red] {e}")
                raise typer.Exit(code=1)
                
    except ImportError:
        console.print("[red]Error:[/red] Configuration system not available.")
        console.print("The YAML configuration system may not be fully installed.")
        raise typer.Exit(code=1)


@system_app.command("config-init")
def config_init(
    environment: str = typer.Option(
        "development",
        "--environment",
        "-e",
        help="Environment to create config for (development, production, testing, minimal).",
    ),
    output_path: Optional[str] = typer.Option(
        None,
        "--output",
        "-o",
        help="Output path for configuration file (default: ./naq.yaml).",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Overwrite existing configuration file.",
    ),
):
    """
    Initialize a new NAQ configuration file.
    """
    try:
        from ..config.defaults import get_config_template
        import yaml
        from pathlib import Path
        
        # Determine output path
        if output_path is None:
            output_path = "./naq.yaml"
        
        output_file = Path(output_path)
        
        # Check if file exists and force flag
        if output_file.exists() and not force:
            console.print(f"[red]Error:[/red] Configuration file '{output_path}' already exists.")
            console.print("Use --force to overwrite, or specify a different output path.")
            raise typer.Exit(code=1)
        
        # Get template configuration
        try:
            config_template = get_config_template(environment)
        except Exception:
            console.print(f"[red]Error:[/red] Unknown environment '{environment}'.")
            console.print("Available environments: development, production, testing, minimal")
            raise typer.Exit(code=1)
        
        # Write configuration file
        try:
            with open(output_file, 'w') as f:
                f.write(f"# NAQ Configuration - {environment.title()} Environment\n")
                f.write(f"# Generated by NAQ CLI\n\n")
                yaml.dump(config_template, f, default_flow_style=False, sort_keys=True)
            
            console.print(f"[green]✓[/green] Configuration file created: '{output_path}'")
            console.print(f"Environment: {environment}")
            console.print("\nTo use this configuration:")
            console.print("1. Edit the file to match your requirements")
            console.print("2. Set NAQ_ENVIRONMENT environment variable if using environment overrides")
            console.print("3. Run 'naq system config --validate' to check your configuration")
            
        except Exception as e:
            console.print(f"[red]Error:[/red] Failed to write configuration file: {e}")
            raise typer.Exit(code=1)
            
    except ImportError:
        console.print("[red]Error:[/red] Configuration system not available.")
        raise typer.Exit(code=1)


# Backward compatibility alias for tests
system_commands = system_app