import asyncio
import logging
import sys
from typing import List, Optional

import typer
from loguru import logger # Use loguru consistently

from .worker import Worker
from .settings import DEFAULT_NATS_URL
from . import __version__
from .queue import Queue # Import Queue
from .scheduler import Scheduler # Import Scheduler

app = typer.Typer(
    name="naq",
    help="A simple NATS-based queueing system, similar to RQ.",
    add_completion=False,
)

# --- Helper Functions ---

def version_callback(value: bool):
    if value:
        print(f"naq version: {__version__}")
        raise typer.Exit()
        
def setup_logging(level: str = "INFO"):
    """Configures logging based on the provided level string using loguru."""
    logger.remove()  # Remove default handler
    logger.add(
        sys.stdout,
        level=level.upper(),
        format="{time} - {name} - {level} - {message}",
        colorize=True,
    )
    # Optionally silence overly verbose libraries if needed
    # logging.getLogger("nats").setLevel(logging.WARNING)


# --- CLI Commands ---

@app.command()
def worker(
    queues: List[str] = typer.Argument(..., help="The names of the queues to listen to."),
    nats_url: Optional[str] = typer.Option(
        DEFAULT_NATS_URL,
        "--nats-url",
        "-u",
        help="URL of the NATS server.",
        envvar="NAQ_NATS_URL", # Allow setting via env var
    ),
    concurrency: int = typer.Option(
        10,
        "--concurrency",
        "-c",
        min=1,
        help="Maximum number of concurrent jobs to process.",
    ),
    name: Optional[str] = typer.Option(
        None,
        "--name",
        "-n",
        help="Optional name for this worker instance.",
    ),
     log_level: str = typer.Option(
        "INFO",
        "--log-level",
        "-l",
        help="Set logging level (e.g., DEBUG, INFO, WARNING, ERROR).",
    ),
):
    """
    Starts a naq worker process to listen for and execute jobs on the specified queues.
    """
    setup_logging(log_level)
    # Use loguru directly
    logger.info(f"Starting worker '{name or 'default'}' for queues: {queues}")
    logger.info(f"NATS URL: {nats_url}")
    logger.info(f"Concurrency: {concurrency}")

    w = Worker(
        queues=queues,
        nats_url=nats_url,
        concurrency=concurrency,
        worker_name=name,
    )
    try:
        asyncio.run(w.run())
    except KeyboardInterrupt:
        logger.info("Worker interrupted by user (KeyboardInterrupt). Shutting down.")
    except Exception as e:
        logger.exception(f"Worker failed unexpectedly: {e}") # Use logger.exception for stack trace
        raise typer.Exit(code=1)
    finally:
        logger.info("Worker process finished.")


@app.command()
def purge(
    queues: List[str] = typer.Argument(..., help="The names of the queues to purge."),
    nats_url: Optional[str] = typer.Option(
        DEFAULT_NATS_URL,
        "--nats-url",
        "-u",
        help="URL of the NATS server.",
        envvar="NAQ_NATS_URL",
    ),
    log_level: str = typer.Option(
        "INFO",
        "--log-level",
        "-l",
        help="Set logging level (e.g., DEBUG, INFO, WARNING, ERROR).",
    ),
):
    """
    Removes all jobs from the specified queues.
    """
    setup_logging(log_level)
    logger.info(f"Attempting to purge queues: {queues}")
    logger.info(f"Using NATS URL: {nats_url}")

    async def _purge_queues():
        total_purged = 0
        for queue_name in queues:
            try:
                q = Queue(name=queue_name, nats_url=nats_url)
                purged_count = await q.purge()
                logger.info(f"Successfully purged queue '{queue_name}'. Removed {purged_count} jobs.")
                total_purged += purged_count
            except Exception as e:
                logger.error(f"Failed to purge queue '{queue_name}': {e}")
                # Optionally exit or continue with other queues
        logger.info(f"Purge operation finished. Total jobs removed: {total_purged}")
        # Close connection if opened by Queue instances
        from .connection import close_nats_connection
        await close_nats_connection()


    try:
        asyncio.run(_purge_queues())
    except Exception as e:
        logger.exception(f"Purge command failed unexpectedly: {e}")
        raise typer.Exit(code=1)
    finally:
        logger.info("Purge process finished.")


@app.command()
def scheduler(
    nats_url: Optional[str] = typer.Option(
        DEFAULT_NATS_URL,
        "--nats-url",
        "-u",
        help="URL of the NATS server.",
        envvar="NAQ_NATS_URL",
    ),
    poll_interval: float = typer.Option(
        1.0,
        "--poll-interval",
        "-i",
        min=0.1,
        help="How often (in seconds) to check for scheduled jobs.",
    ),
    log_level: str = typer.Option(
        "INFO",
        "--log-level",
        "-l",
        help="Set logging level (e.g., DEBUG, INFO, WARNING, ERROR).",
    ),
):
    """
    Starts the naq scheduler process to enqueue scheduled jobs.
    """
    setup_logging(log_level)
    logger.info("Starting naq scheduler...")
    logger.info(f"NATS URL: {nats_url}")
    logger.info(f"Polling Interval: {poll_interval}s")

    s = Scheduler(nats_url=nats_url, poll_interval=poll_interval)
    try:
        asyncio.run(s.run())
    except KeyboardInterrupt:
        logger.info("Scheduler interrupted by user (KeyboardInterrupt). Shutting down.")
    except Exception as e:
        logger.exception(f"Scheduler failed unexpectedly: {e}")
        raise typer.Exit(code=1)
    finally:
        logger.info("Scheduler process finished.")


# --- Version Option ---

@app.callback()
def main(
    version: Optional[bool] = typer.Option(
        None, "--version", callback=version_callback, is_eager=True, help="Show the application's version and exit."
    ),
):
    """
    naq CLI entry point.
    """
    pass


# --- Main Execution Guard ---
# (Typer handles this implicitly when run as a script via the entry point)
