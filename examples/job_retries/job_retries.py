import asyncio
import os
import pathlib
import sys
from naq import enqueue, setup_logging

# Define a temporary file to track attempts
COUNTER_FILE = pathlib.Path(__file__).parent / "retry_counter.tmp"
MAX_FAILURES = 2 # Fail the first 2 times, succeed on the 3rd attempt (retry #2)
DEFAULT_NATS_URL = os.getenv("NAQ_NATS_URL", "nats://localhost:4222")
async def flaky_task(message: str):
    """
    A task that simulates transient failures.
    It reads a counter from a file, increments it, and fails if the count
    is below MAX_FAILURES.
    """
    attempt = 0
    try:
        if COUNTER_FILE.exists():
            attempt = int(COUNTER_FILE.read_text())
        COUNTER_FILE.write_text(str(attempt + 1))
        print(f"--- Running flaky_task (Attempt: {attempt + 1}) ---")
    except Exception as e:
        print(f"Error managing counter file: {e}", file=sys.stderr)
        # Decide how to handle file errors, maybe raise to prevent infinite loops
        raise

    if attempt < MAX_FAILURES:
        print(f"Task failed on attempt {attempt + 1}. Raising exception to trigger retry.")
        raise ValueError(f"Simulated failure on attempt {attempt + 1}")
    else:
        print(f"Task succeeded on attempt {attempt + 1}: {message}")
        # Clean up the counter file on success
        if COUNTER_FILE.exists():
            COUNTER_FILE.unlink()
        return f"Success after {attempt + 1} attempts"

async def main():
    # Ensure counter file is reset before enqueueing
    if COUNTER_FILE.exists():
        COUNTER_FILE.unlink()
    COUNTER_FILE.write_text("0")
    print(f"Reset counter file: {COUNTER_FILE}")

    # Enqueue the flaky task with retry settings
    job = await enqueue(
        flaky_task,
        message="Hello from retry example!",
        queue_name="naq_default_queue", # Explicitly use the default queue
        max_retries=3,
        retry_delay=[2, 4, 6], # Retry after 2s, then 4s, then 6s
    )
if __name__ == "__main__":
    # Setup logging to see DEBUG messages from naq, including retry info
    setup_logging(level="DEBUG")
    asyncio.run(main())