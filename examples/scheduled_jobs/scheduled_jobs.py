import asyncio
from datetime import datetime, timedelta, timezone

from naq import enqueue_at_sync, enqueue_in_sync, setup_logging

# Define a simple function to be executed by the jobs
async def print_message(message: str):
    """Prints a message to the console."""
    print(f"Job executed: {message}")

async def main():
    # Schedule a job to run at a specific time (e.g., 5 seconds from now)
    run_at = datetime.now(timezone.utc) + timedelta(seconds=5)
    job_at = await enqueue_at_sync(print_message, run_at, message="Job scheduled with enqueue_at_sync")
    print(f"Scheduled job {job_at.id} to run at {run_at.isoformat()} using enqueue_at_sync.")

    # Schedule a job to run after a specific delay (e.g., 10 seconds from now)
    run_in = timedelta(seconds=10)
    job_in = await enqueue_in_sync(print_message, run_in, message="Job scheduled with enqueue_in_sync")
    print(f"Scheduled job {job_in.id} to run in {run_in} using enqueue_in_sync.")

if __name__ == "__main__":
    # Setup basic logging for NAQ
    setup_logging()
    asyncio.run(main())