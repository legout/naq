import datetime
#import time # Keep commented or remove if truly unused

from naq import schedule_sync, setup_logging

# Configure logging
setup_logging()

def log_timestamp(message: str):
    """A simple function that logs a message and the current timestamp."""
    print(f"{message}: {datetime.datetime.now()}")

if __name__ == "__main__":
    print("Scheduling recurring jobs using naq.schedule_sync...")
    print("-" * 40)

    # --- Interval-based recurring job ---
    print("Scheduling a job to run every 15 seconds, 5 times...")
    interval_job = schedule_sync(
        log_timestamp,
        "Interval job",
        interval=datetime.timedelta(seconds=15),
        repeat=5,
    )
    print(f"Scheduled interval job with ID: {interval_job.id}")
    print("-" * 40)

    # --- Cron-based recurring job ---
    # Note: Requires the 'croniter' library (pip install croniter)
    print("Scheduling a job to run every minute using a cron string ('* * * * *')...")
    try:
        cron_job = schedule_sync(
            log_timestamp,
            "Cron job",
            cron="* * * * *",  # Every minute
            repeat=None,      # Repeat indefinitely
        )
        print(f"Scheduled cron job with ID: {cron_job.id}")
    except ImportError:
        pass # Indicate empty block
    print("-" * 40) # This line should be outside the except block, same level as try/except

    # Keep the main script running for a bit to allow jobs to be picked up by the scheduler/worker
    # In a real application, this script might exit, but the scheduler keeps track of recurring jobs.
    # time.sleep(65) # Optional: uncomment to see the first cron job run