import time
from naq import enqueue_sync, setup_logging

# Configure logging for naq (optional, but helpful)
setup_logging(level="INFO")

# Define a simple function to be executed by the worker
def say_hello(name: str):
    """A simple job function that prints a greeting."""
    print(f"Hello, {name}!")
    # Simulate some work
    time.sleep(2)
    print(f"Finished greeting {name}.")
    return f"Greeting for {name} completed."

if __name__ == "__main__":
    print("Enqueuing hello world job...")

    # Enqueue the job synchronously using the default queue ('naq_default_queue')
    # This function will block until the job is published to NATS.
    # It uses its own NATS connection which is closed afterwards.
    job = enqueue_sync(say_hello, name="World")

    print(f"Job enqueued with ID: {job.job_id}")
    print("Run 'naq worker naq_default_queue' in another terminal to process the job.")

    # You could optionally try fetching the result later (requires result backend)
    # print("Waiting a bit for worker to potentially finish...")
    # time.sleep(5)
    # try:
    #     result = job.fetch_result_sync(job.job_id)
    #     print(f"Job {job.job_id} result: {result}")
    # except Exception as e:
    #     print(f"Could not fetch result for job {job.job_id}: {e}")
