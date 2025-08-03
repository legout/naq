import time
import asyncio
from naq import enqueue, setup_logging

# Configure logging for naq (optional, but helpful)
setup_logging(level="INFO")


# Define a simple function to be executed by the worker
async def say_hello_async(name: str):
    """A simple job function that prints a greeting."""
    print(f"Hello, {name}!")
    # Simulate some work
    await asyncio.sleep(2)
    print(f"Finished greeting {name}.")
    return f"Greeting for {name} completed."


def say_hello_sync(name: str):
    """A simple job function that prints a greeting."""
    print(f"Hello, {name}!")
    # Simulate some work
    time.sleep(2)
    print(f"Finished greeting {name}.")
    return f"Greeting for {name} completed."


async def main():
    for i in range(5):
        job = await enqueue(
            say_hello_async, name="World (async)", nats_url="nats://localhost:4222"
        )
    for i in range(5):
        job = await enqueue(
            say_hello_sync, name="World (sync)", nats_url="nats://localhost:4222"
        )
    # return job


if __name__ == "__main__":
    # Enqueue the job synchronously using the default queue ('naq_default_queue')
    # This function will block until the job is published to NATS.
    # It uses its own NATS connection which is closed afterwards.
    job = asyncio.run(main())

    # You could optionally try fetching the result later (requires result backend)
    # print("Waiting a bit for worker to potentially finish...")
    # time.sleep(5)
    # try:
    #     result = job.fetch_result_sync(job.job_id)
    #     print(f"Job {job.job_id} result: {result}")
    # except Exception as e:
    #     print(f"Could not fetch result for job {job.job_id}: {e}")
