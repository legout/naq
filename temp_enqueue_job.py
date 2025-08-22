import asyncio
import os
from naq.queue import enqueue

async def main():
    def my_test_function(a, b):
        print(f"Executing my_test_function with {a} and {b}")
        return a + b

    print("Attempting to enqueue job...")
    job = await enqueue(my_test_function, 1, 2)
    print(f"Job enqueued successfully with ID: {job.job_id}")

if __name__ == "__main__":
    asyncio.run(main())