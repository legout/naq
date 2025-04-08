import time
from naq import enqueue_sync, setup_logging, Job
from naq.exceptions import JobNotFoundError, JobExecutionError

# Configure logging
setup_logging()

def process_data(data_id: int):
    """Simulates processing data, takes some time."""
    print(f"Processing data {data_id}...")
    time.sleep(2)  # Simulate work
    result = f"Processed {data_id}"
    print(f"Finished processing {data_id}.")
    return result

def send_notification(user_id: str):
    """Simulates sending a notification."""
    print(f"Sending notification to {user_id}...")
    time.sleep(0.5) # Simulate quick work
    status = f"Notification sent to {user_id}"
    print(f"Finished sending notification to {user_id}.")
    return status

if __name__ == "__main__":
    print("Enqueuing jobs to different queues...")

    # Enqueue a job to the 'data_processing' queue
    job_data = enqueue_sync(
        process_data,
        data_id=123,
        queue_name='data_processing'
    )
    print(f"Enqueued job {job_data.job_id} to queue '{job_data.queue_name}' for function '{job_data.function}'")

    # Enqueue a job to the 'notifications' queue
    job_notify = enqueue_sync(
        send_notification,
        user_id="user_abc",
        queue_name='notifications'
    )
    print(f"Enqueued job {job_notify.job_id} to queue '{job_notify.queue_name}' for function '{job_notify.function}'")

    print("\nWaiting a few seconds to allow workers to potentially pick up and finish jobs...")
    time.sleep(5) # Give workers some time

    print("\nAttempting to fetch job results...")

    # Fetch result for the data processing job
    print(f"\nFetching result for job {job_data.job_id} (data_processing)...")
    try:
        result_data = Job.fetch_result_sync(job_data.job_id)
        print(f"  Result: {result_data}")
    except JobNotFoundError:
        print(f"  Job {job_data.job_id} not found or result expired/not available yet.")
    except JobExecutionError as e:
        print(f"  Job {job_data.job_id} failed: {e}")
    except Exception as e:
        print(f"  An unexpected error occurred fetching result for job {job_data.job_id}: {e}")


    # Fetch result for the notification job
    print(f"\nFetching result for job {job_notify.job_id} (notifications)...")
    try:
        result_notify = Job.fetch_result_sync(job_notify.job_id)
        print(f"  Result: {result_notify}")
    except JobNotFoundError:
        print(f"  Job {job_notify.job_id} not found or result expired/not available yet.")
    except JobExecutionError as e:
        print(f"  Job {job_notify.job_id} failed: {e}")
    except Exception as e:
        print(f"  An unexpected error occurred fetching result for job {job_notify.job_id}: {e}")

    print("\nExample finished.")