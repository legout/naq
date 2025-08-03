import time
from naq import enqueue_sync

# Enable debug logging to see dependency check messages
# setup_logging(level="DEBUG")


def task_a():
    """A simple task that takes 3 seconds."""
    print("Task A starting...")
    time.sleep(3)
    print("Task A finished.")
    return "task_a"


def task_b():
    """A simple task that takes 5 seconds."""
    print("Task B starting...")
    time.sleep(5)
    print("Task B finished.")


def final_task():
    """This task runs only after task_a and task_b are complete."""
    print("Final task running (dependencies met!).")


if __name__ == "__main__":
    job_a = enqueue_sync(task_a)

    job_b = enqueue_sync(task_b)

    # You can pass Job instances or job IDs
    job_final = enqueue_sync(final_task, depends_on=[job_a, job_b])
    # Alternatively: job_final = enqueue_sync(final_task, depends_on=[job_a.job_id, job_b.job_id])
