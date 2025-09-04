#!/usr/bin/env python3
"""
Multiple Queues with Results Example

This example demonstrates how to work with multiple queues and retrieve
job results from different queues.

Before running:
1. Start NATS: `cd docker && docker-compose up -d`
2. Set secure serializer: `export NAQ_JOB_SERIALIZER=json`
3. Start workers for each queue: `naq worker data_processing` and `naq worker notifications`
4. Run this script: `python multiple_queues_results.py`
"""

import time
from typing import Any

from loguru import logger

from naq import NatsClient, setup_logging, get_result_sync
from naq.config import NatsConfig, QueueConfig
from naq.exceptions import JobNotFoundError, JobExecutionError

# Configure logging
setup_logging(level="INFO")

def process_data(data_id: int) -> str:
    """
    Simulates processing data, takes some time.
    
    Args:
        data_id: ID of the data to process
        
    Returns:
        Processing result message
    """
    logger.info(f"Processing data {data_id}...")
    time.sleep(2)  # Simulate work
    result = f"Processed {data_id}"
    logger.success(f"Finished processing {data_id}.")
    return result

def send_notification(user_id: str) -> str:
    """
    Simulates sending a notification.
    
    Args:
        user_id: ID of the user to notify
        
    Returns:
        Notification status message
    """
    logger.info(f"Sending notification to {user_id}...")
    time.sleep(0.5) # Simulate quick work
    status = f"Notification sent to {user_id}"
    logger.success(f"Finished sending notification to {user_id}.")
    return status

def main() -> int:
    """
    Main function demonstrating multiple queues with results.
    
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        # Create configuration for data processing queue
        data_processing_nats_config = NatsConfig(
            servers=["nats://localhost:4222"],
            connect_timeout=5,
            max_reconnect_attempts=3
        )
        
        data_processing_queue_config = QueueConfig(
            name="data_processing",
            stream_name="NAQ_JOBS",
            consumer_name="data_processing_consumer"
        )
        
        # Create configuration for notifications queue
        notifications_nats_config = NatsConfig(
            servers=["nats://localhost:4222"],
            connect_timeout=5,
            max_reconnect_attempts=3
        )
        
        notifications_queue_config = QueueConfig(
            name="notifications",
            stream_name="NAQ_JOBS",
            consumer_name="notifications_consumer"
        )

        # Enqueue a job to the 'data_processing' queue
        with NatsClient(nats_config=data_processing_nats_config, queue_config=data_processing_queue_config) as client:
            job_data = client.enqueue(
                process_data,
                data_id=123
            )

        # Enqueue a job to the 'notifications' queue
        with NatsClient(nats_config=notifications_nats_config, queue_config=notifications_queue_config) as client:
            job_notify = client.enqueue(
                send_notification,
                user_id="user_abc"
            )

        logger.info("\nWaiting a few seconds to allow workers to potentially pick up and finish jobs...")
        time.sleep(5) # Give workers some time

        logger.info("\nAttempting to fetch job results...")

        # Fetch result for the data processing job
        logger.info(f"\nFetching result for job {job_data.job_id} (data_processing)...")
        try:
            result_data = get_result_sync(job_data.job_id)
            logger.info(f"  Result: {result_data}")
        except JobNotFoundError:
            logger.warning(f"  Job {job_data.job_id} not found or result expired/not available yet.")
        except JobExecutionError as e:
            logger.error(f"  Job {job_data.job_id} failed: {e}")
        except Exception as e:
            logger.error(f"  An unexpected error occurred fetching result for job {job_data.job_id}: {e}")

        # Fetch result for the notification job
        logger.info(f"\nFetching result for job {job_notify.job_id} (notifications)...")
        try:
            result_notify = get_result_sync(job_notify.job_id)
            logger.info(f"  Result: {result_notify}")
        except JobNotFoundError:
            logger.warning(f"  Job {job_notify.job_id} not found or result expired/not available yet.")
        except JobExecutionError as e:
            logger.error(f"  Job {job_notify.job_id} failed: {e}")
        except Exception as e:
            logger.error(f"  An unexpected error occurred fetching result for job {job_notify.job_id}: {e}")

        logger.info("\nExample finished.")
        
        logger.info("\n💡 Multiple Queues Tips:")
        logger.info("=" * 30)
        logger.info("• Use separate queues for different types of work")
        logger.info("• Start dedicated workers for each queue: naq worker queue_name")
        logger.info("• Results can be retrieved regardless of which queue processed the job")
        logger.info("• Consider queue priorities and worker scaling for production")
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        logger.error("\n🔧 Troubleshooting:")
        logger.error("   - Is NATS running? (cd docker && docker-compose up -d)")
        logger.error("   - Are workers running for both queues?")
        logger.error("   - Is NAQ_JOB_SERIALIZER=json set?")
        return 1


if __name__ == "__main__":
    exit(main())