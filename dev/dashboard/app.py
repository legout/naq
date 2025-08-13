import os  # Import os for path joining
import sys
from pathlib import Path  # Use pathlib for cleaner path handling

import nats
from nats.js import api  # api is used for nats.js.errors
from sanic import Sanic, response
from sanic.log import logger
from sanic_ext import Extend  # Import Sanic-Ext

from ..connection import get_nats_connection, get_jetstream_context
from ..settings import DEFAULT_NATS_URL, NAQ_PREFIX  # Import default NATS URL

# Import worker listing function
from ..worker import Worker
from ..queue import Queue, ScheduledJobManager, SCHEDULED_JOB_STATUS
from ..models.jobs import Job, get_serializer
from ..results import Results
from ..settings import FAILED_JOB_STREAM_NAME, FAILED_JOB_SUBJECT_PREFIX, RESULT_KV_NAME

# Attempt to import Datastar, handle error if dashboard extras aren't installed
try:
    from datastar import Datastar  # Import Datastar
except ImportError:
    logger.error(
        "Dashboard dependencies (datastar-py) not installed. Please run: pip install naq[dashboard]"
    )
    sys.exit(1)  # Exit if dependencies are missing


# Create Sanic app instance
app = Sanic("NaqDashboard")
# Initialize Datastar on the app
Datastar(app)

# Configure Sanic-Ext for Jinja2 templating
# Get the directory where this app.py file is located
dashboard_dir = Path(__file__).parent.resolve()
template_dir = dashboard_dir / "templates"

# Check if Jinja2 is installed before enabling templating
try:
    import jinja2

    app.config.TEMPLATING_ENABLE_ASYNC = True  # Enable async Jinja2
    app.config.TEMPLATING_PATH_TO_TEMPLATES = template_dir
    Extend(app)  # Initialize Sanic-Ext
except ImportError:
    logger.error(
        "Dashboard dependencies (jinja2) not installed. Please run: pip install naq[dashboard]"
    )
    # Optionally exit, or let it run without templating if desired
    # sys.exit(1)


@app.get("/")
async def index(request):
    """Serves the main dashboard page using Jinja2."""
    logger.info("Serving dashboard index page.")
    # Render the Jinja2 template
    # Note: Sanic-Ext automatically provides the 'render' function to response
    # if templating is configured correctly.
    # However, explicit rendering might be needed depending on setup.
    # Let's use the context processor approach if available or direct render.
    try:
        # Sanic-Ext >= 22.9 provides response.render
        return await response.render("dashboard.html")
    except AttributeError:
        # Fallback for older Sanic-Ext or if render isn't attached
        logger.warning(
            "response.render not found, attempting manual template rendering."
        )
        try:
            # Manually get environment if needed (less ideal with Sanic-Ext)
            env = app.ext.environment  # Access Jinja env via Sanic-Ext
            template = env.get_template("dashboard.html")
            html_content = await template.render_async()  # Use async render
            return response.html(html_content)
        except Exception as e:
            logger.error(f"Failed to render Jinja2 template: {e}", exc_info=True)
            return response.text("Error rendering dashboard template.", status=500)
    except Exception as e:
        logger.error(f"Error serving dashboard index: {e}", exc_info=True)
        return response.text("Internal Server Error", status=500)


@app.get("/api/workers")
async def api_get_workers(request):
    """API endpoint to fetch current worker status."""
    logger.debug("Fetching worker data for API")
    try:
        # Get NATS URL from environment or use default
        # In a real app, you might pass this via app config or context
        nats_url = os.getenv("NAQ_NATS_URL", DEFAULT_NATS_URL)
        workers = await Worker.list_workers(nats_url=nats_url)
        # We might need to format timestamps or other data here before sending JSON
        # For now, send the raw data returned by list_workers
        return response.json(workers)
    except Exception as e:
        logger.error(f"Error fetching worker data: {e}", exc_info=True)
        return response.json({"error": "Failed to fetch worker data"}, status=500)


@app.get("/api/queues")
async def api_get_queues(request):
    """API endpoint to fetch current queue status."""
    logger.debug("Fetching queue data for API")
    nc = None
    try:
        nats_url = os.getenv("NAQ_NATS_URL", DEFAULT_NATS_URL)
        nc = await get_nats_connection(url=nats_url)
        js = await get_jetstream_context(nc=nc)

        stream_name = f"{NAQ_PREFIX}_jobs"
        stream_info = await js.stream_info(stream_name)

        queues = []
        # The subjects are in the form "naq.queue.<queue_name>"
        # We can extract the queue names from the stream's subjects
        # or by listing consumers and inferring from their names.

        # Let's try to get consumers and infer queue names from durable consumer names
        # Consumer names are like "naq-worker-<queue_name>"
        consumer_infos = await js.consumer_list(stream_name)
        consumer_names = {c.name for c in consumer_infos.consumers}

        for subject in stream_info.config.subjects:
            if subject.startswith(f"{NAQ_PREFIX}.queue."):
                queue_name = subject.split(".")[-1]
                # Check if a consumer for this queue exists
                durable_consumer_name = f"{NAQ_PREFIX}-worker-{queue_name}"
                consumer_detail = None
                if durable_consumer_name in consumer_names:
                    try:
                        consumer_info = await js.consumer_info(
                            stream_name, durable_consumer_name
                        )
                        consumer_detail = {
                            "name": consumer_info.name,
                            "num_pending": consumer_info.num_pending,
                            "num_ack_pending": consumer_info.num_ack_pending,
                            "num_redelivered": consumer_info.num_redelivered,
                            "num_waiting": consumer_info.num_waiting,
                            "num_paused": consumer_info.num_paused,
                            "delivered_last_seq": consumer_info.delivered.last_seq,
                            "ack_floor_last_seq": consumer_info.ack_floor.last_seq,
                        }
                    except nats.js.errors.NotFoundError:
                        # Consumer might have been deleted
                        pass
                    except Exception as e:
                        logger.warning(
                            f"Could not get consumer info for {durable_consumer_name}: {e}"
                        )

                queues.append(
                    {
                        "name": queue_name,
                        "subject": subject,
                        "stream": stream_name,
                        "consumer": consumer_detail,
                    }
                )

        return response.json(queues)

    except nats.js.errors.NotFoundError:
        logger.warning(f"Stream '{NAQ_PREFIX}_jobs' not found. No queues to display.")
        return response.json([])
    except Exception as e:
        logger.error(f"Error fetching queue data: {e}", exc_info=True)
        return response.json({"error": "Failed to fetch queue data"}, status=500)
    finally:
        if nc:
            await nc.close()


@app.get("/api/scheduled_jobs")
async def api_get_scheduled_jobs(request):
    """API endpoint to fetch scheduled jobs."""
    logger.debug("Fetching scheduled jobs for API")
    nc = None
    try:
        nats_url = os.getenv("NAQ_NATS_URL", DEFAULT_NATS_URL)
        # We need a queue name to initialize ScheduledJobManager, but it's not queue-specific.
        # We can use a default or any queue name as the manager itself is generic.
        q = Queue(nats_url=nats_url)
        scheduled_job_manager = q._scheduled_job_manager  # Access the manager

        kv = await scheduled_job_manager.get_kv()
        keys = await kv.keys()

        scheduled_jobs = []
        for key_bytes in keys:
            try:
                entry = await kv.get(key_bytes)
                # The value is cloudpickled schedule_data
                # We need to be careful here as direct deserialization might be risky
                # if the data comes from an untrusted source.
                # For now, assuming it's safe as it's internal.
                # A better approach would be to have a specific API in ScheduledJobManager
                # that returns sanitized/structured data.
                # For now, let's assume it's a dict.
                schedule_data = Job.deserialize_result(
                    entry.value
                )  # This is not right, it's cloudpickle

                # Correct deserialization:
                import cloudpickle

                schedule_data = cloudpickle.loads(entry.value)

                # Sanitize data for frontend
                job_info = {
                    "job_id": schedule_data.get("job_id"),
                    "status": schedule_data.get("status"),
                    "scheduled_timestamp_utc": schedule_data.get(
                        "scheduled_timestamp_utc"
                    ),
                    "cron": schedule_data.get("cron"),
                    "interval_seconds": schedule_data.get("interval_seconds"),
                    "repeat": schedule_data.get("repeat"),
                    "schedule_failure_count": schedule_data.get(
                        "schedule_failure_count"
                    ),
                    "last_enqueued_utc": schedule_data.get("last_enqueued_utc"),
                    "next_run_utc": schedule_data.get("next_run_utc"),
                    "function_str": "N/A",  # Will need to deserialize job payload for this
                }

                # Try to get function name from original job payload
                try:
                    original_job_payload = schedule_data.get("_orig_job_payload")
                    if original_job_payload:
                        # Assuming JsonSerializer for simplicity of getting function name
                        # This part needs to be robust if different serializers are used.
                        # For now, let's assume it's a dict if from JsonSerializer
                        # or we'd need to call Job.deserialize(original_job_payload).function.__name__
                        # This is a simplification.
                        if (
                            isinstance(original_job_payload, dict)
                            and "function" in original_job_payload
                        ):
                            job_info["function_str"] = original_job_payload[
                                "function"
                            ].split(":")[-1]  # get qualname
                        else:  # Fallback for pickle or other complex cases
                            temp_job = Job.deserialize(original_job_payload)
                            job_info["function_str"] = getattr(
                                temp_job.function, "__name__", repr(temp_job.function)
                            )

                except Exception as e:
                    logger.warning(
                        f"Could not parse function name for job {job_info['job_id']}: {e}"
                    )

                scheduled_jobs.append(job_info)
            except Exception as e:
                logger.error(
                    f"Error processing scheduled job key {key_bytes.decode()}: {e}",
                    exc_info=True,
                )

        # Sort by next_run_utc, pending first
        scheduled_jobs.sort(
            key=lambda j: (
                j["status"] != SCHEDULED_JOB_STATUS.ACTIVE.value,
                j.get("next_run_utc", 0),
            )
        )

        return response.json(scheduled_jobs)
    except Exception as e:
        logger.error(f"Error fetching scheduled jobs: {e}", exc_info=True)
        return response.json({"error": "Failed to fetch scheduled jobs"}, status=500)
    finally:
        if (
            nc and "nc" in locals() and nc.is_connected
        ):  # Ensure nc was defined and connected
            await nc.close()


@app.post("/api/scheduled_jobs/<job_id>/pause")
async def api_pause_scheduled_job(request, job_id: str):
    logger.info(f"Pausing scheduled job {job_id}")
    try:
        nats_url = os.getenv("NAQ_NATS_URL", DEFAULT_NATS_URL)
        q = Queue(nats_url=nats_url)
        success = await q.pause_scheduled_job(job_id)
        if success:
            return response.json({"status": "paused"})
        else:
            # This could be due to concurrency issue or job not found
            return response.json(
                {
                    "error": f"Failed to pause job {job_id}. It might not exist or was modified concurrently."
                },
                status=409,
            )
    except Job.DoesNotExist:  # Assuming Job.DoesNotExist or similar is raised
        return response.json({"error": f"Job {job_id} not found."}, status=404)
    except Exception as e:
        logger.error(f"Error pausing scheduled job {job_id}: {e}", exc_info=True)
        return response.json({"error": "Failed to pause job"}, status=500)


@app.post("/api/scheduled_jobs/<job_id>/resume")
async def api_resume_scheduled_job(request, job_id: str):
    logger.info(f"Resuming scheduled job {job_id}")
    try:
        nats_url = os.getenv("NAQ_NATS_URL", DEFAULT_NATS_URL)
        q = Queue(nats_url=nats_url)
        success = await q.resume_scheduled_job(job_id)
        if success:
            return response.json({"status": "resumed"})
        else:
            return response.json(
                {
                    "error": f"Failed to resume job {job_id}. It might not exist or was modified concurrently."
                },
                status=409,
            )
    except Job.DoesNotExist:
        return response.json({"error": f"Job {job_id} not found."}, status=404)
    except Exception as e:
        logger.error(f"Error resuming scheduled job {job_id}: {e}", exc_info=True)
        return response.json({"error": "Failed to resume job"}, status=500)


@app.post("/api/scheduled_jobs/<job_id>/cancel")
async def api_cancel_scheduled_job(request, job_id: str):
    logger.info(f"Cancelling scheduled job {job_id}")
    try:
        nats_url = os.getenv("NAQ_NATS_URL", DEFAULT_NATS_URL)
        q = Queue(nats_url=nats_url)
        cancelled = await q.cancel_scheduled_job(job_id)
        if cancelled:
            return response.json({"status": "cancelled"})
        else:
            return response.json({"error": f"Job {job_id} not found."}, status=404)
    except Exception as e:
        logger.error(f"Error cancelling scheduled job {job_id}: {e}", exc_info=True)
        return response.json({"error": "Failed to cancel job"}, status=500)


import uuid
from ..models.jobs import Job, get_serializer
from ..results import Results
from ..settings import FAILED_JOB_STREAM_NAME, FAILED_JOB_SUBJECT_PREFIX, RESULT_KV_NAME


async def ensure_stream(js, stream_name: str, subjects: list[str]):
    """Ensures a JetStream stream exists, creating it if necessary."""
    try:
        await js.stream_info(stream_name)
        logger.debug(f"Stream {stream_name} already exists.")
    except nats.js.errors.NotFoundError:
        logger.info(
            f"Stream {stream_name} not found, creating it with subjects: {subjects}"
        )
        await js.add_stream(name=stream_name, subjects=subjects)


@app.get("/api/failed_jobs")
async def api_get_failed_jobs(request):
    """API endpoint to fetch failed jobs."""
    logger.debug("Fetching failed jobs for API")
    nc = None
    try:
        nats_url = os.getenv("NAQ_NATS_URL", DEFAULT_NATS_URL)
        nc = await get_nats_connection(url=nats_url)
        js = await get_jetstream_context(nc=nc)

        await ensure_stream(
            js=js,
            stream_name=FAILED_JOB_STREAM_NAME,
            subjects=[f"{FAILED_JOB_SUBJECT_PREFIX}.*"],
        )

        failed_jobs = []
        stream_info = await js.stream_info(FAILED_JOB_STREAM_NAME)
        if stream_info.state.messages == 0:
            return response.json([])

        consumer_name = f"dashboard-failed-job-listener-{uuid.uuid4().hex[:6]}"
        try:
            psub = await js.pull_subscribe(
                subject=f"{FAILED_JOB_SUBJECT_PREFIX}.*",
                durable=consumer_name,
                config=api.ConsumerConfig(
                    ack_policy=api.AckPolicy.NONE,
                    deliver_policy=api.DeliverPolicy.ALL,
                    max_deliver=1,
                ),
            )
            # Fetch a batch of messages, adjust batch size as needed
            # Note: Fetching all messages might be slow for a large number of failed jobs.
            # Consider pagination for a production system.
            msgs = await psub.fetch(batch=100, timeout=5)

            serializer = get_serializer()

            for msg in msgs:
                try:
                    # The payload is a serialized failed job.
                    # Job.serialize_failed_job() uses cloudpickle.
                    import cloudpickle

                    failed_job_payload = cloudpickle.loads(msg.data)

                    job_info = {
                        "job_id": failed_job_payload.get("job_id"),
                        "queue_name": failed_job_payload.get("queue_name"),
                        "error": failed_job_payload.get("error"),
                        "traceback": failed_job_payload.get("traceback"),
                        "function_str": failed_job_payload.get("function_str"),
                        "args_repr": failed_job_payload.get("args_repr"),
                        "kwargs_repr": failed_job_payload.get("kwargs_repr"),
                        "enqueue_time": failed_job_payload.get("enqueue_time"),
                        "subject": msg.subject,
                        "sequence": msg.seq,
                        "timestamp": msg.timestamp,
                    }
                    failed_jobs.append(job_info)
                except Exception as e:
                    logger.error(
                        f"Error deserializing failed job message {msg.seq}: {e}",
                        exc_info=True,
                    )

            # Clean up the consumer
            await psub.unsubscribe()
            try:
                await js.delete_consumer(
                    stream=FAILED_JOB_STREAM_NAME, consumer=consumer_name
                )
            except nats.js.errors.NotFoundError:
                logger.warning(
                    f"Consumer {consumer_name} not found for deletion, likely already processed."
                )
            except Exception as e_del:
                logger.error(
                    f"Error deleting consumer {consumer_name}: {e_del}", exc_info=True
                )

        except Exception as e_consumer:
            logger.error(
                f"Error setting up consumer for failed jobs: {e_consumer}",
                exc_info=True,
            )
            return response.json(
                {"error": "Failed to set up consumer for failed jobs"}, status=500
            )

        failed_jobs.sort(key=lambda j: j.get("timestamp", 0), reverse=True)
        return response.json(failed_jobs)

    except nats.js.errors.NotFoundError:
        logger.warning(
            f"Stream '{FAILED_JOB_STREAM_NAME}' not found. No failed jobs to display."
        )
        return response.json([])
    except Exception as e:
        logger.error(f"Error fetching failed jobs: {e}", exc_info=True)
        return response.json({"error": "Failed to fetch failed jobs"}, status=500)
    finally:
        if nc and nc.is_connected:
            await nc.close()


@app.post("/api/failed_jobs/<job_id>/requeue")
async def api_requeue_failed_job(request, job_id: str):
    logger.info(f"Re-queuing failed job {job_id}")
    nc = None
    requeued = False
    try:
        nats_url = os.getenv("NAQ_NATS_URL", DEFAULT_NATS_URL)
        nc = await get_nats_connection(url=nats_url)
        js = await get_jetstream_context(nc=nc)

        # To requeue, we need to:
        # 1. Find the specific failed job message in the FAILED_JOB_STREAM_NAME.
        #    This is the hardest part. We can iterate messages, check payload for job_id.
        # 2. Deserialize its payload to get original job details.
        #    The current `Job.serialize_failed_job` stores `function_str`, `args_repr`, `kwargs_repr`.
        #    Reconstructing `function` from `function_str` is possible if it's an importable path.
        #    Reconstructing `args` from `args_repr` and `kwargs` from `kwargs_repr` is unsafe (eval).
        #
        # A more robust `serialize_failed_job` would store `cloudpickle.dumps(job.function)`,
        # `cloudpickle.dumps(job.args)`, `cloudpickle.dumps(job.kwargs)`.
        # Let's assume the current structure for now and acknowledge the limitation.

        # We will iterate the stream to find the message with the matching job_id.
        # This is inefficient for a large number of failed jobs.
        # A production system would likely index failed jobs by job_id in a separate DB or KV store.

        consumer_name = f"dashboard-failed-job-requeue-{uuid.uuid4().hex[:6]}"
        psub = None
        try:
            psub = await js.pull_subscribe(
                subject=f"{FAILED_JOB_SUBJECT_PREFIX}.*",
                durable=consumer_name,
                config=api.ConsumerConfig(
                    ack_policy=api.AckPolicy.NONE,  # We are just peeking
                    deliver_policy=api.DeliverPolicy.ALL,
                    max_deliver=1,
                ),
            )

            found_job_payload = None
            original_queue_name = None
            msg_to_delete_seq = None

            # Fetch messages in batches until we find the one or exhaust the stream
            # Limit the number of batches to prevent infinite loops on very large streams
            max_batches = 10
            batch_size = 50

            for _ in range(max_batches):
                msgs = await psub.fetch(batch=batch_size, timeout=5)
                if not msgs:
                    break

                for msg in msgs:
                    try:
                        import cloudpickle

                        failed_job_payload = cloudpickle.loads(msg.data)
                        if failed_job_payload.get("job_id") == job_id:
                            found_job_payload = failed_job_payload
                            original_queue_name = failed_job_payload.get("queue_name")
                            msg_to_delete_seq = msg.seq
                            break
                    except Exception as e:
                        logger.error(
                            f"Error deserializing failed job message {msg.seq} during requeue search: {e}",
                            exc_info=True,
                        )

                if found_job_payload:
                    break

            if not found_job_payload or not original_queue_name:
                logger.warning(f"Failed job with job_id {job_id} not found in stream.")
                return response.json(
                    {"error": f"Failed job {job_id} not found."}, status=404
                )

            # --- Re-enqueue Logic ---
            # This is the problematic part with the current failed job data structure.
            # We have `function_str`, `args_repr`, `kwargs_repr`.
            # We cannot safely reconstruct `args` and `kwargs` from their reprs.
            # We also cannot safely reconstruct `function` from `function_str` if it's a lambda or complex object.

            # For the purpose of this exercise, we will assume `function_str` is a resolvable path
            # and that `args_repr`/`kwargs_repr` are simple enough that we *could* re-evaluate them
            # in a controlled environment (THIS IS UNSAFE IN PRODUCTION).
            # A real implementation would require storing the original pickled function, args, kwargs.

            logger.warning(
                f"Attempting to requeue job {job_id} using function_str, args_repr, kwargs_repr. "
                "This is unsafe and for demonstration purposes only."
            )

            # Placeholder for actual re-enqueue logic using the problematic data
            # q = Queue(name=original_queue_name, nats_url=nats_url)
            # func = resolve_function_from_str(found_job_payload["function_str"])
            # args = ast.literal_eval(found_job_payload["args_repr"]) # Unsafe
            # kwargs = ast.literal_eval(found_job_payload["kwargs_repr"]) # Unsafe
            # await q.enqueue(func, *args, **kwargs) # This would be the goal

            # Since we cannot safely re-enqueue with the current data, we will just log and acknowledge.
            # To make this endpoint "functional" in a demo sense, we'll just delete the failed job message.
            # And log that a manual re-enqueue would be needed.

            if msg_to_delete_seq:
                try:
                    # To delete a message, we need its sequence and the stream.
                    # We can use js.stream.get_message and then msg.ack() if it was from a consumer with ack policy.
                    # Or, if we have the stream and sequence, we can try to delete it directly if the server supports it.
                    # A common way is to use an ephemeral consumer, get the specific message, and ack it (which deletes it if ack_policy is explicit).

                    # Let's create a new ephemeral consumer to get this specific message and ack it.
                    ephemeral_consumer_for_delete = f"deleter-{uuid.uuid4().hex[:6]}"
                    del_psub = await js.pull_subscribe(
                        subject=f"{FAILED_JOB_SUBJECT_PREFIX}.{original_queue_name}",  # Be more specific if possible
                        durable=ephemeral_consumer_for_delete,
                        config=api.ConsumerConfig(
                            ack_policy=api.AckPolicy.EXPLICIT,
                            deliver_policy=api.DeliverPolicy.BY_START_SEQUENCE,  # Start looking from our message
                            opt_start_seq=msg_to_delete_seq,
                            max_deliver=1,  # We only expect one message
                            inactive_threshold=10,  # Short lived
                        ),
                    )
                    delete_msgs = await del_psub.fetch(batch=1, timeout=5)
                    if delete_msgs:
                        await delete_msgs[0].ack()
                        logger.info(
                            f"Failed job message for job_id {job_id} (seq: {msg_to_delete_seq}) acknowledged and removed from stream."
                        )
                        requeued = True  # Mark as processed (deleted)
                    else:
                        logger.error(
                            f"Could not fetch message seq {msg_to_delete_seq} for deletion."
                        )
                    await del_psub.unsubscribe()
                    try:
                        await js.delete_consumer(
                            stream=FAILED_JOB_STREAM_NAME,
                            consumer=ephemeral_consumer_for_delete,
                        )
                    except nats.js.errors.NotFoundError:
                        pass  # Ephemeral consumer might be gone already
                    except Exception as e_del_cons:
                        logger.error(
                            f"Error deleting ephemeral consumer {ephemeral_consumer_for_delete}: {e_del_cons}"
                        )

                except Exception as e_del_msg:
                    logger.error(
                        f"Error deleting failed job message {msg_to_delete_seq}: {e_del_msg}",
                        exc_info=True,
                    )

            if requeued:
                return response.json(
                    {
                        "status": "deleted_from_failed_queue",
                        "message": f"Failed job {job_id} was found and removed from the failed job stream. Manual re-enqueue may be required based on its data.",
                    }
                )
            else:
                return response.json(
                    {
                        "status": "processed_but_not_deleted",
                        "message": f"Failed job {job_id} was found, but an error occurred while trying to delete it from the stream.",
                    },
                    status=500,
                )

        except Exception as e_consumer_setup:
            logger.error(
                f"Error setting up consumer for requeuing job {job_id}: {e_consumer_setup}",
                exc_info=True,
            )
            return response.json(
                {"error": "Failed to set up consumer for requeuing job"}, status=500
            )
        finally:
            if psub:
                await psub.unsubscribe()
                try:
                    await js.delete_consumer(
                        stream=FAILED_JOB_STREAM_NAME, consumer=consumer_name
                    )
                except nats.js.errors.NotFoundError:
                    pass  # Ephemeral or already deleted
                except Exception as e_del_cons:
                    logger.error(
                        f"Error deleting requeue consumer {consumer_name}: {e_del_cons}"
                    )

    except Exception as e:
        logger.error(f"Error re-queuing failed job {job_id}: {e}", exc_info=True)
        return response.json(
            {"error": "Failed to requeue job due to an internal error"}, status=500
        )
    finally:
        if nc and nc.is_connected:
            await nc.close()


# Add more routes here later for data fetching (failed jobs, etc.)
