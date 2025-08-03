import os  # Import os for path joining
import sys
from pathlib import Path  # Use pathlib for cleaner path handling

from sanic import Sanic, response
from sanic.log import logger
from sanic_ext import Extend  # Import Sanic-Ext

from ..settings import DEFAULT_NATS_URL  # Import default NATS URL

# Import worker listing function
from ..worker import Worker

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
    """API endpoint to fetch queue information including pending messages and consumer counts."""
    logger.debug("Fetching queue data for API")
    try:
        from ..connection import get_nats_connection, get_jetstream_context, close_nats_connection
        from ..settings import NAQ_PREFIX
        
        nats_url = os.getenv("NAQ_NATS_URL", DEFAULT_NATS_URL)
        nc = await get_nats_connection(url=nats_url)
        js = await get_jetstream_context(nc)
        
        # Get stream info for job queues
        stream_name = f"{NAQ_PREFIX}_jobs"
        try:
            stream_info = await js.stream_info(stream_name)
            
            # Extract queue information from stream subjects
            queues = []
            if stream_info.config.subjects:
                for subject in stream_info.config.subjects:
                    if subject.startswith(f"{NAQ_PREFIX}.queue."):
                        queue_name = subject.replace(f"{NAQ_PREFIX}.queue.", "")
                        
                        # Get consumer info for this queue
                        consumers = []
                        try:
                            for consumer in await js.consumers_info(stream_name):
                                if consumer.config.filter_subject == subject:
                                    consumers.append({
                                        "name": consumer.name,
                                        "num_pending": consumer.num_pending,
                                        "num_ack_pending": consumer.num_ack_pending,
                                        "delivered": consumer.delivered.stream_seq if consumer.delivered else 0,
                                    })
                        except Exception as e:
                            logger.warning(f"Failed to get consumer info for queue {queue_name}: {e}")
                        
                        queues.append({
                            "name": queue_name,
                            "subject": subject,
                            "consumers": consumers,
                            "total_pending": sum(c["num_pending"] for c in consumers),
                        })
            
            # Add stream-level statistics
            result = {
                "queues": queues,
                "stream_info": {
                    "name": stream_info.config.name,
                    "messages": stream_info.state.messages,
                    "bytes": stream_info.state.bytes,
                    "first_seq": stream_info.state.first_seq,
                    "last_seq": stream_info.state.last_seq,
                }
            }
            
        except Exception as e:
            logger.warning(f"Failed to get stream info: {e}")
            result = {"queues": [], "stream_info": None, "error": str(e)}
        
        finally:
            await close_nats_connection()
        
        return response.json(result)
        
    except Exception as e:
        logger.error(f"Error fetching queue data: {e}", exc_info=True)
        return response.json({"error": "Failed to fetch queue data"}, status=500)


@app.get("/api/scheduled-jobs")
async def api_get_scheduled_jobs(request):
    """API endpoint to fetch scheduled job information."""
    logger.debug("Fetching scheduled job data for API")
    try:
        from ..connection import get_nats_connection, get_jetstream_context, close_nats_connection
        from ..settings import SCHEDULED_JOBS_KV_NAME
        import cloudpickle
        
        nats_url = os.getenv("NAQ_NATS_URL", DEFAULT_NATS_URL)
        nc = await get_nats_connection(url=nats_url)
        js = await get_jetstream_context(nc)
        
        scheduled_jobs = []
        try:
            kv = await js.key_value(bucket=SCHEDULED_JOBS_KV_NAME)
            
            # Get all scheduled jobs
            async for entry in kv.watch(""):
                if entry.operation == "PUT":
                    try:
                        # Deserialize job data
                        job_data = cloudpickle.loads(entry.value)
                        scheduled_jobs.append({
                            "job_id": job_data.get("job_id"),
                            "status": job_data.get("status"),
                            "queue_name": job_data.get("queue_name"),
                            "scheduled_time": job_data.get("scheduled_time"),
                            "next_run": job_data.get("next_run"),
                            "cron": job_data.get("cron"),
                            "interval": job_data.get("interval"),
                            "function_name": job_data.get("function_name", "unknown"),
                            "created_at": job_data.get("created_at"),
                            "last_run": job_data.get("last_run"),
                        })
                    except Exception as e:
                        logger.warning(f"Failed to deserialize scheduled job: {e}")
                        
        except Exception as e:
            logger.warning(f"Failed to get scheduled jobs: {e}")
            scheduled_jobs = []
        
        finally:
            await close_nats_connection()
        
        return response.json({"scheduled_jobs": scheduled_jobs})
        
    except Exception as e:
        logger.error(f"Error fetching scheduled job data: {e}", exc_info=True)
        return response.json({"error": "Failed to fetch scheduled job data"}, status=500)


@app.get("/api/failed-jobs")
async def api_get_failed_jobs(request):
    """API endpoint to fetch failed job information."""
    logger.debug("Fetching failed job data for API")
    try:
        from ..connection import get_nats_connection, get_jetstream_context, close_nats_connection
        from ..settings import FAILED_JOB_STREAM_NAME, FAILED_JOB_SUBJECT_PREFIX
        import cloudpickle
        
        nats_url = os.getenv("NAQ_NATS_URL", DEFAULT_NATS_URL)
        nc = await get_nats_connection(url=nats_url)
        js = await get_jetstream_context(nc)
        
        failed_jobs = []
        try:
            # Get failed jobs from the failed job stream
            stream_info = await js.stream_info(FAILED_JOB_STREAM_NAME)
            
            # Create a consumer to fetch messages
            consumer_config = {
                "deliver_policy": "all",
                "ack_policy": "none",
                "filter_subject": f"{FAILED_JOB_SUBJECT_PREFIX}.*"
            }
            
            consumer = await js.create_consumer(
                stream=FAILED_JOB_STREAM_NAME,
                config=consumer_config
            )
            
            # Fetch recent failed jobs (limit to 100 for performance)
            messages = await consumer.fetch(batch=100, timeout=2.0)
            
            for msg in messages:
                try:
                    # Deserialize failed job data
                    job_data = cloudpickle.loads(msg.data)
                    failed_jobs.append({
                        "job_id": job_data.get("job_id"),
                        "queue_name": job_data.get("queue_name"),
                        "function_str": job_data.get("function_str"),
                        "args_repr": job_data.get("args_repr"),
                        "kwargs_repr": job_data.get("kwargs_repr"),
                        "error": job_data.get("error"),
                        "traceback": job_data.get("traceback"),
                        "enqueue_time": job_data.get("enqueue_time"),
                        "max_retries": job_data.get("max_retries"),
                        "subject": msg.subject,
                        "sequence": msg.seq,
                    })
                except Exception as e:
                    logger.warning(f"Failed to deserialize failed job: {e}")
            
            # Delete the temporary consumer
            await consumer.delete()
            
        except Exception as e:
            logger.warning(f"Failed to get failed jobs: {e}")
            failed_jobs = []
        
        finally:
            await close_nats_connection()
        
        return response.json({"failed_jobs": failed_jobs})
        
    except Exception as e:
        logger.error(f"Error fetching failed job data: {e}", exc_info=True)
        return response.json({"error": "Failed to fetch failed job data"}, status=500)


@app.get("/api/stats")
async def api_get_stats(request):
    """API endpoint to fetch overall system statistics."""
    logger.debug("Fetching system stats for API")
    try:
        from ..connection import get_nats_connection, get_jetstream_context, close_nats_connection
        from ..settings import NAQ_PREFIX, WORKER_KV_NAME, SCHEDULED_JOBS_KV_NAME, FAILED_JOB_STREAM_NAME
        
        nats_url = os.getenv("NAQ_NATS_URL", DEFAULT_NATS_URL)
        nc = await get_nats_connection(url=nats_url)
        js = await get_jetstream_context(nc)
        
        stats = {
            "workers": {"total": 0, "active": 0, "idle": 0},
            "jobs": {"pending": 0, "total_processed": 0},
            "scheduled_jobs": {"total": 0},
            "failed_jobs": {"total": 0},
        }
        
        try:
            # Worker statistics
            workers = await Worker.list_workers(nats_url=nats_url)
            stats["workers"]["total"] = len(workers)
            stats["workers"]["active"] = len([w for w in workers if w.get("status") == "active"])
            stats["workers"]["idle"] = len([w for w in workers if w.get("status") == "idle"])
            
            # Job stream statistics
            stream_name = f"{NAQ_PREFIX}_jobs"
            try:
                stream_info = await js.stream_info(stream_name)
                stats["jobs"]["pending"] = stream_info.state.messages
                stats["jobs"]["total_processed"] = stream_info.state.last_seq
            except:
                pass
            
            # Scheduled jobs count
            try:
                kv = await js.key_value(bucket=SCHEDULED_JOBS_KV_NAME)
                count = 0
                async for _ in kv.keys():
                    count += 1
                stats["scheduled_jobs"]["total"] = count
            except:
                pass
            
            # Failed jobs count
            try:
                stream_info = await js.stream_info(FAILED_JOB_STREAM_NAME)
                stats["failed_jobs"]["total"] = stream_info.state.messages
            except:
                pass
            
        except Exception as e:
            logger.warning(f"Failed to collect some stats: {e}")
        
        finally:
            await close_nats_connection()
        
        return response.json(stats)
        
    except Exception as e:
        logger.error(f"Error fetching system stats: {e}", exc_info=True)
        return response.json({"error": "Failed to fetch system stats"}, status=500)


# Add more routes here for additional dashboard functionality
