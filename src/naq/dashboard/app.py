import sys
import os # Import os for path joining
from pathlib import Path # Use pathlib for cleaner path handling
from sanic import Sanic, response
from sanic.log import logger
from sanic_ext import Extend # Import Sanic-Ext

# Import worker listing function
from ..worker import Worker
from ..settings import DEFAULT_NATS_URL # Import default NATS URL

# Attempt to import Datastar, handle error if dashboard extras aren't installed
try:
    from datastar import Datastar # Import Datastar
except ImportError:
    logger.error("Dashboard dependencies (datastar-py) not installed. Please run: pip install naq[dashboard]")
    sys.exit(1) # Exit if dependencies are missing


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
    app.config.TEMPLATING_ENABLE_ASYNC = True # Enable async Jinja2
    app.config.TEMPLATING_PATH_TO_TEMPLATES = template_dir
    Extend(app) # Initialize Sanic-Ext
except ImportError:
     logger.error("Dashboard dependencies (jinja2) not installed. Please run: pip install naq[dashboard]")
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
        logger.warning("response.render not found, attempting manual template rendering.")
        try:
            # Manually get environment if needed (less ideal with Sanic-Ext)
            env = app.ext.environment # Access Jinja env via Sanic-Ext
            template = env.get_template("dashboard.html")
            html_content = await template.render_async() # Use async render
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

# Add more routes here later for data fetching (queues, etc.)
