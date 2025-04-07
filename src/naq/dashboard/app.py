# src/naq/dashboard/app.py
from sanic import Sanic, response
from sanic.log import logger

# Attempt to import HTMY, handle error if dashboard extras aren't installed
#try:
from htmy.renderer import Renderer
from .templates import DashboardPage
#except ImportError:
#    logger.error("Dashboard dependencies (htmy) not installed. Please run: pip install naq[dashboard]")
#    sys.exit(1) # Exit if dependencies are missing

# Create Sanic app instance
app = Sanic("NaqDashboard")

@app.get("/")
async def index(request):
    """Serves the main dashboard page."""
    logger.info("Serving dashboard index page.")
    # Render the HTMY component to HTML
    html_content = Renderer(DashboardPage()).render()
    return response.html(html_content)

# Add more routes here later for data fetching (workers, queues, etc.)
# These routes will likely be used by Datastar on the frontend.
