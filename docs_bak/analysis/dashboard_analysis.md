# NAQ Dashboard Component Analysis

## 1. Overview and Role

The NAQ dashboard is a web-based interface designed to provide real-time monitoring and visibility into the status of NAQ workers. Its primary role is to allow users to observe:

*   Active workers and their current status (e.g., idle, busy).
*   The queues each worker is processing.
*   Details about ongoing tasks, such as current job ID and active task counts.
*   Worker health through last heartbeat times, including a staleness indicator.

It is built using the Sanic web framework, Jinja2 for templating, and Datastar for dynamic/reactive UI updates without full page reloads.

## 2. Public Interfaces

### a. Web Endpoints (defined in `src/naq/dashboard/app.py`)

*   **`GET /`**:
    *   **Description**: Serves the main dashboard HTML page.
    *   **Implementation**: Renders the `dashboard.html` template.
    *   **Handler**: `index(request)`
*   **`GET /api/workers`**:
    *   **Description**: Provides a JSON API endpoint to fetch a list of current worker statuses.
    *   **Implementation**: Calls `Worker.list_workers()` to retrieve data from NATS.
    *   **Handler**: `api_get_workers(request)`

### b. Key UI Elements (defined in `src/naq/dashboard/templates/dashboard.html`)

The dashboard primarily features a "Workers" section with a table displaying:

*   **Worker ID**: Unique identifier of the worker.
*   **Status**: Current operational state (e.g., `idle`, `busy`, `starting`, `stopping`), visually indicated by color.
*   **Queues**: List of queues the worker is subscribed to.
*   **Current Job**: ID of the job currently being processed (if busy).
*   **Active Tasks**: Number of tasks currently running versus the worker's concurrency limit.
*   **Last Heartbeat**: Timestamp of the last heartbeat received, shown as both an absolute time and a relative "ago" time. A "STALE" warning appears if the heartbeat is older than a defined threshold (currently 60 seconds in the template's display logic).

The UI also includes:

*   "Loading workers..." message while data is being fetched.
*   Error message display if fetching worker data fails.
*   "No active workers found." message if the API returns an empty list.

### c. Core Classes and Functions (in `src/naq/dashboard/app.py`)

*   **`app = Sanic("NaqDashboard")`**: The Sanic application instance.
*   **`Datastar(app)`**: Integration of the Datastar library for reactive UI.
*   **Jinja2 Templating Setup**: Configures Sanic-Ext to use Jinja2 for rendering HTML templates located in the `src/naq/dashboard/templates` directory.
*   **`index(request)` function**: Handles requests to the root URL, rendering the main dashboard page.
*   **`api_get_workers(request)` function**: Handles requests to the `/api/workers` endpoint, fetching and returning worker data.

## 3. Interactions with Other Library Components

The dashboard interacts with other NAQ components as follows:

*   **`Worker` Component (`src/naq/worker.py`):**
    *   The `/api/workers` endpoint directly calls the static method `Worker.list_workers()` to retrieve information about all registered workers. This method likely queries a central store (e.g., NATS JetStream or a shared data structure populated via NATS) where workers publish their heartbeats and status.
*   **`Connection` (implicitly via `Worker` and NATS):**
    *   Although not directly instantiated in `app.py`, the `Worker.list_workers()` method would internally use a NATS connection (configured via `nats_url`, defaulting to `DEFAULT_NATS_URL` or `NAQ_NATS_URL` environment variable) to communicate and fetch worker data.
*   **`Settings` (`src/naq/settings.py`):**
    *   Uses `DEFAULT_NATS_URL` as a fallback if the `NAQ_NATS_URL` environment variable is not set.
*   **Queues, Scheduler, Job (Indirectly/Potentially in the future):**
    *   Currently, the dashboard primarily focuses on workers. The template (`src/naq/dashboard/templates/dashboard.html`) has a placeholder comment `<!-- Placeholder for other sections (Queues, Jobs, etc.) -->`, suggesting future plans to display information about queues (e.g., message counts, consumer counts) and jobs (e.g., job details, status, history). These would require new API endpoints in `app.py` that interact with the `Queue`, `Job`, and potentially `Scheduler` components of the `naq` library.