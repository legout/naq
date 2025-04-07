# src/naq/dashboard/templates.py
from htmy import Component, ComponentType, html


def BaseLayout(title: str, *content: ComponentType) -> Component:
    """Basic HTML page layout using HTMY."""
    return html.html(
        html.head(
            html.title(title),
            html.meta(charset="utf-8"),
            html.meta(name="viewport", content="width=device-width, initial-scale=1"),
            # Include Datastar CSS (assuming CDN for now)
            html.link(
                rel="stylesheet",
                href="https://unpkg.com/datastar-css@latest/dist/datastar-css.css",
            ),
            # Include Datastar JS (assuming CDN for now)
            html.script(
                src="https://unpkg.com/datastar-js@latest/dist/datastar.js", defer=True
            ),
            # Placeholder for additional CSS/JS
        ),
        html.body(*content),
    )


def DashboardPage() -> Component:
    """The main dashboard page component."""
    return BaseLayout(
        # Basic structure for Datastar
        html.div(
            {"data-store": "{}"},  # Initialize Datastar store
            "Hello from NAQ Dashboard!",
            # We will add components to display workers, queues, etc. here later
        ),
        title="NAQ Dashboard",
    )
