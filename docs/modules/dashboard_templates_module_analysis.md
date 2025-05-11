# Analysis of [`src/naq/dashboard/templates.py`](src/naq/dashboard/templates.py)

## 1. Module Overview and Role

The [`src/naq/dashboard/templates.py`](src/naq/dashboard/templates.py) module defines reusable, programmatic HTML page layouts and components for the NAQ dashboard using the [`htmy`](https://github.com/volfpeter/htmy) library. Its primary responsibility is to provide Python functions that generate HTML structures as `htmy.Component` objects, enabling dynamic UI construction for the dashboard. While the main dashboard rendering currently uses Jinja2 templates, this module offers a Python-centric alternative or supplement for building and composing dashboard pages, especially for Datastar-powered interactive UIs.

## 2. Public Interfaces

### A. `BaseLayout(title: str, *content: ComponentType) -> Component` ([`src/naq/dashboard/templates.py:5`](src/naq/dashboard/templates.py:5))

- **Purpose:** Constructs a complete HTML page skeleton, including `<html>`, `<head>`, and `<body>`, with essential meta tags and CDN links for Datastar CSS/JS.
- **Parameters:**
  - `title`: The HTML page title.
  - `*content`: Variable number of `htmy.ComponentType` elements to be inserted into the `<body>`.
- **Returns:** An `htmy.Component` representing the full HTML page.

### B. `DashboardPage() -> Component` ([`src/naq/dashboard/templates.py:27`](src/naq/dashboard/templates.py:27))

- **Purpose:** Builds the main dashboard page structure, using `BaseLayout` as the foundation.
- **Parameters:** None.
- **Returns:** An `htmy.Component` representing the dashboard page, including a Datastar-initialized `<div>` and placeholder content.

## 3. Key Functionalities

- **Programmatic HTML Generation:** Enables dynamic construction of HTML using Python, leveraging `htmy` for composability and type safety.
- **Reusable Layouts:** `BaseLayout` provides a consistent, extensible page skeleton with built-in support for Datastar assets.
- **Dashboard Scaffold:** `DashboardPage` sets up the initial dashboard structure, including a Datastar-compatible `<div>` and a placeholder for future widgets (e.g., workers, queues).

## 4. Dependencies and Interactions

- **External Dependencies:**
  - [`htmy`](https://github.com/volfpeter/htmy): Supplies `Component`, `ComponentType`, and the `html` element factory for declarative HTML construction.
  - **Datastar CSS/JS:** Included via CDN links in the generated HTML for interactive dashboard features.
- **Internal Interactions:**
  - [`src/naq/dashboard/app.py`](src/naq/dashboard/app.py): The Sanic app currently uses Jinja2 templates for rendering, but could be extended to use these Python-based components for alternative or hybrid rendering strategies.
  - **Self-Composition:** `DashboardPage()` composes its structure by calling `BaseLayout()`.

## 5. Notable Implementation Details

- **htmy-Driven Templates:** The exclusive use of `htmy` allows for Pythonic, composable, and testable HTML generation, as opposed to string-based templates.
- **Datastar-First Design:** Both layout and page components are pre-wired for Datastar, supporting reactive front-end features out of the box.
- **Extensibility:** The placeholder content and comments in `DashboardPage` indicate a scaffold intended for future expansion (e.g., widgets for workers, queues, etc.).
- **Architectural Flexibility:** The coexistence of Jinja2 and `htmy`-based templates suggests a flexible architecture, supporting multiple rendering paradigms or phased migration.

## 6. Mermaid Diagram

```mermaid
classDiagram
    class BaseLayout {
        +BaseLayout(title, content)
        #Creates HTML skeleton
        #Includes meta tags, Datastar CSS/JS
        #Embeds provided content in body
    }
    
    class DashboardPage {
        +DashboardPage()
        #Uses BaseLayout for dashboard page
        #Sets title to NAQ Dashboard
        #Adds div with data-store
        #Placeholder: Hello from NAQ Dashboard!
    }
    
    class Component {
        <<External Library>>
        #Represents an HTML component
    }
    
    class html {
        <<External Library Helper>>
        #Factory for HTML elements
    }
    
    DashboardPage --> BaseLayout : uses
    BaseLayout --|> Component : returns
    DashboardPage --|> Component : returns
    BaseLayout --> html : uses
    DashboardPage --> html : uses
    
    note for BaseLayout "Module: src/naq/dashboard/templates.py"
    note for DashboardPage "Module: src/naq/dashboard/templates.py"