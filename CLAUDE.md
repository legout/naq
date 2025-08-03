# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Development & Testing
```bash
# Run tests
python -m pytest tests/

# Run specific test types
python -m pytest tests/unit/
python -m pytest tests/integration/
python -m pytest tests/scenario/
python -m pytest tests/smoke/

# Run single test file
python -m pytest tests/unit/test_unit_job.py

# Linting and formatting
ruff check src/ tests/
ruff format src/ tests/

# Install dependencies
pip install -e .
pip install -e .[dev]
pip install -e .[test]
pip install -e .[dashboard]
```

### CLI Commands
```bash
# Start worker for default queue
naq worker default

# Start scheduler for scheduled/recurring jobs
naq scheduler

# Queue management
naq queue list
naq queue purge <queue_name>

# Dashboard (requires dashboard extras)
naq dashboard

# NATS server (via Docker)
cd docker && docker-compose up -d
```

## Architecture

NAQ is a NATS-based distributed task queue system with these core components:

### Core Modules
- **`connection.py`** - NATS connection management and JetStream setup
- **`job.py`** - Job serialization, execution, and lifecycle management  
- **`queue.py`** - Job enqueueing, scheduling, and queue operations
- **`worker.py`** - Job processing with concurrency and heartbeat monitoring
- **`scheduler.py`** - Scheduled job polling and leader election
- **`settings.py`** - Configuration constants and status enums
- **`cli.py`** - Typer-based CLI with Rich output formatting

### Key Patterns
- **Async-first design** - All core operations use asyncio with sync wrappers for convenience
- **NATS JetStream** - Jobs stored in streams with WORK_QUEUE retention for at-least-once delivery
- **Cloudpickle serialization** - Functions and arguments serialized as Job objects (with JSON alternative)
- **KV store usage** - Results, scheduled jobs, and worker monitoring use NATS KV
- **Leader election** - Scheduler uses NATS KV for high availability
- **SyncClient** - Efficient synchronous API that reuses NATS connections

### Testing Structure
- **Unit tests** (`tests/unit/`) - Individual component testing
- **Integration tests** (`tests/integration/`) - Cross-component testing  
- **Scenario tests** (`tests/scenario/`) - End-to-end workflows
- **Smoke tests** (`tests/smoke/`) - Basic functionality verification

### Dependencies
- Requires Python 3.12+
- Core: nats-py, cloudpickle, typer, loguru, croniter, rich
- Dashboard: sanic, jinja2, datastar-py, htmy
- Testing: pytest, pytest-asyncio, pytest-mock

### Development Notes
- Uses `pyproject.toml` for configuration (no setup.py or requirements.txt)
- CLI entry point: `naq = "naq.cli:app"`
- Package structure follows `src/` layout with `src/naq/` as main module
- Async tests use `asyncio_mode = "strict"` in pytest configuration