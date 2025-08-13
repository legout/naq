# src/naq/models.py
"""
Model definitions for the NAQ job queue system.

This module is kept for backward compatibility during the refactoring process.
All model classes have been moved to their respective modules in the models package:
- Enums: src/naq/models/enums.py
- Events: src/naq/models/events.py
- Jobs: src/naq/models/jobs.py
- Schedules: src/naq/models/schedules.py

This file will be removed in a future version.
"""

import time
from typing import Any, Dict, List, Optional

import msgspec

from .exceptions import NaqException
from .settings import DEFAULT_QUEUE_NAME
from .jobs import Job
