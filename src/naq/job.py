# src/naq/job.py
import asyncio
import time
import traceback
import uuid
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

from .exceptions import JobExecutionError, JobNotFoundError, NaqConnectionError, NaqException, SerializationError
from .models import Job, JOB_STATUS, RetryDelayType
from .results import Results
from .settings import (
    DEFAULT_NATS_URL,
    DEFAULT_QUEUE_NAME,
)
from .utils.serialization import get_serializer

# Define retry strategies
from .settings import RETRY_STRATEGY

# Normalize valid strategies to string values for consistent comparison and error messages
VALID_RETRY_STRATEGIES = {"linear", "exponential"}