# src/naq/job.py
# This file now imports from models.py for backward compatibility
from .models import Job, JOB_STATUS, RetryDelayType, VALID_RETRY_STRATEGIES

# Re-export for backward compatibility
__all__ = ["Job", "JOB_STATUS", "RetryDelayType", "VALID_RETRY_STRATEGIES"]