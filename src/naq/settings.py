# src/naq/settings.py
import os

# Default NATS server URL
DEFAULT_NATS_URL = os.getenv("NAQ_NATS_URL", "nats://localhost:4222")

# Default queue name (maps to a NATS subject/stream)
DEFAULT_QUEUE_NAME = os.getenv("NAQ_DEFAULT_QUEUE", "naq_default_queue")

# Prefix for NATS subjects/streams used by naq
NAQ_PREFIX = "naq"

# How jobs are serialized
# Options: 'pickle' (default, more flexible), 'json' (safer, less flexible)
JOB_SERIALIZER = os.getenv("NAQ_JOB_SERIALIZER", "pickle")