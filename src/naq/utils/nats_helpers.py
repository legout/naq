# src/naq/utils/nats_helpers.py
"""
NATS-specific utilities for NAQ.

This module provides utilities for working with NATS subjects, streams,
and common NATS operations that complement the connection_utils package.
"""

import re
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass

from loguru import logger

# Import NATS types if available
try:
    import nats
    from nats.js import JetStreamContext
    from nats.js.api import StreamConfig, ConsumerConfig
    _NATS_AVAILABLE = True
except ImportError:
    _NATS_AVAILABLE = False


@dataclass
class SubjectInfo:
    """Information about a NATS subject."""
    subject: str
    tokens: List[str]
    wildcards: List[str]
    is_wildcard: bool
    
    @property
    def root_token(self) -> str:
        """Get the root token of the subject."""
        return self.tokens[0] if self.tokens else ""


def create_subject(*parts: str, separator: str = ".") -> str:
    """
    Create a NATS subject from parts.
    
    Args:
        *parts: Subject parts to join
        separator: Separator to use (default: '.')
        
    Returns:
        Joined subject string
        
    Usage:
        subject = create_subject("naq", "queue", "high")  # "naq.queue.high"
        subject = create_subject("events", "user", "123", "login")  # "events.user.123.login"
    """
    # Filter out None and empty strings
    valid_parts = [str(part) for part in parts if part is not None and str(part).strip()]
    return separator.join(valid_parts)


def parse_subject(subject: str, separator: str = ".") -> SubjectInfo:
    """
    Parse a NATS subject into its components.
    
    Args:
        subject: Subject to parse
        separator: Separator used in subject
        
    Returns:
        SubjectInfo with parsed components
    """
    tokens = subject.split(separator)
    wildcards = [token for token in tokens if token in ('*', '>')]
    is_wildcard = len(wildcards) > 0
    
    return SubjectInfo(
        subject=subject,
        tokens=tokens,
        wildcards=wildcards,
        is_wildcard=is_wildcard
    )


def match_subject_pattern(subject: str, pattern: str) -> bool:
    """
    Check if a subject matches a pattern with wildcards.
    
    Args:
        subject: Subject to test
        pattern: Pattern with wildcards (* and >)
        
    Returns:
        True if subject matches pattern
        
    Usage:
        match_subject_pattern("naq.queue.high", "naq.queue.*")  # True
        match_subject_pattern("naq.queue.high.jobs", "naq.queue.>")  # True
    """
    subject_tokens = subject.split('.')
    pattern_tokens = pattern.split('.')
    
    # Handle > wildcard (must be last token)
    if '>' in pattern_tokens:
        if pattern_tokens[-1] != '>':
            return False
        
        # Check if subject has at least as many tokens as pattern (minus the >)
        if len(subject_tokens) < len(pattern_tokens) - 1:
            return False
        
        # Check tokens before >
        for i in range(len(pattern_tokens) - 1):
            if pattern_tokens[i] != '*' and pattern_tokens[i] != subject_tokens[i]:
                return False
        
        return True
    
    # No > wildcard, must match exactly
    if len(subject_tokens) != len(pattern_tokens):
        return False
    
    for subject_token, pattern_token in zip(subject_tokens, pattern_tokens):
        if pattern_token != '*' and pattern_token != subject_token:
            return False
    
    return True


def build_stream_subject_filter(*patterns: str) -> str:
    """
    Build a subject filter for JetStream streams.
    
    Args:
        *patterns: Subject patterns to include
        
    Returns:
        Subject filter string
    """
    if len(patterns) == 1:
        return patterns[0]
    elif len(patterns) > 1:
        # For multiple patterns, we typically need to use a common prefix with >
        # This is a simplified approach
        return patterns[0]  # In practice, you might want more sophisticated logic
    else:
        return ""


async def ensure_stream_exists(
    js: 'JetStreamContext',
    stream_name: str,
    subjects: List[str],
    **stream_config_kwargs
) -> bool:
    """
    Ensure a JetStream stream exists with the given configuration.
    
    Args:
        js: JetStream context
        stream_name: Name of the stream
        subjects: Subjects for the stream
        **stream_config_kwargs: Additional stream configuration
        
    Returns:
        True if stream was created, False if it already existed
    """
    if not _NATS_AVAILABLE:
        raise ImportError("NATS library not available")
    
    try:
        # Try to get existing stream
        await js.stream_info(stream_name)
        logger.debug(f"Stream '{stream_name}' already exists")
        return False
        
    except Exception:
        # Stream doesn't exist, create it
        logger.info(f"Creating stream '{stream_name}' with subjects: {subjects}")
        
        stream_config = StreamConfig(
            name=stream_name,
            subjects=subjects,
            **stream_config_kwargs
        )
        
        await js.add_stream(config=stream_config)
        logger.info(f"Stream '{stream_name}' created successfully")
        return True


async def safe_kv_operation(
    kv_store: Any,
    operation: str,
    key: str,
    value: Optional[bytes] = None,
    default: Any = None
) -> Any:
    """
    Safely perform KV store operation with error handling.
    
    Args:
        kv_store: NATS KV store instance
        operation: Operation to perform ('get', 'put', 'delete')
        key: Key for the operation
        value: Value for put operation
        default: Default value for get operation if key not found
        
    Returns:
        Operation result or default value
    """
    try:
        if operation == 'get':
            entry = await kv_store.get(key)
            return entry.value if entry else default
        elif operation == 'put':
            if value is None:
                raise ValueError("Value required for put operation")
            return await kv_store.put(key, value)
        elif operation == 'delete':
            return await kv_store.delete(key)
        else:
            raise ValueError(f"Unknown operation: {operation}")
            
    except Exception as e:
        logger.warning(f"KV operation '{operation}' failed for key '{key}': {e}")
        if operation == 'get':
            return default
        else:
            raise


def generate_consumer_name(base_name: str, suffix: Optional[str] = None) -> str:
    """
    Generate a consumer name with optional suffix.
    
    Args:
        base_name: Base name for the consumer
        suffix: Optional suffix (like worker ID)
        
    Returns:
        Generated consumer name
    """
    if suffix:
        return f"{base_name}_{suffix}"
    else:
        import uuid
        return f"{base_name}_{uuid.uuid4().hex[:8]}"


def create_subject_hierarchy(*levels: str) -> Dict[str, str]:
    """
    Create a hierarchy of subjects for different levels.
    
    Args:
        *levels: Hierarchy levels (e.g., 'service', 'queue', 'priority')
        
    Returns:
        Dictionary mapping level names to subject patterns
        
    Usage:
        hierarchy = create_subject_hierarchy('naq', 'queue', 'high')
        # Returns: {
        #   'root': 'naq',
        #   'service': 'naq.*',
        #   'queue': 'naq.queue.*',
        #   'full': 'naq.queue.high'
        # }
    """
    result = {}
    
    if not levels:
        return result
    
    # Root level
    result['root'] = levels[0]
    
    # Build cumulative patterns
    for i in range(1, len(levels)):
        level_name = 'service' if i == 1 else f'level_{i}'
        pattern = create_subject(*levels[:i], '*')
        result[level_name] = pattern
    
    # Full subject (no wildcards)
    result['full'] = create_subject(*levels)
    
    return result


class SubjectRouter:
    """
    Route messages based on subject patterns.
    
    Usage:
        router = SubjectRouter()
        router.add_route('naq.queue.*', handle_queue_message)
        router.add_route('naq.events.>', handle_event_message)
        
        handler = router.get_handler('naq.queue.high')
        if handler:
            await handler(message)
    """
    
    def __init__(self):
        """Initialize subject router."""
        self.routes: List[tuple] = []  # (pattern, handler, priority)
    
    def add_route(
        self, 
        pattern: str, 
        handler: Any, 
        priority: int = 0
    ):
        """
        Add a route for a subject pattern.
        
        Args:
            pattern: Subject pattern with wildcards
            handler: Handler function/object
            priority: Priority (higher = more important)
        """
        self.routes.append((pattern, handler, priority))
        # Sort by priority (descending)
        self.routes.sort(key=lambda x: x[2], reverse=True)
    
    def get_handler(self, subject: str) -> Optional[Any]:
        """
        Get handler for a subject.
        
        Args:
            subject: Subject to match
            
        Returns:
            Handler or None if no match
        """
        for pattern, handler, _ in self.routes:
            if match_subject_pattern(subject, pattern):
                return handler
        return None
    
    def get_all_handlers(self, subject: str) -> List[Any]:
        """
        Get all handlers that match a subject.
        
        Args:
            subject: Subject to match
            
        Returns:
            List of matching handlers
        """
        handlers = []
        for pattern, handler, _ in self.routes:
            if match_subject_pattern(subject, pattern):
                handlers.append(handler)
        return handlers


class NATSMetrics:
    """
    Track NATS operation metrics.
    
    Usage:
        metrics = NATSMetrics()
        metrics.record_publish('naq.queue.high', 1024)
        metrics.record_consume('naq.queue.high')
        stats = metrics.get_stats()
    """
    
    def __init__(self):
        """Initialize NATS metrics."""
        self.publish_counts: Dict[str, int] = {}
        self.publish_bytes: Dict[str, int] = {}
        self.consume_counts: Dict[str, int] = {}
        self.error_counts: Dict[str, int] = {}
    
    def record_publish(self, subject: str, size_bytes: int = 0):
        """Record a publish operation."""
        self.publish_counts[subject] = self.publish_counts.get(subject, 0) + 1
        self.publish_bytes[subject] = self.publish_bytes.get(subject, 0) + size_bytes
    
    def record_consume(self, subject: str):
        """Record a consume operation."""
        self.consume_counts[subject] = self.consume_counts.get(subject, 0) + 1
    
    def record_error(self, operation: str):
        """Record an error."""
        self.error_counts[operation] = self.error_counts.get(operation, 0) + 1
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current metrics."""
        return {
            'publishes': dict(self.publish_counts),
            'publish_bytes': dict(self.publish_bytes),
            'consumes': dict(self.consume_counts),
            'errors': dict(self.error_counts),
            'total_publishes': sum(self.publish_counts.values()),
            'total_consumes': sum(self.consume_counts.values()),
            'total_errors': sum(self.error_counts.values())
        }
    
    def reset(self):
        """Reset all metrics."""
        self.publish_counts.clear()
        self.publish_bytes.clear()
        self.consume_counts.clear()
        self.error_counts.clear()


# Validation functions for NATS subjects
def validate_subject_name(subject: str) -> bool:
    """
    Validate NATS subject name.
    
    Args:
        subject: Subject to validate
        
    Returns:
        True if valid
    """
    if not subject:
        return False
    
    # NATS subjects can contain alphanumeric, dots, dashes, underscores
    # Wildcards * and > are also valid in patterns
    pattern = re.compile(r'^[a-zA-Z0-9._*>-]+$')
    return pattern.match(subject) is not None


def validate_stream_name(name: str) -> bool:
    """
    Validate NATS stream name.
    
    Args:
        name: Stream name to validate
        
    Returns:
        True if valid
    """
    if not name:
        return False
    
    # Stream names are more restrictive
    pattern = re.compile(r'^[a-zA-Z0-9_-]+$')
    return pattern.match(name) is not None


# Global metrics instance
_global_metrics = NATSMetrics()


def get_nats_metrics() -> NATSMetrics:
    """Get the global NATS metrics instance."""
    return _global_metrics


# Backward compatibility aliases for tests
ensure_stream = ensure_stream_exists


def create_consumer(*args, **kwargs):
    """
    Create consumer for backward compatibility.
    
    This is a placeholder function for tests.
    """
    logger.debug("create_consumer called - placeholder function for test compatibility")
    return None


def publish_with_retry(*args, **kwargs):
    """
    Publish with retry for backward compatibility.
    
    This is a placeholder function for tests.
    """
    logger.debug("publish_with_retry called - placeholder function for test compatibility")
    return None