# src/naq/utils/serialization.py
"""
Serialization utilities for NAQ.

This module provides safe serialization helpers with fallback mechanisms,
metadata support, and error handling.
"""

import time
import json
import pickle
import gzip
from typing import Any, Dict, Type, Optional, Tuple, Union
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict

from loguru import logger

# Import NAQ exceptions
try:
    from ..exceptions import SerializationError, NaqException
except ImportError:
    class SerializationError(Exception):
        """Serialization error."""
        pass
    
    class NaqException(Exception):
        """Base NAQ exception."""
        pass


@dataclass
class SerializationMetadata:
    """Metadata for serialized data."""
    serializer: str
    timestamp: float
    version: str = "1.0"
    compressed: bool = False
    content_type: Optional[str] = None
    size_bytes: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SerializationMetadata':
        """Create from dictionary."""
        return cls(**data)


class Serializer(ABC):
    """Abstract base class for serializers."""
    
    @abstractmethod
    def serialize(self, data: Any) -> bytes:
        """Serialize data to bytes."""
        pass
    
    @abstractmethod
    def deserialize(self, data: bytes) -> Any:
        """Deserialize bytes to data."""
        pass
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Serializer name."""
        pass


class PickleSerializer(Serializer):
    """Pickle-based serializer."""
    
    def __init__(self, protocol: int = pickle.HIGHEST_PROTOCOL):
        """Initialize with pickle protocol."""
        self.protocol = protocol
    
    def serialize(self, data: Any) -> bytes:
        """Serialize using pickle."""
        try:
            return pickle.dumps(data, protocol=self.protocol)
        except Exception as e:
            raise SerializationError(f"Pickle serialization failed: {e}") from e
    
    def deserialize(self, data: bytes) -> Any:
        """Deserialize using pickle."""
        try:
            return pickle.loads(data)
        except Exception as e:
            raise SerializationError(f"Pickle deserialization failed: {e}") from e
    
    @property
    def name(self) -> str:
        return "pickle"


class JSONSerializer(Serializer):
    """JSON-based serializer."""
    
    def __init__(self, ensure_ascii: bool = False, indent: Optional[int] = None):
        """Initialize JSON serializer."""
        self.ensure_ascii = ensure_ascii
        self.indent = indent
    
    def serialize(self, data: Any) -> bytes:
        """Serialize using JSON."""
        try:
            json_str = json.dumps(
                data, 
                default=str, 
                ensure_ascii=self.ensure_ascii,
                indent=self.indent
            )
            return json_str.encode('utf-8')
        except Exception as e:
            raise SerializationError(f"JSON serialization failed: {e}") from e
    
    def deserialize(self, data: bytes) -> Any:
        """Deserialize using JSON."""
        try:
            json_str = data.decode('utf-8')
            return json.loads(json_str)
        except Exception as e:
            raise SerializationError(f"JSON deserialization failed: {e}") from e
    
    @property
    def name(self) -> str:
        return "json"


class SerializationHelper:
    """Centralized serialization utilities with fallback mechanisms."""
    
    def __init__(self):
        """Initialize with default serializers."""
        self.serializers = {
            'pickle': PickleSerializer(),
            'json': JSONSerializer()
        }
        self.default_serializer = 'pickle'
        self.fallback_serializer = 'json'
    
    def register_serializer(self, serializer: Serializer):
        """Register a custom serializer."""
        self.serializers[serializer.name] = serializer
    
    def safe_serialize(
        self,
        data: Any, 
        serializer: str = None,
        fallback_serializer: Optional[str] = None,
        compress: bool = False
    ) -> bytes:
        """
        Serialize data with fallback option.
        
        Args:
            data: Data to serialize
            serializer: Primary serializer to use
            fallback_serializer: Fallback serializer if primary fails
            compress: Whether to compress the result
            
        Returns:
            Serialized bytes
        """
        serializer = serializer or self.default_serializer
        fallback_serializer = fallback_serializer or self.fallback_serializer
        
        try:
            if serializer not in self.serializers:
                raise SerializationError(f"Unknown serializer: {serializer}")
            
            result = self.serializers[serializer].serialize(data)
            
            if compress:
                result = gzip.compress(result)
            
            return result
            
        except Exception as e:
            if fallback_serializer and fallback_serializer != serializer:
                logger.warning(
                    f"Serialization with {serializer} failed, using {fallback_serializer}: {e}"
                )
                return self.safe_serialize(
                    data, 
                    fallback_serializer, 
                    None,  # No further fallback
                    compress
                )
            raise SerializationError(f"All serialization attempts failed: {e}") from e
    
    def safe_deserialize(
        self,
        data: bytes, 
        serializer: str = None,
        expected_type: Optional[Type] = None,
        decompress: bool = False
    ) -> Any:
        """
        Deserialize data with type checking.
        
        Args:
            data: Serialized bytes
            serializer: Serializer to use
            expected_type: Expected result type
            decompress: Whether to decompress first
            
        Returns:
            Deserialized data
        """
        serializer = serializer or self.default_serializer
        
        try:
            if decompress:
                data = gzip.decompress(data)
            
            if serializer not in self.serializers:
                raise SerializationError(f"Unknown serializer: {serializer}")
            
            result = self.serializers[serializer].deserialize(data)
            
            if expected_type and not isinstance(result, expected_type):
                raise SerializationError(
                    f"Deserialized object is {type(result)}, expected {expected_type}"
                )
            
            return result
            
        except Exception as e:
            raise SerializationError(f"Deserialization failed: {e}") from e
    
    def auto_deserialize(self, data: bytes, decompress: bool = False) -> Tuple[Any, str]:
        """
        Attempt to deserialize using multiple serializers.
        
        Args:
            data: Serialized bytes
            decompress: Whether to decompress first
            
        Returns:
            Tuple of (deserialized_data, serializer_used)
        """
        if decompress:
            try:
                data = gzip.decompress(data)
            except Exception:
                pass  # Not compressed
        
        # Try serializers in priority order
        serializers_to_try = [self.default_serializer, self.fallback_serializer]
        serializers_to_try.extend([
            name for name in self.serializers.keys() 
            if name not in serializers_to_try
        ])
        
        last_error = None
        for serializer_name in serializers_to_try:
            try:
                result = self.serializers[serializer_name].deserialize(data)
                return result, serializer_name
            except Exception as e:
                last_error = e
                continue
        
        raise SerializationError(f"Could not deserialize with any serializer: {last_error}")


def serialize_with_metadata(
    data: Any,
    serializer: str = "pickle",
    metadata: Optional[Dict[str, Any]] = None,
    compress: bool = False
) -> bytes:
    """
    Serialize data with metadata header.
    
    Args:
        data: Data to serialize
        serializer: Serializer to use
        metadata: Additional metadata
        compress: Whether to compress
        
    Returns:
        Serialized bytes with metadata
    """
    helper = SerializationHelper()
    
    # Serialize the actual data
    serialized_data = helper.safe_serialize(data, serializer, compress=compress)
    
    # Create metadata
    meta = SerializationMetadata(
        serializer=serializer,
        timestamp=time.time(),
        compressed=compress,
        size_bytes=len(serialized_data)
    )
    
    if metadata:
        meta_dict = meta.to_dict()
        meta_dict.update(metadata)
        meta = SerializationMetadata.from_dict(meta_dict)
    
    # Create payload with metadata
    payload = {
        'metadata': meta.to_dict(),
        'data': serialized_data
    }
    
    # Serialize the entire payload (always use pickle for the wrapper)
    return pickle.dumps(payload)


def deserialize_with_metadata(data: bytes) -> Tuple[Any, SerializationMetadata]:
    """
    Deserialize data and return data + metadata.
    
    Args:
        data: Serialized bytes with metadata
        
    Returns:
        Tuple of (deserialized_data, metadata)
    """
    helper = SerializationHelper()
    
    try:
        # Try to deserialize as payload with metadata
        payload = pickle.loads(data)
        
        if isinstance(payload, dict) and 'metadata' in payload and 'data' in payload:
            # Extract metadata and data
            metadata = SerializationMetadata.from_dict(payload['metadata'])
            serialized_data = payload['data']
            
            # Deserialize the actual data
            actual_data = helper.safe_deserialize(
                serialized_data,
                metadata.serializer,
                decompress=metadata.compressed
            )
            
            return actual_data, metadata
        else:
            # Legacy data - no metadata wrapper
            return payload, SerializationMetadata(
                serializer="pickle",
                timestamp=time.time()
            )
            
    except Exception:
        # Try auto-deserialization
        try:
            result, serializer_used = helper.auto_deserialize(data)
            return result, SerializationMetadata(
                serializer=serializer_used,
                timestamp=time.time()
            )
        except Exception as e:
            raise SerializationError(f"Could not deserialize data: {e}") from e


class CompressedSerializer:
    """
    Wrapper that adds compression to any serializer.
    
    Usage:
        compressed_pickle = CompressedSerializer(PickleSerializer())
        data = compressed_pickle.serialize(large_object)
    """
    
    def __init__(self, base_serializer: Serializer, compression_level: int = 6):
        """Initialize with base serializer and compression level."""
        self.base_serializer = base_serializer
        self.compression_level = compression_level
    
    def serialize(self, data: Any) -> bytes:
        """Serialize and compress."""
        serialized = self.base_serializer.serialize(data)
        return gzip.compress(serialized, compresslevel=self.compression_level)
    
    def deserialize(self, data: bytes) -> Any:
        """Decompress and deserialize."""
        decompressed = gzip.decompress(data)
        return self.base_serializer.deserialize(decompressed)
    
    @property
    def name(self) -> str:
        return f"compressed_{self.base_serializer.name}"


# Global serialization helper instance
_global_helper = SerializationHelper()


def get_serialization_helper() -> SerializationHelper:
    """Get the global serialization helper."""
    return _global_helper


# Convenience functions using global helper
def serialize(data: Any, serializer: str = "pickle", **kwargs) -> bytes:
    """Serialize data using global helper."""
    return _global_helper.safe_serialize(data, serializer, **kwargs)


def deserialize(data: bytes, serializer: str = "pickle", **kwargs) -> Any:
    """Deserialize data using global helper."""
    return _global_helper.safe_deserialize(data, serializer, **kwargs)


def auto_deserialize(data: bytes, **kwargs) -> Tuple[Any, str]:
    """Auto-deserialize data using global helper."""
    return _global_helper.auto_deserialize(data, **kwargs)


def serialize_job_data(data: Any, serializer: str = "pickle", **kwargs) -> bytes:
    """
    Serialize job data for backward compatibility.
    
    Args:
        data: Data to serialize
        serializer: Serializer to use
        **kwargs: Additional arguments
        
    Returns:
        Serialized data
    """
    return serialize(data, serializer, **kwargs)


def deserialize_job_data(data: bytes, serializer: str = "pickle", **kwargs) -> Any:
    """
    Deserialize job data for backward compatibility.
    
    Args:
        data: Serialized data
        serializer: Serializer to use
        **kwargs: Additional arguments
        
    Returns:
        Deserialized data
    """
    return deserialize(data, serializer, **kwargs)


def serialize_result(data: Any, serializer: str = "pickle", **kwargs) -> bytes:
    """Serialize result data for backward compatibility."""
    return serialize(data, serializer, **kwargs)


def deserialize_result(data: bytes, serializer: str = "pickle", **kwargs) -> Any:
    """Deserialize result data for backward compatibility."""
    return deserialize(data, serializer, **kwargs)