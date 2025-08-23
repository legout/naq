"""Serialization utilities for NAQ.

This module contains common serialization utilities used throughout the NAQ codebase.
"""

import datetime
import json
import pickle
from typing import Any, Dict, Optional, Type, Union

from naq.exceptions import SerializationError


class SerializationHelper:
    """Helper class for consistent serialization and deserialization operations.
    
    Provides static methods for safe serialization and deserialization using
    different serialization formats (pickle, json) with proper error handling.
    """

    @staticmethod
    def safe_serialize(
        data: Any,
        serializer: str = "pickle",
        fallback_serializer: Optional[str] = None,
    ) -> Union[bytes, str]:
        """Safely serialize data using the specified serializer.
        
        Args:
            data: The data to serialize.
            serializer: The serializer type to use ("pickle" or "json").
            fallback_serializer: Optional fallback serializer if the primary fails.
            
        Returns:
            Serialized data as bytes (for pickle) or str (for json).
            
        Raises:
            SerializationError: If serialization fails with both primary and fallback serializers.
            ValueError: If an unsupported serializer type is specified.
        """
        if serializer not in ("pickle", "json"):
            raise ValueError(f"Unsupported serializer: {serializer}. Use 'pickle' or 'json'.")
        
        if fallback_serializer and fallback_serializer not in ("pickle", "json"):
            raise ValueError(
                f"Unsupported fallback serializer: {fallback_serializer}. Use 'pickle' or 'json'."
            )
        
        try:
            if serializer == "pickle":
                return pickle.dumps(data)
            else:  # json
                return json.dumps(data)
        except (pickle.PicklingError, TypeError, ValueError) as e:
            if fallback_serializer and fallback_serializer != serializer:
                try:
                    if fallback_serializer == "pickle":
                        return pickle.dumps(data)
                    else:  # json
                        return json.dumps(data)
                except (pickle.PicklingError, TypeError, ValueError) as fallback_error:
                    raise SerializationError(
                        f"Failed to serialize data with both {serializer} and "
                        f"{fallback_serializer}. Primary error: {str(e)}. "
                        f"Fallback error: {str(fallback_error)}"
                    ) from fallback_error
            else:
                raise SerializationError(f"Failed to serialize data with {serializer}: {str(e)}") from e

    @staticmethod
    def safe_deserialize(
        bytes_data: Union[bytes, str],
        serializer: str = "pickle",
        expected_type: Optional[Type[Any]] = None,
    ) -> Any:
        """Safely deserialize data using the specified serializer.
        
        Args:
            bytes_data: The serialized data to deserialize.
            serializer: The serializer type to use ("pickle" or "json").
            expected_type: Optional type to check the deserialized data against.
            
        Returns:
            The deserialized data.
            
        Raises:
            SerializationError: If deserialization fails.
            ValueError: If an unsupported serializer type is specified.
            TypeError: If the deserialized data doesn't match the expected_type.
        """
        if serializer not in ("pickle", "json"):
            raise ValueError(f"Unsupported serializer: {serializer}. Use 'pickle' or 'json'.")
        
        try:
            if serializer == "pickle":
                if not isinstance(bytes_data, bytes):
                    raise TypeError("Pickle deserialization requires bytes data")
                data = pickle.loads(bytes_data)
            else:  # json
                if not isinstance(bytes_data, (str, bytes)):
                    raise TypeError("JSON deserialization requires string or bytes data")
                if isinstance(bytes_data, bytes):
                    bytes_data = bytes_data.decode("utf-8")
                data = json.loads(bytes_data)
            
            # Type checking if expected_type is provided
            if expected_type is not None and not isinstance(data, expected_type):
                raise TypeError(
                    f"Deserialized data is of type {type(data).__name__}, "
                    f"but expected {expected_type.__name__}"
                )
            
            return data
        except (pickle.UnpicklingError, json.JSONDecodeError, TypeError, ValueError) as e:
            raise SerializationError(f"Failed to deserialize data with {serializer}: {str(e)}") from e


def serialize_with_metadata(
    data: Any,
    serializer: str = "pickle",
    metadata: Optional[Dict[str, Any]] = None,
) -> Union[bytes, str]:
    """Serialize data along with metadata, including serializer type and timestamp.
    
    Args:
        data: The data to serialize.
        serializer: The serializer type to use ("pickle" or "json"). Defaults to "pickle".
        metadata: Optional metadata dictionary to include with the serialized data.
                 Defaults to an empty dictionary.
                 
    Returns:
        Serialized data as bytes (for pickle) or str (for json).
        
    Raises:
        SerializationError: If serialization fails.
        ValueError: If an unsupported serializer type is specified.
    """
    if metadata is None:
        metadata = {}
    
    # Generate timestamp
    timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    
    # Create metadata dictionary with serializer info and timestamp
    enhanced_metadata = {
        "serializer": serializer,
        "timestamp": timestamp,
        **metadata,
    }
    
    # Construct payload dictionary
    payload = {
        "metadata": enhanced_metadata,
        "data": data,
    }
    
    # Serialize the payload using SerializationHelper.safe_serialize
    return SerializationHelper.safe_serialize(payload, serializer)


def deserialize_with_metadata(
    bytes_data: bytes,
) -> tuple[Any, Dict[str, Any]]:
    """Deserialize data and extract both the data payload and its associated metadata.
    
    This function attempts to deserialize the data using pickle.loads first, then falls back
    to json.loads if pickle fails. It checks for a 'metadata' key in the deserialized payload
    and returns both the data and metadata if present, or the data and an empty dictionary
    if no metadata is found.
    
    Args:
        bytes_data: The serialized data as bytes to deserialize.
        
    Returns:
        A tuple containing (data, metadata) where:
        - data: The deserialized data payload
        - metadata: A dictionary containing metadata, or an empty dictionary if no metadata
                   was found in the payload
        
    Raises:
        SerializationError: If deserialization fails completely with both pickle and json.
        
    Examples:
        >>> # Example with metadata
        >>> data_with_metadata = {"data": [1, 2, 3], "metadata": {"source": "test"}}
        >>> serialized = pickle.dumps(data_with_metadata)
        >>> data, metadata = deserialize_with_metadata(serialized)
        >>> data
        [1, 2, 3]
        >>> metadata
        {'source': 'test'}
        
        >>> # Example without metadata
        >>> data_without_metadata = {"payload": "simple data"}
        >>> serialized = pickle.dumps(data_without_metadata)
        >>> data, metadata = deserialize_with_metadata(serialized)
        >>> data
        {'payload': 'simple data'}
        >>> metadata
        {}
    """
    # Try to deserialize with pickle first
    try:
        deserialized = pickle.loads(bytes_data)
    except (pickle.UnpicklingError, TypeError, ValueError, EOFError) as pickle_error:
        # If pickle fails, try with json
        try:
            # Handle both bytes and string input for json deserialization
            if isinstance(bytes_data, bytes):
                json_str = bytes_data.decode("utf-8")
            else:
                json_str = bytes_data
            deserialized = json.loads(json_str)
        except (json.JSONDecodeError, UnicodeDecodeError, TypeError, ValueError) as json_error:
            # If both pickle and json fail, raise SerializationError
            raise SerializationError(
                f"Failed to deserialize data with both pickle and json. "
                f"Pickle error: {str(pickle_error)}. JSON error: {str(json_error)}"
            ) from json_error
    
    # Check if the deserialized data has a 'metadata' key
    if isinstance(deserialized, dict) and "metadata" in deserialized:
        metadata = deserialized["metadata"]
        # Extract the data payload - could be in 'data' key or the entire dict minus metadata
        if "data" in deserialized:
            data = deserialized["data"]
        else:
            # If no explicit 'data' key, create a copy of the dict without metadata
            data = {k: v for k, v in deserialized.items() if k != "metadata"}
        return data, metadata
    else:
        # No metadata found, return the deserialized data and empty dict
        return deserialized, {}