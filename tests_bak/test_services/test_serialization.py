"""Tests for the serialization utilities."""

import json
import pickle
import pytest

from naq.exceptions import SerializationError
from naq.utils.serialization import deserialize_with_metadata, serialize_with_metadata


class TestDeserializeWithMetadata:
    """Test cases for the deserialize_with_metadata function."""

    def test_deserialize_with_metadata_pickle(self):
        """Test deserializing data with metadata using pickle."""
        # Create test data with metadata
        test_data = {"result": [1, 2, 3], "count": 3}
        test_metadata = {"source": "test", "version": "1.0"}
        payload = {"data": test_data, "metadata": test_metadata}
        
        # Serialize with pickle
        serialized = pickle.dumps(payload)
        
        # Deserialize and verify
        data, metadata = deserialize_with_metadata(serialized)
        assert data == test_data
        assert metadata == test_metadata

    def test_deserialize_with_metadata_json(self):
        """Test deserializing data with metadata using JSON."""
        # Create test data with metadata
        test_data = {"result": [1, 2, 3], "count": 3}
        test_metadata = {"source": "test", "version": "1.0"}
        payload = {"data": test_data, "metadata": test_metadata}
        
        # Serialize with JSON
        serialized = json.dumps(payload).encode("utf-8")
        
        # Deserialize and verify
        data, metadata = deserialize_with_metadata(serialized)
        assert data == test_data
        assert metadata == test_metadata

    def test_deserialize_without_metadata_pickle(self):
        """Test deserializing data without metadata using pickle."""
        # Create test data without metadata
        test_data = {"result": [1, 2, 3], "count": 3}
        
        # Serialize with pickle
        serialized = pickle.dumps(test_data)
        
        # Deserialize and verify
        data, metadata = deserialize_with_metadata(serialized)
        assert data == test_data
        assert metadata == {}

    def test_deserialize_without_metadata_json(self):
        """Test deserializing data without metadata using JSON."""
        # Create test data without metadata
        test_data = {"result": [1, 2, 3], "count": 3}
        
        # Serialize with JSON
        serialized = json.dumps(test_data).encode("utf-8")
        
        # Deserialize and verify
        data, metadata = deserialize_with_metadata(serialized)
        assert data == test_data
        assert metadata == {}

    def test_deserialize_with_metadata_no_data_key_pickle(self):
        """Test deserializing data with metadata but no explicit data key using pickle."""
        # Create test data with metadata but no explicit data key
        test_metadata = {"source": "test", "version": "1.0"}
        payload = {"result": [1, 2, 3], "count": 3, "metadata": test_metadata}
        
        # Serialize with pickle
        serialized = pickle.dumps(payload)
        
        # Deserialize and verify
        data, metadata = deserialize_with_metadata(serialized)
        expected_data = {"result": [1, 2, 3], "count": 3}
        assert data == expected_data
        assert metadata == test_metadata

    def test_deserialize_with_metadata_no_data_key_json(self):
        """Test deserializing data with metadata but no explicit data key using JSON."""
        # Create test data with metadata but no explicit data key
        test_metadata = {"source": "test", "version": "1.0"}
        payload = {"result": [1, 2, 3], "count": 3, "metadata": test_metadata}
        
        # Serialize with JSON
        serialized = json.dumps(payload).encode("utf-8")
        
        # Deserialize and verify
        data, metadata = deserialize_with_metadata(serialized)
        expected_data = {"result": [1, 2, 3], "count": 3}
        assert data == expected_data
        assert metadata == test_metadata

    def test_deserialize_fallback_to_json(self):
        """Test that deserialization falls back to JSON when pickle fails."""
        # Create test data with metadata
        test_data = {"result": [1, 2, 3], "count": 3}
        test_metadata = {"source": "test", "version": "1.0"}
        payload = {"data": test_data, "metadata": test_metadata}
        
        # Serialize with JSON
        serialized = json.dumps(payload).encode("utf-8")
        
        # Deserialize and verify (should fall back to JSON)
        data, metadata = deserialize_with_metadata(serialized)
        assert data == test_data
        assert metadata == test_metadata

    def test_deserialize_invalid_data(self):
        """Test that deserialization raises SerializationError for invalid data."""
        # Create invalid bytes that can't be deserialized
        invalid_bytes = b"invalid serialization data"
        
        # Should raise SerializationError
        with pytest.raises(SerializationError):
            deserialize_with_metadata(invalid_bytes)

    def test_deserialize_empty_bytes(self):
        """Test that deserialization raises SerializationError for empty bytes."""
        # Empty bytes
        empty_bytes = b""
        
        # Should raise SerializationError
        with pytest.raises(SerializationError):
            deserialize_with_metadata(empty_bytes)

    def test_compatibility_with_serialize_with_metadata(self):
        """Test compatibility with serialize_with_metadata function."""
        # Create test data and metadata
        test_data = {"result": [1, 2, 3], "count": 3}
        test_metadata = {"source": "test", "version": "1.0"}
        
        # Serialize using serialize_with_metadata with pickle
        serialized_pickle = serialize_with_metadata(test_data, "pickle", test_metadata)
        
        # Deserialize and verify
        data, metadata = deserialize_with_metadata(serialized_pickle)
        assert data == test_data
        assert metadata["source"] == "test"
        assert metadata["version"] == "1.0"
        assert "serializer" in metadata
        assert metadata["serializer"] == "pickle"
        assert "timestamp" in metadata
        
        # Serialize using serialize_with_metadata with json
        serialized_json = serialize_with_metadata(test_data, "json", test_metadata)
        
        # Convert string to bytes for deserialize_with_metadata
        if isinstance(serialized_json, str):
            serialized_json = serialized_json.encode("utf-8")
        
        # Deserialize and verify
        data, metadata = deserialize_with_metadata(serialized_json)
        assert data == test_data
        assert metadata["source"] == "test"
        assert metadata["version"] == "1.0"
        assert "serializer" in metadata
        assert metadata["serializer"] == "json"
        assert "timestamp" in metadata