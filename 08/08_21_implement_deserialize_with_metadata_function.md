### Sub-task: Implement `deserialize_with_metadata` Function
**Description:** Implement a utility function `deserialize_with_metadata` to deserialize data and extract both the data payload and its associated metadata.
**Implementation Steps:**
- Implement the `deserialize_with_metadata` function in `src/naq/utils/serialization.py`.
- The function should accept bytes data.
- It should attempt to deserialize the data using `pickle.loads` first, then fall back to `json.loads` if pickle fails.
- It should check for a `metadata` key in the deserialized payload.
- If metadata is present, return both the data and metadata; otherwise, return the data and an empty dictionary.
- Raise `SerializationError` if deserialization fails completely.
**Success Criteria:**
- The `deserialize_with_metadata` function is correctly implemented in `src/naq/utils/serialization.py`.
- It can correctly deserialize data and extract metadata.
- It handles data without metadata gracefully.
**Testing:**
- Unit tests for `deserialize_with_metadata` to verify:
    - Successful deserialization and metadata extraction for data serialized with metadata.
    - Correct handling of data without metadata.
    - Robust error handling for corrupted or un-deserializable data.
**Documentation:**
- Add a comprehensive docstring to the `deserialize_with_metadata` function explaining its purpose, parameters, and usage examples.
- Update the API documentation for `src/naq/utils/serialization.py` to include `deserialize_with_metadata`.