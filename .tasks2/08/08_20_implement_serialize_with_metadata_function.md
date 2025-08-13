### Sub-task: Implement `serialize_with_metadata` Function
**Description:** Implement a utility function `serialize_with_metadata` to serialize data along with metadata, including serializer type and timestamp.
**Implementation Steps:**
- Implement the `serialize_with_metadata` function in `src/naq/utils/serialization.py`.
- The function should accept data, an optional `serializer` type, and optional `metadata`.
- It should create a payload dictionary containing `metadata` and the actual `data`.
- The `metadata` should include `serializer` type and `timestamp`.
- It should use `SerializationHelper.safe_serialize` internally.
**Success Criteria:**
- The `serialize_with_metadata` function is correctly implemented in `src/naq/utils/serialization.py`.
- It correctly embeds metadata within the serialized data.
**Testing:**
- Unit tests for `serialize_with_metadata` to verify:
    - Metadata is correctly added to the serialized output.
    - Serializer type and timestamp are correctly recorded in metadata.
    - Data can be successfully deserialized with metadata.
**Documentation:**
- Add a comprehensive docstring to the `serialize_with_metadata` function explaining its purpose, parameters, and usage examples.
- Update the API documentation for `src/naq/utils/serialization.py` to include `serialize_with_metadata`.