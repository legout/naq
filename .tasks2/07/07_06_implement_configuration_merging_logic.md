### Sub-task 6: Implement Configuration Merging Logic
**Description:** Develop a utility for merging configuration dictionaries, handling nested structures and lists appropriately.
**Implementation Steps:**
- Create the file `src/naq/config/merger.py`.
- Implement a function (e.g., `merge_config`) that recursively merges two dictionaries. This function should handle overwriting scalar values and merging nested dictionaries. For lists, a clear strategy needs to be defined (e.g., replace, append, or merge based on unique keys). The provided task implies a simple overwrite for lists, but the `merger.py` file suggests a dedicated module.
- Integrate this merging logic into the `ConfigLoader`'s `load_config` method.
**Success Criteria:**
- The `src/naq/config/merger.py` file is created.
- The merging function correctly combines configuration from different sources, respecting the hierarchy.
**Testing:**
- Create unit tests for `merger.py` covering:
    - Merging simple dictionaries.
    - Merging nested dictionaries.
    - Behavior when merging lists (e.g., replacement).
    - Merging with empty dictionaries.
**Documentation:**
- Document the merging strategy used in the configuration system.