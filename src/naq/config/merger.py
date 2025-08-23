"""Configuration merging utilities for NAQ."""

from typing import Any, Dict


def merge_config(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge two dictionaries with proper handling of different data types.

    This function merges the override dictionary into the base dictionary, with the
    following behavior:
    - For scalar values, values from override overwrite values from base
    - For nested dictionaries, the function recursively merges the dictionaries
    - For lists, the list in base is replaced with the list from override

    The function is pure and returns a new dictionary without modifying the inputs.

    Args:
        base: The base dictionary to merge into.
        override: The dictionary containing overriding values.

    Returns:
        A new dictionary containing the merged result.

    Example:
        >>> base = {"a": 1, "b": {"c": 2, "d": [1, 2]}}
        >>> override = {"b": {"c": 3, "e": 4}, "f": [5, 6]}
        >>> merge_config(base, override)
        {'a': 1, 'b': {'c': 3, 'd': [1, 2], 'e': 4}, 'f': [5, 6]}
    """
    # Create a deep copy of the base dictionary to avoid modifying the original
    result = base.copy()

    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            # Recursively merge nested dictionaries
            result[key] = merge_config(result[key], value)
        else:
            # For all other cases (including lists), replace with override value
            result[key] = value

    return result
