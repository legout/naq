"""Unit tests for configuration merging utilities."""

import pytest
from typing import Any, Dict

from naq.config.merger import merge_config


class TestMergeConfig:
    """Test cases for merge_config function."""

    def test_merge_simple_dictionaries(self) -> None:
        """Test merging simple dictionaries."""
        base = {"a": 1, "b": 2}
        override = {"b": 3, "c": 4}
        result = merge_config(base, override)
        
        expected = {"a": 1, "b": 3, "c": 4}
        assert result == expected

    def test_merge_nested_dictionaries(self) -> None:
        """Test merging nested dictionaries."""
        base = {
            "a": 1,
            "b": {
                "x": 10,
                "y": 20
            }
        }
        override = {
            "b": {
                "y": 30,
                "z": 40
            },
            "c": 3
        }
        result = merge_config(base, override)
        
        expected = {
            "a": 1,
            "b": {
                "x": 10,  # From base
                "y": 30,  # From override
                "z": 40   # From override
            },
            "c": 3  # From override
        }
        assert result == expected

    def test_merge_deeply_nested_dictionaries(self) -> None:
        """Test merging deeply nested dictionaries."""
        base = {
            "level1": {
                "level2": {
                    "level3": {
                        "a": 1,
                        "b": 2
                    },
                    "other": "base"
                }
            }
        }
        override = {
            "level1": {
                "level2": {
                    "level3": {
                        "b": 3,
                        "c": 4
                    }
                }
            }
        }
        result = merge_config(base, override)
        
        expected = {
            "level1": {
                "level2": {
                    "level3": {
                        "a": 1,  # From base
                        "b": 3,  # From override
                        "c": 4   # From override
                    },
                    "other": "base"  # From base
                }
            }
        }
        assert result == expected

    def test_merge_with_lists_replacement(self) -> None:
        """Test that lists are replaced rather than merged."""
        base = {
            "a": [1, 2, 3],
            "b": {
                "x": ["base1", "base2"]
            }
        }
        override = {
            "a": [4, 5],
            "b": {
                "x": ["override1"]
            }
        }
        result = merge_config(base, override)
        
        expected = {
            "a": [4, 5],  # Replaced
            "b": {
                "x": ["override1"]  # Replaced
            }
        }
        assert result == expected

    def test_merge_with_empty_base(self) -> None:
        """Test merging with empty base dictionary."""
        base = {}
        override = {"a": 1, "b": {"x": 2}}
        result = merge_config(base, override)
        
        expected = {"a": 1, "b": {"x": 2}}
        assert result == expected

    def test_merge_with_empty_override(self) -> None:
        """Test merging with empty override dictionary."""
        base = {"a": 1, "b": {"x": 2}}
        override = {}
        result = merge_config(base, override)
        
        expected = {"a": 1, "b": {"x": 2}}
        assert result == expected

    def test_merge_with_both_empty(self) -> None:
        """Test merging with both dictionaries empty."""
        base = {}
        override = {}
        result = merge_config(base, override)
        
        expected = {}
        assert result == expected

    def test_merge_preserves_base_types(self) -> None:
        """Test that merge preserves original data types."""
        base = {
            "str": "string",
            "int": 42,
            "float": 3.14,
            "bool": True,
            "none": None,
            "list": [1, 2, 3],
            "dict": {"nested": "value"}
        }
        override = {
            "str": "overridden",
            "int": 100,
            "float": 2.71,
            "bool": False,
            "none": "not_none",
            "list": [4, 5],
            "dict": {"nested": "overridden", "new": "value"}
        }
        result = merge_config(base, override)
        
        expected = {
            "str": "overridden",
            "int": 100,
            "float": 2.71,
            "bool": False,
            "none": "not_none",
            "list": [4, 5],
            "dict": {"nested": "overridden", "new": "value"}
        }
        assert result == expected

    def test_merge_with_none_values(self) -> None:
        """Test merging with None values."""
        base = {
            "a": 1,
            "b": {"x": 2},
            "c": None
        }
        override = {
            "a": None,
            "b": None,
            "d": 4
        }
        result = merge_config(base, override)
        
        expected = {
            "a": None,
            "b": None,
            "c": None,
            "d": 4
        }
        assert result == expected

    def test_merge_with_mixed_types(self) -> None:
        """Test merging with mixed data types."""
        base = {
            "scalar": "base",
            "nested": {
                "a": 1,
                "b": [1, 2, 3]
            }
        }
        override = {
            "scalar": {"new": "structure"},  # Replace scalar with dict
            "nested": {
                "b": "not_a_list",  # Replace list with string
                "c": [4, 5, 6]  # Add new list
            }
        }
        result = merge_config(base, override)
        
        expected = {
            "scalar": {"new": "structure"},
            "nested": {
                "a": 1,  # From base
                "b": "not_a_list",  # From override
                "c": [4, 5, 6]  # From override
            }
        }
        assert result == expected

    def test_merge_does_not_modify_originals(self) -> None:
        """Test that merge doesn't modify original dictionaries."""
        base = {"a": 1, "b": {"x": 2}}
        override = {"b": {"y": 3}, "c": 4}
        
        # Make copies to compare against
        original_base = base.copy()
        original_override = override.copy()
        
        result = merge_config(base, override)
        
        # Check originals are unchanged
        assert base == original_base
        assert override == original_override
        
        # Check result is correct
        expected = {"a": 1, "b": {"x": 2, "y": 3}, "c": 4}
        assert result == expected

    def test_merge_with_complex_nested_structure(self) -> None:
        """Test merging with complex nested structure."""
        base = {
            "nats": {
                "servers": ["nats://localhost:4222"],
                "client_name": "base-client",
                "auth": None
            },
            "workers": {
                "concurrency": 1,
                "pools": {
                    "default": {"size": 5}
                }
            }
        }
        override = {
            "nats": {
                "servers": ["nats://prod:4222"],
                "auth": {"user": "admin", "password": "secret"}
            },
            "workers": {
                "concurrency": 4,
                "heartbeat_interval": 60.0
            }
        }
        result = merge_config(base, override)
        
        expected = {
            "nats": {
                "servers": ["nats://prod:4222"],  # From override
                "client_name": "base-client",  # From base
                "auth": {"user": "admin", "password": "secret"}  # From override
            },
            "workers": {
                "concurrency": 4,  # From override
                "pools": {  # From base
                    "default": {"size": 5}
                },
                "heartbeat_interval": 60.0  # From override
            }
        }
        assert result == expected

    def test_merge_with_special_characters_in_keys(self) -> None:
        """Test merging with special characters in dictionary keys."""
        base = {
            "key.with.dots": {"nested": "value"},
            "key-with-dashes": [1, 2, 3],
            "key_with_underscores": "string"
        }
        override = {
            "key.with.dots": {"new": "value"},
            "key-with-dashes": [4, 5],
            "new_key": "new_value"
        }
        result = merge_config(base, override)
        
        expected = {
            "key.with.dots": {"nested": "value", "new": "value"},
            "key-with-dashes": [4, 5],
            "key_with_underscores": "string",
            "new_key": "new_value"
        }
        assert result == expected

    def test_merge_with_unicode_keys_and_values(self) -> None:
        """Test merging with unicode keys and values."""
        base = {
            "café": {"value": "base"},
            "naïve": [1, 2, 3],
            "résumé": "base_value"
        }
        override = {
            "café": {"value": "override"},
            "naïve": [4, 5],
            "new_unicode": "nëw_välüe"
        }
        result = merge_config(base, override)
        
        expected = {
            "café": {"value": "override"},
            "naïve": [4, 5],
            "résumé": "base_value",
            "new_unicode": "nëw_välüe"
        }
        assert result == expected