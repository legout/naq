"""Unit tests for configuration loader."""

import os
import tempfile
import pytest
from unittest.mock import patch, MagicMock
from typing import Any, Dict

import yaml

from naq.config.loader import ConfigLoader
from naq.exceptions import ConfigurationError


class TestConfigLoader:
    """Test cases for ConfigLoader class."""

    def test_init_with_explicit_path(self) -> None:
        """Test ConfigLoader initialization with explicit config path."""
        config_path = "/path/to/config.yaml"
        loader = ConfigLoader(config_path)
        assert loader.config_path == config_path

    def test_init_without_explicit_path(self) -> None:
        """Test ConfigLoader initialization without explicit config path."""
        loader = ConfigLoader()
        assert loader.config_path is None

    def test_load_config_default_paths_priority(self) -> None:
        """Test that config loading respects default paths priority."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create multiple config files
            config1_path = os.path.join(temp_dir, "naq.yaml")
            config2_path = os.path.join(temp_dir, "config", "naq.yaml")
            os.makedirs(os.path.join(temp_dir, "config"), exist_ok=True)
            
            config1_content = {"nats": {"servers": ["nats://server1:4222"]}}
            config2_content = {"nats": {"servers": ["nats://server2:4222"]}}
            
            with open(config1_path, "w") as f:
                yaml.dump(config1_content, f)
            with open(config2_path, "w") as f:
                yaml.dump(config2_content, f)
            
            # Test that the first found config is used
            with patch("naq.config.loader.ConfigLoader.DEFAULT_CONFIG_PATHS", [
                config1_path, config2_path
            ]):
                loader = ConfigLoader()
                config = loader.load_config()
                # Should use config1 since it's first in the list
                assert config["nats"]["servers"] == ["nats://server1:4222"]

    def test_load_config_with_explicit_path(self) -> None:
        """Test config loading with explicit path."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "custom_config.yaml")
            config_content = {"nats": {"servers": ["nats://custom:4222"]}}
            
            with open(config_path, "w") as f:
                yaml.dump(config_content, f)
            
            loader = ConfigLoader(config_path)
            config = loader.load_config()
            assert config["nats"]["servers"] == ["nats://custom:4222"]

    def test_load_config_nonexistent_explicit_path(self) -> None:
        """Test that loading nonexistent explicit config path raises error."""
        loader = ConfigLoader("/nonexistent/path/config.yaml")
        with pytest.raises(ConfigurationError, match="Failed to load explicit config file"):
            loader.load_config()

    def test_load_config_invalid_yaml(self) -> None:
        """Test that loading invalid YAML raises error."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "invalid.yaml")
            with open(config_path, "w") as f:
                f.write("invalid: yaml: content: [")
            
            loader = ConfigLoader(config_path)
            with pytest.raises(ConfigurationError, match="Invalid YAML"):
                loader.load_config()

    def test_load_yaml_file_success(self) -> None:
        """Test successful YAML file loading."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "test.yaml")
            config_content = {
                "nats": {"servers": ["nats://localhost:4222"]},
                "workers": {"concurrency": 4}
            }
            
            with open(config_path, "w") as f:
                yaml.dump(config_content, f)
            
            loader = ConfigLoader()
            result = loader._load_yaml_file(config_path)
            assert result == config_content

    def test_load_yaml_file_nonexistent(self) -> None:
        """Test loading nonexistent YAML file."""
        loader = ConfigLoader()
        with pytest.raises(FileNotFoundError):
            loader._load_yaml_file("/nonexistent/file.yaml")

    def test_interpolate_env_vars_with_defaults(self) -> None:
        """Test environment variable interpolation with default values."""
        content = """
        nats:
          servers: ["${NATS_SERVER:nats://localhost:4222}"]
          client_name: "${CLIENT_NAME:naq-client}"
        """
        
        # Without env vars set, should use defaults
        loader = ConfigLoader()
        result = loader._interpolate_env_vars(content)
        assert "nats://localhost:4222" in result
        assert "naq-client" in result

    def test_interpolate_env_vars_with_values(self) -> None:
        """Test environment variable interpolation with set values."""
        content = """
        nats:
          servers: ["${NATS_SERVER:nats://localhost:4222}"]
          client_name: "${CLIENT_NAME:naq-client}"
        """
        
        # Set environment variables
        with patch.dict(os.environ, {
            "NATS_SERVER": "nats://prod:4222",
            "CLIENT_NAME": "prod-client"
        }):
            loader = ConfigLoader()
            result = loader._interpolate_env_vars(content)
            assert "nats://prod:4222" in result
            assert "prod-client" in result

    def test_interpolate_env_vars_no_defaults(self) -> None:
        """Test environment variable interpolation without defaults."""
        content = """
        nats:
          servers: ["${NATS_SERVER}"]
          client_name: "${CLIENT_NAME}"
        """
        
        # Set environment variables
        with patch.dict(os.environ, {
            "NATS_SERVER": "nats://prod:4222",
            "CLIENT_NAME": "prod-client"
        }):
            loader = ConfigLoader()
            result = loader._interpolate_env_vars(content)
            assert "nats://prod:4222" in result
            assert "prod-client" in result

    def test_interpolate_env_vars_unset_no_defaults(self) -> None:
        """Test environment variable interpolation with unset vars and no defaults."""
        content = """
        nats:
          servers: ["${UNSET_VAR}"]
        """
        
        # Ensure UNSET_VAR is not set
        with patch.dict(os.environ, {}, clear=True):
            loader = ConfigLoader()
            result = loader._interpolate_env_vars(content)
            # Should replace with empty string
            assert 'servers: [""]' in result

    def test_load_env_variables(self) -> None:
        """Test loading environment variables into config structure."""
        env_vars = {
            "NAQ_NATS__SERVERS": "nats://env:4222",
            "NAQ_NATS__CLIENT_NAME": "env-client",
            "NAQ_WORKERS__CONCURRENCY": "8",
            "NAQ_WORKERS__HEARTBEAT_INTERVAL": "60.0",
            "NAQ_EVENTS__ENABLED": "true",
            "NAQ_EVENTS__BATCH_SIZE": "200"
        }
        
        with patch.dict(os.environ, env_vars):
            loader = ConfigLoader()
            result = loader._load_env_variables()
            
            assert result["nats"]["servers"] == "nats://env:4222"
            assert result["nats"]["client_name"] == "env-client"
            assert result["workers"]["concurrency"] == 8
            assert result["workers"]["heartbeat_interval"] == 60.0
            assert result["events"]["enabled"] is True
            assert result["events"]["batch_size"] == 200

    def test_load_env_variables_non_naq(self) -> None:
        """Test that non-NAQ environment variables are ignored."""
        env_vars = {
            "OTHER_VAR": "should-be-ignored",
            "NAQ_NATS__SERVERS": "nats://env:4222"
        }
        
        with patch.dict(os.environ, env_vars):
            loader = ConfigLoader()
            result = loader._load_env_variables()
            
            assert "OTHER_VAR" not in result
            assert result["nats"]["servers"] == "nats://env:4222"

    def test_convert_env_value_boolean(self) -> None:
        """Test environment variable value conversion for booleans."""
        loader = ConfigLoader()
        
        # Test various boolean representations
        assert loader._convert_env_value("true") is True
        assert loader._convert_env_value("True") is True
        assert loader._convert_env_value("TRUE") is True
        assert loader._convert_env_value("yes") is True
        assert loader._convert_env_value("YES") is True
        assert loader._convert_env_value("1") is True
        assert loader._convert_env_value("on") is True
        assert loader._convert_env_value("ON") is True
        
        assert loader._convert_env_value("false") is False
        assert loader._convert_env_value("False") is False
        assert loader._convert_env_value("FALSE") is False
        assert loader._convert_env_value("no") is False
        assert loader._convert_env_value("NO") is False
        assert loader._convert_env_value("0") is False
        assert loader._convert_env_value("off") is False
        assert loader._convert_env_value("OFF") is False

    def test_convert_env_value_integer(self) -> None:
        """Test environment variable value conversion for integers."""
        loader = ConfigLoader()
        
        assert loader._convert_env_value("42") == 42
        assert loader._convert_env_value("0") == 0
        assert loader._convert_env_value("-1") == -1
        assert loader._convert_env_value("999999") == 999999

    def test_convert_env_value_float(self) -> None:
        """Test environment variable value conversion for floats."""
        loader = ConfigLoader()
        
        assert loader._convert_env_value("3.14") == 3.14
        assert loader._convert_env_value("0.0") == 0.0
        assert loader._convert_env_value("-2.5") == -2.5
        assert loader._convert_env_value("1e-6") == 1e-6

    def test_convert_env_value_string(self) -> None:
        """Test environment variable value conversion for strings."""
        loader = ConfigLoader()
        
        assert loader._convert_env_value("hello") == "hello"
        assert loader._convert_env_value("123abc") == "123abc"
        assert loader._convert_env_value("true-ish") == "true-ish"
        assert loader._convert_env_value("") == ""

    def test_apply_environment_overrides(self) -> None:
        """Test applying environment variable overrides to config."""
        base_config = {
            "nats": {
                "servers": ["nats://default:4222"],
                "client_name": "default-client"
            },
            "workers": {
                "concurrency": 1,
                "heartbeat_interval": 30.0
            }
        }
        
        env_vars = {
            "NAQ_NATS__SERVERS": "nats://env:4222",
            "NAQ_WORKERS__CONCURRENCY": "8"
        }
        
        with patch.dict(os.environ, env_vars):
            loader = ConfigLoader()
            result = loader._apply_environment_overrides(base_config)
            
            assert result["nats"]["servers"] == "nats://env:4222"
            assert result["nats"]["client_name"] == "default-client"  # Unchanged
            assert result["workers"]["concurrency"] == 8
            assert result["workers"]["heartbeat_interval"] == 30.0  # Unchanged

    def test_load_config_full_priority_order(self) -> None:
        """Test full configuration loading priority order."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create default config file
            default_config_path = os.path.join(temp_dir, "naq.yaml")
            default_config_content = {
                "nats": {"servers": ["nats://default:4222"], "client_name": "default"},
                "workers": {"concurrency": 1, "heartbeat_interval": 30.0}
            }
            with open(default_config_path, "w") as f:
                yaml.dump(default_config_content, f)
            
            # Create explicit config file
            explicit_config_path = os.path.join(temp_dir, "explicit.yaml")
            explicit_config_content = {
                "nats": {"servers": ["nats://explicit:4222"]},
                "workers": {"concurrency": 4}
            }
            with open(explicit_config_path, "w") as f:
                yaml.dump(explicit_config_content, f)
            
            # Set environment variables
            env_vars = {
                "NAQ_WORKERS__HEARTBEAT_INTERVAL": "60.0"
            }
            
            with patch.dict(os.environ, env_vars):
                with patch("naq.config.loader.ConfigLoader.DEFAULT_CONFIG_PATHS", [default_config_path]):
                    loader = ConfigLoader(explicit_config_path)
                    result = loader.load_config()
                    
                    # Check priority: explicit > env > default
                    assert result["nats"]["servers"] == ["nats://explicit:4222"]  # From explicit
                    assert result["nats"]["client_name"] == "default"  # From default
                    assert result["workers"]["concurrency"] == 4  # From explicit
                    assert result["workers"]["heartbeat_interval"] == 60.0  # From env

    def test_load_config_with_env_interpolation(self) -> None:
        """Test config loading with environment variable interpolation in YAML."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "test.yaml")
            config_content = """
            nats:
              servers: ["${NATS_SERVER:nats://localhost:4222}"]
              client_name: "${CLIENT_NAME:naq-client}"
            workers:
              concurrency: ${WORKER_CONCURRENCY:1}
            """
            
            with open(config_path, "w") as f:
                f.write(config_content)
            
            # Set some environment variables
            env_vars = {
                "NATS_SERVER": "nats://env:4222",
                "WORKER_CONCURRENCY": "4"
            }
            
            with patch.dict(os.environ, env_vars):
                loader = ConfigLoader(config_path)
                result = loader.load_config()
                
                assert result["nats"]["servers"] == ["nats://env:4222"]
                assert result["nats"]["client_name"] == "naq-client"  # Default
                assert result["workers"]["concurrency"] == 4