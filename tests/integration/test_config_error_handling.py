"""Integration tests for ConfigurationError handling."""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from naq.config import load_config, get_config, reload_config
from naq.exceptions import ConfigurationError


class TestConfigErrorHandling:
    """Test ConfigurationError handling across the configuration system."""

    def test_load_config_with_invalid_yaml(self):
        """Test that load_config raises ConfigurationError for invalid YAML."""
        # Create a temporary file with invalid YAML
        with tempfile.TemporaryDirectory() as temp_dir:
            config_file = Path(temp_dir) / "invalid.yaml"
            config_file.write_text("""
nats:
  servers:
    - nats://localhost:4222
  invalid_yaml: [
""")
            
            # Should raise ConfigurationError
            with pytest.raises(ConfigurationError) as exc_info:
                load_config(str(config_file))
            
            # Verify the error message
            assert "Failed to load configuration" in str(exc_info.value)

    def test_load_config_with_nonexistent_file(self):
        """Test that load_config raises ConfigurationError for nonexistent file."""
        # Path to a nonexistent file
        nonexistent_file = "/nonexistent/config.yaml"
        
        # Should raise ConfigurationError
        with pytest.raises(ConfigurationError) as exc_info:
            load_config(nonexistent_file)
        
        # Verify the error message
        assert "Failed to load configuration" in str(exc_info.value)

    def test_load_config_with_invalid_schema(self):
        """Test that load_config raises ConfigurationError for invalid schema."""
        # Create a temporary file with invalid schema
        with tempfile.TemporaryDirectory() as temp_dir:
            config_file = Path(temp_dir) / "invalid_schema.yaml"
            config_file.write_text("""
nats:
  servers: invalid_value  # Should be a list
""")
            
            # Should raise ConfigurationError when validation is enabled
            with pytest.raises(ConfigurationError) as exc_info:
                load_config(str(config_file), validate=True)
            
            # Verify the error message
            assert "Configuration validation failed" in str(exc_info.value)

    def test_load_config_without_validation_bypasses_schema_errors(self):
        """Test that load_config without validation bypasses schema errors."""
        # Create a temporary file with invalid schema
        with tempfile.TemporaryDirectory() as temp_dir:
            config_file = Path(temp_dir) / "invalid_schema.yaml"
            config_file.write_text("""
nats:
  servers: invalid_value  # Should be a list
""")
            
            # Should not raise ConfigurationError when validation is disabled
            config = load_config(str(config_file), validate=False)
            
            # Should have default values for invalid fields
            assert config.nats.servers == ["nats://localhost:4222"]

    def test_reload_config_with_invalid_file(self):
        """Test that reload_config raises ConfigurationError for invalid file."""
        # Load a valid config first
        load_config()
        
        # Create a temporary file with invalid YAML
        with tempfile.TemporaryDirectory() as temp_dir:
            config_file = Path(temp_dir) / "invalid.yaml"
            config_file.write_text("""
nats:
  servers:
    - nats://localhost:4222
  invalid_yaml: [
""")
            
            # Should raise ConfigurationError
            with pytest.raises(ConfigurationError) as exc_info:
                reload_config(str(config_file))
            
            # Verify the error message
            assert "Failed to load configuration" in str(exc_info.value)
            
            # Global config should still be the original valid one
            assert get_config().nats.servers == ["nats://localhost:4222"]

    def test_reload_config_preserves_original_on_error(self):
        """Test that reload_config preserves original config on error."""
        # Create a temporary file with valid config
        with tempfile.TemporaryDirectory() as temp_dir:
            config_file1 = Path(temp_dir) / "valid1.yaml"
            config_file1.write_text("""
nats:
  servers:
    - nats://server1:4222
""")
            
            # Load initial config
            config1 = load_config(str(config_file1))
            assert config1.nats.servers == ["nats://server1:4222"]
            
            # Create a temporary file with invalid config
            config_file2 = Path(temp_dir) / "invalid.yaml"
            config_file2.write_text("""
nats:
  servers: invalid_value
""")
            
            # Try to reload with invalid config
            with pytest.raises(ConfigurationError):
                reload_config(str(config_file2))
            
            # Global config should still be the original valid one
            assert get_config() is config1
            assert get_config().nats.servers == ["nats://server1:4222"]

    def test_get_config_with_invalid_environment_variable(self):
        """Test that get_config handles invalid environment variables."""
        # Set an invalid environment variable
        os.environ["NAQ_NATS_URL"] = "invalid-url"
        
        try:
            # Clear any existing config
            reload_config._config_instance = None
            
            # Should not raise ConfigurationError, but should use defaults
            config = get_config()
            
            # Should have default values
            assert config.nats.servers == ["nats://localhost:4222"]
        finally:
            # Clean up
            if "NAQ_NATS_URL" in os.environ:
                del os.environ["NAQ_NATS_URL"]
            reload_config()

    def test_configuration_error_inheritance(self):
        """Test that ConfigurationError inherits from NaqException."""
        # Create a temporary file with invalid YAML
        with tempfile.TemporaryDirectory() as temp_dir:
            config_file = Path(temp_dir) / "invalid.yaml"
            config_file.write_text("""
nats:
  servers:
    - nats://localhost:4222
  invalid_yaml: [
""")
            
            # Should raise ConfigurationError
            with pytest.raises(ConfigurationError) as exc_info:
                load_config(str(config_file))
            
            # Verify that it's a NaqException
            assert isinstance(exc_info.value, ConfigurationError)
            assert isinstance(exc_info.value, Exception)

    def test_configuration_error_with_custom_message(self):
        """Test ConfigurationError with custom message."""
        # Create a ConfigurationError with a custom message
        error = ConfigurationError("Custom error message")
        
        # Verify the message
        assert str(error) == "Custom error message"

    def test_configuration_error_chaining(self):
        """Test ConfigurationError with exception chaining."""
        # Create a temporary file with invalid YAML
        with tempfile.TemporaryDirectory() as temp_dir:
            config_file = Path(temp_dir) / "invalid.yaml"
            config_file.write_text("""
nats:
  servers:
    - nats://localhost:4222
  invalid_yaml: [
""")
            
            # Should raise ConfigurationError
            with pytest.raises(ConfigurationError) as exc_info:
                load_config(str(config_file))
            
            # Verify that the original exception is chained
            assert exc_info.value.__cause__ is not None
            assert isinstance(exc_info.value.__cause__, Exception)

    def test_multiple_configuration_errors(self):
        """Test handling of multiple configuration errors."""
        # Create a temporary file with multiple issues
        with tempfile.TemporaryDirectory() as temp_dir:
            config_file = Path(temp_dir) / "multiple_errors.yaml"
            config_file.write_text("""
nats:
  servers: invalid_value  # Invalid type
queues:
  default_name: 123  # Invalid type
""")
            
            # Should raise ConfigurationError
            with pytest.raises(ConfigurationError) as exc_info:
                load_config(str(config_file), validate=True)
            
            # Verify the error message contains information about the validation errors
            error_message = str(exc_info.value)
            assert "Configuration validation failed" in error_message

    def test_configuration_error_in_service_config(self):
        """Test ConfigurationError in service configuration."""
        from naq.services.config import create_config_from_env
        
        # Set an invalid environment variable
        os.environ["NAQ_NATS_URL"] = "invalid-url"
        
        try:
            # Should not raise ConfigurationError, but should handle gracefully
            config = create_config_from_env("connection")
            
            # Should have default values
            assert config.nats_url == "invalid-url"  # Environment variable is used as-is
        finally:
            # Clean up
            if "NAQ_NATS_URL" in os.environ:
                del os.environ["NAQ_NATS_URL"]

    def test_configuration_error_in_cli_commands(self):
        """Test ConfigurationError handling in CLI commands."""
        from naq.cli.system_commands import config_show, config_validate
        
        # Create a temporary file with invalid YAML
        with tempfile.TemporaryDirectory() as temp_dir:
            config_file = Path(temp_dir) / "invalid.yaml"
            config_file.write_text("""
nats:
  servers:
    - nats://localhost:4222
  invalid_yaml: [
""")
            
            # Mock the console.print to capture error message
            with patch("naq.cli.system_commands.console.print") as mock_print:
                # Create a mock context
                ctx = MagicMock()
                
                # Test config_show with invalid file
                with pytest.raises(SystemExit) as exc_info:
                    config_show(ctx, config_path=config_file)
                
                # Verify that the command exited with code 1
                assert exc_info.value.code == 1
                
                # Verify that an error message was printed
                mock_print.assert_called_once()
                assert "Error loading configuration" in mock_print.call_args[0][0]

    def test_configuration_error_with_empty_file(self):
        """Test ConfigurationError with empty configuration file."""
        # Create a temporary empty file
        with tempfile.TemporaryDirectory() as temp_dir:
            config_file = Path(temp_dir) / "empty.yaml"
            config_file.write_text("")
            
            # Should raise ConfigurationError
            with pytest.raises(ConfigurationError) as exc_info:
                load_config(str(config_file))
            
            # Verify the error message
            assert "Failed to load configuration" in str(exc_info.value)

    def test_configuration_error_with_directory_instead_of_file(self):
        """Test ConfigurationError when directory is provided instead of file."""
        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Try to load the directory as a config file
            with pytest.raises(ConfigurationError) as exc_info:
                load_config(temp_dir)
            
            # Verify the error message
            assert "Failed to load configuration" in str(exc_info.value)

    def test_configuration_error_with_unreadable_file(self):
        """Test ConfigurationError with unreadable file."""
        # Create a temporary file
        with tempfile.TemporaryDirectory() as temp_dir:
            config_file = Path(temp_dir) / "unreadable.yaml"
            config_file.write_text("""
nats:
  servers:
    - nats://localhost:4222
""")
            
            # Make the file unreadable
            os.chmod(config_file, 0o000)
            
            try:
                # Should raise ConfigurationError
                with pytest.raises(ConfigurationError) as exc_info:
                    load_config(str(config_file))
                
                # Verify the error message
                assert "Failed to load configuration" in str(exc_info.value)
            finally:
                # Restore permissions so the file can be deleted
                os.chmod(config_file, 0o644)