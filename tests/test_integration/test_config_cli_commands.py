"""Integration tests for CLI configuration commands."""

import os
import tempfile
import json
import yaml
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest

from naq.config import load_config, get_config, reload_config
from naq.cli.system_commands import config_show, config_validate, generate_config_cmd
from naq.exceptions import ConfigurationError


class TestConfigCLICommands:
    """Test integration between CLI commands and configuration system."""

    def test_config_show_with_default_config(self):
        """Test config_show command with default configuration."""
        # Ensure no config file exists and no environment variables are set
        for key in ["NAQ_NATS_URL", "NAQ_DEFAULT_QUEUE", "NAQ_LOG_LEVEL"]:
            if key in os.environ:
                del os.environ[key]
        
        reload_config()
        
        # Mock the console.print_json to capture output
        with patch("naq.cli.system_commands.console.print_json") as mock_print:
            # Create a mock context
            ctx = MagicMock()
            
            # Call config_show without a config path
            config_show(ctx, config_path=None)
            
            # Verify that print_json was called
            mock_print.assert_called_once()
            
            # Get the JSON string that was printed
            json_str = mock_print.call_args[0][0]
            config_data = json.loads(json_str)
            
            # Verify that default values are present
            assert "nats" in config_data
            assert "servers" in config_data["nats"]
            assert config_data["nats"]["servers"] == ["nats://localhost:4222"]
            assert "queues" in config_data
            assert "default_name" in config_data["queues"]
            assert config_data["queues"]["default_name"] == "naq_default_queue"

    def test_config_show_with_custom_config(self):
        """Test config_show command with custom configuration file."""
        # Create a temporary config file
        with tempfile.TemporaryDirectory() as temp_dir:
            config_file = Path(temp_dir) / "config.yaml"
            config_file.write_text("""
nats:
  servers:
    - nats://custom-server:4222
queues:
  default_name: custom_queue
logging:
  level: DEBUG
""")
            
            # Mock the console.print_json to capture output
            with patch("naq.cli.system_commands.console.print_json") as mock_print:
                # Create a mock context
                ctx = MagicMock()
                
                # Call config_show with the config path
                config_show(ctx, config_path=config_file)
                
                # Verify that print_json was called
                mock_print.assert_called_once()
                
                # Get the JSON string that was printed
                json_str = mock_print.call_args[0][0]
                config_data = json.loads(json_str)
                
                # Verify that custom values are present
                assert config_data["nats"]["servers"] == ["nats://custom-server:4222"]
                assert config_data["queues"]["default_name"] == "custom_queue"
                assert config_data["logging"]["level"] == "DEBUG"

    def test_config_show_with_invalid_config(self):
        """Test config_show command with invalid configuration file."""
        # Create a temporary invalid config file
        with tempfile.TemporaryDirectory() as temp_dir:
            config_file = Path(temp_dir) / "invalid_config.yaml"
            config_file.write_text("""
nats:
  servers: invalid_value  # Should be a list
""")
            
            # Mock the console.print to capture error message
            with patch("naq.cli.system_commands.console.print") as mock_print:
                # Create a mock context
                ctx = MagicMock()
                
                # Call config_show with the invalid config path
                with pytest.raises(SystemExit) as exc_info:
                    config_show(ctx, config_path=config_file)
                
                # Verify that the command exited with code 1
                assert exc_info.value.code == 1
                
                # Verify that an error message was printed
                mock_print.assert_called_once()
                assert "Error loading configuration" in mock_print.call_args[0][0]

    def test_config_validate_with_valid_config(self):
        """Test config_validate command with valid configuration."""
        # Create a temporary valid config file
        with tempfile.TemporaryDirectory() as temp_dir:
            config_file = Path(temp_dir) / "valid_config.yaml"
            config_file.write_text("""
nats:
  servers:
    - nats://localhost:4222
queues:
  default_name: naq_default_queue
""")
            
            # Mock the console.print to capture success message
            with patch("naq.cli.system_commands.console.print") as mock_print:
                # Create a mock context
                ctx = MagicMock()
                
                # Call config_validate with the valid config path
                config_validate(ctx, config_path=config_file)
                
                # Verify that a success message was printed
                mock_print.assert_called_once()
                assert "Configuration is valid!" in mock_print.call_args[0][0]

    def test_config_validate_with_invalid_config(self):
        """Test config_validate command with invalid configuration."""
        # Create a temporary invalid config file
        with tempfile.TemporaryDirectory() as temp_dir:
            config_file = Path(temp_dir) / "invalid_config.yaml"
            config_file.write_text("""
nats:
  servers: invalid_value  # Should be a list
""")
            
            # Mock the console.print to capture error message
            with patch("naq.cli.system_commands.console.print") as mock_print:
                # Create a mock context
                ctx = MagicMock()
                
                # Call config_validate with the invalid config path
                with pytest.raises(SystemExit) as exc_info:
                    config_validate(ctx, config_path=config_file)
                
                # Verify that the command exited with code 1
                assert exc_info.value.code == 1
                
                # Verify that an error message was printed
                mock_print.assert_called_once()
                assert "Configuration validation failed" in mock_print.call_args[0][0]

    def test_config_validate_with_default_config(self):
        """Test config_validate command with default configuration."""
        # Ensure no config file exists and no environment variables are set
        for key in ["NAQ_NATS_URL", "NAQ_DEFAULT_QUEUE", "NAQ_LOG_LEVEL"]:
            if key in os.environ:
                del os.environ[key]
        
        reload_config()
        
        # Mock the console.print to capture success message
        with patch("naq.cli.system_commands.console.print") as mock_print:
            # Create a mock context
            ctx = MagicMock()
            
            # Call config_validate without a config path
            config_validate(ctx, config_path=None)
            
            # Verify that a success message was printed
            mock_print.assert_called_once()
            assert "Configuration is valid!" in mock_print.call_args[0][0]

    def test_generate_config_cmd_default(self):
        """Test generate_config_cmd with default settings."""
        # Create a temporary directory for output
        with tempfile.TemporaryDirectory() as temp_dir:
            output_file = Path(temp_dir) / "generated_config.yaml"
            
            # Create a mock context
            ctx = MagicMock()
            
            # Call generate_config_cmd
            generate_config_cmd(ctx, output=output_file, environment="default")
            
            # Verify that the file was created
            assert output_file.exists()
            
            # Load and verify the generated config
            with open(output_file, "r") as f:
                config_data = yaml.safe_load(f)
            
            # Verify that the config contains expected sections
            assert "nats" in config_data
            assert "queues" in config_data
            assert "logging" in config_data
            assert "workers" in config_data
            assert "scheduler" in config_data
            
            # Verify that default values are present
            assert config_data["nats"]["servers"] == ["nats://localhost:4222"]
            assert config_data["queues"]["default_name"] == "naq_default_queue"

    def test_generate_config_cmd_with_existing_file(self):
        """Test generate_config_cmd when output file already exists."""
        # Create a temporary directory for output
        with tempfile.TemporaryDirectory() as temp_dir:
            output_file = Path(temp_dir) / "existing_config.yaml"
            
            # Create an existing file
            output_file.write_text("# Existing config")
            
            # Create a mock context
            ctx = MagicMock()
            
            # Call generate_config_cmd
            generate_config_cmd(ctx, output=output_file, environment="default")
            
            # Verify that the file was overwritten
            assert output_file.exists()
            
            # Load and verify the generated config
            with open(output_file, "r") as f:
                config_data = yaml.safe_load(f)
            
            # Verify that the file contains generated config, not the original content
            assert "nats" in config_data
            assert "# Existing config" not in str(config_data)

    def test_config_show_with_environment_variables(self):
        """Test config_show command with environment variables."""
        # Set environment variables
        os.environ["NAQ_NATS_URL"] = "nats://env-server:4222"
        os.environ["NAQ_DEFAULT_QUEUE"] = "env_queue"
        os.environ["NAQ_LOG_LEVEL"] = "DEBUG"
        
        try:
            # Mock the console.print_json to capture output
            with patch("naq.cli.system_commands.console.print_json") as mock_print:
                # Create a mock context
                ctx = MagicMock()
                
                # Call config_show without a config path
                config_show(ctx, config_path=None)
                
                # Verify that print_json was called
                mock_print.assert_called_once()
                
                # Get the JSON string that was printed
                json_str = mock_print.call_args[0][0]
                config_data = json.loads(json_str)
                
                # Verify that environment variable values are present
                assert config_data["nats"]["servers"] == ["nats://env-server:4222"]
                assert config_data["queues"]["default_name"] == "env_queue"
                assert config_data["logging"]["level"] == "DEBUG"
        finally:
            # Clean up
            for key in ["NAQ_NATS_URL", "NAQ_DEFAULT_QUEUE", "NAQ_LOG_LEVEL"]:
                if key in os.environ:
                    del os.environ[key]
            reload_config()

    def test_config_show_with_config_and_env_vars(self):
        """Test config_show command with both config file and environment variables."""
        # Set environment variables
        os.environ["NAQ_NATS_URL"] = "nats://env-server:4222"
        
        try:
            # Create a temporary config file
            with tempfile.TemporaryDirectory() as temp_dir:
                config_file = Path(temp_dir) / "config.yaml"
                config_file.write_text("""
nats:
  servers:
    - nats://config-server:4222
queues:
  default_name: config_queue
""")
                
                # Mock the console.print_json to capture output
                with patch("naq.cli.system_commands.console.print_json") as mock_print:
                    # Create a mock context
                    ctx = MagicMock()
                    
                    # Call config_show with the config path
                    config_show(ctx, config_path=config_file)
                    
                    # Verify that print_json was called
                    mock_print.assert_called_once()
                    
                    # Get the JSON string that was printed
                    json_str = mock_print.call_args[0][0]
                    config_data = json.loads(json_str)
                    
                    # Verify that environment variable takes precedence
                    assert config_data["nats"]["servers"] == ["nats://env-server:4222"]
                    # But config file values are still used for non-env settings
                    assert config_data["queues"]["default_name"] == "config_queue"
        finally:
            # Clean up
            if "NAQ_NATS_URL" in os.environ:
                del os.environ["NAQ_NATS_URL"]
            reload_config()

    def test_config_validate_with_nonexistent_file(self):
        """Test config_validate command with nonexistent file."""
        # Create a path to a nonexistent file
        nonexistent_file = Path("/nonexistent/config.yaml")
        
        # Mock the console.print to capture error message
        with patch("naq.cli.system_commands.console.print") as mock_print:
            # Create a mock context
            ctx = MagicMock()
            
            # Call config_validate with the nonexistent file
            with pytest.raises(SystemExit) as exc_info:
                config_validate(ctx, config_path=nonexistent_file)
            
            # Verify that the command exited with code 1
            assert exc_info.value.code == 1
            
            # Verify that an error message was printed
            mock_print.assert_called_once()
            assert "Configuration validation failed" in mock_print.call_args[0][0]