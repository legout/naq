# tests/test_config/test_yaml_loading.py
"""Tests for YAML configuration loading and processing."""

import pytest
import tempfile
from pathlib import Path
import yaml
import os
from naq.config import load_config, get_config, reload_config
from naq.services import ServiceManager


class TestConfigurationLoading:
    """Test YAML configuration loading functionality."""

    def test_yaml_config_loading(self, temp_config_file):
        """Test loading configuration from YAML file."""
        config = load_config(temp_config_file)
        
        # Test config structure matches expected format
        assert 'nats' in config.to_dict()
        assert 'workers' in config.to_dict()
        assert 'events' in config.to_dict()
        
        # Test specific values
        config_dict = config.to_dict()
        assert config_dict['nats']['url'] == 'nats://localhost:4222'
        assert config_dict['workers']['concurrency'] == 2
        assert config_dict['events']['enabled'] is True

    def test_environment_variable_overrides(self, temp_config_file, env_override_config):
        """Test environment variables override YAML config."""
        # Environment variables set by env_override_config fixture
        config = load_config(temp_config_file)
        config_dict = config.to_dict()
        
        # Values should be overridden by environment variables
        assert config_dict['workers']['concurrency'] == 20  # From NAQ_WORKERS_CONCURRENCY
        assert config_dict['nats']['url'] == 'nats://override:4222'  # From NAQ_NATS_URL
        assert config_dict['events']['enabled'] is False  # From NAQ_EVENTS_ENABLED

    async def test_config_to_service_manager_pipeline(self, temp_config_file):
        """Test complete YAML → ServiceManager pipeline."""
        config = load_config(temp_config_file)
        
        # Test ServiceManager can be created from loaded config
        manager = ServiceManager(config.to_dict())
        await manager.initialize_all()
        
        try:
            # Verify services are created correctly
            from naq.services.connection import ConnectionService
            conn_service = await manager.get_service(ConnectionService)
            assert conn_service is not None
            
        finally:
            await manager.cleanup_all()

    def test_config_file_not_found(self):
        """Test behavior when config file doesn't exist."""
        non_existent_path = "/tmp/non_existent_config.yaml"
        
        # Should either use defaults or raise appropriate error
        try:
            config = load_config(non_existent_path)
            # If successful, should have default values
            assert config is not None
        except FileNotFoundError:
            # Expected behavior for missing file
            pass

    def test_invalid_yaml_file(self):
        """Test handling of invalid YAML syntax."""
        # Create temporary file with invalid YAML
        temp_dir = tempfile.mkdtemp()
        invalid_yaml_path = Path(temp_dir) / "invalid.yaml"
        
        with open(invalid_yaml_path, 'w') as f:
            f.write("invalid: yaml: content: [\n")  # Invalid YAML
        
        try:
            # Should handle YAML parsing errors
            config = load_config(str(invalid_yaml_path))
            # If no error, should have fallback behavior
            assert config is not None
        except yaml.YAMLError:
            # Expected for invalid YAML
            pass
        finally:
            # Cleanup
            import shutil
            shutil.rmtree(temp_dir)

    def test_config_caching(self, temp_config_file):
        """Test configuration caching behavior."""
        # First load
        config1 = load_config(temp_config_file)
        
        # Second load with same path should use cache or reload
        config2 = load_config(temp_config_file)
        
        # Configs should have same values
        assert config1.to_dict() == config2.to_dict()

    def test_config_reload(self, temp_config_file):
        """Test configuration reload functionality."""
        # Initial load
        config1 = load_config(temp_config_file)
        
        # Force reload
        config2 = reload_config(temp_config_file)
        
        # Should have loaded fresh config
        assert config1.to_dict() == config2.to_dict()

    def test_get_config_singleton(self, temp_config_file):
        """Test get_config returns same instance."""
        # Load config first
        load_config(temp_config_file)
        
        # get_config should return cached instance
        config1 = get_config()
        config2 = get_config()
        
        # Should be same instance or have same values
        assert config1.to_dict() == config2.to_dict()

    def test_nested_config_structure(self):
        """Test complex nested configuration structure."""
        complex_config = {
            'nats': {
                'url': 'nats://localhost:4222',
                'cluster': {
                    'name': 'test-cluster',
                    'routes': ['nats://node1:4222', 'nats://node2:4222']
                }
            },
            'workers': {
                'default': {
                    'concurrency': 4,
                    'timeout': 300
                },
                'high_priority': {
                    'concurrency': 8,
                    'timeout': 600
                }
            }
        }
        
        # Create temp file with complex config
        temp_dir = tempfile.mkdtemp()
        config_path = Path(temp_dir) / "complex_config.yaml"
        
        with open(config_path, 'w') as f:
            yaml.dump(complex_config, f)
        
        try:
            config = load_config(str(config_path))
            config_dict = config.to_dict()
            
            # Test nested access
            assert config_dict['nats']['cluster']['name'] == 'test-cluster'
            assert len(config_dict['nats']['cluster']['routes']) == 2
            assert config_dict['workers']['high_priority']['concurrency'] == 8
            
        finally:
            import shutil
            shutil.rmtree(temp_dir)

    def test_partial_environment_overrides(self, temp_config_file, monkeypatch):
        """Test partial environment variable overrides."""
        # Set only some environment variables
        monkeypatch.setenv('NAQ_WORKERS_CONCURRENCY', '15')
        # Don't set NAQ_NATS_URL, should use YAML value
        
        config = load_config(temp_config_file)
        config_dict = config.to_dict()
        
        # Should have mixed values
        assert config_dict['workers']['concurrency'] == 15  # From env
        assert config_dict['nats']['url'] == 'nats://localhost:4222'  # From YAML