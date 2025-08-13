# tests/test_config/test_validation.py
"""Tests for configuration validation."""

import pytest
from naq.services import ServiceManager
from naq.config.validator import ConfigValidator
from naq.exceptions import ConfigurationError


class TestConfigurationValidation:
    """Test configuration validation functionality."""

    def test_valid_config_passes_validation(self, service_test_config):
        """Test that valid configuration passes validation."""
        # Should not raise any errors
        manager = ServiceManager(service_test_config)
        assert manager is not None

    def test_invalid_nats_url_raises_error(self):
        """Test that invalid NATS URL raises validation error."""
        invalid_config = {
            'nats': {
                'url': '',  # Empty URL should be invalid
            },
            'workers': {
                'concurrency': 2
            }
        }
        
        # Should raise validation error
        with pytest.raises((ValueError, ConfigurationError)):
            ServiceManager(invalid_config)

    def test_negative_concurrency_raises_error(self):
        """Test that negative concurrency raises validation error."""
        invalid_config = {
            'nats': {
                'url': 'nats://localhost:4222'
            },
            'workers': {
                'concurrency': -1  # Negative concurrency should be invalid
            }
        }
        
        with pytest.raises((ValueError, ConfigurationError)):
            ServiceManager(invalid_config)

    def test_zero_concurrency_raises_error(self):
        """Test that zero concurrency raises validation error."""
        invalid_config = {
            'nats': {
                'url': 'nats://localhost:4222'
            },
            'workers': {
                'concurrency': 0  # Zero concurrency should be invalid
            }
        }
        
        with pytest.raises((ValueError, ConfigurationError)):
            ServiceManager(invalid_config)

    def test_invalid_max_reconnect_attempts(self):
        """Test validation of max_reconnect_attempts."""
        invalid_config = {
            'nats': {
                'url': 'nats://localhost:4222',
                'max_reconnect_attempts': -1  # Negative value should be invalid
            },
            'workers': {
                'concurrency': 2
            }
        }
        
        with pytest.raises((ValueError, ConfigurationError)):
            ServiceManager(invalid_config)

    def test_invalid_heartbeat_interval(self):
        """Test validation of worker heartbeat interval."""
        invalid_config = {
            'nats': {
                'url': 'nats://localhost:4222'
            },
            'workers': {
                'concurrency': 2,
                'heartbeat_interval': -5  # Negative interval should be invalid
            }
        }
        
        with pytest.raises((ValueError, ConfigurationError)):
            ServiceManager(invalid_config)

    def test_invalid_ttl_value(self):
        """Test validation of TTL values."""
        invalid_config = {
            'nats': {
                'url': 'nats://localhost:4222'
            },
            'workers': {
                'concurrency': 2,
                'ttl': 0  # Zero or negative TTL should be invalid
            }
        }
        
        with pytest.raises((ValueError, ConfigurationError)):
            ServiceManager(invalid_config)

    def test_missing_required_sections(self):
        """Test validation with missing required configuration sections."""
        # Completely empty config
        empty_config = {}
        
        # Should either provide defaults or raise error
        try:
            manager = ServiceManager(empty_config)
            # If successful, should have reasonable defaults
            assert manager is not None
        except (ValueError, ConfigurationError, KeyError):
            # Expected if required sections are missing
            pass

    def test_unknown_config_sections_ignored(self, service_test_config):
        """Test that unknown configuration sections are ignored."""
        config_with_extra = service_test_config.copy()
        config_with_extra['unknown_section'] = {
            'random_key': 'random_value'
        }
        
        # Should not raise error for unknown sections
        manager = ServiceManager(config_with_extra)
        assert manager is not None

    def test_invalid_event_batch_size(self):
        """Test validation of event batch size."""
        invalid_config = {
            'nats': {
                'url': 'nats://localhost:4222'
            },
            'workers': {
                'concurrency': 2
            },
            'events': {
                'enabled': True,
                'batch_size': 0  # Zero batch size should be invalid
            }
        }
        
        with pytest.raises((ValueError, ConfigurationError)):
            ServiceManager(invalid_config)

    def test_invalid_flush_interval(self):
        """Test validation of event flush interval."""
        invalid_config = {
            'nats': {
                'url': 'nats://localhost:4222'
            },
            'workers': {
                'concurrency': 2
            },
            'events': {
                'enabled': True,
                'flush_interval': -1.0  # Negative interval should be invalid
            }
        }
        
        with pytest.raises((ValueError, ConfigurationError)):
            ServiceManager(invalid_config)

    def test_config_validator_direct_usage(self):
        """Test ConfigValidator class directly."""
        validator = ConfigValidator()
        
        valid_config = {
            'nats': {
                'url': 'nats://localhost:4222',
                'max_reconnect_attempts': 3
            },
            'workers': {
                'concurrency': 4,
                'heartbeat_interval': 5,
                'ttl': 30
            }
        }
        
        # Should validate successfully
        is_valid, errors = validator.validate_config(valid_config)
        assert is_valid is True
        assert len(errors) == 0

    def test_config_validator_with_errors(self):
        """Test ConfigValidator returns proper errors."""
        validator = ConfigValidator()
        
        invalid_config = {
            'nats': {
                'url': '',  # Invalid URL
                'max_reconnect_attempts': -1  # Invalid value
            },
            'workers': {
                'concurrency': 0  # Invalid concurrency
            }
        }
        
        is_valid, errors = validator.validate_config(invalid_config)
        assert is_valid is False
        assert len(errors) > 0
        
        # Errors should contain descriptive messages
        error_text = ' '.join(errors)
        assert 'url' in error_text.lower() or 'nats' in error_text.lower()

    def test_type_validation(self):
        """Test validation of configuration value types."""
        invalid_config = {
            'nats': {
                'url': 'nats://localhost:4222',
                'max_reconnect_attempts': 'invalid'  # Should be integer
            },
            'workers': {
                'concurrency': '2'  # Should be integer, not string
            }
        }
        
        # Type validation should catch incorrect types
        with pytest.raises((ValueError, ConfigurationError, TypeError)):
            ServiceManager(invalid_config)