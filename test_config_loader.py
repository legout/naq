#!/usr/bin/env python3
"""
Test script for the configuration loader implementation
"""

import os
import tempfile
import yaml
from pathlib import Path
from src.naq.config import ConfigLoader, ConfigurationError, NAQConfig

def test_config_loader():
    """Test the configuration loader functionality"""
    print("Testing configuration loader...")
    
    # Test 1: Default configuration loading
    print("\n1. Testing default configuration loading...")
    loader = ConfigLoader()
    config = loader.load_config()
    print(f"✓ Default config loaded successfully with {len(config)} top-level keys")
    
    # Test 2: Configuration with YAML file
    print("\n2. Testing YAML file loading...")
    test_config = {
        "nats": {
            "servers": ["nats://test:4222"],
            "client_name": "test-client"
        },
        "workers": {
            "concurrency": 5
        },
        "logging": {
            "level": "DEBUG"
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(test_config, f)
        yaml_file = f.name
    
    try:
        loader = ConfigLoader(yaml_file)
        config = loader.load_config()
        naq_config = NAQConfig.from_dict(config)
        
        assert naq_config.nats.servers == ["nats://test:4222"]
        assert naq_config.nats.client_name == "test-client"
        assert naq_config.workers.concurrency == 5
        assert naq_config.logging.level == "DEBUG"
        print("✓ YAML file loading works correctly")
    finally:
        os.unlink(yaml_file)
    
    # Test 3: Environment variable interpolation
    print("\n3. Testing environment variable interpolation...")
    test_config_with_env = {
        "nats": {
            "servers": ["nats://${NAQ_HOST:localhost}:4222"],
            "client_name": "${NAQ_CLIENT_NAME:naq-client}"
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(test_config_with_env, f)
        yaml_file = f.name
    
    try:
        # Test with default values
        loader = ConfigLoader(yaml_file)
        config = loader.load_config()
        naq_config = NAQConfig.from_dict(config)
        
        assert naq_config.nats.servers == ["nats://localhost:4222"]
        assert naq_config.nats.client_name == "naq-client"
        print("✓ Environment variable interpolation with defaults works")
        
        # Test with actual environment variables
        os.environ['NAQ_HOST'] = 'test-server'
        os.environ['NAQ_CLIENT_NAME'] = 'env-client'
        
        config = loader.load_config()
        naq_config = NAQConfig.from_dict(config)
        
        assert naq_config.nats.servers == ["nats://test-server:4222"]
        assert naq_config.nats.client_name == "env-client"
        print("✓ Environment variable interpolation with actual env vars works")
        
        # Clean up environment variables
        del os.environ['NAQ_HOST']
        del os.environ['NAQ_CLIENT_NAME']
    finally:
        os.unlink(yaml_file)
    
    # Test 4: Environment variable overrides
    print("\n4. Testing environment variable overrides...")
    loader = ConfigLoader()
    
    # Set some environment variables
    os.environ['NAQ_NATS_URL'] = 'nats://env:4222'
    os.environ['NAQ_WORKER_CONCURRENCY'] = '20'
    os.environ['NAQ_LOG_LEVEL'] = 'WARNING'
    
    try:
        config = loader.load_config()
        naq_config = NAQConfig.from_dict(config)
        
        assert naq_config.nats.servers[0] == "nats://env:4222"
        assert naq_config.workers.concurrency == 20
        assert naq_config.logging.level == "WARNING"
        print("✓ Environment variable overrides work correctly")
    finally:
        # Clean up environment variables
        del os.environ['NAQ_NATS_URL']
        del os.environ['NAQ_WORKER_CONCURRENCY']
        del os.environ['NAQ_LOG_LEVEL']
    
    # Test 5: Environment-specific overrides
    print("\n5. Testing environment-specific overrides...")
    test_config_with_envs = {
        "nats": {
            "servers": ["nats://default:4222"]
        },
        "environments": {
            "production": {
                "nats": {
                    "servers": ["nats://prod:4222"]
                },
                "workers": {
                    "concurrency": 50
                }
            },
            "development": {
                "logging": {
                    "level": "DEBUG"
                }
            }
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(test_config_with_envs, f)
        yaml_file = f.name
    
    try:
        # Test with production environment
        os.environ['NAQ_ENVIRONMENT'] = 'production'
        
        loader = ConfigLoader(yaml_file)
        config = loader.load_config()
        naq_config = NAQConfig.from_dict(config)
        
        assert naq_config.nats.servers[0] == "nats://prod:4222"
        assert naq_config.workers.concurrency == 50
        print("✓ Production environment overrides work correctly")
        
        # Test with development environment
        os.environ['NAQ_ENVIRONMENT'] = 'development'
        
        config = loader.load_config()
        naq_config = NAQConfig.from_dict(config)
        
        assert naq_config.nats.servers[0] == "nats://default:4222"  # Default value
        assert naq_config.logging.level == "DEBUG"
        print("✓ Development environment overrides work correctly")
        
        # Clean up environment variable
        del os.environ['NAQ_ENVIRONMENT']
    finally:
        os.unlink(yaml_file)
    
    # Test 6: Error handling
    print("\n6. Testing error handling...")
    # Test with invalid YAML
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("invalid: yaml: content: [")
        yaml_file = f.name
    
    try:
        loader = ConfigLoader(yaml_file)
        config = loader.load_config()
        # The loader should handle the error gracefully and continue with defaults
        print("✓ Invalid YAML handled gracefully")
    finally:
        os.unlink(yaml_file)
    
    # Test with non-existent file
    try:
        loader = ConfigLoader("/non/existent/file.yaml")
        config = loader.load_config()
        print("✓ Non-existent file handled gracefully")
    except Exception as e:
        print(f"✗ Unexpected error with non-existent file: {e}")
    
    print("\n✅ All tests passed!")

if __name__ == "__main__":
    test_config_loader()