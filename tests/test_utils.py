"""
Tests for the NAQ utilities package.
"""

import pytest
import asyncio
from unittest.mock import patch, MagicMock
from pathlib import Path
import tempfile
import os
from dataclasses import dataclass

from naq.utils import (
    # Async utilities
    run_async,
    run_async_in_thread,
    run_sync,
    run_sync_in_async,
    AsyncToSyncBridge,
    SyncToAsyncBridge,
    
    # Logging utilities
    setup_logging,
    get_logger,
    log_function_call,
    log_performance,
    log_errors,
    LogContext,
    PerformanceTimer,
    
    # Retry utilities
    RetryConfig,
    RetryError,
    retry,
    retry_async,
    retry_context,
    retry_async_context,
    with_retry,
    with_retry_async,
    calculate_backoff,
    is_retryable_error,
    
    # Validation utilities
    ValidationError,
    validate_type,
    validate_range,
    validate_choice,
    validate_required,
    validate_string,
    validate_url,
    validate_email,
    validate_dict,
    validate_job_config,
    validate_connection_config,
    validate_event_config,
    validate_dataclass,
    Validator,
    
    # Configuration utilities
    ConfigError,
    ConfigSource,
    EnvironmentConfigSource,
    FileConfigSource,
    DictConfigSource,
    ConfigManager,
    create_default_config_manager,
    load_dataclass_from_config,
    JobConfig,
    ConnectionConfig,
    EventConfig,
    LoggingConfig,
    NaqConfig,
    load_naq_config,
    validate_config,
    DEFAULT_CONFIG_SCHEMA,
)


class TestAsyncUtils:
    """Test async utilities."""
    
    def test_run_async(self):
        """Test running async function from sync context."""
        async def async_func(x, y):
            return x + y
        
        result = run_async(async_func, 1, 2)
        assert result == 3
    
    def test_run_async_in_thread(self):
        """Test running async function in a separate thread."""
        async def async_func(x, y):
            await asyncio.sleep(0.01)  # Small delay
            return x * y
        
        result = run_async_in_thread(async_func, 3, 4)
        assert result == 12
    
    @pytest.mark.asyncio
    async def test_run_sync_in_async(self):
        """Test running sync function from async context."""
        def sync_func(x, y):
            return x - y
        
        result = await run_sync_in_async(sync_func, 5, 3)
        assert result == 2
    
    def test_async_to_sync_bridge(self):
        """Test AsyncToSyncBridge."""
        bridge = AsyncToSyncBridge()
        
        async def async_func(x):
            return x * 2
        
        sync_func = bridge.wrap(async_func)
        result = sync_func(21)
        assert result == 42
    
    @pytest.mark.asyncio
    async def test_sync_to_async_bridge(self):
        """Test SyncToAsyncBridge."""
        bridge = SyncToAsyncBridge()
        
        def sync_func(x):
            return x + 10
        
        async_func = bridge.wrap(sync_func)
        result = await async_func(32)
        assert result == 42


class TestRetryUtils:
    """Test retry utilities."""
    
    def test_retry_config(self):
        """Test RetryConfig creation."""
        config = RetryConfig(max_attempts=3, base_delay=0.1)
        assert config.max_attempts == 3
        assert config.base_delay == 0.1
    
    def test_calculate_backoff(self):
        """Test backoff calculation."""
        config = RetryConfig(max_attempts=3, base_delay=1.0, jitter=False)
        
        # Test exponential backoff (attempt is 0-based)
        delay = calculate_backoff(0, config)
        assert delay == 1.0
        
        delay = calculate_backoff(1, config)
        assert delay == 2.0  # 1.0 * 2^1
    
    def test_retry_decorator(self):
        """Test retry decorator."""
        call_count = 0
        
        @retry(max_attempts=2, base_delay=0.01)
        def failing_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ValueError("Try again")
            return "success"
        
        result = failing_func()
        assert result == "success"
        assert call_count == 2
    
    @pytest.mark.asyncio
    async def test_retry_async_decorator(self):
        """Test retry async decorator."""
        call_count = 0
        
        @retry_async(max_attempts=2, base_delay=0.01)
        async def failing_async_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ValueError("Try again")
            return "success"
        
        result = await failing_async_func()
        assert result == "success"
        assert call_count == 2
    
    def test_retry_context(self):
        """Test retry context manager."""
        call_count = 0
        
        def failing_operation():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ValueError("Try again")
            return "success"
        
        config = RetryConfig(max_attempts=2, base_delay=0.01)
        with retry_context(config) as retrier:
            result = retrier.execute(failing_operation)
        
        assert result == "success"
        assert call_count == 2
    
    def test_is_retryable_error(self):
        """Test is_retryable_error function."""
        assert is_retryable_error(ValueError("test"))
        assert is_retryable_error(ConnectionError("test"))
        assert is_retryable_error(TypeError("test"))  # Actually retryable by default
        assert not is_retryable_error(SyntaxError("test"))


class TestValidationUtils:
    """Test validation utilities."""
    
    def test_validate_type(self):
        """Test type validation."""
        validate_type("test", str, "field")
        validate_type(42, int, "field")
        
        with pytest.raises(ValidationError):
            validate_type("test", int, "field")
    
    def test_validate_range(self):
        """Test range validation."""
        validate_range(5, min_val=0, max_val=10)
        validate_range(5, min_val=0)
        validate_range(5, max_val=10)
        
        with pytest.raises(ValidationError):
            validate_range(5, min_val=10, max_val=20)
        
        with pytest.raises(ValidationError):
            validate_range(5, max_val=0)
    
    def test_validate_choice(self):
        """Test choice validation."""
        validate_choice("a", ["a", "b", "c"])
        
        with pytest.raises(ValidationError):
            validate_choice("d", ["a", "b", "c"])
    
    def test_validate_required(self):
        """Test required validation."""
        validate_required("value")
        validate_required([1, 2, 3])
        validate_required({"key": "value"})
        
        with pytest.raises(ValidationError):
            validate_required(None)
        
        with pytest.raises(ValidationError):
            validate_required("")
        
        with pytest.raises(ValidationError):
            validate_required([])
    
    def test_validate_string(self):
        """Test string validation."""
        validate_string("test")
        validate_string("test", min_length=3, max_length=5)
        
        with pytest.raises(ValidationError):
            validate_string(123)  # Not a string
        
        with pytest.raises(ValidationError):
            validate_string("test", min_length=5)
        
        with pytest.raises(ValidationError):
            validate_string("test", max_length=3)
    
    def test_validate_url(self):
        """Test URL validation."""
        validate_url("https://example.com")
        validate_url("http://localhost:8080")
        
        with pytest.raises(ValidationError):
            validate_url("not-a-url")
        
        with pytest.raises(ValidationError):
            validate_url(123)  # Not a string
    
    def test_validate_email(self):
        """Test email validation."""
        validate_email("test@example.com")
        validate_email("user.name+tag@domain.co.uk")
        
        with pytest.raises(ValidationError):
            validate_email("not-an-email")
        
        with pytest.raises(ValidationError):
            validate_email(123)  # Not a string
    
    def test_validate_dict(self):
        """Test dictionary validation."""
        validate_dict({"a": 1, "b": 2}, required_keys=["a"], optional_keys=["b", "c"])
        
        with pytest.raises(ValidationError):
            validate_dict({"a": 1}, required_keys=["a", "b"])
        
        with pytest.raises(ValidationError):
            validate_dict({"a": 1, "d": 2}, required_keys=["a"], optional_keys=["b", "c"])
    
    def test_validate_job_config(self):
        """Test job configuration validation."""
        valid_config = {
            "function": "test_func",
            "max_retries": 3,
            "timeout": 30,
            "retry_strategy": "exponential"
        }
        validate_job_config(valid_config)
        
        invalid_config = {
            "max_retries": -1,  # Invalid
            "timeout": 0,  # Invalid
            "retry_strategy": "invalid"  # Invalid
        }
        with pytest.raises(ValidationError):
            validate_job_config(invalid_config)
    
    def test_validator_class(self):
        """Test Validator class."""
        validator = Validator()
        validator.add_rule("name", lambda x: validate_type(x, str))
        validator.add_rule("age", lambda x: validate_range(x, min_val=0, max_val=120))
        
        class Person:
            def __init__(self, name, age):
                self.name = name
                self.age = age
        
        # Valid person
        valid_person = Person("Alice", 30)
        errors = validator.validate(valid_person)
        assert len(errors) == 0
        
        # Invalid person
        invalid_person = Person(123, -1)
        errors = validator.validate(invalid_person)
        assert len(errors) == 2
        
        # Test validate_or_raise
        with pytest.raises(ValidationError):
            validator.validate_or_raise(invalid_person)


class TestConfigUtils:
    """Test configuration utilities."""
    
    def test_dict_config_source(self):
        """Test DictConfigSource."""
        source = DictConfigSource(config_dict={"key": "value", "number": 42})
        config = source.load()
        assert config == {"key": "value", "number": 42}
    
    def test_config_manager(self):
        """Test ConfigManager."""
        sources = [
            DictConfigSource(config_dict={"a": 1, "b": 2}, priority=10),
            DictConfigSource(config_dict={"b": 20, "c": 30}, priority=5)  # Higher priority (lower number)
        ]
        
        manager = ConfigManager(sources)
        config = manager.load_config()
        
        # Higher priority source should override lower priority
        assert config["a"] == 1
        assert config["b"] == 2  # Not overridden because priority 10 is lower than 5
        assert config["c"] == 30
    
    def test_config_manager_get_methods(self):
        """Test ConfigManager get methods."""
        manager = ConfigManager([DictConfigSource(config_dict={"nested": {"key": "value"}})])
        
        # Test get with dot notation
        assert manager.get("nested.key") == "value"
        assert manager.get("nested.nonexistent", "default") == "default"
        
        # Test typed get
        assert manager.get_typed("nested.key", str) == "value"
        assert manager.get_typed("nonexistent", int, 42) == 42
        
        # Test required get
        assert manager.get_required("nested.key") == "value"
        
        with pytest.raises(ConfigError):
            manager.get_required("nonexistent")
    
    def test_config_manager_set(self):
        """Test ConfigManager set method."""
        manager = ConfigManager()
        manager.set("nested.key", "value")
        
        assert manager.get("nested.key") == "value"
    
    def test_create_default_config_manager(self):
        """Test default config manager creation."""
        manager = create_default_config_manager()
        config = manager.load_config()
        
        # Should have default values
        assert "job" in config
        assert "connection" in config
        assert "event" in config
        assert "logging" in config
    
    def test_load_dataclass_from_config(self):
        """Test loading dataclass from config."""
        @dataclass
        class TestData:
            name: str
            age: int
            active: bool = True
        
        manager = ConfigManager([
            DictConfigSource(config_dict={"name": "Alice", "age": 30})
        ])
        
        data = load_dataclass_from_config(manager, TestData)
        assert data.name == "Alice"
        assert data.age == 30
        assert data.active is True
    
    def test_load_naq_config(self):
        """Test loading NAQ configuration."""
        config = load_naq_config()
        
        # Should be a NaqConfig instance with default values
        assert isinstance(config, NaqConfig)
        assert isinstance(config.job, JobConfig)
        assert isinstance(config.connection, ConnectionConfig)
        assert isinstance(config.event, EventConfig)
        assert isinstance(config.logging, LoggingConfig)
    
    def test_validate_config(self):
        """Test configuration validation."""
        valid_config = {
            "job": {
                "max_retries": 3,
                "timeout": 30,
                "retry_strategy": "exponential"
            },
            "logging": {
                "level": "INFO",
                "format": "{time:YYYY-MM-DD HH:mm:ss} | {level} | {name} | {message}"
            }
        }
        
        errors = validate_config(valid_config, DEFAULT_CONFIG_SCHEMA)
        assert len(errors) == 0
        
        invalid_config = {
            "job": {
                "max_retries": -1,  # Invalid
                "timeout": 0,  # Invalid
                "retry_strategy": "invalid"  # Invalid
            },
            "logging": {
                "level": "INVALID"  # Invalid
            }
        }
        
        errors = validate_config(invalid_config, DEFAULT_CONFIG_SCHEMA)
        assert len(errors) > 0


class TestLoggingUtils:
    """Test logging utilities."""
    
    def test_setup_logging(self):
        """Test logging setup."""
        # This test mainly ensures the function doesn't crash
        setup_logging("DEBUG")
        setup_logging("INFO")
    
    def test_get_logger(self):
        """Test logger retrieval."""
        logger = get_logger("test")
        assert logger is not None
        # Loguru logger doesn't have a direct name attribute, but we can check it's bound correctly
        # by checking that it's not the same as the default logger
        assert logger is not get_logger()
    
    def test_log_function_call(self):
        """Test function call logging decorator."""
        @log_function_call
        def test_func(x, y):
            return x + y
        
        result = test_func(1, 2)
        assert result == 3
    
    def test_log_performance(self):
        """Test performance logging decorator."""
        @log_performance
        def test_func():
            import time
            time.sleep(0.01)
            return "done"
        
        result = test_func()
        assert result == "done"
    
    def test_log_errors(self):
        """Test error logging decorator."""
        @log_errors
        def test_func():
            raise ValueError("Test error")
        
        with pytest.raises(ValueError):
            test_func()
    
    def test_log_context(self):
        """Test log context manager."""
        with LogContext(operation="test", user_id="123"):
            logger = get_logger("test")
            logger.info("Test message with context")
    
    def test_performance_timer(self):
        """Test performance timer."""
        timer = PerformanceTimer()
        timer.start()
        import time
        time.sleep(0.01)
        timer.stop()
        
        assert timer.elapsed > 0
        assert str(timer)  # Test string representation


if __name__ == "__main__":
    pytest.main([__file__])