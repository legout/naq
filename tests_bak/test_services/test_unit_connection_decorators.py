import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock

from naq.connection.decorators import with_nats_connection, with_jetstream_context
from naq.exceptions import NaqConnectionError
from naq.services.config import GlobalServiceConfig


class TestWithNatsConnection:
    """Test cases for the with_nats_connection decorator."""

    @pytest.mark.asyncio
    async def test_decorator_injects_connection(self):
        """Test that the decorator injects a NATS connection as the first argument."""
        # Mock the nats_connection context manager and its return value
        mock_nc = AsyncMock()
        mock_nc.publish = AsyncMock()
        
        with patch('naq.connection.decorators.nats_connection') as mock_context:
            mock_context.return_value.__aenter__.return_value = mock_nc
            mock_context.return_value.__aexit__.return_value = None
            
            # Create a decorated function
            @with_nats_connection()
            async def test_function(nc, subject, message):
                return await nc.publish(subject, message)
            
            # Call the decorated function
            result = await test_function("test.subject", b"test message")
            
            # Verify the connection was used
            mock_nc.publish.assert_called_once_with("test.subject", b"test message")
            # Verify the context manager was used
            mock_context.assert_called_once()

    @pytest.mark.asyncio
    async def test_decorator_with_custom_config(self):
        """Test that the decorator uses the provided configuration."""
        # Mock the nats_connection context manager and its return value
        mock_nc = AsyncMock()
        mock_nc.publish = AsyncMock()
        
        # Create a custom config
        custom_config = GlobalServiceConfig(nats_url="nats://custom:4222")
        
        with patch('naq.connection.decorators.nats_connection') as mock_context:
            mock_context.return_value.__aenter__.return_value = mock_nc
            mock_context.return_value.__aexit__.return_value = None
            
            # Create a decorated function with custom config
            @with_nats_connection(config=custom_config)
            async def test_function(nc, subject, message):
                return await nc.publish(subject, message)
            
            # Call the decorated function
            await test_function("test.subject", b"test message")
            
            # Verify the context manager was called with the custom config
            mock_context.assert_called_once_with(custom_config)

    @pytest.mark.asyncio
    async def test_decorator_closes_connection_on_success(self):
        """Test that the decorator properly closes the connection after successful execution."""
        # Mock the nats_connection context manager and its return value
        mock_nc = AsyncMock()
        mock_nc.publish = AsyncMock()
        
        with patch('naq.connection.decorators.nats_connection') as mock_context:
            mock_context.return_value.__aenter__.return_value = mock_nc
            mock_context.return_value.__aexit__.return_value = None
            
            # Create a decorated function
            @with_nats_connection()
            async def test_function(nc, subject, message):
                return await nc.publish(subject, message)
            
            # Call the decorated function
            await test_function("test.subject", b"test message")
            
            # Verify the context manager was properly used
            mock_context.return_value.__aenter__.assert_called_once()
            mock_context.return_value.__aexit__.assert_called_once()

    @pytest.mark.asyncio
    async def test_decorator_closes_connection_on_exception(self):
        """Test that the decorator properly closes the connection even when the decorated function raises an exception."""
        # Mock the nats_connection context manager and its return value
        mock_nc = AsyncMock()
        
        with patch('naq.connection.decorators.nats_connection') as mock_context:
            mock_context.return_value.__aenter__.return_value = mock_nc
            mock_context.return_value.__aexit__.return_value = None
            
            # Create a decorated function that raises an exception
            @with_nats_connection()
            async def test_function(nc):
                raise ValueError("Test exception")
            
            # Call the decorated function and expect an exception
            with pytest.raises(ValueError, match="Test exception"):
                await test_function()
            
            # Verify the context manager was properly used despite the exception
            mock_context.return_value.__aenter__.assert_called_once()
            mock_context.return_value.__aexit__.assert_called_once()

    @pytest.mark.asyncio
    async def test_decorator_preserves_function_metadata(self):
        """Test that the decorator preserves the original function's metadata."""
        with patch('naq.connection.decorators.nats_connection') as mock_context:
            mock_context.return_value.__aenter__.return_value = AsyncMock()
            mock_context.return_value.__aexit__.return_value = None
            
            # Create a decorated function with docstring and name
            @with_nats_connection()
            async def test_function(nc, arg1, arg2):
                """Test function docstring."""
                return f"{arg1}-{arg2}"
            
            # Verify metadata is preserved
            assert test_function.__name__ == "test_function"
            assert test_function.__doc__ == "Test function docstring."
            
            # Call the function to verify it works
            result = await test_function("arg1", "arg2")
            assert result == "arg1-arg2"

    @pytest.mark.asyncio
    async def test_decorator_handles_connection_error(self):
        """Test that the decorator properly handles NATS connection errors."""
        with patch('naq.connection.decorators.nats_connection') as mock_context:
            # Make the context manager raise a connection error
            mock_context.return_value.__aenter__.side_effect = NaqConnectionError("Connection failed")
            mock_context.return_value.__aexit__.return_value = None
            
            # Create a decorated function
            @with_nats_connection()
            async def test_function(nc):
                return "should not reach here"
            
            # Call the decorated function and expect a connection error
            with pytest.raises(NaqConnectionError, match="Connection failed"):
                await test_function()

    @pytest.mark.asyncio
    async def test_decorator_with_multiple_arguments(self):
        """Test that the decorator works with functions that have multiple arguments."""
        # Mock the nats_connection context manager and its return value
        mock_nc = AsyncMock()
        mock_nc.publish = AsyncMock()
        
        with patch('naq.connection.decorators.nats_connection') as mock_context:
            mock_context.return_value.__aenter__.return_value = mock_nc
            mock_context.return_value.__aexit__.return_value = None
            
            # Create a decorated function with multiple arguments
            @with_nats_connection()
            async def test_function(nc, subject, message, priority, timeout):
                await nc.publish(subject, message)
                return f"Published with priority {priority} and timeout {timeout}"
            
            # Call the decorated function with multiple arguments
            result = await test_function("test.subject", b"test message", 1, 5.0)
            
            # Verify the connection was used and the result is correct
            mock_nc.publish.assert_called_once_with("test.subject", b"test message")
            assert result == "Published with priority 1 and timeout 5.0"

    @pytest.mark.asyncio
    async def test_decorator_with_keyword_arguments(self):
        """Test that the decorator works with functions that use keyword arguments."""
        # Mock the nats_connection context manager and its return value
        mock_nc = AsyncMock()
        mock_nc.publish = AsyncMock()
        
        with patch('naq.connection.decorators.nats_connection') as mock_context:
            mock_context.return_value.__aenter__.return_value = mock_nc
            mock_context.return_value.__aexit__.return_value = None
            
            # Create a decorated function that uses keyword arguments
            @with_nats_connection()
            async def test_function(nc, subject, message, **kwargs):
                await nc.publish(subject, message)
                return f"Published with kwargs: {kwargs}"
            
            # Call the decorated function with keyword arguments
            result = await test_function("test.subject", b"test message", priority=1, timeout=5.0)
            
            # Verify the connection was used and the result is correct
            mock_nc.publish.assert_called_once_with("test.subject", b"test message")
            assert result == "Published with kwargs: {'priority': 1, 'timeout': 5.0}"

    @pytest.mark.asyncio
    async def test_decorator_return_value_propagation(self):
        """Test that the decorator properly propagates the return value from the decorated function."""
        # Mock the nats_connection context manager and its return value
        mock_nc = AsyncMock()
        
        with patch('naq.connection.decorators.nats_connection') as mock_context:
            mock_context.return_value.__aenter__.return_value = mock_nc
            mock_context.return_value.__aexit__.return_value = None
            
            # Create a decorated function that returns a specific value
            @with_nats_connection()
            async def test_function(nc, value):
                return f"processed-{value}"
            
            # Call the decorated function
            result = await test_function("test")
            
            # Verify the return value is correct
            assert result == "processed-test"

    @pytest.mark.asyncio
    async def test_decorator_exception_propagation(self):
        """Test that the decorator properly propagates exceptions from the decorated function."""
        # Mock the nats_connection context manager and its return value
        mock_nc = AsyncMock()
        
        with patch('naq.connection.decorators.nats_connection') as mock_context:
            mock_context.return_value.__aenter__.return_value = mock_nc
            mock_context.return_value.__aexit__.return_value = None
            
            # Create a decorated function that raises a specific exception
            @with_nats_connection()
            async def test_function(nc):
                raise RuntimeError("Function-specific error")
            
            # Call the decorated function and expect the specific exception
            with pytest.raises(RuntimeError, match="Function-specific error"):
                await test_function()


class TestWithJetStreamContext:
    """Test cases for the with_jetstream_context decorator."""

    @pytest.mark.asyncio
    async def test_decorator_injects_jetstream_context(self):
        """Test that the decorator injects a JetStream context as the first argument."""
        # Mock the nats_jetstream context manager and its return value
        mock_nc = AsyncMock()
        mock_js = AsyncMock()
        mock_js.add_stream = AsyncMock()
        
        with patch('naq.connection.decorators.nats_jetstream') as mock_context:
            mock_context.return_value.__aenter__.return_value = (mock_nc, mock_js)
            mock_context.return_value.__aexit__.return_value = None
            
            # Create a decorated function
            @with_jetstream_context()
            async def test_function(js, stream_name, subjects):
                return await js.add_stream(name=stream_name, subjects=subjects)
            
            # Call the decorated function
            result = await test_function("test_stream", ["test.subject"])
            
            # Verify the JetStream context was used
            mock_js.add_stream.assert_called_once_with(name="test_stream", subjects=["test.subject"])
            # Verify the context manager was used
            mock_context.assert_called_once()

    @pytest.mark.asyncio
    async def test_decorator_with_custom_config(self):
        """Test that the decorator uses the provided configuration."""
        # Mock the nats_jetstream context manager and its return value
        mock_nc = AsyncMock()
        mock_js = AsyncMock()
        mock_js.add_stream = AsyncMock()
        
        # Create a custom config
        custom_config = GlobalServiceConfig(nats_url="nats://custom:4222")
        
        with patch('naq.connection.decorators.nats_jetstream') as mock_context:
            mock_context.return_value.__aenter__.return_value = (mock_nc, mock_js)
            mock_context.return_value.__aexit__.return_value = None
            
            # Create a decorated function with custom config
            @with_jetstream_context(config=custom_config)
            async def test_function(js, stream_name, subjects):
                return await js.add_stream(name=stream_name, subjects=subjects)
            
            # Call the decorated function
            await test_function("test_stream", ["test.subject"])
            
            # Verify the context manager was called with the custom config
            mock_context.assert_called_once_with(custom_config)

    @pytest.mark.asyncio
    async def test_decorator_closes_connection_on_success(self):
        """Test that the decorator properly closes the connection after successful execution."""
        # Mock the nats_jetstream context manager and its return value
        mock_nc = AsyncMock()
        mock_js = AsyncMock()
        mock_js.add_stream = AsyncMock()
        
        with patch('naq.connection.decorators.nats_jetstream') as mock_context:
            mock_context.return_value.__aenter__.return_value = (mock_nc, mock_js)
            mock_context.return_value.__aexit__.return_value = None
            
            # Create a decorated function
            @with_jetstream_context()
            async def test_function(js, stream_name, subjects):
                return await js.add_stream(name=stream_name, subjects=subjects)
            
            # Call the decorated function
            await test_function("test_stream", ["test.subject"])
            
            # Verify the context manager was properly used
            mock_context.return_value.__aenter__.assert_called_once()
            mock_context.return_value.__aexit__.assert_called_once()

    @pytest.mark.asyncio
    async def test_decorator_closes_connection_on_exception(self):
        """Test that the decorator properly closes the connection even when the decorated function raises an exception."""
        # Mock the nats_jetstream context manager and its return value
        mock_nc = AsyncMock()
        mock_js = AsyncMock()
        
        with patch('naq.connection.decorators.nats_jetstream') as mock_context:
            mock_context.return_value.__aenter__.return_value = (mock_nc, mock_js)
            mock_context.return_value.__aexit__.return_value = None
            
            # Create a decorated function that raises an exception
            @with_jetstream_context()
            async def test_function(js):
                raise ValueError("Test exception")
            
            # Call the decorated function and expect an exception
            with pytest.raises(ValueError, match="Test exception"):
                await test_function()
            
            # Verify the context manager was properly used despite the exception
            mock_context.return_value.__aenter__.assert_called_once()
            mock_context.return_value.__aexit__.assert_called_once()

    @pytest.mark.asyncio
    async def test_decorator_preserves_function_metadata(self):
        """Test that the decorator preserves the original function's metadata."""
        with patch('naq.connection.decorators.nats_jetstream') as mock_context:
            mock_context.return_value.__aenter__.return_value = (AsyncMock(), AsyncMock())
            mock_context.return_value.__aexit__.return_value = None
            
            # Create a decorated function with docstring and name
            @with_jetstream_context()
            async def test_function(js, arg1, arg2):
                """Test function docstring."""
                return f"{arg1}-{arg2}"
            
            # Verify metadata is preserved
            assert test_function.__name__ == "test_function"
            assert test_function.__doc__ == "Test function docstring."
            
            # Call the function to verify it works
            result = await test_function("arg1", "arg2")
            assert result == "arg1-arg2"

    @pytest.mark.asyncio
    async def test_decorator_handles_connection_error(self):
        """Test that the decorator properly handles NATS connection errors."""
        with patch('naq.connection.decorators.nats_jetstream') as mock_context:
            # Make the context manager raise a connection error
            mock_context.return_value.__aenter__.side_effect = NaqConnectionError("Connection failed")
            mock_context.return_value.__aexit__.return_value = None
            
            # Create a decorated function
            @with_jetstream_context()
            async def test_function(js):
                return "should not reach here"
            
            # Call the decorated function and expect a connection error
            with pytest.raises(NaqConnectionError, match="Connection failed"):
                await test_function()

    @pytest.mark.asyncio
    async def test_decorator_with_multiple_arguments(self):
        """Test that the decorator works with functions that have multiple arguments."""
        # Mock the nats_jetstream context manager and its return value
        mock_nc = AsyncMock()
        mock_js = AsyncMock()
        mock_js.add_stream = AsyncMock()
        
        with patch('naq.connection.decorators.nats_jetstream') as mock_context:
            mock_context.return_value.__aenter__.return_value = (mock_nc, mock_js)
            mock_context.return_value.__aexit__.return_value = None
            
            # Create a decorated function with multiple arguments
            @with_jetstream_context()
            async def test_function(js, stream_name, subjects, retention, replicas):
                await js.add_stream(name=stream_name, subjects=subjects)
                return f"Created stream with retention {retention} and {replicas} replicas"
            
            # Call the decorated function with multiple arguments
            result = await test_function("test_stream", ["test.subject"], "limits", 3)
            
            # Verify the JetStream context was used and the result is correct
            mock_js.add_stream.assert_called_once_with(name="test_stream", subjects=["test.subject"])
            assert result == "Created stream with retention limits and 3 replicas"

    @pytest.mark.asyncio
    async def test_decorator_with_keyword_arguments(self):
        """Test that the decorator works with functions that use keyword arguments."""
        # Mock the nats_jetstream context manager and its return value
        mock_nc = AsyncMock()
        mock_js = AsyncMock()
        mock_js.add_stream = AsyncMock()
        
        with patch('naq.connection.decorators.nats_jetstream') as mock_context:
            mock_context.return_value.__aenter__.return_value = (mock_nc, mock_js)
            mock_context.return_value.__aexit__.return_value = None
            
            # Create a decorated function that uses keyword arguments
            @with_jetstream_context()
            async def test_function(js, stream_name, subjects, **kwargs):
                await js.add_stream(name=stream_name, subjects=subjects)
                return f"Created stream with kwargs: {kwargs}"
            
            # Call the decorated function with keyword arguments
            result = await test_function("test_stream", ["test.subject"], retention="limits", replicas=3)
            
            # Verify the JetStream context was used and the result is correct
            mock_js.add_stream.assert_called_once_with(name="test_stream", subjects=["test.subject"])
            assert result == "Created stream with kwargs: {'retention': 'limits', 'replicas': 3}"

    @pytest.mark.asyncio
    async def test_decorator_return_value_propagation(self):
        """Test that the decorator properly propagates the return value from the decorated function."""
        # Mock the nats_jetstream context manager and its return value
        mock_nc = AsyncMock()
        mock_js = AsyncMock()
        
        with patch('naq.connection.decorators.nats_jetstream') as mock_context:
            mock_context.return_value.__aenter__.return_value = (mock_nc, mock_js)
            mock_context.return_value.__aexit__.return_value = None
            
            # Create a decorated function that returns a specific value
            @with_jetstream_context()
            async def test_function(js, value):
                return f"processed-{value}"
            
            # Call the decorated function
            result = await test_function("test")
            
            # Verify the return value is correct
            assert result == "processed-test"

    @pytest.mark.asyncio
    async def test_decorator_exception_propagation(self):
        """Test that the decorator properly propagates exceptions from the decorated function."""
        # Mock the nats_jetstream context manager and its return value
        mock_nc = AsyncMock()
        mock_js = AsyncMock()
        
        with patch('naq.connection.decorators.nats_jetstream') as mock_context:
            mock_context.return_value.__aenter__.return_value = (mock_nc, mock_js)
            mock_context.return_value.__aexit__.return_value = None
            
            # Create a decorated function that raises a specific exception
            @with_jetstream_context()
            async def test_function(js):
                raise RuntimeError("Function-specific error")
            
            # Call the decorated function and expect the specific exception
            with pytest.raises(RuntimeError, match="Function-specific error"):
                await test_function()

    @pytest.mark.asyncio
    async def test_decorator_jetstream_operations(self):
        """Test that the decorator allows successful JetStream operations."""
        # Mock the nats_jetstream context manager and its return value
        mock_nc = AsyncMock()
        mock_js = AsyncMock()
        mock_js.add_stream = AsyncMock()
        mock_js.publish = AsyncMock()
        
        with patch('naq.connection.decorators.nats_jetstream') as mock_context:
            mock_context.return_value.__aenter__.return_value = (mock_nc, mock_js)
            mock_context.return_value.__aexit__.return_value = None
            
            # Create a decorated function that performs JetStream operations
            @with_jetstream_context()
            async def test_function(js, stream_name, subject, message):
                # Create a stream
                await js.add_stream(name=stream_name, subjects=[subject])
                # Publish a message
                await js.publish(subject, message)
                return f"Stream {stream_name} created and message published"
            
            # Call the decorated function
            result = await test_function("test_stream", "test.subject", b"test message")
            
            # Verify the JetStream operations were called
            mock_js.add_stream.assert_called_once_with(name="test_stream", subjects=["test.subject"])
            mock_js.publish.assert_called_once_with("test.subject", b"test message")
            assert result == "Stream test_stream created and message published"