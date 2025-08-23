"""Unit tests for the timing utility functions."""

import time
import pytest
from unittest.mock import Mock

from naq.utils.timing import Stopwatch, measure_execution_time, measure_execution_time_cm


class TestStopwatch:
    """Test cases for the Stopwatch class."""

    def test_stopwatch_initialization(self):
        """Test Stopwatch initialization."""
        stopwatch = Stopwatch()
        assert stopwatch._start_time is None
        assert stopwatch._end_time is None
        assert stopwatch._elapsed_time is None

    def test_stopwatch_start(self):
        """Test Stopwatch start method."""
        stopwatch = Stopwatch()
        stopwatch.start()
        assert stopwatch._start_time is not None
        assert stopwatch._end_time is None
        assert stopwatch._elapsed_time is None

    def test_stopwatch_stop(self):
        """Test Stopwatch stop method."""
        stopwatch = Stopwatch()
        stopwatch.start()
        time.sleep(0.01)  # Small delay to ensure measurable time
        stopwatch.stop()
        assert stopwatch._end_time is not None
        assert stopwatch._elapsed_time is not None
        assert stopwatch._elapsed_time > 0

    def test_stopwatch_stop_without_start(self):
        """Test Stopwatch stop method without calling start first."""
        stopwatch = Stopwatch()
        with pytest.raises(ValueError, match="Stopwatch has not been started"):
            stopwatch.stop()

    def test_stopwatch_elapsed_when_stopped(self):
        """Test Stopwatch elapsed method when stopwatch is stopped."""
        stopwatch = Stopwatch()
        stopwatch.start()
        time.sleep(0.01)  # Small delay to ensure measurable time
        stopwatch.stop()
        elapsed = stopwatch.elapsed()
        assert elapsed > 0
        assert elapsed == stopwatch._elapsed_time

    def test_stopwatch_elapsed_when_running(self):
        """Test Stopwatch elapsed method when stopwatch is still running."""
        stopwatch = Stopwatch()
        stopwatch.start()
        time.sleep(0.01)  # Small delay to ensure measurable time
        elapsed = stopwatch.elapsed()
        assert elapsed > 0
        assert stopwatch._elapsed_time is None  # Should still be None when running

    def test_stopwatch_elapsed_without_start(self):
        """Test Stopwatch elapsed method without calling start first."""
        stopwatch = Stopwatch()
        with pytest.raises(ValueError, match="Stopwatch has not been started"):
            stopwatch.elapsed()

    def test_stopwatch_reset(self):
        """Test Stopwatch reset method."""
        stopwatch = Stopwatch()
        stopwatch.start()
        time.sleep(0.01)  # Small delay to ensure measurable time
        stopwatch.stop()
        stopwatch.reset()
        assert stopwatch._start_time is None
        assert stopwatch._end_time is None
        assert stopwatch._elapsed_time is None

    def test_stopwatch_context_manager(self):
        """Test Stopwatch as a context manager."""
        with Stopwatch() as stopwatch:
            time.sleep(0.01)  # Small delay to ensure measurable time
        
        assert stopwatch._start_time is not None
        assert stopwatch._end_time is not None
        assert stopwatch._elapsed_time is not None
        assert stopwatch._elapsed_time > 0

    def test_stopwatch_multiple_measurements(self):
        """Test Stopwatch with multiple start/stop cycles."""
        stopwatch = Stopwatch()
        
        # First measurement
        stopwatch.start()
        time.sleep(0.01)
        stopwatch.stop()
        first_elapsed = stopwatch.elapsed()
        
        # Second measurement
        stopwatch.start()
        time.sleep(0.02)
        stopwatch.stop()
        second_elapsed = stopwatch.elapsed()
        
        assert first_elapsed > 0
        assert second_elapsed > 0
        assert second_elapsed > first_elapsed  # Second measurement should be longer


class TestMeasureExecutionTimeDecorator:
    """Test cases for the measure_execution_time decorator."""

    def test_measure_execution_time_decorator_without_logger(self):
        """Test measure_execution_time decorator without logger."""
        @measure_execution_time
        def test_function():
            time.sleep(0.01)
            return "test_result"
        
        result = test_function()
        assert result == "test_result"
        assert hasattr(test_function, '_execution_times')
        assert len(test_function._execution_times) == 1
        assert test_function._execution_times[0] > 0

    def test_measure_execution_time_decorator_with_logger(self):
        """Test measure_execution_time decorator with logger."""
        mock_logger = Mock()
        
        @measure_execution_time(logger=mock_logger)
        def test_function():
            time.sleep(0.01)
            return "test_result"
        
        result = test_function()
        assert result == "test_result"
        mock_logger.assert_called_once()
        assert "Function 'test_function' executed in" in mock_logger.call_args[0][0]

    def test_measure_execution_time_decorator_with_parentheses(self):
        """Test measure_execution_time decorator used with parentheses but no logger."""
        @measure_execution_time()
        def test_function():
            time.sleep(0.01)
            return "test_result"
        
        result = test_function()
        assert result == "test_result"
        assert hasattr(test_function, '_execution_times')
        assert len(test_function._execution_times) == 1
        assert test_function._execution_times[0] > 0

    def test_measure_execution_time_decorator_multiple_calls(self):
        """Test measure_execution_time decorator with multiple function calls."""
        @measure_execution_time
        def test_function():
            time.sleep(0.01)
            return "test_result"
        
        # Call the function multiple times
        for _ in range(3):
            test_function()
        
        assert hasattr(test_function, '_execution_times')
        assert len(test_function._execution_times) == 3
        for execution_time in test_function._execution_times:
            assert execution_time > 0

    def test_measure_execution_time_decorator_with_exception(self):
        """Test measure_execution_time decorator when function raises an exception."""
        @measure_execution_time
        def test_function():
            time.sleep(0.01)
            raise ValueError("Test exception")
        
        with pytest.raises(ValueError, match="Test exception"):
            test_function()
        
        # Execution time should still be recorded
        assert hasattr(test_function, '_execution_times')
        assert len(test_function._execution_times) == 1
        assert test_function._execution_times[0] > 0

    def test_measure_execution_time_decorator_preserves_function_metadata(self):
        """Test measure_execution_time decorator preserves function metadata."""
        @measure_execution_time
        def test_function():
            """Test function docstring."""
            time.sleep(0.01)
            return "test_result"
        
        assert test_function.__name__ == "test_function"
        assert test_function.__doc__ == "Test function docstring."


class TestMeasureExecutionTimeContextManager:
    """Test cases for the measure_execution_time_cm context manager."""

    def test_measure_execution_time_cm_without_name_and_logger(self):
        """Test measure_execution_time_cm without name and logger."""
        with measure_execution_time_cm():
            time.sleep(0.01)
        
        # No assertions needed, just verify it doesn't raise an exception

    def test_measure_execution_time_cm_with_name_and_logger(self):
        """Test measure_execution_time_cm with name and logger."""
        mock_logger = Mock()
        
        with measure_execution_time_cm("test_block", logger=mock_logger):
            time.sleep(0.01)
        
        mock_logger.assert_called_once()
        assert "Code block 'test_block' executed in" in mock_logger.call_args[0][0]

    def test_measure_execution_time_cm_with_name_only(self):
        """Test measure_execution_time_cm with name but no logger."""
        with measure_execution_time_cm("test_block"):
            time.sleep(0.01)
        
        # No assertions needed, just verify it doesn't raise an exception

    def test_measure_execution_time_cm_with_logger_only(self):
        """Test measure_execution_time_cm with logger but no name."""
        mock_logger = Mock()
        
        with measure_execution_time_cm(logger=mock_logger):
            time.sleep(0.01)
        
        mock_logger.assert_called_once()
        assert "Code block executed in" in mock_logger.call_args[0][0]

    def test_measure_execution_time_cm_with_exception(self):
        """Test measure_execution_time_cm when code block raises an exception."""
        mock_logger = Mock()
        
        with pytest.raises(ValueError, match="Test exception"):
            with measure_execution_time_cm("test_block", logger=mock_logger):
                time.sleep(0.01)
                raise ValueError("Test exception")
        
        # Logger should still be called even when an exception occurs
        mock_logger.assert_called_once()
        assert "Code block 'test_block' executed in" in mock_logger.call_args[0][0]

    def test_measure_execution_time_cm_nested(self):
        """Test measure_execution_time_cm with nested context managers."""
        mock_logger = Mock()
        
        with measure_execution_time_cm("outer", logger=mock_logger):
            time.sleep(0.01)
            with measure_execution_time_cm("inner", logger=mock_logger):
                time.sleep(0.01)
        
        # Logger should be called twice
        assert mock_logger.call_count == 2
        # The inner context manager exits first, so its log comes first
        inner_call = mock_logger.call_args_list[0][0][0]
        outer_call = mock_logger.call_args_list[1][0][0]
        assert "Code block 'outer' executed in" in outer_call
        assert "Code block 'inner' executed in" in inner_call