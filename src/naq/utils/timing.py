"""Timing utilities for NAQ.

This module contains common timing utilities used throughout the NAQ codebase.
"""

import time
import functools
from contextlib import contextmanager
from typing import Callable, TypeVar, Any, Optional, Generator

# Type variable for generic function return types
T = TypeVar('T')


class Stopwatch:
    """A simple stopwatch for measuring elapsed time.
    
    This class provides a straightforward way to measure time intervals
    with start, stop, and elapsed functionality.
    
    Example:
        >>> stopwatch = Stopwatch()
        >>> stopwatch.start()
        >>> # Do some work
        >>> stopwatch.stop()
        >>> print(f"Elapsed time: {stopwatch.elapsed()} seconds")
    """
    
    def __init__(self) -> None:
        """Initialize a new stopwatch."""
        self._start_time: Optional[float] = None
        self._end_time: Optional[float] = None
        self._elapsed_time: Optional[float] = None
    
    def start(self) -> None:
        """Start the stopwatch.
        
        Resets any previous timing and begins a new measurement.
        """
        self._start_time = time.perf_counter()
        self._end_time = None
        self._elapsed_time = None
    
    def stop(self) -> None:
        """Stop the stopwatch.
        
        Stops the timing and calculates the elapsed time.
        
        Raises:
            ValueError: If the stopwatch has not been started.
        """
        if self._start_time is None:
            raise ValueError("Stopwatch has not been started")
        
        self._end_time = time.perf_counter()
        self._elapsed_time = self._end_time - self._start_time
    
    def elapsed(self) -> float:
        """Get the elapsed time in seconds.
        
        Returns:
            float: The elapsed time in seconds. If the stopwatch is still
                  running, returns the time from start to now. If stopped,
                  returns the time from start to stop.
                  
        Raises:
            ValueError: If the stopwatch has not been started.
        """
        if self._start_time is None:
            raise ValueError("Stopwatch has not been started")
        
        if self._elapsed_time is not None:
            return self._elapsed_time
        
        # If stopwatch is still running, calculate current elapsed time
        return time.perf_counter() - self._start_time
    
    def reset(self) -> None:
        """Reset the stopwatch.
        
        Clears all timing information, returning the stopwatch to its initial state.
        """
        self._start_time = None
        self._end_time = None
        self._elapsed_time = None
    
    def __enter__(self) -> 'Stopwatch':
        """Enter the context manager, starting the stopwatch."""
        self.start()
        return self
    
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit the context manager, stopping the stopwatch."""
        self.stop()


def measure_execution_time(func: Optional[Callable[..., T]] = None, *,
                          logger: Optional[Callable[[str], None]] = None) -> Callable[..., T]:
    """Decorator to measure the execution time of a function.
    
    This decorator can be used with or without parentheses. When used with
    parentheses, it accepts an optional logger function to log the execution time.
    
    Args:
        func: The function to be decorated. When used with parentheses,
              this will be None and the decorator will return a new decorator.
        logger: Optional logger function to log the execution time.
                If provided, the execution time will be logged using this function.
    
    Returns:
        The decorated function or a new decorator if used with parentheses.
    
    Example:
        >>> @measure_execution_time
        ... def slow_function():
        ...     time.sleep(1)
        ...     return "done"
        >>>
        >>> @measure_execution_time(logger=print)
        ... def another_slow_function():
        ...     time.sleep(0.5)
        ...     return "also done"
    """
    def decorator(f: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(f)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            stopwatch = Stopwatch()
            stopwatch.start()
            
            try:
                result = f(*args, **kwargs)
                return result
            finally:
                stopwatch.stop()
                execution_time = stopwatch.elapsed()
                
                if logger:
                    logger(f"Function '{f.__name__}' executed in {execution_time:.6f} seconds")
                
                # Store execution time as an attribute of the function
                if not hasattr(wrapper, '_execution_times'):
                    wrapper._execution_times = []  # type: ignore
                wrapper._execution_times.append(execution_time)  # type: ignore
        
        return wrapper
    
    # Handle both @measure_execution_time and @measure_execution_time() syntax
    if func is None:
        return decorator
    else:
        return decorator(func)


@contextmanager
def measure_execution_time_cm(name: Optional[str] = None,
                             logger: Optional[Callable[[str], None]] = None) -> Generator[None, None, None]:
    """Context manager to measure execution time of a code block.
    
    Args:
        name: Optional name for the code block being measured. If provided,
              this name will be included in any log messages.
        logger: Optional logger function to log the execution time.
                If provided, the execution time will be logged using this function.
    
    Yields:
        None: This context manager doesn't yield any value.
    
    Example:
        >>> with measure_execution_time_cm("data processing", logger=print):
        ...     # Process some data
        ...     time.sleep(0.5)
        >>>
        >>> # Without a name or logger
        >>> with measure_execution_time_cm():
        ...     # Do some work
        ...     pass
    """
    stopwatch = Stopwatch()
    stopwatch.start()
    
    try:
        yield
    finally:
        stopwatch.stop()
        execution_time = stopwatch.elapsed()
        
        if logger:
            block_name = f" '{name}'" if name else ""
            logger(f"Code block{block_name} executed in {execution_time:.6f} seconds")