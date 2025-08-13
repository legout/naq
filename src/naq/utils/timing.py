"""
Timing utilities for NAQ.

This module provides comprehensive timing utilities for performance measurement,
timeout management, scheduling, and benchmarking. It supports both synchronous
and asynchronous operations with high-resolution timing capabilities.

The module includes:
- High-resolution timing functions for measuring execution time
- Both synchronous and asynchronous timing support
- Statistical timing analysis (min, max, average, percentiles)
- Timing aggregation and reporting
- Timeout utilities for operations with configurable durations
- Both sync and async timeout context managers
- Timeout exception handling and recovery
- Integration with existing retry mechanisms
- Delayed execution utilities
- Periodic task scheduling
- Cron-like scheduling capabilities
- Task cancellation and management
- Benchmarking utilities for performance testing
- Memory usage tracking during timing
- Comparative benchmarking between different implementations
- Benchmark result reporting and analysis
- Context managers for automatic timing measurement
- Nested timing support with proper hierarchy
- Timing context propagation across async boundaries
- Integration with existing logging utilities
"""

import asyncio
import contextlib
import functools
import inspect
import statistics
import time
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import (
    Any, AsyncIterator, Callable, Dict, Iterator, List, Optional, Tuple, Type, Union,
    TypeVar, Awaitable, Generic, AsyncContextManager, ContextManager
)

from ..exceptions import NaqException
from .logging import get_logger, StructuredLogger

# Type variables for generic timing utilities
T = TypeVar("T")
R = TypeVar("R")


# =============================================================================
# Exceptions
# =============================================================================

class TimeoutError(NaqException):
    """Raised when an operation times out."""
    
    def __init__(self, message: str, timeout_seconds: float, operation: str = None):
        super().__init__(message)
        self.timeout_seconds = timeout_seconds
        self.operation = operation


class TimingError(NaqException):
    """Raised when there's an error with timing operations."""
    pass


class SchedulingError(NaqException):
    """Raised when there's an error with scheduling operations."""
    pass


class BenchmarkError(NaqException):
    """Raised when there's an error with benchmarking operations."""
    pass


# =============================================================================
# Performance Timing Utilities
# =============================================================================

@dataclass
class TimingResult:
    """Result of a timing measurement."""
    
    operation: str
    duration: float
    start_time: float
    end_time: float
    success: bool = True
    error: Optional[Exception] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __str__(self) -> str:
        status = "completed" if self.success else "failed"
        return f"{self.operation} {status} in {self.duration:.6f}s"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "operation": self.operation,
            "duration": self.duration,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "success": self.success,
            "error": str(self.error) if self.error else None,
            "metadata": self.metadata
        }


@dataclass
class TimingStats:
    """Statistical analysis of timing results."""
    
    results: List[TimingResult] = field(default_factory=list)
    
    @property
    def count(self) -> int:
        """Number of timing results."""
        return len(self.results)
    
    @property
    def durations(self) -> List[float]:
        """List of all durations."""
        return [r.duration for r in self.results]
    
    @property
    def successful_durations(self) -> List[float]:
        """List of successful durations."""
        return [r.duration for r in self.results if r.success]
    
    @property
    def min_duration(self) -> Optional[float]:
        """Minimum duration."""
        durations = self.successful_durations
        return min(durations) if durations else None
    
    @property
    def max_duration(self) -> Optional[float]:
        """Maximum duration."""
        durations = self.successful_durations
        return max(durations) if durations else None
    
    @property
    def avg_duration(self) -> Optional[float]:
        """Average duration."""
        durations = self.successful_durations
        return statistics.mean(durations) if durations else None
    
    @property
    def median_duration(self) -> Optional[float]:
        """Median duration."""
        durations = self.successful_durations
        return statistics.median(durations) if durations else None
    
    @property
    def stdev_duration(self) -> Optional[float]:
        """Standard deviation of durations."""
        durations = self.successful_durations
        return statistics.stdev(durations) if len(durations) > 1 else None
    
    @property
    def success_rate(self) -> float:
        """Success rate (0.0 to 1.0)."""
        if not self.results:
            return 0.0
        return len(self.successful_durations) / len(self.results)
    
    def percentile(self, p: float) -> Optional[float]:
        """
        Calculate the p-th percentile of durations.
        
        Args:
            p: Percentile (0.0 to 100.0)
            
        Returns:
            Percentile value or None if no data
        """
        durations = self.successful_durations
        if not durations:
            return None
        return statistics.quantiles(durations, n=100)[int(p) - 1] if p <= 100 else max(durations)
    
    def summary(self) -> Dict[str, Any]:
        """Get a summary of timing statistics."""
        return {
            "count": self.count,
            "success_count": len(self.successful_durations),
            "success_rate": self.success_rate,
            "min_duration": self.min_duration,
            "max_duration": self.max_duration,
            "avg_duration": self.avg_duration,
            "median_duration": self.median_duration,
            "stdev_duration": self.stdev_duration,
            "percentiles": {
                "p50": self.percentile(50),
                "p90": self.percentile(90),
                "p95": self.percentile(95),
                "p99": self.percentile(99),
            }
        }


class Timer:
    """
    High-resolution timer for measuring execution time.
    
    This timer provides both manual and context manager interfaces
    for timing operations with high precision.
    """
    
    def __init__(self, name: str = None, auto_start: bool = False):
        """
        Initialize the timer.
        
        Args:
            name: Optional name for the timer
            auto_start: Whether to start the timer immediately
        """
        self.name = name or "Timer"
        self._start_time: Optional[float] = None
        self._end_time: Optional[float] = None
        self._results: List[TimingResult] = []
        
        if auto_start:
            self.start()
    
    def start(self) -> None:
        """Start the timer."""
        self._start_time = time.perf_counter()
        self._end_time = None
    
    def stop(self, operation: str = None) -> TimingResult:
        """
        Stop the timer and return the result.
        
        Args:
            operation: Optional operation name (uses timer name if not provided)
            
        Returns:
            TimingResult with the measurement
        """
        if self._start_time is None:
            raise TimingError("Timer not started")
        
        self._end_time = time.perf_counter()
        duration = self._end_time - self._start_time
        
        op_name = operation or self.name
        result = TimingResult(
            operation=op_name,
            duration=duration,
            start_time=self._start_time,
            end_time=self._end_time
        )
        
        self._results.append(result)
        return result
    
    def reset(self) -> None:
        """Reset the timer."""
        self._start_time = None
        self._end_time = None
    
    @property
    def elapsed(self) -> Optional[float]:
        """Get elapsed time if timer is running."""
        if self._start_time is None:
            return None
        end_time = self._end_time or time.perf_counter()
        return end_time - self._start_time
    
    @property
    def is_running(self) -> bool:
        """Check if timer is running."""
        return self._start_time is not None and self._end_time is None
    
    @property
    def results(self) -> List[TimingResult]:
        """Get all timing results."""
        return self._results.copy()
    
    def get_stats(self) -> TimingStats:
        """Get statistics for all timing results."""
        return TimingStats(self._results)
    
    def __enter__(self) -> 'Timer':
        """Context manager entry."""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        if exc_type is None:
            self.stop()
        else:
            # Record failed timing
            if self._start_time is not None:
                self._end_time = time.perf_counter()
                duration = self._end_time - self._start_time
                
                result = TimingResult(
                    operation=self.name,
                    duration=duration,
                    start_time=self._start_time,
                    end_time=self._end_time,
                    success=False,
                    error=exc_val
                )
                
                self._results.append(result)
    
    def __str__(self) -> str:
        """String representation."""
        if self.is_running:
            return f"{self.name} (running: {self.elapsed:.6f}s)"
        elif self._results:
            last_result = self._results[-1]
            return f"{self.name}: {last_result.duration:.6f}s"
        else:
            return f"{self.name} (not started)"


class AsyncTimer(Timer):
    """
    Asynchronous version of the Timer class.
    
    This timer provides the same functionality as Timer but with
    async context manager support.
    """
    
    async def __aenter__(self) -> 'AsyncTimer':
        """Async context manager entry."""
        self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        if exc_type is None:
            self.stop()
        else:
            # Record failed timing
            if self._start_time is not None:
                self._end_time = time.perf_counter()
                duration = self._end_time - self._start_time
                
                result = TimingResult(
                    operation=self.name,
                    duration=duration,
                    start_time=self._start_time,
                    end_time=self._end_time,
                    success=False,
                    error=exc_val
                )
                
                self._results.append(result)


def time_function(func: Callable = None, *, name: str = None, logger: StructuredLogger = None):
    """
    Decorator to time function execution.
    
    Args:
        func: Function to decorate
        name: Optional name for the timing (uses function name if not provided)
        logger: Optional logger for timing results
        
    Returns:
        Decorated function
    """
    def decorator(f: Callable) -> Callable:
        timer = Timer(name or f.__name__)
        
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            timer.start()
            try:
                result = f(*args, **kwargs)
                timing_result = timer.stop()
                if logger:
                    logger.info(f"Function {timing_result.operation} completed in {timing_result.duration:.6f}s")
                return result
            except Exception as e:
                if timer.is_running:
                    timer.stop()
                raise
        
        return wrapper
    
    if func is None:
        return decorator
    return decorator(func)


def time_async_function(func: Callable = None, *, name: str = None, logger: StructuredLogger = None):
    """
    Decorator to time async function execution.
    
    Args:
        func: Async function to decorate
        name: Optional name for the timing (uses function name if not provided)
        logger: Optional logger for timing results
        
    Returns:
        Decorated async function
    """
    def decorator(f: Callable) -> Callable:
        timer = AsyncTimer(name or f.__name__)
        
        @functools.wraps(f)
        async def wrapper(*args, **kwargs):
            timer.start()
            try:
                result = await f(*args, **kwargs)
                timing_result = timer.stop()
                if logger:
                    logger.info(f"Async function {timing_result.operation} completed in {timing_result.duration:.6f}s")
                return result
            except Exception as e:
                if timer.is_running:
                    timer.stop()
                raise
        
        return wrapper
    
    if func is None:
        return decorator
    return decorator(func)


# =============================================================================
# Timeout Management
# =============================================================================

@asynccontextmanager
async def timeout_context(seconds: float, operation: str = None) -> AsyncIterator[None]:
    """
    Async context manager for timeout operations.
    
    Args:
        seconds: Timeout in seconds
        operation: Optional operation name for error messages
        
    Yields:
        None
        
    Raises:
        TimeoutError: If the operation times out
    """
    logger = get_logger(__name__)
    
    try:
        async with asyncio.timeout(seconds):
            yield
            
    except asyncio.TimeoutError:
        op_name = operation or "Operation"
        logger.warning(f"{op_name} timed out after {seconds} seconds")
        raise TimeoutError(f"{op_name} timed out after {seconds} seconds", seconds, operation)


@contextmanager
def timeout_context_sync(seconds: float, operation: str = None) -> Iterator[None]:
    """
    Synchronous context manager for timeout operations.
    
    Args:
        seconds: Timeout in seconds
        operation: Optional operation name for error messages
        
    Yields:
        None
        
    Raises:
        TimeoutError: If the operation times out
    """
    logger = get_logger(__name__)
    start_time = time.time()
    
    try:
        yield
        
    finally:
        elapsed = time.time() - start_time
        if elapsed > seconds:
            op_name = operation or "Operation"
            logger.warning(f"{op_name} exceeded timeout of {seconds} seconds (took {elapsed:.2f}s)")
            raise TimeoutError(f"{op_name} timed out after {seconds} seconds", seconds, operation)


async def wait_with_timeout(coro: Awaitable[T], timeout: float, operation: str = None) -> T:
    """
    Wait for a coroutine to complete with a timeout.
    
    Args:
        coro: Coroutine to wait for
        timeout: Timeout in seconds
        operation: Optional operation name for error messages
        
    Returns:
        Result of the coroutine
        
    Raises:
        TimeoutError: If the coroutine times out
    """
    try:
        async with timeout_context(timeout, operation):
            return await coro
    except asyncio.TimeoutError:
        op_name = operation or "Operation"
        raise TimeoutError(f"{op_name} timed out after {timeout} seconds", timeout, operation)


def run_with_timeout(func: Callable[..., T], timeout: float, *args, operation: str = None, **kwargs) -> T:
    """
    Run a function with a timeout.
    
    Args:
        func: Function to run
        timeout: Timeout in seconds
        *args: Positional arguments to pass to the function
        operation: Optional operation name for error messages
        **kwargs: Keyword arguments to pass to the function
        
    Returns:
        Result of the function
        
    Raises:
        TimeoutError: If the function times out
    """
    import threading
    
    result = None
    exception = None
    
    def worker():
        nonlocal result, exception
        try:
            result = func(*args, **kwargs)
        except Exception as e:
            exception = e
    
    thread = threading.Thread(target=worker)
    thread.start()
    thread.join(timeout=timeout)
    
    if thread.is_alive():
        op_name = operation or "Operation"
        raise TimeoutError(f"{op_name} timed out after {timeout} seconds", timeout, operation)
    
    if exception is not None:
        raise exception
    
    return result


# =============================================================================
# Scheduling Utilities
# =============================================================================

class Scheduler:
    """
    Task scheduler for delayed and periodic execution.
    
    This scheduler supports both synchronous and asynchronous tasks
    with flexible scheduling options.
    """
    
    def __init__(self, logger: StructuredLogger = None):
        """
        Initialize the scheduler.
        
        Args:
            logger: Optional logger instance
        """
        self.logger = logger or get_logger(__name__)
        self._tasks: Dict[str, asyncio.Task] = {}
        self._running = False
    
    async def start(self) -> None:
        """Start the scheduler."""
        if self._running:
            return
        
        self._running = True
        self.logger.info("Scheduler started")
    
    async def stop(self) -> None:
        """Stop the scheduler and cancel all tasks."""
        if not self._running:
            return
        
        self._running = False
        
        # Cancel all tasks
        for task_id, task in self._tasks.items():
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        self._tasks.clear()
        self.logger.info("Scheduler stopped")
    
    async def schedule_delayed(
        self,
        func: Union[Callable[..., T], Callable[..., Awaitable[T]]],
        delay: float,
        *args,
        task_id: str = None,
        **kwargs
    ) -> str:
        """
        Schedule a function to run after a delay.
        
        Args:
            func: Function to schedule (sync or async)
            delay: Delay in seconds
            *args: Positional arguments to pass to the function
            task_id: Optional task ID (generated if not provided)
            **kwargs: Keyword arguments to pass to the function
            
        Returns:
            Task ID
        """
        if not self._running:
            raise SchedulingError("Scheduler is not running")
        
        if task_id is None:
            task_id = f"delayed_{int(time.time())}_{id(func)}"
        
        if task_id in self._tasks:
            raise SchedulingError(f"Task ID {task_id} already exists")
        
        async def delayed_task():
            await asyncio.sleep(delay)
            
            try:
                if inspect.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    result = func(*args, **kwargs)
                
                self.logger.debug(f"Delayed task {task_id} completed successfully")
                return result
            except Exception as e:
                self.logger.error(f"Delayed task {task_id} failed: {e}")
                raise
            finally:
                self._tasks.pop(task_id, None)
        
        task = asyncio.create_task(delayed_task(), name=task_id)
        self._tasks[task_id] = task
        
        self.logger.debug(f"Scheduled delayed task {task_id} with delay {delay}s")
        return task_id
    
    async def schedule_periodic(
        self,
        func: Union[Callable[..., T], Callable[..., Awaitable[T]]],
        interval: float,
        *args,
        task_id: str = None,
        max_runs: int = None,
        **kwargs
    ) -> str:
        """
        Schedule a function to run periodically.
        
        Args:
            func: Function to schedule (sync or async)
            interval: Interval in seconds
            *args: Positional arguments to pass to the function
            task_id: Optional task ID (generated if not provided)
            max_runs: Maximum number of runs (None for unlimited)
            **kwargs: Keyword arguments to pass to the function
            
        Returns:
            Task ID
        """
        if not self._running:
            raise SchedulingError("Scheduler is not running")
        
        if task_id is None:
            task_id = f"periodic_{int(time.time())}_{id(func)}"
        
        if task_id in self._tasks:
            raise SchedulingError(f"Task ID {task_id} already exists")
        
        async def periodic_task():
            run_count = 0
            
            while self._running and (max_runs is None or run_count < max_runs):
                try:
                    if inspect.iscoroutinefunction(func):
                        result = await func(*args, **kwargs)
                    else:
                        result = func(*args, **kwargs)
                    
                    run_count += 1
                    self.logger.debug(f"Periodic task {task_id} run {run_count} completed")
                    
                    if max_runs is not None and run_count >= max_runs:
                        break
                    
                    await asyncio.sleep(interval)
                    
                except asyncio.CancelledError:
                    self.logger.debug(f"Periodic task {task_id} cancelled")
                    break
                except Exception as e:
                    self.logger.error(f"Periodic task {task_id} failed: {e}")
                    await asyncio.sleep(interval)  # Wait before retrying
            
            self._tasks.pop(task_id, None)
            self.logger.debug(f"Periodic task {task_id} stopped after {run_count} runs")
        
        task = asyncio.create_task(periodic_task(), name=task_id)
        self._tasks[task_id] = task
        
        self.logger.debug(f"Scheduled periodic task {task_id} with interval {interval}s")
        return task_id
    
    async def cancel_task(self, task_id: str) -> bool:
        """
        Cancel a scheduled task.
        
        Args:
            task_id: ID of the task to cancel
            
        Returns:
            True if task was cancelled, False if not found
        """
        task = self._tasks.get(task_id)
        if task is None:
            return False
        
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        self._tasks.pop(task_id, None)
        self.logger.debug(f"Cancelled task {task_id}")
        return True
    
    def get_task_status(self, task_id: str) -> Optional[str]:
        """
        Get the status of a task.
        
        Args:
            task_id: ID of the task
            
        Returns:
            Task status or None if not found
        """
        task = self._tasks.get(task_id)
        if task is None:
            return None
        
        if task.done():
            if task.cancelled():
                return "cancelled"
            elif task.exception():
                return "failed"
            else:
                return "completed"
        else:
            return "running"
    
    def list_tasks(self) -> List[str]:
        """
        Get list of all task IDs.
        
        Returns:
            List of task IDs
        """
        return list(self._tasks.keys())


# =============================================================================
# Benchmarking Helpers
# =============================================================================

@dataclass
class BenchmarkResult:
    """Result of a benchmark run."""
    
    name: str
    stats: TimingStats
    memory_usage: Optional[Dict[str, float]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "name": self.name,
            "stats": self.stats.summary(),
            "memory_usage": self.memory_usage,
            "metadata": self.metadata
        }


@dataclass
class BenchmarkComparison:
    """Comparison between multiple benchmark results."""
    
    results: List[BenchmarkResult]
    
    def get_fastest(self) -> Optional[BenchmarkResult]:
        """Get the fastest benchmark result."""
        if not self.results:
            return None
        
        successful_results = [r for r in self.results if r.stats.successful_durations]
        if not successful_results:
            return None
        
        return min(successful_results, key=lambda r: r.stats.avg_duration or float('inf'))
    
    def get_slowest(self) -> Optional[BenchmarkResult]:
        """Get the slowest benchmark result."""
        if not self.results:
            return None
        
        successful_results = [r for r in self.results if r.stats.successful_durations]
        if not successful_results:
            return None
        
        return max(successful_results, key=lambda r: r.stats.avg_duration or 0)
    
    def get_ranking(self) -> List[BenchmarkResult]:
        """Get benchmark results ranked by performance."""
        successful_results = [r for r in self.results if r.stats.successful_durations]
        return sorted(successful_results, key=lambda r: r.stats.avg_duration or float('inf'))
    
    def summary(self) -> Dict[str, Any]:
        """Get a summary of the comparison."""
        if not self.results:
            return {"error": "No benchmark results to compare"}
        
        fastest = self.get_fastest()
        slowest = self.get_slowest()
        ranking = self.get_ranking()
        
        return {
            "total_benchmarks": len(self.results),
            "successful_benchmarks": len([r for r in self.results if r.stats.successful_durations]),
            "fastest": fastest.name if fastest else None,
            "slowest": slowest.name if slowest else None,
            "ranking": [r.name for r in ranking],
            "detailed_results": [r.to_dict() for r in self.results]
        }


class Benchmark:
    """
    Benchmarking utility for performance testing.
    
    This class provides comprehensive benchmarking capabilities including
    timing measurement, memory tracking, and comparative analysis.
    """
    
    def __init__(self, name: str, logger: StructuredLogger = None):
        """
        Initialize the benchmark.
        
        Args:
            name: Name of the benchmark
            logger: Optional logger instance
        """
        self.name = name
        self.logger = logger or get_logger(__name__)
        self._results: List[TimingResult] = []
        self._memory_before: Optional[Dict[str, float]] = None
        self._memory_after: Optional[Dict[str, float]] = None
    
    def _get_memory_usage(self) -> Dict[str, float]:
        """Get current memory usage."""
        try:
            import psutil
            process = psutil.Process()
            memory_info = process.memory_info()
            return {
                "rss": memory_info.rss / 1024 / 1024,  # MB
                "vms": memory_info.vms / 1024 / 1024,  # MB
                "percent": process.memory_percent()
            }
        except ImportError:
            return {}
    
    def start_memory_tracking(self) -> None:
        """Start tracking memory usage."""
        self._memory_before = self._get_memory_usage()
    
    def stop_memory_tracking(self) -> Dict[str, float]:
        """Stop tracking memory usage and return the difference."""
        self._memory_after = self._get_memory_usage()
        
        if not self._memory_before or not self._memory_after:
            return {}
        
        return {
            key: self._memory_after[key] - self._memory_before[key]
            for key in self._memory_before
        }
    
    def run_once(
        self,
        func: Union[Callable[..., T], Callable[..., Awaitable[T]]],
        *args,
        track_memory: bool = False,
        **kwargs
    ) -> TimingResult:
        """
        Run a function once and return timing result.
        
        Args:
            func: Function to benchmark (sync or async)
            *args: Positional arguments to pass to the function
            track_memory: Whether to track memory usage
            **kwargs: Keyword arguments to pass to the function
            
        Returns:
            TimingResult
        """
        timer = Timer(f"{self.name}_single")
        
        if track_memory:
            self.start_memory_tracking()
        
        timer.start()
        
        try:
            if inspect.iscoroutinefunction(func):
                # For async functions, we need to run them in an event loop
                try:
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                
                result = loop.run_until_complete(func(*args, **kwargs))
            else:
                result = func(*args, **kwargs)
            
            timing_result = timer.stop()
            timing_result.metadata["result"] = result
            
            if track_memory:
                memory_diff = self.stop_memory_tracking()
                timing_result.metadata["memory_diff"] = memory_diff
            
            return timing_result
            
        except Exception as e:
            if timer.is_running:
                timer.stop()
            raise
    
    async def run_once_async(
        self,
        func: Callable[..., Awaitable[T]],
        *args,
        track_memory: bool = False,
        **kwargs
    ) -> TimingResult:
        """
        Run an async function once and return timing result.
        
        Args:
            func: Async function to benchmark
            *args: Positional arguments to pass to the function
            track_memory: Whether to track memory usage
            **kwargs: Keyword arguments to pass to the function
            
        Returns:
            TimingResult
        """
        timer = AsyncTimer(f"{self.name}_single_async")
        
        if track_memory:
            self.start_memory_tracking()
        
        timer.start()
        
        try:
            result = await func(*args, **kwargs)
            
            timing_result = timer.stop()
            timing_result.metadata["result"] = result
            
            if track_memory:
                memory_diff = self.stop_memory_tracking()
                timing_result.metadata["memory_diff"] = memory_diff
            
            return timing_result
            
        except Exception as e:
            if timer.is_running:
                timer.stop()
            raise
    
    def run_multiple(
        self,
        func: Union[Callable[..., T], Callable[..., Awaitable[T]]],
        runs: int,
        *args,
        track_memory: bool = False,
        **kwargs
    ) -> BenchmarkResult:
        """
        Run a function multiple times and return benchmark result.
        
        Args:
            func: Function to benchmark (sync or async)
            runs: Number of runs
            *args: Positional arguments to pass to the function
            track_memory: Whether to track memory usage
            **kwargs: Keyword arguments to pass to the function
            
        Returns:
            BenchmarkResult
        """
        self._results.clear()
        
        if track_memory:
            self.start_memory_tracking()
        
        for i in range(runs):
            try:
                result = self.run_once(func, *args, track_memory=False, **kwargs)
                self._results.append(result)
            except Exception as e:
                self.logger.error(f"Benchmark run {i+1} failed: {e}")
                # Create a failed timing result
                failed_result = TimingResult(
                    operation=f"{self.name}_run_{i+1}",
                    duration=0,
                    start_time=0,
                    end_time=0,
                    success=False,
                    error=e
                )
                self._results.append(failed_result)
        
        memory_usage = None
        if track_memory:
            memory_usage = self.stop_memory_tracking()
        
        stats = TimingStats(self._results)
        return BenchmarkResult(
            name=self.name,
            stats=stats,
            memory_usage=memory_usage
        )
    
    async def run_multiple_async(
        self,
        func: Callable[..., Awaitable[T]],
        runs: int,
        *args,
        track_memory: bool = False,
        **kwargs
    ) -> BenchmarkResult:
        """
        Run an async function multiple times and return benchmark result.
        
        Args:
            func: Async function to benchmark
            runs: Number of runs
            *args: Positional arguments to pass to the function
            track_memory: Whether to track memory usage
            **kwargs: Keyword arguments to pass to the function
            
        Returns:
            BenchmarkResult
        """
        self._results.clear()
        
        if track_memory:
            self.start_memory_tracking()
        
        for i in range(runs):
            try:
                result = await self.run_once_async(func, *args, track_memory=False, **kwargs)
                self._results.append(result)
            except Exception as e:
                self.logger.error(f"Benchmark run {i+1} failed: {e}")
                # Create a failed timing result
                failed_result = TimingResult(
                    operation=f"{self.name}_run_{i+1}",
                    duration=0,
                    start_time=0,
                    end_time=0,
                    success=False,
                    error=e
                )
                self._results.append(failed_result)
        
        memory_usage = None
        if track_memory:
            memory_usage = self.stop_memory_tracking()
        
        stats = TimingStats(self._results)
        return BenchmarkResult(
            name=self.name,
            stats=stats,
            memory_usage=memory_usage
        )
    
    def get_stats(self) -> TimingStats:
        """Get statistics for all benchmark runs."""
        return TimingStats(self._results)


def compare_benchmarks(*benchmarks: BenchmarkResult) -> BenchmarkComparison:
    """
    Compare multiple benchmark results.
    
    Args:
        *benchmarks: BenchmarkResult objects to compare
        
    Returns:
        BenchmarkComparison object
    """
    return BenchmarkComparison(list(benchmarks))


# =============================================================================
# Timing Context Managers
# =============================================================================

@asynccontextmanager
async def timing_context(
    operation: str,
    logger: StructuredLogger = None,
    log_level: str = "INFO",
    slow_threshold: float = None,
    slow_log_level: str = "WARNING"
) -> AsyncIterator[TimingResult]:
    """
    Async context manager for timing operations with logging.
    
    Args:
        operation: Name of the operation being timed
        logger: Optional logger instance
        log_level: Log level for normal timing
        slow_threshold: Optional threshold for slow operations
        slow_log_level: Log level for slow operations
        
    Yields:
        TimingResult object
    """
    logger_ = logger or get_logger(__name__)
    timer = AsyncTimer(operation)
    
    with timer:
        try:
            yield timer
        finally:
            if timer.results:
                result = timer.results[-1]
                
                # Determine log level based on threshold
                actual_log_level = slow_log_level if slow_threshold and result.duration > slow_threshold else log_level
                
                if result.success:
                    getattr(logger_, actual_log_level.lower())(
                        f"{operation} completed in {result.duration:.6f}s"
                    )
                else:
                    logger_.error(f"{operation} failed after {result.duration:.6f}s: {result.error}")


@contextmanager
def timing_context_sync(
    operation: str,
    logger: StructuredLogger = None,
    log_level: str = "INFO",
    slow_threshold: float = None,
    slow_log_level: str = "WARNING"
) -> Iterator[TimingResult]:
    """
    Synchronous context manager for timing operations with logging.
    
    Args:
        operation: Name of the operation being timed
        logger: Optional logger instance
        log_level: Log level for normal timing
        slow_threshold: Optional threshold for slow operations
        slow_log_level: Log level for slow operations
        
    Yields:
        TimingResult object
    """
    logger_ = logger or get_logger(__name__)
    timer = Timer(operation)
    
    with timer:
        try:
            yield timer
        finally:
            if timer.results:
                result = timer.results[-1]
                
                # Determine log level based on threshold
                actual_log_level = slow_log_level if slow_threshold and result.duration > slow_threshold else log_level
                
                if result.success:
                    getattr(logger_, actual_log_level.lower())(
                        f"{operation} completed in {result.duration:.6f}s"
                    )
                else:
                    logger_.error(f"{operation} failed after {result.duration:.6f}s: {result.error}")


class NestedTimer:
    """
    Timer that supports nested timing operations.
    
    This timer allows for hierarchical timing where sub-operations
    can be timed within the context of parent operations.
    """
    
    def __init__(self, name: str, parent: 'NestedTimer' = None, logger: StructuredLogger = None):
        """
        Initialize the nested timer.
        
        Args:
            name: Name of the timer
            parent: Optional parent timer
            logger: Optional logger instance
        """
        self.name = name
        self.parent = parent
        self.logger = logger or get_logger(__name__)
        self._start_time: Optional[float] = None
        self._end_time: Optional[float] = None
        self._children: List[NestedTimer] = []
        self._result: Optional[TimingResult] = None
    
    def start(self) -> None:
        """Start the timer."""
        self._start_time = time.perf_counter()
        self._end_time = None
        self._result = None
    
    def stop(self) -> TimingResult:
        """
        Stop the timer and return the result.
        
        Returns:
            TimingResult with the measurement
        """
        if self._start_time is None:
            raise TimingError("Timer not started")
        
        self._end_time = time.perf_counter()
        duration = self._end_time - self._start_time
        
        # Calculate exclusive time (time not spent in children)
        children_time = sum(child._result.duration if child._result else 0 for child in self._children)
        exclusive_time = duration - children_time
        
        self._result = TimingResult(
            operation=self.name,
            duration=duration,
            start_time=self._start_time,
            end_time=self._end_time,
            metadata={
                "exclusive_time": exclusive_time,
                "children_time": children_time,
                "children_count": len(self._children)
            }
        )
        
        return self._result
    
    def child(self, name: str) -> 'NestedTimer':
        """
        Create a child timer.
        
        Args:
            name: Name of the child timer
            
        Returns:
            NestedTimer instance
        """
        child = NestedTimer(name, parent=self, logger=self.logger)
        self._children.append(child)
        return child
    
    def get_hierarchy(self) -> Dict[str, Any]:
        """
        Get the timing hierarchy as a dictionary.
        
        Returns:
            Dictionary representing the timing hierarchy
        """
        if self._result is None:
            return {}
        
        result = {
            "operation": self._result.operation,
            "duration": self._result.duration,
            "exclusive_time": self._result.metadata.get("exclusive_time", 0),
            "children_time": self._result.metadata.get("children_time", 0),
            "children": [child.get_hierarchy() for child in self._children]
        }
        
        return result
    
    def __enter__(self) -> 'NestedTimer':
        """Context manager entry."""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        if exc_type is None:
            self.stop()
        else:
            # Record failed timing
            if self._start_time is not None:
                self._end_time = time.perf_counter()
                duration = self._end_time - self._start_time
                
                self._result = TimingResult(
                    operation=self.name,
                    duration=duration,
                    start_time=self._start_time,
                    end_time=self._end_time,
                    success=False,
                    error=exc_val
                )


class AsyncNestedTimer(NestedTimer):
    """
    Asynchronous version of the NestedTimer class.
    """
    
    async def __aenter__(self) -> 'AsyncNestedTimer':
        """Async context manager entry."""
        self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        if exc_type is None:
            self.stop()
        else:
            # Record failed timing
            if self._start_time is not None:
                self._end_time = time.perf_counter()
                duration = self._end_time - self._start_time
                
                self._result = TimingResult(
                    operation=self.name,
                    duration=duration,
                    start_time=self._start_time,
                    end_time=self._end_time,
                    success=False,
                    error=exc_val
                )