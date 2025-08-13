# src/naq/utils/timing.py
"""
Timing and performance measurement utilities for NAQ.

This module provides utilities for measuring performance, benchmarking,
and timing operations.
"""

import time
import asyncio
import statistics
from typing import Callable, Any, Optional, Dict, List, Union, ContextManager
from contextlib import contextmanager, asynccontextmanager
from dataclasses import dataclass, field
from collections import defaultdict

from loguru import logger


@dataclass
class TimingResult:
    """Result from a timing measurement."""
    operation: str
    duration_ms: float
    start_time: float
    end_time: float
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __str__(self) -> str:
        return f"{self.operation}: {self.duration_ms:.2f}ms"


@dataclass
class BenchmarkResult:
    """Result from a benchmark run."""
    operation: str
    iterations: int
    total_time_ms: float
    avg_time_ms: float
    min_time_ms: float
    max_time_ms: float
    median_time_ms: float
    std_dev_ms: float
    times_ms: List[float]
    
    def __str__(self) -> str:
        return (
            f"{self.operation} ({self.iterations} iterations): "
            f"avg={self.avg_time_ms:.2f}ms, "
            f"min={self.min_time_ms:.2f}ms, "
            f"max={self.max_time_ms:.2f}ms, "
            f"std={self.std_dev_ms:.2f}ms"
        )


class Timer:
    """High-precision timer for performance measurement."""
    
    def __init__(self, name: str = "operation"):
        """Initialize timer with operation name."""
        self.name = name
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.laps: List[float] = []
        self.lap_times: List[float] = []
    
    def start(self):
        """Start the timer."""
        self.start_time = time.perf_counter()
        self.laps = [self.start_time]
        return self
    
    def lap(self, name: Optional[str] = None) -> float:
        """Record a lap time and return duration since last lap."""
        if self.start_time is None:
            raise ValueError("Timer not started")
        
        current_time = time.perf_counter()
        self.laps.append(current_time)
        
        lap_duration = (current_time - self.laps[-2]) * 1000
        self.lap_times.append(lap_duration)
        
        if name:
            logger.debug(f"{self.name} - {name}: {lap_duration:.2f}ms")
        
        return lap_duration
    
    def stop(self) -> TimingResult:
        """Stop the timer and return results."""
        if self.start_time is None:
            raise ValueError("Timer not started")
        
        self.end_time = time.perf_counter()
        duration_ms = (self.end_time - self.start_time) * 1000
        
        return TimingResult(
            operation=self.name,
            duration_ms=duration_ms,
            start_time=self.start_time,
            end_time=self.end_time,
            metadata={
                'laps': len(self.laps) - 1,
                'lap_times_ms': self.lap_times
            }
        )
    
    @property
    def elapsed_ms(self) -> float:
        """Get elapsed time in milliseconds."""
        if self.start_time is None:
            return 0.0
        
        current_time = self.end_time or time.perf_counter()
        return (current_time - self.start_time) * 1000
    
    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        result = self.stop()
        logger.debug(str(result))


@contextmanager
def measure_time(operation_name: str, logger_instance=None, threshold_ms: Optional[float] = None):
    """Context manager to measure operation time."""
    log = logger_instance or logger
    timer = Timer(operation_name)
    
    timer.start()
    try:
        yield timer
    finally:
        result = timer.stop()
        
        if threshold_ms is None or result.duration_ms > threshold_ms:
            log.info(f"{operation_name} took {result.duration_ms:.2f}ms")


@asynccontextmanager
async def async_measure_time(operation_name: str, logger_instance=None, threshold_ms: Optional[float] = None):
    """Async context manager to measure operation time."""
    log = logger_instance or logger
    timer = Timer(operation_name)
    
    timer.start()
    try:
        yield timer
    finally:
        result = timer.stop()
        
        if threshold_ms is None or result.duration_ms > threshold_ms:
            log.info(f"{operation_name} took {result.duration_ms:.2f}ms")


def measure_performance(func: Callable, *args, **kwargs) -> tuple[Any, TimingResult]:
    """Measure performance of a function call."""
    timer = Timer(func.__name__)
    
    timer.start()
    try:
        result = func(*args, **kwargs)
        return result, timer.stop()
    except Exception as e:
        timing_result = timer.stop()
        timing_result.metadata['error'] = str(e)
        raise


async def async_measure_performance(func: Callable, *args, **kwargs) -> tuple[Any, TimingResult]:
    """Measure performance of an async function call."""
    timer = Timer(func.__name__)
    
    timer.start()
    try:
        result = await func(*args, **kwargs)
        return result, timer.stop()
    except Exception as e:
        timing_result = timer.stop()
        timing_result.metadata['error'] = str(e)
        raise


def benchmark_function(
    func: Callable,
    *args,
    iterations: int = 100,
    warmup: int = 10,
    **kwargs
) -> BenchmarkResult:
    """
    Benchmark a function by running it multiple times.
    
    Args:
        func: Function to benchmark
        *args: Arguments for the function
        iterations: Number of iterations to run
        warmup: Number of warmup iterations (not included in results)
        **kwargs: Keyword arguments for the function
        
    Returns:
        BenchmarkResult with statistics
    """
    operation_name = func.__name__
    times = []
    
    # Warmup runs
    for _ in range(warmup):
        start = time.perf_counter()
        func(*args, **kwargs)
        end = time.perf_counter()
    
    # Actual benchmark runs
    for _ in range(iterations):
        start = time.perf_counter()
        func(*args, **kwargs)
        end = time.perf_counter()
        times.append((end - start) * 1000)  # Convert to ms
    
    # Calculate statistics
    total_time = sum(times)
    avg_time = total_time / len(times)
    min_time = min(times)
    max_time = max(times)
    median_time = statistics.median(times)
    std_dev = statistics.stdev(times) if len(times) > 1 else 0.0
    
    return BenchmarkResult(
        operation=operation_name,
        iterations=iterations,
        total_time_ms=total_time,
        avg_time_ms=avg_time,
        min_time_ms=min_time,
        max_time_ms=max_time,
        median_time_ms=median_time,
        std_dev_ms=std_dev,
        times_ms=times
    )


async def async_benchmark_function(
    func: Callable,
    *args,
    iterations: int = 100,
    warmup: int = 10,
    **kwargs
) -> BenchmarkResult:
    """
    Benchmark an async function by running it multiple times.
    
    Args:
        func: Async function to benchmark
        *args: Arguments for the function
        iterations: Number of iterations to run
        warmup: Number of warmup iterations (not included in results)
        **kwargs: Keyword arguments for the function
        
    Returns:
        BenchmarkResult with statistics
    """
    operation_name = func.__name__
    times = []
    
    # Warmup runs
    for _ in range(warmup):
        start = time.perf_counter()
        await func(*args, **kwargs)
        end = time.perf_counter()
    
    # Actual benchmark runs
    for _ in range(iterations):
        start = time.perf_counter()
        await func(*args, **kwargs)
        end = time.perf_counter()
        times.append((end - start) * 1000)  # Convert to ms
    
    # Calculate statistics
    total_time = sum(times)
    avg_time = total_time / len(times)
    min_time = min(times)
    max_time = max(times)
    median_time = statistics.median(times)
    std_dev = statistics.stdev(times) if len(times) > 1 else 0.0
    
    return BenchmarkResult(
        operation=operation_name,
        iterations=iterations,
        total_time_ms=total_time,
        avg_time_ms=avg_time,
        min_time_ms=min_time,
        max_time_ms=max_time,
        median_time_ms=median_time,
        std_dev_ms=std_dev,
        times_ms=times
    )


class PerformanceTracker:
    """Track performance metrics across multiple operations."""
    
    def __init__(self):
        """Initialize performance tracker."""
        self.measurements: Dict[str, List[TimingResult]] = defaultdict(list)
        self.benchmarks: Dict[str, BenchmarkResult] = {}
    
    def record_timing(self, result: TimingResult):
        """Record a timing result."""
        self.measurements[result.operation].append(result)
    
    def record_benchmark(self, result: BenchmarkResult):
        """Record a benchmark result."""
        self.benchmarks[result.operation] = result
    
    def get_stats(self, operation: str) -> Optional[Dict[str, float]]:
        """Get statistics for an operation."""
        if operation not in self.measurements:
            return None
        
        times = [m.duration_ms for m in self.measurements[operation]]
        
        if not times:
            return None
        
        return {
            'count': len(times),
            'total_ms': sum(times),
            'avg_ms': sum(times) / len(times),
            'min_ms': min(times),
            'max_ms': max(times),
            'median_ms': statistics.median(times),
            'std_dev_ms': statistics.stdev(times) if len(times) > 1 else 0.0
        }
    
    def get_all_stats(self) -> Dict[str, Dict[str, float]]:
        """Get statistics for all operations."""
        return {
            operation: self.get_stats(operation)
            for operation in self.measurements.keys()
            if self.get_stats(operation) is not None
        }
    
    @contextmanager
    def track(self, operation_name: str):
        """Context manager to track an operation."""
        timer = Timer(operation_name)
        timer.start()
        
        try:
            yield timer
        finally:
            result = timer.stop()
            self.record_timing(result)
    
    def clear(self, operation: Optional[str] = None):
        """Clear measurements for an operation or all operations."""
        if operation:
            self.measurements.pop(operation, None)
            self.benchmarks.pop(operation, None)
        else:
            self.measurements.clear()
            self.benchmarks.clear()


class RateLimiter:
    """Rate limiter with timing measurements."""
    
    def __init__(self, max_calls: int, time_window: float):
        """Initialize rate limiter."""
        self.max_calls = max_calls
        self.time_window = time_window
        self.calls: List[float] = []
        self.wait_times: List[float] = []
    
    def acquire(self) -> float:
        """Acquire permission and return wait time."""
        now = time.perf_counter()
        
        # Remove old calls outside the time window
        cutoff = now - self.time_window
        self.calls = [call_time for call_time in self.calls if call_time > cutoff]
        
        if len(self.calls) < self.max_calls:
            # Can proceed immediately
            self.calls.append(now)
            return 0.0
        else:
            # Need to wait
            oldest_call = min(self.calls)
            wait_time = (oldest_call + self.time_window) - now
            
            if wait_time > 0:
                time.sleep(wait_time)
                actual_wait = wait_time
            else:
                actual_wait = 0.0
            
            self.calls.append(time.perf_counter())
            self.wait_times.append(actual_wait)
            return actual_wait
    
    async def async_acquire(self) -> float:
        """Async version of acquire."""
        now = time.perf_counter()
        
        # Remove old calls outside the time window
        cutoff = now - self.time_window
        self.calls = [call_time for call_time in self.calls if call_time > cutoff]
        
        if len(self.calls) < self.max_calls:
            # Can proceed immediately
            self.calls.append(now)
            return 0.0
        else:
            # Need to wait
            oldest_call = min(self.calls)
            wait_time = (oldest_call + self.time_window) - now
            
            if wait_time > 0:
                await asyncio.sleep(wait_time)
                actual_wait = wait_time
            else:
                actual_wait = 0.0
            
            self.calls.append(time.perf_counter())
            self.wait_times.append(actual_wait)
            return actual_wait
    
    def get_stats(self) -> Dict[str, float]:
        """Get rate limiter statistics."""
        if not self.wait_times:
            return {'total_waits': 0, 'avg_wait_ms': 0, 'max_wait_ms': 0}
        
        wait_times_ms = [w * 1000 for w in self.wait_times]
        
        return {
            'total_waits': len(self.wait_times),
            'avg_wait_ms': sum(wait_times_ms) / len(wait_times_ms),
            'max_wait_ms': max(wait_times_ms)
        }


# Global performance tracker
_global_tracker = PerformanceTracker()


def get_performance_tracker() -> PerformanceTracker:
    """Get the global performance tracker."""
    return _global_tracker


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable string for backward compatibility.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted duration string
    """
    if seconds < 1:
        return f"{seconds*1000:.1f}ms"
    elif seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.1f}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = seconds % 60
        return f"{hours}h {minutes}m {secs:.1f}s"