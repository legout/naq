# Subtask 02: Create Connection Utilities

## Overview
Create connection monitoring, testing, and utility functions to support the new connection management system.

## Objectives
- Implement connection monitoring and metrics collection
- Create connection health testing utilities
- Develop connection waiting/retry mechanisms
- Add connection performance tracking
- Create global connection monitor instance

## Implementation Details

### Files to Create
- `src/naq/connection/utils.py`

### Utilities to Implement

#### 1. Connection Metrics Dataclass
```python
@dataclass
class ConnectionMetrics:
    """Connection usage metrics."""
    total_connections: int = 0
    active_connections: int = 0
    failed_connections: int = 0
    average_connection_time: float = 0.0
    total_connection_time: float = 0.0
    successful_connections: int = 0
```

#### 2. Connection Monitor Class
```python
class ConnectionMonitor:
    """Monitor connection usage and performance."""
    
    def __init__(self):
        self.metrics = ConnectionMetrics()
        self._connection_times: List[float] = []
        self._lock = asyncio.Lock()
    
    async def record_connection_start(self):
        """Record connection start."""
        async with self._lock:
            self.metrics.total_connections += 1
            self.metrics.active_connections += 1
    
    async def record_connection_end(self, duration: float):
        """Record connection end."""
        async with self._lock:
            self.metrics.active_connections -= 1
            self._connection_times.append(duration)
            self.metrics.total_connection_time += duration
            self.metrics.average_connection_time = (
                self.metrics.total_connection_time / self.metrics.total_connections
            )
            self.metrics.successful_connections += 1
    
    async def record_connection_failure(self):
        """Record connection failure."""
        async with self._lock:
            self.metrics.failed_connections += 1
    
    def get_metrics(self) -> ConnectionMetrics:
        """Get current connection metrics."""
        return self.metrics
    
    def reset_metrics(self):
        """Reset all metrics."""
        async with self._lock:
            self.metrics = ConnectionMetrics()
            self._connection_times.clear()
```

#### 3. Connection Testing Utilities
```python
async def test_nats_connection(config: Optional[Config] = None) -> bool:
    """Test NATS connection health."""
    try:
        async with nats_connection(config) as conn:
            # Simple ping test
            await conn.flush(timeout=5.0)
            return True
    except Exception as e:
        logger.error(f"NATS connection test failed: {e}")
        return False

async def wait_for_nats_connection(
    config: Optional[Config] = None, 
    timeout: int = 30, 
    interval: float = 1.0
) -> bool:
    """Wait for NATS connection to be available."""
    start_time = asyncio.get_event_loop().time()
    
    while (asyncio.get_event_loop().time() - start_time) < timeout:
        if await test_nats_connection(config):
            return True
        await asyncio.sleep(interval)
    
    return False

async def test_jetstream_connection(config: Optional[Config] = None) -> bool:
    """Test JetStream connection health."""
    try:
        async with nats_jetstream(config) as (conn, js):
            # Test JetStream functionality
            streams = await js.streams_info()
            return True
    except Exception as e:
        logger.error(f"JetStream connection test failed: {e}")
        return False
```

#### 4. Connection Performance Utilities
```python
async def measure_connection_performance(
    config: Optional[Config] = None,
    iterations: int = 10
) -> Dict[str, float]:
    """Measure connection establishment performance."""
    times = []
    
    for _ in range(iterations):
        start_time = asyncio.get_event_loop().time()
        try:
            async with nats_connection(config) as conn:
                end_time = asyncio.get_event_loop().time()
                times.append(end_time - start_time)
        except Exception as e:
            logger.error(f"Connection performance test failed: {e}")
    
    return {
        "average_time": sum(times) / len(times) if times else 0.0,
        "min_time": min(times) if times else 0.0,
        "max_time": max(times) if times else 0.0,
        "success_rate": len(times) / iterations
    }
```

#### 5. Global Connection Monitor
```python
# Global connection monitor instance
connection_monitor = ConnectionMonitor()

# Utility functions for global monitor
async def get_connection_metrics() -> ConnectionMetrics:
    """Get global connection metrics."""
    return connection_monitor.get_metrics()

async def reset_connection_metrics():
    """Reset global connection metrics."""
    connection_monitor.reset_metrics()
```

### Requirements
- Thread-safe metrics collection using asyncio.Lock
- Comprehensive connection health testing
- Performance measurement capabilities
- Global monitor instance for system-wide metrics
- Async/await patterns throughout
- Proper error handling and logging
- Type hints for all functions and dataclasses

## Success Criteria
- [ ] All utility functions implemented with proper error handling
- [ ] Thread-safe metrics collection working correctly
- [ ] Connection testing utilities functional
- [ ] Performance measurement capabilities working
- [ ] Global monitor instance accessible throughout system
- [ ] All utilities tested with unit tests
- [ ] No race conditions in metrics collection

## Dependencies
- Task 01 (Connection Context Managers) for core connection functions
- Existing `src/naq/settings.py` for configuration management
- `asyncio` for concurrency and locking

## Estimated Time
- 3-4 hours