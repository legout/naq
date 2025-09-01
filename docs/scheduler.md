# NAQ Scheduler Documentation

## Overview

The NAQ Scheduler is a high-availability, distributed job scheduling system built on top of NATS. It provides reliable scheduling of jobs with leader election to ensure that only one instance processes scheduled jobs at a time in a high-availability setup.

## Key Features

- **High Availability**: Leader election ensures only one scheduler instance processes jobs
- **Fault Tolerance**: Automatic leader failover if the current leader becomes unavailable
- **Flexible Scheduling**: Support for both interval-based and cron-based scheduling
- **Resource Management**: Efficient connection and resource management
- **Comprehensive Logging**: Detailed logging for monitoring and debugging
- **Error Handling**: Robust error handling and recovery mechanisms

## Architecture

### Components

The scheduler consists of two main components:

1. **LeaderElection**: Handles leader election using NATS KV store
2. **Scheduler**: Manages job scheduling and processing

### Leader Election

The `LeaderElection` class implements a distributed leader election algorithm using a lock stored in NATS KV store:

- Each scheduler instance attempts to acquire a leader lock
- The instance that holds the lock becomes the leader and processes scheduled jobs
- Other instances remain as followers and do not process jobs
- The leader periodically renews the lock to maintain leadership
- If the leader fails, the lock expires and another instance can acquire it

### Scheduler Loop

The `Scheduler` class implements the main scheduling loop:

1. **Leadership Transition**: Determine if this instance should be the leader
2. **Job Processing**: If leader, process due jobs
3. **Wait**: Wait for the next polling cycle

## Usage

### Basic Usage

```python
import asyncio
from naq.scheduler import Scheduler

async def main():
    # Create a scheduler instance
    scheduler = Scheduler(
        nats_url="nats://localhost:4222",
        poll_interval=1.0,  # Check for jobs every second
        enable_ha=True  # Enable high availability mode
    )
    
    # Run the scheduler
    await scheduler.run()

if __name__ == "__main__":
    asyncio.run(main())
```

### Using with Service Manager

```python
import asyncio
from naq.scheduler import Scheduler
from naq.services.base import ServiceManager

async def main():
    # Create a service manager
    service_manager = ServiceManager()
    
    # Create a scheduler instance with the service manager
    scheduler = Scheduler(
        service_manager=service_manager,
        poll_interval=1.0,
        enable_ha=True
    )
    
    # Run the scheduler
    await scheduler.run()

if __name__ == "__main__":
    asyncio.run(main())
```

### Using as a Context Manager

```python
import asyncio
from naq.scheduler import Scheduler

async def main():
    async with Scheduler(
        nats_url="nats://localhost:4222",
        poll_interval=1.0,
        enable_ha=True
    ) as scheduler:
        # The scheduler is automatically connected and cleaned up
        await scheduler.run()

if __name__ == "__main__":
    asyncio.run(main())
```

### Disabling High Availability

```python
import asyncio
from naq.scheduler import Scheduler

async def main():
    # Create a scheduler instance with HA disabled
    scheduler = Scheduler(
        nats_url="nats://localhost:4222",
        poll_interval=1.0,
        enable_ha=False  # Disable high availability mode
    )
    
    # Run the scheduler
    await scheduler.run()

if __name__ == "__main__":
    asyncio.run(main())
```

## Configuration

### Scheduler Parameters

- `nats_url`: NATS server URL for connection
- `service_manager`: Service manager for accessing NAQ services
- `poll_interval`: Interval in seconds to check for scheduled jobs (default: 1.0)
- `instance_id`: Unique identifier for this scheduler instance (auto-generated if not provided)
- `enable_ha`: Whether to enable high availability mode with leader election (default: True)
- `config`: Global service configuration for backward compatibility

### Leader Election Parameters

- `lock_ttl`: Time-to-live for the leader lock in seconds (default: 30)
- `lock_renew_interval`: Interval at which to renew the leader lock in seconds (default: 10)

## Monitoring and Debugging

### Leader Lock Health

You can check the health of the leader lock using the `check_leader_lock_health` method:

```python
# Get leader election instance
leader_election = scheduler._leader_election

# Check lock health
health = await leader_election.check_leader_lock_health()
print(f"Lock status: {health['status']}")
print(f"Is leader: {health['is_leader']}")
print(f"Lock owner: {health['lock_owner']}")
print(f"Time until expiry: {health['time_until_expiry']} seconds")
```

### Logging

The scheduler provides detailed logging for monitoring and debugging:

- **INFO**: Major state changes (leadership transitions, lock acquisition/release)
- **DEBUG**: Detailed operational information (lock checks, renewal attempts)
- **WARNING**: Non-critical issues (timeouts, failed attempts)
- **ERROR**: Critical errors (connection failures, unrecoverable errors)

### Example Log Output

```
2023-01-01 12:00:00 | INFO     | naq.scheduler:__init__:673 - Scheduler instance test-instance-12345678 started. Polling interval: 1.0s
2023-01-01 12:00:00 | INFO     | naq.scheduler:__init__:675 - High availability mode: enabled
2023-01-01 12:00:00 | DEBUG   | naq.scheduler:_handle_leadership_transition:844 - Handling leadership transition. Was leader: False, Is leader: False, HA enabled: True
2023-01-01 12:00:00 | DEBUG   | naq.scheduler:_handle_ha_leadership:862 - Instance test-instance-12345678 attempting to become leader
2023-01-01 12:00:00 | INFO     | naq.scheduler:try_become_leader:131 - Instance test-instance-12345678 successfully acquired leader lock
2023-01-01 12:00:00 | INFO     | naq.scheduler:_handle_ha_leadership:865 - Instance test-instance-12345678 successfully became leader
2023-01-01 12:00:01 | DEBUG   | naq.scheduler:_process_scheduled_jobs:881 - Processing scheduled jobs as leader instance test-instance-12345678
2023-01-01 12:00:01 | INFO     | naq.scheduler:_process_scheduled_jobs:889 - Scheduler processed 5 ready jobs, encountered 0 errors
```

## Error Handling

The scheduler implements robust error handling:

### Connection Errors

- Automatic retry with exponential backoff
- Graceful degradation when services are unavailable
- Detailed error logging for troubleshooting

### Leader Election Errors

- Automatic retry on failed lock acquisition
- Health checks to detect and recover from inconsistent states
- Graceful leadership relinquishment on repeated failures

### Job Processing Errors

- Individual job errors don't stop the scheduler
- Error counting and reporting
- Continuation of processing despite errors

## Best Practices

### Deployment

1. **Multiple Instances**: Deploy multiple scheduler instances for high availability
2. **Resource Allocation**: Ensure sufficient resources for each instance
3. **Network Configuration**: Configure NATS for high availability and fault tolerance
4. **Monitoring**: Implement monitoring for scheduler health and performance

### Configuration

1. **Poll Interval**: Adjust based on job scheduling requirements
2. **Lock TTL**: Set based on expected failure detection time
3. **Renewal Interval**: Set to a fraction of the lock TTL (e.g., 1/3)
4. **Instance IDs**: Use meaningful instance IDs for easier debugging

### Operations

1. **Graceful Shutdown**: Use SIGTERM or SIGINT for graceful shutdown
2. **Log Monitoring**: Monitor logs for errors and warnings
3. **Health Checks**: Implement regular health checks
4. **Capacity Planning**: Monitor and adjust capacity based on load

## Troubleshooting

### Common Issues

#### Scheduler Not Processing Jobs

1. **Check Leadership**: Verify the instance is the leader
   ```python
   print(f"Is leader: {scheduler.is_leader}")
   ```

2. **Check Lock Health**: Verify the leader lock is healthy
   ```python
   health = await scheduler._leader_election.check_leader_lock_health()
   print(f"Lock health: {health}")
   ```

3. **Check Scheduler Service**: Verify the scheduler service is available
   ```python
   print(f"Scheduler service: {scheduler._scheduler_service}")
   ```

#### Leadership Transitions

1. **Check Lock Expiry**: Verify the lock hasn't expired
   ```python
   health = await scheduler._leader_election.check_leader_lock_health()
   print(f"Time until expiry: {health['time_until_expiry']} seconds")
   ```

2. **Check Renewal Task**: Verify the renewal task is running
   ```python
   print(f"Renewal task: {scheduler._leader_election._lock_renewal_task}")
   ```

#### Connection Issues

1. **Check NATS Connection**: Verify NATS connection is established
   ```python
   print(f"Connection service: {scheduler._connection_service}")
   ```

2. **Check Service Manager**: Verify services are initialized
   ```python
   print(f"KV store service: {scheduler._kv_store_service}")
   print(f"Event service: {scheduler._event_service}")
   ```

### Debug Mode

Enable debug logging for detailed troubleshooting:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Testing

The scheduler includes comprehensive tests:

### Unit Tests

- Leader election functionality
- Scheduler core functionality
- Error handling scenarios
- Configuration options

### Integration Tests

- End-to-end scheduler operation
- Multi-instance coordination
- Leader election and failover
- Context manager behavior

### Running Tests

```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/test_scheduler.py

# Run specific test
pytest tests/test_scheduler.py::test_scheduler_initialization

# Run with verbose output
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src/naq/scheduler
```

## API Reference

### LeaderElection Class

#### Methods

- `initialize()`: Initialize the leader election system
- `try_become_leader()`: Attempt to acquire the leader lock
- `check_leader_lock_health()`: Check the health of the leader lock
- `start_renewal_task()`: Start the lock renewal task
- `stop_renewal_task()`: Stop the lock renewal task
- `release_lock()`: Release the leader lock

#### Properties

- `is_leader`: Returns True if this instance is currently the leader

### Scheduler Class

#### Methods

- `run()`: Start the scheduler loop
- `_connect()`: Establish service connections
- `_handle_leadership_transition()`: Handle leadership transition logic
- `_process_scheduled_jobs()`: Process scheduled jobs if leader
- `_wait_for_next_cycle()`: Wait for the next scheduler cycle
- `_shutdown()`: Perform graceful shutdown
- `signal_handler()`: Handle termination signals
- `install_signal_handlers()`: Install signal handlers

#### Properties

- `is_leader`: Returns True if this scheduler instance is currently the leader

## Contributing

When contributing to the scheduler code:

1. **Follow Code Style**: Adhere to the project's code style guidelines
2. **Add Tests**: Include tests for new functionality
3. **Update Documentation**: Update documentation for API changes
4. **Error Handling**: Implement robust error handling
5. **Logging**: Add appropriate logging for new features

## License

This project is licensed under the MIT License.