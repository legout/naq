# src/naq/connection/utils.py
"""
Connection utilities for monitoring and testing NATS connections.

Provides utilities for connection health testing, monitoring, and metrics
collection to support robust connection management.
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from threading import Lock

from loguru import logger

from .context_managers import nats_connection
from ..exceptions import NaqConnectionError
from ..settings import DEFAULT_NATS_URL


@dataclass
class ConnectionMetrics:
    """Connection usage metrics and statistics."""
    
    total_connections: int = 0
    active_connections: int = 0
    failed_connections: int = 0
    average_connection_time: float = 0.0
    
    # Timing statistics
    min_connection_time: float = float('inf')
    max_connection_time: float = 0.0
    total_connection_time: float = 0.0
    
    # Success/failure tracking
    successful_connections: int = 0
    connection_timeouts: int = 0
    
    def reset(self) -> None:
        """Reset all metrics to initial state."""
        self.total_connections = 0
        self.active_connections = 0
        self.failed_connections = 0
        self.average_connection_time = 0.0
        self.min_connection_time = float('inf')
        self.max_connection_time = 0.0
        self.total_connection_time = 0.0
        self.successful_connections = 0
        self.connection_timeouts = 0


class ConnectionMonitor:
    """Monitor connection usage and performance metrics."""
    
    def __init__(self):
        self.metrics = ConnectionMetrics()
        self._connection_times: List[float] = []
        self._lock = Lock()
    
    def record_connection_start(self) -> None:
        """Record the start of a connection attempt."""
        with self._lock:
            self.metrics.total_connections += 1
            self.metrics.active_connections += 1
    
    def record_connection_success(self, duration: float) -> None:
        """
        Record a successful connection.
        
        Args:
            duration: Time taken to establish connection in seconds
        """
        with self._lock:
            self.metrics.active_connections -= 1
            self.metrics.successful_connections += 1
            
            # Update timing statistics
            self._connection_times.append(duration)
            self.metrics.total_connection_time += duration
            self.metrics.min_connection_time = min(self.metrics.min_connection_time, duration)
            self.metrics.max_connection_time = max(self.metrics.max_connection_time, duration)
            
            # Calculate running average
            if self._connection_times:
                self.metrics.average_connection_time = (
                    self.metrics.total_connection_time / len(self._connection_times)
                )
    
    def record_connection_failure(self, timeout: bool = False) -> None:
        """
        Record a failed connection attempt.
        
        Args:
            timeout: Whether the failure was due to timeout
        """
        with self._lock:
            self.metrics.active_connections -= 1
            self.metrics.failed_connections += 1
            
            if timeout:
                self.metrics.connection_timeouts += 1
    
    def get_metrics(self) -> ConnectionMetrics:
        """Get a copy of current metrics."""
        with self._lock:
            # Create a copy to avoid race conditions
            return ConnectionMetrics(
                total_connections=self.metrics.total_connections,
                active_connections=self.metrics.active_connections,
                failed_connections=self.metrics.failed_connections,
                average_connection_time=self.metrics.average_connection_time,
                min_connection_time=self.metrics.min_connection_time,
                max_connection_time=self.metrics.max_connection_time,
                total_connection_time=self.metrics.total_connection_time,
                successful_connections=self.metrics.successful_connections,
                connection_timeouts=self.metrics.connection_timeouts
            )
    
    def reset_metrics(self) -> None:
        """Reset all metrics to initial state."""
        with self._lock:
            self.metrics.reset()
            self._connection_times.clear()


# Global connection monitor instance
connection_monitor = ConnectionMonitor()


async def test_nats_connection(config: Optional[dict] = None, timeout: float = 10.0) -> bool:
    """
    Test NATS connection health.
    
    Performs a basic connectivity test by establishing a connection,
    performing a flush operation, and cleaning up.
    
    Args:
        config: Optional connection configuration
        timeout: Connection test timeout in seconds
        
    Returns:
        True if connection test successful, False otherwise
    """
    connection_monitor.record_connection_start()
    start_time = time.time()
    
    try:
        # Set timeout for the entire operation
        async with asyncio.timeout(timeout):
            async with nats_connection(config) as conn:
                # Simple ping test using flush
                await conn.flush(timeout=5.0)
                
        duration = time.time() - start_time
        connection_monitor.record_connection_success(duration)
        logger.debug(f"NATS connection test successful in {duration:.3f}s")
        return True
        
    except asyncio.TimeoutError:
        connection_monitor.record_connection_failure(timeout=True)
        logger.error(f"NATS connection test timed out after {timeout}s")
        return False
        
    except Exception as e:
        connection_monitor.record_connection_failure(timeout=False)
        logger.error(f"NATS connection test failed: {e}")
        return False


async def wait_for_nats_connection(
    config: Optional[dict] = None, 
    timeout: int = 30, 
    retry_interval: float = 1.0
) -> bool:
    """
    Wait for NATS connection to be available.
    
    Continuously tests connection health until successful or timeout reached.
    Useful for startup sequences and health checks.
    
    Args:
        config: Optional connection configuration
        timeout: Maximum time to wait in seconds
        retry_interval: Time between retry attempts in seconds
        
    Returns:
        True if connection became available within timeout, False otherwise
    """
    start_time = time.time()
    attempt = 1
    
    logger.info(f"Waiting for NATS connection (timeout: {timeout}s)")
    
    while (time.time() - start_time) < timeout:
        logger.debug(f"NATS connection attempt {attempt}")
        
        if await test_nats_connection(config, timeout=min(5.0, timeout/4)):
            elapsed = time.time() - start_time
            logger.info(f"NATS connection available after {elapsed:.1f}s ({attempt} attempts)")
            return True
        
        # Calculate remaining time and adjust sleep interval if needed
        remaining_time = timeout - (time.time() - start_time)
        sleep_time = min(retry_interval, remaining_time)
        
        if sleep_time <= 0:
            break
            
        await asyncio.sleep(sleep_time)
        attempt += 1
    
    elapsed = time.time() - start_time
    logger.warning(f"NATS connection not available after {elapsed:.1f}s ({attempt-1} attempts)")
    return False


async def ping_nats_server(config: Optional[dict] = None, timeout: float = 5.0) -> float:
    """
    Ping NATS server and measure round-trip time.
    
    Args:
        config: Optional connection configuration
        timeout: Ping timeout in seconds
        
    Returns:
        Round-trip time in seconds
        
    Raises:
        NaqConnectionError: If ping fails
    """
    start_time = time.time()
    
    try:
        async with asyncio.timeout(timeout):
            async with nats_connection(config) as conn:
                ping_start = time.time()
                await conn.flush(timeout=timeout/2)
                ping_time = time.time() - ping_start
                
        return ping_time
        
    except asyncio.TimeoutError:
        raise NaqConnectionError(f"NATS ping timed out after {timeout}s")
        
    except Exception as e:
        raise NaqConnectionError(f"NATS ping failed: {e}") from e


async def get_nats_server_info(config: Optional[dict] = None) -> Dict:
    """
    Get NATS server information.
    
    Args:
        config: Optional connection configuration
        
    Returns:
        Dictionary containing server information
        
    Raises:
        NaqConnectionError: If unable to get server info
    """
    try:
        async with nats_connection(config) as conn:
            server_info = conn.connected_server_info()
            
            return {
                'server_id': server_info.get('server_id'),
                'server_name': server_info.get('server_name'),
                'version': server_info.get('version'),
                'proto': server_info.get('proto'),
                'host': server_info.get('host'),
                'port': server_info.get('port'),
                'max_payload': server_info.get('max_payload'),
                'client_id': server_info.get('client_id'),
                'connect_urls': server_info.get('connect_urls', []),
            }
            
    except Exception as e:
        raise NaqConnectionError(f"Failed to get NATS server info: {e}") from e


def get_connection_metrics() -> ConnectionMetrics:
    """
    Get current connection metrics.
    
    Returns:
        Copy of current connection metrics
    """
    return connection_monitor.get_metrics()


def reset_connection_metrics() -> None:
    """Reset connection metrics to initial state."""
    connection_monitor.reset_metrics()


async def diagnose_connection_issues(config: Optional[dict] = None) -> Dict:
    """
    Perform comprehensive connection diagnostics.
    
    Args:
        config: Optional connection configuration
        
    Returns:
        Dictionary containing diagnostic results
    """
    if config is None:
        try:
            from ..settings import get_config
            config = get_config()
        except:
            config = {'nats_url': DEFAULT_NATS_URL}
    
    nats_url = config.get('nats_url', DEFAULT_NATS_URL)
    
    diagnostics = {
        'nats_url': nats_url,
        'timestamp': time.time(),
        'tests': {}
    }
    
    # Basic connectivity test
    logger.info("Running NATS connection diagnostics...")
    
    try:
        connectivity_start = time.time()
        is_reachable = await test_nats_connection(config, timeout=10.0)
        connectivity_time = time.time() - connectivity_start
        
        diagnostics['tests']['connectivity'] = {
            'status': 'pass' if is_reachable else 'fail',
            'duration': connectivity_time,
            'message': 'Connection successful' if is_reachable else 'Connection failed'
        }
        
        if is_reachable:
            # Ping test
            try:
                ping_time = await ping_nats_server(config, timeout=5.0)
                diagnostics['tests']['ping'] = {
                    'status': 'pass',
                    'duration': ping_time,
                    'message': f'Ping successful ({ping_time*1000:.1f}ms)'
                }
            except Exception as e:
                diagnostics['tests']['ping'] = {
                    'status': 'fail',
                    'duration': 0,
                    'message': f'Ping failed: {e}'
                }
            
            # Server info test
            try:
                server_info = await get_nats_server_info(config)
                diagnostics['tests']['server_info'] = {
                    'status': 'pass',
                    'data': server_info,
                    'message': f'Server: {server_info.get("server_name", "unknown")} v{server_info.get("version", "unknown")}'
                }
            except Exception as e:
                diagnostics['tests']['server_info'] = {
                    'status': 'fail',
                    'message': f'Server info failed: {e}'
                }
        
    except Exception as e:
        diagnostics['tests']['connectivity'] = {
            'status': 'error',
            'duration': 0,
            'message': f'Diagnostic error: {e}'
        }
    
    # Connection metrics
    metrics = get_connection_metrics()
    diagnostics['metrics'] = {
        'total_connections': metrics.total_connections,
        'successful_connections': metrics.successful_connections,
        'failed_connections': metrics.failed_connections,
        'average_connection_time': metrics.average_connection_time,
        'connection_success_rate': (
            metrics.successful_connections / max(1, metrics.total_connections) * 100
        )
    }
    
    return diagnostics