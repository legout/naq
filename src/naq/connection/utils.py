from typing import Dict, List, Optional
import asyncio
from dataclasses import dataclass
from loguru import logger

from .context_managers import nats_connection, Config


@dataclass
class ConnectionMetrics:
    """Connection usage metrics."""
    total_connections: int = 0
    active_connections: int = 0
    failed_connections: int = 0
    average_connection_time: float = 0.0


class ConnectionMonitor:
    """Monitor connection usage and performance."""
    
    def __init__(self):
        self.metrics = ConnectionMetrics()
        self._connection_times: List[float] = []
    
    def record_connection_start(self):
        """Record connection start."""
        self.metrics.total_connections += 1
        self.metrics.active_connections += 1
    
    def record_connection_end(self, duration: float):
        """Record connection end."""
        self.metrics.active_connections -= 1
        self._connection_times.append(duration)
        self.metrics.average_connection_time = sum(self._connection_times) / len(self._connection_times)
    
    def record_connection_failure(self):
        """Record connection failure."""
        self.metrics.failed_connections += 1


# Global connection monitor
connection_monitor = ConnectionMonitor()


async def test_nats_connection(config: Optional[Config] = None) -> bool:
    """Test NATS connection health."""
    try:
        async with nats_connection(config) as conn:
            # Simple ping test
            await conn.flush(timeout=5)
            return True
    except Exception as e:
        logger.error(f"NATS connection test failed: {e}")
        return False


async def wait_for_nats_connection(config: Optional[Config] = None, timeout: int = 30) -> bool:
    """Wait for NATS connection to be available."""
    start_time = asyncio.get_event_loop().time()
    
    while (asyncio.get_event_loop().time() - start_time) < timeout:
        if await test_nats_connection(config):
            return True
        await asyncio.sleep(1.0)
    
    return False