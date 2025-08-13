# Subtask 05.02: Implement Base Service Classes

## Overview
Implement the foundational base service classes and service manager that will provide the consistent interface and lifecycle management for all services in the service layer.

## Objectives
- Implement `BaseService` abstract base class
- Implement `ServiceManager` for service instance and dependency management
- Establish consistent service lifecycle patterns
- Provide context manager support for all services
- Create service registry and dependency injection system

## Implementation Steps

### 1. Implement BaseService Abstract Class
Create the abstract base class that all services will inherit from:

```python
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, Type
import asyncio
from contextlib import asynccontextmanager

class BaseService(ABC):
    """Base class for all NAQ services."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self._initialized = False
        self._logger = None  # Will be set by ServiceManager
        
    async def initialize(self) -> None:
        """Initialize the service."""
        if not self._initialized:
            await self._do_initialize()
            self._initialized = True
            
    @abstractmethod
    async def _do_initialize(self) -> None:
        """Implement service-specific initialization."""
        
    async def cleanup(self) -> None:
        """Cleanup service resources."""
        if self._initialized:
            await self._do_cleanup()
            self._initialized = False
            
    async def _do_cleanup(self) -> None:
        """Override to implement service-specific cleanup."""
        pass
        
    async def __aenter__(self):
        await self.initialize()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cleanup()
        
    @property
    def is_initialized(self) -> bool:
        """Check if service is initialized."""
        return self._initialized
```

### 2. Implement ServiceManager
Create the service manager that handles service instances and dependencies:

```python
class ServiceManager:
    """Manages service instances and dependencies."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self._services: Dict[str, BaseService] = {}
        self._service_types: Dict[str, Type[BaseService]] = {}
        self._logger = None  # Will be initialized
        
    def register_service(self, name: str, service_type: Type[BaseService]) -> None:
        """Register a service type."""
        self._service_types[name] = service_type
        
    async def get_service(self, service_type: Type[BaseService]) -> BaseService:
        """Get or create service instance by type."""
        service_name = self._get_service_name(service_type)
        return await self.get_service_by_name(service_name)
        
    async def get_service_by_name(self, name: str) -> BaseService:
        """Get or create service instance by name."""
        if name not in self._services:
            if name not in self._service_types:
                raise ValueError(f"Unknown service type: {name}")
            
            service_type = self._service_types[name]
            service = service_type(self.config)
            await service.initialize()
            self._services[name] = service
            
        return self._services[name]
        
    def _get_service_name(self, service_type: Type[BaseService]) -> str:
        """Get service name from type."""
        # Simple mapping - can be enhanced with decorators or registry
        service_name_map = {
            ConnectionService: "connection",
            StreamService: "streams",
            KVStoreService: "kv_stores",
            JobService: "jobs",
            EventService: "events",
            SchedulerService: "scheduler",
        }
        
        service_name = service_name_map.get(service_type)
        if service_name is None:
            raise ValueError(f"Unknown service type: {service_type}")
            
        return service_name
        
    async def cleanup_all(self) -> None:
        """Cleanup all services."""
        for service in self._services.values():
            await service.cleanup()
        self._services.clear()
        
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cleanup_all()
```

### 3. Add Service Registration Decorator
Create a decorator for easy service registration:

```python
def service(name: str):
    """Decorator to register a service."""
    def decorator(service_class: Type[BaseService]):
        # Store service name for registration
        service_class._service_name = name
        return service_class
    return decorator
```

### 4. Implement Error Handling Base
Add base error handling patterns:

```python
class ServiceError(Exception):
    """Base exception for service errors."""
    pass

class ServiceInitializationError(ServiceError):
    """Service initialization failed."""
    pass

class ServiceCleanupError(ServiceError):
    """Service cleanup failed."""
    pass
```

### 5. Update Service Imports
Update `__init__.py` to export base classes:

```python
from .base import BaseService, ServiceManager, ServiceError, ServiceInitializationError, ServiceCleanupError

__all__ = [
    "BaseService",
    "ServiceManager", 
    "ServiceError",
    "ServiceInitializationError",
    "ServiceCleanupError",
]
```

### 6. Create Service Configuration Schema
Define base configuration structure:

```python
from typing import Dict, Any, Optional

class ServiceConfig:
    """Base service configuration."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.nats_url = config.get("nats_url", "nats://localhost:4222")
        self.timeout = config.get("timeout", 30)
        self.retry_attempts = config.get("retry_attempts", 3)
        self.logger_config = config.get("logger", {})
```

## Success Criteria
- [ ] BaseService abstract class implemented with proper lifecycle methods
- [ ] ServiceManager implemented with service registration and dependency injection
- [ ] Context manager support working for all services
- [ ] Service registration system functional
- [ ] Error handling base classes established
- [ ] Basic service configuration schema defined
- [ ] No circular import issues

## Dependencies
- **Depends on**: Subtask 05.01 (Create Service Package Structure)
- **Prepares for**: Subtask 05.03 (Implement ConnectionService)

## Estimated Time
- **Implementation**: 2-3 hours
- **Testing**: 1 hour
- **Total**: 3-4 hours