### Sub-task 6: Implement Service Usage Patterns
**Description:** Implement the defined service usage patterns for short-lived operations (e.g., CLI commands) and long-lived components (e.g., Worker, Queue, Scheduler), and ensure backward compatibility for synchronous APIs.
**Implementation Steps:**
- For short-lived operations, implement the pattern:
    ```python
    async def cli_operation():
        config = get_config()
        async with ServiceManager(config) as services:
            job_service = await services.get_service(JobService)
            result = await job_service.some_operation()
            return result
    ```
- For long-lived components, implement the pattern:
    ```python
    class LongLivedComponent:
        def __init__(self, config: NAQConfig):
            self.config = config
            self._services: Optional[ServiceManager] = None
        
        async def start(self):
            self._services = ServiceManager(self.config)
            await self._services.initialize()
        
        async def operation(self):
            if not self._services:
                raise RuntimeError("Component not started")
            
            job_service = await self._services.get_service(JobService)
            return await job_service.some_operation()
        
        async def stop(self):
            if self._services:
                await self._services.cleanup()
    ```
- For synchronous API compatibility, implement the pattern:
    ```python
    def sync_operation(*args, **kwargs):
        """Sync wrapper maintaining backward compatibility."""
        config = get_config()
        
        async def async_impl():
            async with ServiceManager(config) as services:
                job_service = await services.get_service(JobService)
                return await job_service.operation(*args, **kwargs)
        
        return asyncio.run(async_impl())
    ```
**Success Criteria:**
- Service usage patterns are correctly implemented in relevant parts of the codebase.
- Short-lived operations correctly utilize the `ServiceManager` context.
- Long-lived components correctly initialize, use, and clean up `ServiceManager`.
- Synchronous API wrappers maintain backward compatibility.
**Testing:**
- Unit tests for short-lived operation patterns to ensure correct service lifecycle.
- Unit tests for long-lived components to verify `start`, `operation`, and `stop` methods.
- Integration tests for synchronous API wrappers to confirm they function correctly.
**Documentation:**
- Update relevant sections in `docs/quickstart.qmd` and `docs/advanced.qmd` with these service usage patterns.
- Ensure clear explanations of when to use each pattern.