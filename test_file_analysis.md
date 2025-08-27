# Test File Analysis Report

## Summary
- Total Test Files: 59
- Total Test Classes/Functions: 700
- Total Potential Issues: 108
- Critical Issues: 29
- High Priority Issues: 48
- Medium Priority Issues: 31
- Low Priority Issues: 0

## Issue Categories

### Critical Priority (29 issues)
| File | Type | Description |
|------|------|-------------|
| tests/test_integration/test_smoke_job.py | ParseError | Could not parse file: cannot access local variable 'item' where it is not associated with a value |
| tests/test_integration/test_smoke_worker.py | ConnectionService | Uses ConnectionService - may require proper setup |
| tests/test_integration/test_integration_kv_simple.py | ConnectionService | Uses ConnectionService - may require proper setup |
| tests/test_integration/test_config_service_integration.py | ServiceManager | Uses ServiceManager - may require proper setup |
| tests/test_integration/test_config_service_integration.py | ConnectionService | Uses ConnectionService - may require proper setup |
| tests/test_integration/test_scenario_worker.py | ParseError | Could not parse file: cannot access local variable 'item' where it is not associated with a value |
| tests/test_integration/test_integration_worker.py | ParseError | Could not parse file: cannot access local variable 'item' where it is not associated with a value |
| tests/test_integration/test_centralized_config_integration.py | ServiceManager | Uses ServiceManager - may require proper setup |
| tests/test_integration/test_centralized_config_integration.py | ConnectionService | Uses ConnectionService - may require proper setup |
| tests/test_integration/test_integration_jobs.py | ParseError | Could not parse file: cannot access local variable 'item' where it is not associated with a value |
| tests/test_integration/test_integration_kv_stores.py | ParseError | Could not parse file: cannot access local variable 'item' where it is not associated with a value |
| tests/test_integration/test_scenario_job.py | ParseError | Could not parse file: cannot access local variable 'item' where it is not associated with a value |
| tests/test_services/test_unit_kv_stores.py | ConnectionService | Uses ConnectionService - may require proper setup |
| tests/test_services/test_deprecation_warnings.py | ParseError | Could not parse file: cannot access local variable 'item' where it is not associated with a value |
| tests/test_services/test_error_handling_consistency.py | ConnectionService | Uses ConnectionService - may require proper setup |
| tests/test_services/test_unit_jobs.py | ParseError | Could not parse file: cannot access local variable 'item' where it is not associated with a value |
| tests/test_services/test_service_context.py | ServiceManager | Uses ServiceManager - may require proper setup |
| tests/test_services/test_service_context.py | ConnectionService | Uses ConnectionService - may require proper setup |
| tests/test_services/test_long_lived_components.py | ServiceManager | Uses ServiceManager - may require proper setup |
| tests/test_services/test_unit_failed_job_handler.py | ParseError | Could not parse file: cannot access local variable 'item' where it is not associated with a value |
| tests/test_services/test_connection_monitoring_health.py | ConnectionService | Uses ConnectionService - may require proper setup |
| tests/test_services/test_unit_worker_status.py | ServiceManager | Uses ServiceManager - may require proper setup |
| tests/test_cli/test_commands.py | ParseError | Could not parse file: cannot access local variable 'item' where it is not associated with a value |
| tests/test_performance/test_event_overhead.py | ParseError | Could not parse file: cannot access local variable 'item' where it is not associated with a value |
| tests/test_performance/test_event_logging_overhead.py | ParseError | Could not parse file: cannot access local variable 'item' where it is not associated with a value |
| tests/test_performance/test_memory_leaks.py | ParseError | Could not parse file: cannot access local variable 'item' where it is not associated with a value |
| tests/test_performance/test_service_layer_overhead.py | ParseError | Could not parse file: cannot access local variable 'item' where it is not associated with a value |
| tests/test_compatibility/test_user_workflows.py | ParseError | Could not parse file: cannot access local variable 'item' where it is not associated with a value |
| tests/test_compatibility/test_imports.py | ParseError | Could not parse file: cannot access local variable 'item' where it is not associated with a value |

### High Priority (48 issues)
| File | Type | Description |
|------|------|-------------|
| tests/test_integration/test_config_error_handling.py | ExternalDependency | May have external dependencies that need setup |
| tests/test_integration/test_smoke_worker.py | Async | Contains async tests - may require async test runner |
| tests/test_integration/test_smoke_worker.py | ExternalDependency | May have external dependencies that need setup |
| tests/test_integration/test_config_env_interpolation.py | ExternalDependency | May have external dependencies that need setup |
| tests/test_integration/test_integration_kv_simple.py | Async | Contains async tests - may require async test runner |
| tests/test_integration/test_integration_kv_simple.py | ExternalDependency | May have external dependencies that need setup |
| tests/test_integration/test_config_service_integration.py | Async | Contains async tests - may require async test runner |
| tests/test_integration/test_config_service_integration.py | ExternalDependency | May have external dependencies that need setup |
| tests/test_integration/test_config_schema_validation.py | ExternalDependency | May have external dependencies that need setup |
| tests/test_integration/test_event_cli_commands.py | Async | Contains async tests - may require async test runner |
| tests/test_integration/test_event_cli_commands.py | ExternalDependency | May have external dependencies that need setup |
| tests/test_integration/test_sync_api_integration.py | ExternalDependency | May have external dependencies that need setup |
| tests/test_integration/test_centralized_config_integration.py | Async | Contains async tests - may require async test runner |
| tests/test_integration/test_centralized_config_integration.py | ExternalDependency | May have external dependencies that need setup |
| tests/test_models/test_jobs.py | Async | Contains async tests - may require async test runner |
| tests/test_services/test_unit_nats_connection.py | Async | Contains async tests - may require async test runner |
| tests/test_services/test_unit_nats_connection.py | ExternalDependency | May have external dependencies that need setup |
| tests/test_services/test_unit_connection_monitor.py | ExternalDependency | May have external dependencies that need setup |
| tests/test_services/test_unit_kv_stores.py | Async | Contains async tests - may require async test runner |
| tests/test_services/test_unit_kv_stores.py | ExternalDependency | May have external dependencies that need setup |
| tests/test_services/test_async_helpers.py | Async | Contains async tests - may require async test runner |
| tests/test_services/test_error_handling.py | Async | Contains async tests - may require async test runner |
| tests/test_services/test_error_handling.py | ExternalDependency | May have external dependencies that need setup |
| tests/test_services/test_error_handling_consistency.py | Async | Contains async tests - may require async test runner |
| tests/test_services/test_error_handling_consistency.py | ExternalDependency | May have external dependencies that need setup |
| tests/test_services/test_service_context.py | Async | Contains async tests - may require async test runner |
| tests/test_services/test_service_context.py | ExternalDependency | May have external dependencies that need setup |
| tests/test_services/test_unit_connection_decorators.py | Async | Contains async tests - may require async test runner |
| tests/test_services/test_unit_connection_decorators.py | ExternalDependency | May have external dependencies that need setup |
| tests/test_services/test_unit_settings.py | ExternalDependency | May have external dependencies that need setup |
| tests/test_services/test_unit_worker.py | Async | Contains async tests - may require async test runner |
| tests/test_services/test_unit_worker.py | ExternalDependency | May have external dependencies that need setup |
| tests/test_services/test_unit_job.py | Async | Contains async tests - may require async test runner |
| tests/test_services/test_unit_queue.py | Async | Contains async tests - may require async test runner |
| tests/test_services/test_unit_queue.py | ExternalDependency | May have external dependencies that need setup |
| tests/test_services/test_long_lived_components.py | Async | Contains async tests - may require async test runner |
| tests/test_services/test_long_lived_components.py | ExternalDependency | May have external dependencies that need setup |
| tests/test_services/test_connection_monitoring_health.py | Async | Contains async tests - may require async test runner |
| tests/test_services/test_connection_monitoring_health.py | ExternalDependency | May have external dependencies that need setup |
| tests/test_services/test_nats_helpers.py | Async | Contains async tests - may require async test runner |
| tests/test_services/test_nats_helpers.py | ExternalDependency | May have external dependencies that need setup |
| tests/test_services/test_unit_worker_status.py | Async | Contains async tests - may require async test runner |
| tests/test_services/test_unit_worker_status.py | ExternalDependency | May have external dependencies that need setup |
| tests/test_services/test_types.py | ExternalDependency | May have external dependencies that need setup |
| tests/test_config/test_schema.py | ExternalDependency | May have external dependencies that need setup |
| tests/test_config/test_api.py | ExternalDependency | May have external dependencies that need setup |
| tests/test_config/test_config_types.py | ExternalDependency | May have external dependencies that need setup |
| tests/test_config/test_defaults.py | ExternalDependency | May have external dependencies that need setup |

### Medium Priority (31 issues)
| File | Type | Description |
|------|------|-------------|
| tests/test_integration/test_config_hot_reloading.py | Mocking | Uses mocking - check if mocks are properly configured |
| tests/test_integration/test_config_error_handling.py | Mocking | Uses mocking - check if mocks are properly configured |
| tests/test_integration/test_smoke_worker.py | Mocking | Uses mocking - check if mocks are properly configured |
| tests/test_integration/test_config_settings_compatibility.py | Mocking | Uses mocking - check if mocks are properly configured |
| tests/test_integration/test_config_env_interpolation.py | Mocking | Uses mocking - check if mocks are properly configured |
| tests/test_integration/test_config_loading_priority.py | Mocking | Uses mocking - check if mocks are properly configured |
| tests/test_integration/test_config_service_integration.py | Mocking | Uses mocking - check if mocks are properly configured |
| tests/test_integration/test_config_schema_validation.py | Mocking | Uses mocking - check if mocks are properly configured |
| tests/test_integration/test_event_cli_commands.py | Mocking | Uses mocking - check if mocks are properly configured |
| tests/test_integration/test_integration_job.py | Mocking | Uses mocking - check if mocks are properly configured |
| tests/test_integration/test_sync_api_integration.py | Mocking | Uses mocking - check if mocks are properly configured |
| tests/test_integration/test_centralized_config_integration.py | Mocking | Uses mocking - check if mocks are properly configured |
| tests/test_integration/test_config_cli_commands.py | Mocking | Uses mocking - check if mocks are properly configured |
| tests/test_services/test_timing.py | Mocking | Uses mocking - check if mocks are properly configured |
| tests/test_services/test_unit_nats_connection.py | Mocking | Uses mocking - check if mocks are properly configured |
| tests/test_services/test_unit_kv_stores.py | Mocking | Uses mocking - check if mocks are properly configured |
| tests/test_services/test_async_helpers.py | Mocking | Uses mocking - check if mocks are properly configured |
| tests/test_services/test_error_handling.py | Mocking | Uses mocking - check if mocks are properly configured |
| tests/test_services/test_error_handling_consistency.py | Mocking | Uses mocking - check if mocks are properly configured |
| tests/test_services/test_service_context.py | Mocking | Uses mocking - check if mocks are properly configured |
| tests/test_services/test_unit_connection_decorators.py | Mocking | Uses mocking - check if mocks are properly configured |
| tests/test_services/test_unit_settings.py | Mocking | Uses mocking - check if mocks are properly configured |
| tests/test_services/test_unit_worker.py | Mocking | Uses mocking - check if mocks are properly configured |
| tests/test_services/test_unit_queue.py | Mocking | Uses mocking - check if mocks are properly configured |
| tests/test_services/test_long_lived_components.py | Mocking | Uses mocking - check if mocks are properly configured |
| tests/test_services/test_connection_monitoring_health.py | Mocking | Uses mocking - check if mocks are properly configured |
| tests/test_services/test_nats_helpers.py | Mocking | Uses mocking - check if mocks are properly configured |
| tests/test_services/test_sync_api.py | Mocking | Uses mocking - check if mocks are properly configured |
| tests/test_services/test_unit_worker_status.py | Mocking | Uses mocking - check if mocks are properly configured |
| tests/test_config/test_loader.py | Mocking | Uses mocking - check if mocks are properly configured |
| tests/test_config/test_api.py | Mocking | Uses mocking - check if mocks are properly configured |

### Low Priority (0 issues)
No issues in this category.

## Files with Most Issues
- tests/test_integration/test_config_service_integration.py: 5 issues
- tests/test_integration/test_centralized_config_integration.py: 5 issues
- tests/test_services/test_service_context.py: 5 issues
- tests/test_integration/test_smoke_worker.py: 4 issues
- tests/test_services/test_unit_kv_stores.py: 4 issues
- tests/test_services/test_error_handling_consistency.py: 4 issues
- tests/test_services/test_long_lived_components.py: 4 issues
- tests/test_services/test_connection_monitoring_health.py: 4 issues
- tests/test_services/test_unit_worker_status.py: 4 issues
- tests/test_integration/test_integration_kv_simple.py: 3 issues

## Recommendations

### Immediate Actions Required
Address critical issues first:
- tests/test_integration/test_smoke_job.py: Could not parse file: cannot access local variable 'item' where it is not associated with a value
- tests/test_integration/test_smoke_worker.py: Uses ConnectionService - may require proper setup
- tests/test_integration/test_integration_kv_simple.py: Uses ConnectionService - may require proper setup
- tests/test_integration/test_config_service_integration.py: Uses ServiceManager - may require proper setup
- tests/test_integration/test_config_service_integration.py: Uses ConnectionService - may require proper setup

### General Recommendations
1. Set up proper ServiceManager and ConnectionService fixtures
2. Configure external dependencies (database, Redis) for tests
3. Ensure async tests are properly handled
4. Review and update mock configurations
5. Consider using test factories for complex object creation

## Test Structure Analysis
- Class-based tests: 80
- Function-based tests: 620

### Most Common Imports
- pytest: 57 files
- unittest.mock.patch: 30 files
- naq.models.jobs.Job: 28 files
- asyncio: 24 files
- typing.Any: 21 files
- time: 21 files
- unittest.mock.AsyncMock: 20 files
- unittest.mock.MagicMock: 20 files
- typing.Dict: 18 files
- naq.services.kv_stores.KVStoreService: 17 files