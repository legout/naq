# Subtask 05.01: Create Service Package Structure

## Overview
Create the basic directory structure and files for the new service layer package to support centralized service management and eliminate code duplication.

## Objectives
- Create the services package directory structure
- Set up basic module files with proper imports
- Prepare for implementation of base service classes and service manager
- Establish foundation for service layer architecture

## Implementation Steps

### 1. Create Package Directory
```bash
mkdir -p src/naq/services
```

### 2. Create Module Files
Create the following empty module files:
- `src/naq/services/__init__.py` - Service layer public API
- `src/naq/services/base.py` - Base service classes and utilities
- `src/naq/services/connection.py` - NATS connection management
- `src/naq/services/streams.py` - JetStream stream operations
- `src/naq/services/kv_stores.py` - KeyValue store operations
- `src/naq/services/jobs.py` - Job execution services
- `src/naq/services/events.py` - Event logging services
- `src/naq/services/scheduler.py` - Scheduler operations

### 3. Set Up Basic Imports
Add initial import structure to prepare for service implementation:
- `__init__.py` should export main service classes
- Each module should have proper imports from other naq modules
- Ensure no circular imports are introduced
- Import necessary third-party dependencies (nats, etc.)

### 4. Verify Structure
Confirm the following structure exists:
```
src/naq/services/
├── __init__.py
├── base.py
├── connection.py
├── streams.py
├── kv_stores.py
├── jobs.py
├── events.py
└── scheduler.py
```

### 5. Create Initial Service Registry
Set up basic service registration mechanism in `__init__.py`:
- Import all service classes
- Define service registry structure
- Prepare for service discovery

## Success Criteria
- [ ] Services package directory structure created
- [ ] All module files exist with proper basic imports
- [ ] No syntax errors in the basic structure
- [ ] Ready for base service class implementation in subsequent subtasks
- [ ] Service registry foundation established

## Dependencies
- **Depends on**: Tasks 01-04 (Split packages) - services will use split components
- **Prepares for**: Subtasks 05.02-05.08 (service implementations)

## Estimated Time
- **Implementation**: 45 minutes
- **Testing**: 15 minutes
- **Total**: 1 hour