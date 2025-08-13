# Subtask 04: Identify Connection Patterns

## Overview
Systematically identify and catalog all 44+ instances of NATS connection usage patterns throughout the codebase to prepare for migration.

## Objectives
- Find all files using NATS connections
- Categorize connection patterns by type
- Document current error handling behavior
- Create migration priority list
- Baseline current performance characteristics

## Implementation Details

### Analysis Commands to Execute

#### 1. Find All Connection Usage
```bash
# Find all NATS connection function calls
grep -r "get_nats_connection\|get_jetstream_context" src/naq --exclude-dir=__pycache__ --line-number > connection_usage_analysis.txt

# Find all direct NATS connection usage
grep -r "await.*connect\|\.jetstream\(\)" src/naq --exclude-dir=__pycache__ --line-number >> connection_usage_analysis.txt

# Find all connection cleanup calls
grep -r "close_nats_connection\|\.close\(\)" src/naq --exclude-dir=__pycache__ --line-number >> connection_usage_analysis.txt
```

#### 2. Pattern Analysis Script
Create `scripts/analyze_connection_patterns.py`:

```python
import re
import ast
from pathlib import Path
from typing import Dict, List, Tuple
from dataclasses import dataclass

@dataclass
class ConnectionPattern:
    file_path: str
    line_number: int
    pattern_type: str
    complexity: str
    error_handling: str
    description: str

def analyze_connection_patterns():
    """Analyze all connection patterns in the codebase."""
    patterns = []
    src_dir = Path("src/naq")
    
    for py_file in src_dir.rglob("*.py"):
        if py_file.name == "__init__.py":
            continue
            
        with open(py_file, 'r') as f:
            content = f.read()
            lines = content.split('\n')
            
        # Analyze each line for connection patterns
        for line_num, line in enumerate(lines, 1):
            if re.search(r'get_nats_connection|get_jetstream_context', line):
                pattern = analyze_connection_line(py_file, line_num, line)
                if pattern:
                    patterns.append(pattern)
    
    return patterns

def analyze_connection_line(file_path: Path, line_num: int, line: str) -> ConnectionPattern:
    """Analyze a single line for connection pattern."""
    pattern_type = classify_connection_pattern(line)
    complexity = assess_complexity(line)
    error_handling = assess_error_handling(file_path, line_num)
    
    return ConnectionPattern(
        file_path=str(file_path),
        line_number=line_num,
        pattern_type=pattern_type,
        complexity=complexity,
        error_handling=error_handling,
        description=line.strip()
    )

def classify_connection_pattern(line: str) -> str:
    """Classify the type of connection pattern."""
    if 'get_nats_connection' in line and 'get_jetstream_context' in line:
        return "COMBINED_NATS_JETSTREAM"
    elif 'get_nats_connection' in line:
        return "SIMPLE_NATS"
    elif 'get_jetstream_context' in line:
        return "JETSTREAM_ONLY"
    elif 'nats.connect' in line:
        return "DIRECT_CONNECTION"
    elif 'kv_store' in line or 'key_value' in line:
        return "KV_STORE"
    else:
        return "UNKNOWN"

def assess_complexity(line: str) -> str:
    """Assess complexity of connection usage."""
    if 'try:' in line or 'except' in line:
        return "HIGH"
    elif 'async with' in line:
        return "LOW"
    elif 'finally:' in line:
        return "MEDIUM"
    else:
        return "UNKNOWN"

def assess_error_handling(file_path: Path, line_num: int) -> str:
    """Assess error handling around connection."""
    # Look at surrounding lines for error handling patterns
    return "TO_BE_ANALYZED"
```

### Pattern Categories to Identify

#### Category A: Simple Connection + Operation
```python
# Pattern: Simple publish/subscribe
nc = await get_nats_connection(url)  
await nc.publish(subject, data)
await close_nats_connection(nc)

# Files likely affected:
# - src/naq/cli/ (command operations)
# - src/naq/utils.py (utility functions)
# - src/naq/results.py (result publishing)
```

#### Category B: JetStream Operations
```python
# Pattern: Stream/Consumer management
nc = await get_nats_connection(url)
js = await get_jetstream_context(nc)
await js.add_stream(config)
await close_nats_connection(nc)

# Files likely affected:
# - src/naq/queue/core.py
# - src/naq/queue/scheduled.py
# - src/naq/scheduler.py
```

#### Category C: KV Store Operations
```python
# Pattern: KeyValue operations
nc = await get_nats_connection(url)
js = await get_jetstream_context(nc)
kv = await js.key_value(bucket)
await kv.put(key, value)
await close_nats_connection(nc)

# Files likely affected:
# - src/naq/worker/status.py
# - src/naq/scheduler.py
# - src/naq/events/
```

#### Category D: Complex Connection Handling
```python
# Pattern: Multiple operations with connection reuse
nc = await get_nats_connection(url)
try:
    js = await get_jetstream_context(nc)
    # Multiple operations
    await js.publish(stream, msg1)
    await js.publish(stream, msg2)
    # More operations...
finally:
    await close_nats_connection(nc)

# Files likely affected:
# - src/naq/worker/core.py
# - src/naq/queue/batch.py
# - src/naq/events/
```

### Migration Priority Assessment

#### High Priority (Immediate Migration)
1. **Queue operations** - High frequency, performance critical
2. **Worker job processing** - Core functionality
3. **Event logging** - System-wide usage

#### Medium Priority (Batch Migration)
1. **Scheduler operations** - Moderate frequency
2. **Status management** - Important but less critical
3. **Result handling** - Moderate usage

#### Low Priority (Final Migration)
1. **CLI commands** - User-facing, less performance critical
2. **Utility functions** - Infrequent usage
3. **Dashboard operations** - Optional features

### Documentation to Create

#### 1. Connection Usage Inventory
Create `docs/connection_usage_inventory.md`:

```markdown
# NATS Connection Usage Inventory

## Summary
- Total connection usage locations: [count]
- Pattern distribution:
  - Simple NATS: [count]
  - JetStream: [count] 
  - KV Store: [count]
  - Complex: [count]

## Files by Priority

### High Priority Files
1. `src/naq/queue/core.py` - [description]
2. `src/naq/worker/core.py` - [description]
3. `src/naq/events/` - [description]

### Medium Priority Files
1. `src/naq/scheduler.py` - [description]
2. `src/naq/queue/scheduled.py` - [description]
3. `src/naq/worker/status.py` - [description]

### Low Priority Files
1. `src/naq/cli/` - [description]
2. `src/naq/utils.py` - [description]
3. `src/naq/results.py` - [description]
```

#### 2. Pattern Migration Guide
Create `docs/pattern_migration_guide.md`:

```markdown
# Connection Pattern Migration Guide

## Pattern A: Simple Connection + Operation
### Before
```python
nc = await get_nats_connection(url)  
await nc.publish(subject, data)
await close_nats_connection(nc)
```

### After
```python
async with nats_connection() as conn:
    await conn.publish(subject, data)
```

## Pattern B: JetStream Operations
### Before
```python
nc = await get_nats_connection(url)
js = await get_jetstream_context(nc)
await js.add_stream(config)
await close_nats_connection(nc)
```

### After
```python
async with nats_jetstream() as (conn, js):
    await js.add_stream(config)
```

## Pattern C: KV Store Operations
### Before
```python
nc = await get_nats_connection(url)
js = await get_jetstream_context(nc)
kv = await js.key_value(bucket)
await kv.put(key, value)
await close_nats_connection(nc)
```

### After
```python
async with nats_kv_store(bucket) as kv:
    await kv.put(key, value)
```
```

### Requirements
- Comprehensive inventory of all connection usage locations
- Clear categorization of patterns by complexity
- Migration priority assessment
- Pattern-specific migration examples
- Baseline performance measurements
- Error handling behavior documentation

## Success Criteria
- [ ] All 44+ connection usage locations identified and cataloged
- [ ] Patterns categorized by type and complexity
- [ ] Migration priority list created
- [ ] Pattern migration guide documented
- [ ] Baseline performance metrics established
- [ ] Error handling behavior documented

## Dependencies
- Access to full source code in `src/naq/`
- Existing connection function implementations
- Performance measurement tools

## Estimated Time
- 4-6 hours