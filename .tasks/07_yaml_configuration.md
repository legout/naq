# Task 07: Implement YAML Configuration System

## Overview
Implement a comprehensive YAML configuration system that supports hierarchical configuration loading (YAML files → environment variables → defaults) to replace the current environment-variable-only configuration approach.

## Current State
- Configuration only through environment variables
- Settings scattered across multiple files
- No validation or schema enforcement
- No configuration file support
- Limited configuration documentation

## Goals
- YAML configuration file support with multiple file locations
- Configuration hierarchy: YAML > Environment Variables > Defaults
- Configuration validation and schema enforcement
- Backward compatibility with existing environment variables
- Hot-reloading support for configuration changes
- Clear documentation and examples

## Target Configuration Structure

### File Locations (Priority Order)
1. **Command-line specified**: `--config /path/to/config.yaml`
2. **Current directory**: `./naq.yaml` or `./naq.yml` 
3. **User config**: `~/.naq/config.yaml`
4. **System config**: `/etc/naq/config.yaml`
5. **Environment variables**: `NAQ_*` variables (backward compatibility)
6. **Built-in defaults**: Hardcoded sensible defaults

### YAML Configuration Schema
```yaml
# Complete NAQ configuration example
nats:
  servers:
    - "nats://localhost:4222"
  client_name: "naq-client"
  max_reconnect_attempts: 5
  reconnect_time_wait: 2.0
  connection_timeout: 10.0
  drain_timeout: 30.0
  
  # Authentication (optional)
  auth:
    username: ""
    password: ""
    token: ""
    
  # TLS settings (optional)
  tls:
    enabled: false
    cert_file: ""
    key_file: ""
    ca_file: ""

queues:
  default: "naq_default_queue"
  prefix: "naq"
  
  # Queue-specific settings
  configs:
    high_priority:
      ack_wait: 30
      max_deliver: 3
    low_priority:
      ack_wait: 60
      max_deliver: 5

workers:
  concurrency: 10
  heartbeat_interval: 15
  ttl: 60
  
  # Worker behavior
  max_job_duration: 3600  # 1 hour
  shutdown_timeout: 30
  
  # Worker pools (optional)
  pools:
    cpu_intensive:
      concurrency: 2
      queues: ["compute"]
    io_intensive:
      concurrency: 20
      queues: ["io", "network"]

scheduler:
  enabled: true
  lock_ttl: 30
  lock_renew_interval: 15
  max_failures: 5
  scan_interval: 1.0

events:
  enabled: true
  batch_size: 100
  flush_interval: 5.0
  max_buffer_size: 10000
  
  # Event stream configuration
  stream:
    name: "NAQ_JOB_EVENTS"
    max_age: "168h"  # 7 days
    max_bytes: "1GB"
    replicas: 1
  
  # Event filtering
  filters:
    exclude_heartbeats: true
    min_job_duration: 0  # ms
    
results:
  ttl: 604800  # 7 days
  cleanup_interval: 3600  # 1 hour

serialization:
  job_serializer: "pickle"  # or "json"
  compression: false
  
  # JSON-specific settings
  json:
    encoder: "json.JSONEncoder" 
    decoder: "json.JSONDecoder"

logging:
  level: "INFO"
  format: "json"  # or "text"
  file: ""  # empty = stdout
  max_size: "100MB"
  backup_count: 5

# Environment-specific overrides
environments:
  development:
    logging:
      level: "DEBUG"
    events:
      batch_size: 10
      
  production:
    nats:
      servers:
        - "nats://nats-1.prod:4222"
        - "nats://nats-2.prod:4222"
        - "nats://nats-3.prod:4222"
    workers:
      concurrency: 50
    events:
      batch_size: 500
      flush_interval: 1.0

  testing:
    nats:
      servers: ["nats://localhost:4222"]
    events:
      enabled: false
```

## Implementation Architecture

### Package Structure
```
src/naq/config/
├── __init__.py           # Public API
├── loader.py            # Configuration loading logic
├── schema.py            # Configuration validation schema
├── defaults.py          # Default configuration values
├── merger.py            # Configuration merging logic
├── validator.py         # Configuration validation
└── types.py             # Configuration type definitions
```

### Configuration Classes
```python
# types.py - Configuration data structures
from dataclasses import dataclass
from typing import List, Dict, Optional, Any

@dataclass
class NatsConfig:
    servers: List[str]
    client_name: str
    max_reconnect_attempts: int
    reconnect_time_wait: float
    connection_timeout: float
    drain_timeout: float
    auth: Optional[Dict[str, str]] = None
    tls: Optional[Dict[str, Any]] = None

@dataclass  
class WorkerConfig:
    concurrency: int
    heartbeat_interval: int
    ttl: int
    max_job_duration: int
    shutdown_timeout: int
    pools: Optional[Dict[str, Dict[str, Any]]] = None

@dataclass
class EventsConfig:
    enabled: bool
    batch_size: int
    flush_interval: float
    max_buffer_size: int
    stream: Dict[str, Any]
    filters: Dict[str, Any]

@dataclass
class NAQConfig:
    nats: NatsConfig
    workers: WorkerConfig
    events: EventsConfig
    queues: Dict[str, Any]
    scheduler: Dict[str, Any] 
    results: Dict[str, Any]
    serialization: Dict[str, Any]
    logging: Dict[str, Any]
    
    @property
    def environment(self) -> Optional[str]:
        return os.getenv('NAQ_ENVIRONMENT')
```

## Detailed Implementation

### 1. Configuration Loading (loader.py)
```python
import os
import yaml
from pathlib import Path
from typing import Optional, Dict, Any, List

class ConfigLoader:
    """Loads configuration from multiple sources with priority."""
    
    DEFAULT_CONFIG_PATHS = [
        Path.cwd() / "naq.yaml",
        Path.cwd() / "naq.yml", 
        Path.home() / ".naq" / "config.yaml",
        Path("/etc/naq/config.yaml"),
    ]
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = Path(config_path) if config_path else None
        
    def load_config(self) -> Dict[str, Any]:
        """Load configuration from all sources with proper priority."""
        config = self._load_defaults()
        
        # Load from files (lowest priority first)
        if self.config_path:
            # Explicit config file has highest priority among files
            config = self._merge_config(config, self._load_yaml_file(self.config_path))
        else:
            # Search default locations (reverse priority order)  
            for path in reversed(self.DEFAULT_CONFIG_PATHS):
                if path.exists():
                    file_config = self._load_yaml_file(path)
                    config = self._merge_config(config, file_config)
        
        # Environment variables override file configs
        env_config = self._load_env_variables()
        config = self._merge_config(config, env_config)
        
        # Apply environment-specific overrides
        config = self._apply_environment_overrides(config)
        
        return config
        
    def _load_yaml_file(self, path: Path) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(path, 'r') as f:
                content = f.read()
                
            # Support environment variable interpolation
            content = self._interpolate_env_vars(content)
            return yaml.safe_load(content) or {}
            
        except FileNotFoundError:
            return {}
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML in {path}: {e}")
        except Exception as e:
            raise ConfigurationError(f"Error loading config from {path}: {e}")
    
    def _interpolate_env_vars(self, content: str) -> str:
        """Replace ${VAR:default} patterns with environment variables."""
        import re
        
        def replace_env_var(match):
            var_expr = match.group(1)
            if ':' in var_expr:
                var_name, default = var_expr.split(':', 1)
            else:
                var_name, default = var_expr, ""
            return os.getenv(var_name.strip(), default.strip())
        
        return re.sub(r'\$\{([^}]+)\}', replace_env_var, content)
    
    def _load_env_variables(self) -> Dict[str, Any]:
        """Load configuration from environment variables."""
        env_config = {}
        
        # Map environment variables to config structure
        env_mappings = {
            'NAQ_NATS_URL': 'nats.servers[0]',
            'NAQ_DEFAULT_QUEUE': 'queues.default', 
            'NAQ_WORKER_CONCURRENCY': 'workers.concurrency',
            'NAQ_WORKER_HEARTBEAT_INTERVAL': 'workers.heartbeat_interval',
            'NAQ_WORKER_TTL': 'workers.ttl',
            'NAQ_EVENTS_ENABLED': 'events.enabled',
            'NAQ_EVENT_LOGGER_BATCH_SIZE': 'events.batch_size',
            'NAQ_EVENT_LOGGER_FLUSH_INTERVAL': 'events.flush_interval',
            'NAQ_DEFAULT_RESULT_TTL': 'results.ttl',
            'NAQ_JOB_SERIALIZER': 'serialization.job_serializer',
            'NAQ_LOG_LEVEL': 'logging.level',
        }
        
        for env_var, config_path in env_mappings.items():
            value = os.getenv(env_var)
            if value is not None:
                self._set_nested_value(env_config, config_path, self._convert_env_value(value))
                
        return env_config
    
    def _convert_env_value(self, value: str) -> Any:
        """Convert environment variable string to appropriate type."""
        # Boolean conversion
        if value.lower() in ('true', '1', 'yes', 'on'):
            return True
        elif value.lower() in ('false', '0', 'no', 'off'):
            return False
        
        # Numeric conversion
        try:
            if '.' in value:
                return float(value)
            else:
                return int(value)
        except ValueError:
            pass
        
        # String value
        return value
```

### 2. Configuration Schema (schema.py)
```python
from jsonschema import validate, ValidationError
from typing import Dict, Any

CONFIG_SCHEMA = {
    "type": "object",
    "properties": {
        "nats": {
            "type": "object", 
            "properties": {
                "servers": {
                    "type": "array",
                    "items": {"type": "string", "format": "uri"},
                    "minItems": 1
                },
                "client_name": {"type": "string"},
                "max_reconnect_attempts": {"type": "integer", "minimum": 0},
                "reconnect_time_wait": {"type": "number", "minimum": 0},
                "connection_timeout": {"type": "number", "minimum": 0}
            },
            "required": ["servers"],
            "additionalProperties": True
        },
        "workers": {
            "type": "object",
            "properties": {
                "concurrency": {"type": "integer", "minimum": 1},
                "heartbeat_interval": {"type": "integer", "minimum": 1},
                "ttl": {"type": "integer", "minimum": 1}
            },
            "required": ["concurrency"],
            "additionalProperties": True
        },
        "events": {
            "type": "object", 
            "properties": {
                "enabled": {"type": "boolean"},
                "batch_size": {"type": "integer", "minimum": 1},
                "flush_interval": {"type": "number", "minimum": 0.1},
                "max_buffer_size": {"type": "integer", "minimum": 1}
            },
            "additionalProperties": True
        }
    },
    "additionalProperties": True
}

class ConfigValidator:
    """Validates configuration against schema."""
    
    def __init__(self, schema: Dict[str, Any] = CONFIG_SCHEMA):
        self.schema = schema
        
    def validate(self, config: Dict[str, Any]) -> None:
        """Validate configuration against schema."""
        try:
            validate(instance=config, schema=self.schema)
        except ValidationError as e:
            raise ConfigurationError(f"Configuration validation failed: {e.message}")
    
    def validate_nats_servers(self, servers: List[str]) -> None:
        """Validate NATS server URLs."""
        for server in servers:
            if not server.startswith(('nats://', 'nats-tls://')):
                raise ConfigurationError(f"Invalid NATS server URL: {server}")
```

### 3. Configuration API (config/__init__.py)
```python
from typing import Optional
from functools import lru_cache
from .loader import ConfigLoader
from .types import NAQConfig
from .validator import ConfigValidator

# Global configuration instance
_config_instance: Optional[NAQConfig] = None

def load_config(config_path: Optional[str] = None, validate: bool = True) -> NAQConfig:
    """Load and return configuration."""
    global _config_instance
    
    if _config_instance is None:
        loader = ConfigLoader(config_path)
        config_dict = loader.load_config()
        
        if validate:
            validator = ConfigValidator()
            validator.validate(config_dict)
            
        _config_instance = NAQConfig.from_dict(config_dict)
    
    return _config_instance

def get_config() -> NAQConfig:
    """Get current configuration instance."""
    if _config_instance is None:
        return load_config()
    return _config_instance

def reload_config(config_path: Optional[str] = None) -> NAQConfig:
    """Reload configuration from sources."""
    global _config_instance
    _config_instance = None
    return load_config(config_path)

# Configuration context manager for testing
@contextmanager
def temp_config(config_dict: Dict[str, Any]):
    """Temporarily override configuration for testing."""
    global _config_instance
    original = _config_instance
    _config_instance = NAQConfig.from_dict(config_dict)
    try:
        yield _config_instance
    finally:
        _config_instance = original
```

## Integration with Existing Code

### Settings Module Integration
Update `settings.py` to use new configuration system while maintaining backward compatibility:

```python
# settings.py - Backward compatibility layer
import os
from .config import get_config

def _get_env_or_config(env_var: str, config_path: str, default: Any = None) -> Any:
    """Get value from environment or config with fallback."""
    # Environment variable takes precedence for backward compatibility
    env_value = os.getenv(env_var)
    if env_value is not None:
        return env_value
    
    # Get from configuration
    config = get_config()
    try:
        return config.get_nested(config_path)
    except (AttributeError, KeyError):
        return default

# Update existing settings to use configuration
DEFAULT_NATS_URL = _get_env_or_config('NAQ_NATS_URL', 'nats.servers[0]', 'nats://localhost:4222')
DEFAULT_QUEUE_NAME = _get_env_or_config('NAQ_DEFAULT_QUEUE', 'queues.default', 'naq_default_queue')
# ... other settings
```

### Service Integration
Services use configuration directly:

```python
# In service classes
class ConnectionService(BaseService):
    def __init__(self, config: NAQConfig):
        super().__init__(config)
        self.nats_config = config.nats
        
    async def get_connection(self):
        return await nats.connect(
            servers=self.nats_config.servers,
            name=self.nats_config.client_name,
            max_reconnect_attempts=self.nats_config.max_reconnect_attempts,
            # ... other config-driven parameters
        )
```

## CLI Integration

### Configuration Commands
Add CLI commands for configuration management:

```python
# In cli/system_commands.py
@system_app.command()
def config(
    show: bool = typer.Option(False, "--show", help="Show current configuration"),
    validate: bool = typer.Option(False, "--validate", help="Validate configuration"),
    config_file: Optional[str] = typer.Option(None, "--config", help="Configuration file path"),
):
    """Configuration management commands."""
    if show:
        config = load_config(config_file)
        console.print_json(config.to_dict())
    elif validate:
        try:
            load_config(config_file, validate=True)
            console.print("✅ Configuration is valid")
        except ConfigurationError as e:
            console.print(f"❌ Configuration error: {e}")
            raise typer.Exit(1)
```

### Configuration File Generation
```python
@system_app.command()
def generate_config(
    output: str = typer.Option("naq.yaml", "--output", "-o", help="Output file path"),
    environment: str = typer.Option("development", help="Environment template"),
):
    """Generate example configuration file."""
    template = get_config_template(environment)
    
    with open(output, 'w') as f:
        yaml.dump(template, f, default_flow_style=False, sort_keys=False)
    
    console.print(f"✅ Configuration template generated: {output}")
```

## Testing Strategy

### Configuration Loading Tests
- Test file discovery and priority
- Test environment variable override
- Test environment interpolation
- Test schema validation
- Test error handling

### Integration Tests  
- Test services use configuration correctly
- Test backward compatibility with env vars
- Test configuration hot-reloading
- Test CLI configuration commands

### Example Test
```python
def test_config_priority():
    """Test configuration loading priority."""
    with temp_config_file({
        'nats': {'servers': ['nats://file:4222']},
        'workers': {'concurrency': 5}
    }):
        with env_override('NAQ_WORKER_CONCURRENCY', '10'):
            config = load_config()
            assert config.nats.servers == ['nats://file:4222']  # From file
            assert config.workers.concurrency == 10  # From env var (higher priority)
```

## Documentation

### Configuration Reference
Create comprehensive configuration documentation:
- Complete YAML schema reference  
- Environment variable mapping
- Configuration examples for different environments
- Migration guide from env-only configuration

### Configuration Examples
```yaml
# examples/configs/development.yaml
nats:
  servers: ["nats://localhost:4222"]
workers:
  concurrency: 2
events:
  enabled: true
  batch_size: 10
logging:
  level: "DEBUG"

# examples/configs/production.yaml  
nats:
  servers:
    - "nats://nats-1.prod:4222"
    - "nats://nats-2.prod:4222"
workers:
  concurrency: 50
events:
  batch_size: 500
  flush_interval: 1.0
logging:
  level: "INFO"
  file: "/var/log/naq/naq.log"
```

## Migration Guide

### For Existing Users
1. **No Changes Required**: Environment variables continue to work
2. **Optional Migration**: Create YAML config files for complex setups
3. **Gradual Adoption**: Mix YAML and environment variables during transition

### Environment Variable Mapping
Document exact mapping between environment variables and YAML paths:

```
NAQ_NATS_URL              → nats.servers[0]
NAQ_DEFAULT_QUEUE         → queues.default
NAQ_WORKER_CONCURRENCY    → workers.concurrency
NAQ_EVENTS_ENABLED        → events.enabled
# ... complete mapping
```

## Files to Create

### New Files
- `src/naq/config/__init__.py`
- `src/naq/config/loader.py`
- `src/naq/config/schema.py`
- `src/naq/config/defaults.py`
- `src/naq/config/merger.py`
- `src/naq/config/validator.py`
- `src/naq/config/types.py`
- `examples/configs/development.yaml`
- `examples/configs/production.yaml`
- `examples/configs/testing.yaml`

### Files to Update
- `src/naq/settings.py` - Add backward compatibility layer
- `src/naq/cli/system_commands.py` - Add config commands
- All service classes - Use configuration objects
- Documentation files

## Success Criteria

### Functional Requirements
- [ ] YAML configuration loading works from all specified locations
- [ ] Configuration priority respected (YAML → env vars → defaults)
- [ ] Environment variable interpolation works correctly
- [ ] Schema validation prevents invalid configurations
- [ ] Backward compatibility maintained for all existing env vars
- [ ] CLI configuration management commands work
- [ ] Hot-reloading support implemented

### Quality Requirements  
- [ ] Comprehensive test coverage for all configuration scenarios
- [ ] Clear error messages for configuration problems
- [ ] Complete documentation with examples
- [ ] Performance impact minimal
- [ ] Configuration changes don't break existing deployments

## Dependencies
- **Depends on**: None (foundational task)
- **Blocks**: Tasks 05, 06 (Service Layer, Connection Management) need configuration

## Estimated Time
- **Configuration System**: 12-15 hours
- **Integration**: 8-10 hours
- **CLI Commands**: 4-6 hours
- **Testing**: 6-8 hours
- **Documentation**: 4-6 hours
- **Total**: 34-45 hours