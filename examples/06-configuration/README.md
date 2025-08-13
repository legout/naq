# NAQ Configuration Examples

This directory contains example configuration files demonstrating how to configure NAQ for different environments and use cases.

## Configuration Files

- `development.yaml` - Development environment configuration with debug logging and reduced concurrency
- `production.yaml` - Production environment configuration optimized for performance and monitoring
- `testing.yaml` - Testing environment configuration with minimal services for unit tests
- `minimal.yaml` - Minimal configuration for embedded use cases
- `advanced.yaml` - Advanced configuration showcasing all available options
- `docker.yaml` - Configuration optimized for Docker/containerized deployments

## Using Configuration Files

### 1. Basic Usage

Place a configuration file in one of these locations:
- Current directory: `./naq.yaml` or `./naq.yml`
- User config: `~/.naq/config.yaml`
- System config: `/etc/naq/config.yaml`

```bash
# Copy desired example
cp examples/06-configuration/development.yaml ./naq.yaml

# Validate configuration
naq system config --validate

# View current configuration
naq system config --show
```

### 2. Environment-Specific Configuration

Set the `NAQ_ENVIRONMENT` environment variable to apply environment-specific overrides:

```bash
export NAQ_ENVIRONMENT=production
naq worker start
```

### 3. Command-Line Configuration

Specify configuration file explicitly:

```bash
naq worker start --config ./production.yaml
```

### 4. Initialize New Configuration

Use the CLI to create a new configuration file:

```bash
# Create development configuration
naq system config-init --environment development

# Create production configuration with custom path
naq system config-init --environment production --output ./prod-config.yaml

# Force overwrite existing file
naq system config-init --environment testing --force
```

## Configuration Hierarchy

NAQ loads configuration from multiple sources in this order (highest to lowest priority):

1. Explicit config file (via `--config` flag)
2. Current directory: `./naq.yaml` or `./naq.yml`
3. User config: `~/.naq/config.yaml`
4. System config: `/etc/naq/config.yaml`
5. Environment variables: `NAQ_*` variables
6. Built-in defaults

## Environment Variable Interpolation

Configuration files support environment variable interpolation:

```yaml
nats:
  servers:
    - ${NAQ_NATS_URL:nats://localhost:4222}
    - ${NATS_BACKUP_URL:nats://backup:4222}

events:
  storage_url: ${EVENT_STORAGE_URL}  # Required, no default
```

## Environment Overrides

Define environment-specific overrides in your configuration:

```yaml
environments:
  development:
    name: development
    overrides:
      logging.level: DEBUG
      workers.concurrency: 2
      
  production:
    name: production
    overrides:
      logging.level: INFO
      workers.concurrency: 50
      events.batch_size: 500
```

## Backward Compatibility

The new YAML configuration system is fully backward compatible with environment variables. All existing `NAQ_*` environment variables continue to work and will override corresponding YAML configuration values.

## Configuration Validation

All configuration files include JSON Schema validation to catch errors early:

```bash
# Validate current configuration
naq system config --validate

# Validate specific file
naq system config --validate --config ./production.yaml

# View configuration sources and validation status
naq system config --sources
```

## Common Patterns

### Multiple NATS Servers

```yaml
nats:
  servers:
    - nats://primary:4222
    - nats://backup1:4222 
    - nats://backup2:4222
```

### Worker Pools

```yaml
workers:
  concurrency: 10  # Default
  pools:
    high_priority:
      concurrency: 20
      queues: ["urgent", "critical"]
    background:
      concurrency: 5
      queues: ["email", "notifications"]
```

### Queue-Specific Settings

```yaml
queues:
  default: main_queue
  prefix: myapp
  configs:
    urgent:
      ack_wait: 30
      max_deliver: 5
    background:
      ack_wait: 300
      max_deliver: 3
```

### Event Storage Configuration

```yaml
events:
  enabled: true
  batch_size: 100
  flush_interval: 5.0
  stream:
    name: MYAPP_JOB_EVENTS
    max_age: "7d"
    max_bytes: "1GB"
    replicas: 3
  filters:
    exclude_heartbeats: true
    min_job_duration: 1000  # Only log jobs > 1s
```

### TLS Configuration

```yaml
nats:
  servers: ["nats-tls://secure.example.com:4222"]
  tls:
    enabled: true
    cert_file: /path/to/client.crt
    key_file: /path/to/client.key
    ca_file: /path/to/ca.crt
```

### Authentication

```yaml
nats:
  auth:
    # Option 1: Username/Password
    username: ${NATS_USER}
    password: ${NATS_PASS}
    
    # Option 2: Token (alternative to username/password)
    # token: ${NATS_TOKEN}
```

For more information, see the main NAQ documentation.