# Security Best Practices - Secure NAQ Usage

This example demonstrates security best practices for using NAQ in production environments, including secure serialization, input validation, and monitoring.

## What You'll Learn

- Why pickle serialization is dangerous
- How to use secure JSON serialization
- Input validation and sanitization
- Security monitoring and alerting
- Safe function design patterns
- Production security checklist

## Critical Security Warning

**NAQ uses pickle serialization by default, which allows arbitrary code execution!**

Malicious job data can execute any Python code on your worker machines. Always use JSON serialization in production:

```bash
export NAQ_JOB_SERIALIZER=json
```

## Security Vulnerabilities

### Pickle Deserialization Attacks

Pickle can execute arbitrary code during deserialization:

```python
# DANGEROUS - Never accept pickle data from untrusted sources
import pickle

# This malicious payload executes system commands
malicious_data = b"cos\nsystem\n(S'rm -rf /'\ntR."
pickle.loads(malicious_data)  # Executes 'rm -rf /' command!
```

### How NAQ is Vulnerable

With pickle serialization, attackers can:
1. Enqueue malicious jobs from compromised clients
2. Inject malicious data via network attacks
3. Execute arbitrary code on all worker machines
4. Access sensitive data and credentials
5. Install persistent backdoors

## Secure Configuration

### 1. Use JSON Serialization

**Always set this in production:**
```bash
export NAQ_JOB_SERIALIZER=json
```

JSON serialization:
- ✅ Prevents arbitrary code execution
- ✅ Only allows importing existing functions
- ✅ Provides clear error messages for unsafe operations
- ✅ Is human-readable for debugging

### 2. Environment Configuration

```bash
# Required security settings
export NAQ_JOB_SERIALIZER=json
export NAQ_NATS_URL=nats://secure-nats:4222

# Optional security enhancements
export NAQ_RESULT_TTL=3600          # Limit result storage time
export NAQ_WORKER_TTL=300           # Limit worker registration time
```

### 3. Function Design

Only use functions that can be safely imported:

```python
# GOOD - Module-level function, can be imported safely
def process_user_data(user_id: int, action: str) -> str:
    # Validate inputs
    if not isinstance(user_id, int) or user_id <= 0:
        raise ValueError("Invalid user ID")
    
    if action not in ['create', 'update', 'delete']:
        raise ValueError("Invalid action")
    
    # Safe processing
    return f"Processed {action} for user {user_id}"

# BAD - Lambda functions cannot be imported
bad_func = lambda x: x * 2

# BAD - Nested functions cannot be imported  
def outer():
    def inner():
        return "bad"
    return inner
```

## Prerequisites

1. **NATS Server** with authentication/TLS enabled
2. **Secure Configuration**: `export NAQ_JOB_SERIALIZER=json`
3. **Network Security**: Firewall rules, VPN access
4. **Access Control**: Limited job enqueueing permissions

## Running the Examples

### 1. Secure Setup

```bash
# Start NATS with authentication (recommended)
cd docker && docker-compose -f docker-compose-secure.yml up -d

# Set secure serialization (REQUIRED)
export NAQ_JOB_SERIALIZER=json

# Verify secure configuration
python verify_security.py

# Start worker with security monitoring
naq worker secure_queue --log-level INFO
```

### 2. Run Security Examples

```bash
cd examples/03-production/01-security-best-practices

# Demonstrate secure vs insecure patterns
python secure_patterns.py

# Show input validation
python input_validation.py

# Security monitoring example
python security_monitoring.py
```

## Security Checklist

### Pre-Production
- [ ] Set `NAQ_JOB_SERIALIZER=json`
- [ ] Enable NATS authentication
- [ ] Configure TLS encryption
- [ ] Implement input validation
- [ ] Review all job functions for safety
- [ ] Set up monitoring and alerting

### Production Deployment
- [ ] Use secure NATS configuration
- [ ] Restrict network access to NATS
- [ ] Run workers with minimal privileges
- [ ] Monitor for security warnings
- [ ] Regular security audits
- [ ] Incident response plan

### Ongoing Security
- [ ] Monitor NAQ security warnings
- [ ] Regular dependency updates
- [ ] Review job function changes
- [ ] Monitor worker resource usage
- [ ] Alert on unusual job patterns
- [ ] Regular penetration testing

## Input Validation Patterns

### Validate All Inputs
```python
def safe_user_operation(user_id: int, operation: str, data: dict) -> str:
    # Type validation
    if not isinstance(user_id, int):
        raise ValueError(f"user_id must be int, got {type(user_id)}")
    
    # Range validation
    if user_id <= 0 or user_id > 1000000:
        raise ValueError(f"user_id out of range: {user_id}")
    
    # Enum validation
    valid_operations = {'create', 'read', 'update', 'delete'}
    if operation not in valid_operations:
        raise ValueError(f"Invalid operation: {operation}")
    
    # Data structure validation
    if not isinstance(data, dict):
        raise ValueError("data must be a dictionary")
    
    # Safe processing...
    return f"Processed {operation} for user {user_id}"
```

### Sanitize String Inputs
```python
import re
from html import escape

def safe_content_processing(title: str, content: str) -> str:
    # Length limits
    if len(title) > 100:
        raise ValueError("Title too long")
    
    if len(content) > 10000:
        raise ValueError("Content too long")
    
    # Pattern validation
    if not re.match(r'^[a-zA-Z0-9\s\-_.]+$', title):
        raise ValueError("Title contains invalid characters")
    
    # HTML escaping
    safe_title = escape(title)
    safe_content = escape(content)
    
    return f"Processed content: {safe_title}"
```

## Monitoring and Alerting

### Security Warnings
NAQ automatically logs security warnings:
```python
# These warnings appear when using pickle
"⚠️ SECURITY WARNING: Using 'pickle' serializer which allows arbitrary code execution!"
```

### Monitor for Threats
```python
# Monitor job patterns
def monitor_job_security(job_data):
    # Alert on suspicious patterns
    if 'eval' in str(job_data) or 'exec' in str(job_data):
        alert_security_team("Suspicious job content detected")
    
    # Monitor job sources
    if job_source not in TRUSTED_SOURCES:
        alert_security_team("Job from untrusted source")
    
    # Rate limiting
    if job_rate > RATE_LIMIT:
        alert_security_team("Unusual job submission rate")
```

### Security Metrics
- Job enqueueing rates and patterns
- Worker resource consumption
- Failed serialization attempts
- Network connection sources
- Authentication failures

## Network Security

### NATS Security
```yaml
# NATS configuration with authentication
authorization: {
  users: [
    {user: "naq_worker", password: "secure_password"}
    {user: "naq_client", password: "secure_password"}
  ]
}

# TLS configuration
tls: {
  cert_file: "/path/to/server-cert.pem"
  key_file: "/path/to/server-key.pem"
  ca_file: "/path/to/ca.pem"
}
```

### Firewall Rules
```bash
# Only allow NAQ traffic on specific ports
iptables -A INPUT -p tcp --dport 4222 -s 10.0.0.0/8 -j ACCEPT
iptables -A INPUT -p tcp --dport 4222 -j DROP
```

## Incident Response

### Security Breach Response
1. **Immediate**: Stop all workers
2. **Isolate**: Disconnect from NATS
3. **Investigate**: Check logs for malicious jobs
4. **Clean**: Purge suspicious job queues
5. **Recover**: Restart with secure configuration
6. **Monitor**: Enhanced monitoring for 24-48 hours

### Recovery Checklist
- [ ] Worker processes stopped
- [ ] NATS connections severed
- [ ] Job queues purged
- [ ] Security configuration verified
- [ ] Clean worker restart
- [ ] Enhanced monitoring enabled

## Next Steps

- Review [error handling](../03-error-handling/) for secure error management
- Check [configuration management](../04-configuration/) for deployment security
- Explore [monitoring dashboard](../02-monitoring-dashboard/) for security oversight
- Implement [high availability](../../05-advanced/02-high-availability/) with security