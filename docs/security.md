# Security Considerations for NAQ

## Critical Security Vulnerability: Pickle Serialization

### Overview

NAQ currently uses Python's `pickle` serialization by default for job data. **This presents a critical security vulnerability** that allows arbitrary code execution when processing malicious job data.

### The Risk

The `pickle` module can deserialize and execute arbitrary Python code. If an attacker can enqueue malicious job data, they can:

- Execute arbitrary system commands
- Access sensitive files and environment variables
- Install malware or backdoors
- Perform denial-of-service attacks
- Compromise the entire worker process and potentially the host system

### Attack Scenarios

1. **Malicious Job Enqueueing**: An attacker with access to enqueue jobs could submit crafted payloads that execute malicious code when deserialized by workers.

2. **Man-in-the-Middle**: If NATS traffic is not encrypted, an attacker could inject malicious job data into the stream.

3. **Compromised Job Source**: If the application enqueueing jobs is compromised, all job data becomes untrustworthy.

### Example Vulnerable Code

```python
import pickle
import subprocess

# Malicious payload that could be embedded in job data
class MaliciousPayload:
    def __reduce__(self):
        return (subprocess.call, (['rm', '-rf', '/tmp/*'],))

# When pickled and then unpickled, this executes the command
dangerous_data = pickle.dumps(MaliciousPayload())
pickle.loads(dangerous_data)  # Executes 'rm -rf /tmp/*'
```

## Mitigation Strategies

### 1. Switch to JSON Serialization (Recommended)

NAQ provides a secure JSON-based serializer that prevents arbitrary code execution:

```bash
# Set environment variable to use JSON serializer
export NAQ_JOB_SERIALIZER=json
```

**Advantages:**
- No arbitrary code execution risk
- Human-readable job data
- Cross-language compatibility
- Better debugging capabilities

**Limitations:**
- Only supports functions that can be imported by their module path
- Cannot serialize closures or dynamically created functions
- Non-JSON-serializable arguments are converted to string representations

### 2. Network Security

- **Use TLS/SSL**: Configure NATS with TLS encryption to prevent man-in-the-middle attacks
- **Authentication**: Implement NATS authentication to control who can enqueue jobs
- **Network Isolation**: Run NAQ in a isolated network environment
- **Firewall Rules**: Restrict access to NATS ports

### 3. Access Control

- **Principle of Least Privilege**: Run workers with minimal system permissions
- **User Isolation**: Use separate user accounts for worker processes
- **Container Security**: Run workers in containers with restricted capabilities
- **Input Validation**: Validate job data before enqueueing when possible

### 4. Monitoring and Detection

- **Job Audit Logging**: Log all job enqueue/dequeue operations
- **Anomaly Detection**: Monitor for unusual job patterns or resource usage
- **Security Scanning**: Regularly scan worker environments for threats
- **Runtime Monitoring**: Monitor worker processes for suspicious activity

## Migration from Pickle to JSON

### Step 1: Assess Function Compatibility

Review your job functions to ensure they can be imported by module path:

```python
# Good - can be imported
def my_task(data):
    return process_data(data)

# Bad - closure cannot be imported  
def create_task(config):
    def inner_task(data):
        return process_data(data, config)
    return inner_task
```

### Step 2: Test with JSON Serializer

Test your application with the JSON serializer in a development environment:

```bash
export NAQ_JOB_SERIALIZER=json
python your_app.py
```

### Step 3: Handle Incompatible Functions

For functions that cannot be JSON-serialized, consider:

- Refactoring to use module-level functions
- Passing configuration as job arguments instead of closures
- Using factory patterns for complex job creation

### Step 4: Deploy Gradually

- Deploy JSON serialization to non-production environments first
- Monitor for serialization errors
- Plan downtime for production migration if needed

## Security Best Practices

### For Development

1. **Never use pickle in production** without understanding the risks
2. **Validate all input data** before job enqueueing
3. **Use static analysis tools** to detect pickle usage
4. **Code review security implications** of serialization choices

### For Production

1. **Use JSON serialization** whenever possible
2. **Implement comprehensive monitoring** for security events
3. **Regular security audits** of job processing code
4. **Incident response plan** for security breaches
5. **Keep NAQ and dependencies updated** for security patches

### For Operations

1. **Network segmentation** for NAQ infrastructure
2. **Regular backup and recovery testing**
3. **Monitor resource usage** for anomalies
4. **Log aggregation** for security analysis
5. **Access control reviews** for job submission endpoints

## Reporting Security Issues

If you discover security vulnerabilities in NAQ:

1. **Do not** create public GitHub issues for security vulnerabilities
2. Report privately to the maintainers
3. Provide detailed reproduction steps
4. Include suggested fixes if possible

## References

- [Python Pickle Security Risks](https://docs.python.org/3/library/pickle.html#security)
- [OWASP Deserialization Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Deserialization_Cheat_Sheet.html)
- [NATS Security Documentation](https://docs.nats.io/running-a-nats-service/configuration/securing_nats)