# Security Policy

## Serializer Security Warning

The `naq` library uses serialization to transmit job functions, arguments, and keyword arguments between the process that enqueues a job and the worker process that executes it. The choice of serializer has significant security implications.

### Default Serializer: `cloudpickle`

By default, `naq` uses `cloudpickle` for serialization. **`cloudpickle` can execute arbitrary code during deserialization.** This is a powerful feature that allows serializing a wide range of Python objects, but it also presents a serious security risk.

**Risk:** If an attacker can control the input to a job that is deserialized with `cloudpickle`, they can potentially execute arbitrary code on the worker machine. This could lead to:
*   Remote Code Execution (RCE)
*   Data theft or corruption
*   Denial of Service
*   Full system compromise

**Recommendation:** **NEVER use `cloudpickle` in production environments where the job producer (the client enqueuing jobs) is not fully trusted.** This includes any scenario where untrusted users or external systems can submit jobs to the queue.

### Safe Alternative: `JsonSerializer`

For production environments, especially those where job producers are not fully trusted, `naq` provides a `JsonSerializer`. This serializer uses the standard `json` library and is inherently safer because it does not execute code during deserialization.

**Benefits of `JsonSerializer`:**
*   **No Arbitrary Code Execution:** Deserializing JSON data will not lead to code execution.
*   **Standard and Interoperable:** JSON is a universal data format, making it easier to interact with other systems and languages.
*   **Auditability:** JSON data is human-readable and easy to inspect.

**Limitations of `JsonSerializer`:**
*   **Object Type Support:** `JsonSerializer` can only handle standard JSON types (strings, numbers, booleans, lists, dictionaries). Custom Python objects or complex types (like lambdas, closures, or classes with complex internal states) cannot be serialized directly.
*   **Function Serialization:** You cannot directly serialize a Python function object with `JsonSerializer`. Instead, you must provide a way to look up the function on the worker side, typically by its import path (e.g., `"my_module.my_function"`).

### How to Use `JsonSerializer`

To configure `naq` to use the `JsonSerializer`, you can set it globally or on a per-queue basis.

#### Global Configuration

You can set the serializer for all queues by setting the `NAQ_SERIALIZER` environment variable to `json`:

```bash
export NAQ_SERIALIZER=json
```

Alternatively, you can set it in your application's settings:

```python
# your_app_settings.py
from naq.settings import SETTINGS

SETTINGS.serializer = "json"
```

#### Per-Queue Configuration

You can also specify the serializer when creating a `Queue` instance:

```python
from naq.queue import Queue
from naq.job import JsonSerializer

# Using the string name
q = Queue(name="my_safe_queue", serializer="json")

# Using the class
q = Queue(name="my_safe_queue", serializer=JsonSerializer)
```

#### Registering Custom Encoders/Decoders

If you need to serialize custom object types with `JsonSerializer`, you can register custom JSON encoders and decoders.

```python
import json
from naq.job import JsonSerializer

class CustomObject:
    def __init__(self, value):
        self.value = value

def custom_encoder(obj):
    if isinstance(obj, CustomObject):
        return {"__custom__": True, "value": obj.value}
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

def custom_decoder(dct):
    if dct.get("__custom__"):
        return CustomObject(dct["value"])
    return dct

# Register the custom encoder and decoder
JsonSerializer.register_custom_encoder(custom_encoder)
JsonSerializer.register_custom_decoder(custom_decoder)

# Now you can enqueue jobs with CustomObject arguments
# assuming the worker also has these custom encoders/decoders registered.
```

**Important:** Both the enqueuing process and the worker process must have the same custom encoders and decoders registered for deserialization to work correctly.

### General Security Best Practices

1.  **Principle of Least Privilege:** Run your `naq` workers with the minimum set of permissions necessary to perform their tasks.
2.  **Input Validation:** Even with `JsonSerializer`, always validate and sanitize any data that comes from untrusted sources before processing it within a job.
3.  **Network Security:** Secure your NATS server. Use TLS for encryption in transit. Employ firewalls and VPNs to restrict access to the NATS server to only your application servers.
4.  **Environment Isolation:** Consider running workers for different queues in separate processes or containers, especially if they handle jobs with varying levels of trust or sensitivity.
5.  **Keep Dependencies Updated:** Regularly update `naq` and its dependencies (including `cloudpickle` if used) to benefit from the latest security patches.

## Reporting a Vulnerability

If you discover a potential security vulnerability in `naq`, please report it responsibly. Do not create a public GitHub issue.

Instead, please send an email to [your-security-contact@example.com]. If possible, please include the following information:

*   A description of the vulnerability.
*   Steps to reproduce the issue.
*   Any potential impact of the vulnerability.
*   Your suggestions for a fix.

We will acknowledge receipt of your report and aim to provide an initial response within 5 business days. We will work with you to understand and resolve the issue in a timely manner. Thank you for helping to keep `naq` safe.