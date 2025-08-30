# Serializers

This document describes the available serializers in NAQ and their performance characteristics.

## Overview

NAQ supports three serializers for job serialization and deserialization:

1. **JsonSerializer** - Uses msgspec for JSON encoding/decoding
2. **MsgPackSerializer** - Uses msgspec for MessagePack encoding/decoding
3. **PickleSerializer** - Uses Python's pickle module

## MsgPackSerializer

The MsgPackSerializer is the recommended serializer for most use cases due to its superior performance and smaller serialized size.

### Features

- **High Performance**: 2.18x faster than JsonSerializer and 32.32x faster than PickleSerializer
- **Compact Size**: Produces serialized data that is 2.19x smaller than JsonSerializer and 13.19x smaller than PickleSerializer
- **Type Safety**: Uses msgspec Struct for schema validation
- **Security**: Requires importable functions (no lambda or local functions)
- **Integrity**: Supports checksum and signature verification

### Usage

```python
from naq.serializers import MsgPackSerializer
from naq.models.jobs import Job

def my_function(x, y):
    return x + y

# Create a job
job = Job(function=my_function, args=(1, 2), kwargs={})
job.job_id = 'my-job-id'

# Serialize the job
serialized = MsgPackSerializer.serialize_job(job)

# Deserialize the job
deserialized = MsgPackSerializer.deserialize_job(serialized)

# Execute the deserialized job
result = deserialized.function(*deserialized.args, **deserialized.kwargs)
```

### Configuration

MsgPackSerializer can be configured through the following settings:

- `SERIALIZATION_CHECKSUM_ENABLED`: Enable checksum verification (default: False)
- `SERIALIZATION_CHECKSUM_ALGORITHM`: Checksum algorithm (default: "sha256")
- `SERIALIZATION_SIGNATURE_KEY`: Secret key for signature verification (default: None)
- `SERIALIZATION_MAX_SIZE_BYTES`: Maximum serialized size in bytes (default: 10MB)

## Performance Comparison

Based on benchmarks with 1000 iterations:

| Serializer | Time (seconds) | Size (bytes) | Ops/sec |
|------------|----------------|--------------|---------|
| MsgPack    | 0.0150         | 341          | 66,733  |
| JSON       | 0.0327         | 747          | 30,618  |
| Pickle     | 0.4843         | 4,498        | 2,065   |

### Key Findings

1. **MsgPackSerializer is 2.18x faster** than JsonSerializer
2. **MsgPackSerializer produces 2.19x smaller** serialized data than JsonSerializer
3. **MsgPackSerializer is 32.32x faster** than PickleSerializer
4. **MsgPackSerializer produces 13.19x smaller** serialized data than PickleSerializer

## Choosing a Serializer

### Use MsgPackSerializer when:

- Performance is critical
- Network bandwidth is limited
- You need the smallest possible serialized size
- You're working with large datasets
- Security is important (importable functions only)

### Use JsonSerializer when:

- You need human-readable serialized data
- You're debugging serialization issues
- You're integrating with systems that only support JSON
- Backward compatibility with existing JSON data is required

### Use PickleSerializer when:

- You need to serialize lambda functions or local functions
- You're working with complex Python objects that msgspec can't handle
- You need to preserve object identity and references
- You're migrating from a pickle-based system

## Migration Guide

### From JsonSerializer to MsgPackSerializer

1. Replace import:
   ```python
   # Before
   from naq.serializers import JsonSerializer
   
   # After
   from naq.serializers import MsgPackSerializer
   ```

2. Update serializer usage:
   ```python
   # Before
   serialized = JsonSerializer.serialize_job(job)
   deserialized = JsonSerializer.deserialize_job(serialized)
   
   # After
   serialized = MsgPackSerializer.serialize_job(job)
   deserialized = MsgPackSerializer.deserialize_job(serialized)
   ```

3. Ensure all functions are importable (no lambda or local functions)

### From PickleSerializer to MsgPackSerializer

1. Replace import:
   ```python
   # Before
   from naq.serializers import PickleSerializer
   
   # After
   from naq.serializers import MsgPackSerializer
   ```

2. Update serializer usage:
   ```python
   # Before
   serialized = PickleSerializer.serialize_job(job)
   deserialized = PickleSerializer.deserialize_job(serialized)
   
   # After
   serialized = MsgPackSerializer.serialize_job(job)
   deserialized = MsgPackSerializer.deserialize_job(serialized)
   ```

3. Refactor lambda and local functions to module-level functions

## Security Considerations

### MsgPackSerializer Security

- **Function Importability**: Only allows serialization of importable functions
- **No Lambda Functions**: Lambda functions cannot be serialized
- **No Local Functions**: Functions defined inside other functions cannot be serialized
- **Checksum Verification**: Optional checksum verification for data integrity
- **Signature Verification**: Optional signature verification for authentication

### JsonSerializer Security

- Same security features as MsgPackSerializer
- Human-readable format makes debugging easier
- Larger serialized size may impact performance

### PickleSerializer Security

- **Security Risk**: Pickle can execute arbitrary code during deserialization
- **No Function Restrictions**: Can serialize any Python function
- **Use with Caution**: Only use with trusted data sources

## Best Practices

1. **Use MsgPackSerializer** for most production use cases
2. **Keep Functions Simple**: Complex functions may have serialization issues
3. **Test Serialization**: Always test serialization/deserialization with your specific use cases
4. **Monitor Performance**: Benchmark with your actual data to ensure performance meets requirements
5. **Consider Security**: Always consider security implications when choosing a serializer

## Troubleshooting

### Common Issues

1. **"Object is not importable" Error**
   - Cause: Trying to serialize a lambda or local function
   - Solution: Move the function to module level

2. **"size exceeds maximum allowed size" Error**
   - Cause: Serialized data is too large
   - Solution: Increase `SERIALIZATION_MAX_SIZE_BYTES` or reduce data size

3. **"Unsupported checksum algorithm" Error**
   - Cause: Invalid checksum algorithm specified
   - Solution: Use a valid algorithm like "md5", "sha256", or "sha512"

### Debugging Tips

1. Use JsonSerializer for debugging since it produces human-readable output
2. Test with simple functions first
3. Check function importability using `importlib.import_module`
4. Verify data types are supported by msgspec

## Future Enhancements

Planned improvements to serializers:

1. **Compression Support**: Optional compression for further size reduction
2. **Custom Encoders**: Support for custom type encoders
3. **Streaming Support**: Stream large datasets during serialization
4. **Schema Evolution**: Support for schema changes without breaking compatibility