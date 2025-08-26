#!/usr/bin/env python3
"""Debug script to test validation behavior."""

import sys
sys.path.insert(0, 'src')

from naq.cli.event_commands import EventCommandHandler
from naq.exceptions import ValidationError

def test_validation():
    from naq.utils.validation import validate_parameter
    
    # Test NATS URL validation directly
    print("Testing NATS URL validation directly...")
    try:
        validate_parameter(
            value="invalid-url",
            param_name="nats_url",
            not_none=True,
            regex_pattern=r"^(nats://)?[a-zA-Z0-9.-]+(:[0-9]+)?(,[a-zA-Z0-9.-]+(:[0-9]+)?)*$",
            error_message="Invalid NATS URL format"
        )
        print("  No exception raised for invalid NATS URL")
    except ValidationError as e:
        print(f"  ValidationError raised: {e}")
    except Exception as e:
        print(f"  Other exception raised: {type(e).__name__}: {e}")
    
    # Test worker ID validation directly
    print("\nTesting worker ID validation directly...")
    try:
        validate_parameter(
            value="",
            param_name="worker_id",
            not_none=True,
            error_message="worker_id cannot be empty"
        )
        print("  No exception raised for empty worker ID")
    except ValidationError as e:
        print(f"  ValidationError raised: {e}")
    except Exception as e:
        print(f"  Other exception raised: {type(e).__name__}: {e}")
    
    handler = EventCommandHandler()
    
    # Test NATS URL validation through handler
    print("\nTesting NATS URL validation through handler...")
    try:
        handler.validate_common_parameters(
            nats_url="invalid-url",
            log_level="INFO",
            limit=100,
            worker_id="test-worker",
        )
        print("  No exception raised for invalid NATS URL")
    except ValidationError as e:
        print(f"  ValidationError raised: {e}")
    except Exception as e:
        print(f"  Other exception raised: {type(e).__name__}: {e}")
    
    # Test worker ID validation through handler
    print("\nTesting worker ID validation through handler...")
    try:
        handler.validate_common_parameters(
            nats_url="nats://localhost:4222",
            log_level="INFO",
            limit=100,
            worker_id="",
        )
        print("  No exception raised for empty worker ID")
    except ValidationError as e:
        print(f"  ValidationError raised: {e}")
    except Exception as e:
        print(f"  Other exception raised: {type(e).__name__}: {e}")

if __name__ == "__main__":
    test_validation()