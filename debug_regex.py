#!/usr/bin/env python3
"""Debug script to test regex pattern."""

import re

def test_nats_url_regex():
    pattern = r"^(nats://)?[a-zA-Z0-9.-]+(:[0-9]+)?(,[a-zA-Z0-9.-]+(:[0-9]+)?)*$"
    
    # Test cases
    test_cases = [
        ("nats://localhost:4222", True),
        ("localhost:4222", True),
        ("nats://example.com", True),
        ("example.com", True),
        ("nats://192.168.1.1:4222", True),
        ("192.168.1.1:4222", True),
        ("nats://server1:4222,server2:4222", True),
        ("server1:4222,server2:4222", True),
        ("invalid-url", False),
        ("", False),
        ("nats://", False),
        ("nats://:", False),
        ("nats://:4222", False),
    ]
    
    print("Testing NATS URL regex pattern...")
    for url, expected in test_cases:
        result = bool(re.match(pattern, url))
        status = "✓" if result == expected else "✗"
        print(f"  {status} URL: '{url}' -> Expected: {expected}, Got: {result}")

if __name__ == "__main__":
    test_nats_url_regex()