#!/usr/bin/env python3
"""
Secure NAQ Patterns

This example demonstrates secure patterns for using NAQ in production:
- Secure JSON serialization
- Input validation and sanitization
- Safe function design
- Security monitoring

Before running:
1. Start NATS: `cd docker && docker-compose up -d`
2. Set secure serializer: `export NAQ_JOB_SERIALIZER=json`
3. Start worker: `naq worker secure_queue --log-level INFO`
4. Run this script: `python secure_patterns.py`
"""

import os
import re
import time
from html import escape
from typing import Dict, List, Any, Optional

from naq import SyncClient, setup_logging

# CRITICAL: Always use JSON serialization in production
os.environ.setdefault('NAQ_JOB_SERIALIZER', 'json')

# Setup logging to see security warnings
setup_logging(level="INFO")


# ===== SECURE FUNCTION PATTERNS =====

def secure_user_processing(user_id: int, operation: str, data: Dict[str, Any]) -> str:
    """
    Secure user data processing with comprehensive input validation.
    
    This function demonstrates secure patterns:
    - Type validation for all inputs
    - Range checking for numeric values
    - Enum validation for string choices
    - Structure validation for complex data
    - Safe error handling
    
    Args:
        user_id: Numeric user identifier
        operation: Operation type (create/read/update/delete)
        data: User data dictionary
        
    Returns:
        Processing result message
        
    Raises:
        ValueError: For invalid inputs
    """
    print(f"🔒 Secure processing: User {user_id}, Operation {operation}")
    
    # 1. Type validation
    if not isinstance(user_id, int):
        raise ValueError(f"user_id must be int, got {type(user_id).__name__}")
    
    if not isinstance(operation, str):
        raise ValueError(f"operation must be str, got {type(operation).__name__}")
    
    if not isinstance(data, dict):
        raise ValueError(f"data must be dict, got {type(data).__name__}")
    
    # 2. Range validation
    if user_id <= 0 or user_id > 1000000:
        raise ValueError(f"user_id out of valid range: {user_id}")
    
    # 3. Enum validation
    valid_operations = {'create', 'read', 'update', 'delete'}
    if operation not in valid_operations:
        raise ValueError(f"Invalid operation '{operation}', must be one of: {valid_operations}")
    
    # 4. Data structure validation
    required_fields = {'name', 'email'}
    missing_fields = required_fields - set(data.keys())
    if missing_fields:
        raise ValueError(f"Missing required fields: {missing_fields}")
    
    # 5. Content validation and sanitization
    name = str(data['name']).strip()
    email = str(data['email']).strip().lower()
    
    # Name validation
    if not re.match(r'^[a-zA-Z\s\-\'\.]{1,50}$', name):
        raise ValueError("Name contains invalid characters or is too long")
    
    # Email validation (basic)
    if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email):
        raise ValueError(f"Invalid email format: {email}")
    
    # Simulate secure processing
    time.sleep(0.5)
    
    result = f"Securely processed {operation} for user {user_id} (name: {name}, email: {email})"
    print(f"✅ {result}")
    
    return result


def secure_content_processing(title: str, content: str, category: str) -> str:
    """
    Secure content processing with sanitization and validation.
    
    Args:
        title: Content title
        content: Content body
        category: Content category
        
    Returns:
        Processing result
        
    Raises:
        ValueError: For invalid or dangerous content
    """
    print(f"📝 Secure content processing: {title[:30]}...")
    
    # 1. Type validation
    if not all(isinstance(x, str) for x in [title, content, category]):
        raise ValueError("All inputs must be strings")
    
    # 2. Length validation
    if len(title) > 100:
        raise ValueError(f"Title too long: {len(title)} chars (max 100)")
    
    if len(content) > 5000:
        raise ValueError(f"Content too long: {len(content)} chars (max 5000)")
    
    # 3. Content validation
    if not title.strip():
        raise ValueError("Title cannot be empty")
    
    if not content.strip():
        raise ValueError("Content cannot be empty")
    
    # 4. Category validation
    valid_categories = {'news', 'blog', 'documentation', 'help'}
    if category not in valid_categories:
        raise ValueError(f"Invalid category: {category}")
    
    # 5. Security checks
    dangerous_patterns = [
        r'<script.*?>.*?</script>',  # Script tags
        r'javascript:',              # JavaScript URLs
        r'on\w+\s*=',               # Event handlers
        r'eval\s*\(',               # eval calls
        r'exec\s*\(',               # exec calls
    ]
    
    combined_text = f"{title} {content}".lower()
    for pattern in dangerous_patterns:
        if re.search(pattern, combined_text, re.IGNORECASE):
            raise ValueError(f"Content contains potentially dangerous pattern: {pattern}")
    
    # 6. Sanitization
    safe_title = escape(title.strip())
    safe_content = escape(content.strip())
    
    # Simulate processing
    time.sleep(1)
    
    result = f"Processed {category} content: '{safe_title}' ({len(safe_content)} chars)"
    print(f"✅ {result}")
    
    return result


def secure_file_processing(filename: str, file_type: str, size_mb: int) -> str:
    """
    Secure file processing with validation and safety checks.
    
    Args:
        filename: Name of file to process
        file_type: Type of file (pdf, jpg, etc.)
        size_mb: File size in megabytes
        
    Returns:
        Processing result
        
    Raises:
        ValueError: For invalid or unsafe files
    """
    print(f"📁 Secure file processing: {filename}")
    
    # 1. Type validation
    if not isinstance(filename, str) or not isinstance(file_type, str):
        raise ValueError("filename and file_type must be strings")
    
    if not isinstance(size_mb, (int, float)):
        raise ValueError("size_mb must be a number")
    
    # 2. Filename validation
    if not re.match(r'^[a-zA-Z0-9._\-]{1,100}$', filename):
        raise ValueError("Filename contains invalid characters or is too long")
    
    # 3. File type validation
    allowed_types = {'pdf', 'jpg', 'jpeg', 'png', 'gif', 'txt', 'csv', 'docx'}
    if file_type.lower() not in allowed_types:
        raise ValueError(f"File type '{file_type}' not allowed. Allowed: {allowed_types}")
    
    # 4. Size validation
    if size_mb <= 0:
        raise ValueError("File size must be positive")
    
    if size_mb > 100:  # 100MB limit
        raise ValueError(f"File too large: {size_mb}MB (max 100MB)")
    
    # 5. Security checks
    dangerous_extensions = {'.exe', '.bat', '.cmd', '.scr', '.vbs', '.js'}
    file_ext = '.' + file_type.lower()
    if file_ext in dangerous_extensions:
        raise ValueError(f"Dangerous file type: {file_type}")
    
    # Simulate secure processing
    processing_time = size_mb * 0.1  # 0.1 seconds per MB
    time.sleep(processing_time)
    
    result = f"Securely processed {file_type.upper()} file: {filename} ({size_mb}MB)"
    print(f"✅ {result}")
    
    return result


def secure_api_call(endpoint: str, method: str, params: Dict[str, Any]) -> str:
    """
    Secure API call processing with validation and sanitization.
    
    Args:
        endpoint: API endpoint path
        method: HTTP method
        params: Request parameters
        
    Returns:
        API call result
        
    Raises:
        ValueError: For invalid API parameters
    """
    print(f"🌐 Secure API call: {method} {endpoint}")
    
    # 1. Type validation
    if not isinstance(endpoint, str) or not isinstance(method, str):
        raise ValueError("endpoint and method must be strings")
    
    if not isinstance(params, dict):
        raise ValueError("params must be a dictionary")
    
    # 2. Method validation
    valid_methods = {'GET', 'POST', 'PUT', 'DELETE', 'PATCH'}
    if method.upper() not in valid_methods:
        raise ValueError(f"Invalid HTTP method: {method}")
    
    # 3. Endpoint validation
    if not re.match(r'^/[a-zA-Z0-9/_\-]*$', endpoint):
        raise ValueError("Invalid endpoint format")
    
    if len(endpoint) > 200:
        raise ValueError("Endpoint path too long")
    
    # 4. Parameter validation
    if len(params) > 50:
        raise ValueError("Too many parameters")
    
    for key, value in params.items():
        # Key validation
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', key):
            raise ValueError(f"Invalid parameter key: {key}")
        
        # Value validation (convert to string for checking)
        str_value = str(value)
        if len(str_value) > 1000:
            raise ValueError(f"Parameter value too long: {key}")
    
    # 5. Security checks
    combined_data = f"{endpoint} {method} {params}".lower()
    if any(dangerous in combined_data for dangerous in ['../', '..\\', 'union select', '<script']):
        raise ValueError("Request contains potentially dangerous content")
    
    # Simulate secure API call
    time.sleep(0.5)
    
    result = f"Secure {method} to {endpoint} with {len(params)} parameters"
    print(f"✅ {result}")
    
    return result


# ===== INSECURE PATTERNS (FOR DEMONSTRATION) =====

def insecure_function_example():
    """
    This function demonstrates patterns that CANNOT be used with JSON serialization.
    These would only work with pickle (which is insecure).
    """
    # These patterns don't work with JSON serialization:
    
    # 1. Lambda functions
    # bad_lambda = lambda x: x * 2  # Cannot be imported
    
    # 2. Nested functions
    # def nested():
    #     return "nested"
    
    # 3. Functions with non-serializable arguments
    # complex_object = SomeComplexClass()
    
    print("❌ This demonstrates patterns that don't work with secure JSON serialization")
    return "These patterns require the insecure pickle serializer"


# ===== DEMONSTRATION FUNCTIONS =====

def demonstrate_secure_patterns():
    """
    Demonstrate secure job patterns with comprehensive validation.
    """
    print("🔒 Secure Job Patterns Demo")
    print("-" * 40)
    
    with SyncClient() as client:
        jobs = []
        
        # 1. Secure user processing
        print("📤 Secure user processing jobs...")
        user_jobs = [
            (1001, "create", {"name": "Alice Johnson", "email": "alice@example.com"}),
            (1002, "update", {"name": "Bob Smith", "email": "bob@company.com"}),
            (1003, "read", {"name": "Carol Davis", "email": "carol@service.org"}),
        ]
        
        for user_id, operation, data in user_jobs:
            job = client.enqueue(
                secure_user_processing,
                user_id=user_id,
                operation=operation,
                data=data,
                queue_name="secure_queue"
            )
            jobs.append(job)
            print(f"  ✅ User job: {operation} user {user_id} ({job.job_id})")
        
        # 2. Secure content processing
        print("\n📤 Secure content processing jobs...")
        content_jobs = [
            ("Getting Started with NAQ", "This guide explains how to use NAQ securely...", "documentation"),
            ("Weekly Newsletter", "This week's updates and important announcements...", "news"),
            ("API Best Practices", "Learn how to build secure and efficient APIs...", "blog"),
        ]
        
        for title, content, category in content_jobs:
            job = client.enqueue(
                secure_content_processing,
                title=title,
                content=content,
                category=category,
                queue_name="secure_queue"
            )
            jobs.append(job)
            print(f"  ✅ Content job: {title[:30]}... ({job.job_id})")
        
        # 3. Secure file processing
        print("\n📤 Secure file processing jobs...")
        file_jobs = [
            ("report.pdf", "pdf", 5),
            ("image.jpg", "jpg", 2),
            ("data.csv", "csv", 8),
            ("document.docx", "docx", 3),
        ]
        
        for filename, file_type, size in file_jobs:
            job = client.enqueue(
                secure_file_processing,
                filename=filename,
                file_type=file_type,
                size_mb=size,
                queue_name="secure_queue"
            )
            jobs.append(job)
            print(f"  ✅ File job: {filename} ({job.job_id})")
        
        # 4. Secure API calls
        print("\n📤 Secure API call jobs...")
        api_jobs = [
            ("/users/profile", "GET", {"user_id": "123"}),
            ("/orders", "POST", {"user_id": "456", "product": "widget"}),
            ("/notifications", "PUT", {"id": "789", "read": True}),
        ]
        
        for endpoint, method, params in api_jobs:
            job = client.enqueue(
                secure_api_call,
                endpoint=endpoint,
                method=method,
                params=params,
                queue_name="secure_queue"
            )
            jobs.append(job)
            print(f"  ✅ API job: {method} {endpoint} ({job.job_id})")
        
        return jobs


def demonstrate_invalid_patterns():
    """
    Demonstrate what happens with invalid inputs (these will fail gracefully).
    """
    print("\n⚠️  Invalid Input Handling Demo")
    print("-" * 40)
    
    with SyncClient() as client:
        jobs = []
        
        # These jobs will fail with validation errors (which is good!)
        print("📤 Jobs that will fail validation (demonstrating security)...")
        
        invalid_jobs = [
            # Invalid user ID
            ("secure_user_processing", {
                "user_id": -1,  # Invalid: negative
                "operation": "create",
                "data": {"name": "Test", "email": "test@example.com"}
            }),
            
            # Invalid operation
            ("secure_user_processing", {
                "user_id": 1004,
                "operation": "hack",  # Invalid: not in allowed operations
                "data": {"name": "Test", "email": "test@example.com"}
            }),
            
            # Invalid content (too long title)
            ("secure_content_processing", {
                "title": "x" * 150,  # Invalid: too long
                "content": "Valid content",
                "category": "blog"
            }),
            
            # Invalid file type
            ("secure_file_processing", {
                "filename": "malware.exe",
                "file_type": "exe",  # Invalid: dangerous file type
                "size_mb": 1
            }),
        ]
        
        for func_name, kwargs in invalid_jobs:
            try:
                if func_name == "secure_user_processing":
                    job = client.enqueue(secure_user_processing, **kwargs, queue_name="secure_queue")
                elif func_name == "secure_content_processing":
                    job = client.enqueue(secure_content_processing, **kwargs, queue_name="secure_queue")
                elif func_name == "secure_file_processing":
                    job = client.enqueue(secure_file_processing, **kwargs, queue_name="secure_queue")
                
                jobs.append(job)
                print(f"  ⚠️  Invalid job enqueued: {job.job_id} (will fail validation)")
            except Exception as e:
                # This is expected for some validation errors
                print(f"  ✅ Caught validation error (good!): {e}")
        
        return jobs


def main():
    """
    Main function demonstrating secure NAQ patterns.
    """
    print("🛡️  NAQ Security Best Practices Demo")
    print("=" * 50)
    
    # Check if we're using secure serialization
    serializer = os.environ.get('NAQ_JOB_SERIALIZER', 'pickle')
    if serializer != 'json':
        print("❌ SECURITY WARNING: Not using JSON serialization!")
        print("   Set: export NAQ_JOB_SERIALIZER=json")
        print("   This demo requires secure JSON serialization.")
        return 1
    else:
        print("✅ Using secure JSON serialization")
    
    try:
        # Demonstrate secure patterns
        secure_jobs = demonstrate_secure_patterns()
        invalid_jobs = demonstrate_invalid_patterns()
        
        total_jobs = len(secure_jobs) + len(invalid_jobs)
        
        print(f"\n🎉 Security demo completed!")
        print(f"📊 Enqueued {total_jobs} jobs total:")
        print(f"   ✅ {len(secure_jobs)} secure jobs (will succeed)")
        print(f"   ⚠️  {len(invalid_jobs)} invalid jobs (will fail validation)")
        
        print("\n🛡️  Security Highlights:")
        print("   • JSON serialization prevents code execution")
        print("   • All inputs are validated and sanitized")
        print("   • Dangerous patterns are detected and rejected")
        print("   • Type checking prevents injection attacks")
        print("   • Size limits prevent resource exhaustion")
        
        print("\n📋 Production Security Checklist:")
        print("   ✅ Use NAQ_JOB_SERIALIZER=json")
        print("   ✅ Validate all job function inputs")
        print("   ✅ Sanitize string data")
        print("   ✅ Implement size and type limits")
        print("   ✅ Monitor for security warnings")
        print("   ✅ Use NATS authentication/TLS")
        print("   ✅ Run workers with minimal privileges")
        
        print("\n💡 Watch the worker logs to see:")
        print("   • Secure jobs processing successfully")
        print("   • Invalid jobs failing with clear error messages")
        print("   • Security validation in action")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\n🔧 Troubleshooting:")
        print("   - Is NATS running? (cd docker && docker-compose up -d)")
        print("   - Is NAQ_JOB_SERIALIZER=json set?")
        print("   - Are workers running? (naq worker secure_queue)")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())