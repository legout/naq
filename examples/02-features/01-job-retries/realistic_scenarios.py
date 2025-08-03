#!/usr/bin/env python3
"""
Realistic Retry Scenarios

This example demonstrates real-world retry patterns:
- API calls with different error types
- Database operations with connection issues
- File processing with temporary failures
- Email sending with transient errors

Before running:
1. Start NATS: `cd docker && docker-compose up -d`
2. Set secure serializer: `export NAQ_JOB_SERIALIZER=json`
3. Start worker: `naq worker default api_queue db_queue file_queue email_queue --log-level DEBUG`
4. Run this script: `python realistic_scenarios.py`
"""

import os
import random
import time
from typing import Dict, Any

from naq import SyncClient, setup_logging

# Configure secure JSON serialization
os.environ.setdefault('NAQ_JOB_SERIALIZER', 'json')

# Setup logging
setup_logging(level="INFO")


class APIError(Exception):
    """Custom exception for API errors."""
    pass


class AuthenticationError(Exception):
    """Authentication errors should not be retried."""
    pass


class DatabaseConnectionError(Exception):
    """Database connection errors."""
    pass


class FileSystemError(Exception):
    """File system errors."""
    pass


class EmailDeliveryError(Exception):
    """Email delivery errors."""
    pass


def call_external_api(endpoint: str, user_id: int) -> Dict[str, Any]:
    """
    Simulate calling an external API with various failure modes.
    
    Args:
        endpoint: API endpoint to call
        user_id: User ID for the request
        
    Returns:
        API response data
        
    Raises:
        APIError: For retryable API errors
        AuthenticationError: For non-retryable auth errors
        ConnectionError: For network issues
        TimeoutError: For timeout issues
    """
    print(f"🌐 Calling API {endpoint} for user {user_id}")
    
    # Simulate API call delay
    time.sleep(random.uniform(0.5, 1.5))
    
    failure_type = random.random()
    
    if failure_type < 0.1:  # 10% auth errors (don't retry)
        print(f"🔐 Authentication failed for user {user_id}")
        raise AuthenticationError(f"Invalid token for user {user_id}")
    elif failure_type < 0.3:  # 20% connection errors (retry)
        print(f"🔌 Connection lost to {endpoint}")
        raise ConnectionError(f"Network connection failed")
    elif failure_type < 0.5:  # 20% timeout errors (retry)
        print(f"⏱️  Request to {endpoint} timed out")
        raise TimeoutError(f"API request timeout")
    elif failure_type < 0.6:  # 10% API errors (retry)
        print(f"⚠️  API error from {endpoint}")
        raise APIError(f"Internal API error: {random.choice(['Rate limit', 'Service unavailable', 'Internal error'])}")
    else:  # 40% success
        print(f"✅ API call to {endpoint} successful")
        return {
            "user_id": user_id,
            "endpoint": endpoint,
            "data": f"User data for {user_id}",
            "timestamp": time.time()
        }


def process_database_record(record_id: int, operation: str) -> str:
    """
    Simulate database operations with connection issues.
    
    Args:
        record_id: Database record ID
        operation: Type of operation (SELECT, UPDATE, INSERT)
        
    Returns:
        Operation result
        
    Raises:
        DatabaseConnectionError: For connection issues (retryable)
        ValueError: For invalid data (not retryable)
    """
    print(f"🗄️  Database {operation} for record {record_id}")
    
    # Simulate database operation delay
    time.sleep(random.uniform(0.2, 0.8))
    
    failure_type = random.random()
    
    if failure_type < 0.05:  # 5% invalid data (don't retry)
        print(f"❌ Invalid data for record {record_id}")
        raise ValueError(f"Record {record_id} has invalid data format")
    elif failure_type < 0.25:  # 20% connection errors (retry)
        print(f"🔌 Database connection lost")
        raise DatabaseConnectionError("Connection to database lost")
    elif failure_type < 0.35:  # 10% timeout (retry)
        print(f"⏱️  Database operation timed out")
        raise DatabaseConnectionError("Database operation timeout")
    else:  # 65% success
        print(f"✅ Database {operation} successful for record {record_id}")
        return f"Database {operation} completed for record {record_id}"


def process_file_upload(filename: str, file_size: int) -> str:
    """
    Simulate file processing with temporary failures.
    
    Args:
        filename: Name of file to process
        file_size: Size of file in MB
        
    Returns:
        Processing result
        
    Raises:
        FileSystemError: For temporary file system issues (retryable)
        ValueError: For unsupported file types (not retryable)
    """
    print(f"📁 Processing file: {filename} ({file_size}MB)")
    
    # Simulate processing time based on file size
    processing_time = file_size * 0.1  # 0.1 seconds per MB
    time.sleep(processing_time)
    
    failure_type = random.random()
    
    if failure_type < 0.05:  # 5% unsupported format (don't retry)
        print(f"❌ Unsupported file format: {filename}")
        raise ValueError(f"File format not supported: {filename}")
    elif failure_type < 0.2:  # 15% disk space issues (retry)
        print(f"💾 Insufficient disk space for {filename}")
        raise FileSystemError("Insufficient disk space")
    elif failure_type < 0.3:  # 10% permission issues (retry)
        print(f"🔒 Permission denied for {filename}")
        raise FileSystemError("Permission denied")
    else:  # 70% success
        print(f"✅ File {filename} processed successfully")
        return f"File {filename} ({file_size}MB) processed and uploaded"


def send_email_notification(email: str, subject: str, template: str) -> str:
    """
    Simulate email sending with delivery issues.
    
    Args:
        email: Recipient email address
        subject: Email subject
        template: Email template name
        
    Returns:
        Delivery confirmation
        
    Raises:
        EmailDeliveryError: For delivery issues (retryable)
        ValueError: For invalid email format (not retryable)
    """
    print(f"📧 Sending email to {email}: {subject}")
    
    # Simulate email sending delay
    time.sleep(random.uniform(0.3, 1.0))
    
    failure_type = random.random()
    
    if failure_type < 0.02:  # 2% invalid email (don't retry)
        print(f"❌ Invalid email format: {email}")
        raise ValueError(f"Invalid email address: {email}")
    elif failure_type < 0.15:  # 13% SMTP errors (retry)
        print(f"📮 SMTP server error")
        raise EmailDeliveryError("SMTP server temporarily unavailable")
    elif failure_type < 0.25:  # 10% rate limiting (retry)
        print(f"🚫 Rate limit exceeded")
        raise EmailDeliveryError("Email rate limit exceeded")
    else:  # 75% success
        print(f"✅ Email sent successfully to {email}")
        return f"Email '{subject}' delivered to {email}"


def demonstrate_api_retry_patterns():
    """
    Demonstrate API call retry patterns with selective error handling.
    """
    print("🌐 API Retry Patterns")
    print("-" * 30)
    
    with SyncClient() as client:
        jobs = []
        
        # API calls with selective retries
        api_endpoints = [
            "/users/profile",
            "/orders/recent", 
            "/notifications/unread",
            "/reports/daily"
        ]
        
        for i, endpoint in enumerate(api_endpoints):
            job = client.enqueue(
                call_external_api,
                endpoint=endpoint,
                user_id=1000 + i,
                queue_name="api_queue",
                max_retries=4,
                retry_delay=3,
                retry_strategy="exponential",
                # Only retry network and API errors, not auth errors
                retry_on=(ConnectionError, TimeoutError, APIError),
                ignore_on=(AuthenticationError,)
            )
            jobs.append(job)
            print(f"  📤 API job {i+1}: {endpoint} ({job.job_id})")
        
        return jobs


def demonstrate_database_retry_patterns():
    """
    Demonstrate database operation retry patterns.
    """
    print("\n🗄️  Database Retry Patterns")
    print("-" * 30)
    
    with SyncClient() as client:
        jobs = []
        
        # Database operations
        operations = [
            (101, "SELECT"),
            (102, "UPDATE"),
            (103, "INSERT"),
            (104, "DELETE")
        ]
        
        for record_id, operation in operations:
            job = client.enqueue(
                process_database_record,
                record_id=record_id,
                operation=operation,
                queue_name="db_queue",
                max_retries=5,  # Database issues might need more retries
                retry_delay=[2, 4, 8, 16, 32],  # Exponential backoff
                retry_strategy="linear",  # Use linear with custom delays
                retry_on=(DatabaseConnectionError,),
                ignore_on=(ValueError,)  # Don't retry data validation errors
            )
            jobs.append(job)
            print(f"  📤 DB job: {operation} record {record_id} ({job.job_id})")
        
        return jobs


def demonstrate_file_processing_retries():
    """
    Demonstrate file processing retry patterns.
    """
    print("\n📁 File Processing Retry Patterns")
    print("-" * 35)
    
    with SyncClient() as client:
        jobs = []
        
        # File processing jobs
        files = [
            ("document.pdf", 5),
            ("image.jpg", 2),
            ("video.mp4", 50),
            ("data.csv", 10)
        ]
        
        for filename, size in files:
            job = client.enqueue(
                process_file_upload,
                filename=filename,
                file_size=size,
                queue_name="file_queue",
                max_retries=3,
                retry_delay=5,  # Fixed delay for file operations
                retry_strategy="linear",
                retry_on=(FileSystemError,),
                ignore_on=(ValueError,)  # Don't retry format errors
            )
            jobs.append(job)
            print(f"  📤 File job: {filename} ({size}MB) ({job.job_id})")
        
        return jobs


def demonstrate_email_retry_patterns():
    """
    Demonstrate email sending retry patterns.
    """
    print("\n📧 Email Retry Patterns")
    print("-" * 25)
    
    with SyncClient() as client:
        jobs = []
        
        # Email sending jobs
        emails = [
            ("user@example.com", "Welcome!", "welcome"),
            ("admin@company.com", "Daily Report", "report"),
            ("customer@domain.com", "Order Confirmation", "order"),
            ("support@service.com", "Ticket Update", "support")
        ]
        
        for email, subject, template in emails:
            job = client.enqueue(
                send_email_notification,
                email=email,
                subject=subject,
                template=template,
                queue_name="email_queue",
                max_retries=3,
                retry_delay=10,  # Longer delays for email to avoid rate limits
                retry_strategy="linear",
                retry_on=(EmailDeliveryError,),
                ignore_on=(ValueError,)  # Don't retry invalid addresses
            )
            jobs.append(job)
            print(f"  📤 Email job: {subject} to {email} ({job.job_id})")
        
        return jobs


def main():
    """
    Main function demonstrating realistic retry scenarios.
    """
    print("🚀 NAQ Realistic Retry Scenarios")
    print("=" * 50)
    
    try:
        # Demonstrate different real-world scenarios
        api_jobs = demonstrate_api_retry_patterns()
        db_jobs = demonstrate_database_retry_patterns()
        file_jobs = demonstrate_file_processing_retries()
        email_jobs = demonstrate_email_retry_patterns()
        
        all_jobs = api_jobs + db_jobs + file_jobs + email_jobs
        
        print(f"\n🎉 Enqueued {len(all_jobs)} realistic jobs!")
        
        print("\n" + "=" * 50)
        print("📊 Scenario Summary:")
        print("=" * 50)
        print(f"API calls: {len(api_jobs)} jobs (selective retries)")
        print(f"Database ops: {len(db_jobs)} jobs (connection retries only)")
        print(f"File processing: {len(file_jobs)} jobs (filesystem retries only)")
        print(f"Email sending: {len(email_jobs)} jobs (delivery retries only)")
        
        print("\n🎯 Retry Strategy Highlights:")
        print("   • API: Retry network/API errors, skip auth errors")
        print("   • Database: Retry connection issues, skip data errors")
        print("   • Files: Retry filesystem issues, skip format errors")
        print("   • Email: Retry delivery issues, skip invalid addresses")
        
        print("\n💡 Watch for these patterns:")
        print("   • Jobs that fail immediately (ignored errors)")
        print("   • Jobs that retry multiple times (retryable errors)")
        print("   • Different retry delays per scenario")
        print("   • Final success after retries")
        
        print("\n📋 Production Tips:")
        print("   • Monitor retry patterns to identify systemic issues")
        print("   • Adjust retry counts based on real failure rates")
        print("   • Use appropriate delays for different service types")
        print("   • Implement circuit breakers for external services")
        print("   • Set up alerts for excessive retry rates")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\n🔧 Troubleshooting:")
        print("   - Is NATS running? (cd docker && docker-compose up -d)")
        print("   - Are workers running for all queues?")
        print("   - Is NAQ_JOB_SERIALIZER=json set?")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())