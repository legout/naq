#!/usr/bin/env python3
"""
Complete Email System - Main Implementation

This example demonstrates a comprehensive email system built with NAQ,
including campaign management, transactional emails, template processing,
delivery tracking, and bounce handling.

Before running:
1. Start NATS: `cd docker && docker-compose up -d`
2. Set secure serializer: `export NAQ_JOB_SERIALIZER=json`
3. Configure email provider (see README.md)
4. Start workers: `naq worker default email_campaign email_transactional email_ab_test --log-level INFO`
5. Run this script: `python email_system.py`
"""

import os
import time
import uuid
import random
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from html import escape

from naq import SyncClient, setup_logging

# Configure secure JSON serialization
os.environ.setdefault('NAQ_JOB_SERIALIZER', 'json')

# Setup logging
setup_logging(level="INFO")


# ===== EMAIL SENDING FUNCTIONS =====

def send_transactional_email(template: str, to: str, context: Dict[str, Any], 
                           user_id: Optional[str] = None) -> str:
    """
    Send a transactional email using a template.
    
    Args:
        template: Email template name
        to: Recipient email address
        context: Template context data
        user_id: Optional user ID for tracking
        
    Returns:
        Result message
    """
    email_id = str(uuid.uuid4())
    
    print(f"📧 Sending transactional email: {template} to {to}")
    print(f"   Email ID: {email_id}")
    
    # Simulate template rendering
    subject = f"NAQ System - {template.replace('_', ' ').title()}"
    
    # Basic template rendering (in production, use Jinja2 or similar)
    if template == "welcome":
        subject = "Welcome to NAQ!"
        content = f"Hello {context.get('name', 'Friend')}, welcome to our service!"
    elif template == "order_confirmation":
        subject = f"Order Confirmation - {context.get('order', {}).get('id', 'N/A')}"
        content = f"Your order has been confirmed. Total: ${context.get('total', 0):.2f}"
    elif template == "password_reset":
        subject = "Password Reset Request"
        content = f"Hello {context.get('name', 'Friend')}, click here to reset your password."
    else:
        content = f"Email content for {template} template."
    
    # Simulate email sending
    time.sleep(0.5)  # Simulate network delay
    
    # Random failure simulation (5% chance)
    if random.random() < 0.05:
        raise RuntimeError(f"Email delivery failed for {email_id}")
    
    result = f"Transactional email sent: {template} to {to} ({email_id})"
    print(f"✅ {result}")
    
    return result


def send_campaign_batch(campaign_id: str, users: List[Dict[str, Any]], 
                       segment: str) -> str:
    """
    Send campaign emails to a batch of users.
    
    Args:
        campaign_id: Campaign identifier
        users: List of user dictionaries
        segment: User segment name
        
    Returns:
        Batch processing result
    """
    print(f"📮 Processing campaign batch: {campaign_id}")
    print(f"   Segment: {segment}, Users: {len(users)}")
    
    sent_count = 0
    failed_count = 0
    
    for user in users:
        try:
            # Simulate sending to each user
            print(f"   📧 Sending to {user.get('email', 'unknown')}")
            time.sleep(0.1)  # Simulate processing time
            
            # Random failure simulation (2% chance)
            if random.random() < 0.02:
                failed_count += 1
                print(f"   ❌ Failed to send to {user.get('email')}")
            else:
                sent_count += 1
                print(f"   ✅ Sent to {user.get('email')}")
        except Exception as e:
            failed_count += 1
            print(f"   ❌ Error sending to {user.get('email', 'unknown')}: {e}")
    
    result = f"Campaign batch {campaign_id} processed: {sent_count} sent, {failed_count} failed"
    print(f"📊 {result}")
    
    return result


def send_email_with_retries(email_data: Dict[str, Any]) -> str:
    """
    Send email with comprehensive retry handling.
    
    Args:
        email_data: Email details including recipient, subject, content
        
    Returns:
        Send result
    """
    email_id = email_data.get('id', str(uuid.uuid4()))
    to = email_data.get('to', 'unknown@example.com')
    subject = email_data.get('subject', 'No Subject')
    
    print(f"📤 Sending email with retries: {email_id}")
    print(f"   To: {to}")
    print(f"   Subject: {subject}")
    
    # Simulate email provider API call
    time.sleep(0.8)  # Simulate network delay
    
    # Simulate various failure types
    failure_chance = random.random()
    
    if failure_chance < 0.15:  # 15% chance of temporary failure
        print(f"⏱️  Temporary failure for {email_id} (will retry)")
        raise ConnectionError(f"Temporary network failure for email {email_id}")
    elif failure_chance < 0.2:  # 5% chance of rate limiting
        print(f"🚦 Rate limited for {email_id} (will retry)")
        raise RuntimeError(f"Rate limit exceeded for email {email_id}")
    
    # Success
    result = f"Email sent successfully: {email_id} to {to}"
    print(f"✅ {result}")
    
    return result


def process_email_bounce(bounce_data: Dict[str, Any]) -> str:
    """
    Process email bounce notifications.
    
    Args:
        bounce_data: Bounce notification data
        
    Returns:
        Processing result
    """
    email_address = bounce_data.get('email', 'unknown')
    bounce_type = bounce_data.get('type', 'unknown')
    reason = bounce_data.get('reason', 'No reason provided')
    
    print(f"🚫 Processing email bounce: {email_address}")
    print(f"   Type: {bounce_type}")
    print(f"   Reason: {reason}")
    
    if bounce_type == 'hard':
        print(f"   🔒 Adding {email_address} to permanent suppression list")
        # In production: add to database suppression list
        action = "Added to permanent suppression list"
    elif bounce_type == 'soft':
        print(f"   ⚠️  Soft bounce for {email_address} - tracking for future suppression")
        # In production: increment soft bounce counter
        action = "Incremented soft bounce counter"
    elif bounce_type == 'complaint':
        print(f"   🚨 Spam complaint from {email_address} - immediate suppression")
        # In production: add to suppression list and alert team
        action = "Added to suppression list due to spam complaint"
    else:
        print(f"   ❓ Unknown bounce type: {bounce_type}")
        action = "Logged unknown bounce type"
    
    result = f"Bounce processed: {email_address} ({bounce_type}) - {action}"
    print(f"✅ {result}")
    
    return result


# ===== SUPPORT FUNCTIONS =====

def generate_test_users(count: int = 50) -> List[Dict[str, Any]]:
    """Generate test user data for demonstrations."""
    users = []
    names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"]
    domains = ["example.com", "test.org", "demo.net", "sample.co"]
    
    for i in range(count):
        name = random.choice(names)
        domain = random.choice(domains)
        users.append({
            'id': i + 1,
            'name': f"{name} {i+1}",
            'email': f"{name.lower()}{i+1}@{domain}",
            'segment': random.choice(['premium', 'standard', 'trial']),
            'preferences': {
                'language': random.choice(['en', 'es', 'fr']),
                'timezone': random.choice(['UTC', 'EST', 'PST'])
            }
        })
    
    return users


# ===== DEMONSTRATION FUNCTIONS =====

def demonstrate_transactional_emails():
    """Demonstrate transactional email patterns."""
    print("📍 Transactional Emails Demo")
    print("-" * 40)
    
    with SyncClient() as client:
        jobs = []
        
        # Welcome email series
        print("📤 Welcome email series...")
        welcome_job = client.enqueue(
            send_transactional_email,
            template="welcome",
            to="alice@example.com",
            context={"name": "Alice Johnson"},
            user_id="user_1001",
            queue_name="email_transactional",
            priority="high"
        )
        jobs.append(welcome_job)
        print(f"  ✅ Welcome email: {welcome_job.job_id}")
        
        # Order confirmation
        print("\n📤 Order confirmation...")
        order_job = client.enqueue(
            send_transactional_email,
            template="order_confirmation",
            to="bob@company.com",
            context={
                "name": "Bob Smith",
                "order": {"id": "ORD-12345"},
                "total": 129.99
            },
            queue_name="email_transactional",
            priority="high"
        )
        jobs.append(order_job)
        print(f"  ✅ Order confirmation: {order_job.job_id}")
        
        # Password reset
        print("\n📤 Password reset...")
        reset_job = client.enqueue(
            send_transactional_email,
            template="password_reset",
            to="charlie@service.org",
            context={"name": "Charlie Davis"},
            queue_name="email_transactional"
        )
        jobs.append(reset_job)
        print(f"  ✅ Password reset: {reset_job.job_id}")
        
        return jobs


def demonstrate_campaign_emails():
    """Demonstrate campaign email patterns."""
    print("\n📍 Campaign Emails Demo")
    print("-" * 40)
    
    with SyncClient() as client:
        jobs = []
        users = generate_test_users(20)
        
        # Segment users
        premium_users = [u for u in users if u['segment'] == 'premium']
        standard_users = [u for u in users if u['segment'] == 'standard']
        
        # Premium segment campaign
        if premium_users:
            print(f"📤 Premium segment campaign ({len(premium_users)} users)...")
            premium_job = client.enqueue(
                send_campaign_batch,
                campaign_id="CAMP-PREMIUM-001",
                users=premium_users,
                segment="premium",
                queue_name="email_campaign",
                priority="normal"
            )
            jobs.append(premium_job)
            print(f"  ✅ Premium campaign: {premium_job.job_id}")
        
        # Standard segment campaign
        if standard_users:
            print(f"📤 Standard segment campaign ({len(standard_users)} users)...")
            standard_job = client.enqueue(
                send_campaign_batch,
                campaign_id="CAMP-STANDARD-001", 
                users=standard_users,
                segment="standard",
                queue_name="email_campaign"
            )
            jobs.append(standard_job)
            print(f"  ✅ Standard campaign: {standard_job.job_id}")
        
        return jobs


def demonstrate_retry_patterns():
    """Demonstrate email retry patterns."""
    print("\n📍 Email Retry Patterns Demo")
    print("-" * 40)
    
    with SyncClient() as client:
        jobs = []
        
        # High-failure rate emails with retries
        print("📤 Emails with retry handling...")
        
        for i in range(5):
            email_data = {
                'id': f"email-retry-{i+1}",
                'to': f"retry{i+1}@example.com",
                'subject': f"Test Email with Retries #{i+1}",
                'content': f"This is test email #{i+1} with retry handling"
            }
            
            job = client.enqueue(
                send_email_with_retries,
                email_data=email_data,
                queue_name="email_transactional",
                max_retries=3,
                retry_delay=2,
                retry_strategy="exponential"  # 2s, 4s, 8s delays
            )
            jobs.append(job)
            print(f"  ✅ Retry email {i+1}: {job.job_id}")
        
        return jobs


def demonstrate_bounce_handling():
    """Demonstrate bounce handling patterns."""
    print("\n📍 Bounce Handling Demo")
    print("-" * 40)
    
    with SyncClient() as client:
        jobs = []
        
        # Simulate different bounce types
        bounce_scenarios = [
            {
                'email': 'nonexistent@invalid-domain.xyz',
                'type': 'hard',
                'reason': 'Domain not found'
            },
            {
                'email': 'temporary.issue@example.com',
                'type': 'soft', 
                'reason': 'Mailbox temporarily unavailable'
            },
            {
                'email': 'spam.reporter@example.com',
                'type': 'complaint',
                'reason': 'Recipient marked as spam'
            }
        ]
        
        print("📤 Processing bounce notifications...")
        for bounce in bounce_scenarios:
            job = client.enqueue(
                process_email_bounce,
                bounce_data=bounce,
                queue_name="email_transactional"
            )
            jobs.append(job)
            print(f"  ✅ Bounce handling: {bounce['type']} - {job.job_id}")
        
        return jobs


def main():
    """
    Main function demonstrating the complete email system.
    """
    print("📧 NAQ Complete Email System Demo")
    print("=" * 50)
    
    # Check configuration
    serializer = os.environ.get('NAQ_JOB_SERIALIZER', 'pickle')
    if serializer != 'json':
        print("❌ SECURITY WARNING: Not using JSON serialization!")
        print("   Set: export NAQ_JOB_SERIALIZER=json")
        return 1
    else:
        print("✅ Using secure JSON serialization")
    
    try:
        # Run demonstrations
        transactional_jobs = demonstrate_transactional_emails()
        campaign_jobs = demonstrate_campaign_emails()
        retry_jobs = demonstrate_retry_patterns()
        bounce_jobs = demonstrate_bounce_handling()
        
        # Summary
        total_jobs = len(transactional_jobs) + len(campaign_jobs) + len(retry_jobs) + len(bounce_jobs)
        
        print(f"\n🎉 Email system demo completed!")
        print(f"📊 Enqueued {total_jobs} jobs total:")
        print(f"   📧 {len(transactional_jobs)} transactional emails")
        print(f"   📮 {len(campaign_jobs)} campaign batches")
        print(f"   🔄 {len(retry_jobs)} retry-enabled emails")
        print(f"   🚫 {len(bounce_jobs)} bounce handlers")
        
        print("\n🛡️  Email System Features Demonstrated:")
        print("   • Transactional email processing")
        print("   • Campaign batch processing")
        print("   • Segmented user targeting")
        print("   • Retry handling for failures")
        print("   • Bounce and complaint processing")
        print("   • Priority queue management")
        print("   • Secure job serialization")
        
        print("\n💡 Watch your worker logs to see:")
        print("   - Email processing in real-time")
        print("   - Retry attempts for failed emails")
        print("   - Bounce handling and suppression")
        print("   - Campaign batch processing")
        
        print("\n📋 Next Steps:")
        print("   - Try campaign_manager.py for advanced campaign features")
        print("   - Check template_processor.py for template management")
        print("   - Implement actual email provider integration")
        print("   - Add database for tracking and analytics")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\n🔧 Troubleshooting:")
        print("   - Is NATS running? (cd docker && docker-compose up -d)")
        print("   - Are workers running? (naq worker default email_campaign email_transactional)")
        print("   - Is NAQ_JOB_SERIALIZER=json set?")
        print("   - Check email provider configuration")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())