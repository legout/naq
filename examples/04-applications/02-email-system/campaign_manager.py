#!/usr/bin/env python3
"""
Email Campaign Manager - Advanced Campaign Features

This example demonstrates advanced campaign management features:
- Campaign creation and scheduling
- A/B testing with multiple variants
- Segmented user targeting
- Campaign performance tracking
- Automated winner selection

Before running:
1. Start NATS: `cd docker && docker-compose up -d`
2. Set secure serializer: `export NAQ_JOB_SERIALIZER=json`
3. Start workers: `naq worker default email_campaign email_ab_test --log-level INFO`
4. Start scheduler: `naq scheduler --log-level INFO`
5. Run this script: `python campaign_manager.py`
"""

import os
import time
import uuid
import random
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

from naq import SyncClient, setup_logging

# Configure secure JSON serialization
os.environ.setdefault('NAQ_JOB_SERIALIZER', 'json')

# Setup logging
setup_logging(level="INFO")


# ===== CAMPAIGN MANAGEMENT FUNCTIONS =====

def create_campaign(campaign_data: Dict[str, Any]) -> str:
    """
    Create and initialize a new email campaign.
    
    Args:
        campaign_data: Campaign configuration data
        
    Returns:
        Campaign creation result
    """
    campaign_id = campaign_data.get('id', str(uuid.uuid4()))
    name = campaign_data.get('name', 'Untitled Campaign')
    
    print(f"📊 Creating campaign: {name}")
    print(f"   Campaign ID: {campaign_id}")
    print(f"   Subject: {campaign_data.get('subject', 'No Subject')}")
    print(f"   Template: {campaign_data.get('template', 'default')}")
    
    # Simulate campaign creation in database
    time.sleep(0.5)
    
    # Store campaign metadata (in production, save to database)
    campaign_record = {
        'id': campaign_id,
        'name': name,
        'subject': campaign_data.get('subject'),
        'template': campaign_data.get('template'),
        'segments': campaign_data.get('segments', []),
        'status': 'created',
        'created_at': datetime.utcnow().isoformat(),
        'metrics': {
            'sent': 0,
            'delivered': 0,
            'opened': 0,
            'clicked': 0,
            'bounced': 0
        }
    }
    
    result = f"Campaign created: {name} ({campaign_id})"
    print(f"✅ {result}")
    
    return result


def send_campaign_to_segment(campaign_id: str, segment_name: str, 
                           users: List[Dict[str, Any]], template: str) -> str:
    """
    Send campaign to a specific user segment.
    
    Args:
        campaign_id: Campaign identifier
        segment_name: Name of the user segment
        users: List of users in this segment
        template: Email template to use
        
    Returns:
        Segment processing result
    """
    print(f"🎯 Sending campaign to segment: {segment_name}")
    print(f"   Campaign: {campaign_id}")
    print(f"   Users: {len(users)}")
    print(f"   Template: {template}")
    
    sent_count = 0
    batch_size = 10  # Process in small batches
    
    # Process users in batches
    for i in range(0, len(users), batch_size):
        batch = users[i:i + batch_size]
        
        print(f"   📤 Processing batch {i//batch_size + 1}: {len(batch)} users")
        
        for user in batch:
            try:
                # Simulate email sending
                time.sleep(0.1)
                
                # Random failure simulation (3% chance)
                if random.random() < 0.03:
                    print(f"     ❌ Failed: {user.get('email')}")
                else:
                    sent_count += 1
                    print(f"     ✅ Sent: {user.get('email')}")
                    
            except Exception as e:
                print(f"     ❌ Error sending to {user.get('email')}: {e}")
        
        # Brief pause between batches
        time.sleep(0.2)
    
    # Update campaign metrics (in production, update database)
    print(f"   📊 Segment results: {sent_count}/{len(users)} sent")
    
    result = f"Campaign {campaign_id} sent to {segment_name}: {sent_count}/{len(users)} successful"
    print(f"✅ {result}")
    
    return result


def send_ab_test_variant(campaign_id: str, variant_name: str, 
                        variant_config: Dict[str, Any], users: List[Dict[str, Any]]) -> str:
    """
    Send A/B test variant to selected users.
    
    Args:
        campaign_id: Campaign identifier
        variant_name: Name of the variant (A, B, C, etc.)
        variant_config: Variant configuration (subject, template, etc.)
        users: Users to receive this variant
        
    Returns:
        Variant send result
    """
    print(f"🔬 Sending A/B test variant: {variant_name}")
    print(f"   Campaign: {campaign_id}")
    print(f"   Subject: {variant_config.get('subject', 'No Subject')}")
    print(f"   Users: {len(users)}")
    
    # Track variant metrics
    variant_metrics = {
        'variant': variant_name,
        'sent': 0,
        'delivered': 0,
        'opened': 0,
        'clicked': 0,
        'send_time': datetime.utcnow().isoformat()
    }
    
    sent_count = 0
    
    for user in users:
        try:
            # Simulate sending with variant-specific template
            time.sleep(0.1)
            
            # Random failure simulation (2% chance)
            if random.random() < 0.02:
                print(f"   ❌ Failed variant {variant_name}: {user.get('email')}")
            else:
                sent_count += 1
                print(f"   ✅ Sent variant {variant_name}: {user.get('email')}")
                
                # Simulate engagement metrics
                if random.random() < 0.8:  # 80% delivery rate
                    variant_metrics['delivered'] += 1
                    
                    if random.random() < variant_config.get('expected_open_rate', 0.25):
                        variant_metrics['opened'] += 1
                        
                        if random.random() < variant_config.get('expected_click_rate', 0.05):
                            variant_metrics['clicked'] += 1
                            
        except Exception as e:
            print(f"   ❌ Error sending variant {variant_name} to {user.get('email')}: {e}")
    
    variant_metrics['sent'] = sent_count
    
    # Calculate performance metrics
    if sent_count > 0:
        open_rate = variant_metrics['opened'] / sent_count * 100
        click_rate = variant_metrics['clicked'] / sent_count * 100
        
        print(f"   📊 Variant {variant_name} performance:")
        print(f"     Sent: {sent_count}, Opened: {variant_metrics['opened']} ({open_rate:.1f}%)")
        print(f"     Clicked: {variant_metrics['clicked']} ({click_rate:.1f}%)")
    
    result = f"A/B variant {variant_name} sent: {sent_count} emails, {variant_metrics['opened']} opens, {variant_metrics['clicked']} clicks"
    print(f"✅ {result}")
    
    return result


def analyze_ab_test_results(campaign_id: str, variants: List[Dict[str, Any]]) -> str:
    """
    Analyze A/B test results and determine winner.
    
    Args:
        campaign_id: Campaign identifier
        variants: List of variant performance data
        
    Returns:
        Analysis result with winner selection
    """
    print(f"📈 Analyzing A/B test results for campaign: {campaign_id}")
    print(f"   Variants tested: {len(variants)}")
    
    # Simulate analysis delay
    time.sleep(1)
    
    best_variant = None
    best_score = 0
    
    print("   📊 Variant Performance:")
    for variant in variants:
        name = variant.get('name', 'Unknown')
        opens = variant.get('opens', 0)
        clicks = variant.get('clicks', 0)
        sent = variant.get('sent', 1)
        
        # Calculate composite score (weighted open rate + click rate)
        open_rate = opens / sent if sent > 0 else 0
        click_rate = clicks / sent if sent > 0 else 0
        score = (open_rate * 0.3) + (click_rate * 0.7)  # Weight clicks more heavily
        
        print(f"     {name}: Score {score:.3f} (Open: {open_rate:.1%}, Click: {click_rate:.1%})")
        
        if score > best_score:
            best_score = score
            best_variant = variant
    
    if best_variant:
        winner_name = best_variant.get('name', 'Unknown')
        print(f"   🏆 Winner: Variant {winner_name} (score: {best_score:.3f})")
        
        # In production: schedule the winning variant for remaining audience
        remaining_users = 1000 - sum(v.get('sent', 0) for v in variants)
        if remaining_users > 0:
            print(f"   📅 Scheduling winner for {remaining_users} remaining users")
    else:
        winner_name = "None"
        print("   ❌ No clear winner found")
    
    result = f"A/B test analysis complete: Winner is variant {winner_name} with score {best_score:.3f}"
    print(f"✅ {result}")
    
    return result


def schedule_campaign(campaign_id: str, schedule_data: Dict[str, Any]) -> str:
    """
    Schedule a campaign for future execution.
    
    Args:
        campaign_id: Campaign identifier
        schedule_data: Scheduling configuration
        
    Returns:
        Scheduling result
    """
    schedule_time = schedule_data.get('run_at', datetime.utcnow() + timedelta(hours=1))
    schedule_type = schedule_data.get('type', 'once')  # once, daily, weekly
    
    print(f"📅 Scheduling campaign: {campaign_id}")
    print(f"   Schedule time: {schedule_time}")
    print(f"   Schedule type: {schedule_type}")
    
    # Simulate schedule creation
    time.sleep(0.3)
    
    if schedule_type == 'once':
        print(f"   ⏰ One-time execution scheduled")
    elif schedule_type == 'daily':
        print(f"   📆 Daily recurring execution scheduled")
    elif schedule_type == 'weekly':
        print(f"   📅 Weekly recurring execution scheduled")
    
    result = f"Campaign {campaign_id} scheduled for {schedule_type} execution at {schedule_time}"
    print(f"✅ {result}")
    
    return result


# ===== SUPPORT FUNCTIONS =====

def generate_campaign_users(segments: List[str], count: int = 100) -> Dict[str, List[Dict[str, Any]]]:
    """Generate test users segmented by type."""
    users_by_segment = {}
    
    names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack"]
    domains = ["example.com", "test.org", "demo.net", "sample.co"]
    
    for segment in segments:
        segment_users = []
        segment_count = count // len(segments)
        
        for i in range(segment_count):
            name = random.choice(names)
            domain = random.choice(domains)
            segment_users.append({
                'id': f"{segment}_{i+1}",
                'name': f"{name} {segment.title()}{i+1}",
                'email': f"{name.lower()}.{segment}{i+1}@{domain}",
                'segment': segment,
                'preferences': {
                    'language': random.choice(['en', 'es', 'fr']),
                    'timezone': random.choice(['UTC', 'EST', 'PST']),
                    'frequency': random.choice(['daily', 'weekly', 'monthly'])
                },
                'engagement_history': {
                    'avg_open_rate': random.uniform(0.15, 0.45),
                    'avg_click_rate': random.uniform(0.02, 0.12),
                    'last_activity': datetime.utcnow() - timedelta(days=random.randint(1, 30))
                }
            })
        
        users_by_segment[segment] = segment_users
    
    return users_by_segment


# ===== DEMONSTRATION FUNCTIONS =====

def demonstrate_campaign_creation():
    """Demonstrate campaign creation and management."""
    print("📍 Campaign Creation Demo")
    print("-" * 40)
    
    with SyncClient() as client:
        jobs = []
        
        # Create different types of campaigns
        campaigns = [
            {
                'name': 'Spring Sale Newsletter',
                'subject': '🌸 Spring Sale - Up to 50% Off!',
                'template': 'sale_newsletter',
                'segments': ['premium', 'standard']
            },
            {
                'name': 'Product Update Announcement',
                'subject': 'Exciting New Features Released!',
                'template': 'product_update',
                'segments': ['premium', 'trial']
            },
            {
                'name': 'Welcome Series - Part 1',
                'subject': 'Welcome! Let\'s get you started',
                'template': 'welcome_series_1',
                'segments': ['new_users']
            }
        ]
        
        print("📤 Creating campaigns...")
        for campaign in campaigns:
            job = client.enqueue(
                create_campaign,
                campaign_data=campaign,
                queue_name="email_campaign"
            )
            jobs.append(job)
            print(f"  ✅ Campaign: {campaign['name']} ({job.job_id})")
        
        return jobs


def demonstrate_segmented_campaigns():
    """Demonstrate segmented campaign targeting."""
    print("\n📍 Segmented Campaign Demo")
    print("-" * 40)
    
    # Generate segmented users
    segments = ['premium', 'standard', 'trial']
    users_by_segment = generate_campaign_users(segments, 60)
    
    with SyncClient() as client:
        jobs = []
        campaign_id = "CAMP-SEGMENTED-001"
        
        print(f"📤 Sending segmented campaigns for: {campaign_id}")
        
        for segment, users in users_by_segment.items():
            if users:
                # Choose template based on segment
                if segment == 'premium':
                    template = 'premium_exclusive'
                elif segment == 'standard':
                    template = 'standard_offer'
                else:
                    template = 'trial_upgrade'
                
                job = client.enqueue(
                    send_campaign_to_segment,
                    campaign_id=campaign_id,
                    segment_name=segment,
                    users=users,
                    template=template,
                    queue_name="email_campaign"
                )
                jobs.append(job)
                print(f"  ✅ Segment {segment}: {len(users)} users ({job.job_id})")
        
        return jobs


def demonstrate_ab_testing():
    """Demonstrate A/B testing functionality."""
    print("\n📍 A/B Testing Demo")
    print("-" * 40)
    
    # Generate test users
    test_users = generate_campaign_users(['test'], 30)['test']
    
    # Define A/B test variants
    variants = [
        {
            'name': 'A',
            'subject': 'Don\'t Miss Out - Limited Time Offer!',
            'template': 'urgency_template',
            'expected_open_rate': 0.28,
            'expected_click_rate': 0.06
        },
        {
            'name': 'B', 
            'subject': 'Special Discount Just for You',
            'template': 'personal_template',
            'expected_open_rate': 0.22,
            'expected_click_rate': 0.08
        },
        {
            'name': 'C',
            'subject': 'New Products You\'ll Love',
            'template': 'product_focus_template',
            'expected_open_rate': 0.25,
            'expected_click_rate': 0.04
        }
    ]
    
    with SyncClient() as client:
        jobs = []
        campaign_id = "CAMP-ABTEST-001"
        
        # Split users among variants
        users_per_variant = len(test_users) // len(variants)
        
        print(f"📤 Running A/B test for campaign: {campaign_id}")
        print(f"   Variants: {len(variants)}, Users per variant: {users_per_variant}")
        
        variant_results = []
        
        for i, variant in enumerate(variants):
            start_idx = i * users_per_variant
            end_idx = (i + 1) * users_per_variant if i < len(variants) - 1 else len(test_users)
            variant_users = test_users[start_idx:end_idx]
            
            job = client.enqueue(
                send_ab_test_variant,
                campaign_id=campaign_id,
                variant_name=variant['name'],
                variant_config=variant,
                users=variant_users,
                queue_name="email_ab_test"
            )
            jobs.append(job)
            print(f"  ✅ Variant {variant['name']}: {len(variant_users)} users ({job.job_id})")
            
            # Store expected results for analysis
            variant_results.append({
                'name': variant['name'],
                'sent': len(variant_users),
                'opens': int(len(variant_users) * variant['expected_open_rate']),
                'clicks': int(len(variant_users) * variant['expected_click_rate'])
            })
        
        # Schedule analysis job
        analysis_job = client.enqueue(
            analyze_ab_test_results,
            campaign_id=campaign_id,
            variants=variant_results,
            queue_name="email_campaign"
        )
        jobs.append(analysis_job)
        print(f"  📊 A/B analysis scheduled: {analysis_job.job_id}")
        
        return jobs


def demonstrate_campaign_scheduling():
    """Demonstrate campaign scheduling features."""
    print("\n📍 Campaign Scheduling Demo")
    print("-" * 40)
    
    with SyncClient() as client:
        jobs = []
        
        # Schedule different types of campaigns
        schedules = [
            {
                'campaign_id': 'CAMP-SCHEDULED-001',
                'schedule': {
                    'type': 'once',
                    'run_at': datetime.utcnow() + timedelta(minutes=5)
                }
            },
            {
                'campaign_id': 'CAMP-DAILY-001',
                'schedule': {
                    'type': 'daily',
                    'run_at': datetime.utcnow().replace(hour=9, minute=0)
                }
            },
            {
                'campaign_id': 'CAMP-WEEKLY-001',
                'schedule': {
                    'type': 'weekly',
                    'run_at': datetime.utcnow() + timedelta(days=7)
                }
            }
        ]
        
        print("📤 Scheduling campaigns...")
        for schedule_config in schedules:
            job = client.enqueue(
                schedule_campaign,
                campaign_id=schedule_config['campaign_id'],
                schedule_data=schedule_config['schedule'],
                queue_name="email_campaign"
            )
            jobs.append(job)
            schedule_type = schedule_config['schedule']['type']
            print(f"  ✅ {schedule_type.title()} campaign: {schedule_config['campaign_id']} ({job.job_id})")
        
        return jobs


def main():
    """
    Main function demonstrating advanced campaign management.
    """
    print("📊 NAQ Advanced Campaign Manager Demo")
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
        creation_jobs = demonstrate_campaign_creation()
        segmented_jobs = demonstrate_segmented_campaigns()
        ab_jobs = demonstrate_ab_testing()
        schedule_jobs = demonstrate_campaign_scheduling()
        
        # Summary
        total_jobs = len(creation_jobs) + len(segmented_jobs) + len(ab_jobs) + len(schedule_jobs)
        
        print(f"\n🎉 Campaign manager demo completed!")
        print(f"📊 Enqueued {total_jobs} jobs total:")
        print(f"   📝 {len(creation_jobs)} campaign creations")
        print(f"   🎯 {len(segmented_jobs)} segmented campaigns")
        print(f"   🔬 {len(ab_jobs)} A/B test jobs")
        print(f"   📅 {len(schedule_jobs)} scheduled campaigns")
        
        print("\n🚀 Advanced Campaign Features Demonstrated:")
        print("   • Campaign creation and management")
        print("   • Segmented user targeting")
        print("   • A/B testing with multiple variants")
        print("   • Automated winner selection")
        print("   • Campaign scheduling (one-time, recurring)")
        print("   • Performance metrics tracking")
        print("   • Queue priority management")
        
        print("\n💡 Watch your worker logs to see:")
        print("   - Campaign processing in real-time")
        print("   - Segmented audience targeting")
        print("   - A/B test variant performance")
        print("   - Scheduled campaign execution")
        
        print("\n📋 Next Steps:")
        print("   - Try template_processor.py for template management")
        print("   - Implement database for campaign storage")
        print("   - Add real-time analytics dashboard")
        print("   - Integrate with email service providers")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\n🔧 Troubleshooting:")
        print("   - Is NATS running? (cd docker && docker-compose up -d)")
        print("   - Are workers running? (naq worker default email_campaign email_ab_test)")
        print("   - Is scheduler running? (naq scheduler)")
        print("   - Is NAQ_JOB_SERIALIZER=json set?")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())