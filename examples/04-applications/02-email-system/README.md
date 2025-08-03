# Email System - Comprehensive Email Management

This example demonstrates building a complete email system with NAQ, including campaign management, transactional emails, template processing, delivery tracking, and bounce handling.

## What You'll Learn

- Email campaign management and scheduling
- Transactional email processing
- Template rendering and personalization
- Email delivery tracking and analytics
- Bounce and complaint handling
- Rate limiting and reputation management
- A/B testing for email campaigns
- Production email system architecture

## Email System Architecture

### Core Components
```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Web API       │───▶│  NAQ Job Queue   │───▶│  Email Workers  │
│                 │    │                  │    │                 │
│ • Campaign Mgmt │    │ • Email Queue    │    │ • SMTP Sending  │
│ • Templates     │    │ • Priority Queue │    │ • Template Proc │
│ • Analytics     │    │ • Retry Logic    │    │ • Delivery Trk  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                        │                        │
         │              ┌─────────▼─────────┐              │
         │              │  Email Scheduler  │              │
         │              │                   │              │
         │              │ • Campaign Timing │              │
         └──────────────│ • Send Rate Ctrl  │──────────────┘
                        │ • Segmentation    │
                        └───────────────────┘
```

### Email Types
- **Transactional**: Welcome emails, order confirmations, password resets
- **Campaign**: Marketing newsletters, product announcements
- **Triggered**: Abandoned cart, re-engagement, birthday emails
- **System**: Alerts, notifications, reports

## Email Campaign Management

### Campaign Creation
```python
from naq import SyncClient

def create_email_campaign(campaign_data):
    """Create and schedule email campaign."""
    
    campaign = {
        'id': generate_campaign_id(),
        'name': campaign_data['name'],
        'subject': campaign_data['subject'],
        'template': campaign_data['template'],
        'segments': campaign_data['segments'],
        'schedule': campaign_data.get('schedule'),
        'created_at': datetime.utcnow()
    }
    
    # Save campaign
    save_campaign(campaign)
    
    # Schedule or send immediately
    if campaign.get('schedule'):
        schedule_campaign(campaign)
    else:
        send_campaign_immediately(campaign)
    
    return campaign
```

### Segmented Campaigns
```python
def send_segmented_campaign(campaign_id, segments):
    """Send campaign to different user segments."""
    
    with SyncClient() as client:
        for segment in segments:
            # Get users for this segment
            users = get_users_by_segment(segment['criteria'])
            
            # Enqueue jobs for each user batch
            batch_size = 100
            for i in range(0, len(users), batch_size):
                batch = users[i:i + batch_size]
                
                client.enqueue(
                    send_campaign_batch,
                    campaign_id=campaign_id,
                    users=batch,
                    segment=segment['name'],
                    queue_name='email_campaign',
                    priority=segment.get('priority', 'normal')
                )
```

### A/B Testing
```python
def send_ab_test_campaign(campaign_id, variants, test_percentage=20):
    """Send A/B test campaign with multiple variants."""
    
    users = get_campaign_users(campaign_id)
    
    # Split users for A/B testing
    test_size = int(len(users) * test_percentage / 100)
    test_users = users[:test_size]
    remaining_users = users[test_size:]
    
    # Send test variants
    for i, variant in enumerate(variants):
        variant_users = test_users[i::len(variants)]
        
        enqueue_sync(
            send_email_variant,
            campaign_id=campaign_id,
            variant=variant,
            users=variant_users,
            queue_name='email_ab_test'
        )
    
    # Schedule winner sending after test period
    schedule_sync(
        send_winning_variant,
        campaign_id=campaign_id,
        remaining_users=remaining_users,
        cron=f"0 * * * *",  # Check hourly for winner
        schedule_id=f"ab_winner_{campaign_id}"
    )
```

## Transactional Emails

### Welcome Email Workflow
```python
def send_welcome_series(user_id, email):
    """Send welcome email series to new user."""
    
    with SyncClient() as client:
        # Immediate welcome email
        welcome_job = client.enqueue(
            send_transactional_email,
            template='welcome',
            to=email,
            user_id=user_id,
            queue_name='email_transactional',
            priority='high'
        )
        
        # Follow-up emails
        client.enqueue_in(
            send_transactional_email,
            template='getting_started',
            to=email,
            user_id=user_id,
            delay=timedelta(days=1),
            queue_name='email_transactional'
        )
        
        client.enqueue_in(
            send_transactional_email,
            template='tips_and_tricks',
            to=email,
            user_id=user_id,
            delay=timedelta(days=7),
            queue_name='email_transactional'
        )
```

### Order Confirmation
```python
def send_order_confirmation(order_id, customer_email):
    """Send order confirmation email."""
    
    # Get order details
    order = get_order(order_id)
    
    # Enqueue confirmation email
    enqueue_sync(
        send_transactional_email,
        template='order_confirmation',
        to=customer_email,
        context={
            'order': order,
            'customer': get_customer(order['customer_id']),
            'items': get_order_items(order_id),
            'total': order['total']
        },
        queue_name='email_transactional',
        priority='high'
    )
    
    # Schedule shipping notification
    if order['estimated_delivery']:
        schedule_sync(
            send_shipping_notification,
            order_id=order_id,
            run_at=order['estimated_delivery'] - timedelta(days=1),
            queue_name='email_transactional'
        )
```

## Template Management

### Dynamic Template Rendering
```python
from jinja2 import Environment, FileSystemLoader

def render_email_template(template_name, context, user_preferences=None):
    """Render email template with personalization."""
    
    # Load template
    env = Environment(loader=FileSystemLoader('templates/email'))
    template = env.get_template(f"{template_name}.html")
    
    # Add user preferences and personalization
    if user_preferences:
        context.update({
            'timezone': user_preferences.get('timezone', 'UTC'),
            'language': user_preferences.get('language', 'en'),
            'name': user_preferences.get('name', 'Friend')
        })
    
    # Add dynamic content
    context.update({
        'unsubscribe_url': generate_unsubscribe_url(context.get('user_id')),
        'tracking_pixel': generate_tracking_pixel(context.get('email_id')),
        'current_date': datetime.now(),
        'company_info': get_company_info()
    })
    
    # Render template
    html_content = template.render(**context)
    
    # Generate text version
    text_content = html_to_text(html_content)
    
    return {
        'html': html_content,
        'text': text_content,
        'subject': template.module.subject if hasattr(template.module, 'subject') else None
    }
```

### Multi-language Support
```python
def send_localized_email(template, user_id, context):
    """Send email in user's preferred language."""
    
    user = get_user(user_id)
    language = user.get('language', 'en')
    
    # Render template in user's language
    rendered = render_email_template(
        f"{template}_{language}",
        context,
        user_preferences=user.get('preferences', {})
    )
    
    enqueue_sync(
        send_email,
        to=user['email'],
        subject=rendered['subject'],
        html_content=rendered['html'],
        text_content=rendered['text'],
        language=language,
        queue_name='email_transactional'
    )
```

## Delivery Management

### Email Sending with Retries
```python
def send_email_with_delivery_tracking(email_data):
    """Send email with comprehensive delivery tracking."""
    
    email_id = email_data['id']
    
    try:
        # Update status to sending
        update_email_status(email_id, 'sending')
        
        # Send via SMTP/API
        result = send_via_provider(email_data)
        
        # Update status based on result
        if result['success']:
            update_email_status(email_id, 'sent', {
                'provider_id': result['message_id'],
                'sent_at': datetime.utcnow()
            })
        else:
            update_email_status(email_id, 'failed', {
                'error': result['error'],
                'failed_at': datetime.utcnow()
            })
            
            # Re-raise for retry handling
            raise EmailDeliveryError(result['error'])
            
    except Exception as e:
        update_email_status(email_id, 'failed', {
            'error': str(e),
            'failed_at': datetime.utcnow()
        })
        raise
```

### Bounce and Complaint Handling
```python
def handle_email_bounce(bounce_data):
    """Handle email bounce notification."""
    
    email_address = bounce_data['email']
    bounce_type = bounce_data['type']  # hard, soft, complaint
    
    if bounce_type == 'hard':
        # Hard bounce - suppress email permanently
        add_to_suppression_list(email_address, 'hard_bounce')
        
        # Cancel any pending emails
        cancel_pending_emails(email_address)
        
    elif bounce_type == 'soft':
        # Soft bounce - track and suppress if too many
        increment_soft_bounce_count(email_address)
        
        if get_soft_bounce_count(email_address) >= 5:
            add_to_suppression_list(email_address, 'soft_bounce_limit')
            
    elif bounce_type == 'complaint':
        # Spam complaint - immediate suppression
        add_to_suppression_list(email_address, 'spam_complaint')
        
        # Update sender reputation metrics
        update_reputation_metrics(bounce_data)
    
    # Log bounce for analytics
    log_bounce_event(bounce_data)
```

## Rate Limiting and Reputation

### Send Rate Management
```python
class EmailRateLimiter:
    """Manage email sending rates to protect reputation."""
    
    def __init__(self, redis_client):
        self.redis = redis_client
        self.limits = {
            'per_second': 10,
            'per_minute': 500,
            'per_hour': 10000,
            'per_day': 100000
        }
    
    def can_send_email(self, provider='default'):
        """Check if we can send an email without exceeding limits."""
        
        current_time = time.time()
        
        for period, limit in self.limits.items():
            window = self._get_window_seconds(period)
            key = f"email_rate:{provider}:{period}:{int(current_time // window)}"
            
            current_count = self.redis.get(key) or 0
            if int(current_count) >= limit:
                return False, f"Rate limit exceeded for {period}"
        
        return True, None
    
    def record_sent_email(self, provider='default'):
        """Record that an email was sent."""
        
        current_time = time.time()
        
        for period in self.limits.keys():
            window = self._get_window_seconds(period)
            key = f"email_rate:{provider}:{period}:{int(current_time // window)}"
            
            self.redis.incr(key)
            self.redis.expire(key, window * 2)  # Keep for 2 windows
```

### Reputation Monitoring
```python
def monitor_sender_reputation():
    """Monitor and adjust email sending based on reputation metrics."""
    
    metrics = get_reputation_metrics()
    
    # Check bounce rate
    if metrics['bounce_rate'] > 0.05:  # 5% threshold
        # Reduce sending rate
        update_rate_limits(factor=0.5)
        alert_team("High bounce rate detected", metrics)
    
    # Check complaint rate
    if metrics['complaint_rate'] > 0.001:  # 0.1% threshold
        # Pause sending for review
        pause_all_campaigns()
        alert_team("High complaint rate detected", metrics)
    
    # Check deliverability
    if metrics['delivery_rate'] < 0.95:  # 95% threshold
        # Review and clean lists
        enqueue_sync(
            review_email_lists,
            threshold=metrics['delivery_rate'],
            queue_name='email_maintenance'
        )
```

## Analytics and Reporting

### Email Analytics
```python
def track_email_engagement(email_id, event_type, user_data=None):
    """Track email engagement events."""
    
    event = {
        'email_id': email_id,
        'event_type': event_type,  # sent, delivered, opened, clicked, bounced
        'timestamp': datetime.utcnow(),
        'user_data': user_data
    }
    
    # Store event
    store_email_event(event)
    
    # Update email status
    if event_type == 'delivered':
        update_email_status(email_id, 'delivered')
    elif event_type == 'opened':
        update_email_status(email_id, 'opened')
    elif event_type == 'clicked':
        update_email_status(email_id, 'clicked')
    
    # Update campaign metrics
    email = get_email(email_id)
    if email.get('campaign_id'):
        update_campaign_metrics(email['campaign_id'], event_type)
```

### Campaign Performance Reports
```python
def generate_campaign_report(campaign_id):
    """Generate comprehensive campaign performance report."""
    
    campaign = get_campaign(campaign_id)
    events = get_campaign_events(campaign_id)
    
    # Calculate metrics
    total_sent = len([e for e in events if e['event_type'] == 'sent'])
    total_delivered = len([e for e in events if e['event_type'] == 'delivered'])
    total_opened = len([e for e in events if e['event_type'] == 'opened'])
    total_clicked = len([e for e in events if e['event_type'] == 'clicked'])
    total_bounced = len([e for e in events if e['event_type'] == 'bounced'])
    
    report = {
        'campaign': campaign,
        'metrics': {
            'sent': total_sent,
            'delivered': total_delivered,
            'opened': total_opened,
            'clicked': total_clicked,
            'bounced': total_bounced,
            'delivery_rate': total_delivered / total_sent if total_sent > 0 else 0,
            'open_rate': total_opened / total_delivered if total_delivered > 0 else 0,
            'click_rate': total_clicked / total_delivered if total_delivered > 0 else 0,
            'bounce_rate': total_bounced / total_sent if total_sent > 0 else 0
        },
        'timeline': generate_event_timeline(events),
        'top_links': get_top_clicked_links(campaign_id),
        'geographic_data': get_geographic_engagement(campaign_id)
    }
    
    return report
```

## Prerequisites

1. **NATS Server** running with JetStream
2. **Email Provider** (SMTP server, SendGrid, Mailgun, etc.)
3. **Database** for storing campaigns, templates, and analytics
4. **Redis** for rate limiting and caching
5. **NAQ Workers** for email processing
6. **Secure Configuration**: `export NAQ_JOB_SERIALIZER=json`

## Running the Examples

### 1. Start Services

```bash
# Start NATS
cd docker && docker-compose up -d

# Set secure serialization
export NAQ_JOB_SERIALIZER=json

# Start email workers
naq worker default email_campaign email_transactional email_ab_test --log-level INFO &

# Start scheduler for scheduled campaigns
naq scheduler --log-level INFO &
```

### 2. Configure Email Provider

```bash
# Set email provider configuration
export EMAIL_PROVIDER=smtp
export SMTP_HOST=smtp.gmail.com
export SMTP_PORT=587
export SMTP_USERNAME=your_email@gmail.com
export SMTP_PASSWORD=your_app_password

# Or use API-based provider
export EMAIL_PROVIDER=sendgrid
export SENDGRID_API_KEY=your_sendgrid_api_key
```

### 3. Run Examples

```bash
cd examples/04-applications/02-email-system

# Complete email system
python email_system.py

# Campaign management
python campaign_manager.py

# Template processing
python template_processor.py
```

## Production Considerations

### Scalability
- Use dedicated workers for different email types
- Implement queue priority for transactional emails
- Scale workers based on sending volume
- Use database connection pooling

### Reliability
- Implement comprehensive retry logic
- Use dead letter queues for failed emails
- Monitor and alert on system health
- Regular backup of templates and campaigns

### Compliance
- Implement GDPR/CAN-SPAM compliance
- Manage unsubscribe lists
- Handle data retention requirements
- Audit email sending activities

### Security
- Encrypt sensitive data in transit and at rest
- Validate email templates for XSS
- Implement proper authentication
- Monitor for suspicious activity

## Next Steps

- Explore [image processing](../03-image-processing/) for email attachments
- Learn about [data pipeline](../04-data-pipeline/) for email analytics ETL
- Check out [performance optimization](../../05-advanced/03-performance-optimization/) for high-volume sending
- Implement [monitoring dashboard](../../03-production/02-monitoring-dashboard/) for email system oversight