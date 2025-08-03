# Monitoring Dashboard - Production Oversight

This example demonstrates comprehensive monitoring and observability for NAQ applications in production, including the built-in web dashboard, custom metrics, and monitoring best practices.

## What You'll Learn

- Built-in web dashboard usage
- Custom monitoring implementations  
- Performance metrics collection
- Alert system integration
- Troubleshooting and debugging
- Production monitoring best practices

## Built-in Web Dashboard

NAQ includes a comprehensive web dashboard for monitoring:

### Starting the Dashboard
```bash
# Start dashboard on default port (8000)
naq dashboard

# Start on custom port
naq dashboard --port 8080

# Start with specific host binding
naq dashboard --host 0.0.0.0 --port 8000
```

### Dashboard Features
- **Queue Status**: Active jobs, pending jobs, failed jobs
- **Worker Monitoring**: Active workers, processing rates, health status  
- **Job Details**: Individual job status, execution history, error logs
- **Schedule Overview**: Scheduled jobs, next execution times, success rates
- **System Metrics**: Memory usage, processing times, throughput stats

### Dashboard URLs
```
Main Dashboard:     http://localhost:8000/
Queue Status:       http://localhost:8000/queues
Worker Status:      http://localhost:8000/workers  
Job Details:        http://localhost:8000/jobs
Schedule Status:    http://localhost:8000/schedules
System Metrics:     http://localhost:8000/metrics
```

## Custom Monitoring

### Programmatic Queue Monitoring
```python
from naq import list_workers_sync, list_queues_sync

# Monitor worker status
workers = list_workers_sync()
for worker in workers:
    print(f"Worker {worker['worker_id']}: {worker['status']}")
    print(f"  Queue: {worker['queue_name']}")
    print(f"  Jobs processed: {worker['jobs_processed']}")
    print(f"  Last heartbeat: {worker['last_heartbeat']}")

# Monitor queue status  
queues = list_queues_sync()
for queue in queues:
    print(f"Queue {queue['name']}: {queue['pending_jobs']} pending")
```

### Job Status Monitoring
```python
from naq import get_job_status_sync

job_status = get_job_status_sync(job_id)
print(f"Job Status: {job_status['status']}")
print(f"Started: {job_status['started_at']}")
print(f"Duration: {job_status['duration_ms']}ms")
if job_status['error']:
    print(f"Error: {job_status['error']}")
```

## Prerequisites

1. **NATS Server** running with JetStream
2. **NAQ Workers** running for monitoring
3. **Sample Jobs** for demonstration data
4. **Secure Configuration**: `export NAQ_JOB_SERIALIZER=json`

## Running the Examples

### 1. Start Services

```bash
# Start NATS
cd docker && docker-compose up -d

# Set secure serialization
export NAQ_JOB_SERIALIZER=json

# Start workers for monitoring
naq worker default monitoring_queue demo_queue --log-level INFO &

# Start scheduler for scheduled job monitoring
naq scheduler --log-level INFO &

# Start dashboard
naq dashboard --port 8000 &
```

### 2. Generate Sample Data

```bash
cd examples/03-production/02-monitoring-dashboard

# Generate sample jobs and metrics
python generate_sample_data.py

# Custom monitoring implementation
python custom_monitoring.py

# Performance monitoring
python performance_monitoring.py
```

### 3. Access Dashboard

```bash
# Open dashboard in browser
open http://localhost:8000

# Or visit manually
curl http://localhost:8000/api/status
```

## Monitoring Categories

### System Health Monitoring
```python
def monitor_system_health():
    """Monitor overall system health metrics."""
    
    # Worker health
    workers = list_workers_sync()
    active_workers = [w for w in workers if w['status'] == 'active']
    
    if len(active_workers) == 0:
        alert("CRITICAL: No active workers!")
    elif len(active_workers) < MIN_WORKERS:
        alert("WARNING: Low worker count")
    
    # Queue depth monitoring
    queues = list_queues_sync()
    for queue in queues:
        if queue['pending_jobs'] > QUEUE_DEPTH_THRESHOLD:
            alert(f"WARNING: Queue {queue['name']} has {queue['pending_jobs']} pending jobs")
```

### Performance Monitoring
```python
def monitor_performance():
    """Monitor job processing performance."""
    
    # Job completion rates
    recent_jobs = get_recent_jobs_sync(hours=1)
    completed = [j for j in recent_jobs if j['status'] == 'completed']
    failed = [j for j in recent_jobs if j['status'] == 'failed']
    
    success_rate = len(completed) / len(recent_jobs) if recent_jobs else 0
    
    if success_rate < SUCCESS_RATE_THRESHOLD:
        alert(f"WARNING: Low success rate: {success_rate:.2%}")
    
    # Processing time analysis
    avg_duration = sum(j['duration_ms'] for j in completed) / len(completed)
    if avg_duration > DURATION_THRESHOLD_MS:
        alert(f"WARNING: High average processing time: {avg_duration}ms")
```

### Business Metrics
```python
def monitor_business_metrics():
    """Monitor business-specific metrics."""
    
    # Email delivery monitoring
    email_jobs = get_jobs_by_function_sync('send_email', hours=24)
    email_success_rate = calculate_success_rate(email_jobs)
    
    # Report generation monitoring  
    report_jobs = get_jobs_by_function_sync('generate_report', hours=24)
    report_completion_time = calculate_avg_duration(report_jobs)
    
    # Data processing monitoring
    etl_jobs = get_jobs_by_function_sync('process_data', hours=24)
    etl_throughput = calculate_throughput(etl_jobs)
```

## Alert Integration

### Email Alerts
```python
import smtplib
from email.mime.text import MimeText

def send_email_alert(subject, message, recipients):
    """Send email alert for critical issues."""
    
    msg = MimeText(message)
    msg['Subject'] = f"NAQ Alert: {subject}"
    msg['From'] = "naq-alerts@company.com"
    msg['To'] = ", ".join(recipients)
    
    with smtplib.SMTP('localhost') as server:
        server.send_message(msg)
```

### Slack Integration
```python
import requests

def send_slack_alert(channel, message, urgency="info"):
    """Send Slack notification for monitoring alerts."""
    
    colors = {"info": "good", "warning": "warning", "critical": "danger"}
    
    payload = {
        "channel": channel,
        "attachments": [{
            "color": colors.get(urgency, "good"),
            "title": "NAQ Monitoring Alert",
            "text": message,
            "ts": time.time()
        }]
    }
    
    requests.post(SLACK_WEBHOOK_URL, json=payload)
```

### PagerDuty Integration
```python
def trigger_pagerduty_alert(service_key, description, details):
    """Trigger PagerDuty alert for critical issues."""
    
    payload = {
        "service_key": service_key,
        "event_type": "trigger",
        "description": description,
        "details": details
    }
    
    requests.post(
        "https://events.pagerduty.com/generic/2010-04-15/create_event.json",
        json=payload
    )
```

## Metrics Collection

### Custom Metrics
```python
class NAQMetricsCollector:
    """Custom metrics collector for NAQ monitoring."""
    
    def __init__(self):
        self.metrics = {}
    
    def collect_worker_metrics(self):
        """Collect worker performance metrics."""
        workers = list_workers_sync()
        
        self.metrics['worker_count'] = len(workers)
        self.metrics['active_workers'] = len([w for w in workers if w['status'] == 'active'])
        self.metrics['avg_jobs_per_worker'] = sum(w['jobs_processed'] for w in workers) / len(workers)
    
    def collect_queue_metrics(self):
        """Collect queue depth and processing metrics."""
        queues = list_queues_sync()
        
        for queue in queues:
            self.metrics[f'queue_{queue["name"]}_pending'] = queue['pending_jobs']
            self.metrics[f'queue_{queue["name"]}_processing'] = queue['processing_jobs']
    
    def collect_job_metrics(self):
        """Collect job execution metrics."""
        recent_jobs = get_recent_jobs_sync(hours=1)
        
        self.metrics['jobs_completed_last_hour'] = len([j for j in recent_jobs if j['status'] == 'completed'])
        self.metrics['jobs_failed_last_hour'] = len([j for j in recent_jobs if j['status'] == 'failed'])
        
        durations = [j['duration_ms'] for j in recent_jobs if j['status'] == 'completed']
        if durations:
            self.metrics['avg_job_duration_ms'] = sum(durations) / len(durations)
            self.metrics['max_job_duration_ms'] = max(durations)
```

### Prometheus Integration
```python
from prometheus_client import Gauge, Counter, Histogram, start_http_server

# Define metrics
worker_count = Gauge('naq_workers_total', 'Total number of NAQ workers')
jobs_completed = Counter('naq_jobs_completed_total', 'Total completed jobs')
job_duration = Histogram('naq_job_duration_seconds', 'Job execution duration')

def export_prometheus_metrics():
    """Export NAQ metrics to Prometheus."""
    
    # Update worker metrics
    workers = list_workers_sync()
    worker_count.set(len(workers))
    
    # Update job metrics
    recent_jobs = get_recent_jobs_sync(minutes=5)
    for job in recent_jobs:
        if job['status'] == 'completed':
            jobs_completed.inc()
            job_duration.observe(job['duration_ms'] / 1000)

# Start Prometheus metrics server
start_http_server(8001)
```

## Dashboard Customization

### Custom Dashboard Views
```python
def create_custom_dashboard():
    """Create custom dashboard views for specific metrics."""
    
    dashboard_data = {
        "overview": {
            "active_workers": len(list_workers_sync()),
            "pending_jobs": sum(q['pending_jobs'] for q in list_queues_sync()),
            "success_rate": calculate_success_rate(get_recent_jobs_sync(hours=24))
        },
        "performance": {
            "avg_processing_time": calculate_avg_duration(get_recent_jobs_sync(hours=1)),
            "throughput": calculate_throughput(get_recent_jobs_sync(hours=1)),
            "error_rate": calculate_error_rate(get_recent_jobs_sync(hours=1))
        },
        "queues": [
            {
                "name": queue['name'],
                "pending": queue['pending_jobs'],
                "processing": queue['processing_jobs']
            }
            for queue in list_queues_sync()
        ]
    }
    
    return dashboard_data
```

### Real-time Updates
```python
import asyncio
import websockets

async def stream_metrics(websocket, path):
    """Stream real-time metrics to dashboard."""
    
    while True:
        metrics = {
            "timestamp": time.time(),
            "workers": len(list_workers_sync()),
            "queues": {q['name']: q['pending_jobs'] for q in list_queues_sync()}
        }
        
        await websocket.send(json.dumps(metrics))
        await asyncio.sleep(5)  # Update every 5 seconds

# Start WebSocket server for real-time updates
start_server = websockets.serve(stream_metrics, "localhost", 8765)
```

## Troubleshooting Dashboard

### Common Issues

1. **Dashboard Not Loading**
   - Check if port 8000 is available
   - Verify NATS connection
   - Check dashboard logs

2. **Missing Data**
   - Ensure workers are running
   - Check job execution
   - Verify queue configuration

3. **Performance Issues**
   - Monitor dashboard resource usage
   - Check NATS server performance
   - Review data retention settings

### Debug Commands
```bash
# Check dashboard status
curl http://localhost:8000/api/health

# Get system status
curl http://localhost:8000/api/status

# Check specific queue
curl http://localhost:8000/api/queues/default

# Get worker information
curl http://localhost:8000/api/workers
```

## Production Best Practices

### 1. Monitoring Strategy
- Monitor business metrics, not just technical metrics
- Set up alerts for critical thresholds
- Use multiple monitoring tools for redundancy
- Regular monitoring health checks

### 2. Alert Management
- Avoid alert fatigue with proper thresholds
- Escalate critical alerts appropriately
- Document alert response procedures
- Regular alert rule reviews

### 3. Performance Baselines
- Establish normal operation baselines
- Monitor trends over time
- Capacity planning based on metrics
- Regular performance reviews

### 4. Data Retention
- Configure appropriate metric retention
- Archive historical data
- Balance storage vs. query performance
- Regular cleanup procedures

## Security Considerations

### Dashboard Access
```python
# Secure dashboard with authentication
from flask_httpauth import HTTPBasicAuth

auth = HTTPBasicAuth()

@auth.verify_password
def verify_password(username, password):
    return username == 'admin' and password == 'secure_password'

@app.route('/dashboard')
@auth.login_required
def dashboard():
    return render_dashboard()
```

### API Security
```python
# Secure monitoring APIs
import jwt

def verify_token(token):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        return payload['user_id']
    except jwt.InvalidTokenError:
        return None

@app.route('/api/metrics')
def get_metrics():
    token = request.headers.get('Authorization', '').replace('Bearer ', '')
    if not verify_token(token):
        return {"error": "Unauthorized"}, 401
    
    return get_system_metrics()
```

## Next Steps

- Explore [error handling](../03-error-handling/) for comprehensive error monitoring
- Learn about [configuration management](../04-configuration/) for environment-specific monitoring
- Check out [high availability](../../05-advanced/02-high-availability/) for distributed monitoring
- Implement [performance optimization](../../05-advanced/03-performance-optimization/) based on monitoring insights