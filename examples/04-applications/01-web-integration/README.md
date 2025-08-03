# Web Integration - Modern Web Applications

This example demonstrates integrating NAQ with modern web applications, including Flask, FastAPI, and async frameworks for background job processing, webhooks, and API-driven workflows.

## What You'll Learn

- Flask and FastAPI integration patterns
- Background job processing from web requests
- Webhook handling and processing
- File upload processing
- Real-time job status updates
- API-driven job management
- Production web application patterns

## Integration Patterns

### Flask Integration
```python
from flask import Flask, request, jsonify
from naq import SyncClient

app = Flask(__name__)
naq_client = SyncClient()

@app.route('/api/process', methods=['POST'])
def process_data():
    data = request.json
    
    # Enqueue background job
    job = naq_client.enqueue(
        process_user_data,
        user_id=data['user_id'],
        operation=data['operation'],
        queue_name='web_queue'
    )
    
    return jsonify({
        'job_id': job.job_id,
        'status': 'enqueued',
        'estimated_completion': '2-5 minutes'
    })

@app.route('/api/jobs/<job_id>', methods=['GET'])
def get_job_status(job_id):
    status = naq_client.get_job_status(job_id)
    return jsonify(status)
```

### FastAPI Integration
```python
from fastapi import FastAPI, BackgroundTasks, HTTPException
from naq import AsyncClient
import asyncio

app = FastAPI()
naq_client = AsyncClient()

@app.post("/api/async-process")
async def async_process(data: ProcessRequest):
    # Enqueue async job
    job = await naq_client.enqueue(
        async_process_data,
        user_data=data.dict(),
        queue_name='async_web_queue'
    )
    
    return {
        'job_id': job.job_id,
        'status': 'processing',
        'webhook_url': f'/api/webhooks/job-complete/{job.job_id}'
    }

@app.get("/api/jobs/{job_id}/status")
async def get_async_job_status(job_id: str):
    status = await naq_client.get_job_status(job_id)
    if not status:
        raise HTTPException(status_code=404, detail="Job not found")
    return status
```

### WebSocket Real-time Updates
```python
from flask_socketio import SocketIO, emit
import redis

app = Flask(__name__)
socketio = SocketIO(app)
redis_client = redis.Redis()

def job_status_changed(job_id, status):
    """Called when job status changes."""
    socketio.emit('job_update', {
        'job_id': job_id,
        'status': status,
        'timestamp': datetime.now().isoformat()
    }, room=f'job_{job_id}')

@socketio.on('subscribe_job')
def handle_job_subscription(data):
    job_id = data['job_id']
    join_room(f'job_{job_id}')
    emit('subscribed', {'job_id': job_id})
```

## Common Web Application Patterns

### File Upload Processing
```python
@app.route('/api/upload', methods=['POST'])
def handle_file_upload():
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    
    # Save file temporarily
    filename = secure_filename(file.filename)
    file_path = os.path.join(UPLOAD_FOLDER, filename)
    file.save(file_path)
    
    # Enqueue processing job
    job = naq_client.enqueue(
        process_uploaded_file,
        file_path=file_path,
        original_filename=filename,
        user_id=request.form.get('user_id'),
        queue_name='file_processing'
    )
    
    return jsonify({
        'job_id': job.job_id,
        'message': 'File uploaded and queued for processing'
    })
```

### Webhook Processing
```python
@app.route('/webhooks/github', methods=['POST'])
def handle_github_webhook():
    payload = request.json
    signature = request.headers.get('X-Hub-Signature-256')
    
    # Verify webhook signature
    if not verify_github_signature(payload, signature):
        return 'Unauthorized', 401
    
    # Enqueue webhook processing
    job = naq_client.enqueue(
        process_github_webhook,
        event_type=request.headers.get('X-GitHub-Event'),
        payload=payload,
        queue_name='webhook_queue'
    )
    
    return jsonify({'received': True, 'job_id': job.job_id})
```

### Email Campaign Processing
```python
@app.route('/api/campaigns', methods=['POST'])
def create_email_campaign():
    campaign_data = request.json
    
    # Validate campaign data
    if not validate_campaign(campaign_data):
        return jsonify({'error': 'Invalid campaign data'}), 400
    
    # Create campaign record
    campaign = create_campaign_record(campaign_data)
    
    # Enqueue email sending jobs
    jobs = []
    for recipient in campaign_data['recipients']:
        job = naq_client.enqueue(
            send_campaign_email,
            campaign_id=campaign['id'],
            recipient=recipient,
            template=campaign_data['template'],
            queue_name='email_queue'
        )
        jobs.append(job.job_id)
    
    return jsonify({
        'campaign_id': campaign['id'],
        'jobs_enqueued': len(jobs),
        'job_ids': jobs
    })
```

## Prerequisites

1. **NATS Server** running with JetStream
2. **Web Framework** (Flask or FastAPI) installed
3. **NAQ Workers** running for web queues
4. **Secure Configuration**: `export NAQ_JOB_SERIALIZER=json`

## Running the Examples

### 1. Install Dependencies

```bash
# For Flask example
pip install flask flask-socketio redis

# For FastAPI example  
pip install fastapi uvicorn websockets

# For both
pip install python-multipart  # File uploads
```

### 2. Start Services

```bash
# Start NATS
cd docker && docker-compose up -d

# Set secure serialization
export NAQ_JOB_SERIALIZER=json

# Start workers for web processing
naq worker default web_queue file_processing webhook_queue email_queue --log-level INFO &

# Start dashboard for monitoring
naq dashboard --port 8000 &
```

### 3. Run Web Applications

```bash
cd examples/04-applications/01-web-integration

# Flask application
python flask_app.py

# FastAPI application
python fastapi_app.py

# Test the applications
python test_web_integration.py
```

## Example Applications

### E-commerce Order Processing
```python
@app.route('/api/orders', methods=['POST'])
def create_order():
    order_data = request.json
    
    # Create order record
    order = create_order_record(order_data)
    
    # Enqueue order processing workflow
    jobs = {
        'payment': naq_client.enqueue(
            process_payment,
            order_id=order['id'],
            amount=order['total'],
            queue_name='payment_queue'
        ),
        'inventory': naq_client.enqueue(
            update_inventory,
            items=order['items'],
            queue_name='inventory_queue'
        ),
        'notification': naq_client.enqueue(
            send_order_confirmation,
            order_id=order['id'],
            customer_email=order['customer']['email'],
            queue_name='email_queue'
        )
    }
    
    return jsonify({
        'order_id': order['id'],
        'status': 'processing',
        'jobs': {k: v.job_id for k, v in jobs.items()}
    })
```

### Social Media Content Processing
```python
@app.route('/api/posts', methods=['POST'])
def create_social_post():
    post_data = request.json
    
    # Enqueue content processing pipeline
    jobs = []
    
    # Content moderation
    moderation_job = naq_client.enqueue(
        moderate_content,
        content=post_data['content'],
        user_id=post_data['user_id'],
        queue_name='moderation_queue'
    )
    jobs.append(moderation_job)
    
    # Image processing (if images attached)
    if post_data.get('images'):
        for image_url in post_data['images']:
            image_job = naq_client.enqueue(
                process_social_image,
                image_url=image_url,
                depends_on=[moderation_job],
                queue_name='image_queue'
            )
            jobs.append(image_job)
    
    # Analytics tracking
    analytics_job = naq_client.enqueue(
        track_post_analytics,
        post_data=post_data,
        depends_on=jobs,
        queue_name='analytics_queue'
    )
    
    return jsonify({
        'post_id': post_data['id'],
        'jobs_enqueued': len(jobs) + 1,
        'moderation_job': moderation_job.job_id
    })
```

### Report Generation Service
```python
@app.route('/api/reports', methods=['POST'])
def generate_report():
    report_request = request.json
    
    # Validate report parameters
    if not validate_report_request(report_request):
        return jsonify({'error': 'Invalid report parameters'}), 400
    
    # Estimate processing time
    estimated_time = estimate_report_time(report_request)
    
    # Enqueue report generation
    job = naq_client.enqueue(
        generate_business_report,
        report_type=report_request['type'],
        date_range=report_request['date_range'],
        filters=report_request.get('filters', {}),
        format=report_request.get('format', 'pdf'),
        user_id=request.headers.get('User-ID'),
        queue_name='report_queue',
        # Long-running reports might need higher timeout
        job_timeout=timedelta(minutes=30)
    )
    
    return jsonify({
        'job_id': job.job_id,
        'estimated_time_minutes': estimated_time,
        'status_url': f'/api/reports/{job.job_id}/status',
        'download_url': f'/api/reports/{job.job_id}/download'
    })

@app.route('/api/reports/<job_id>/download')
def download_report(job_id):
    # Check job status
    status = naq_client.get_job_status(job_id)
    
    if status['status'] != 'completed':
        return jsonify({'error': 'Report not ready'}), 404
    
    # Get report file path from job result
    file_path = status['result']['file_path']
    
    return send_file(file_path, as_attachment=True)
```

## Real-time Features

### Server-Sent Events (SSE)
```python
from flask import Response
import json

@app.route('/api/jobs/<job_id>/stream')
def stream_job_progress(job_id):
    def event_stream():
        # Subscribe to job updates
        while True:
            status = naq_client.get_job_status(job_id)
            
            data = json.dumps({
                'job_id': job_id,
                'status': status['status'],
                'progress': status.get('progress', 0),
                'timestamp': datetime.now().isoformat()
            })
            
            yield f"data: {data}\n\n"
            
            if status['status'] in ['completed', 'failed']:
                break
            
            time.sleep(1)
    
    return Response(event_stream(), mimetype='text/plain')
```

### Progressive Web App Support
```python
@app.route('/api/jobs/<job_id>/notifications')
def setup_push_notifications(job_id):
    # Register for push notifications
    subscription = request.json
    
    # Store subscription for this job
    store_push_subscription(job_id, subscription)
    
    # Enqueue notification job
    naq_client.enqueue(
        send_push_notification_on_completion,
        job_id=job_id,
        subscription=subscription,
        queue_name='notification_queue'
    )
    
    return jsonify({'status': 'subscribed'})
```

## Testing Web Integration

### Unit Tests
```python
import unittest
from unittest.mock import patch, MagicMock

class TestWebIntegration(unittest.TestCase):
    def setUp(self):
        self.app = create_app(testing=True)
        self.client = self.app.test_client()
    
    @patch('naq.SyncClient')
    def test_job_creation(self, mock_naq):
        mock_job = MagicMock()
        mock_job.job_id = 'test_job_123'
        mock_naq.return_value.enqueue.return_value = mock_job
        
        response = self.client.post('/api/process', json={
            'user_id': 123,
            'operation': 'test'
        })
        
        self.assertEqual(response.status_code, 200)
        self.assertIn('job_id', response.json)
```

### Integration Tests
```python
def test_end_to_end_workflow():
    # Create test job
    response = client.post('/api/process', json=test_data)
    job_id = response.json['job_id']
    
    # Wait for completion
    timeout = 30
    while timeout > 0:
        status_response = client.get(f'/api/jobs/{job_id}')
        status = status_response.json['status']
        
        if status in ['completed', 'failed']:
            break
        
        time.sleep(1)
        timeout -= 1
    
    assert status == 'completed'
```

## Production Considerations

### Performance Optimization
- Use connection pooling for NAQ clients
- Implement request deduplication
- Cache job status for frequently checked jobs
- Use async endpoints for I/O heavy operations

### Security
- Validate all input data before enqueueing jobs
- Implement rate limiting for job creation
- Use authentication for job status endpoints
- Sanitize file uploads

### Monitoring
- Track job creation rates
- Monitor queue depths from web requests
- Alert on high failure rates
- Log all web-to-job interactions

### Scalability
- Use multiple worker instances
- Implement job priority based on user tiers
- Consider separate queues for different request types
- Plan for job result storage and cleanup

## Next Steps

- Explore [email system](../02-email-system/) for communication workflows
- Learn about [image processing](../03-image-processing/) for media handling
- Check out [data pipeline](../04-data-pipeline/) for ETL workflows
- Implement [high availability](../../05-advanced/02-high-availability/) for production web apps