#!/usr/bin/env python3
"""
Flask Web Integration Example

This example demonstrates integrating NAQ with Flask for real-world web applications:
- Background job processing from web requests
- File upload handling
- Webhook processing
- Real-time job status updates
- API-driven job management

Before running:
1. Start NATS: `cd docker && docker-compose up -d`
2. Set secure serializer: `export NAQ_JOB_SERIALIZER=json`
3. Start workers: `naq worker default web_queue file_queue webhook_queue email_queue --log-level INFO`
4. Install Flask: `pip install flask flask-socketio redis python-multipart`
5. Run this script: `python flask_app.py`
"""

import os
import time
import uuid
import hashlib
import hmac
import json
from datetime import datetime, timedelta
from werkzeug.utils import secure_filename
from werkzeug.exceptions import RequestEntityTooLarge

from flask import Flask, request, jsonify, send_file, Response
from flask_socketio import SocketIO, emit, join_room, leave_room

from naq import SyncClient, setup_logging

# Configure secure JSON serialization
os.environ.setdefault('NAQ_JOB_SERIALIZER', 'json')

# Setup logging
setup_logging(level="INFO")

# Flask app configuration
app = Flask(__name__)
app.config['SECRET_KEY'] = 'dev_secret_key'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
app.config['UPLOAD_FOLDER'] = '/tmp/naq_uploads'

# Initialize SocketIO for real-time updates
socketio = SocketIO(app, cors_allowed_origins="*")

# NAQ client
naq_client = SyncClient()

# In-memory storage for demo (use Redis/database in production)
job_subscriptions = {}
active_jobs = {}

# Ensure upload directory exists
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)


# ===== JOB FUNCTIONS =====

def process_user_data(user_id: int, operation: str, data: dict = None) -> dict:
    """
    Process user data in background.
    
    Args:
        user_id: User identifier
        operation: Type of operation
        data: Additional data
        
    Returns:
        Processing results
    """
    print(f"👤 Processing user data: ID={user_id}, Operation={operation}")
    
    # Simulate processing time
    processing_time = 2 + (hash(str(user_id)) % 5)  # 2-6 seconds
    time.sleep(processing_time)
    
    result = {
        "user_id": user_id,
        "operation": operation,
        "processed_at": datetime.now().isoformat(),
        "processing_time_seconds": processing_time,
        "data_processed": data is not None,
        "status": "completed"
    }
    
    print(f"✅ User data processing completed for user {user_id}")
    
    # Notify via SocketIO if there are subscribers
    if user_id in job_subscriptions:
        socketio.emit('job_completed', {
            'user_id': user_id,
            'operation': operation,
            'result': result
        }, room=f'user_{user_id}')
    
    return result


def process_uploaded_file(file_path: str, original_filename: str, user_id: str = None) -> dict:
    """
    Process an uploaded file.
    
    Args:
        file_path: Path to uploaded file
        original_filename: Original filename
        user_id: User who uploaded the file
        
    Returns:
        File processing results
    """
    print(f"📁 Processing uploaded file: {original_filename}")
    
    try:
        # Get file info
        file_size = os.path.getsize(file_path)
        
        # Simulate file processing (analysis, conversion, etc.)
        processing_time = min(file_size / 100000, 10)  # Scale with file size, max 10s
        time.sleep(processing_time)
        
        # Generate processed file info
        result = {
            "original_filename": original_filename,
            "file_size_bytes": file_size,
            "processed_at": datetime.now().isoformat(),
            "processing_time_seconds": processing_time,
            "file_type": original_filename.split('.')[-1] if '.' in original_filename else 'unknown',
            "processed_file_path": f"/tmp/processed_{original_filename}",
            "status": "completed"
        }
        
        # Clean up original file
        if os.path.exists(file_path):
            os.remove(file_path)
        
        print(f"✅ File processing completed: {original_filename}")
        return result
        
    except Exception as e:
        print(f"❌ File processing failed: {e}")
        # Clean up on failure
        if os.path.exists(file_path):
            os.remove(file_path)
        raise


def process_webhook(event_type: str, payload: dict, source: str = "unknown") -> dict:
    """
    Process webhook payload.
    
    Args:
        event_type: Type of webhook event
        payload: Webhook payload
        source: Source of webhook (github, stripe, etc.)
        
    Returns:
        Webhook processing results
    """
    print(f"🔗 Processing webhook: {source} - {event_type}")
    
    # Simulate webhook processing
    time.sleep(1 + (hash(event_type) % 3))  # 1-3 seconds
    
    result = {
        "source": source,
        "event_type": event_type,
        "payload_size": len(str(payload)),
        "processed_at": datetime.now().isoformat(),
        "actions_taken": [],
        "status": "completed"
    }
    
    # Simulate different actions based on event type
    if event_type == "push":
        result["actions_taken"].append("trigger_ci_build")
    elif event_type == "pull_request":
        result["actions_taken"].append("run_tests")
    elif event_type == "payment.succeeded":
        result["actions_taken"].append("fulfill_order")
    elif event_type == "user.created":
        result["actions_taken"].append("send_welcome_email")
    
    print(f"✅ Webhook processing completed: {source} - {event_type}")
    return result


def send_email_campaign(campaign_id: str, recipient: str, template: str, data: dict = None) -> dict:
    """
    Send campaign email to recipient.
    
    Args:
        campaign_id: Campaign identifier
        recipient: Email recipient
        template: Email template
        data: Template data
        
    Returns:
        Email sending results
    """
    print(f"📧 Sending campaign email: {campaign_id} to {recipient}")
    
    # Simulate email sending
    time.sleep(0.5 + (hash(recipient) % 2))  # 0.5-2.5 seconds
    
    result = {
        "campaign_id": campaign_id,
        "recipient": recipient,
        "template": template,
        "sent_at": datetime.now().isoformat(),
        "message_id": f"msg_{uuid.uuid4().hex[:8]}",
        "status": "sent"
    }
    
    print(f"✅ Email sent: {campaign_id} to {recipient}")
    return result


def generate_report(report_type: str, date_range: dict, filters: dict = None, format: str = "pdf") -> dict:
    """
    Generate business report.
    
    Args:
        report_type: Type of report
        date_range: Date range for report
        filters: Additional filters
        format: Output format
        
    Returns:
        Report generation results
    """
    print(f"📊 Generating report: {report_type} ({format})")
    
    # Simulate report generation time
    complexity_factor = len(filters) if filters else 1
    processing_time = 5 + (complexity_factor * 2)  # 5+ seconds based on complexity
    time.sleep(processing_time)
    
    # Generate dummy report file
    report_filename = f"report_{report_type}_{uuid.uuid4().hex[:8]}.{format}"
    report_path = f"/tmp/{report_filename}"
    
    with open(report_path, 'w') as f:
        f.write(f"Report: {report_type}\n")
        f.write(f"Generated: {datetime.now().isoformat()}\n")
        f.write(f"Date Range: {date_range}\n")
        f.write(f"Filters: {filters}\n")
    
    result = {
        "report_type": report_type,
        "date_range": date_range,
        "filters": filters,
        "format": format,
        "file_path": report_path,
        "file_size": os.path.getsize(report_path),
        "generated_at": datetime.now().isoformat(),
        "processing_time_seconds": processing_time,
        "status": "completed"
    }
    
    print(f"✅ Report generated: {report_type}")
    return result


# ===== UTILITY FUNCTIONS =====

def verify_webhook_signature(payload: bytes, signature: str, secret: str) -> bool:
    """Verify webhook signature (GitHub style)."""
    if not signature.startswith('sha256='):
        return False
    
    expected_signature = hmac.new(
        secret.encode('utf-8'),
        payload,
        hashlib.sha256
    ).hexdigest()
    
    return hmac.compare_digest(f"sha256={expected_signature}", signature)


def validate_campaign_data(data: dict) -> bool:
    """Validate email campaign data."""
    required_fields = ['name', 'template', 'recipients']
    return all(field in data for field in required_fields)


def estimate_report_time(report_request: dict) -> int:
    """Estimate report generation time in minutes."""
    base_time = 2  # Base 2 minutes
    
    # Add time based on complexity
    if report_request.get('filters'):
        base_time += len(report_request['filters'])
    
    if report_request.get('format') == 'excel':
        base_time += 1
    
    return min(base_time, 15)  # Cap at 15 minutes


# ===== WEB API ROUTES =====

@app.route('/')
def index():
    """Home page with API documentation."""
    return jsonify({
        "message": "NAQ Flask Integration Demo",
        "version": "1.0.0",
        "endpoints": {
            "POST /api/process": "Process user data in background",
            "POST /api/upload": "Upload and process files",
            "POST /webhooks/github": "Handle GitHub webhooks",
            "POST /api/campaigns": "Create email campaigns",
            "POST /api/reports": "Generate reports",
            "GET /api/jobs/<job_id>": "Get job status",
            "GET /api/jobs/<job_id>/stream": "Stream job progress"
        },
        "websocket": "Real-time job updates available",
        "dashboard": "http://localhost:8000"
    })


@app.route('/api/process', methods=['POST'])
def process_data():
    """Process user data in background."""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'error': 'No JSON data provided'}), 400
        
        # Validate required fields
        if 'user_id' not in data or 'operation' not in data:
            return jsonify({'error': 'user_id and operation are required'}), 400
        
        # Enqueue background job
        job = naq_client.enqueue(
            process_user_data,
            user_id=data['user_id'],
            operation=data['operation'],
            data=data.get('data'),
            queue_name='web_queue'
        )
        
        # Store job info for tracking
        active_jobs[job.job_id] = {
            'type': 'user_data_processing',
            'user_id': data['user_id'],
            'operation': data['operation'],
            'created_at': datetime.now().isoformat()
        }
        
        return jsonify({
            'job_id': job.job_id,
            'status': 'enqueued',
            'estimated_completion': '2-6 minutes',
            'status_url': f'/api/jobs/{job.job_id}',
            'stream_url': f'/api/jobs/{job.job_id}/stream'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/upload', methods=['POST'])
def handle_file_upload():
    """Handle file upload and processing."""
    try:
        # Check if file is in request
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        # Secure filename
        filename = secure_filename(file.filename)
        if not filename:
            return jsonify({'error': 'Invalid filename'}), 400
        
        # Save file temporarily
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], f"{uuid.uuid4().hex}_{filename}")
        file.save(file_path)
        
        # Enqueue file processing job
        job = naq_client.enqueue(
            process_uploaded_file,
            file_path=file_path,
            original_filename=filename,
            user_id=request.form.get('user_id'),
            queue_name='file_queue'
        )
        
        # Store job info
        active_jobs[job.job_id] = {
            'type': 'file_processing',
            'filename': filename,
            'file_size': os.path.getsize(file_path),
            'created_at': datetime.now().isoformat()
        }
        
        return jsonify({
            'job_id': job.job_id,
            'filename': filename,
            'status': 'enqueued',
            'message': 'File uploaded and queued for processing',
            'status_url': f'/api/jobs/{job.job_id}'
        })
        
    except RequestEntityTooLarge:
        return jsonify({'error': 'File too large'}), 413
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/webhooks/github', methods=['POST'])
def handle_github_webhook():
    """Handle GitHub webhook."""
    try:
        # Get webhook signature
        signature = request.headers.get('X-Hub-Signature-256', '')
        
        # In production, verify signature
        # if not verify_webhook_signature(request.data, signature, GITHUB_SECRET):
        #     return 'Unauthorized', 401
        
        payload = request.get_json()
        event_type = request.headers.get('X-GitHub-Event', 'unknown')
        
        # Enqueue webhook processing
        job = naq_client.enqueue(
            process_webhook,
            event_type=event_type,
            payload=payload,
            source='github',
            queue_name='webhook_queue'
        )
        
        # Store job info
        active_jobs[job.job_id] = {
            'type': 'webhook_processing',
            'source': 'github',
            'event_type': event_type,
            'created_at': datetime.now().isoformat()
        }
        
        return jsonify({
            'received': True,
            'job_id': job.job_id,
            'event_type': event_type
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/campaigns', methods=['POST'])
def create_email_campaign():
    """Create and process email campaign."""
    try:
        campaign_data = request.get_json()
        
        # Validate campaign data
        if not validate_campaign_data(campaign_data):
            return jsonify({'error': 'Invalid campaign data'}), 400
        
        campaign_id = f"camp_{uuid.uuid4().hex[:8]}"
        
        # Enqueue email sending jobs
        jobs = []
        for recipient in campaign_data['recipients']:
            job = naq_client.enqueue(
                send_email_campaign,
                campaign_id=campaign_id,
                recipient=recipient,
                template=campaign_data['template'],
                data=campaign_data.get('data', {}),
                queue_name='email_queue'
            )
            jobs.append(job.job_id)
        
        # Store campaign info
        for job_id in jobs:
            active_jobs[job_id] = {
                'type': 'email_campaign',
                'campaign_id': campaign_id,
                'created_at': datetime.now().isoformat()
            }
        
        return jsonify({
            'campaign_id': campaign_id,
            'jobs_enqueued': len(jobs),
            'job_ids': jobs,
            'recipients': len(campaign_data['recipients']),
            'template': campaign_data['template']
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/reports', methods=['POST'])
def generate_business_report():
    """Generate business report."""
    try:
        report_request = request.get_json()
        
        # Validate report request
        required_fields = ['type', 'date_range']
        if not all(field in report_request for field in required_fields):
            return jsonify({'error': 'type and date_range are required'}), 400
        
        # Estimate processing time
        estimated_time = estimate_report_time(report_request)
        
        # Enqueue report generation
        job = naq_client.enqueue(
            generate_report,
            report_type=report_request['type'],
            date_range=report_request['date_range'],
            filters=report_request.get('filters', {}),
            format=report_request.get('format', 'pdf'),
            queue_name='web_queue',
            # Long-running reports need higher timeout
            job_timeout=timedelta(minutes=30)
        )
        
        # Store job info
        active_jobs[job.job_id] = {
            'type': 'report_generation',
            'report_type': report_request['type'],
            'format': report_request.get('format', 'pdf'),
            'created_at': datetime.now().isoformat()
        }
        
        return jsonify({
            'job_id': job.job_id,
            'report_type': report_request['type'],
            'estimated_time_minutes': estimated_time,
            'status_url': f'/api/jobs/{job.job_id}',
            'download_url': f'/api/reports/{job.job_id}/download'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/jobs/<job_id>', methods=['GET'])
def get_job_status(job_id):
    """Get job status."""
    try:
        # Get status from NAQ
        status = naq_client.get_job_status(job_id)
        
        if not status:
            return jsonify({'error': 'Job not found'}), 404
        
        # Add additional info if available
        if job_id in active_jobs:
            status.update(active_jobs[job_id])
        
        return jsonify(status)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/jobs/<job_id>/stream')
def stream_job_progress(job_id):
    """Stream job progress using Server-Sent Events."""
    def event_stream():
        try:
            while True:
                # Get current status
                status = naq_client.get_job_status(job_id)
                
                if not status:
                    yield f"data: {json.dumps({'error': 'Job not found'})}\n\n"
                    break
                
                # Add timestamp
                status['timestamp'] = datetime.now().isoformat()
                
                # Send status update
                yield f"data: {json.dumps(status)}\n\n"
                
                # Stop streaming if job is complete
                if status.get('status') in ['completed', 'failed']:
                    break
                
                time.sleep(2)  # Update every 2 seconds
                
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
    
    return Response(event_stream(), mimetype='text/plain')


@app.route('/api/reports/<job_id>/download')
def download_report(job_id):
    """Download generated report."""
    try:
        # Check job status
        status = naq_client.get_job_status(job_id)
        
        if not status:
            return jsonify({'error': 'Job not found'}), 404
        
        if status.get('status') != 'completed':
            return jsonify({'error': 'Report not ready', 'status': status.get('status')}), 404
        
        # Get report file path from job result
        result = status.get('result', {})
        file_path = result.get('file_path')
        
        if not file_path or not os.path.exists(file_path):
            return jsonify({'error': 'Report file not found'}), 404
        
        return send_file(file_path, as_attachment=True)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ===== WEBSOCKET EVENTS =====

@socketio.on('connect')
def handle_connect():
    """Handle client connection."""
    print(f"🔌 Client connected: {request.sid}")
    emit('connected', {'message': 'Connected to NAQ job updates'})


@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection."""
    print(f"🔌 Client disconnected: {request.sid}")


@socketio.on('subscribe_job')
def handle_job_subscription(data):
    """Subscribe to job updates."""
    job_id = data.get('job_id')
    if job_id:
        join_room(f'job_{job_id}')
        job_subscriptions[job_id] = request.sid
        emit('subscribed', {'job_id': job_id})
        print(f"📺 Client subscribed to job: {job_id}")


@socketio.on('unsubscribe_job')
def handle_job_unsubscription(data):
    """Unsubscribe from job updates."""
    job_id = data.get('job_id')
    if job_id:
        leave_room(f'job_{job_id}')
        if job_id in job_subscriptions:
            del job_subscriptions[job_id]
        emit('unsubscribed', {'job_id': job_id})
        print(f"📺 Client unsubscribed from job: {job_id}")


@socketio.on('subscribe_user')
def handle_user_subscription(data):
    """Subscribe to user-specific updates."""
    user_id = data.get('user_id')
    if user_id:
        join_room(f'user_{user_id}')
        emit('user_subscribed', {'user_id': user_id})
        print(f"👤 Client subscribed to user updates: {user_id}")


# ===== ERROR HANDLERS =====

@app.errorhandler(413)
def handle_large_file(e):
    """Handle file too large error."""
    return jsonify({'error': 'File too large. Maximum size is 16MB.'}), 413


@app.errorhandler(404)
def handle_not_found(e):
    """Handle 404 errors."""
    return jsonify({'error': 'Endpoint not found'}), 404


@app.errorhandler(500)
def handle_server_error(e):
    """Handle 500 errors."""
    return jsonify({'error': 'Internal server error'}), 500


# ===== MAIN APPLICATION =====

def main():
    """Main application entry point."""
    print("🚀 Starting NAQ Flask Integration Demo")
    print("=" * 50)
    
    print("📋 Available Endpoints:")
    print("   POST /api/process - Process user data")
    print("   POST /api/upload - Upload and process files")
    print("   POST /webhooks/github - Handle GitHub webhooks")
    print("   POST /api/campaigns - Create email campaigns")
    print("   POST /api/reports - Generate reports")
    print("   GET /api/jobs/<job_id> - Get job status")
    print("   GET /api/jobs/<job_id>/stream - Stream job progress")
    print("   GET /api/reports/<job_id>/download - Download reports")
    
    print("\n🔌 WebSocket Events:")
    print("   subscribe_job - Subscribe to job updates")
    print("   subscribe_user - Subscribe to user updates")
    
    print("\n💡 Test the application:")
    print("   curl -X POST http://localhost:5000/api/process \\")
    print("     -H 'Content-Type: application/json' \\")
    print("     -d '{\"user_id\": 123, \"operation\": \"test\"}'")
    
    print("\n🌐 Starting Flask application on http://localhost:5000")
    print("📊 NAQ Dashboard available at http://localhost:8000")
    
    # Run the application
    socketio.run(app, host='0.0.0.0', port=5000, debug=True)


if __name__ == '__main__':
    main()