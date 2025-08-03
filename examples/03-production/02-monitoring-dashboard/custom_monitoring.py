#!/usr/bin/env python3
"""
Custom Monitoring Implementation

This example demonstrates how to build custom monitoring solutions for NAQ:
- System health monitoring
- Performance metrics collection  
- Alert system integration
- Custom dashboard data

Before running:
1. Start NATS: `cd docker && docker-compose up -d`
2. Set secure serializer: `export NAQ_JOB_SERIALIZER=json`
3. Start workers: `naq worker default monitoring_queue --log-level INFO &`
4. Start dashboard: `naq dashboard --port 8000 &`
5. Run this script: `python custom_monitoring.py`
"""

import os
import time
import json
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict

from naq import SyncClient, setup_logging

# Configure secure JSON serialization
os.environ.setdefault('NAQ_JOB_SERIALIZER', 'json')

# Setup logging
setup_logging(level="INFO")


@dataclass
class WorkerMetrics:
    """Worker performance metrics."""
    worker_id: str
    status: str
    queue_name: str
    jobs_processed: int
    last_heartbeat: str
    cpu_usage: float
    memory_usage: float


@dataclass
class QueueMetrics:
    """Queue status metrics."""
    name: str
    pending_jobs: int
    processing_jobs: int
    completed_jobs: int
    failed_jobs: int
    avg_processing_time: float


@dataclass
class SystemMetrics:
    """Overall system metrics."""
    timestamp: str
    total_workers: int
    active_workers: int
    total_queues: int
    total_pending_jobs: int
    total_processing_jobs: int
    success_rate: float
    avg_job_duration: float


class NAQMonitor:
    """
    Comprehensive NAQ monitoring system.
    """
    
    def __init__(self):
        self.client = SyncClient()
        self.metrics_history = []
        self.alert_thresholds = {
            'min_active_workers': 1,
            'max_queue_depth': 100,
            'min_success_rate': 0.95,
            'max_avg_duration': 5000,  # 5 seconds
            'max_worker_cpu': 80.0,
            'max_worker_memory': 80.0
        }
        self.alert_callbacks = []
    
    def add_alert_callback(self, callback):
        """Add callback function for alerts."""
        self.alert_callbacks.append(callback)
    
    def send_alert(self, level: str, message: str, details: Dict[str, Any] = None):
        """Send alert to all registered callbacks."""
        alert = {
            'timestamp': datetime.now().isoformat(),
            'level': level,
            'message': message,
            'details': details or {}
        }
        
        print(f"🚨 ALERT [{level}]: {message}")
        if details:
            print(f"   Details: {details}")
        
        for callback in self.alert_callbacks:
            try:
                callback(alert)
            except Exception as e:
                print(f"Alert callback failed: {e}")
    
    def collect_worker_metrics(self) -> List[WorkerMetrics]:
        """Collect detailed worker metrics."""
        try:
            workers_data = self.client.list_workers()
            workers = []
            
            for worker_data in workers_data:
                # Simulate additional metrics that would come from system monitoring
                worker = WorkerMetrics(
                    worker_id=worker_data.get('worker_id', 'unknown'),
                    status=worker_data.get('status', 'unknown'),
                    queue_name=worker_data.get('queue_name', 'unknown'),
                    jobs_processed=worker_data.get('jobs_processed', 0),
                    last_heartbeat=worker_data.get('last_heartbeat', ''),
                    cpu_usage=self._simulate_cpu_usage(),
                    memory_usage=self._simulate_memory_usage()
                )
                workers.append(worker)
            
            return workers
            
        except Exception as e:
            print(f"Error collecting worker metrics: {e}")
            return []
    
    def collect_queue_metrics(self) -> List[QueueMetrics]:
        """Collect detailed queue metrics."""
        try:
            queues_data = self.client.list_queues()
            queues = []
            
            for queue_data in queues_data:
                # Simulate additional metrics
                queue = QueueMetrics(
                    name=queue_data.get('name', 'unknown'),
                    pending_jobs=queue_data.get('pending_jobs', 0),
                    processing_jobs=queue_data.get('processing_jobs', 0),
                    completed_jobs=queue_data.get('completed_jobs', 0),
                    failed_jobs=queue_data.get('failed_jobs', 0),
                    avg_processing_time=self._simulate_processing_time()
                )
                queues.append(queue)
            
            return queues
            
        except Exception as e:
            print(f"Error collecting queue metrics: {e}")
            return []
    
    def collect_system_metrics(self) -> SystemMetrics:
        """Collect overall system metrics."""
        worker_metrics = self.collect_worker_metrics()
        queue_metrics = self.collect_queue_metrics()
        
        active_workers = len([w for w in worker_metrics if w.status == 'active'])
        total_pending = sum(q.pending_jobs for q in queue_metrics)
        total_processing = sum(q.processing_jobs for q in queue_metrics)
        
        # Calculate success rate
        total_completed = sum(q.completed_jobs for q in queue_metrics)
        total_failed = sum(q.failed_jobs for q in queue_metrics)
        success_rate = total_completed / (total_completed + total_failed) if (total_completed + total_failed) > 0 else 1.0
        
        # Calculate average duration
        avg_duration = sum(q.avg_processing_time for q in queue_metrics) / len(queue_metrics) if queue_metrics else 0
        
        return SystemMetrics(
            timestamp=datetime.now().isoformat(),
            total_workers=len(worker_metrics),
            active_workers=active_workers,
            total_queues=len(queue_metrics),
            total_pending_jobs=total_pending,
            total_processing_jobs=total_processing,
            success_rate=success_rate,
            avg_job_duration=avg_duration
        )
    
    def check_health_thresholds(self, system_metrics: SystemMetrics, worker_metrics: List[WorkerMetrics], queue_metrics: List[QueueMetrics]):
        """Check metrics against health thresholds and send alerts."""
        
        # Check minimum active workers
        if system_metrics.active_workers < self.alert_thresholds['min_active_workers']:
            self.send_alert(
                'CRITICAL',
                f'Too few active workers: {system_metrics.active_workers}',
                {'threshold': self.alert_thresholds['min_active_workers']}
            )
        
        # Check queue depths
        for queue in queue_metrics:
            if queue.pending_jobs > self.alert_thresholds['max_queue_depth']:
                self.send_alert(
                    'WARNING',
                    f'Queue {queue.name} has high pending jobs: {queue.pending_jobs}',
                    {'queue': queue.name, 'threshold': self.alert_thresholds['max_queue_depth']}
                )
        
        # Check success rate
        if system_metrics.success_rate < self.alert_thresholds['min_success_rate']:
            self.send_alert(
                'WARNING',
                f'Low system success rate: {system_metrics.success_rate:.2%}',
                {'threshold': self.alert_thresholds['min_success_rate']}
            )
        
        # Check average job duration
        if system_metrics.avg_job_duration > self.alert_thresholds['max_avg_duration']:
            self.send_alert(
                'WARNING',
                f'High average job duration: {system_metrics.avg_job_duration:.2f}ms',
                {'threshold': self.alert_thresholds['max_avg_duration']}
            )
        
        # Check individual worker health
        for worker in worker_metrics:
            if worker.cpu_usage > self.alert_thresholds['max_worker_cpu']:
                self.send_alert(
                    'WARNING',
                    f'Worker {worker.worker_id} high CPU usage: {worker.cpu_usage:.1f}%',
                    {'worker_id': worker.worker_id, 'threshold': self.alert_thresholds['max_worker_cpu']}
                )
            
            if worker.memory_usage > self.alert_thresholds['max_worker_memory']:
                self.send_alert(
                    'WARNING',
                    f'Worker {worker.worker_id} high memory usage: {worker.memory_usage:.1f}%',
                    {'worker_id': worker.worker_id, 'threshold': self.alert_thresholds['max_worker_memory']}
                )
    
    def _simulate_cpu_usage(self) -> float:
        """Simulate CPU usage metrics."""
        import random
        return random.uniform(10, 90)
    
    def _simulate_memory_usage(self) -> float:
        """Simulate memory usage metrics."""
        import random
        return random.uniform(20, 85)
    
    def _simulate_processing_time(self) -> float:
        """Simulate average processing time."""
        import random
        return random.uniform(100, 3000)  # 100ms to 3 seconds
    
    def run_monitoring_cycle(self):
        """Run a complete monitoring cycle."""
        print("🔍 Running monitoring cycle...")
        
        # Collect all metrics
        system_metrics = self.collect_system_metrics()
        worker_metrics = self.collect_worker_metrics()
        queue_metrics = self.collect_queue_metrics()
        
        # Store metrics history
        self.metrics_history.append({
            'system': asdict(system_metrics),
            'workers': [asdict(w) for w in worker_metrics],
            'queues': [asdict(q) for q in queue_metrics]
        })
        
        # Keep only last 100 entries
        if len(self.metrics_history) > 100:
            self.metrics_history.pop(0)
        
        # Check health thresholds
        self.check_health_thresholds(system_metrics, worker_metrics, queue_metrics)
        
        # Display summary
        self.display_monitoring_summary(system_metrics, worker_metrics, queue_metrics)
        
        return {
            'system': system_metrics,
            'workers': worker_metrics,
            'queues': queue_metrics
        }
    
    def display_monitoring_summary(self, system_metrics: SystemMetrics, worker_metrics: List[WorkerMetrics], queue_metrics: List[QueueMetrics]):
        """Display monitoring summary."""
        print(f"\n📊 Monitoring Summary - {system_metrics.timestamp}")
        print("=" * 60)
        
        # System overview
        print(f"🖥️  System Overview:")
        print(f"   Workers: {system_metrics.active_workers}/{system_metrics.total_workers} active")
        print(f"   Queues: {system_metrics.total_queues} total")
        print(f"   Jobs: {system_metrics.total_pending_jobs} pending, {system_metrics.total_processing_jobs} processing")
        print(f"   Success Rate: {system_metrics.success_rate:.2%}")
        print(f"   Avg Duration: {system_metrics.avg_job_duration:.1f}ms")
        
        # Worker details
        if worker_metrics:
            print(f"\n👥 Worker Details:")
            for worker in worker_metrics:
                status_icon = "✅" if worker.status == "active" else "❌"
                print(f"   {status_icon} {worker.worker_id[:8]}...")
                print(f"      Queue: {worker.queue_name}")
                print(f"      Jobs: {worker.jobs_processed}")
                print(f"      CPU: {worker.cpu_usage:.1f}% | Memory: {worker.memory_usage:.1f}%")
        
        # Queue details
        if queue_metrics:
            print(f"\n📋 Queue Details:")
            for queue in queue_metrics:
                print(f"   📂 {queue.name}:")
                print(f"      Pending: {queue.pending_jobs} | Processing: {queue.processing_jobs}")
                print(f"      Completed: {queue.completed_jobs} | Failed: {queue.failed_jobs}")
                print(f"      Avg Time: {queue.avg_processing_time:.1f}ms")
    
    def start_continuous_monitoring(self, interval_seconds: int = 30):
        """Start continuous monitoring in background thread."""
        def monitor_loop():
            while True:
                try:
                    self.run_monitoring_cycle()
                    time.sleep(interval_seconds)
                except KeyboardInterrupt:
                    print("\n🛑 Monitoring stopped by user")
                    break
                except Exception as e:
                    print(f"❌ Monitoring error: {e}")
                    time.sleep(interval_seconds)
        
        monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
        monitor_thread.start()
        return monitor_thread
    
    def get_metrics_history(self, last_n: int = 10) -> List[Dict[str, Any]]:
        """Get recent metrics history."""
        return self.metrics_history[-last_n:]
    
    def export_metrics_json(self, filename: str = None):
        """Export current metrics to JSON file."""
        if filename is None:
            filename = f"naq_metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        current_metrics = self.run_monitoring_cycle()
        
        with open(filename, 'w') as f:
            json.dump({
                'export_time': datetime.now().isoformat(),
                'metrics': current_metrics,
                'history': self.metrics_history
            }, f, indent=2, default=str)
        
        print(f"📄 Metrics exported to {filename}")
        return filename


def email_alert_callback(alert: Dict[str, Any]):
    """Simulate email alert callback."""
    print(f"📧 [EMAIL ALERT] {alert['level']}: {alert['message']}")
    # In production, this would send actual emails


def slack_alert_callback(alert: Dict[str, Any]):
    """Simulate Slack alert callback."""
    print(f"💬 [SLACK ALERT] {alert['level']}: {alert['message']}")
    # In production, this would send to Slack webhook


def pagerduty_alert_callback(alert: Dict[str, Any]):
    """Simulate PagerDuty alert callback."""
    if alert['level'] == 'CRITICAL':
        print(f"📟 [PAGERDUTY ALERT] {alert['message']}")
        # In production, this would trigger PagerDuty incident


def demonstrate_basic_monitoring():
    """Demonstrate basic monitoring functionality."""
    print("📍 Basic Monitoring Demo")
    print("-" * 40)
    
    monitor = NAQMonitor()
    
    # Add alert callbacks
    monitor.add_alert_callback(email_alert_callback)
    monitor.add_alert_callback(slack_alert_callback)
    monitor.add_alert_callback(pagerduty_alert_callback)
    
    # Run single monitoring cycle
    print("🔍 Running single monitoring cycle...")
    metrics = monitor.run_monitoring_cycle()
    
    # Export metrics
    print("\n📄 Exporting metrics...")
    filename = monitor.export_metrics_json()
    
    return monitor, metrics


def demonstrate_continuous_monitoring():
    """Demonstrate continuous monitoring."""
    print("\n📍 Continuous Monitoring Demo")
    print("-" * 40)
    
    monitor = NAQMonitor()
    
    # Customize alert thresholds for demo
    monitor.alert_thresholds.update({
        'min_active_workers': 0,  # Lower threshold for demo
        'max_queue_depth': 5,     # Lower threshold to trigger alerts
        'min_success_rate': 0.90, # Lower threshold for demo
        'max_avg_duration': 2000  # Lower threshold for demo
    })
    
    # Add alert callbacks
    monitor.add_alert_callback(email_alert_callback)
    monitor.add_alert_callback(slack_alert_callback)
    
    print("🔄 Starting continuous monitoring (30-second intervals)...")
    print("   Press Ctrl+C to stop")
    
    try:
        # Start continuous monitoring
        monitor_thread = monitor.start_continuous_monitoring(interval_seconds=30)
        
        # Let it run for demonstration
        time.sleep(120)  # Run for 2 minutes
        
    except KeyboardInterrupt:
        print("\n🛑 Continuous monitoring stopped")
    
    return monitor


def demonstrate_metrics_history():
    """Demonstrate metrics history and analysis."""
    print("\n📍 Metrics History Demo")
    print("-" * 40)
    
    monitor = NAQMonitor()
    
    # Collect several metrics snapshots
    print("📊 Collecting metrics history...")
    for i in range(5):
        print(f"   Snapshot {i+1}/5...")
        monitor.run_monitoring_cycle()
        time.sleep(5)
    
    # Analyze history
    history = monitor.get_metrics_history()
    
    print(f"\n📈 Metrics History Analysis:")
    print(f"   Total snapshots: {len(history)}")
    
    if history:
        # Worker count trend
        worker_counts = [entry['system']['active_workers'] for entry in history]
        print(f"   Worker count range: {min(worker_counts)} - {max(worker_counts)}")
        
        # Success rate trend
        success_rates = [entry['system']['success_rate'] for entry in history]
        avg_success_rate = sum(success_rates) / len(success_rates)
        print(f"   Average success rate: {avg_success_rate:.2%}")
        
        # Duration trend
        durations = [entry['system']['avg_job_duration'] for entry in history]
        avg_duration = sum(durations) / len(durations)
        print(f"   Average job duration: {avg_duration:.1f}ms")
    
    return monitor


def main():
    """
    Main function demonstrating custom monitoring implementation.
    """
    print("🚀 NAQ Custom Monitoring Demo")
    print("=" * 50)
    
    try:
        # Demonstrate different monitoring patterns
        basic_monitor, basic_metrics = demonstrate_basic_monitoring()
        continuous_monitor = demonstrate_continuous_monitoring()
        history_monitor = demonstrate_metrics_history()
        
        print(f"\n🎉 Custom monitoring demo completed!")
        
        print("\n" + "=" * 50)
        print("📊 Monitoring Features Demonstrated:")
        print("=" * 50)
        print("✅ Basic system metrics collection")
        print("✅ Worker and queue monitoring")
        print("✅ Health threshold checking")
        print("✅ Multi-channel alerting (email, Slack, PagerDuty)")
        print("✅ Continuous monitoring with background threads")
        print("✅ Metrics history and trend analysis")
        print("✅ JSON export for external systems")
        
        print("\n🎯 Production Implementation Tips:")
        print("   • Set appropriate alert thresholds for your environment")
        print("   • Implement actual alert delivery (email, Slack, etc.)")
        print("   • Store metrics in time-series database (InfluxDB, Prometheus)")
        print("   • Create custom dashboards for your specific needs")
        print("   • Monitor business metrics alongside technical metrics")
        
        print("\n📋 Next Steps:")
        print("   • Integrate with your existing monitoring infrastructure")
        print("   • Set up automated alerting and escalation")
        print("   • Create custom dashboards for different stakeholders")
        print("   • Implement performance baselines and SLA monitoring")
        
        print("\n💡 Access the built-in dashboard:")
        print("   • Web dashboard: http://localhost:8000")
        print("   • API endpoints: http://localhost:8000/api/status")
        print("   • Use both custom monitoring AND built-in dashboard")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\n🔧 Troubleshooting:")
        print("   - Is NATS running? (cd docker && docker-compose up -d)")
        print("   - Are workers running? (naq worker default monitoring_queue)")
        print("   - Is dashboard running? (naq dashboard --port 8000)")
        print("   - Is NAQ_JOB_SERIALIZER=json set?")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())