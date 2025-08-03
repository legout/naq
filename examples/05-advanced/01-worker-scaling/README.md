# Worker Scaling - Advanced Worker Management

This example demonstrates advanced worker scaling patterns for NAQ applications, including horizontal scaling, auto-scaling, load balancing, and dynamic worker management for high-throughput production systems.

## What You'll Learn

- Horizontal worker scaling strategies
- Dynamic auto-scaling based on queue metrics
- Load balancing across worker pools
- Queue-specific worker allocation
- Resource-aware worker management
- Container orchestration patterns
- Performance optimization techniques
- Production scaling best practices

## Scaling Patterns

### Horizontal Scaling
Scale workers across multiple machines:

```bash
# Machine 1: General purpose workers
naq worker default general_queue --concurrency 4

# Machine 2: CPU-intensive workers  
naq worker default cpu_queue --concurrency 2

# Machine 3: I/O intensive workers
naq worker default io_queue --concurrency 8

# Machine 4: Mixed workload
naq worker default general_queue cpu_queue --concurrency 6
```

### Vertical Scaling
Optimize single-machine worker configuration:

```bash
# High-concurrency setup for I/O bound jobs
naq worker default io_queue --concurrency 20 --worker-ttl 300

# Low-concurrency setup for CPU bound jobs  
naq worker default cpu_queue --concurrency $(nproc) --worker-ttl 600

# Memory-optimized setup
naq worker default memory_queue --concurrency 4 --max-memory 2GB
```

### Queue-Specific Scaling
Different scaling strategies per queue:

```python
SCALING_CONFIG = {
    'high_priority': {
        'min_workers': 5,
        'max_workers': 20,
        'target_queue_depth': 10,
        'scale_up_threshold': 20,
        'scale_down_threshold': 5
    },
    'batch_processing': {
        'min_workers': 2,
        'max_workers': 50,
        'target_queue_depth': 100,
        'scale_up_threshold': 200,
        'scale_down_threshold': 50
    },
    'real_time': {
        'min_workers': 10,
        'max_workers': 30,
        'target_queue_depth': 0,
        'scale_up_threshold': 5,
        'scale_down_threshold': 0
    }
}
```

## Auto-Scaling Implementation

### Metrics-Based Auto-Scaling
```python
class NAQAutoScaler:
    """Automatic worker scaling based on queue metrics."""
    
    def __init__(self, scaling_config):
        self.config = scaling_config
        self.client = SyncClient()
        self.active_workers = {}
    
    def check_scaling_needs(self):
        """Check if scaling is needed for any queue."""
        
        queues = self.client.list_queues()
        
        for queue in queues:
            queue_name = queue['name']
            if queue_name not in self.config:
                continue
            
            config = self.config[queue_name]
            current_workers = self.get_active_workers(queue_name)
            pending_jobs = queue['pending_jobs']
            
            # Determine scaling action
            if pending_jobs > config['scale_up_threshold']:
                if current_workers < config['max_workers']:
                    self.scale_up(queue_name, config)
            
            elif pending_jobs < config['scale_down_threshold']:
                if current_workers > config['min_workers']:
                    self.scale_down(queue_name, config)
    
    def scale_up(self, queue_name, config):
        """Scale up workers for a queue."""
        
        # Calculate how many workers to add
        current_workers = self.get_active_workers(queue_name)
        target_workers = min(
            current_workers + self.calculate_scale_increment(queue_name),
            config['max_workers']
        )
        
        workers_to_add = target_workers - current_workers
        
        if workers_to_add > 0:
            self.spawn_workers(queue_name, workers_to_add)
            logger.info(f"Scaled up {queue_name}: +{workers_to_add} workers ({current_workers} -> {target_workers})")
    
    def scale_down(self, queue_name, config):
        """Scale down workers for a queue."""
        
        current_workers = self.get_active_workers(queue_name)
        target_workers = max(
            current_workers - self.calculate_scale_decrement(queue_name),
            config['min_workers']
        )
        
        workers_to_remove = current_workers - target_workers
        
        if workers_to_remove > 0:
            self.terminate_workers(queue_name, workers_to_remove)
            logger.info(f"Scaled down {queue_name}: -{workers_to_remove} workers ({current_workers} -> {target_workers})")
```

### Predictive Scaling
```python
class PredictiveScaler:
    """Predictive scaling based on historical patterns."""
    
    def __init__(self):
        self.historical_data = {}
        self.models = {}
    
    def collect_metrics(self):
        """Collect historical queue depth and processing metrics."""
        
        timestamp = datetime.utcnow()
        hour_of_day = timestamp.hour
        day_of_week = timestamp.weekday()
        
        queues = self.client.list_queues()
        
        for queue in queues:
            key = (queue['name'], hour_of_day, day_of_week)
            
            if key not in self.historical_data:
                self.historical_data[key] = []
            
            self.historical_data[key].append({
                'timestamp': timestamp,
                'pending_jobs': queue['pending_jobs'],
                'processing_jobs': queue['processing_jobs'],
                'active_workers': self.get_active_workers(queue['name'])
            })
    
    def predict_scaling_needs(self, queue_name, lookahead_minutes=30):
        """Predict scaling needs for the next period."""
        
        now = datetime.utcnow()
        future_time = now + timedelta(minutes=lookahead_minutes)
        
        # Use historical patterns to predict load
        predicted_load = self.predict_load(queue_name, future_time)
        
        # Calculate optimal worker count
        optimal_workers = self.calculate_optimal_workers(queue_name, predicted_load)
        
        return optimal_workers
    
    def predict_load(self, queue_name, target_time):
        """Predict queue load at target time."""
        
        hour = target_time.hour
        day_of_week = target_time.weekday()
        
        # Get historical data for this time pattern
        historical_key = (queue_name, hour, day_of_week)
        historical_loads = self.historical_data.get(historical_key, [])
        
        if not historical_loads:
            return 0
        
        # Simple average (in production, use ML models)
        avg_load = sum(d['pending_jobs'] for d in historical_loads) / len(historical_loads)
        
        return avg_load
```

## Container Orchestration

### Docker Scaling
```dockerfile
# Dockerfile for NAQ worker
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

# Default command - can be overridden
CMD ["naq", "worker", "default", "general_queue", "--concurrency", "4"]
```

```yaml
# docker-compose.yml for scaling
version: '3.8'
services:
  naq-worker-general:
    build: .
    environment:
      - NAQ_NATS_URL=nats://nats:4222
      - NAQ_JOB_SERIALIZER=json
    command: ["naq", "worker", "default", "general_queue", "--concurrency", "4"]
    deploy:
      replicas: 3
    depends_on:
      - nats

  naq-worker-cpu:
    build: .
    environment:
      - NAQ_NATS_URL=nats://nats:4222
      - NAQ_JOB_SERIALIZER=json
    command: ["naq", "worker", "default", "cpu_queue", "--concurrency", "2"]
    deploy:
      replicas: 2
    depends_on:
      - nats

  naq-worker-io:
    build: .
    environment:
      - NAQ_NATS_URL=nats://nats:4222
      - NAQ_JOB_SERIALIZER=json
    command: ["naq", "worker", "default", "io_queue", "--concurrency", "8"]
    deploy:
      replicas: 4
    depends_on:
      - nats
```

### Kubernetes Scaling
```yaml
# k8s-worker-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: naq-worker-general
spec:
  replicas: 3
  selector:
    matchLabels:
      app: naq-worker-general
  template:
    metadata:
      labels:
        app: naq-worker-general
    spec:
      containers:
      - name: worker
        image: naq-worker:latest
        command: ["naq", "worker", "default", "general_queue"]
        args: ["--concurrency", "4"]
        env:
        - name: NAQ_NATS_URL
          value: "nats://nats-service:4222"
        - name: NAQ_JOB_SERIALIZER
          value: "json"
        resources:
          requests:
            memory: "256Mi"
            cpu: "250m"
          limits:
            memory: "512Mi"
            cpu: "500m"

---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: naq-worker-general-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: naq-worker-general
  minReplicas: 2
  maxReplicas: 20
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
```

### Custom Metrics Scaling
```yaml
# Custom metrics for queue depth based scaling
apiVersion: v2
kind: HorizontalPodAutoscaler
metadata:
  name: naq-worker-queue-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: naq-worker-general
  minReplicas: 2
  maxReplicas: 50
  metrics:
  - type: External
    external:
      metric:
        name: naq_queue_depth
        selector:
          matchLabels:
            queue: general_queue
      target:
        type: AverageValue
        averageValue: "10"
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 60
      policies:
      - type: Percent
        value: 100
        periodSeconds: 60
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
      - type: Percent
        value: 50
        periodSeconds: 60
```

## Load Balancing Strategies

### Queue-Based Load Balancing
```python
class QueueLoadBalancer:
    """Balance load across different queues and workers."""
    
    def __init__(self):
        self.queue_weights = {
            'high_priority': 1.0,
            'normal_priority': 0.7,
            'low_priority': 0.3,
            'batch_processing': 0.1
        }
    
    def distribute_jobs(self, jobs):
        """Distribute jobs across queues based on priority and load."""
        
        # Get current queue loads
        queue_loads = self.get_queue_loads()
        
        # Calculate effective capacity for each queue
        queue_capacities = {}
        for queue, weight in self.queue_weights.items():
            current_load = queue_loads.get(queue, 0)
            active_workers = self.get_active_workers(queue)
            capacity = (active_workers * weight) - current_load
            queue_capacities[queue] = max(0, capacity)
        
        # Distribute jobs
        for job in jobs:
            best_queue = self.select_best_queue(job, queue_capacities)
            self.enqueue_job(job, best_queue)
            queue_capacities[best_queue] -= 1
    
    def select_best_queue(self, job, capacities):
        """Select the best queue for a job."""
        
        # Get job priority
        priority = job.get('priority', 'normal')
        
        # Prefer queues that match job priority
        preferred_queues = [
            f"{priority}_priority" for q in capacities.keys() 
            if q.startswith(priority)
        ]
        
        # Select queue with highest available capacity
        available_queues = {
            q: cap for q, cap in capacities.items() 
            if cap > 0 and (not preferred_queues or q in preferred_queues)
        }
        
        if available_queues:
            return max(available_queues, key=available_queues.get)
        
        # Fallback to any available queue
        return max(capacities, key=capacities.get)
```

### Geographic Load Balancing
```python
class GeographicLoadBalancer:
    """Balance load across geographic regions."""
    
    def __init__(self):
        self.regions = {
            'us-east-1': {'latency': 10, 'capacity': 100},
            'us-west-2': {'latency': 50, 'capacity': 80},
            'eu-west-1': {'latency': 100, 'capacity': 60}
        }
    
    def select_region(self, job):
        """Select optimal region for job processing."""
        
        # Get job location hint
        job_location = job.get('location', 'us-east-1')
        
        # Calculate scores for each region
        scores = {}
        for region, info in self.regions.items():
            # Score based on latency and available capacity
            latency_score = 1.0 / (1.0 + info['latency'] / 100.0)
            capacity_score = self.get_available_capacity(region) / info['capacity']
            
            # Weight based on geographic proximity
            proximity_weight = 2.0 if region == job_location else 1.0
            
            scores[region] = (latency_score + capacity_score) * proximity_weight
        
        return max(scores, key=scores.get)
```

## Performance Optimization

### Worker Pool Optimization
```python
class OptimizedWorkerPool:
    """Optimized worker pool with advanced features."""
    
    def __init__(self, queue_name, initial_size=4):
        self.queue_name = queue_name
        self.workers = []
        self.worker_stats = {}
        self.optimization_enabled = True
        
        # Start initial workers
        for i in range(initial_size):
            self.add_worker()
    
    def add_worker(self, worker_config=None):
        """Add optimized worker to pool."""
        
        if not worker_config:
            worker_config = self.get_optimal_worker_config()
        
        worker = NAQWorker(
            queue_name=self.queue_name,
            concurrency=worker_config['concurrency'],
            prefetch_count=worker_config['prefetch_count'],
            heartbeat_interval=worker_config['heartbeat_interval']
        )
        
        worker.start()
        self.workers.append(worker)
        self.worker_stats[worker.id] = WorkerStats()
    
    def get_optimal_worker_config(self):
        """Calculate optimal worker configuration."""
        
        # Analyze job characteristics
        job_stats = self.analyze_recent_jobs()
        
        # Optimize based on job types
        if job_stats['avg_cpu_usage'] > 0.8:
            # CPU-bound jobs - lower concurrency
            concurrency = max(1, os.cpu_count() // 2)
            prefetch_count = 1
        elif job_stats['avg_io_wait'] > 0.5:
            # I/O-bound jobs - higher concurrency
            concurrency = min(20, os.cpu_count() * 4)
            prefetch_count = 5
        else:
            # Mixed workload - balanced configuration
            concurrency = os.cpu_count()
            prefetch_count = 3
        
        return {
            'concurrency': concurrency,
            'prefetch_count': prefetch_count,
            'heartbeat_interval': 30
        }
    
    def optimize_continuously(self):
        """Continuously optimize worker performance."""
        
        while self.optimization_enabled:
            try:
                # Collect performance metrics
                self.collect_worker_metrics()
                
                # Identify underperforming workers
                slow_workers = self.identify_slow_workers()
                
                # Replace slow workers
                for worker in slow_workers:
                    self.replace_worker(worker)
                
                # Adjust configurations
                self.adjust_worker_configurations()
                
                time.sleep(60)  # Optimize every minute
                
            except Exception as e:
                logger.error(f"Optimization error: {e}")
                time.sleep(30)
```

### Memory-Aware Scaling
```python
class MemoryAwareScaler:
    """Scale workers based on memory usage patterns."""
    
    def __init__(self, max_memory_gb=8):
        self.max_memory_gb = max_memory_gb
        self.memory_threshold = 0.8  # 80% memory usage threshold
    
    def should_scale_up(self, queue_name):
        """Check if we should scale up based on memory availability."""
        
        current_memory_usage = self.get_current_memory_usage()
        available_memory = self.max_memory_gb - current_memory_usage
        
        # Estimate memory needed for new worker
        avg_worker_memory = self.get_average_worker_memory(queue_name)
        
        if available_memory > avg_worker_memory * 1.2:  # 20% buffer
            return True
        
        return False
    
    def should_scale_down(self, queue_name):
        """Check if we should scale down to free memory."""
        
        current_memory_usage = self.get_current_memory_usage()
        memory_usage_ratio = current_memory_usage / self.max_memory_gb
        
        if memory_usage_ratio > self.memory_threshold:
            # Find least productive worker to remove
            workers = self.get_workers_by_queue(queue_name)
            if len(workers) > 1:  # Keep at least one worker
                return True
        
        return False
```

## Prerequisites

1. **NATS Server** with JetStream
2. **Monitoring System** (Prometheus, Grafana)
3. **Container Orchestration** (Docker, Kubernetes)
4. **Resource Monitoring** (CPU, memory, network)
5. **Secure Configuration**: `export NAQ_JOB_SERIALIZER=json`

## Running the Examples

### 1. Start Base Services

```bash
# Start NATS
cd docker && docker-compose up -d

# Set secure serialization
export NAQ_JOB_SERIALIZER=json

# Start monitoring
naq dashboard --port 8000 &
```

### 2. Run Scaling Examples

```bash
cd examples/05-advanced/01-worker-scaling

# Auto-scaling demo
python auto_scaler.py

# Load balancing demo
python load_balancer.py

# Performance optimization
python worker_optimizer.py
```

### 3. Test Scaling Scenarios

```bash
# Generate load for testing
python load_generator.py --queue general_queue --jobs 1000

# Monitor scaling behavior
python scaling_monitor.py --interval 10
```

## Production Best Practices

### Scaling Strategy
- Start with conservative scaling thresholds
- Monitor and adjust based on real traffic patterns
- Use predictive scaling for known traffic patterns
- Implement circuit breakers for external dependencies

### Resource Management
- Set appropriate resource limits and requests
- Monitor memory leaks and restart workers periodically
- Use dedicated nodes for different worker types
- Implement proper cleanup procedures

### Monitoring and Alerting
- Track queue depths and processing times
- Monitor worker health and performance
- Alert on scaling events and failures
- Regular capacity planning reviews

### Cost Optimization
- Use spot instances for batch processing workers
- Schedule non-urgent work during off-peak hours
- Implement efficient worker shutdown procedures
- Monitor and optimize resource utilization

## Next Steps

- Explore [high availability](../02-high-availability/) for resilient scaling
- Learn about [performance optimization](../03-performance-optimization/) for efficient workers
- Check out [custom extensions](../04-custom-extensions/) for specialized scaling logic
- Implement [monitoring dashboard](../../03-production/02-monitoring-dashboard/) for scaling oversight