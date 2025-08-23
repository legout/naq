#!/usr/bin/env python3
"""
Connection Performance Benchmark

This script benchmarks the connection performance to verify there are no
regressions after the connection management migration.

It measures:
1. Connection establishment time
2. Job enqueue performance with individual connections
3. Job enqueue performance with Queue class
4. Connection cleanup time
"""

import os
import time
import asyncio
import statistics
from datetime import datetime, timezone
from typing import List, Dict, Any

# Import NATS for JetStream
import nats
from nats.js import api as js_api
from nats.js.errors import NotFoundError

# Import NAQ components
from naq.queue.sync_api import enqueue_sync
from naq.utils import setup_logging
from naq.queue import Queue
from naq.utils import run_async_from_sync
from naq.services.connection import ConnectionService, ConnectionManager
from naq.connection import nats_jetstream

# Configure secure JSON serialization
os.environ.setdefault('NAQ_JOB_SERIALIZER', 'json')

# Setup logging
setup_logging(level="INFO")


def simple_task(task_id: int, data: str = "test") -> str:
    """Simple task for benchmarking."""
    return f"Task {task_id} processed with data: {data}"


async def benchmark_connection_establishment(iterations: int = 10) -> Dict[str, Any]:
    """Benchmark connection establishment time."""
    print(f"🔌 Benchmarking connection establishment ({iterations} iterations)...")
    
    times = []
    for i in range(iterations):
        start_time = time.time()
        
        # Create a new connection service each time
        conn_service = ConnectionService()
        await conn_service.initialize()
        
        # Time to establish connection
        connection_time = time.time() - start_time
        times.append(connection_time)
        
        # Clean up
        await conn_service.cleanup()
    
    return {
        "iterations": iterations,
        "average_time": statistics.mean(times),
        "min_time": min(times),
        "max_time": max(times),
        "median_time": statistics.median(times),
        "stdev": statistics.stdev(times) if len(times) > 1 else 0
    }


async def benchmark_individual_enqueue(iterations: int = 20) -> Dict[str, Any]:
    """Benchmark job enqueue with individual connections."""
    print(f"📤 Benchmarking individual enqueue ({iterations} jobs)...")
    
    from naq.queue.async_api import enqueue
    
    start_time = time.time()
    jobs = []
    
    for i in range(iterations):
        job = await enqueue(
            simple_task,
            task_id=i,
            data="benchmark_individual",
            queue_name="benchmark",
            prefer_thread_local=True
        )
        jobs.append(job)
    
    total_time = time.time() - start_time
    avg_time_per_job = total_time / iterations
    
    return {
        "iterations": iterations,
        "total_time": total_time,
        "average_time_per_job": avg_time_per_job,
        "jobs_per_second": iterations / total_time
    }


def benchmark_queue_enqueue(iterations: int = 20) -> Dict[str, Any]:
    """Benchmark job enqueue with Queue class."""
    print(f"🚀 Benchmarking Queue enqueue ({iterations} jobs)...")
    
    async def _enqueue_jobs():
        start_time = time.time()
        jobs = []
        
        async with Queue(name="benchmark") as queue:
            for i in range(iterations):
                job = await queue.enqueue(
                    simple_task,
                    task_id=i + 1000,  # Different IDs to avoid confusion
                    data="benchmark_queue"
                )
                jobs.append(job)
        
        total_time = time.time() - start_time
        avg_time_per_job = total_time / iterations
        
        return {
            "iterations": iterations,
            "total_time": total_time,
            "average_time_per_job": avg_time_per_job,
            "jobs_per_second": iterations / total_time
        }
    
    return run_async_from_sync(_enqueue_jobs)


async def benchmark_connection_cleanup(iterations: int = 10) -> Dict[str, Any]:
    """Benchmark connection cleanup time."""
    print(f"🧹 Benchmarking connection cleanup ({iterations} iterations)...")
    
    times = []
    for i in range(iterations):
        # Create and initialize a connection service
        conn_service = ConnectionService()
        await conn_service.initialize()
        
        # Time the cleanup
        start_time = time.time()
        await conn_service.cleanup()
        cleanup_time = time.time() - start_time
        
        times.append(cleanup_time)
    
    return {
        "iterations": iterations,
        "average_time": statistics.mean(times),
        "min_time": min(times),
        "max_time": max(times),
        "median_time": statistics.median(times),
        "stdev": statistics.stdev(times) if len(times) > 1 else 0
    }


async def benchmark_connection_manager() -> Dict[str, Any]:
    """Benchmark ConnectionManager performance."""
    print("🏗️ Benchmarking ConnectionManager...")
    
    # Get the global connection manager
    conn_manager = ConnectionManager()
    
    # Benchmark getting connections
    get_times = []
    for i in range(20):
        start_time = time.time()
        nc = await conn_manager.get_connection()
        get_time = time.time() - start_time
        get_times.append(get_time)
    
    return {
        "get_connection": {
            "average_time": statistics.mean(get_times),
            "min_time": min(get_times),
            "max_time": max(get_times),
            "median_time": statistics.median(get_times),
            "stdev": statistics.stdev(get_times) if len(get_times) > 1 else 0
        }
    }


def print_results(results: Dict[str, Any]) -> None:
    """Print benchmark results in a readable format."""
    print("\n" + "=" * 60)
    print("📊 BENCHMARK RESULTS")
    print("=" * 60)
    
    # Connection Establishment
    conn_est = results["connection_establishment"]
    print("\n🔌 Connection Establishment:")
    print(f"   Iterations: {conn_est['iterations']}")
    print(f"   Average time: {conn_est['average_time']:.4f}s")
    print(f"   Min time: {conn_est['min_time']:.4f}s")
    print(f"   Max time: {conn_est['max_time']:.4f}s")
    print(f"   Median time: {conn_est['median_time']:.4f}s")
    print(f"   Std dev: {conn_est['stdev']:.4f}s")
    
    # Individual Enqueue
    individual = results["individual_enqueue"]
    print("\n📤 Individual Enqueue:")
    print(f"   Jobs: {individual['iterations']}")
    print(f"   Total time: {individual['total_time']:.4f}s")
    print(f"   Average per job: {individual['average_time_per_job']:.4f}s")
    print(f"   Jobs per second: {individual['jobs_per_second']:.2f}")
    
    # Queue Enqueue
    queue_enqueue = results["queue_enqueue"]
    print("\n🚀 Queue Enqueue:")
    print(f"   Jobs: {queue_enqueue['iterations']}")
    print(f"   Total time: {queue_enqueue['total_time']:.4f}s")
    print(f"   Average per job: {queue_enqueue['average_time_per_job']:.4f}s")
    print(f"   Jobs per second: {queue_enqueue['jobs_per_second']:.2f}")
    
    # Performance improvement
    improvement = ((individual['average_time_per_job'] - queue_enqueue['average_time_per_job']) /
                   individual['average_time_per_job']) * 100
    print(f"\n📈 Queue is {improvement:.1f}% faster than individual enqueues")
    
    # Connection Cleanup
    cleanup = results["connection_cleanup"]
    print("\n🧹 Connection Cleanup:")
    print(f"   Iterations: {cleanup['iterations']}")
    print(f"   Average time: {cleanup['average_time']:.4f}s")
    print(f"   Min time: {cleanup['min_time']:.4f}s")
    print(f"   Max time: {cleanup['max_time']:.4f}s")
    print(f"   Median time: {cleanup['median_time']:.4f}s")
    print(f"   Std dev: {cleanup['stdev']:.4f}s")
    
    # Connection Manager
    conn_mgr = results["connection_manager"]
    print("\n🏗️ Connection Manager:")
    print("   Get Connection:")
    print(f"     Average time: {conn_mgr['get_connection']['average_time']:.4f}s")
    print(f"     Min time: {conn_mgr['get_connection']['min_time']:.4f}s")
    print(f"     Max time: {conn_mgr['get_connection']['max_time']:.4f}s")
    print(f"     Median time: {conn_mgr['get_connection']['median_time']:.4f}s")
    print(f"     Std dev: {conn_mgr['get_connection']['stdev']:.4f}s")
    
    print("\n" + "=" * 60)


async def initialize_jetstream_stream():
    """Initialize the JetStream stream for benchmarking."""
    print("🔧 Initializing JetStream stream...")
    
    try:
        # Use the deprecated ensure_stream function which is known to work
        from naq.connection import ensure_stream
        
        # Initialize the naq_jobs stream
        await ensure_stream(
            stream_name="naq_jobs",
            subjects=["naq_jobs.*"]
        )
        
        # Initialize the benchmark stream
        await ensure_stream(
            stream_name="benchmark",
            subjects=["benchmark.*"]
        )
        
        print("✅ JetStream streams initialized successfully.")
                
    except Exception as e:
        print(f"❌ Failed to initialize JetStream stream: {e}")
        raise


async def main():
    """Main benchmark function."""
    print("🚀 Connection Performance Benchmark")
    print("=" * 60)
    print("This benchmark measures connection performance after the migration.")
    print("Make sure NATS is running: cd docker && docker-compose up -d")
    print("=" * 60)
    
    try:
        # Initialize JetStream stream first
        await initialize_jetstream_stream()
        
        # Run all benchmarks
        results = {}
        
        # Connection establishment benchmark
        results["connection_establishment"] = await benchmark_connection_establishment()
        
        # Enqueue benchmarks
        results["individual_enqueue"] = await benchmark_individual_enqueue()
        results["queue_enqueue"] = benchmark_queue_enqueue()
        
        # Connection cleanup benchmark
        results["connection_cleanup"] = await benchmark_connection_cleanup()
        
        # Connection manager benchmark
        results["connection_manager"] = await benchmark_connection_manager()
        
        # Print results
        print_results(results)
        
        # Save results to file for future comparison
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"benchmark_results_{timestamp}.json"
        
        import json
        with open(filename, "w") as f:
            json.dump(results, f, indent=2)
        
        print(f"\n💾 Results saved to {filename}")
        print("\n✅ Benchmark completed successfully!")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Error running benchmark: {e}")
        print("\n🔧 Troubleshooting:")
        print("   - Is NATS running? (cd docker && docker-compose up -d)")
        print("   - Is NAQ_JOB_SERIALIZER=json set?")
        return 1


if __name__ == "__main__":
    exit(asyncio.run(main()))