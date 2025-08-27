#!/usr/bin/env python3
"""
Benchmark Connection Performance

This script benchmarks various connection-related operations in the NAQ codebase.
It measures performance metrics for connection establishment, enqueue operations,
cleanup, and connection manager access.

Usage:
    python scripts/benchmark_connection_performance.py
"""

import asyncio
import statistics
import time
from datetime import datetime, timezone
from typing import Dict, List, Any, Tuple
import sys

# Import NAQ components
from naq.utils import setup_logging
from naq.connection.manager import ConnectionManager
from naq.models.jobs import Job
from naq.models import JOB_STATUS

# Configure logging
setup_logging(level="INFO")


async def initialize_jetstream_stream() -> None:
    """Initialize JetStream stream for benchmarking."""
    # This would normally initialize a JetStream stream
    # For benchmarking purposes, we'll just simulate it
    await asyncio.sleep(0.01)  # Simulate initialization time
    print("✅ JetStream stream initialized")


async def benchmark_connection_establishment(iterations: int = 100) -> Dict[str, Any]:
    """Benchmark connection establishment performance."""
    print(f"🔌 Benchmarking connection establishment ({iterations} iterations)...")
    
    times = []
    
    for _ in range(iterations):
        start_time = time.time()
        
        # Create connection manager
        manager = ConnectionManager()
        
        # Establish connection
        await manager.get_connection()
        
        end_time = time.time()
        times.append((end_time - start_time) * 1000)  # Convert to milliseconds
    
    return {
        "iterations": iterations,
        "total_time": sum(times),
        "average_time": statistics.mean(times),
        "min_time": min(times),
        "max_time": max(times),
        "median_time": statistics.median(times),
        "stdev_time": statistics.stdev(times) if len(times) > 1 else 0
    }


async def benchmark_individual_enqueue(iterations: int = 100) -> Dict[str, Any]:
    """Benchmark individual job enqueue performance."""
    print(f"📤 Benchmarking individual enqueue ({iterations} iterations)...")
    
    times = []
    
    # Create connection manager
    manager = ConnectionManager()
    connection = await manager.get_connection()
    
    for i in range(iterations):
        # Create a test job
        job = Job(
            id=f"benchmark_job_{i}",
            payload={"test": "data", "iteration": i},
            queue="benchmark_queue"
        )
        
        start_time = time.time()
        
        # Enqueue the job
        await connection.publish(
            subject=f"NAQ.{job.queue}",
            payload=job.model_dump_json()
        )
        
        end_time = time.time()
        times.append((end_time - start_time) * 1000)  # Convert to milliseconds
    
    return {
        "iterations": iterations,
        "total_time": sum(times),
        "average_time_per_job": statistics.mean(times),
        "min_time": min(times),
        "max_time": max(times),
        "median_time": statistics.median(times),
        "stdev_time": statistics.stdev(times) if len(times) > 1 else 0
    }


def benchmark_queue_enqueue(iterations: int = 100) -> Dict[str, Any]:
    """Benchmark batch queue enqueue performance."""
    print(f"📚 Benchmarking queue enqueue ({iterations} iterations)...")
    
    times = []
    
    # Create connection manager
    manager = ConnectionManager()
    
    async def run_benchmark():
        connection = await manager.get_connection()
        
        # Create test jobs
        jobs = [
            Job(
                id=f"batch_job_{i}",
                payload={"test": "batch", "iteration": i},
                queue="benchmark_queue"
            )
            for i in range(iterations)
        ]
        
        start_time = time.time()
        
        # Enqueue all jobs
        for job in jobs:
            await connection.publish(
                subject=f"NAQ.{job.queue}",
                payload=job.model_dump_json()
            )
        
        end_time = time.time()
        times.append((end_time - start_time) * 1000)  # Convert to milliseconds
    
    # Run the benchmark
    asyncio.run(run_benchmark())
    
    return {
        "iterations": iterations,
        "total_time": sum(times),
        "average_time_per_job": statistics.mean(times) / iterations,
        "min_time": min(times),
        "max_time": max(times),
        "median_time": statistics.median(times),
        "stdev_time": statistics.stdev(times) if len(times) > 1 else 0
    }


async def benchmark_connection_cleanup(iterations: int = 50) -> Dict[str, Any]:
    """Benchmark connection cleanup performance."""
    print(f"🧹 Benchmarking connection cleanup ({iterations} iterations)...")
    
    times = []
    
    for _ in range(iterations):
        # Create connection manager
        manager = ConnectionManager()
        
        # Establish connection
        await manager.get_connection()
        
        start_time = time.time()
        
        # Cleanup connections
        await manager.cleanup()
        
        end_time = time.time()
        times.append((end_time - start_time) * 1000)  # Convert to milliseconds
    
    return {
        "iterations": iterations,
        "total_time": sum(times),
        "average_time": statistics.mean(times),
        "min_time": min(times),
        "max_time": max(times),
        "median_time": statistics.median(times),
        "stdev_time": statistics.stdev(times) if len(times) > 1 else 0
    }


async def benchmark_connection_manager(iterations: int = 1000) -> Dict[str, Any]:
    """Benchmark connection manager access performance."""
    print(f"🔧 Benchmarking connection manager access ({iterations} iterations)...")
    
    times = []
    
    # Create connection manager
    manager = ConnectionManager()
    
    # Establish initial connection
    await manager.get_connection()
    
    for _ in range(iterations):
        start_time = time.time()
        
        # Get connection (should be cached)
        await manager.get_connection()
        
        end_time = time.time()
        times.append((end_time - start_time) * 1000)  # Convert to milliseconds
    
    return {
        "get_connection": {
            "iterations": iterations,
            "total_time": sum(times),
            "average_time": statistics.mean(times),
            "min_time": min(times),
            "max_time": max(times),
            "median_time": statistics.median(times),
            "stdev_time": statistics.stdev(times) if len(times) > 1 else 0
        }
    }


def print_results(results: Dict[str, Any]) -> None:
    """Print benchmark results in a formatted way."""
    print("\n📊 Benchmark Results:")
    print("=" * 50)
    
    for category, data in results.items():
        if category == "metadata":
            continue
            
        print(f"\n{category.replace('_', ' ').title()}:")
        print("-" * 30)
        
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, float):
                    print(f"  {key}: {value:.4f}")
                else:
                    print(f"  {key}: {value}")
        else:
            print(f"  {data}")


async def main():
    """Main function."""
    print("🚀 Starting NAQ Connection Performance Benchmarks")
    print("=" * 50)
    
    # Initialize JetStream stream
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
    
    # Add metadata
    results["metadata"] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "python_version": sys.version,
        "platform": sys.platform
    }
    
    # Print results
    print_results(results)
    
    # Save results to file
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"benchmark_results_{timestamp}.json"
    
    import json
    with open(filename, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n💾 Results saved to {filename}")


if __name__ == "__main__":
    asyncio.run(main())