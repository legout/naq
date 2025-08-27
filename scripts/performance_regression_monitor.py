#!/usr/bin/env python3
"""
Performance Regression Monitor

This script monitors performance metrics to detect regressions in the NAQ codebase.
It runs benchmarks, compares results against baselines, and reports any significant
degradations in performance.

Usage:
    python scripts/performance_regression_monitor.py [--baseline FILE] [--output FILE]
"""

import argparse
import asyncio
import json
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import sys

# Import NAQ components
from naq.utils import setup_logging
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))
from benchmark_connection_performance import (
    benchmark_connection_establishment,
    benchmark_individual_enqueue,
    benchmark_queue_enqueue,
    benchmark_connection_cleanup,
    benchmark_connection_manager,
    initialize_jetstream_stream,
    print_results
)

# Configure logging
setup_logging(level="INFO")


class PerformanceRegressionMonitor:
    """Monitors performance metrics and detects regressions."""
    
    def __init__(self, baseline_file: Optional[str] = None):
        """Initialize the performance monitor.
        
        Args:
            baseline_file: Path to JSON file containing baseline performance metrics.
                          If None, will look for the most recent baseline file.
        """
        self.baseline_file = baseline_file
        self.baseline_data: Optional[Dict[str, Any]] = None
        self.current_results: Dict[str, Any] = {}
        self.regressions: List[Dict[str, Any]] = []
        
        # Performance thresholds (percentage increase considered a regression)
        self.thresholds = {
            "connection_establishment": 20.0,  # 20% increase in connection time
            "individual_enqueue": 15.0,        # 15% increase in enqueue time
            "queue_enqueue": 15.0,             # 15% increase in queue enqueue time
            "connection_cleanup": 20.0,        # 20% increase in cleanup time
            "connection_manager": 25.0,        # 25% increase in manager access time
        }
        
        # Load baseline data if provided
        if baseline_file:
            self.load_baseline(baseline_file)
        else:
            self.find_latest_baseline()
    
    def find_latest_baseline(self) -> None:
        """Find the most recent baseline file."""
        baseline_dir = Path("performance_baselines")
        if not baseline_dir.exists():
            print("⚠️  No performance_baselines directory found")
            return
        
        baseline_files = list(baseline_dir.glob("baseline_*.json"))
        if not baseline_files:
            print("⚠️  No baseline files found")
            return
        
        # Sort by modification time and get the most recent
        latest_baseline = max(baseline_files, key=lambda f: f.stat().st_mtime)
        print(f"📊 Using latest baseline: {latest_baseline}")
        self.load_baseline(str(latest_baseline))
    
    def load_baseline(self, baseline_file: str) -> None:
        """Load baseline performance data from file."""
        try:
            with open(baseline_file, 'r') as f:
                self.baseline_data = json.load(f)
            print(f"✅ Loaded baseline from {baseline_file}")
        except FileNotFoundError:
            print(f"⚠️  Baseline file not found: {baseline_file}")
        except json.JSONDecodeError as e:
            print(f"❌ Error parsing baseline file: {e}")
    
    async def run_benchmarks(self) -> Dict[str, Any]:
        """Run all performance benchmarks."""
        print("🚀 Running performance benchmarks...")
        
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
            
            # Add metadata
            results["metadata"] = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "python_version": sys.version,
                "platform": sys.platform
            }
            
            self.current_results = results
            print("✅ Benchmarks completed successfully")
            return results
            
        except Exception as e:
            print(f"❌ Error running benchmarks: {e}")
            raise
    
    def compare_with_baseline(self) -> List[Dict[str, Any]]:
        """Compare current results with baseline and detect regressions."""
        if not self.baseline_data:
            print("⚠️  No baseline data available for comparison")
            return []
        
        if not self.current_results:
            print("⚠️  No current results available for comparison")
            return []
        
        regressions = []
        
        # Compare each benchmark category
        for category in self.thresholds.keys():
            if category not in self.baseline_data or category not in self.current_results:
                continue
            
            baseline = self.baseline_data[category]
            current = self.current_results[category]
            threshold = self.thresholds[category]
            
            # Compare relevant metrics
            if category == "connection_establishment":
                regression = self._compare_time_metrics(
                    baseline, current, "average_time", threshold, category
                )
            elif category in ["individual_enqueue", "queue_enqueue"]:
                regression = self._compare_time_metrics(
                    baseline, current, "average_time_per_job", threshold, category
                )
            elif category == "connection_cleanup":
                regression = self._compare_time_metrics(
                    baseline, current, "average_time", threshold, category
                )
            elif category == "connection_manager":
                regression = self._compare_time_metrics(
                    baseline["get_connection"], current["get_connection"], 
                    "average_time", threshold, f"{category}.get_connection"
                )
            
            if regression:
                regressions.append(regression)
        
        self.regressions = regressions
        return regressions
    
    def _compare_time_metrics(
        self, 
        baseline: Dict[str, Any], 
        current: Dict[str, Any], 
        metric: str, 
        threshold: float,
        category: str
    ) -> Optional[Dict[str, Any]]:
        """Compare time metrics between baseline and current results."""
        if metric not in baseline or metric not in current:
            return None
        
        baseline_value = baseline[metric]
        current_value = current[metric]
        
        if baseline_value == 0:
            return None  # Avoid division by zero
        
        percentage_increase = ((current_value - baseline_value) / baseline_value) * 100
        
        if percentage_increase > threshold:
            return {
                "category": category,
                "metric": metric,
                "baseline_value": baseline_value,
                "current_value": current_value,
                "percentage_increase": percentage_increase,
                "threshold": threshold,
                "severity": "high" if percentage_increase > threshold * 2 else "medium"
            }
        
        return None
    
    def save_baseline(self, output_file: str) -> None:
        """Save current results as a new baseline."""
        if not self.current_results:
            print("⚠️  No results to save as baseline")
            return
        
        # Create directory if it doesn't exist
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w') as f:
            json.dump(self.current_results, f, indent=2)
        
        print(f"💾 Saved baseline to {output_file}")
    
    def generate_report(self) -> str:
        """Generate a performance regression report."""
        report = []
        report.append("📊 Performance Regression Report")
        report.append("=" * 50)
        
        if not self.current_results:
            report.append("❌ No benchmark results available")
            return "\n".join(report)
        
        # Add timestamp
        timestamp = self.current_results.get("metadata", {}).get("timestamp", "Unknown")
        report.append(f"📅 Generated: {timestamp}")
        report.append("")
        
        # Summary
        if self.regressions:
            report.append(f"❌ {len(self.regressions)} performance regression(s) detected")
        else:
            report.append("✅ No performance regressions detected")
        report.append("")
        
        # Detailed results
        report.append("📈 Benchmark Results:")
        report.append("-" * 30)
        
        for category, data in self.current_results.items():
            if category == "metadata":
                continue
            
            report.append(f"\n{category.replace('_', ' ').title()}:")
            
            if isinstance(data, dict):
                for key, value in data.items():
                    if isinstance(value, float):
                        report.append(f"  {key}: {value:.4f}")
                    else:
                        report.append(f"  {key}: {value}")
            else:
                report.append(f"  {data}")
        
        # Regressions details
        if self.regressions:
            report.append("\n🚨 Performance Regressions:")
            report.append("-" * 30)
            
            for regression in self.regressions:
                severity_icon = "🔴" if regression["severity"] == "high" else "🟡"
                report.append(f"\n{severity_icon} {regression['category'].replace('_', ' ').title()}")
                report.append(f"  Metric: {regression['metric']}")
                report.append(f"  Baseline: {regression['baseline_value']:.4f}")
                report.append(f"  Current: {regression['current_value']:.4f}")
                report.append(f"  Increase: {regression['percentage_increase']:.1f}%")
                report.append(f"  Threshold: {regression['threshold']:.1f}%")
        
        # Recommendations
        report.append("\n💡 Recommendations:")
        report.append("-" * 30)
        
        if self.regressions:
            report.append("• Investigate the detected performance regressions")
            report.append("• Consider rolling back changes if regressions are severe")
            report.append("• Profile the affected code paths to identify bottlenecks")
        else:
            report.append("• Performance is within acceptable limits")
            report.append("• Continue monitoring with regular benchmark runs")
        
        report.append("")
        report.append("=" * 50)
        
        return "\n".join(report)
    
    def save_report(self, output_file: str) -> None:
        """Save the performance report to a file."""
        report = self.generate_report()
        
        with open(output_file, 'w') as f:
            f.write(report)
        
        print(f"📄 Report saved to {output_file}")


async def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Monitor performance regressions in NAQ codebase"
    )
    parser.add_argument(
        "--baseline", 
        help="Path to baseline JSON file (default: find latest)"
    )
    parser.add_argument(
        "--output", 
        default="performance_report.txt",
        help="Output file for the report (default: performance_report.txt)"
    )
    parser.add_argument(
        "--save-baseline",
        help="Save current results as a new baseline to this file"
    )
    parser.add_argument(
        "--fail-on-regression",
        action="store_true",
        help="Exit with non-zero code if regressions are detected"
    )
    
    args = parser.parse_args()
    
    # Initialize monitor
    monitor = PerformanceRegressionMonitor(args.baseline)
    
    try:
        # Run benchmarks
        await monitor.run_benchmarks()
        
        # Compare with baseline
        regressions = monitor.compare_with_baseline()
        
        # Generate and save report
        report = monitor.generate_report()
        print("\n" + report)
        monitor.save_report(args.output)
        
        # Save baseline if requested
        if args.save_baseline:
            monitor.save_baseline(args.save_baseline)
        
        # Exit with appropriate code
        if args.fail_on_regression and regressions:
            print(f"\n❌ Exiting with error code due to {len(regressions)} regression(s)")
            sys.exit(1)
        else:
            print("\n✅ Performance monitoring completed successfully")
            sys.exit(0)
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())