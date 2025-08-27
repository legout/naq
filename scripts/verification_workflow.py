#!/usr/bin/env python3
"""
Verification Workflow for NAQ

This script runs a comprehensive verification workflow including:
1. Code quality checks (linting, formatting)
2. Type checking
3. Unit tests with coverage measurement
4. Performance regression monitoring
5. Documentation validation

Usage:
    python scripts/verification_workflow.py [--baseline FILE] [--output DIR]
"""

import argparse
import asyncio
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import shutil

# Import NAQ components
from naq.utils import setup_logging
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))
from performance_regression_monitor import PerformanceRegressionMonitor

# Configure logging
setup_logging(level="INFO")


class VerificationWorkflow:
    """Comprehensive verification workflow for NAQ."""
    
    def __init__(self, baseline_file: Optional[str] = None, output_dir: Optional[str] = None):
        """Initialize the verification workflow.
        
        Args:
            baseline_file: Path to performance baseline file.
            output_dir: Directory to save verification reports.
        """
        self.baseline_file = baseline_file
        self.output_dir = Path(output_dir) if output_dir else Path("verification_reports")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize components
        self.performance_monitor = PerformanceRegressionMonitor(baseline_file)
        
        # Verification results
        self.results: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "python_version": sys.version,
            "platform": sys.platform,
            "checks": {},
            "summary": {
                "total_checks": 0,
                "passed_checks": 0,
                "failed_checks": 0,
                "warnings": 0
            }
        }
        
        # Success criteria
        self.success_criteria = {
            "min_coverage": 80.0,  # Minimum test coverage percentage
            "max_lint_errors": 0,  # Maximum allowed linting errors
            "max_type_errors": 0,  # Maximum allowed type errors
            "max_performance_regressions": 0  # Maximum allowed performance regressions
        }
    
    def run_command(self, command: List[str], cwd: Optional[str] = None) -> Tuple[int, str, str]:
        """Run a command and return exit code, stdout, and stderr."""
        try:
            result = subprocess.run(
                command,
                cwd=cwd,
                capture_output=True,
                text=True,
                check=False
            )
            return result.returncode, result.stdout, result.stderr
        except Exception as e:
            return 1, "", str(e)
    
    def check_code_quality(self) -> Dict[str, Any]:
        """Run code quality checks (linting and formatting)."""
        print("🔍 Running code quality checks...")
        
        check_results = {
            "linting": {"status": "pending", "details": ""},
            "formatting": {"status": "pending", "details": ""}
        }
        
        # Run linting
        print("  Running ruff check...")
        exit_code, stdout, stderr = self.run_command(["uv", "run", "ruff", "check", "src/"])
        check_results["linting"]["status"] = "passed" if exit_code == 0 else "failed"
        check_results["linting"]["details"] = stdout + stderr
        
        # Run formatting check
        print("  Running ruff format check...")
        exit_code, stdout, stderr = self.run_command(["uv", "run", "ruff", "format", "src/", "--check"])
        check_results["formatting"]["status"] = "passed" if exit_code == 0 else "failed"
        check_results["formatting"]["details"] = stdout + stderr
        
        return check_results
    
    def check_type_safety(self) -> Dict[str, Any]:
        """Run type checking."""
        print("🔍 Running type checking...")
        
        check_results = {
            "type_checking": {"status": "pending", "details": ""}
        }
        
        # Run type checking
        print("  Running ruff type check...")
        exit_code, stdout, stderr = self.run_command(["uv", "run", "ruff", "check", "src/", "--select", "E,F,W"])
        check_results["type_checking"]["status"] = "passed" if exit_code == 0 else "failed"
        check_results["type_checking"]["details"] = stdout + stderr
        
        return check_results
    
    def check_tests(self) -> Dict[str, Any]:
        """Run unit tests with coverage measurement."""
        print("🔍 Running unit tests with coverage...")
        
        check_results = {
            "unit_tests": {"status": "pending", "details": "", "coverage": 0.0}
        }
        
        # Run tests with coverage
        print("  Running pytest with coverage...")
        exit_code, stdout, stderr = self.run_command([
            "uv", "run", "pytest", "tests/", 
            "--cov=src/naq", 
            "--cov-report=term-missing",
            "--cov-report=json:coverage_report.json"
        ])
        
        check_results["unit_tests"]["status"] = "passed" if exit_code == 0 else "failed"
        check_results["unit_tests"]["details"] = stdout + stderr
        
        # Parse coverage report
        try:
            with open("coverage_report.json", "r") as f:
                coverage_data = json.load(f)
                check_results["unit_tests"]["coverage"] = coverage_data.get("totals", {}).get("percent_covered", 0.0)
        except (FileNotFoundError, json.JSONDecodeError, KeyError):
            check_results["unit_tests"]["coverage"] = 0.0
        
        return check_results
    
    async def check_performance(self) -> Dict[str, Any]:
        """Run performance regression monitoring."""
        print("🔍 Running performance regression monitoring...")
        
        check_results = {
            "performance": {"status": "pending", "details": "", "regressions": []}
        }
        
        try:
            # Run benchmarks
            await self.performance_monitor.run_benchmarks()
            
            # Compare with baseline
            regressions = self.performance_monitor.compare_with_baseline()
            
            check_results["performance"]["status"] = "passed" if not regressions else "failed"
            check_results["performance"]["regressions"] = regressions
            check_results["performance"]["details"] = self.performance_monitor.generate_report()
            
        except Exception as e:
            check_results["performance"]["status"] = "failed"
            check_results["performance"]["details"] = str(e)
        
        return check_results
    
    def check_documentation(self) -> Dict[str, Any]:
        """Check documentation build."""
        print("🔍 Checking documentation...")
        
        check_results = {
            "documentation": {"status": "pending", "details": ""}
        }
        
        # Check if documentation can be built (if quarto is available)
        exit_code, stdout, stderr = self.run_command(["which", "quarto"])
        if exit_code == 0:
            print("  Running quarto render...")
            exit_code, stdout, stderr = self.run_command(["quarto", "render"], cwd="docs")
            check_results["documentation"]["status"] = "passed" if exit_code == 0 else "failed"
            check_results["documentation"]["details"] = stdout + stderr
        else:
            print("  Quarto not found, skipping documentation check")
            check_results["documentation"]["status"] = "skipped"
            check_results["documentation"]["details"] = "Quarto not installed"
        
        return check_results
    
    def evaluate_results(self) -> Dict[str, Any]:
        """Evaluate verification results against success criteria."""
        print("📊 Evaluating verification results...")
        
        evaluation = {
            "overall_status": "passed",
            "criteria_met": [],
            "criteria_failed": [],
            "warnings": []
        }
        
        # Check code quality
        linting_passed = self.results["checks"]["code_quality"]["linting"]["status"] == "passed"
        formatting_passed = self.results["checks"]["code_quality"]["formatting"]["status"] == "passed"
        
        if linting_passed and formatting_passed:
            evaluation["criteria_met"].append("Code quality checks passed")
        else:
            evaluation["criteria_failed"].append("Code quality checks failed")
        
        # Check type safety
        type_checking_passed = self.results["checks"]["type_safety"]["type_checking"]["status"] == "passed"
        if type_checking_passed:
            evaluation["criteria_met"].append("Type safety checks passed")
        else:
            evaluation["criteria_failed"].append("Type safety checks failed")
        
        # Check test coverage
        coverage = self.results["checks"]["tests"]["unit_tests"]["coverage"]
        if coverage >= self.success_criteria["min_coverage"]:
            evaluation["criteria_met"].append(f"Test coverage {coverage:.1f}% meets minimum requirement")
        else:
            evaluation["criteria_failed"].append(f"Test coverage {coverage:.1f}% below minimum requirement {self.success_criteria['min_coverage']}%")
        
        # Check performance
        performance_regressions = self.results["checks"]["performance"]["performance"]["regressions"]
        if len(performance_regressions) <= self.success_criteria["max_performance_regressions"]:
            evaluation["criteria_met"].append(f"Performance regressions ({len(performance_regressions)}) within acceptable limit")
        else:
            evaluation["criteria_failed"].append(f"Too many performance regressions ({len(performance_regressions)})")
        
        # Check overall status
        if evaluation["criteria_failed"]:
            evaluation["overall_status"] = "failed"
        
        return evaluation
    
    def generate_report(self) -> str:
        """Generate a comprehensive verification report."""
        report = []
        report.append("🔍 NAQ Verification Report")
        report.append("=" * 50)
        
        # Add timestamp and metadata
        report.append(f"📅 Generated: {self.results['timestamp']}")
        report.append(f"🐍 Python: {self.results['python_version'].split()[0]}")
        report.append(f"💻 Platform: {self.results['platform']}")
        report.append("")
        
        # Summary
        evaluation = self.results.get("evaluation", {})
        report.append(f"📋 Overall Status: {evaluation['overall_status'].upper()}")
        report.append(f"✅ Criteria Met: {len(evaluation['criteria_met'])}")
        report.append(f"❌ Criteria Failed: {len(evaluation['criteria_failed'])}")
        report.append(f"⚠️  Warnings: {len(evaluation['warnings'])}")
        report.append("")
        
        # Detailed results
        report.append("📊 Detailed Results:")
        report.append("-" * 30)
        
        # Code quality
        code_quality = self.results["checks"]["code_quality"]
        report.append("\n🔧 Code Quality:")
        report.append(f"  Linting: {code_quality['linting']['status'].upper()}")
        report.append(f"  Formatting: {code_quality['formatting']['status'].upper()}")
        
        # Type safety
        type_safety = self.results["checks"]["type_safety"]
        report.append("\n🔒 Type Safety:")
        report.append(f"  Type Checking: {type_safety['type_checking']['status'].upper()}")
        
        # Tests
        tests = self.results["checks"]["tests"]
        coverage = tests["unit_tests"]["coverage"]
        report.append("\n🧪 Unit Tests:")
        report.append(f"  Status: {tests['unit_tests']['status'].upper()}")
        report.append(f"  Coverage: {coverage:.1f}%")
        
        # Performance
        performance = self.results["checks"]["performance"]
        regressions = performance["performance"]["regressions"]
        report.append("\n📈 Performance:")
        report.append(f"  Status: {performance['performance']['status'].upper()}")
        report.append(f"  Regressions: {len(regressions)}")
        
        if regressions:
            report.append("  Regression Details:")
            for regression in regressions:
                severity_icon = "🔴" if regression["severity"] == "high" else "🟡"
                report.append(f"    {severity_icon} {regression['category']}: +{regression['percentage_increase']:.1f}%")
        
        # Documentation
        docs = self.results["checks"]["documentation"]
        report.append("\n📚 Documentation:")
        report.append(f"  Status: {docs['documentation']['status'].upper()}")
        
        # Criteria evaluation
        report.append("\n🎯 Success Criteria Evaluation:")
        report.append("-" * 30)
        
        for criterion in evaluation["criteria_met"]:
            report.append(f"  ✅ {criterion}")
        
        for criterion in evaluation["criteria_failed"]:
            report.append(f"  ❌ {criterion}")
        
        for warning in evaluation["warnings"]:
            report.append(f"  ⚠️  {warning}")
        
        # Recommendations
        report.append("\n💡 Recommendations:")
        report.append("-" * 30)
        
        if evaluation["overall_status"] == "failed":
            report.append("• Address failed criteria before merging")
            report.append("• Review error messages for specific issues")
            report.append("• Consider rolling back problematic changes")
        else:
            report.append("• All verification checks passed")
            report.append("• Code is ready for deployment")
            report.append("• Continue monitoring in production")
        
        report.append("")
        report.append("=" * 50)
        
        return "\n".join(report)
    
    def save_report(self) -> str:
        """Save the verification report to a file."""
        report = self.generate_report()
        
        # Generate filename with timestamp
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"verification_report_{timestamp}.txt"
        filepath = self.output_dir / filename
        
        with open(filepath, 'w') as f:
            f.write(report)
        
        # Also save raw results as JSON
        json_filename = f"verification_results_{timestamp}.json"
        json_filepath = self.output_dir / json_filename
        
        with open(json_filepath, 'w') as f:
            json.dump(self.results, f, indent=2)
        
        print(f"📄 Verification report saved to {filepath}")
        print(f"📄 Raw results saved to {json_filepath}")
        
        return str(filepath)
    
    async def run_verification(self) -> Dict[str, Any]:
        """Run the complete verification workflow."""
        print("🚀 Starting NAQ verification workflow...")
        print("=" * 50)
        
        # Run all checks
        self.results["checks"]["code_quality"] = self.check_code_quality()
        self.results["checks"]["type_safety"] = self.check_type_safety()
        self.results["checks"]["tests"] = self.check_tests()
        self.results["checks"]["performance"] = await self.check_performance()
        self.results["checks"]["documentation"] = self.check_documentation()
        
        # Evaluate results
        self.results["evaluation"] = self.evaluate_results()
        
        # Update summary
        self.results["summary"]["total_checks"] = len(self.results["checks"])
        self.results["summary"]["passed_checks"] = sum(
            1 for check in self.results["checks"].values()
            if any(result.get("status") == "passed" for result in check.values())
        )
        self.results["summary"]["failed_checks"] = sum(
            1 for check in self.results["checks"].values()
            if any(result.get("status") == "failed" for result in check.values())
        )
        self.results["summary"]["warnings"] = len(self.results["evaluation"]["warnings"])
        
        # Generate and save report
        report_path = self.save_report()
        
        # Print summary
        print("\n" + "=" * 50)
        print("📊 Verification Summary:")
        print(f"  Overall Status: {self.results['evaluation']['overall_status'].upper()}")
        print(f"  Passed Checks: {self.results['summary']['passed_checks']}")
        print(f"  Failed Checks: {self.results['summary']['failed_checks']}")
        print(f"  Warnings: {self.results['summary']['warnings']}")
        print(f"  Report: {report_path}")
        print("=" * 50)
        
        return self.results


async def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Run comprehensive verification workflow for NAQ"
    )
    parser.add_argument(
        "--baseline", 
        help="Path to performance baseline JSON file"
    )
    parser.add_argument(
        "--output", 
        default="verification_reports",
        help="Output directory for verification reports"
    )
    parser.add_argument(
        "--fail-on-error",
        action="store_true",
        help="Exit with non-zero code if verification fails"
    )
    
    args = parser.parse_args()
    
    # Initialize workflow
    workflow = VerificationWorkflow(args.baseline, args.output)
    
    try:
        # Run verification
        results = await workflow.run_verification()
        
        # Exit with appropriate code
        if args.fail_on_error and results["evaluation"]["overall_status"] == "failed":
            print("\n❌ Verification failed - exiting with error code")
            sys.exit(1)
        else:
            print("\n✅ Verification completed successfully")
            sys.exit(0)
            
    except Exception as e:
        print(f"\n❌ Error during verification: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())