#!/usr/bin/env python3
"""
Script to get test failures using pytest's JSON output.
This provides a more reliable way to categorize test failures.
"""

import json
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Set
from collections import defaultdict


def run_pytest_json() -> Dict:
    """Run pytest with JSON output to get detailed test results."""
    try:
        result = subprocess.run(
            ["uv", "run", "pytest", "tests/", "--json-report", "--json-report-file=test_results.json"],
            capture_output=True,
            text=True,
            cwd=Path.cwd()
        )
        return json.loads(Path("test_results.json").read_text())
    except Exception as e:
        print(f"Error running pytest with JSON: {e}")
        return {}


def categorize_by_priority(test_results: Dict) -> Dict[str, List[Dict]]:
    """Categorize failures by priority based on test results."""
    categories = {
        "critical": [],
        "high": [],
        "medium": [],
        "low": []
    }
    
    if "tests" not in test_results:
        return categories
    
    for test in test_results["tests"]:
        if test.get("outcome") in ["failed", "error"]:
            test_name = test["nodeid"]
            error_message = test.get("call", {}).get("crash", {}).get("message", "")
            
            if not error_message and "call" in test:
                error_message = test["call"].get("longrepr", "Unknown error")
            
            # Categorize based on error patterns
            if any(pattern in error_message for pattern in [
                "ServiceManager is required",
                "Service.*not registered",
                "Connection.*required",
                "AttributeError.*does not have the attribute",
                "ModuleNotFoundError",
                "ImportError"
            ]):
                categories["critical"].append({
                    "name": test_name,
                    "error": error_message,
                    "duration": test.get("duration", 0)
                })
            elif any(pattern in error_message for pattern in [
                "ConfigurationError",
                "ValidationError",
                "TypeConversionError",
                "NaqException",
                "AssertionError.*Expected.*Called"
            ]):
                categories["high"].append({
                    "name": test_name,
                    "error": error_message,
                    "duration": test.get("duration", 0)
                })
            elif any(pattern in error_message for pattern in [
                "AssertionError",
                "ValueError",
                "TypeError",
                "Failed: DID NOT RAISE"
            ]):
                categories["medium"].append({
                    "name": test_name,
                    "error": error_message,
                    "duration": test.get("duration", 0)
                })
            else:
                categories["low"].append({
                    "name": test_name,
                    "error": error_message,
                    "duration": test.get("duration", 0)
                })
    
    return categories


def categorize_by_module(test_results: Dict) -> Dict[str, List[Dict]]:
    """Categorize failures by test module."""
    module_categories = defaultdict(list)
    
    if "tests" not in test_results:
        return dict(module_categories)
    
    for test in test_results["tests"]:
        if test.get("outcome") in ["failed", "error"]:
            test_name = test["nodeid"]
            module = test_name.split("::")[0]
            error_message = test.get("call", {}).get("crash", {}).get("message", "")
            
            if not error_message and "call" in test:
                error_message = test["call"].get("longrepr", "Unknown error")
            
            module_categories[module].append({
                "name": test_name,
                "error": error_message,
                "duration": test.get("duration", 0)
            })
    
    return dict(module_categories)


def generate_report(priority_categories: Dict[str, List[Dict]], 
                   module_categories: Dict[str, List[Dict]]) -> str:
    """Generate a comprehensive report of test failures."""
    report = []
    
    # Summary
    total_failures = sum(len(failures) for failures in priority_categories.values())
    report.append(f"# Test Failure Analysis Report")
    report.append(f"\n## Summary")
    report.append(f"- Total Failures: {total_failures}")
    report.append(f"- Critical: {len(priority_categories['critical'])}")
    report.append(f"- High Priority: {len(priority_categories['high'])}")
    report.append(f"- Medium Priority: {len(priority_categories['medium'])}")
    report.append(f"- Low Priority: {len(priority_categories['low'])}")
    
    # Priority-based breakdown
    report.append(f"\n## Priority-Based Breakdown")
    
    for priority, failures in priority_categories.items():
        report.append(f"\n### {priority.title()} Priority ({len(failures)} failures)")
        if failures:
            report.append("| Test Name | Error | Duration (s) |")
            report.append("|-----------|-------|-------------|")
            for failure in failures[:10]:  # Show first 10
                test_name = failure["name"]
                error = failure["error"]
                duration = failure["duration"]
                # Truncate long error messages
                short_error = error[:100] + "..." if len(error) > 100 else error
                # Escape pipes in markdown
                short_error = short_error.replace("|", "\\|")
                report.append(f"| {test_name} | {short_error} | {duration:.3f} |")
            if len(failures) > 10:
                report.append(f"| ... and {len(failures) - 10} more | | |")
        else:
            report.append("No failures in this category.")
    
    # Module-based breakdown
    report.append(f"\n## Module-Based Breakdown")
    
    sorted_modules = sorted(module_categories.items(), key=lambda x: len(x[1]), reverse=True)
    
    for module, failures in sorted_modules:
        report.append(f"\n### {module} ({len(failures)} failures)")
        if failures:
            report.append("| Test Name | Error | Duration (s) |")
            report.append("|-----------|-------|-------------|")
            for failure in failures[:5]:  # Show first 5 per module
                test_name = failure["name"]
                error = failure["error"]
                duration = failure["duration"]
                # Truncate long error messages
                short_error = error[:100] + "..." if len(error) > 100 else error
                # Escape pipes in markdown
                short_error = short_error.replace("|", "\\|")
                report.append(f"| {test_name} | {short_error} | {duration:.3f} |")
            if len(failures) > 5:
                report.append(f"| ... and {len(failures) - 5} more | | |")
    
    # Recommendations
    report.append(f"\n## Recommendations")
    
    if priority_categories["critical"]:
        report.append("\n### Immediate Actions Required")
        report.append("Address critical failures first as they likely block core functionality:")
        for failure in priority_categories["critical"][:5]:  # Show first 5
            report.append(f"- {failure['name']}")
        if len(priority_categories["critical"]) > 5:
            report.append(f"- ... and {len(priority_categories['critical']) - 5} more")
    
    report.append("\n### General Recommendations")
    report.append("1. Focus on service-related failures (ServiceManager, ConnectionService)")
    report.append("2. Address configuration and validation errors")
    report.append("3. Fix import and attribute errors")
    report.append("4. Update deprecated test patterns")
    report.append("5. Consider reducing test coverage threshold temporarily")
    
    # Top failing modules
    if module_categories:
        report.append("\n### Top Failing Modules")
        top_modules = sorted(module_categories.items(), key=lambda x: len(x[1]), reverse=True)[:5]
        for module, failures in top_modules:
            report.append(f"- {module}: {len(failures)} failures")
    
    return "\n".join(report)


def main():
    """Main function to run the failure analysis."""
    print("Running test failure analysis with JSON output...")
    
    # Install pytest-json-report if not available
    try:
        subprocess.run(
            ["uv", "run", "pip", "install", "pytest-json-report"],
            capture_output=True,
            check=True
        )
    except subprocess.CalledProcessError:
        print("Failed to install pytest-json-report")
        sys.exit(1)
    
    # Get pytest JSON output
    test_results = run_pytest_json()
    if not test_results:
        print("Failed to get pytest JSON output")
        sys.exit(1)
    
    # Categorize failures
    priority_categories = categorize_by_priority(test_results)
    module_categories = categorize_by_module(test_results)
    
    # Generate report
    report = generate_report(priority_categories, module_categories)
    
    # Save report
    report_path = Path("test_failure_analysis.md")
    with open(report_path, "w") as f:
        f.write(report)
    
    print(f"Report saved to {report_path}")
    
    # Print summary
    print("\nSummary:")
    for priority, failures in priority_categories.items():
        print(f"  {priority.title()}: {len(failures)} failures")
    
    # Cleanup
    json_file = Path("test_results.json")
    if json_file.exists():
        json_file.unlink()


if __name__ == "__main__":
    main()