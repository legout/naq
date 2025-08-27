#!/usr/bin/env python3
"""
Script to categorize test failures by priority and impact.
This helps prioritize which test failures to address first.
"""

import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple, Set


def run_pytest_with_failures() -> str:
    """Run pytest and capture the output to analyze failures."""
    try:
        result = subprocess.run(
            ["uv", "run", "pytest", "tests/", "--tb=no", "-q"],
            capture_output=True,
            text=True,
            cwd=Path.cwd()
        )
        return result.stdout
    except Exception as e:
        print(f"Error running pytest: {e}")
        return ""


def parse_failures(output: str) -> List[Tuple[str, str]]:
    """Parse pytest output to extract failed test names and error messages."""
    failures = []
    
    # Extract FAILED lines - multiple patterns to catch different formats
    failed_patterns = [
        r"FAILED (.*?)::(.*?) - (.+)",
        r"FAILED (.*?)::(.*)",
        r"FAILED (.+)"
    ]
    
    for pattern in failed_patterns:
        matches = re.findall(pattern, output)
        for match in matches:
            if len(match) == 3:  # module::test - error
                module, test_name, error = match
                full_test_name = f"{module}::{test_name}"
                failures.append((full_test_name, error))
            elif len(match) == 2:  # module::test
                module, test_name = match
                full_test_name = f"{module}::{test_name}"
                failures.append((full_test_name, "FAILED"))
            else:  # single match
                full_test_name = match[0]
                failures.append((full_test_name, "FAILED"))
    
    # Extract ERROR lines - multiple patterns
    error_patterns = [
        r"ERROR (.*?)::(.*?) - (.+)",
        r"ERROR (.*?)::(.*)",
        r"ERROR (.+)"
    ]
    
    for pattern in error_patterns:
        matches = re.findall(pattern, output)
        for match in matches:
            if len(match) == 3:  # module::test - error
                module, test_name, error = match
                full_test_name = f"{module}::{test_name}"
                failures.append((full_test_name, f"ERROR: {error}"))
            elif len(match) == 2:  # module::test
                module, test_name = match
                full_test_name = f"{module}::{test_name}"
                failures.append((full_test_name, "ERROR"))
            else:  # single match
                full_test_name = match[0]
                failures.append((full_test_name, "ERROR"))
    
    # Remove duplicates while preserving order
    seen = set()
    unique_failures = []
    for failure in failures:
        if failure[0] not in seen:
            seen.add(failure[0])
            unique_failures.append(failure)
    
    return unique_failures


def categorize_by_priority(failures: List[Tuple[str, str]]) -> Dict[str, List[Tuple[str, str]]]:
    """Categorize failures by priority based on error patterns and test modules."""
    categories = {
        "critical": [],
        "high": [],
        "medium": [],
        "low": []
    }
    
    critical_patterns = [
        r"ServiceManager is required",
        r"Service.*not registered",
        r"Connection.*required",
        r"AttributeError.*does not have the attribute",
        r"ModuleNotFoundError",
        r"ImportError"
    ]
    
    high_patterns = [
        r"ConfigurationError",
        r"ValidationError",
        r"TypeConversionError",
        r"NaqException",
        r"AssertionError.*Expected.*Called"
    ]
    
    medium_patterns = [
        r"AssertionError",
        r"ValueError",
        r"TypeError",
        r"Failed: DID NOT RAISE"
    ]
    
    for test_name, error in failures:
        # Check critical patterns first
        if any(re.search(pattern, error) for pattern in critical_patterns):
            categories["critical"].append((test_name, error))
        # Check high patterns
        elif any(re.search(pattern, error) for pattern in high_patterns):
            categories["high"].append((test_name, error))
        # Check medium patterns
        elif any(re.search(pattern, error) for pattern in medium_patterns):
            categories["medium"].append((test_name, error))
        else:
            categories["low"].append((test_name, error))
    
    return categories


def categorize_by_module(failures: List[Tuple[str, str]]) -> Dict[str, List[Tuple[str, str]]]:
    """Categorize failures by test module."""
    module_categories = defaultdict(list)
    
    for test_name, error in failures:
        module = test_name.split("::")[0]
        module_categories[module].append((test_name, error))
    
    return dict(module_categories)


def generate_report(priority_categories: Dict[str, List[Tuple[str, str]]], 
                   module_categories: Dict[str, List[Tuple[str, str]]]) -> str:
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
            report.append("| Test Name | Error |")
            report.append("|-----------|-------|")
            for test_name, error in failures:
                # Truncate long error messages
                short_error = error[:100] + "..." if len(error) > 100 else error
                # Escape pipes in markdown
                short_error = short_error.replace("|", "\\|")
                report.append(f"| {test_name} | {short_error} |")
        else:
            report.append("No failures in this category.")
    
    # Module-based breakdown
    report.append(f"\n## Module-Based Breakdown")
    
    for module, failures in module_categories.items():
        report.append(f"\n### {module} ({len(failures)} failures)")
        if failures:
            report.append("| Test Name | Error |")
            report.append("|-----------|-------|")
            for test_name, error in failures:
                # Truncate long error messages
                short_error = error[:100] + "..." if len(error) > 100 else error
                # Escape pipes in markdown
                short_error = short_error.replace("|", "\\|")
                report.append(f"| {test_name} | {short_error} |")
    
    # Recommendations
    report.append(f"\n## Recommendations")
    
    if priority_categories["critical"]:
        report.append("\n### Immediate Actions Required")
        report.append("Address critical failures first as they likely block core functionality:")
        for test_name, _ in priority_categories["critical"][:5]:  # Show first 5
            report.append(f"- {test_name}")
        if len(priority_categories["critical"]) > 5:
            report.append(f"- ... and {len(priority_categories['critical']) - 5} more")
    
    report.append("\n### General Recommendations")
    report.append("1. Focus on service-related failures (ServiceManager, ConnectionService)")
    report.append("2. Address configuration and validation errors")
    report.append("3. Fix import and attribute errors")
    report.append("4. Update deprecated test patterns")
    report.append("5. Consider reducing test coverage threshold temporarily")
    
    return "\n".join(report)


def main():
    """Main function to run the failure analysis."""
    print("Running test failure analysis...")
    
    # Get pytest output
    output = run_pytest_with_failures()
    if not output:
        print("Failed to get pytest output")
        sys.exit(1)
    
    # Parse failures
    failures = parse_failures(output)
    print(f"Found {len(failures)} test failures")
    
    # Categorize failures
    priority_categories = categorize_by_priority(failures)
    module_categories = categorize_by_module(failures)
    
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


if __name__ == "__main__":
    main()