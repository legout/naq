#!/usr/bin/env python3
"""
Simple script to analyze test failures from pytest output.
"""

import subprocess
import re
from pathlib import Path
from typing import Dict, List, Tuple
from collections import defaultdict


def run_pytest_and_capture_output():
    """Run pytest and capture the output."""
    try:
        result = subprocess.run(
            ["uv", "run", "pytest", "tests/", "--tb=short", "-v"],
            capture_output=True,
            text=True,
            cwd=Path.cwd()
        )
        return result.stdout, result.stderr, result.returncode
    except Exception as e:
        print(f"Error running pytest: {e}")
        return "", "", 1


def parse_failures(output: str) -> List[Dict]:
    """Parse pytest output to extract failures."""
    failures = []
    
    # Split output by lines
    lines = output.split('\n')
    
    current_failure = None
    in_failure = False
    error_lines = []
    
    for line in lines:
        # Check for FAILED line
        if line.strip().startswith('FAILED '):
            if current_failure:
                current_failure['error'] = '\n'.join(error_lines)
                failures.append(current_failure)
            
            # Extract test name
            test_name = line.strip()[7:].strip()
            current_failure = {
                'name': test_name,
                'error': '',
                'module': test_name.split('::')[0] if '::' in test_name else 'unknown'
            }
            in_failure = True
            error_lines = []
        
        # Check for ERROR line
        elif line.strip().startswith('ERROR '):
            if current_failure:
                current_failure['error'] = '\n'.join(error_lines)
                failures.append(current_failure)
            
            # Extract test name
            test_name = line.strip()[6:].strip()
            current_failure = {
                'name': test_name,
                'error': '',
                'module': test_name.split('::')[0] if '::' in test_name else 'unknown'
            }
            in_failure = True
            error_lines = []
        
        # Collect error lines
        elif in_failure and (line.strip().startswith('E       ') or 
                            line.strip().startswith('E   ') or
                            line.strip().startswith('    ')):
            error_lines.append(line.strip())
        
        # End of failure section
        elif in_failure and line.strip() == '':
            if current_failure and error_lines:
                current_failure['error'] = '\n'.join(error_lines)
                failures.append(current_failure)
                current_failure = None
                in_failure = False
                error_lines = []
    
    # Add the last failure if exists
    if current_failure:
        current_failure['error'] = '\n'.join(error_lines)
        failures.append(current_failure)
    
    return failures


def categorize_by_priority(failures: List[Dict]) -> Dict[str, List[Dict]]:
    """Categorize failures by priority."""
    categories = {
        "critical": [],
        "high": [],
        "medium": [],
        "low": []
    }
    
    for failure in failures:
        error = failure.get('error', '')
        
        # Categorize based on error patterns
        if any(pattern in error for pattern in [
            "ServiceManager is required",
            "Service.*not registered",
            "Connection.*required",
            "AttributeError.*does not have the attribute",
            "ModuleNotFoundError",
            "ImportError",
            "RuntimeError: Service"
        ]):
            categories["critical"].append(failure)
        elif any(pattern in error for pattern in [
            "ConfigurationError",
            "ValidationError",
            "TypeConversionError",
            "NaqException",
            "AssertionError.*Expected.*Called",
            "RuntimeError: Configuration"
        ]):
            categories["high"].append(failure)
        elif any(pattern in error for pattern in [
            "AssertionError",
            "ValueError",
            "TypeError",
            "Failed: DID NOT RAISE",
            "RuntimeError"
        ]):
            categories["medium"].append(failure)
        else:
            categories["low"].append(failure)
    
    return categories


def categorize_by_module(failures: List[Dict]) -> Dict[str, List[Dict]]:
    """Categorize failures by module."""
    module_categories = defaultdict(list)
    
    for failure in failures:
        module = failure.get('module', 'unknown')
        module_categories[module].append(failure)
    
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
            report.append("| Test Name | Error |")
            report.append("|-----------|-------|")
            for failure in failures[:10]:  # Show first 10
                test_name = failure["name"]
                error = failure["error"]
                # Truncate long error messages
                short_error = error[:100] + "..." if len(error) > 100 else error
                # Escape pipes in markdown
                short_error = short_error.replace("|", "\\|")
                report.append(f"| {test_name} | {short_error} |")
            if len(failures) > 10:
                report.append(f"| ... and {len(failures) - 10} more | |")
        else:
            report.append("No failures in this category.")
    
    # Module-based breakdown
    report.append(f"\n## Module-Based Breakdown")
    
    sorted_modules = sorted(module_categories.items(), key=lambda x: len(x[1]), reverse=True)
    
    for module, failures in sorted_modules:
        report.append(f"\n### {module} ({len(failures)} failures)")
        if failures:
            report.append("| Test Name | Error |")
            report.append("|-----------|-------|")
            for failure in failures[:5]:  # Show first 5 per module
                test_name = failure["name"]
                error = failure["error"]
                # Truncate long error messages
                short_error = error[:100] + "..." if len(error) > 100 else error
                # Escape pipes in markdown
                short_error = short_error.replace("|", "\\|")
                report.append(f"| {test_name} | {short_error} |")
            if len(failures) > 5:
                report.append(f"| ... and {len(failures) - 5} more | |")
    
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
    print("Running test failure analysis...")
    
    # Run pytest and capture output
    stdout, stderr, returncode = run_pytest_and_capture_output()
    
    if returncode != 0:
        print(f"Pytest exited with code {returncode}")
    
    # Parse failures
    failures = parse_failures(stdout)
    print(f"Found {len(failures)} failures")
    
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