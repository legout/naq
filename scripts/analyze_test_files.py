#!/usr/bin/env python3
"""
Script to analyze test files and identify potential issues without running tests.
"""

import ast
import os
from pathlib import Path
from typing import Dict, List, Set, Tuple
from collections import defaultdict


def get_test_files(test_dir: Path) -> List[Path]:
    """Get all test files in the test directory."""
    test_files = []
    for root, dirs, files in os.walk(test_dir):
        for file in files:
            if file.startswith("test_") and file.endswith(".py"):
                test_files.append(Path(root) / file)
    return test_files


def analyze_test_file(file_path: Path) -> Dict:
    """Analyze a single test file for potential issues."""
    issues = {
        "file": str(file_path),
        "imports": [],
        "test_classes": [],
        "test_functions": [],
        "potential_issues": []
    }
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Parse the AST
        tree = ast.parse(content)
        
        # Analyze imports
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    issues["imports"].append(alias.name)
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                for alias in node.names:
                    issues["imports"].append(f"{module}.{alias.name}")
        
        # Analyze test classes and functions
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and node.name.startswith("Test"):
                test_class = {
                    "name": node.name,
                    "methods": []
                }
                for item in node.body:
                    if isinstance(item, ast.FunctionDef) and item.name.startswith("test_"):
                        test_class["methods"].append(item.name)
                issues["test_classes"].append(test_class)
            
            elif isinstance(node, ast.FunctionDef) and item.name.startswith("test_"):
                issues["test_functions"].append(item.name)
        
        # Check for potential issues
        # 1. ServiceManager usage
        if "ServiceManager" in content:
            issues["potential_issues"].append({
                "type": "ServiceManager",
                "description": "Uses ServiceManager - may require proper setup"
            })
        
        # 2. ConnectionService usage
        if "ConnectionService" in content:
            issues["potential_issues"].append({
                "type": "ConnectionService",
                "description": "Uses ConnectionService - may require proper setup"
            })
        
        # 3. Mock usage
        if "mock" in content.lower() or "patch" in content:
            issues["potential_issues"].append({
                "type": "Mocking",
                "description": "Uses mocking - check if mocks are properly configured"
            })
        
        # 4. Async tests
        if "async def" in content:
            issues["potential_issues"].append({
                "type": "Async",
                "description": "Contains async tests - may require async test runner"
            })
        
        # 5. Database/External dependencies
        if any(pattern in content.lower() for pattern in ["database", "redis", "connection", "socket"]):
            issues["potential_issues"].append({
                "type": "ExternalDependency",
                "description": "May have external dependencies that need setup"
            })
        
    except Exception as e:
        issues["potential_issues"].append({
            "type": "ParseError",
            "description": f"Could not parse file: {e}"
        })
    
    return issues


def categorize_issues(analysis_results: List[Dict]) -> Dict[str, List[Dict]]:
    """Categorize issues by priority."""
    categories = {
        "critical": [],
        "high": [],
        "medium": [],
        "low": []
    }
    
    for result in analysis_results:
        file_path = result["file"]
        for issue in result["potential_issues"]:
            categorized_issue = {
                "file": file_path,
                "type": issue["type"],
                "description": issue["description"]
            }
            
            if issue["type"] in ["ServiceManager", "ConnectionService", "ParseError"]:
                categories["critical"].append(categorized_issue)
            elif issue["type"] in ["ExternalDependency", "Async"]:
                categories["high"].append(categorized_issue)
            elif issue["type"] in ["Mocking"]:
                categories["medium"].append(categorized_issue)
            else:
                categories["low"].append(categorized_issue)
    
    return categories


def generate_report(analysis_results: List[Dict], issue_categories: Dict[str, List[Dict]]) -> str:
    """Generate a comprehensive report."""
    report = []
    
    # Summary
    total_files = len(analysis_results)
    total_tests = sum(len(r["test_classes"]) + len(r["test_functions"]) for r in analysis_results)
    total_issues = sum(len(issues) for issues in issue_categories.values())
    
    report.append(f"# Test File Analysis Report")
    report.append(f"\n## Summary")
    report.append(f"- Total Test Files: {total_files}")
    report.append(f"- Total Test Classes/Functions: {total_tests}")
    report.append(f"- Total Potential Issues: {total_issues}")
    report.append(f"- Critical Issues: {len(issue_categories['critical'])}")
    report.append(f"- High Priority Issues: {len(issue_categories['high'])}")
    report.append(f"- Medium Priority Issues: {len(issue_categories['medium'])}")
    report.append(f"- Low Priority Issues: {len(issue_categories['low'])}")
    
    # Issue categories
    report.append(f"\n## Issue Categories")
    
    for priority, issues in issue_categories.items():
        report.append(f"\n### {priority.title()} Priority ({len(issues)} issues)")
        if issues:
            report.append("| File | Type | Description |")
            report.append("|------|------|-------------|")
            for issue in issues:
                file_path = issue["file"]
                issue_type = issue["type"]
                description = issue["description"]
                # Escape pipes in markdown
                description = description.replace("|", "\\|")
                report.append(f"| {file_path} | {issue_type} | {description} |")
        else:
            report.append("No issues in this category.")
    
    # Files with most issues
    report.append(f"\n## Files with Most Issues")
    file_issue_count = defaultdict(int)
    for result in analysis_results:
        file_issue_count[result["file"]] = len(result["potential_issues"])
    
    sorted_files = sorted(file_issue_count.items(), key=lambda x: x[1], reverse=True)[:10]
    for file_path, count in sorted_files:
        if count > 0:
            report.append(f"- {file_path}: {count} issues")
    
    # Recommendations
    report.append(f"\n## Recommendations")
    
    if issue_categories["critical"]:
        report.append("\n### Immediate Actions Required")
        report.append("Address critical issues first:")
        for issue in issue_categories["critical"][:5]:
            report.append(f"- {issue['file']}: {issue['description']}")
    
    report.append("\n### General Recommendations")
    report.append("1. Set up proper ServiceManager and ConnectionService fixtures")
    report.append("2. Configure external dependencies (database, Redis) for tests")
    report.append("3. Ensure async tests are properly handled")
    report.append("4. Review and update mock configurations")
    report.append("5. Consider using test factories for complex object creation")
    
    # Test structure analysis
    report.append(f"\n## Test Structure Analysis")
    
    # Count test types
    class_tests = sum(len(r["test_classes"]) for r in analysis_results)
    function_tests = sum(len(r["test_functions"]) for r in analysis_results)
    
    report.append(f"- Class-based tests: {class_tests}")
    report.append(f"- Function-based tests: {function_tests}")
    
    # Common imports
    all_imports = []
    for result in analysis_results:
        all_imports.extend(result["imports"])
    
    import_count = defaultdict(int)
    for imp in all_imports:
        import_count[imp] += 1
    
    report.append(f"\n### Most Common Imports")
    sorted_imports = sorted(import_count.items(), key=lambda x: x[1], reverse=True)[:10]
    for imp, count in sorted_imports:
        report.append(f"- {imp}: {count} files")
    
    return "\n".join(report)


def main():
    """Main function to run the analysis."""
    print("Analyzing test files...")
    
    # Get all test files
    test_dir = Path("tests")
    if not test_dir.exists():
        print("Tests directory not found")
        return
    
    test_files = get_test_files(test_dir)
    print(f"Found {len(test_files)} test files")
    
    # Analyze each test file
    analysis_results = []
    for test_file in test_files:
        print(f"Analyzing {test_file}...")
        result = analyze_test_file(test_file)
        analysis_results.append(result)
    
    # Categorize issues
    issue_categories = categorize_issues(analysis_results)
    
    # Generate report
    report = generate_report(analysis_results, issue_categories)
    
    # Save report
    report_path = Path("test_file_analysis.md")
    with open(report_path, "w") as f:
        f.write(report)
    
    print(f"Report saved to {report_path}")
    
    # Print summary
    print("\nSummary:")
    for priority, issues in issue_categories.items():
        print(f"  {priority.title()}: {len(issues)} issues")


if __name__ == "__main__":
    main()