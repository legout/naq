#!/usr/bin/env python3
"""Debug script to test the event commands."""

import sys
from unittest.mock import AsyncMock, MagicMock, patch
from typer.testing import CliRunner
from naq.cli.event_commands import event_app

def test_stream_events():
    """Test the stream events command."""
    runner = CliRunner()
    
    # Print help first to see the command options
    print("=== Help output ===")
    result = runner.invoke(event_app, ["--help"])
    print("Exit code:", result.exit_code)
    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)
    
    # Now try the actual command
    print("\n=== Command output ===")
    result = runner.invoke(event_app, ["stream", "--help"])
    print("Exit code:", result.exit_code)
    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)
    
    # Try with the actual parameters
    print("\n=== Actual command ===")
    result = runner.invoke(event_app, ["stream", "--follow", "false", "--tail", "2"])
    print("Exit code:", result.exit_code)
    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)

if __name__ == "__main__":
    test_stream_events()