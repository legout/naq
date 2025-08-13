"""
NAQ CLI package.

This package contains all CLI commands for the NAQ system,
organized into focused modules for better maintainability.
"""

from .main import app

# Make the main CLI app available when importing the package
__all__ = ["app"]