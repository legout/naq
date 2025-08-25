"""Warning utilities for NAQ.

This module contains utilities for handling deprecation warnings and other
warning-related functionality.
"""

import warnings


def deprecated_import_warning(
    old_path: str, 
    new_path: str, 
    version: str = "1.0.0",
    stacklevel: int = 3
) -> None:
    """
    Issue a deprecation warning for legacy import paths.
    
    This function issues a DeprecationWarning with a clear message including
    the old and new import paths and the planned removal version.
    
    Args:
        old_path: The old import path that is being deprecated
        new_path: The new import path that should be used instead
        version: The version when the old import will be removed (default: "1.0.0")
        stacklevel: The stack level for the warning (default: 3)
        
    Example:
        ```python
        from naq.utils.warnings import deprecated_import_warning
        
        deprecated_import_warning(
            old_path="naq.Job",
            new_path="naq.models.jobs.Job"
        )
        ```
    """
    message = (
        f"{old_path} is deprecated and will be removed in version {version}. "
        f"Please use {new_path} instead."
    )
    
    warnings.warn(
        message,
        DeprecationWarning,
        stacklevel=stacklevel
    )


def create_deprecated_class(
    original_class: type,
    old_path: str,
    new_path: str,
    version: str = "1.0.0"
) -> type:
    """
    Create a deprecated version of a class that issues a warning on instantiation.
    
    Args:
        original_class: The original class to wrap
        old_path: The old import path that is being deprecated
        new_path: The new import path that should be used instead
        version: The version when the old import will be removed (default: "1.0.0")
        
    Returns:
        A new class that wraps the original class and issues a deprecation
        warning when instantiated.
        
    Example:
        ```python
        from naq.models.jobs import Job
        from naq.utils.warnings import create_deprecated_class
        
        DeprecatedJob = create_deprecated_class(
            Job,
            old_path="naq.Job",
            new_path="naq.models.jobs.Job"
        )
        ```
    """
    # Create a wrapper class that issues warnings and delegates to the original class
    class DeprecatedClass:
        """Deprecated version of {original_class.__name__}.
        
        This class issues a deprecation warning when instantiated.
        Use {new_path} instead.
        """
        _deprecated_old_path = old_path
        _deprecated_new_path = new_path
        _deprecated_version = version
        _original_class = original_class
        
        def __new__(cls, *args, **kwargs):
            # Issue the deprecation warning
            deprecated_import_warning(
                old_path=cls._deprecated_old_path,
                new_path=cls._deprecated_new_path,
                version=cls._deprecated_version,
                stacklevel=3
            )
            
            # Create an instance of the original class
            return cls._original_class(*args, **kwargs)
        
        @classmethod
        def __init_subclass__(cls, **kwargs):
            # Prevent subclassing of deprecated classes
            raise TypeError(f"Cannot subclass deprecated class {cls.__name__}")
    
    # Set the name and qualname to match the original class
    DeprecatedClass.__name__ = original_class.__name__
    DeprecatedClass.__qualname__ = original_class.__qualname__
    
    # Update the docstring to include deprecation information
    DeprecatedClass.__doc__ = (
        f"Deprecated version of {original_class.__name__}.\n\n"
        f"This class is deprecated and will be removed in version {version}. "
        f"Please use {new_path} instead.\n\n"
        f"Original docstring:\n{original_class.__doc__ or 'No documentation available.'}"
    )
    
    # Copy class attributes from the original class
    for attr_name, attr_value in original_class.__dict__.items():
        if not attr_name.startswith('__'):
            setattr(DeprecatedClass, attr_name, attr_value)
    
    # Copy class methods and static methods
    for name, method in original_class.__dict__.items():
        if isinstance(method, (classmethod, staticmethod)):
            setattr(DeprecatedClass, name, method)
    
    return DeprecatedClass