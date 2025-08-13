### Sub-task 13: Update Documentation for Configuration System
**Description:** Create comprehensive documentation for the new YAML configuration system, including schema reference, examples, and a migration guide. This sub-task consolidates documentation efforts mentioned in previous sub-tasks.
**Implementation Steps:**
- Create new documentation files or sections within the existing documentation structure (e.g., in `docs/`).
- Develop a complete YAML schema reference, explaining each configuration option and its purpose.
- Provide configuration examples for development, production, and testing environments.
- Write a migration guide for existing users, explaining how to transition from environment-variable-only configuration to YAML, including a detailed mapping between old environment variables and new YAML paths.
- Document the new CLI configuration commands.
- Ensure all relevant code (classes, functions) in the `src/naq/config` module has appropriate docstrings.
**Success Criteria:**
- Comprehensive documentation is available and up-to-date.
- Users can easily understand, configure, and migrate to the new system using the provided documentation.
- All code in the new configuration module is well-documented with docstrings.
**Testing:**
- Review the documentation for clarity, accuracy, and completeness.
- Verify that all examples are correct and runnable.
**Documentation:**
- Publish the updated documentation.