# Agent Guidelines

## Commands
- Build: `hatchling build`
- Lint: `ruff check src/`
- Format: `ruff format src/`
- Test: `pytest tests/`
- Single test: `pytest tests/path/to/test.py::test_name`
- Type check: `ruff check src/ --select E,F,W`
- Run: `uv run`

## Code Style
- Use `msgspec.Struct` for data classes with efficient serialization
- Type hints required for all function parameters and return values
- Imports: standard library first, then third-party, then local modules
- Use `loguru` for logging with structured messages
- Error handling: raise custom exceptions from `.exceptions` module
- Async/await patterns with `anyio` for concurrency
- Use `cloudpickle` for function serialization
- Naming: snake_case for functions/variables, PascalCase for classes
- Docstrings for all public classes and methods

## General Guidelines

- Choose the simplest effective solution; avoid unnecessary complexity and over-engineering.
- Break complex problems into smaller, manageable pieces; solve one problem at a time.
- Prioritize readability and maintainability; write code for humans.
- Document the "why" behind non-obvious decisions.
- Remove unused code, variables, and imports.
- Identify repeated patterns and extract them into reusable, well-named functions or modules.
- Prefer composition over inheritance; group related utilities in dedicated modules.
- Keep functions short (≤20 lines) with minimal parameters (3-4); use objects for complex cases.
- Minimize deep nesting (≤3 levels); use early returns for clarity.
- Use classes when maintaining state or modeling real-world entities, but avoid unnecessary classes.
- Refactor regularly and seek feedback early.
- Focus on the 20% of code that delivers 80% of value; balance progress with perfection.       