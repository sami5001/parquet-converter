# Contributing to Parquet Converter

Thank you for considering a contribution to the Parquet Converter project! This document outlines the process for contributing to the project.

## Code of Conduct

This project is part of Sami Adnan's DPhil research at the Nuffield Department of Primary Care Health Sciences, University of Oxford. We expect all contributors to respect each other and create a welcoming environment.

## How Can I Contribute?

### Reporting Bugs

Bugs are tracked as GitHub issues. When you create an issue, please include:

- A clear and descriptive title
- Steps to reproduce the issue
- Expected behavior
- Actual behavior
- Any relevant logs or screenshots
- Your environment (OS, Python version, etc.)

### Suggesting Enhancements

Enhancement suggestions are also tracked as GitHub issues. When suggesting an enhancement, please include:

- A clear and descriptive title
- A detailed description of the proposed enhancement
- An explanation of why this enhancement would be useful
- Possible implementation approaches if you have ideas

### Pull Requests

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Set up the development environment:
   ```bash
   pip install -e ".[dev]"
   pre-commit install
   ```
4. Make your changes
5. Run the tests and ensure they pass:
   ```bash
   pytest
   ```
6. Run the pre-commit hooks to ensure code quality:
   ```bash
   pre-commit run --all-files
   ```
7. Commit your changes with a meaningful commit message:
   ```bash
   git commit -m 'Add some amazing feature'
   ```
8. Push to your branch:
   ```bash
   git push origin feature/amazing-feature
   ```
9. Open a Pull Request against the main repository

## Development Workflow

### Setting Up a Development Environment

Please see [README-setup.md](README-setup.md) for detailed instructions on setting up your development environment.

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage report
pytest --cov=parquet_converter

# Run a specific test file
pytest tests/test_specific_file.py
```

### Code Style

This project follows PEP 8 and uses black, isort, and flake8 for code formatting and linting. These tools are configured in pyproject.toml and .pre-commit-config.yaml.

To ensure your code matches the project's style:

```bash
# Format code
black .
isort .

# Check code quality
flake8
mypy parquet_converter
```

### Documentation

- Document all functions, classes, and modules with docstrings
- Keep the README.md updated with any new features or changes
- Update examples when necessary

## Release Process

Release versions follow [Semantic Versioning](https://semver.org/):

- MAJOR version for incompatible API changes
- MINOR version for new functionality in a backward compatible manner
- PATCH version for backward compatible bug fixes

The project maintainer (Sami Adnan) is responsible for creating new releases.

## Questions?

If you have any questions about contributing, please contact:

Sami Adnan
sami.adnan@phc.ox.ac.uk
Nuffield Department of Primary Care Health Sciences
University of Oxford
