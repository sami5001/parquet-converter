# Development Setup Guide

This document describes how to set up the development environment for the Parquet Converter project.

This project is part of Sami Adnan's DPhil research at the Nuffield Department of Primary Care Health Sciences, University of Oxford.

## Initial Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/sami5001/parquet-converter.git
   cd parquet-converter
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install development dependencies:
   ```bash
   pip install -e ".[dev]"
   ```

4. Set up pre-commit hooks:
   ```bash
   pre-commit install
   ```

## Working with Dependencies

### Using Locked Dependencies

For production or reproducible environments, use the locked dependencies:

```bash
pip install -r requirements-lock.txt
```

### Updating Locked Dependencies

To update the locked dependencies:

1. Make changes to `pyproject.toml` if needed
2. Run:
   ```bash
   pip-compile pyproject.toml --output-file=requirements-lock.txt
   ```

### Development with Latest Versions

For development with the latest package versions:

```bash
pip install -r requirements-dev.txt
```

## Running Tests

```bash
pytest
```

With coverage:

```bash
pytest --cov=parquet_converter
```

## Code Quality

The project uses several tools to maintain code quality:

- **Black**: Code formatting
- **isort**: Import sorting
- **flake8**: Code linting
- **mypy**: Type checking

These tools are configured in `pyproject.toml` and `.pre-commit-config.yaml`.

To manually run these tools:

```bash
black .
isort .
flake8
mypy parquet_converter
```

## Building the Package

```bash
python -m build
```

## Installing from GitHub

```bash
pip install git+https://github.com/sami5001/parquet-converter.git
```

## Parquet Resources for Developers

When working with this project, it may be helpful to understand the Parquet format in detail:

- [Apache Parquet Documentation](https://parquet.apache.org/docs/)
- [PyArrow Parquet API Reference](https://arrow.apache.org/docs/python/parquet.html)
- [Parquet Format Specification](https://github.com/apache/parquet-format)
- [ParquetTools](https://github.com/apache/parquet-mr/tree/master/parquet-tools) - Useful tools for working with Parquet files
- [Parquet Metadata Explained](https://www.databricks.com/blog/2022/11/15/understanding-apache-parquet-metadata.html) - Detailed explanation of Parquet metadata
