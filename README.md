# Parquet Converter

[![PyPI version](https://badge.fury.io/py/parquet-converter.svg)](https://badge.fury.io/py/parquet-converter)
[![Python Versions](https://img.shields.io/pypi/pyversions/parquet-converter.svg)](https://pypi.org/project/parquet-converter/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Downloads](https://pepy.tech/badge/parquet-converter)](https://pepy.tech/project/parquet-converter)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Type Checking: mypy](https://img.shields.io/badge/type%20checking-mypy-blue.svg)](http://mypy-lang.org/)
[![Tests](https://github.com/sami5001/parquet-converter/actions/workflows/tests.yml/badge.svg)](https://github.com/sami5001/parquet-converter/actions/workflows/tests.yml)

A Python utility to convert TXT and CSV files to Parquet format. Developed by Sami Adnan.

## Overview

Parquet Converter is a command-line tool that allows you to convert text-based data files (TXT and CSV) to the Parquet format. It provides options for batch processing, detailed conversion statistics, and flexible configuration.

This project is part of Sami Adnan's DPhil research at the Nuffield Department of Primary Care Health Sciences, University of Oxford.

## Features

- Convert individual files or entire directories of TXT and CSV files to Parquet
- Automatic file type detection based on extension
- High-performance streaming conversion powered by Polars with schema sampling
- Configurable delimiters for CSV and TXT files
- Generate detailed conversion statistics and reports in JSON format
- Built-in Parquet analyzer for profiling existing datasets with rich reports
- Flexible output path configuration with automatic directory creation
- Support for secure configuration via:
  - Environment variables
  - JSON/YAML config files
  - Command-line arguments
- Automatic data type inference for:
  - Integers (int32, int64)
  - Floats (float32, float64)
  - Dates (with custom format support)
  - Booleans
  - Strings
- Configurable parsing options:
  - File encoding
  - Header row position
  - NA value handling
  - Memory usage optimization
- Environment variable support for key configuration options
- Numpy-style documentation for every class and function
- Docstring snapshots for every module in [`README-api.md`](README-api.md)
- Comprehensive logging system:
  - Console output
  - File logging
  - Different log levels
  - Formatted statistics tables
- Conversion statistics and reports:
  - JSON format reports
  - Success/failure tracking
  - Row and column statistics
  - Error and warning logging
- Progress tracking for batch conversions with visual progress bars
- Pre-commit hooks for code quality:
  - Black for code formatting
  - isort for import sorting
  - Flake8 for linting
  - mypy for type checking
- Performance optimization options:
  - Configurable compression (snappy, gzip, etc.)
  - Memory usage control
  - Chunk-based processing for large files

## Installation

### From PyPI

```bash
pip install parquet-converter
```

### From GitHub

```bash
# Clone the repository
git clone https://github.com/sami5001/parquet-converter
cd parquet-converter

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in development mode
pip install -e .
```

### Development Setup

For more detailed setup instructions, see [README-setup.md](README-setup.md).

```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Install pre-commit hooks
pre-commit install
```

## Usage

### Command Line Interface

```bash
# Basic Usage
# Convert a single file
parquet-converter input.csv -o output_dir/

# Convert a directory of files
parquet-converter input_dir/ -o output_dir/

# Advanced Usage
# Use a configuration file
parquet-converter input.csv -c config.yaml

# Enable verbose logging
parquet-converter input.csv -v

# Save current configuration to a file
parquet-converter input.csv --save-config my_config.yaml

# Convert with custom output directory
parquet-converter input.csv -o /path/to/output/

# Convert multiple file types in a directory
parquet-converter data_dir/ -o output_dir/ -c config.yaml

# Convert with verbose logging and custom config
parquet-converter input.csv -v -c custom_config.yaml -o output_dir/
```

### Analyzer Mode

Generate a rich text report describing every Parquet file in a directory:

```bash
# Analyze an existing collection of Parquet files
parquet-converter /path/to/parquet/dir --mode analyze --report-dir reports/
```

By default reports are saved under ``<input_dir>/reports/parquet_analysis_report.txt`` unless ``--report-dir`` or
``ANALYZER_REPORT_DIR`` is provided.

### Example Configuration Files

1. Basic YAML Configuration:
```yaml
# config.yaml
csv:
  delimiter: ","
  encoding: "utf-8"
  header: 0
txt:
  delimiter: "\t"
  encoding: "utf-8"
  header: 0
datetime_formats:
  default: "%Y-%m-%d"
  custom: ["%d/%m/%Y", "%Y-%m-%d %H:%M:%S"]
infer_dtypes: true
compression: "snappy"
log_level: "INFO"
```

2. Advanced Configuration with Performance Settings:
```yaml
# advanced_config.yaml
csv:
  delimiter: ","
  encoding: "utf-8"
  header: 0
  low_memory: true
  chunk_size: 10000
txt:
  delimiter: "\t"
  encoding: "utf-8"
  header: 0
  low_memory: true
  chunk_size: 10000
datetime_formats:
  default: "%Y-%m-%d"
  custom: ["%d/%m/%Y", "%Y-%m-%d %H:%M:%S"]
infer_dtypes: true
compression:
  type: "snappy"
  level: 1
  block_size: "128MB"
log_level: "DEBUG"
log_file: "conversion.log"
parallel:
  enabled: true
  max_workers: 4
```

### Environment Variables

You can configure the converter using environment variables:

```bash
# Set input and output paths
export INPUT_PATH="data.csv"
export OUTPUT_DIR="output/"

# Configure logging
export LOG_LEVEL="DEBUG"
export LOG_FILE="conversion.log"

# Set file-specific options
export DELIMITER=","
export ENCODING="utf-8"
```

## Docstring Standards

All modules, classes, and functions are documented using the NumPy style.

### Parquet Format Benefits

The Parquet format offers several advantages:

- Columnar storage format enabling efficient querying and compression
- Reduced I/O operations when querying specific columns
- Efficient data encoding and compression schemes
- Compatible with various big data tools (Spark, Hadoop, etc.)

### Environment Variables Reference

Available environment variables for configuration:

- `INPUT_PATH`: Path to input file/directory
- `OUTPUT_DIR`: Output directory path
- `LOG_LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- `LOG_FILE`: Path to log file
- `DELIMITER`: Custom delimiter for text files

### Additional Resources

For more information on Parquet, please refer to:
- [Apache Parquet Documentation](https://parquet.apache.org/docs/)
- [PyArrow Parquet Documentation](https://arrow.apache.org/docs/python/parquet.html)
- [Parquet Format Specification](https://github.com/apache/parquet-format)

### Example Workflows

1. **Basic File Conversion**:
```bash
# Convert a single CSV file
parquet-converter data.csv -o output/
```

2. **Batch Processing**:
```bash
# Convert all files in a directory
parquet-converter data_dir/ -o output/ -c config.yaml
```

3. **Debug Mode**:
```bash
# Convert with detailed logging
parquet-converter data.csv -v -o output/
```

4. **Custom Configuration**:
```bash
# Save current settings as config
parquet-converter data.csv --save-config my_config.yaml

# Use custom config
parquet-converter data.csv -c my_config.yaml -o output/
```

5. **Performance-Optimized Conversion**:
```bash
# Convert large files with memory optimization
parquet-converter large_data.csv -c performance_config.yaml -o output/
```

## Output

The converter generates:

1. Parquet files with inferred data types
2. Conversion statistics in JSON format
3. Detailed logs with conversion summary
4. Progress indicators for batch operations

Example conversion report:
```json
{
  "timestamp": "2024-03-14T12:00:00",
  "summary": {
    "total_files": 2,
    "successful": 2,
    "failed": 0
  },
  "files": [
    {
      "input_file": "data.csv",
      "output_file": "data.parquet",
      "rows_processed": 1000,
      "rows_converted": 1000,
      "success": true
    }
  ]
}
```

## Development

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=parquet_converter

# Run specific test file
pytest parquet_converter/tests/test_converter.py
```

### Code Quality

The project uses several tools to maintain code quality:

- Black for code formatting
- isort for import sorting
- Flake8 for linting
- mypy for type checking

These are enforced using pre-commit hooks.

### Performance Considerations

The converter is optimized for performance with the following features:

- Efficient memory management for large files
- Parallel processing for batch conversions
- Optimized data type inference
- Configurable compression options

#### Best Practices

1. **File Size Optimization**
   - For files > 1GB, consider using the `low_memory=True` option
   - Use appropriate compression (snappy for speed, gzip for size)
   - Process large files in chunks when possible

2. **Memory Usage**
   - Monitor memory usage during conversion
   - Use appropriate chunk sizes for large files
   - Consider system resources when processing multiple files

3. **Performance Tuning**
   - Adjust compression settings based on your needs
   - Use appropriate data types to minimize memory usage
   - Consider using SSD storage for better I/O performance

4. **Batch Processing**
   - Use directory conversion for multiple files
   - Monitor system resources during batch operations
   - Consider using parallel processing for large batches

To run performance tests:
```bash
pytest parquet_converter/tests/test_performance.py -v
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions to the Parquet Converter project are welcome! Please refer to [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines on how to contribute.

Briefly, please follow these steps to contribute:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run the tests to ensure everything works (`pytest`)
5. Commit your changes (`git commit -m 'Add some amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## Acknowledgements

- Sami Adnan, Nuffield Department of Primary Care Health Sciences, University of Oxford
- Apache Parquet, PyArrow, and pandas development teams

## Troubleshooting

### Common Issues

1. **Memory Errors**
   - If you encounter memory errors with large files, try:
     - Using `low_memory=True` in configuration
     - Reducing chunk size
     - Processing files in smaller batches

2. **Encoding Issues**
   - If you see encoding errors, try:
     - Specifying the correct encoding in config (e.g., 'utf-8', 'latin1')
     - Using `encoding='utf-8-sig'` for files with BOM

3. **Performance Issues**
   - If conversion is slow:
     - Check if compression settings are appropriate
     - Consider using SSD storage
     - Adjust chunk size based on available memory

### Error Messages

Common error messages and their solutions:

```
ValueError: Could not infer delimiter
Solution: Specify delimiter in config file or use --delimiter option
```

```
MemoryError: Unable to allocate array
Solution: Use low_memory=True or reduce chunk size
```

```
UnicodeDecodeError: 'utf-8' codec can't decode byte
Solution: Specify correct encoding in config file
```

## Advanced Usage

### Custom Data Type Inference

You can customize data type inference by modifying the config:

```yaml
data_types:
  integers:
    - int32
    - int64
  floats:
    - float32
    - float64
  dates:
    - date
    - datetime
  booleans:
    - bool
  strings:
    - string
    - category
```

### Parallel Processing

For batch processing, you can enable parallel processing:

```yaml
parallel:
  enabled: true
  max_workers: 4
  chunk_size: 10000
```

### Custom Compression

Configure compression settings:

```yaml
compression:
  type: snappy  # Options: snappy, gzip, brotli, zstd
  level: 1      # Compression level (1-9)
  block_size: 128MB
```

## API Reference

### Command Line Arguments

```
usage: parquet-converter [-h] [-o OUTPUT_DIR] [-c CONFIG] [-v] input_path

positional arguments:
  input_path            Path to input file or directory

optional arguments:
  -h, --help            Show this help message and exit
  -o OUTPUT_DIR, --output-dir OUTPUT_DIR
                        Output directory path
  -c CONFIG, --config CONFIG
                        Path to configuration file
  -v, --verbose         Enable verbose logging
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| csv.delimiter | string | "," | CSV file delimiter |
| csv.encoding | string | "utf-8" | File encoding |
| csv.header | int | 0 | Header row index |
| txt.delimiter | string | "\t" | TXT file delimiter |
| datetime_formats | list | ["%Y-%m-%d"] | Date format patterns |
| infer_dtypes | bool | true | Enable type inference |
| compression | string | "snappy" | Compression type |
| log_level | string | "INFO" | Logging level |

## Roadmap

Planned features and improvements:

1. **Short Term**
   - Support for more input formats (JSON, Excel)
   - Enhanced data type inference
   - Improved error handling

2. **Medium Term**
   - Distributed processing support
   - Web interface for file conversion
   - Real-time conversion monitoring

3. **Long Term**
   - Cloud storage integration
   - Advanced data validation
   - Custom transformation rules

## Support

For issues and feature requests, please use the GitHub issue tracker.

### Getting Help

- Check the [Troubleshooting](#troubleshooting) section
- Search [existing issues](https://github.com/sami5001/parquet-converter/issues)
- Create a new issue with detailed information
- Join our [Discussions](https://github.com/sami5001/parquet-converter/discussions) for community support
