"""Tests for the logging module."""

import logging
from io import StringIO
from pathlib import Path
from typing import Generator

import pytest

from ..logging import format_stats_table, save_conversion_report, setup_logging
from ..stats import ConversionStats


@pytest.fixture(autouse=True)
def setup_logging_fixture() -> Generator[None, None, None]:
    """Set up logging for each test."""
    # Clear any existing handlers
    root_logger = logging.getLogger()
    root_logger.handlers.clear()

    # Reset logging level
    root_logger.setLevel(logging.NOTSET)

    # Clear package logger handlers
    pkg_logger = logging.getLogger("parquet_converter")
    pkg_logger.handlers.clear()
    pkg_logger.propagate = True

    yield

    # Clean up after test
    root_logger.handlers.clear()
    pkg_logger.handlers.clear()
    pkg_logger.propagate = True


@pytest.fixture
def temp_log_file(tmp_path: Path) -> Path:
    """Create a temporary log file."""
    return tmp_path / "test.log"


def test_setup_logging_console() -> None:
    """Test logging setup with console output."""
    # Create a string buffer to capture log output
    log_buffer = StringIO()
    handler = logging.StreamHandler(log_buffer)
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)

    # Set up logging
    setup_logging(level="INFO", verbose=True)
    logger = logging.getLogger("parquet_converter")
    logger.addHandler(handler)

    # Test logging
    logger.info("Test message")
    assert "Test message" in log_buffer.getvalue()

    # Test debug message with verbose
    logger.debug("Debug message")
    assert "Debug message" in log_buffer.getvalue()


def test_setup_logging_file(temp_log_file: Path) -> None:
    """Test logging setup with file output."""
    # Set up logging
    setup_logging(level="INFO", log_file=temp_log_file)
    logger = logging.getLogger("parquet_converter")

    # Test logging
    test_message = "Test file message"
    logger.info(test_message)

    # Check log file
    assert temp_log_file.exists()
    log_content = temp_log_file.read_text()
    assert test_message in log_content


def test_setup_logging_levels() -> None:
    """Test different logging levels."""
    # Create a string buffer to capture log output
    log_buffer = StringIO()
    handler = logging.StreamHandler(log_buffer)
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)

    # Set up logging
    setup_logging(level="WARNING")
    logger = logging.getLogger("parquet_converter")
    logger.addHandler(handler)

    # Info should not be logged
    logger.info("Info message")
    assert "Info message" not in log_buffer.getvalue()

    # Warning should be logged
    logger.warning("Warning message")
    assert "Warning message" in log_buffer.getvalue()


def test_format_stats_table() -> None:
    """Test statistics table formatting."""
    stats_list = [
        ConversionStats(
            input_file="test1.csv",
            output_file="test1.parquet",
            rows_processed=100,
            rows_converted=100,
            errors=[],
            warnings=[],
        ),
        ConversionStats(
            input_file="test2.csv",
            output_file="test2.parquet",
            rows_processed=200,
            rows_converted=190,
            errors=["Error message"],
            warnings=["Warning message"],
        ),
    ]

    table = format_stats_table(stats_list)
    assert isinstance(table, str)
    assert "test1.csv" in table
    assert "test2.csv" in table
    assert "Success" in table
    assert "Failed" in table


def test_format_stats_table_empty() -> None:
    """Test statistics table formatting with empty list."""
    table = format_stats_table([])
    assert "No files were converted" in table


def test_save_conversion_report(tmp_path: Path) -> None:
    """Test saving conversion report."""
    stats_list = [
        ConversionStats(
            input_file="test.csv",
            output_file="test.parquet",
            rows_processed=100,
            rows_converted=100,
            errors=[],
            warnings=[],
        )
    ]

    config = {"csv": {"delimiter": ","}, "txt": {"delimiter": "\t"}}

    # Save report
    save_conversion_report(stats_list, tmp_path, config)

    # Check report file
    report_file = tmp_path / "conversion_report.json"
    assert report_file.exists()

    # Check report content
    import json

    report = json.loads(report_file.read_text())
    assert "timestamp" in report
    assert "config" in report
    assert "summary" in report
    assert "files" in report
    assert len(report["files"]) == 1
    assert report["summary"]["total_files"] == 1
    assert report["summary"]["successful"] == 1
    assert report["summary"]["failed"] == 0
