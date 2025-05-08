"""Performance tests for the Parquet Converter."""

import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
import pytest

from ..config import Config
from ..converter import convert_directory, convert_file
from ..stats import ConversionStats


def generate_test_file(rows: int, cols: int, file_type: str) -> str:
    """Generate a test file with random data.

    Args:
        rows: Number of rows
        cols: Number of columns
        file_type: File type (csv or txt)

    Returns:
        Path to generated file
    """
    # Generate random data
    data = {}
    for i in range(cols):
        data[f"col{i}"] = np.random.randint(0, 100, size=rows)

    # Create DataFrame
    df = pd.DataFrame(data)

    # Save to file
    suffix = f".{file_type}"
    temp_file = tempfile.mktemp(suffix=suffix)

    if file_type == "csv":
        df.to_csv(temp_file, index=False)
    else:
        df.to_csv(temp_file, sep="\t", index=False)

    return temp_file


@pytest.mark.performance
def test_conversion_performance() -> None:
    """Test conversion performance with different file sizes."""
    test_cases = [
        (1000, 10, "csv"),
        (10000, 10, "csv"),
        (100000, 10, "csv"),
        (1000, 10, "txt"),
        (10000, 10, "txt"),
        (100000, 10, "txt"),
    ]

    results = []

    for rows, cols, file_type in test_cases:
        # Generate test file
        input_file = generate_test_file(rows, cols, file_type)
        output_dir = tempfile.mkdtemp()

        try:
            # Measure conversion time
            start_time = time.time()
            config = Config()
            convert_file(input_file, output_dir, config.model_dump())
            end_time = time.time()

            # Calculate performance metrics
            conversion_time = end_time - start_time
            rows_per_second = rows / conversion_time

            results.append(
                {
                    "file_type": file_type,
                    "rows": rows,
                    "columns": cols,
                    "conversion_time": conversion_time,
                    "rows_per_second": rows_per_second,
                }
            )

        finally:
            # Cleanup
            os.unlink(input_file)
            shutil.rmtree(output_dir)

    # Verify performance thresholds
    for result in results:
        # Convert the value to float explicitly
        performance = result["rows_per_second"]
        if isinstance(performance, (int, float)):
            assert performance > 1000.0, (
                f"Performance below threshold for {result['file_type']} file " f"with {result['rows']} rows"
            )


@pytest.fixture
def large_csv_file(tmp_path: Path) -> Path:
    """Create a large CSV file for testing."""
    file_path = tmp_path / "large.csv"
    rows = 100_000
    # Handle potential OutOfBoundsDatetime error for large date ranges
    try:
        dates = pd.date_range("2023-01-01", periods=rows)
    except pd.errors.OutOfBoundsDatetime:
        # If default range exceeds limits, create a valid bounded range
        # This might not be semantically identical but allows the test to run
        end_date = pd.Timestamp.max - pd.Timedelta(days=1)  # Ensure end is valid
        start_date = end_date - pd.Timedelta(days=rows - 1)
        if start_date < pd.Timestamp.min:
            start_date = pd.Timestamp.min
        dates = pd.date_range(start=start_date, periods=rows)

    df = pd.DataFrame(
        {
            "id": range(rows),
            "value": [f"value_{i}" for i in range(rows)],
            "date": dates,
        }
    )
    df.to_csv(file_path, index=False)
    return file_path


@pytest.fixture
def config() -> Dict:
    """Create a test configuration."""
    return {
        "csv": {
            "delimiter": ",",
            "encoding": "utf-8",
            "header": 0,
            "low_memory": True,
        },
        "txt": {
            "delimiter": "\t",
            "encoding": "utf-8",
            "header": 0,
            "low_memory": True,
        },
        "datetime_formats": {
            "default": "%Y-%m-%d",
            "custom": [],
        },
    }


def test_large_file_performance(
    large_csv_file: Path,
    tmp_path: Path,
    config: Dict,
) -> None:
    """Test performance with a large file."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    # Measure conversion time
    start_time = time.time()
    stats = convert_file(large_csv_file, output_dir, config)
    end_time = time.time()

    # Verify conversion
    assert isinstance(stats, ConversionStats)
    assert stats.rows_processed == 100_000
    assert stats.rows_converted == 100_000
    assert not stats.errors
    assert not stats.warnings

    # Check performance
    conversion_time = end_time - start_time
    assert conversion_time < 10.0  # Should complete within 10 seconds


def test_batch_performance(tmp_path: Path, config: Dict) -> None:
    """Test performance with multiple files."""
    # Create test files
    num_files = 5
    rows_per_file = 20_000
    files: List[Path] = []

    for i in range(num_files):
        file_path = tmp_path / f"test_{i}.csv"
        df = pd.DataFrame(
            {
                "id": range(rows_per_file),
                "value": [f"value_{j}" for j in range(rows_per_file)],
                "date": pd.date_range("2023-01-01", periods=rows_per_file),
            }
        )
        df.to_csv(file_path, index=False)
        files.append(file_path)

    output_dir = tmp_path / "output"
    output_dir.mkdir()

    # Measure conversion time
    start_time = time.time()
    stats_list = convert_directory(tmp_path, output_dir, config)
    end_time = time.time()

    # Verify conversion
    assert len(stats_list) == num_files
    for stats in stats_list:
        assert isinstance(stats, ConversionStats)
        assert stats.rows_processed == rows_per_file
        assert stats.rows_converted == rows_per_file
        assert not stats.errors
        assert not stats.warnings

    # Check performance
    conversion_time = end_time - start_time
    assert conversion_time < 30.0  # Should complete within 30 seconds


if __name__ == "__main__":
    test_conversion_performance()
