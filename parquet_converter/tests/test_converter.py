"""Tests for the converter module."""

from pathlib import Path

import pandas as pd
import pytest

from ..converter import convert_directory, convert_file
from ..stats import ConversionStats


@pytest.fixture
def sample_csv_file(tmp_path: Path) -> Path:
    """Create a sample CSV file for testing."""
    file_path = tmp_path / "test.csv"
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    df.to_csv(file_path, index=False)
    return file_path


@pytest.fixture
def sample_txt_file(tmp_path: Path) -> Path:
    """Create a sample TXT file for testing."""
    file_path = tmp_path / "test.txt"
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    df.to_csv(file_path, sep="\t", index=False)
    return file_path


@pytest.fixture
def output_dir(tmp_path: Path) -> Path:
    """Create a temporary output directory."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    return output_dir


@pytest.fixture
def config() -> dict:
    """Create a test configuration matching Config model structure."""
    return {
        "csv": {
            "delimiter": ",",
            "encoding": "utf-8",
            "header": 0,
        },
        "txt": {
            "delimiter": "\t",
            "encoding": "utf-8",
            "header": 0,
        },
        # Match the structure from Config.datetime_formats.model_dump()
        "datetime_formats": {
            "default": "%Y-%m-%d",
            "custom": [],
        },
        "compression": "snappy",
        "engine": "polars",
        "sample_rows": 10,
        "chunk_size": 50,
        "verify_rows": 2,
        "profiling_column_limit": 5,
    }


def test_convert_file_csv(sample_csv_file: Path, output_dir: Path, config: dict) -> None:
    """Test converting a CSV file."""
    stats = convert_file(sample_csv_file, output_dir, config)

    assert isinstance(stats, ConversionStats)
    assert stats.rows_processed == 3
    assert stats.rows_converted == 3
    assert not stats.errors
    assert not stats.warnings

    output_file = output_dir / f"{sample_csv_file.name}.parquet"
    assert output_file.exists()

    df = pd.read_parquet(output_file)
    assert len(df) == 3
    assert list(df.columns) == ["col1", "col2"]


def test_convert_file_txt(sample_txt_file: Path, output_dir: Path, config: dict) -> None:
    """Test converting a TXT file."""
    stats = convert_file(sample_txt_file, output_dir, config)

    assert isinstance(stats, ConversionStats)
    assert stats.rows_processed == 3
    assert stats.rows_converted == 3
    assert not stats.errors
    assert not stats.warnings

    output_file = output_dir / f"{sample_txt_file.name}.parquet"
    assert output_file.exists()

    df = pd.read_parquet(output_file)
    assert len(df) == 3
    assert list(df.columns) == ["col1", "col2"]


def test_convert_file_invalid_format(tmp_path: Path, output_dir: Path, config: dict) -> None:
    """Test converting a file with invalid format."""
    invalid_file = tmp_path / "test.invalid"
    invalid_file.write_text("invalid data")

    stats = convert_file(invalid_file, output_dir, config)

    assert isinstance(stats, ConversionStats)
    assert stats.rows_processed == 0
    assert stats.rows_converted == 0
    assert len(stats.errors) == 1
    assert "Unsupported file type" in stats.errors[0]


def test_convert_directory(tmp_path: Path, output_dir: Path, config: dict) -> None:
    """Test converting a directory of files."""
    # Create test files
    csv_file = tmp_path / "test1.csv"
    txt_file = tmp_path / "test2.txt"
    invalid_file = tmp_path / "test3.invalid"

    # Create sample data
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    df.to_csv(csv_file, index=False)
    df.to_csv(txt_file, sep="\t", index=False)
    invalid_file.write_text("invalid data")

    # Convert directory
    stats_list = convert_directory(tmp_path, output_dir, config)

    # Verify results
    # Only CSV and TXT files should be processed
    assert len(stats_list) == 2
    # Check that all stats are ConversionStats instances
    # fmt: off
    # noqa: E501
    all_stats_are_instances = all(isinstance(stats, ConversionStats) for stats in stats_list)
    # fmt: on
    assert all_stats_are_instances
    assert all(stats.rows_processed == 3 for stats in stats_list)
    assert all(stats.rows_converted == 3 for stats in stats_list)
    assert all(not stats.errors for stats in stats_list)
    assert all(not stats.warnings for stats in stats_list)

    # Check output files
    parquet_file_1 = output_dir / "test1.csv.parquet"
    assert parquet_file_1.exists()
    parquet_file_2 = output_dir / "test2.txt.parquet"
    assert parquet_file_2.exists()
    invalid_parquet_file = output_dir / "test3.invalid.parquet"
    assert not invalid_parquet_file.exists()


def test_convert_directory_empty(tmp_path: Path, output_dir: Path, config: dict) -> None:
    """Test converting an empty directory."""
    stats_list = convert_directory(tmp_path, output_dir, config)
    assert len(stats_list) == 0
