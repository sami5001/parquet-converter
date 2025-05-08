"""Tests for the Parquet Converter."""

import json
import logging
from pathlib import Path
from typing import Dict

import pandas as pd
import pytest

from ..converter import convert_directory, convert_file
from ..parser import infer_dtypes, parse_file
from ..stats import ConversionStats

# Configure logging for tests
logging.basicConfig(level=logging.DEBUG)


@pytest.fixture
def test_data_dir(tmp_path: Path) -> Path:
    """Create test data directory with sample files."""
    # Create CSV file
    csv_data = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "date": ["2023-01-01", "2023-01-02", "2023-01-03"],
        }
    )
    csv_path = tmp_path / "test.csv"
    csv_data.to_csv(csv_path, index=False)

    # Create text file
    txt_data = pd.DataFrame(
        {
            "id": [4, 5, 6],
            "name": ["David", "Eve", "Frank"],
            "date": ["2023-01-04", "2023-01-05", "2023-01-06"],
        }
    )
    txt_path = tmp_path / "test.txt"
    txt_data.to_csv(txt_path, sep="\t", index=False)

    return tmp_path


@pytest.fixture
def config() -> Dict:
    """Create test configuration."""
    return {
        "csv": {"delimiter": ",", "encoding": "utf-8", "header": 0},
        "txt": {"delimiter": "\t", "encoding": "utf-8", "header": 0},
        "datetime_formats": ["%Y-%m-%d"],
        "infer_dtypes": True,
        "compression": "snappy",
    }


def test_parse_file_csv(test_data_dir: Path, config: Dict):
    """Test parsing CSV file."""
    input_path = test_data_dir / "test.csv"
    df = parse_file(input_path, config)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert list(df.columns) == ["id", "name", "date"]
    assert df["id"].dtype == "int64"
    assert pd.api.types.is_string_dtype(df["name"])
    assert pd.api.types.is_datetime64_any_dtype(df["date"])


def test_parse_file_txt(test_data_dir: Path, config: Dict):
    """Test parsing text file."""
    input_path = test_data_dir / "test.txt"
    df = parse_file(input_path, config)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert list(df.columns) == ["id", "name", "date"]
    assert df["id"].dtype == "int64"
    assert pd.api.types.is_string_dtype(df["name"])
    assert pd.api.types.is_datetime64_any_dtype(df["date"])


def test_infer_dtypes(config: Dict):
    """Test data type inference."""
    df = pd.DataFrame(
        {
            "int_col": ["1", "2", "3"],
            "float_col": ["1.1", "2.2", "3.3"],
            "date_col": ["2023-01-01", "2023-01-02", "2023-01-03"],
            "bool_col": ["True", "False", "True"],
            "str_col": ["a", "b", "c"],
        }
    )

    result = infer_dtypes(df, config)

    assert pd.api.types.is_integer_dtype(result["int_col"])
    assert pd.api.types.is_float_dtype(result["float_col"])
    assert pd.api.types.is_datetime64_any_dtype(result["date_col"])
    assert pd.api.types.is_bool_dtype(result["bool_col"])
    assert pd.api.types.is_string_dtype(result["str_col"])


def test_convert_file(test_data_dir: Path, config: Dict):
    """Test converting a single file."""
    input_path = test_data_dir / "test.csv"
    output_dir = test_data_dir / "output"

    stats = convert_file(input_path, output_dir, config)

    assert isinstance(stats, ConversionStats)
    assert stats.success
    assert stats.rows_processed == 3
    assert stats.rows_converted == 3
    assert stats.error_count == 0
    assert stats.warning_count == 0

    # Check output file
    output_path = output_dir / "test.csv.parquet"
    assert output_path.exists()

    # Read back and verify
    df = pd.read_parquet(output_path)
    assert len(df) == 3
    assert list(df.columns) == ["id", "name", "date"]


def test_convert_directory(test_data_dir: Path, config: Dict):
    """Test converting a directory of files."""
    output_dir = test_data_dir / "output"

    stats_list = convert_directory(test_data_dir, output_dir, config)

    assert len(stats_list) == 2
    assert all(isinstance(stats, ConversionStats) for stats in stats_list)
    assert all(stats.success for stats in stats_list)
    assert all(stats.rows_processed == 3 for stats in stats_list)
    assert all(stats.rows_converted == 3 for stats in stats_list)

    # Check output files
    assert (output_dir / "test.csv.parquet").exists()
    assert (output_dir / "test.txt.parquet").exists()


def test_convert_file_error(test_data_dir: Path, config: Dict):
    """Test error handling in file conversion."""
    input_path = test_data_dir / "nonexistent.csv"
    output_dir = test_data_dir / "output"

    stats = convert_file(input_path, output_dir, config)

    assert isinstance(stats, ConversionStats)
    assert not stats.success
    assert stats.rows_processed == 0
    assert stats.rows_converted == 0
    assert stats.error_count > 0
    assert stats.warning_count == 0
