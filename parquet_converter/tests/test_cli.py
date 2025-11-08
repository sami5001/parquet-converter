"""Tests for the command-line interface."""

from pathlib import Path
from typing import List

import pandas as pd
import pytest

from ..cli import main, parse_args


@pytest.fixture
def test_files(tmp_path: Path) -> List[Path]:
    """Create test files for CLI testing."""
    # Create CSV file
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("id,name\n1,Alice\n2,Bob")

    # Create TXT file
    txt_file = tmp_path / "test.txt"
    txt_file.write_text("id\tname\n1\tAlice\n2\tBob")

    return [csv_file, txt_file]


@pytest.fixture
def config_file(tmp_path: Path) -> Path:
    """Create a test configuration file."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
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
  custom: []
log_level: "INFO"
"""
    )
    return config_path


def test_parse_args_input_path() -> None:
    """Test parsing input path argument."""
    args = parse_args(["input.csv"])
    assert args.input_path == "input.csv"
    assert args.output_dir is None
    assert args.config is None
    assert not args.verbose
    assert args.mode == "convert"


def test_parse_args_output_dir() -> None:
    """Test parsing output directory argument."""
    args = parse_args(["input.csv", "-o", "output"])
    assert args.input_path == "input.csv"
    assert args.output_dir == "output"


def test_parse_args_config() -> None:
    """Test parsing configuration file argument."""
    args = parse_args(["input.csv", "-c", "config.yaml"])
    assert args.input_path == "input.csv"
    assert args.config == "config.yaml"


def test_parse_args_verbose() -> None:
    """Test parsing verbose flag."""
    args = parse_args(["input.csv", "-v"])
    assert args.input_path == "input.csv"
    assert args.verbose


def test_parse_args_save_config() -> None:
    """Test parsing save config argument."""
    args = parse_args(["input.csv", "--save-config", "saved_config.yaml"])
    assert args.input_path == "input.csv"
    assert args.save_config == "saved_config.yaml"


def test_main_single_file(test_files: List[Path], tmp_path: Path) -> None:
    """Test converting a single file."""
    input_file = test_files[0]
    output_dir = tmp_path / "output"

    result = main([str(input_file), "-o", str(output_dir)])
    assert result == 0

    output_file = output_dir / f"{input_file.name}.parquet"
    assert output_file.exists()


def test_main_directory(test_files: List[Path], tmp_path: Path) -> None:
    """Test converting a directory of files."""
    input_dir = test_files[0].parent
    output_dir = tmp_path / "output"

    result = main([str(input_dir), "-o", str(output_dir)])
    assert result == 0

    for test_file in test_files:
        output_file = output_dir / f"{test_file.name}.parquet"
        assert output_file.exists()


def test_main_with_config(
    test_files: List[Path],
    config_file: Path,
    tmp_path: Path,
) -> None:
    """Test converting with configuration file."""
    input_file = test_files[0]
    output_dir = tmp_path / "output"

    args = [
        str(input_file),
        "-o",
        str(output_dir),
        "-c",
        str(config_file),
    ]
    result = main(args)
    assert result == 0

    output_file = output_dir / f"{input_file.name}.parquet"
    assert output_file.exists()


def test_main_nonexistent_input() -> None:
    """Test handling of nonexistent input."""
    result = main(["nonexistent.csv"])
    assert result == 1


def test_main_invalid_config(test_files: List[Path]) -> None:
    """Test handling of invalid configuration."""
    result = main([str(test_files[0]), "-c", "nonexistent.yaml"])
    assert result == 1


def test_main_analyze_mode(tmp_path: Path) -> None:
    """Test analyzer workflow."""
    parquet_dir = tmp_path / "parquet_data"
    parquet_dir.mkdir()
    parquet_file = parquet_dir / "sample.parquet"
    pd.DataFrame({"value": [1, 2]}).to_parquet(parquet_file)
    report_dir = tmp_path / "reports"

    result = main(
        [
            str(parquet_dir),
            "--mode",
            "analyze",
            "--report-dir",
            str(report_dir),
        ]
    )
    assert result == 0
    assert (report_dir / "parquet_analysis_report.txt").exists()
