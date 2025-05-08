"""Tests for the CLI module."""

import os
import tempfile
from pathlib import Path

import pytest

from ..cli import main, parse_args


def test_parse_args():
    """Test argument parsing."""
    # Test with minimal arguments
    args = parse_args(["input.csv"])
    assert args.input_path == "input.csv"
    assert args.output_dir is None
    assert not args.verbose
    assert args.config is None

    # Test with all arguments
    args = parse_args(
        [
            "input.csv",
            "-o",
            "output_dir",
            "-c",
            "config.yaml",
            "-v",
            "--save-config",
            "saved_config.yaml",
        ]
    )
    assert args.input_path == "input.csv"
    assert args.output_dir == "output_dir"
    assert args.verbose
    assert args.config == "config.yaml"
    assert args.save_config == "saved_config.yaml"


def test_main_single_file(tmp_path):
    """Test main function with a single file."""
    # Create test file
    input_file = tmp_path / "test.csv"
    input_file.write_text("col1,col2\n1,2\n3,4\n")

    # Create output directory
    output_dir = tmp_path / "output"

    # Run main
    exit_code = main([str(input_file), "-o", str(output_dir), "-v"])

    assert exit_code == 0
    assert (output_dir / "test.csv.parquet").exists()


def test_main_directory(tmp_path):
    """Test main function with a directory."""
    # Create test files
    input_dir = tmp_path / "input"
    input_dir.mkdir()

    (input_dir / "test1.csv").write_text("col1,col2\n1,2\n3,4\n")
    (input_dir / "test2.txt").write_text("col1\tcol2\n1\t2\n3\t4\n")

    # Create output directory
    output_dir = tmp_path / "output"

    # Run main
    exit_code = main([str(input_dir), "-o", str(output_dir), "-v"])

    assert exit_code == 0
    assert (output_dir / "test1.csv.parquet").exists()
    assert (output_dir / "test2.txt.parquet").exists()


def test_main_with_config(tmp_path):
    """Test main function with configuration file."""
    # Create test file
    input_file = tmp_path / "test.csv"
    input_file.write_text("col1,col2\n1,2\n3,4\n")

    # Create config file
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        """
csv:
  delimiter: ","
  encoding: "utf-8"
  header: 0
txt:
  delimiter: "\t"
  encoding: "utf-8"
  header: 0
    """
    )

    # Create output directory
    output_dir = tmp_path / "output"

    # Run main
    exit_code = main(
        [str(input_file), "-o", str(output_dir), "-c", str(config_file), "-v"]
    )

    assert exit_code == 0
    assert (output_dir / "test.csv.parquet").exists()


def test_main_error_handling(tmp_path):
    """Test main function error handling."""
    # Test with non-existent input
    exit_code = main(["nonexistent.csv"])
    assert exit_code == 1

    # Test with invalid config file
    input_file = tmp_path / "test.csv"
    input_file.write_text("col1,col2\n1,2\n3,4\n")

    config_file = tmp_path / "invalid.yaml"
    config_file.write_text("invalid: yaml: content")

    exit_code = main([str(input_file), "-c", str(config_file)])
    assert exit_code == 1


def test_main_save_config(tmp_path):
    """Test saving configuration."""
    # Create test file
    input_file = tmp_path / "test.csv"
    input_file.write_text("col1,col2\n1,2\n3,4\n")

    # Create output directory and saved config path
    output_dir = tmp_path / "output"
    saved_config = tmp_path / "saved_config.yaml"

    # Run main
    exit_code = main(
        [str(input_file), "-o", str(output_dir), "--save-config", str(saved_config)]
    )

    assert exit_code == 0
    assert saved_config.exists()
    assert saved_config.read_text()
