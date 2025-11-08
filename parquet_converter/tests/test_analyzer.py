"""Test the analyzer module."""

from pathlib import Path

import polars as pl

from .. import analyzer


def _write_sample_parquet(file_path: Path) -> None:
    """Helper to write a tiny parquet file for testing."""
    frame = pl.DataFrame({"value": [1, 2, None], "label": ["a", "b", "b"]})
    frame.write_parquet(file_path)


def test_scan_parquet_files(tmp_path: Path) -> None:
    """Ensure parquet discovery works recursively."""
    data_dir = tmp_path / "data"
    nested_dir = data_dir / "nested"
    nested_dir.mkdir(parents=True)
    file_1 = data_dir / "sample.parquet"
    file_2 = nested_dir / "another.parquet"
    _write_sample_parquet(file_1)
    _write_sample_parquet(file_2)

    results = analyzer.scan_parquet_files(data_dir)
    assert file_1 in results
    assert file_2 in results


def test_analyze_parquet_file_success(tmp_path: Path) -> None:
    """Confirm analyze_parquet_file captures metadata."""
    parquet_file = tmp_path / "sample.parquet"
    _write_sample_parquet(parquet_file)

    analysis = analyzer.analyze_parquet_file(parquet_file)
    assert analysis["success"]
    assert analysis["n_rows"] == 3
    assert analysis["n_cols"] == 2
    assert analysis["numeric_stats"]["value"]["null_count"] == 1


def test_format_analysis_report(tmp_path: Path) -> None:
    """Validate the formatted report contains file names."""
    parquet_file = tmp_path / "sample.parquet"
    _write_sample_parquet(parquet_file)
    analysis = analyzer.analyze_parquet_file(parquet_file)

    report = analyzer.format_analysis_report([analysis], width=80)
    assert "PARQUET FILES ANALYSIS REPORT" in report
    assert "sample.parquet" in report


def test_analyze_directory_creates_report(tmp_path: Path) -> None:
    """Ensure analyze_directory writes a report file."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    parquet_file = data_dir / "sample.parquet"
    _write_sample_parquet(parquet_file)

    report_dir = tmp_path / "reports"
    report_path = analyzer.analyze_directory(data_dir, report_dir)
    assert report_path.exists()
    content = report_path.read_text()
    assert "sample.parquet" in content
