"""Parquet file analysis utilities for the Parquet Converter."""

from __future__ import annotations

import datetime
import io
import os
import random
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple, TypedDict, Union

import humanize
import polars as pl
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table


class NumericColumnStats(TypedDict):
    min: float
    max: float
    mean: float
    median: float
    std_dev: float
    null_count: int
    null_percent: float


class NullCountStats(TypedDict):
    count: int
    percent: float


class UniqueValueStats(TypedDict, total=False):
    count: Optional[int]
    percent: Optional[float]
    most_common: List[Tuple[object, int, float]]


AnalysisResult = Dict[str, Any]

FileLike = Union[str, Path]


def _safe_float(value: Any) -> float:
    """
    Convert potentially nullable numeric values to ``float``.

    Parameters
    ----------
    value : Any
        Numeric-like input retrieved from Polars computations.

    Returns
    -------
    float
        Float representation or ``nan`` when conversion fails.
    """
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("nan")


def scan_parquet_files(input_dir: FileLike, recursive: bool = True) -> List[Path]:
    """
    Discover parquet files within a directory tree.

    Parameters
    ----------
    input_dir : FileLike
        Directory to scan for `.parquet` files.
    recursive : bool, default=True
        Whether to search through subdirectories recursively.

    Returns
    -------
    List[Path]
        Absolute paths to each parquet file that was discovered.

    Examples
    --------
    >>> from pathlib import Path
    >>> files = scan_parquet_files(Path.cwd(), recursive=False)
    >>> isinstance(files, list)
    True
    """
    input_path = Path(input_dir).expanduser().resolve()
    parquet_files: List[Path] = []

    if recursive:
        for root, _, files in os.walk(input_path):
            for file_name in files:
                if file_name.endswith(".parquet"):
                    parquet_files.append(Path(root, file_name))
    else:
        parquet_files.extend(file for file in input_path.glob("*.parquet"))

    return parquet_files


def get_file_size(file_path: FileLike) -> str:
    """
    Represent the size of a file in human readable units.

    Parameters
    ----------
    file_path : FileLike
        File whose size should be reported.

    Returns
    -------
    str
        File size that has been formatted using `humanize.naturalsize`.

    Examples
    --------
    >>> from pathlib import Path
    >>> tmp_file = Path("example_size.bin")
    >>> _ = tmp_file.write_bytes(b"data")
    >>> get_file_size(tmp_file)
    '4 Bytes'
    >>> tmp_file.unlink()
    """
    size_bytes = Path(file_path).stat().st_size
    return humanize.naturalsize(size_bytes)


def get_file_modification_time(file_path: FileLike) -> str:
    """
    Format the last modification time for a file.

    Parameters
    ----------
    file_path : FileLike
        File for which the modification timestamp should be returned.

    Returns
    -------
    str
        Timestamp in the format ``YYYY-MM-DD HH:MM:SS``.

    Examples
    --------
    >>> from pathlib import Path
    >>> tmp_file = Path("example_time.txt")
    >>> _ = tmp_file.write_text("sample")
    >>> mod_time = get_file_modification_time(tmp_file)
    >>> mod_time.count(':') == 2
    True
    >>> tmp_file.unlink()
    """
    mtime = Path(file_path).stat().st_mtime
    return datetime.datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")


def calculate_summary_stats(df: pl.DataFrame) -> Dict[str, NumericColumnStats]:
    """
    Compute summary statistics for numeric columns.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame containing the columns that should be profiled.

    Returns
    -------
    Dict[str, Dict[str, float]]
        Mapping of column name to its numeric summary statistics.

    Examples
    --------
    >>> import polars as pl
    >>> frame = pl.DataFrame({'value': [1, 2, 3]})
    >>> calculate_summary_stats(frame)['value']['min']
    1
    """
    numeric_stats: Dict[str, NumericColumnStats] = {}

    for column_name in df.columns:
        dtype = df[column_name].dtype
        if dtype.is_numeric():
            try:
                stats: NumericColumnStats = {
                    "min": _safe_float(df[column_name].min()),
                    "max": _safe_float(df[column_name].max()),
                    "mean": _safe_float(df[column_name].mean()),
                    "median": _safe_float(df[column_name].median()),
                    "std_dev": _safe_float(df[column_name].std()),
                    "null_count": int(df[column_name].null_count()),
                    "null_percent": round(df[column_name].null_count() / max(df.height, 1) * 100, 2),
                }
                numeric_stats[column_name] = stats
            except pl.exceptions.PolarsError:
                continue

    return numeric_stats


def calculate_null_counts(df: pl.DataFrame) -> Dict[str, NullCountStats]:
    """
    Count null observations for each column.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame that should be profiled for missing values.

    Returns
    -------
    Dict[str, Dict[str, float]]
        Mapping of column name to the absolute and percentage null counts.

    Examples
    --------
    >>> import polars as pl
    >>> frame = pl.DataFrame({'value': [1, None]})
    >>> calculate_null_counts(frame)['value']['count']
    1
    """
    null_stats: Dict[str, NullCountStats] = {}
    for column_name in df.columns:
        null_count = df[column_name].null_count()
        percent = round(null_count / max(df.height, 1) * 100, 2)
        null_stats[column_name] = {"count": null_count, "percent": percent}
    return null_stats


def get_unique_values_info(df: pl.DataFrame) -> Dict[str, UniqueValueStats]:
    """
    Summarize unique value counts per column.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame for which categorical uniqueness should be assessed.

    Returns
    -------
    Dict[str, Dict[str, Union[int, float, List[Tuple[object, int, float]]]]]
        Mapping of column name to uniqueness stats and optional most-common values.

    Examples
    --------
    >>> import polars as pl
    >>> frame = pl.DataFrame({'label': ['a', 'a', 'b']})
    >>> get_unique_values_info(frame)['label']['count']
    2
    """
    unique_stats: Dict[str, UniqueValueStats] = {}

    for column_name in df.columns:
        try:
            unique_count = df[column_name].n_unique()
            percent = round(unique_count / max(df.height, 1) * 100, 2)
            unique_stats[column_name] = {"count": unique_count, "percent": percent}

            if 0 < unique_count <= 20 and df.height > 0:
                try:
                    value_counts = df[column_name].value_counts()
                    top_n = min(5, value_counts.height)
                    most_common: List[Tuple[object, int, float]] = []
                    for idx in range(top_n):
                        value = value_counts[idx, 0]
                        count = int(value_counts[idx, 1])
                        freq_percent = round(count / df.height * 100, 2)
                        most_common.append((value, count, freq_percent))
                    unique_stats[column_name]["most_common"] = most_common
                except pl.exceptions.PolarsError:
                    continue
        except pl.exceptions.PolarsError:
            unique_stats[column_name] = {"count": None, "percent": None}

    return unique_stats


def analyze_parquet_file(file_path: FileLike) -> AnalysisResult:
    """
    Profile a single parquet file and capture descriptive metrics.

    Parameters
    ----------
    file_path : FileLike
        File that should be analyzed.

    Returns
    -------
    Dict[str, object]
        Dictionary containing metadata, summary statistics, and sampled rows.

    Examples
    --------
    >>> import polars as pl
    >>> from pathlib import Path
    >>> tmp_dir = Path("analyzer_examples")
    >>> tmp_dir.mkdir(exist_ok=True)
    >>> tmp_file = tmp_dir / "sample.parquet"
    >>> pl.DataFrame({'value': [1, 2]}).write_parquet(tmp_file)
    >>> analysis = analyze_parquet_file(tmp_file)
    >>> analysis["success"]
    True
    >>> tmp_file.unlink()
    >>> tmp_dir.rmdir()
    """
    file_path = Path(file_path)

    try:
        df = pl.read_parquet(file_path)

        n_rows = df.height
        schema_info = [(column, str(df[column].dtype)) for column in df.columns]

        numeric_stats = calculate_summary_stats(df)
        null_stats = calculate_null_counts(df)
        unique_stats = get_unique_values_info(df)

        if 0 < n_rows <= 10:
            sample_rows = df.rows()
        elif n_rows > 10:
            sample_rows = [df.row(idx) for idx in random.sample(range(n_rows), 10)]
        else:
            sample_rows = []

        analysis: AnalysisResult = {
            "file_path": str(file_path),
            "file_size": get_file_size(file_path),
            "file_mod_time": get_file_modification_time(file_path),
            "n_rows": n_rows,
            "n_cols": df.width,
            "memory_usage": humanize.naturalsize(df.estimated_size()),
            "columns_info": schema_info,
            "numeric_stats": numeric_stats,
            "null_stats": null_stats,
            "unique_stats": unique_stats,
            "sample_rows": sample_rows,
            "first_rows": df.head(min(3, n_rows)).rows() if n_rows else [],
            "last_rows": df.tail(min(3, n_rows)).rows() if n_rows else [],
            "columns": df.columns,
            "success": True,
        }
        return analysis
    except Exception as exc:  # pragma: no cover - formatting branch, still documented
        failure: AnalysisResult = {
            "file_path": str(file_path),
            "error": str(exc),
            "success": False,
        }
        return failure


def format_analysis_report(analyses: Sequence[AnalysisResult], width: int = 150) -> str:
    """
    Convert raw analysis dictionaries into a formatted report string.

    Parameters
    ----------
    analyses : Sequence[AnalysisResult]
        Collection of file-level analysis dictionaries.
    width : int, default=150
        Console width to use when rendering the report.

    Returns
    -------
    str
        Rich-formatted report ready to be written to disk.

    Examples
    --------
    >>> analysis = {'file_path': 'sample.parquet', 'success': False, 'error': 'Missing'}
    >>> formatted = format_analysis_report([analysis], width=80)
    >>> "sample.parquet" in formatted
    True
    """
    output_buffer = io.StringIO()
    console = Console(file=output_buffer, width=width, record=True)

    console.print("[bold cyan]PARQUET FILES ANALYSIS REPORT[/bold cyan]", justify="center")
    console.print(f"Report generated at: {datetime.datetime.now():%Y-%m-%d %H:%M:%S}")
    console.print(f"Total files analyzed: {len(analyses)}\n")

    summary_table = Table(title="Summary of All Parquet Files", box=box.SQUARE)
    summary_table.add_column("#", style="cyan")
    summary_table.add_column("File Name", style="green")
    summary_table.add_column("Rows", justify="right")
    summary_table.add_column("Columns", justify="right")
    summary_table.add_column("Size", justify="right")
    summary_table.add_column("Last Modified", justify="right")

    for idx, analysis in enumerate(analyses, start=1):
        if analysis.get("success"):
            summary_table.add_row(
                str(idx),
                os.path.basename(str(analysis["file_path"])),
                f"{analysis.get('n_rows', 0):,}",
                str(analysis.get("n_cols", "N/A")),
                str(analysis.get("file_size", "N/A")),
                str(analysis.get("file_mod_time", "N/A")),
            )
        else:
            summary_table.add_row(
                str(idx),
                os.path.basename(str(analysis["file_path"])),
                "ERROR",
                "ERROR",
                "ERROR",
                "ERROR",
                style="red",
            )

    console.print(summary_table)
    console.print()

    for idx, analysis in enumerate(analyses, start=1):
        if analysis.get("success"):
            file_info = (
                f"File {idx}: {os.path.basename(str(analysis['file_path']))}\n"
                f"Path: {analysis['file_path']}\n"
                f"Size: {analysis['file_size']} | Last Modified: {analysis['file_mod_time']}\n"
                f"Rows: {analysis['n_rows']:,} | Columns: {analysis['n_cols']} | "
                f"Memory Usage: {analysis['memory_usage']}"
            )
            console.print(Panel(file_info, title="File Information", border_style="green"))

            column_table = Table(title="Columns", box=box.SQUARE)
            column_table.add_column("Column Name", style="cyan")
            column_table.add_column("Data Type", style="magenta")
            column_table.add_column("Unique Values", justify="right")
            column_table.add_column("% Unique", justify="right")
            column_table.add_column("Null Count", justify="right")
            column_table.add_column("% Null", justify="right")

            for column_name, dtype in analysis.get("columns_info", []):
                unique_info = analysis["unique_stats"].get(column_name, {})
                null_info = analysis["null_stats"].get(column_name, {})
                column_table.add_row(
                    column_name,
                    dtype,
                    f"{unique_info.get('count', 'N/A')}",
                    f"{unique_info.get('percent', 'N/A')}%",
                    f"{null_info.get('count', 'N/A')}",
                    f"{null_info.get('percent', 'N/A')}%",
                )
            console.print(column_table)

            numeric_stats = analysis.get("numeric_stats")
            if numeric_stats:
                numeric_table = Table(title="Numeric Column Statistics", box=box.SQUARE)
                numeric_table.add_column("Column", style="cyan")
                numeric_table.add_column("Min", justify="right")
                numeric_table.add_column("Max", justify="right")
                numeric_table.add_column("Mean", justify="right")
                numeric_table.add_column("Median", justify="right")
                numeric_table.add_column("Std Dev", justify="right")
                for column_name, stats in numeric_stats.items():
                    numeric_table.add_row(
                        column_name,
                        str(stats["min"]),
                        str(stats["max"]),
                        str(stats["mean"]),
                        str(stats["median"]),
                        str(stats["std_dev"]),
                    )
                console.print(numeric_table)

            categorical_table = Table(title="Most Common Values (Top 5)", box=box.SQUARE)
            categorical_table.add_column("Column", style="cyan")
            categorical_table.add_column("Value")
            categorical_table.add_column("Count", justify="right")
            categorical_table.add_column("Percentage", justify="right")
            has_categorical_data = False
            for column_name, stats in analysis["unique_stats"].items():
                if "most_common" in stats:
                    has_categorical_data = True
                    for idx_, (value, count, percent) in enumerate(stats["most_common"]):
                        categorical_table.add_row(
                            column_name if idx_ == 0 else "",
                            str(value),
                            f"{count}",
                            f"{percent}%",
                        )
            if has_categorical_data:
                console.print(categorical_table)

            for title, rows in (
                ("First 3 Rows", analysis.get("first_rows", [])),
                ("Last 3 Rows", analysis.get("last_rows", [])),
                ("Sample Rows (Random 10)", analysis.get("sample_rows", [])),
            ):
                if rows:
                    table = Table(title=title, box=box.SQUARE)
                    for column_name in analysis.get("columns", []):
                        table.add_column(column_name, overflow="fold")
                    for row in rows:  # type: ignore[assignment]
                        table.add_row(*[str(value) for value in row])
                    console.print(table)
        else:
            error_info = (
                f"File: {os.path.basename(str(analysis['file_path']))}\n"
                f"Path: {analysis['file_path']}\n"
                f"Error: {analysis.get('error', 'Unknown error')}"
            )
            console.print(Panel(error_info, title="Error Processing File", border_style="red"))

        console.print("\n" + "-" * 100 + "\n")

    return output_buffer.getvalue()


def analyze_directory(input_dir: FileLike, output_dir: Optional[FileLike] = None) -> Path:
    """
    Analyze every parquet file in a directory and emit a text report.

    Parameters
    ----------
    input_dir : FileLike
        Directory that contains parquet files.
    output_dir : Optional[FileLike], default=None
        Directory where the generated report should be written. When omitted the
        report is stored alongside ``input_dir``.

    Returns
    -------
    Path
        Path to the generated report file.

    Examples
    --------
    >>> import polars as pl
    >>> from pathlib import Path
    >>> base = Path("analyzer_reports")
    >>> data_dir = base / "data"
    >>> data_dir.mkdir(parents=True, exist_ok=True)
    >>> file_path = data_dir / "sample.parquet"
    >>> pl.DataFrame({'value': [1]}).write_parquet(file_path)
    >>> report_path = analyze_directory(data_dir, base)
    >>> report_path.exists()
    True
    >>> report_path.unlink()
    >>> file_path.unlink()
    >>> data_dir.rmdir()
    >>> base.rmdir()
    """
    input_path = Path(input_dir).expanduser().resolve()
    if output_dir is not None:
        output_path = Path(output_dir).expanduser().resolve()
    else:
        output_path = input_path / "reports"
    output_path.mkdir(parents=True, exist_ok=True)

    parquet_files = scan_parquet_files(input_path)
    analyses = [analyze_parquet_file(file_path) for file_path in parquet_files]

    report_content = format_analysis_report(analyses)
    report_file = output_path / "parquet_analysis_report.txt"
    report_file.write_text(report_content, encoding="utf-8")

    return report_file
