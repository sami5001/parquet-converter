"""Conversion workflows for the Parquet Converter."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import humanize
import polars as pl
from tabulate import tabulate
from tqdm import tqdm

from .parser import parse_file
from .stats import ConversionStats

logger = logging.getLogger(__name__)

DEFAULT_SAMPLE_ROWS = 100_000
DEFAULT_CHUNK_SIZE = 500_000
DEFAULT_VERIFY_ROWS = 10
DEFAULT_COLUMN_LIMIT = 25


def convert_file(input_path: Union[str, Path], output_dir: Union[str, Path], config: Dict[str, Any]) -> ConversionStats:
    """
    Convert a single delimited file to parquet format.

    Parameters
    ----------
    input_path : Union[str, Path]
        Source file that should be converted.
    output_dir : Union[str, Path]
        Directory where the parquet file should be written.
    config : Dict[str, Any]
        Runtime configuration dictionary produced by :func:`parquet_converter.config.load_config`.

    Returns
    -------
    ConversionStats
        Structured statistics describing the conversion outcome.

    Examples
    --------
    >>> from pathlib import Path
    >>> import pandas as pd
    >>> temp_dir = Path("converter_examples")
    >>> temp_dir.mkdir(exist_ok=True)
    >>> csv_file = temp_dir / "example.csv"
    >>> pd.DataFrame({'value': [1]}).to_csv(csv_file, index=False)
    >>> stats = convert_file(
    ...     csv_file,
    ...     temp_dir,
    ...     {
    ...         'csv': {'delimiter': ',', 'encoding': 'utf-8', 'header': 0},
    ...         'txt': {'delimiter': '\\t', 'encoding': 'utf-8', 'header': 0},
    ...         'engine': 'pandas',
    ...         'compression': 'snappy',
    ...     },
    ... )
    >>> stats.success
    True
    >>> (temp_dir / "example.csv.parquet").exists()
    True
    >>> (temp_dir / "example.csv.parquet").unlink()
    >>> csv_file.unlink()
    >>> temp_dir.rmdir()
    """
    input_path = Path(input_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    defaults = {
        "sample_rows": DEFAULT_SAMPLE_ROWS,
        "chunk_size": DEFAULT_CHUNK_SIZE,
        "verify_rows": DEFAULT_VERIFY_ROWS,
        "profiling_column_limit": DEFAULT_COLUMN_LIMIT,
        "engine": "polars",
    }
    runtime_config = {**defaults, **config}
    engine = runtime_config.get("engine", "polars").lower()

    if engine == "pandas":
        return _convert_with_pandas(input_path, output_dir, runtime_config)

    return _convert_with_polars(input_path, output_dir, runtime_config)


def convert_directory(input_dir: Union[str, Path], output_dir: Union[str, Path], config: Dict[str, Any]) -> List[ConversionStats]:
    """
    Convert every supported file within a directory.

    Parameters
    ----------
    input_dir : Union[str, Path]
        Directory that contains delimited files.
    output_dir : Union[str, Path]
        Destination directory where parquet files should be written.
    config : Dict[str, Any]
        Runtime configuration dictionary shared by :func:`convert_file`.

    Returns
    -------
    List[ConversionStats]
        Per-file statistics for every processed file.

    Examples
    --------
    >>> from pathlib import Path
    >>> import pandas as pd
    >>> temp_dir = Path("converter_dir")
    >>> temp_dir.mkdir(exist_ok=True)
    >>> csv_file = temp_dir / "batch.csv"
    >>> pd.DataFrame({'value': [1]}).to_csv(csv_file, index=False)
    >>> stats_list = convert_directory(
    ...     temp_dir,
    ...     temp_dir / "output",
    ...     {
    ...         'csv': {'delimiter': ',', 'encoding': 'utf-8', 'header': 0},
    ...         'txt': {'delimiter': '\\t', 'encoding': 'utf-8', 'header': 0},
    ...         'engine': 'pandas',
    ...         'compression': 'snappy',
    ...     },
    ... )
    >>> [stat.success for stat in stats_list]
    [True]
    >>> (temp_dir / "output" / "batch.csv.parquet").exists()
    True
    >>> (temp_dir / "output" / "batch.csv.parquet").unlink()
    >>> csv_file.unlink()
    >>> (temp_dir / "output").rmdir()
    >>> temp_dir.rmdir()
    """
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    supported_extensions = {
        ".csv": config.get("csv", {}),
        ".txt": config.get("txt", {}),
    }

    input_files: List[Path] = []
    for extension in supported_extensions:
        input_files.extend(input_dir.glob(f"*{extension}"))

    if not input_files:
        logger.warning("No supported files found in %s", input_dir)
        return []

    stats_list: List[ConversionStats] = []
    for file_path in tqdm(input_files, desc="Converting files"):
        stats = convert_file(file_path, output_dir, config)
        stats_list.append(stats)

    return stats_list


def _convert_with_polars(input_path: Path, output_dir: Path, config: Dict[str, Any]) -> ConversionStats:
    """
    Run the Polars-based conversion workflow for a single file.

    Parameters
    ----------
    input_path : Path
        Source file path.
    output_dir : Path
        Destination directory where the parquet file should be stored.
    config : Dict[str, Any]
        Runtime configuration dictionary.

    Returns
    -------
    ConversionStats
        Conversion statistics populated from the streaming pipeline.

    Examples
    --------
    >>> from pathlib import Path
    >>> import pandas as pd
    >>> tmp_dir = Path("polars_convert")
    >>> tmp_dir.mkdir(exist_ok=True)
    >>> txt_file = tmp_dir / "example.txt"
    >>> pd.DataFrame({'value': [1]}).to_csv(txt_file, sep='\\t', index=False)
    >>> stats = _convert_with_polars(
    ...     txt_file,
    ...     tmp_dir,
    ...     {
    ...         'csv': {'delimiter': ',', 'encoding': 'utf-8', 'header': 0},
    ...         'txt': {'delimiter': '\\t', 'encoding': 'utf-8', 'header': 0},
    ...         'compression': 'snappy',
    ...         'sample_rows': 10,
    ...         'chunk_size': 50,
    ...         'verify_rows': 5,
    ...         'profiling_column_limit': 5,
    ...     },
    ... )
    >>> stats.success
    True
    >>> (tmp_dir / "example.txt.parquet").exists()
    True
    >>> (tmp_dir / "example.txt.parquet").unlink()
    >>> txt_file.unlink()
    >>> tmp_dir.rmdir()
    """
    output_path = output_dir / f"{input_path.name}.parquet"
    try:
        options = _resolve_file_options(input_path, config)
    except ValueError as exc:
        return ConversionStats(
            input_file=str(input_path),
            output_file=str(output_path),
            rows_processed=0,
            rows_converted=0,
            errors=[str(exc)],
            warnings=[],
        )

    schema = _analyze_sample_with_polars(input_path, options, config["sample_rows"])
    success, total_rows, elapsed = _stream_polars_conversion(
        input_path,
        output_path,
        options,
        schema,
        config.get("compression", "snappy"),
        config["chunk_size"],
    )

    if not success:
        return ConversionStats(
            input_file=str(input_path),
            output_file=str(output_path),
            rows_processed=0,
            rows_converted=0,
            errors=["Conversion failed during streaming stage."],
            warnings=[],
        )

    _verify_conversion(output_path, input_path, config["verify_rows"])
    column_stats = _collect_polars_column_stats(output_path, config["profiling_column_limit"])

    stats = ConversionStats(
        input_file=str(input_path),
        output_file=str(output_path),
        rows_processed=total_rows,
        rows_converted=total_rows,
        errors=[],
        warnings=[],
    )
    for column_name, column_info in column_stats.items():
        stats.add_column_stats(column_name, column_info)

    logger.info(
        "Successfully converted %s to %s in %.2f seconds",
        input_path,
        output_path,
        elapsed,
    )
    return stats


def _convert_with_pandas(input_path: Path, output_dir: Path, config: Dict[str, Any]) -> ConversionStats:
    """
    Convert a file using the legacy pandas-based workflow.

    Parameters
    ----------
    input_path : Path
        Source file path.
    output_dir : Path
        Destination directory.
    config : Dict[str, Any]
        Runtime configuration dictionary.

    Returns
    -------
    ConversionStats
        Conversion metadata for the pandas path.

    Examples
    --------
    >>> from pathlib import Path
    >>> import pandas as pd
    >>> tmp_dir = Path("pandas_convert")
    >>> tmp_dir.mkdir(exist_ok=True)
    >>> csv_file = tmp_dir / "legacy.csv"
    >>> pd.DataFrame({'value': [1]}).to_csv(csv_file, index=False)
    >>> stats = _convert_with_pandas(
    ...     csv_file,
    ...     tmp_dir,
    ...     {
    ...         'csv': {'delimiter': ',', 'encoding': 'utf-8', 'header': 0},
    ...         'txt': {'delimiter': '\\t', 'encoding': 'utf-8', 'header': 0},
    ...         'compression': 'snappy',
    ...     },
    ... )
    >>> stats.success
    True
    >>> (tmp_dir / "legacy.csv.parquet").exists()
    True
    >>> (tmp_dir / "legacy.csv.parquet").unlink()
    >>> csv_file.unlink()
    >>> tmp_dir.rmdir()
    """
    output_path = output_dir / f"{input_path.name}.parquet"
    try:
        df = parse_file(input_path, config)
        df.to_parquet(
            output_path,
            compression=config.get("compression", "snappy"),
            index=False,
        )

        stats = ConversionStats(
            input_file=str(input_path),
            output_file=str(output_path),
            rows_processed=len(df),
            rows_converted=len(df),
            errors=[],
            warnings=[],
        )
        for column in df.columns:
            stats.add_column_stats(
                column,
                {
                    "dtype": str(df[column].dtype),
                    "unique_values": len(df[column].unique()),
                    "null_count": df[column].isna().sum(),
                },
            )
        return stats
    except Exception as exc:  # pragma: no cover - surfaced through tests
        logger.error("Error converting %s: %s", input_path, exc)
        return ConversionStats(
            input_file=str(input_path),
            output_file=str(output_path),
            rows_processed=0,
            rows_converted=0,
            errors=[str(exc)],
            warnings=[],
        )


def _resolve_file_options(input_path: Path, config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Select the parsing options for the current file based on its suffix.

    Parameters
    ----------
    input_path : Path
        File being processed.
    config : Dict[str, Any]
        Runtime configuration dictionary.

    Returns
    -------
    Dict[str, Any]
        File-type-specific parsing options.

    Examples
    --------
    >>> from pathlib import Path
    >>> options = _resolve_file_options(Path("file.csv"), {'csv': {'delimiter': ','}, 'txt': {}})
    >>> options['delimiter']
    ','
    """
    suffix = input_path.suffix.lower()
    if suffix == ".csv":
        return config.get("csv", {})
    if suffix == ".txt":
        return config.get("txt", {})
    raise ValueError(f"Unsupported file type: {suffix}")


def _build_polars_csv_kwargs(options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Translate parser options into Polars keyword arguments.

    Parameters
    ----------
    options : Dict[str, Any]
        User-specified parsing options.

    Returns
    -------
    Dict[str, Any]
        Keyword arguments that can be passed to :func:`polars.scan_csv`.

    Examples
    --------
    >>> kwargs = _build_polars_csv_kwargs({'delimiter': ';', 'header': 0})
    >>> kwargs['separator']
    ';'
    """
    has_header = options.get("header", 0) is not None
    return {
        "separator": options.get("delimiter", ","),
        "encoding": _normalize_polars_encoding(options.get("encoding", "utf-8")),
        "has_header": has_header,
        "new_columns": options.get("column_names"),
        "null_values": options.get("na_values"),
        "skip_rows": options.get("skip_rows", 0),
        "ignore_errors": True,
        "n_rows": None,
        "infer_schema_length": min(options.get("infer_schema_length", DEFAULT_SAMPLE_ROWS), DEFAULT_SAMPLE_ROWS),
        "try_parse_dates": True,
    }


def _normalize_polars_encoding(value: str) -> str:
    """
    Normalize user-provided encoding names to the Polars vocabulary.

    Parameters
    ----------
    value : str
        Encoding string such as ``utf-8`` or ``utf8-lossy``.

    Returns
    -------
    str
        Encoding label accepted by :func:`polars.scan_csv`.

    Examples
    --------
    >>> _normalize_polars_encoding("utf-8")
    'utf8'
    """
    normalized = value.lower()
    if normalized == "utf-8":
        return "utf8"
    if normalized not in {"utf8", "utf8-lossy"}:
        return "utf8"
    return normalized


def _analyze_sample_with_polars(input_path: Path, options: Dict[str, Any], sample_rows: int) -> Optional[Dict[str, pl.DataType]]:
    """
    Infer a schema by sampling the source file with Polars.

    Parameters
    ----------
    input_path : Path
        Source file path.
    options : Dict[str, Any]
        File parsing options.
    sample_rows : int
        Number of rows to sample during inference.

    Returns
    -------
    Optional[Dict[str, pl.DataType]]
        Mapping of column name to Polars dtype when inference succeeds.

    Examples
    --------
    >>> from pathlib import Path
    >>> import pandas as pd
    >>> tmp = Path("sample.csv")
    >>> pd.DataFrame({'value': [1]}).to_csv(tmp, index=False)
    >>> schema = _analyze_sample_with_polars(
    ...     tmp,
    ...     {'delimiter': ',', 'encoding': 'utf-8', 'header': 0},
    ...     5,
    ... )
    >>> 'value' in schema
    True
    >>> tmp.unlink()
    """
    try:
        kwargs = _build_polars_csv_kwargs(options)
        sample_df = pl.read_csv(
            input_path,
            separator=kwargs["separator"],
            n_rows=sample_rows,
            has_header=kwargs["has_header"],
            encoding=kwargs["encoding"],
            null_values=kwargs["null_values"],
            try_parse_dates=True,
        )
        schema = sample_df.schema
        table = tabulate(
            [(column, str(dtype)) for column, dtype in schema.items()],
            headers=["Column Name", "Inferred Data Type"],
            tablefmt="grid",
        )
        logger.info("Sample analysis for %s rows:\n%s", sample_rows, table)
        return schema
    except Exception as exc:  # pragma: no cover - logging aid
        logger.warning("Failed to analyze sample for %s: %s", input_path, exc)
        return None


def _stream_polars_conversion(
    input_path: Path,
    output_path: Path,
    options: Dict[str, Any],
    schema: Optional[Dict[str, pl.DataType]],
    compression: str,
    chunk_size: int,
) -> Tuple[bool, int, float]:
    """
    Execute the streaming conversion using :mod:`polars`.

    Parameters
    ----------
    input_path : Path
        Source file that should be converted.
    output_path : Path
        Destination parquet path.
    options : Dict[str, Any]
        Parsing options.
    schema : Optional[Dict[str, pl.DataType]]
        Schema overrides inferred from the sample stage.
    compression : str
        Compression codec for the parquet file.
    chunk_size : int
        Informational chunk size used for logging.

    Returns
    -------
    Tuple[bool, int, float]
        ``(success, total_rows, elapsed_seconds)`` tuple describing the conversion.

    Examples
    --------
    >>> from pathlib import Path
    >>> import pandas as pd
    >>> src = Path("stream.csv")
    >>> dest = Path("stream.csv.parquet")
    >>> pd.DataFrame({'value': [1]}).to_csv(src, index=False)
    >>> success, total_rows, _ = _stream_polars_conversion(
    ...     src,
    ...     dest,
    ...     {'delimiter': ',', 'encoding': 'utf-8', 'header': 0},
    ...     None,
    ...     'snappy',
    ...     1000,
    ... )
    >>> (success, total_rows)
    (True, 1)
    >>> dest.unlink()
    >>> src.unlink()
    """
    logger.info(
        "Starting streaming conversion for %s (chunk size hint: %s rows)",
        input_path,
        humanize.intcomma(chunk_size),
    )
    start_time = time.time()
    try:
        kwargs = _build_polars_csv_kwargs(options)
        if schema:
            kwargs["schema_overrides"] = schema

        lazy_frame = pl.scan_csv(input_path, **kwargs)
        lazy_frame.sink_parquet(output_path, compression=compression)

        total_rows = (
            pl.scan_parquet(output_path)
            .select(pl.len().alias("rows"))
            .collect()["rows"][0]
        )
        elapsed = time.time() - start_time
        logger.info(
            "Streaming conversion finished in %.2f seconds (%s rows)",
            elapsed,
            humanize.intcomma(int(total_rows)),
        )
        return True, int(total_rows), elapsed
    except Exception as exc:  # pragma: no cover - error path
        logger.error("Streaming conversion failed for %s: %s", input_path, exc)
        return False, 0, 0.0


def _collect_polars_column_stats(output_path: Path, column_limit: int) -> Dict[str, Dict[str, Optional[int]]]:
    """
    Collect lightweight column statistics from the generated parquet file.

    Parameters
    ----------
    output_path : Path
        Path to the parquet file.
    column_limit : int
        Maximum number of columns for which statistics should be computed.

    Returns
    -------
    Dict[str, Dict[str, Optional[int]]]
        Column statistics keyed by column name.

    Examples
    --------
    >>> from pathlib import Path
    >>> import polars as pl
    >>> dest = Path("stats.parquet")
    >>> pl.DataFrame({'value': [1, 1, None]}).write_parquet(dest)
    >>> stats = _collect_polars_column_stats(dest, 5)
    >>> stats['value']['null_count']
    1
    >>> dest.unlink()
    """
    lazy_frame = pl.scan_parquet(output_path)
    schema = lazy_frame.collect_schema()
    columns = list(schema.keys())
    profiled_columns = columns[:column_limit]

    exprs = []
    for column in profiled_columns:
        exprs.append(pl.col(column).n_unique().alias(f"{column}__unique"))
        exprs.append(pl.col(column).null_count().alias(f"{column}__null"))

    stats: Dict[str, Dict[str, Optional[int]]] = {}
    result = lazy_frame.select(exprs).collect() if exprs else None

    for column in profiled_columns:
        unique_key = f"{column}__unique"
        null_key = f"{column}__null"
        unique_value: Optional[int] = None
        null_value: Optional[int] = None
        if result is not None:
            unique_value = int(result[unique_key][0])
            null_value = int(result[null_key][0])
        stats[column] = {
            "dtype": str(schema[column]),
            "unique_values": unique_value,
            "null_count": null_value,
        }

    for column in columns[column_limit:]:
        stats[column] = {
            "dtype": str(schema[column]),
            "unique_values": None,
            "null_count": None,
        }

    return stats


def _verify_conversion(output_path: Path, source_path: Path, verify_rows: int) -> None:
    """
    Emit verification logs for the converted parquet file.

    Parameters
    ----------
    output_path : Path
        Path to the generated parquet file.
    source_path : Path
        Original input file path for reference.
    verify_rows : int
        Number of rows to fetch for preview output.

    Returns
    -------
    None
        The verification information is logged for the user.

    Examples
    --------
    >>> import polars as pl
    >>> from pathlib import Path
    >>> src = Path("verify.parquet")
    >>> pl.DataFrame({'value': [1]}).write_parquet(src)
    >>> _verify_conversion(src, src, 1)
    >>> src.unlink()
    """
    logger.info("Verifying parquet output for %s", output_path)
    try:
        lazy_df = pl.scan_parquet(output_path)
        schema = lazy_df.collect_schema()
        head_df = (
            lazy_df.head(verify_rows).collect()
            if verify_rows > 0
            else pl.DataFrame()
        )
        row_count = int(lazy_df.select(pl.len().alias("rows")).collect()["rows"][0])
        col_count = len(schema)

        file_info = [
            ["Source File", str(source_path)],
            ["Parquet File", str(output_path)],
            ["Parquet Size", humanize.naturalsize(output_path.stat().st_size)],
            ["Source Size", humanize.naturalsize(source_path.stat().st_size)],
            ["Compression Ratio", f"{output_path.stat().st_size / max(source_path.stat().st_size, 1):.2f}"],
            ["Rows", f"{row_count:,}"],
            ["Columns", f"{col_count:,}"],
        ]
        logger.info("File information:\n%s", tabulate(file_info, tablefmt="grid"))

        if verify_rows > 0 and head_df.height:
            logger.info("Sample rows (first %s):\n%s", verify_rows, head_df)
    except Exception as exc:  # pragma: no cover - verification is optional
        logger.warning("Unable to verify parquet output %s: %s", output_path, exc)
