"""Logging functionality for the Parquet Converter."""

import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import numpy as np
from tabulate import tabulate

from .stats import ConversionStats

logger = logging.getLogger(__name__)


class JSONEncoder(json.JSONEncoder):
    """
    JSON encoder that gracefully handles numpy and pathlib objects.

    Examples
    --------
    >>> encoder = JSONEncoder()
    >>> encoder.encode({"value": 1})
    '{"value": 1}'
    """

    def default(self, obj: Any) -> Union[int, float, list, str, Any]:
        """
        Convert numpy-specific types into native Python equivalents.

        Parameters
        ----------
        obj : Any
            Object to encode.

        Returns
        -------
        Union[int, float, list, str, Any]
            Serialized representation compatible with ``json``.

        Examples
        --------
        >>> import numpy as np
        >>> JSONEncoder().default(np.int64(1))
        1
        """
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, Path):
            return str(obj)
        return super().default(obj)


def setup_logging(level: str = "INFO", log_file: Optional[Path] = None, verbose: bool = False) -> None:
    """
    Configure console and file logging for the CLI.

    Parameters
    ----------
    level : str, default='INFO'
        Base logging level for console output.
    log_file : Optional[Path], default=None
        Optional destination file for persistent logs.
    verbose : bool, default=False
        When ``True`` the console handler logs at ``DEBUG`` level.

    Returns
    -------
    None
        Logging handlers are configured globally.

    Examples
    --------
    >>> setup_logging(level="DEBUG")
    """
    # Set log level
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {level}")

    # Create formatters
    console_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    file_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # Set up root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG if verbose else numeric_level)

    # Clear existing handlers
    root_logger.handlers.clear()

    # Add console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(logging.DEBUG if verbose else numeric_level)
    root_logger.addHandler(console_handler)

    # Add file handler if log file is specified
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)

    # Set up package logger
    pkg_logger = logging.getLogger("parquet_converter")
    pkg_logger.setLevel(logging.DEBUG if verbose else numeric_level)
    pkg_logger.propagate = True

    # Add console handler to package logger
    pkg_console_handler = logging.StreamHandler(sys.stdout)
    pkg_console_handler.setFormatter(console_formatter)
    pkg_console_handler.setLevel(logging.DEBUG if verbose else numeric_level)
    pkg_logger.addHandler(pkg_console_handler)

    # Add file handler to package logger if specified
    if log_file:
        pkg_file_handler = logging.FileHandler(log_file)
        pkg_file_handler.setFormatter(file_formatter)
        pkg_file_handler.setLevel(numeric_level)
        pkg_logger.addHandler(pkg_file_handler)


def format_stats_table(stats_list: List[ConversionStats]) -> str:
    """
    Render conversion statistics as a textual table.

    Parameters
    ----------
    stats_list : List[ConversionStats]
        Collection of per-file conversion statistics.

    Returns
    -------
    str
        Rich table describing each conversion.

    Examples
    --------
    >>> stats = ConversionStats("a.csv", "a.csv.parquet", 1, 1, [], [])
    >>> "a.csv" in format_stats_table([stats])
    True
    """
    if not stats_list:
        return "No files were converted."

    # Prepare table data
    headers = ["File", "Rows", "Columns", "Output", "Status"]

    rows = []
    for stats in stats_list:
        status = "Success" if not stats.errors else "Failed"
        rows.append(
            [
                stats.input_file,
                stats.rows,
                stats.columns,
                stats.output_file,
                status,
            ]
        )

    return tabulate(rows, headers=headers, tablefmt="grid")


def save_conversion_report(stats_list: List[ConversionStats], output_dir: Path, config: Dict[str, Any]) -> None:
    """
    Persist conversion statistics to ``conversion_report.json``.

    Parameters
    ----------
    stats_list : List[ConversionStats]
        Collection of conversion statistics.
    output_dir : Path
        Directory where the report should be stored.
    config : Dict[str, Any]
        Configuration dictionary written alongside the summary.

    Returns
    -------
    None
        The report is written to disk as a side effect.

    Examples
    --------
    >>> from pathlib import Path
    >>> tmp_dir = Path("reports")
    >>> tmp_dir.mkdir(exist_ok=True)
    >>> stats = ConversionStats("a.csv", "a.csv.parquet", 1, 1, [], [])
    >>> save_conversion_report([stats], tmp_dir, {})
    >>> (tmp_dir / "conversion_report.json").exists()
    True
    >>> (tmp_dir / "conversion_report.json").unlink()
    >>> tmp_dir.rmdir()
    """
    # Process config to handle Path objects
    processed_config = {}
    for k, v in config.items():
        processed_config[k] = str(v) if isinstance(v, Path) else v

    # Create report data
    report = {
        "timestamp": datetime.now().isoformat(),
        "config": processed_config,
        "summary": {
            "total_files": len(stats_list),
            "successful": sum(1 for s in stats_list if not s.errors),
            "failed": sum(1 for s in stats_list if s.errors),
        },
        "files": [stats.to_dict() for stats in stats_list],
    }

    # Save report
    report_file = output_dir / "conversion_report.json"
    with open(report_file, "w") as f:
        json.dump(report, f, indent=2, cls=JSONEncoder)

    logger.info(f"Saved conversion report to {report_file}")


def log_conversion_summary(stats_list: List[ConversionStats]) -> None:
    """
    Emit a textual summary of the conversion process.

    Parameters
    ----------
    stats_list : List[ConversionStats]
        Collection of conversion statistics.

    Returns
    -------
    None
        The summary is written to the configured logger.

    Examples
    --------
    >>> stats = ConversionStats("a.csv", "a.csv.parquet", 1, 1, [], [])
    >>> log_conversion_summary([stats])
    """
    if not stats_list:
        logger.warning("No files were converted.")
        return

    # Calculate summary statistics
    total_files = len(stats_list)
    successful = sum(1 for s in stats_list if not s.errors)
    failed = total_files - successful
    total_rows = sum(s.rows for s in stats_list)
    total_columns = sum(s.columns for s in stats_list)

    # Log summary
    logger.info("Conversion Summary:")
    logger.info(f"Total files processed: {total_files}")
    logger.info(f"Successfully converted: {successful}")
    logger.info(f"Failed conversions: {failed}")
    logger.info(f"Total rows processed: {total_rows}")
    logger.info(f"Total columns processed: {total_columns}")

    # Log detailed table
    logger.info("\nDetailed Results:")
    logger.info(format_stats_table(stats_list))

    # Log any errors
    for stats in stats_list:
        if stats.errors:
            logger.error(f"Errors in {stats.input_file}:")
            for error in stats.errors:
                logger.error(f"  - {error}")
