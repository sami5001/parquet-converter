"""Command-line interface for the Parquet Converter."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import List, Optional

from .analyzer import analyze_directory
from .config import Config, load_config, save_config
from .converter import convert_directory, convert_file
from .logging import log_conversion_summary, save_conversion_report, setup_logging

logger = logging.getLogger(__name__)


def parse_args(args: Optional[List[str]] = None) -> argparse.Namespace:
    """
    Parse command-line arguments for the CLI.

    Parameters
    ----------
    args : Optional[List[str]], default=None
        Optional argument list used for testing. When ``None`` the arguments
        are read from :data:`sys.argv`.

    Returns
    -------
    argparse.Namespace
        Parsed arguments namespace.

    Examples
    --------
    >>> namespace = parse_args(["input.csv", "--mode", "convert"])
    >>> namespace.input_path
    'input.csv'
    """
    parser = argparse.ArgumentParser(description="Convert text files to Parquet format or analyze Parquet assets.")

    parser.add_argument(
        "input_path",
        type=str,
        help="Path to the input file or directory.",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=str,
        help="Output directory (creates a new directory next to input when omitted).",
    )
    parser.add_argument(
        "-c",
        "--config",
        type=str,
        help="Path to configuration file (YAML or JSON).",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose logging.",
    )
    parser.add_argument(
        "--save-config",
        type=str,
        help="Persist the resolved configuration to a file.",
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["convert", "analyze"],
        default="convert",
        help="Choose between conversion and analyzer workflows.",
    )
    parser.add_argument(
        "--report-dir",
        type=str,
        help="Directory where analyzer reports should be written.",
    )

    return parser.parse_args(args)


def _build_runtime_config(config: Config) -> dict:
    """
    Convert the Pydantic config object into a plain dictionary for internal helpers.

    Parameters
    ----------
    config : Config
        Validated configuration object.

    Returns
    -------
    dict
        Dictionary that can be passed to conversion helpers.

    Examples
    --------
    >>> cfg = load_config()
    >>> isinstance(_build_runtime_config(cfg), dict)
    True
    """
    return {
        "csv": config.csv.model_dump(),
        "txt": config.txt.model_dump(),
        "datetime_formats": config.datetime_formats.model_dump(),
        "compression": config.compression,
        "engine": config.engine,
        "sample_rows": config.sample_rows,
        "chunk_size": config.chunk_size,
        "verify_rows": config.verify_rows,
        "profiling_column_limit": config.profiling_column_limit,
    }


def main(args: Optional[List[str]] = None) -> int:
    """
    CLI entry point for both conversion and analyzer workflows.

    Parameters
    ----------
    args : Optional[List[str]], default=None
        Optional argument list primarily used by the test-suite.

    Returns
    -------
    int
        Exit code where ``0`` indicates success.

    Examples
    --------
    >>> main(["--help"])  # doctest: +ELLIPSIS
    0
    """
    try:
        parsed_args = parse_args(args)
        config = load_config(parsed_args.config)

        if parsed_args.output_dir:
            config.output_dir = Path(parsed_args.output_dir)
        elif not config.output_dir:
            input_path = Path(parsed_args.input_path)
            config.output_dir = (input_path.parent if input_path.is_file() else input_path) / "output"

        log_file = Path(config.log_file).resolve() if config.log_file else None
        setup_logging(
            level=config.log_level,
            log_file=log_file,
            verbose=parsed_args.verbose,
        )

        if parsed_args.save_config:
            save_config(config, Path(parsed_args.save_config))

        if parsed_args.mode == "analyze":
            input_path = Path(parsed_args.input_path)
            if not input_path.exists() or not input_path.is_dir():
                logger.error("Analyzer mode requires a directory of parquet files: %s", input_path)
                return 1
            report_dir = (
                Path(parsed_args.report_dir).expanduser() if parsed_args.report_dir else config.analyzer_report_dir
            )
            report_path = analyze_directory(input_path, report_dir)
            logger.info("Analysis complete. Report saved to %s", report_path)
            return 0

        runtime_config = _build_runtime_config(config)
        input_path = Path(parsed_args.input_path)

        if input_path.is_file():
            stats = convert_file(
                input_path,
                config.output_dir,
                runtime_config,
            )
            stats_list = [stats]
        elif input_path.is_dir():
            stats_list = convert_directory(
                input_path,
                config.output_dir,
                runtime_config,
            )
        else:
            logger.error("Input path does not exist: %s", input_path)
            return 1

        log_conversion_summary(stats_list)
        if config.output_dir:
            save_conversion_report(
                stats_list,
                Path(config.output_dir).resolve(),
                config.model_dump(),
            )

        if any(stats.errors for stats in stats_list):
            return 1
        return 0
    except SystemExit:
        return 0
    except Exception as exc:  # pragma: no cover - defensive top-level guard
        logger.exception("An error occurred during conversion: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
