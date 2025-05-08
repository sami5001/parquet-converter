"""Command-line interface for the Parquet Converter."""

import argparse
import logging
import sys
from pathlib import Path

from .config import Config, load_config, save_config
from .converter import convert_directory, convert_file
from .logging import log_conversion_summary, save_conversion_report, setup_logging

logger = logging.getLogger(__name__)


def parse_args(args=None) -> argparse.Namespace:
    """Parse command line arguments.

    Args:
        args: Optional list of command line arguments. If None, sys.argv is used.

    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(description="Convert text files to Parquet format")
    parser.add_argument("input_path", type=str, help="Path to input file or directory")
    parser.add_argument(
        "-o",
        "--output-dir",
        type=str,
        help="Output directory (default: creates a new directory next to input)",
    )
    parser.add_argument("-c", "--config", type=str, help="Path to configuration file")
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging"
    )
    parser.add_argument(
        "--save-config", type=str, help="Save current configuration to file"
    )
    return parser.parse_args(args)


def main(args=None) -> int:
    """Main entry point for the CLI.

    Args:
        args: Optional list of command line arguments. If None, sys.argv is used.

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    args = parse_args(args)

    try:
        # Load configuration
        config = load_config(args.config)

        # Override config with command line arguments
        if args.output_dir:
            config.output_dir = Path(args.output_dir)
        elif not config.output_dir:
            # Create output directory next to input
            input_path = Path(args.input_path)
            if input_path.is_file():
                config.output_dir = input_path.parent / "output"
            else:
                config.output_dir = input_path / "output"

        # Set up logging
        setup_logging(
            level=config.log_level, log_file=config.log_file, verbose=args.verbose
        )

        # Save configuration if requested
        if args.save_config:
            save_config(config, Path(args.save_config))

        # Process input
        input_path = Path(args.input_path)
        if input_path.is_file():
            stats = convert_file(
                input_path,
                config.output_dir,
                {
                    "csv": config.csv.model_dump(),
                    "txt": config.txt.model_dump(),
                    "datetime_formats": config.datetime_formats.model_dump(),
                    "compression": "snappy",
                },
            )
            stats_list = [stats]
        elif input_path.is_dir():
            stats_list = convert_directory(
                input_path,
                config.output_dir,
                {
                    "csv": config.csv.model_dump(),
                    "txt": config.txt.model_dump(),
                    "datetime_formats": config.datetime_formats.model_dump(),
                    "compression": "snappy",
                },
            )
        else:
            logger.error(f"Input path does not exist: {input_path}")
            return 1

        # Log results
        log_conversion_summary(stats_list)

        # Save report if output directory is specified
        if config.output_dir:
            save_conversion_report(stats_list, config.output_dir, config.model_dump())

        # Check for failures
        if any(stats.errors for stats in stats_list):
            return 1

        return 0
    except Exception as e:
        logger.exception("An error occurred during conversion")
        return 1


if __name__ == "__main__":
    sys.exit(main())
