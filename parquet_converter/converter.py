"""Core conversion functionality for the Parquet Converter."""

import logging
from pathlib import Path
from typing import Dict, List, Union

from tqdm import tqdm

from .parser import parse_file
from .stats import ConversionStats

logger = logging.getLogger(__name__)


def convert_file(input_path: Union[str, Path], output_dir: Union[str, Path], config: Dict) -> ConversionStats:
    """Convert a single file to Parquet format.

    Args:
        input_path: Path to input file
        output_dir: Directory to save output file
        config: Configuration dictionary

    Returns:
        ConversionStats object containing conversion statistics
    """
    input_path = Path(input_path)
    output_dir = Path(output_dir)

    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate output path
    output_path = output_dir / f"{input_path.name}.parquet"

    try:
        # Parse input file
        df = parse_file(input_path, config)

        # Save as Parquet
        df.to_parquet(
            output_path,
            compression=config.get("compression", "snappy"),
            index=False,
        )

        # Create stats
        stats = ConversionStats(
            input_file=str(input_path),
            output_file=str(output_path),
            rows_processed=len(df),
            rows_converted=len(df),
            errors=[],
            warnings=[],
        )

        # Add column statistics
        for col in df.columns:
            stats.add_column_stats(
                col,
                {
                    "dtype": str(df[col].dtype),
                    "unique_values": len(df[col].unique()),
                    "null_count": df[col].isna().sum(),
                },
            )

        logger.info(f"Successfully converted {input_path} to {output_path}")
        return stats

    except Exception as e:
        logger.error(f"Error converting {input_path}: {str(e)}")
        return ConversionStats(
            input_file=str(input_path),
            output_file=str(output_path),
            rows_processed=0,
            rows_converted=0,
            errors=[str(e)],
            warnings=[],
        )


def convert_directory(input_dir: Union[str, Path], output_dir: Union[str, Path], config: Dict) -> List[ConversionStats]:
    """Convert all supported files in a directory to Parquet format.

    Args:
        input_dir: Path to input directory
        output_dir: Directory to save output files
        config: Configuration dictionary

    Returns:
        List of ConversionStats objects for each file processed
    """
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)

    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    # Get supported file extensions
    supported_extensions = {
        ".csv": config.get("csv", {}),
        ".txt": config.get("txt", {}),
    }

    # Find all supported files
    input_files: List[Path] = []
    for ext in supported_extensions:
        input_files.extend(input_dir.glob(f"*{ext}"))

    if not input_files:
        logger.warning(f"No supported files found in {input_dir}")
        return []

    # Convert each file
    stats_list = []
    for input_file in tqdm(input_files, desc="Converting files"):
        stats = convert_file(input_file, output_dir, config)
        stats_list.append(stats)

    return stats_list
