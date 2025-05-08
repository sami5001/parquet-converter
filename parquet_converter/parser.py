"""File parsing functionality for the Parquet Converter."""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def parse_file(input_path: Union[str, Path], config: Dict) -> pd.DataFrame:
    """Parse a file into a pandas DataFrame.

    Args:
        input_path: Path to input file
        config: Configuration dictionary

    Returns:
        DataFrame containing parsed data

    Raises:
        ValueError: If file type is not supported
    """
    input_path = Path(input_path)
    file_type = input_path.suffix.lower()

    # Parse file based on type
    if file_type == ".csv":
        df = parse_csv(input_path, config.get("csv", {}))
    elif file_type == ".txt":
        df = parse_txt(input_path, config.get("txt", {}))
    else:
        raise ValueError(f"Unsupported file type: {file_type}")

    # Infer data types if enabled
    if config.get("infer_dtypes", True):
        df = infer_dtypes(df, config)

    return df


def parse_csv(input_path: Path, options: Dict) -> pd.DataFrame:
    """Parse a CSV file.

    Args:
        input_path: Path to CSV file
        options: CSV parsing options

    Returns:
        DataFrame containing parsed data
    """
    try:
        df = pd.read_csv(
            input_path,
            sep=options.get("delimiter", ","),
            encoding=options.get("encoding", "utf-8"),
            header=options.get("header", 0),
            names=options.get("column_names"),
            dtype=options.get("dtypes"),
            na_values=options.get("na_values", ["", "NA", "NULL"]),
            skiprows=options.get("skip_rows", 0),
            skipfooter=options.get("skip_footer", 0),
            engine=options.get("engine", "python"),
        )
        logger.debug(f"Successfully parsed CSV file: {input_path}")
        return df
    except Exception as e:
        logger.error(f"Error parsing CSV file {input_path}: {str(e)}")
        raise


def parse_txt(input_path: Path, options: Dict) -> pd.DataFrame:
    """Parse a text file.

    Args:
        input_path: Path to text file
        options: Text file parsing options

    Returns:
        DataFrame containing parsed data
    """
    try:
        df = pd.read_csv(
            input_path,
            sep=options.get("delimiter", r"\s+"),
            encoding=options.get("encoding", "utf-8"),
            header=options.get("header", 0),
            names=options.get("column_names"),
            dtype=options.get("dtypes"),
            na_values=options.get("na_values", ["", "NA", "NULL"]),
            skiprows=options.get("skip_rows", 0),
            skipfooter=options.get("skip_footer", 0),
            engine=options.get("engine", "python"),
        )
        logger.debug(f"Successfully parsed text file: {input_path}")
        return df
    except Exception as e:
        logger.error(f"Error parsing text file {input_path}: {str(e)}")
        raise


def infer_dtypes(df: pd.DataFrame, config: Dict) -> pd.DataFrame:
    """Infer data types for DataFrame columns.

    Args:
        df: Input DataFrame
        config: Configuration dictionary

    Returns:
        DataFrame with inferred data types
    """
    datetime_formats = []
    if "datetime_formats" in config:
        if isinstance(config["datetime_formats"], dict):
            datetime_formats.extend(
                [
                    config["datetime_formats"].get("default", "%Y-%m-%d"),
                    *config["datetime_formats"].get("custom", []),
                ]
            )
        elif isinstance(config["datetime_formats"], list):
            datetime_formats.extend(config["datetime_formats"])

    for col in df.columns:
        # Skip if dtype is already specified
        if col in config.get("dtypes", {}):
            continue

        # Try to infer datetime
        if datetime_formats:
            for fmt in datetime_formats:
                try:
                    df[col] = pd.to_datetime(df[col], format=fmt)
                    break
                except (ValueError, TypeError):
                    continue
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                continue

        # Try to infer numeric
        try:
            # Try integer first
            numeric_series = pd.to_numeric(df[col], errors="coerce")
            if numeric_series.notna().all():
                if numeric_series.astype("int64").eq(numeric_series).all():
                    df[col] = numeric_series.astype("int64")
                else:
                    df[col] = numeric_series.astype("float64")
                continue
        except (ValueError, TypeError):
            pass

        # Try to infer boolean
        try:
            if df[col].isin(["True", "False", "true", "false", "1", "0"]).all():
                df[col] = df[col].map(
                    {
                        "True": True,
                        "true": True,
                        "1": True,
                        "False": False,
                        "false": False,
                        "0": False,
                    }
                )
                continue
        except (AttributeError, TypeError):
            pass

        # Default to string
        df[col] = df[col].astype("string")

    return df
