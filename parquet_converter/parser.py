"""File parsing functionality for the Parquet Converter."""

import logging
from pathlib import Path
from typing import Dict, Union

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
        config: Configuration dictionary (should contain datetime_formats dict)

    Returns:
        DataFrame with inferred data types
    """
    # Extract datetime format settings safely
    datetime_config = config.get("datetime_formats", {})
    default_dt_format = datetime_config.get("default")
    custom_dt_formats = datetime_config.get("custom", [])
    # Combine default and custom formats for inference attempts
    possible_dt_formats = [default_dt_format] + custom_dt_formats
    possible_dt_formats = [fmt for fmt in possible_dt_formats if fmt]  # Filter out None/empty

    # --- No immediate conversion based on keys like 'default' ---
    # The loop below handles inference for all columns

    for col in df.columns:
        # Skip if dtype is already specified explicitly in config (if provided)
        if col in config.get("dtypes", {}):
            continue

        # Try to infer datetime using the provided formats
        if possible_dt_formats:
            current_col_dtype = df[col].dtype
            # Only attempt datetime conversion if not already datetime
            if not pd.api.types.is_datetime64_any_dtype(current_col_dtype):
                try:
                    # Try converting with pandas' default inference first
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                    # If successful and not all NaT, continue
                    if pd.api.types.is_datetime64_any_dtype(df[col].dtype) and not df[col].isnull().all():
                        continue
                    else:  # Revert if conversion failed or resulted in all NaT
                        df[col] = df[col].astype(current_col_dtype)
                except (ValueError, TypeError):
                    df[col] = df[col].astype(current_col_dtype)  # Revert on error

                # If default pandas conversion didn't work, try specific formats
                if not pd.api.types.is_datetime64_any_dtype(df[col].dtype):
                    for fmt in possible_dt_formats:
                        try:
                            # Attempt conversion with specific format
                            converted_col = pd.to_datetime(df[col], format=fmt, errors="coerce")
                            # If successful and not all NaT, assign and break
                            if not converted_col.isnull().all():
                                df[col] = converted_col
                                break  # Success with this format
                        except (ValueError, TypeError):
                            continue  # Try next format
            # If after all attempts it's a datetime, continue to next column
            if pd.api.types.is_datetime64_any_dtype(df[col].dtype):
                continue

        # Try to infer numeric (existing logic seems okay)
        try:
            # Try integer first
            numeric_series = pd.to_numeric(df[col], errors="coerce")
            # Check if *all* original non-null values were converted successfully
            # And that *all* converted values are not null (meaning no new NaNs introduced)
            original_notna = df[col].notna()
            if numeric_series[original_notna].notna().all():
                # Check if it can be integer
                if numeric_series.dropna().astype("int64").eq(numeric_series.dropna()).all():
                    df[col] = numeric_series.astype("Int64")  # Use nullable Int
                else:
                    df[col] = numeric_series.astype("float64")
                continue
        except (ValueError, TypeError):
            pass

        # Try to infer boolean (existing logic seems okay)
        try:
            # Check potential boolean values, handle actual booleans and NaN correctly
            potential_bools = df[col].dropna().astype(str).str.lower().isin(["true", "false", "1", "0"])
            if potential_bools.all():
                map_dict = {"true": True, "1": True, "false": False, "0": False}
                # Apply map only to strings, preserve existing bools/NaNs
                if pd.api.types.is_string_dtype(df[col]):
                    df[col] = df[col].str.lower().map(map_dict)
                elif df[col].dtype == "object":  # Handle object columns that might mix types
                    df[col] = df[col].apply(lambda x: map_dict.get(str(x).lower()) if pd.notna(x) else x)
                df[col] = df[col].astype("boolean")  # Use nullable Boolean
                continue
        except (AttributeError, TypeError):
            pass

        # Default to string if not already converted and not object (or object handled above)
        if (
            not pd.api.types.is_datetime64_any_dtype(df[col].dtype)
            and not pd.api.types.is_numeric_dtype(df[col].dtype)
            and not pd.api.types.is_bool_dtype(df[col].dtype)
            and df[col].dtype != "object"
        ):  # Avoid converting object if boolean map failed
            df[col] = df[col].astype("string")

    return df
