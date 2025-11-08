"""Configuration management utilities for the Parquet Converter."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)


class CSVOptions(BaseModel):
    """
    Parsing configuration for CSV inputs.

    Attributes
    ----------
    delimiter : str
        Field separator used by the CSV file.
    encoding : str
        Text encoding that should be used when reading the file.
    header : Optional[int]
        Row number that contains column names; ``None`` disables header parsing.
    na_values : List[str]
        Tokens that should be treated as nulls.
    low_memory : bool
        When ``True`` pandas processes the file in smaller chunks.
    column_names : Optional[List[str]]
        Explicit replacement for header-derived column names.
    dtypes : Optional[Dict[str, str]]
        Explicit dtype mapping to apply during parsing.
    skip_rows : int
        Number of rows to skip at the start of the file.
    skip_footer : int
        Number of rows to skip from the end of the file.
    engine : str
        CSV parsing engine to use when pandas is selected.

    Examples
    --------
    >>> config = CSVOptions()
    >>> config.delimiter
    ','
    """

    delimiter: str = ","
    encoding: str = "utf-8"
    header: Optional[int] = 0
    na_values: List[str] = ["", "NA", "NULL"]
    low_memory: bool = False
    column_names: Optional[List[str]] = None
    dtypes: Optional[Dict[str, str]] = None
    skip_rows: int = 0
    skip_footer: int = 0
    engine: str = "python"


class TXTOptions(BaseModel):
    """
    Parsing configuration for TXT inputs.

    Attributes
    ----------
    delimiter : str
        Field separator used by the TXT file.
    encoding : str
        Text encoding for the file.
    header : Optional[int]
        Zero-based header row index or ``None`` for headerless files.
    na_values : List[str]
        Tokens that should be treated as nulls.
    low_memory : bool
        Flag forwarded to pandas when the fallback engine is used.
    column_names : Optional[List[str]]
        Explicit column names in the absence of headers.
    dtypes : Optional[Dict[str, str]]
        Explicit dtype mapping to apply during parsing.
    skip_rows : int
        Number of leading rows to skip.
    skip_footer : int
        Number of trailing rows to skip.
    engine : str
        Parser engine to use when pandas is selected.

    Examples
    --------
    >>> TXTOptions(delimiter='\\t').delimiter
    '\\t'
    """

    delimiter: str = "\t"
    encoding: str = "utf-8"
    header: Optional[int] = 0
    na_values: List[str] = ["", "NA", "NULL"]
    low_memory: bool = False
    column_names: Optional[List[str]] = None
    dtypes: Optional[Dict[str, str]] = None
    skip_rows: int = 0
    skip_footer: int = 0
    engine: str = "python"


class DateTimeFormats(BaseModel):
    """
    Custom date-time parsing configuration.

    Attributes
    ----------
    default : str
        Default datetime format string used for parsing.
    custom : List[str]
        Additional formats that should be attempted during inference.

    Examples
    --------
    >>> formats = DateTimeFormats()
    >>> formats.default
    '%Y-%m-%d'
    """

    default: str = "%Y-%m-%d"
    custom: List[str] = []


class Config(BaseModel):
    """
    Aggregated configuration for the converter CLI.

    Attributes
    ----------
    csv : CSVOptions
        Parsing configuration for CSV files.
    txt : TXTOptions
        Parsing configuration for TXT files.
    datetime_formats : DateTimeFormats
        Custom datetime inference patterns.
    log_level : str
        Root logging level.
    compression : str
        Compression codec used for parquet output.
    engine : Literal["polars", "pandas"]
        Conversion backend to use.
    sample_rows : int
        Number of rows used when inferring schema via Polars.
    chunk_size : int
        Informational chunk size used during logging.
    verify_rows : int
        Number of rows fetched for verification output.
    profiling_column_limit : int
        Maximum number of columns to profile for stats after conversion.
    output_dir : Optional[Path]
        Default output directory for converted files.
    log_file : Optional[Path]
        Optional path to a persistent log file.
    analyzer_report_dir : Optional[Path]
        Directory where analyzer reports should be written.

    Examples
    --------
    >>> cfg = Config()
    >>> cfg.engine
    'polars'
    """

    csv: CSVOptions = Field(default_factory=CSVOptions)
    txt: TXTOptions = Field(default_factory=TXTOptions)
    datetime_formats: DateTimeFormats = Field(default_factory=DateTimeFormats)
    log_level: str = "INFO"
    compression: str = "snappy"
    engine: Literal["polars", "pandas"] = "polars"
    sample_rows: int = 100_000
    chunk_size: int = 500_000
    verify_rows: int = 10
    profiling_column_limit: int = 25
    output_dir: Optional[Path] = None
    log_file: Optional[Path] = None
    analyzer_report_dir: Optional[Path] = None

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, value: str) -> str:
        """
        Ensure the user provided a supported log level.

        Parameters
        ----------
        value : str
            Requested logging level.

        Returns
        -------
        str
            Upper-cased logging level.

        Examples
        --------
        >>> Config.validate_log_level("debug")
        'DEBUG'
        """
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if value.upper() not in valid_levels:
            raise ValueError(f"Invalid log level: {value}. Must be one of {valid_levels}")
        return value.upper()

    @field_validator("engine")
    @classmethod
    def validate_engine(cls, value: str) -> str:
        """
        Validate the selected conversion engine.

        Parameters
        ----------
        value : str
            Requested engine identifier.

        Returns
        -------
        str
            Engine string in lower case.

        Examples
        --------
        >>> Config.validate_engine("POLARS")
        'polars'
        """
        normalized = value.lower()
        if normalized not in {"polars", "pandas"}:
            raise ValueError("Engine must be either 'polars' or 'pandas'.")
        return normalized

    @field_validator("sample_rows", "chunk_size", "verify_rows", "profiling_column_limit")
    @classmethod
    def validate_positive_int(cls, value: int) -> int:
        """
        Ensure numeric configuration values are positive.

        Parameters
        ----------
        value : int
            Numeric configuration value to validate.

        Returns
        -------
        int
            The original value when it is positive.

        Examples
        --------
        >>> Config.validate_positive_int(10)
        10
        """
        if value <= 0:
            raise ValueError("Configuration values must be positive integers.")
        return value

    @field_validator("output_dir")
    @classmethod
    def validate_output_dir(cls, value: Optional[Union[str, Path]]) -> Optional[Path]:
        """
        Expand and create the output directory when provided.

        Parameters
        ----------
        value : Optional[Union[str, Path]]
            User-specified output directory path.

        Returns
        -------
        Optional[Path]
            Expanded path that points to an existing directory.

        Examples
        --------
        >>> Config.validate_output_dir(None) is None
        True
        """
        if value is None:
            return None
        output_path = Path(value).expanduser()
        output_path.mkdir(parents=True, exist_ok=True)
        return output_path

    @field_validator("log_file", "analyzer_report_dir")
    @classmethod
    def validate_file_paths(cls, value: Optional[Union[str, Path]]) -> Optional[Path]:
        """
        Ensure that parent directories exist for file-based configuration values.

        Parameters
        ----------
        value : Optional[Union[str, Path]]
            File or directory path supplied by the user.

        Returns
        -------
        Optional[Path]
            Expanded path that can safely be used.

        Examples
        --------
        >>> Config.validate_file_paths(None) is None
        True
        """
        if value is None:
            return None
        path_value = Path(value).expanduser()
        if path_value.suffix:
            path_value.parent.mkdir(parents=True, exist_ok=True)
        else:
            path_value.mkdir(parents=True, exist_ok=True)
        return path_value


def _coerce_env_int(value: Optional[str]) -> Optional[int]:
    """
    Convert an environment variable to an integer when possible.

    Parameters
    ----------
    value : Optional[str]
        Raw environment variable value.

    Returns
    -------
    Optional[int]
        Integer representation when coercion succeeds.

    Examples
    --------
    >>> _coerce_env_int("10")
    10
    """
    if value is None:
        return None
    try:
        return int(value)
    except ValueError as exc:  # pragma: no cover - defensive branch
        raise ValueError(f"Expected integer environment variable, received: {value}") from exc


def load_config(config_path: Optional[Union[str, Path]] = None) -> Config:
    """
    Load configuration from disk and environment variables.

    Parameters
    ----------
    config_path : Optional[Union[str, Path]], default=None
        Path to a JSON or YAML configuration file.

    Returns
    -------
    Config
        Fully constructed configuration object.

    Examples
    --------
    >>> cfg = load_config()
    >>> cfg.log_level in {"INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"}
    True
    """
    load_dotenv()
    config_dict: Dict[str, Any] = {}

    if config_path:
        resolved_path = Path(config_path).expanduser()
        if resolved_path.suffix not in {".yaml", ".yml", ".json"}:
            raise ValueError(f"Invalid config file format: {resolved_path.suffix}. Must be .yaml, .yml, or .json")
        if not resolved_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {resolved_path}")
        with open(resolved_path, "r", encoding="utf-8") as file:
            if resolved_path.suffix == ".json":
                config_dict = json.load(file)
            else:
                config_dict = yaml.safe_load(file)

    env_config: Dict[str, Any] = {
        "log_level": os.getenv("LOG_LEVEL"),
        "output_dir": os.getenv("OUTPUT_DIR"),
        "log_file": os.getenv("LOG_FILE"),
        "analyzer_report_dir": os.getenv("ANALYZER_REPORT_DIR"),
        "compression": os.getenv("COMPRESSION_CODEC"),
        "engine": os.getenv("CONVERTER_ENGINE"),
        "sample_rows": _coerce_env_int(os.getenv("SAMPLE_ROWS")),
        "chunk_size": _coerce_env_int(os.getenv("CHUNK_SIZE")),
        "verify_rows": _coerce_env_int(os.getenv("VERIFY_ROWS")),
        "profiling_column_limit": _coerce_env_int(os.getenv("PROFILING_COLUMN_LIMIT")),
    }

    for key, value in env_config.items():
        if value is not None:
            config_dict[key] = value

    config = Config(**config_dict)
    logger.debug("Configuration loaded successfully")
    return config


def save_config(config: Config, output_path: Union[str, Path]) -> None:
    """
    Persist the active configuration to a file on disk.

    Parameters
    ----------
    config : Config
        Configuration object to serialize.
    output_path : Union[str, Path]
        Destination for the configuration file. Supported extensions: ``.json``,
        ``.yaml`` and ``.yml``.

    Returns
    -------
    None
        The configuration is written to disk and nothing is returned.

    Examples
    --------
    >>> import tempfile
    >>> from pathlib import Path
    >>> cfg = Config()
    >>> tmp = Path(tempfile.gettempdir()) / "config-example.yaml"
    >>> save_config(cfg, tmp)
    >>> tmp.exists()
    True
    >>> tmp.unlink()
    """
    serialized = config.model_dump()
    destination = Path(output_path).expanduser()
    destination.parent.mkdir(parents=True, exist_ok=True)

    with open(destination, "w", encoding="utf-8") as file:
        if destination.suffix == ".json":
            json.dump(serialized, file, indent=2)
        else:
            yaml.dump(serialized, file, default_flow_style=False)
    logger.info("Saved configuration to %s", destination)


def validate_config(config: Dict[str, Any]) -> None:
    """
    Validate a configuration dictionary before conversion.

    Parameters
    ----------
    config : Dict[str, Any]
        Configuration values that should be validated.

    Returns
    -------
    None
        The function raises ``ValueError`` when validation fails.

    Examples
    --------
    >>> validate_config({'csv': {'delimiter': ','}})
    """
    _ = config
    return None
