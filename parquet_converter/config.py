"""Configuration management for the Parquet Converter."""

import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)


class CSVOptions(BaseModel):
    """CSV file parsing options."""

    delimiter: str = ","
    encoding: str = "utf-8"
    header: Optional[int] = 0
    na_values: List[str] = ["", "NA", "NULL"]
    low_memory: bool = False


class TXTOptions(BaseModel):
    """Text file parsing options."""

    delimiter: str = "\t"
    encoding: str = "utf-8"
    header: Optional[int] = 0
    na_values: List[str] = ["", "NA", "NULL"]
    low_memory: bool = False


class DateTimeFormats(BaseModel):
    """Date and time format options."""

    default: str = "%Y-%m-%d"
    custom: List[str] = []


class Config(BaseModel):
    """Main configuration class."""

    csv: CSVOptions = Field(default_factory=CSVOptions)
    txt: TXTOptions = Field(default_factory=TXTOptions)
    datetime_formats: DateTimeFormats = Field(default_factory=DateTimeFormats)
    log_level: str = "INFO"
    output_dir: Optional[Path] = None
    log_file: Optional[Path] = None

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate log level."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(f"Invalid log level: {v}. Must be one of {valid_levels}")
        return v.upper()

    @field_validator("output_dir")
    @classmethod
    def validate_output_dir(cls, v: Optional[Path]) -> Optional[Path]:
        """Validate output directory."""
        if v is not None:
            v = Path(v)
            v.mkdir(parents=True, exist_ok=True)
        return v


def load_config(config_path: Optional[str] = None) -> Config:
    """Load configuration from file and environment variables.

    Args:
        config_path: Path to configuration file (JSON or YAML)

    Returns:
        Configuration object

    Raises:
        FileNotFoundError: If config file is not found
        ValueError: If config file format is invalid
    """
    # Load environment variables
    load_dotenv()

    # Start with default config
    config_dict: Dict[str, Any] = {}

    # Load from config file if provided
    if config_path:
        config_path = Path(config_path)

        # Check file extension before checking existence
        if config_path.suffix not in [".yaml", ".yml", ".json"]:
            raise ValueError(
                f"Invalid config file format: {config_path.suffix}. Must be .yaml, .yml, or .json"
            )

        # Check file existence
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        # Load config file
        with open(config_path) as f:
            if config_path.suffix == ".json":
                import json

                config_dict = json.load(f)
            else:
                config_dict = yaml.safe_load(f)

    # Load from environment variables
    env_config = {
        "log_level": os.getenv("LOG_LEVEL"),
        "output_dir": os.getenv("OUTPUT_DIR"),
        "log_file": os.getenv("LOG_FILE"),
    }

    # Update config with environment variables
    for key, value in env_config.items():
        if value is not None:
            config_dict[key] = value

    # Create config object
    config = Config(**config_dict)

    # Log success
    logger.debug("Configuration loaded successfully")

    return config


def save_config(config: Config, output_path: Path) -> None:
    """Save configuration to file.

    Args:
        config: Configuration object
        output_path: Path to save configuration
    """
    # Convert config to dictionary
    config_dict = config.model_dump()

    # Save to file
    with open(output_path, "w") as f:
        if output_path.suffix == ".json":
            import json

            json.dump(config_dict, f, indent=2)
        else:
            yaml.dump(config_dict, f, default_flow_style=False)

    # Log success
    logger.info(f"Saved configuration to {output_path}")


def validate_config(config: Dict[str, Any]) -> None:
    """Validate configuration values.

    Args:
        config: Configuration dictionary to validate

    Raises:
        ValueError: If configuration is invalid
    """
    # Add validation logic here as needed
    pass
