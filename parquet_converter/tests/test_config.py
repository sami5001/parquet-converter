"""Tests for the configuration module."""

import os
from pathlib import Path

import pytest
import yaml

from ..config import Config, CSVOptions, DateTimeFormats, TXTOptions, load_config


@pytest.fixture
def config_file(tmp_path):
    """Create a sample configuration file."""
    config_data = {
        "csv": {
            "delimiter": ",",
            "encoding": "utf-8",
            "header": 0,
            "na_values": ["", "NA", "NULL"],
            "low_memory": False,
        },
        "txt": {
            "delimiter": "\t",
            "encoding": "utf-8",
            "header": 0,
            "na_values": ["", "NA", "NULL"],
            "low_memory": False,
        },
        "datetime_formats": {
            "default": "%Y-%m-%d",
            "custom": ["%d/%m/%Y", "%Y-%m-%d %H:%M:%S"],
        },
        "log_level": "INFO",
        "output_dir": "output",
    }

    config_file = tmp_path / "config.yaml"
    with open(config_file, "w") as f:
        yaml.dump(config_data, f)

    return config_file


def test_csv_options():
    """Test CSV options configuration."""
    options = CSVOptions(encoding="latin1", na_values=["N/A"], low_memory=True)

    assert options.encoding == "latin1"
    assert options.na_values == ["N/A"]
    assert options.low_memory is True


def test_txt_options():
    """Test TXT options configuration."""
    options = TXTOptions(encoding="utf-16", na_values=["missing"], low_memory=True)

    assert options.encoding == "utf-16"
    assert options.na_values == ["missing"]
    assert options.low_memory is True


def test_datetime_formats():
    """Test datetime formats configuration."""
    formats = DateTimeFormats(default="%d-%m-%Y", custom=["%Y/%m/%d", "%H:%M:%S"])

    assert formats.default == "%d-%m-%Y"
    assert formats.custom == ["%Y/%m/%d", "%H:%M:%S"]


def test_config_validation():
    """Test configuration validation."""
    # Test valid log level
    config = Config(log_level="DEBUG")
    assert config.log_level == "DEBUG"

    # Test invalid log level
    with pytest.raises(ValueError):
        Config(log_level="INVALID")

    # Test output directory validation
    config = Config(output_dir="output")
    assert isinstance(config.output_dir, Path)


def test_load_config_yaml(config_file):
    """Test loading configuration from YAML file."""
    config = load_config(str(config_file))

    assert isinstance(config, Config)
    assert config.log_level == "INFO"
    assert isinstance(config.output_dir, Path)
    assert config.output_dir == Path("output")


def test_load_config_json(tmp_path):
    """Test loading configuration from JSON file."""
    config_data = {"log_level": "DEBUG", "output_dir": "json_output"}

    config_file = tmp_path / "config.json"
    with open(config_file, "w") as f:
        import json

        json.dump(config_data, f)

    config = load_config(str(config_file))
    assert config.log_level == "DEBUG"
    assert config.output_dir == Path("json_output")


def test_load_config_env(monkeypatch):
    """Test loading configuration from environment variables."""
    monkeypatch.setenv("LOG_LEVEL", "ERROR")
    monkeypatch.setenv("OUTPUT_DIR", "env_output")

    config = load_config()
    assert config.log_level == "ERROR"
    assert config.output_dir == Path("env_output")


def test_load_config_invalid_file():
    """Test loading configuration from invalid file."""
    with pytest.raises(FileNotFoundError):
        load_config("nonexistent.yaml")

    with pytest.raises(ValueError):
        load_config("invalid.txt")
