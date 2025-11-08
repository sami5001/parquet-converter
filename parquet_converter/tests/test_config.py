"""Tests for the configuration module."""

from pathlib import Path
from typing import Dict

import pytest
import yaml

from ..config import Config, load_config, save_config


@pytest.fixture
def config_dict() -> Dict:
    """Create a test configuration dictionary."""
    return {
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
    }


def test_load_config_defaults() -> None:
    """Test loading default configuration."""
    config = load_config()
    assert isinstance(config, Config)
    assert config.csv.delimiter == ","
    assert config.txt.delimiter == "\t"
    assert config.log_level == "INFO"


def test_load_config_yaml(tmp_path: Path, config_dict: Dict) -> None:
    """Test loading configuration from YAML file."""
    config_file = tmp_path / "config.yaml"
    with open(config_file, "w") as f:
        yaml.dump(config_dict, f)

    config = load_config(config_file)
    assert isinstance(config, Config)
    assert config.csv.delimiter == config_dict["csv"]["delimiter"]
    assert config.txt.delimiter == config_dict["txt"]["delimiter"]
    assert config.log_level == config_dict["log_level"]


def test_load_config_json(tmp_path: Path, config_dict: Dict) -> None:
    """Test loading configuration from JSON file."""
    config_file = tmp_path / "config.json"
    with open(config_file, "w") as f:
        import json

        json.dump(config_dict, f)

    config = load_config(config_file)
    assert isinstance(config, Config)
    assert config.csv.delimiter == config_dict["csv"]["delimiter"]
    assert config.txt.delimiter == config_dict["txt"]["delimiter"]
    assert config.log_level == config_dict["log_level"]


def test_save_config_yaml(tmp_path: Path, config_dict: Dict) -> None:
    """Test saving configuration to YAML file."""
    config = Config(**config_dict)
    output_file = tmp_path / "config.yaml"

    save_config(config, output_file)
    assert output_file.exists()

    # Load and verify
    with open(output_file) as f:
        saved_config = yaml.safe_load(f)

    assert saved_config["csv"]["delimiter"] == config_dict["csv"]["delimiter"]
    assert saved_config["txt"]["delimiter"] == config_dict["txt"]["delimiter"]
    assert saved_config["log_level"] == config_dict["log_level"]


def test_save_config_json(tmp_path: Path, config_dict: Dict) -> None:
    """Test saving configuration to JSON file."""
    config = Config(**config_dict)
    output_file = tmp_path / "config.json"

    save_config(config, output_file)
    assert output_file.exists()

    # Load and verify
    with open(output_file) as f:
        import json

        saved_config = json.load(f)

    assert saved_config["csv"]["delimiter"] == config_dict["csv"]["delimiter"]
    assert saved_config["txt"]["delimiter"] == config_dict["txt"]["delimiter"]
    assert saved_config["log_level"] == config_dict["log_level"]


def test_invalid_config_file(tmp_path: Path) -> None:
    """Test handling of invalid configuration file."""
    config_file = tmp_path / "config.invalid"
    config_file.write_text("invalid config")

    with pytest.raises(ValueError):
        load_config(config_file)


def test_missing_config_file(tmp_path: Path) -> None:
    """Test handling of missing configuration file."""
    config_file = tmp_path / "nonexistent.yaml"

    with pytest.raises(FileNotFoundError):
        load_config(config_file)


def test_invalid_log_level(config_dict: Dict) -> None:
    """Test validation of log level."""
    config_dict["log_level"] = "INVALID"

    with pytest.raises(ValueError):
        Config(**config_dict)


def test_load_config_env_overrides(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Ensure environment variables override defaults."""
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path / "out"))
    monkeypatch.setenv("ANALYZER_REPORT_DIR", str(tmp_path / "reports"))
    monkeypatch.setenv("COMPRESSION_CODEC", "gzip")
    monkeypatch.setenv("CONVERTER_ENGINE", "pandas")
    monkeypatch.setenv("SAMPLE_ROWS", "123")
    monkeypatch.setenv("CHUNK_SIZE", "456")
    monkeypatch.setenv("VERIFY_ROWS", "7")
    monkeypatch.setenv("PROFILING_COLUMN_LIMIT", "9")

    config = load_config()

    assert config.log_level == "DEBUG"
    assert config.output_dir and config.output_dir.name == "out"
    assert config.analyzer_report_dir and config.analyzer_report_dir.name == "reports"
    assert config.compression == "gzip"
    assert config.engine == "pandas"
    assert config.sample_rows == 123
    assert config.chunk_size == 456
    assert config.verify_rows == 7
    assert config.profiling_column_limit == 9


def test_config_validates_paths(tmp_path: Path) -> None:
    """Verify Config creates directories for output/log locations."""
    config = Config(
        output_dir=tmp_path / "output",
        log_file=tmp_path / "logs" / "run.log",
        analyzer_report_dir=tmp_path / "reports",
    )

    assert config.output_dir.exists()
    assert config.log_file and config.log_file.parent.exists()
    assert config.analyzer_report_dir and config.analyzer_report_dir.exists()
