"""Statistics tracking for the Parquet Converter."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List


@dataclass
class ConversionStats:
    """Statistics for a single file conversion."""

    input_file: str
    output_file: str
    rows_processed: int
    rows_converted: int
    errors: List[str]
    warnings: List[str]
    column_stats: Dict[str, Dict] = field(default_factory=dict)

    @property
    def input_path(self) -> Path:
        """Path to input file."""
        return Path(self.input_file)

    @property
    def output_path(self) -> Path:
        """Path to output file."""
        return Path(self.output_file)

    @property
    def success(self) -> bool:
        """Whether the conversion was successful."""
        return len(self.errors) == 0

    @property
    def error_count(self) -> int:
        """Number of errors encountered."""
        return len(self.errors)

    @property
    def warning_count(self) -> int:
        """Number of warnings encountered."""
        return len(self.warnings)

    @property
    def rows(self) -> int:
        """Number of rows processed."""
        return self.rows_processed

    @property
    def columns(self) -> int:
        """Number of columns in the file."""
        return len(self.column_stats)

    def add_column_stats(self, column: str, stats: Dict) -> None:
        """Add statistics for a column.

        Args:
            column: Column name
            stats: Dictionary of statistics
        """
        self.column_stats[column] = stats

    def to_dict(self) -> Dict:
        """Convert stats to dictionary format."""
        return {
            "input_file": self.input_file,
            "output_file": self.output_file,
            "rows_processed": self.rows_processed,
            "rows_converted": self.rows_converted,
            "success": self.success,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "errors": self.errors,
            "warnings": self.warnings,
            "column_stats": self.column_stats,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "ConversionStats":
        """Create stats from dictionary format.

        Args:
            data: Dictionary containing stats data

        Returns:
            ConversionStats object
        """
        return cls(
            input_file=data["input_file"],
            output_file=data["output_file"],
            rows_processed=data["rows_processed"],
            rows_converted=data["rows_converted"],
            errors=data.get("errors", []),
            warnings=data.get("warnings", []),
            column_stats=data.get("column_stats", {}),
        )
