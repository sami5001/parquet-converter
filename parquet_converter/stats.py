"""Statistics tracking for the Parquet Converter."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List


@dataclass
class ConversionStats:
    """
    Structured representation of a single file conversion.

    Attributes
    ----------
    input_file : str
        Original input file path.
    output_file : str
        Generated parquet file path.
    rows_processed : int
        Number of rows read from the source file.
    rows_converted : int
        Number of rows successfully written to the parquet file.
    errors : List[str]
        Collection of error messages captured during conversion.
    warnings : List[str]
        Collection of warning messages emitted during conversion.
    column_stats : Dict[str, Dict]
        Optional column-level statistics populated by the converter.

    Examples
    --------
    >>> stats = ConversionStats(
    ...     input_file="data.csv",
    ...     output_file="data.csv.parquet",
    ...     rows_processed=100,
    ...     rows_converted=100,
    ...     errors=[],
    ...     warnings=[],
    ... )
    >>> stats.success
    True
    """

    input_file: str
    output_file: str
    rows_processed: int
    rows_converted: int
    errors: List[str]
    warnings: List[str]
    column_stats: Dict[str, Dict] = field(default_factory=dict)

    @property
    def input_path(self) -> Path:
        """
        Convert the textual input path into a :class:`pathlib.Path`.

        Returns
        -------
        Path
            Path object pointing to the original input file.

        Examples
        --------
        >>> stats = ConversionStats("data.csv", "out.parquet", 1, 1, [], [])
        >>> stats.input_path.name
        'data.csv'
        """
        return Path(self.input_file)

    @property
    def output_path(self) -> Path:
        """
        Convert the textual output path into a :class:`pathlib.Path`.

        Returns
        -------
        Path
            Path object pointing to the converted parquet file.

        Examples
        --------
        >>> stats = ConversionStats("data.csv", "out.parquet", 1, 1, [], [])
        >>> stats.output_path.suffix
        '.parquet'
        """
        return Path(self.output_file)

    @property
    def success(self) -> bool:
        """
        Determine whether the conversion completed without errors.

        Returns
        -------
        bool
            ``True`` when no errors were recorded.

        Examples
        --------
        >>> ConversionStats("data.csv", "out.parquet", 1, 1, [], []).success
        True
        """
        return len(self.errors) == 0

    @property
    def error_count(self) -> int:
        """
        Count the number of recorded errors.

        Returns
        -------
        int
            Number of errors associated with the conversion.

        Examples
        --------
        >>> stats = ConversionStats("data.csv", "out.parquet", 0, 0, ["boom"], [])
        >>> stats.error_count
        1
        """
        return len(self.errors)

    @property
    def warning_count(self) -> int:
        """
        Count the number of recorded warnings.

        Returns
        -------
        int
            Number of warnings associated with the conversion.

        Examples
        --------
        >>> stats = ConversionStats("data.csv", "out.parquet", 1, 1, [], ["slow"])
        >>> stats.warning_count
        1
        """
        return len(self.warnings)

    @property
    def rows(self) -> int:
        """
        Convenience accessor for the number of processed rows.

        Returns
        -------
        int
            The value stored in :attr:`rows_processed`.

        Examples
        --------
        >>> ConversionStats("data.csv", "out.parquet", 5, 5, [], []).rows
        5
        """
        return self.rows_processed

    @property
    def columns(self) -> int:
        """
        Number of profiled columns.

        Returns
        -------
        int
            Length of the :attr:`column_stats` dictionary.

        Examples
        --------
        >>> stats = ConversionStats("data.csv", "out.parquet", 1, 1, [], [])
        >>> stats.add_column_stats("value", {"dtype": "int64"})
        >>> stats.columns
        1
        """
        return len(self.column_stats)

    def add_column_stats(self, column: str, stats: Dict) -> None:
        """
        Register column-level statistics.

        Parameters
        ----------
        column : str
            Name of the column being profiled.
        stats : Dict
            Arbitrary statistics describing the column.

        Returns
        -------
        None
            The internal :attr:`column_stats` dictionary is updated in-place.

        Examples
        --------
        >>> stats = ConversionStats("data.csv", "out.parquet", 1, 1, [], [])
        >>> stats.add_column_stats("value", {"dtype": "int64"})
        >>> "value" in stats.column_stats
        True
        """
        self.column_stats[column] = stats

    def to_dict(self) -> Dict:
        """
        Serialize the conversion statistics into a dictionary.

        Returns
        -------
        Dict
            Plain dictionary representation of the dataclass.

        Examples
        --------
        >>> stats = ConversionStats("data.csv", "out.parquet", 1, 1, [], [])
        >>> stats.to_dict()["input_file"]
        'data.csv'
        """
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
        """
        Restore :class:`ConversionStats` from a dictionary.

        Parameters
        ----------
        data : Dict
            Serialized conversion statistics.

        Returns
        -------
        ConversionStats
            Newly constructed dataclass instance.

        Examples
        --------
        >>> payload = {
        ...     "input_file": "data.csv",
        ...     "output_file": "out.parquet",
        ...     "rows_processed": 1,
        ...     "rows_converted": 1,
        ... }
        >>> ConversionStats.from_dict(payload).input_file
        'data.csv'
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
