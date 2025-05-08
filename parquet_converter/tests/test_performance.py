"""Performance tests for the Parquet Converter."""

import os
import shutil
import tempfile
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from ..config import Config
from ..converter import convert_file


def generate_test_file(rows: int, cols: int, file_type: str) -> str:
    """Generate a test file with random data.

    Args:
        rows: Number of rows
        cols: Number of columns
        file_type: File type (csv or txt)

    Returns:
        Path to generated file
    """
    # Generate random data
    data = {}
    for i in range(cols):
        data[f"col{i}"] = np.random.randint(0, 100, size=rows)

    # Create DataFrame
    df = pd.DataFrame(data)

    # Save to file
    suffix = f".{file_type}"
    temp_file = tempfile.mktemp(suffix=suffix)

    if file_type == "csv":
        df.to_csv(temp_file, index=False)
    else:
        df.to_csv(temp_file, sep="\t", index=False)

    return temp_file


@pytest.mark.performance
def test_conversion_performance():
    """Test conversion performance with different file sizes."""
    test_cases = [
        (1000, 10, "csv"),
        (10000, 10, "csv"),
        (100000, 10, "csv"),
        (1000, 10, "txt"),
        (10000, 10, "txt"),
        (100000, 10, "txt"),
    ]

    results = []

    for rows, cols, file_type in test_cases:
        # Generate test file
        input_file = generate_test_file(rows, cols, file_type)
        output_dir = tempfile.mkdtemp()

        try:
            # Measure conversion time
            start_time = time.time()
            config = Config()
            convert_file(input_file, output_dir, config.model_dump())
            end_time = time.time()

            # Calculate performance metrics
            conversion_time = end_time - start_time
            rows_per_second = rows / conversion_time

            results.append(
                {
                    "file_type": file_type,
                    "rows": rows,
                    "columns": cols,
                    "conversion_time": conversion_time,
                    "rows_per_second": rows_per_second,
                }
            )

        finally:
            # Cleanup
            os.unlink(input_file)
            shutil.rmtree(output_dir)

    # Verify performance thresholds
    for result in results:
        assert (
            result["rows_per_second"] > 1000
        ), f"Performance below threshold for {result['file_type']} file with {result['rows']} rows"


if __name__ == "__main__":
    test_conversion_performance()
