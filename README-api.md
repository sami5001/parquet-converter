# Parquet Converter API Reference

This reference surfaces the full docstring content for the primary modules inside the project so that contributors can
review the documentation without diving into the source. Every entry below mirrors the corresponding NumPy-style
docstring: short summary, parameter descriptions, return values, and example snippets.

---

## Module: `parquet_converter.cli`

### `parse_args(args: Optional[List[str]] = None) -> argparse.Namespace`
- **Summary:** Parse command-line arguments for the CLI (conversion or analyzer modes).
- **Parameters:** Optional argument list (falls back to `sys.argv` when `None`).
- **Returns:** `argparse.Namespace` with `input_path`, `output_dir`, `config`, `verbose`, `save_config`, `mode`, and `report_dir`.
- **Example:**
  ```python
  namespace = parse_args(["input.csv", "--mode", "convert"])
  assert namespace.input_path == "input.csv"
  ```

### `main(args: Optional[List[str]] = None) -> int`
- **Summary:** Unified CLI entry point for conversion and analyzer workflows.
- **Parameters:** Optional argument list for testing; defaults to `sys.argv`.
- **Returns:** Exit code (0 success, 1 failure).
- **Example:** `main(["/path/to/data", "--mode", "analyze"])`

---

## Module: `parquet_converter.converter`

### `convert_file(input_path, output_dir, config) -> ConversionStats`
- Converts a single delimited file to Parquet using the configured engine (Polars streaming by default).
- **Parameters:** Source path, destination directory, config dict (matching `load_config` output).
- **Returns:** `ConversionStats` capturing rows processed, errors, and per-column stats.
- **Example:** Provided in docstring showing CSV → Parquet conversion inside a temporary directory.

### `convert_directory(input_dir, output_dir, config) -> List[ConversionStats]`
- Batch conversion over every supported file extension (`.csv`, `.txt`) in a directory.
- Enumerates files, applies `convert_file`, and aggregates statistics.

### `_convert_with_polars(input_path, output_dir, config)`
- Streaming Polars workflow with schema sampling, verification, and post-conversion profiling.
- Handles analyzer sample logging, chunk-sized streaming, and summary output.

### `_convert_with_pandas(input_path, output_dir, config)`
- Legacy pandas fallback honoring explicit CSV/TXT parsing options and dtype inference.

### `_resolve_file_options(input_path, config)`
- Chooses the correct parser settings dictionary based on file suffix (raises on unsupported extensions).

### `_build_polars_csv_kwargs(options)`
- Normalizes pandas-style parsing options to `polars.scan_csv` keyword arguments (separator, encoding, headers, etc.).

### `_normalize_polars_encoding(value)`
- Maps common encodings (e.g., `utf-8`) to Polars vocabulary (`utf8`, `utf8-lossy`).

### `_analyze_sample_with_polars(input_path, options, sample_rows)`
- Reads a sample of the source file to infer a schema and logs a tabulated summary.

### `_stream_polars_conversion(input_path, output_path, options, schema, compression, chunk_size)`
- Executes the streaming conversion, sinks to Parquet, and returns `(success, total_rows, elapsed_seconds)`.

### `_collect_polars_column_stats(output_path, column_limit)`
- Scans the generated Parquet file lazily, computing unique counts and null counts per column (bounded by `column_limit`).

### `_verify_conversion(output_path, source_path, verify_rows)`
- Logs file size comparisons, row/column counts, and sample head rows for additional assurance.

---

## Module: `parquet_converter.analyzer`

### `scan_parquet_files(input_dir, recursive=True) -> List[Path]`
- Recursively discover `.parquet` files inside a directory tree.

### `get_file_size(file_path) -> str`
- Human-readable file size via `humanize.naturalsize`.

### `get_file_modification_time(file_path) -> str`
- Formats `mtime` as `YYYY-MM-DD HH:MM:SS`.

### `calculate_summary_stats(df)`
- Numeric column profiling (min/max/mean/median/std/null stats).

### `calculate_null_counts(df)`
- Absolute and percentage null counts for all columns.

### `get_unique_values_info(df)`
- Unique counts per column plus top-5 most common categorical values when feasible.

### `analyze_parquet_file(file_path)`
- Full per-file analysis: filesystem metadata, schema, numeric stats, null stats, unique stats, and sampled rows.

### `format_analysis_report(analyses, width=150)`
- Generates a Rich table/report string summarizing results for multiple files (summary table, per-file panels, samples).

### `analyze_directory(input_dir, output_dir=None)`
- Runs the entire analysis pipeline for a directory and writes `parquet_analysis_report.txt`.

---

## Module: `parquet_converter.config`

- **Data models:** `CSVOptions`, `TXTOptions`, `DateTimeFormats`, `Config` — all Pydantic models with attributes for delimiters, encodings, headers, NA tokens, datetime patterns, logging, engine selection, sample sizes, chunk sizes, and analyzer directories.
- **Validators:** Ensure log levels, engine values, and integer configuration options are valid; expand and create directories for output/log/report paths.

### `load_config(config_path=None) -> Config`
- Loads YAML/JSON config, merges environment variables (`LOG_LEVEL`, `COMPRESSION_CODEC`, `CONVERTER_ENGINE`, etc.), and returns a validated `Config` object.

### `save_config(config, output_path)`
- Serializes the current configuration to JSON or YAML (based on file suffix).

### `validate_config(config_dict)`
- Placeholder for additional validation hooks (currently a no-op; extend here as needed).

---

## Module: `parquet_converter.logging`

### `JSONEncoder`
- Custom encoder that serializes NumPy scalars/arrays and `pathlib.Path` objects when building reports.

### `setup_logging(level="INFO", log_file=None, verbose=False)`
- Configures console and optional file handlers for both root and `parquet_converter` loggers.

### `format_stats_table(stats_list)`
- Uses `tabulate` to render per-file conversion outcomes (`Success`/`Failed`).

### `save_conversion_report(stats_list, output_dir, config)`
- Writes `conversion_report.json` containing timestamp, configuration snapshot, summary counts, and per-file stats.

### `log_conversion_summary(stats_list)`
- Logs totals (files, successes, failures, rows, columns) plus the tabulated breakdown; surfaces individual errors.

---

## Module: `parquet_converter.parser`

### `parse_file(input_path, config)`
- Delegates to CSV or TXT parsers, raises `ValueError` on unsupported suffixes, and runs dtype inference unless disabled.

### `parse_csv(input_path, options)`
- Thin wrapper over `pandas.read_csv` honoring delimiter, encoding, header, column names, dtypes, NA tokens, and skip rows.

### `parse_txt(input_path, options)`
- Identical to `parse_csv` but defaults to `\t` delimiters and whitespace fallbacks.

### `infer_dtypes(df, config)`
- Attempts datetime parsing (default & custom formats), numeric casting (int/float), boolean detection, and otherwise coerces to string dtype.

---

## Module: `parquet_converter.stats`

### `ConversionStats`
- Dataclass capturing per-file metrics: input/output paths, rows processed, success flag, errors, warnings, and column stats.
- Provides convenience properties (`input_path`, `output_path`, `success`, `error_count`, `warning_count`, `rows`, `columns`) plus helpers `add_column_stats`, `to_dict`, and `from_dict`.

---

For any functions not explicitly listed above (e.g., additional helpers introduced later), please ensure the module
docstrings follow the NumPy template and update this document with their summaries, parameters, returns, and doctest-style
examples. This guarantees parity between inline documentation and the human-readable API guide.***
