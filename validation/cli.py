"""CLI for the data validation framework (DataComPy-powered).

Usage:
    sas-validate compare --sas-output data/sas/ --spark-output data/spark/
    sas-validate compare --sas-output golden/ --spark-output data/ --mode pandas
    sas-validate snapshot --input data/sas/ --output golden/
"""

from __future__ import annotations

import csv
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

console = Console()

_MODE_CHOICES = click.Choice(["spark", "pandas"], case_sensitive=False)


@click.group()
def main():
    """Data Validation Framework -- compare SAS vs AWS outputs (powered by DataComPy)."""


@main.command()
@click.option("--sas-output", required=True, help="Directory with SAS output files (Parquet/CSV)")
@click.option("--spark-output", required=True, help="Directory with AWS output files")
@click.option("--key-columns", default=None, help="Comma-separated key columns for row matching")
@click.option("--report", default="reports/validation_report.csv", help="Report output path")
@click.option("--tolerance", default=0.001, type=float, help="Numeric comparison tolerance")
@click.option("--mode", "-m", type=_MODE_CHOICES, default="pandas", help="Comparison mode: pandas (no Spark needed) or spark (requires SparkSession)")
@click.option("--full-report", "full_report_path", default=None, type=click.Path(), help="Write DataComPy full-text report to this file")
def compare(
    sas_output: str,
    spark_output: str,
    key_columns: str | None,
    report: str,
    tolerance: float,
    mode: str,
    full_report_path: str | None,
):
    """Compare SAS and AWS output datasets using DataComPy."""
    from validation.comparator import DataComPyComparator

    spark = None
    if mode == "spark":
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("validation").getOrCreate()

    comparator = DataComPyComparator(mode=mode, spark=spark)

    sas_path = Path(sas_output)
    spark_path = Path(spark_output)

    keys = key_columns.split(",") if key_columns else None
    results = []
    full_reports: list[str] = []

    sas_datasets = list(sas_path.glob("*.parquet")) + list(sas_path.glob("*.csv"))
    console.print(f"[blue]Found {len(sas_datasets)} datasets to compare (mode: {mode})[/blue]")

    for sas_file in sorted(sas_datasets):
        dataset_name = sas_file.stem
        spark_file = spark_path / sas_file.name

        if not spark_file.exists():
            for ext in [".parquet", ".csv"]:
                alt = spark_path / f"{dataset_name}{ext}"
                if alt.exists():
                    spark_file = alt
                    break

        if not spark_file.exists():
            console.print(f"[yellow]No AWS output for: {dataset_name}[/yellow]")
            continue

        console.print(f"Comparing: {dataset_name}")

        sas_df = _read_file(sas_file, mode, spark)
        aws_df = _read_file(spark_file, mode, spark)

        result = comparator.compare(sas_df, aws_df, dataset_name, keys, tolerance)
        results.append(result)

        if result.full_report:
            full_reports.append(f"=== {dataset_name} ===\n{result.full_report}\n")

        status = "[green]PASS[/green]" if result.overall_pass else "[red]FAIL[/red]"
        console.print(f"  {status} -- rows: SAS={result.sas_row_count} AWS={result.spark_row_count}")

    _write_validation_report(results, report)
    _print_summary(results)

    if full_report_path and full_reports:
        Path(full_report_path).parent.mkdir(parents=True, exist_ok=True)
        Path(full_report_path).write_text("\n".join(full_reports))
        console.print(f"[green]Full DataComPy report: {full_report_path}[/green]")

    if spark:
        spark.stop()


@main.command()
@click.option("--input", "-i", "input_dir", required=True, help="Directory with SAS output datasets")
@click.option("--output", "-o", "output_dir", required=True, help="Output directory for golden snapshots")
@click.option("--mode", "-m", type=_MODE_CHOICES, default="pandas", help="Read mode: pandas or spark")
def snapshot(input_dir: str, output_dir: str, mode: str):
    """Capture golden snapshots of SAS output for later validation."""
    from validation.comparator import generate_golden_snapshot

    spark = None
    if mode == "spark":
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("snapshot").getOrCreate()

    input_path = Path(input_dir)
    datasets = list(input_path.glob("*.parquet")) + list(input_path.glob("*.csv"))

    console.print(f"[blue]Capturing {len(datasets)} golden snapshots (mode: {mode})...[/blue]")

    for ds_file in sorted(datasets):
        name = ds_file.stem
        df = _read_file(ds_file, mode, spark)
        snapshot_data = generate_golden_snapshot(df, name, output_dir)
        console.print(f"  [green]{name}[/green]: {snapshot_data['row_count']} rows, {snapshot_data['column_count']} cols")

    if spark:
        spark.stop()
    console.print(f"\n[green]Snapshots saved to: {output_dir}[/green]")


def _read_file(path: Path, mode: str, spark: object | None) -> object:
    """Read a Parquet or CSV file as a pandas or PySpark DataFrame."""
    if mode == "pandas":
        import pandas as pd
        if path.suffix == ".parquet":
            return pd.read_parquet(str(path))
        return pd.read_csv(str(path))
    else:
        if path.suffix == ".parquet":
            return spark.read.parquet(str(path))
        return spark.read.option("header", True).option("inferSchema", True).csv(str(path))


def _write_validation_report(results: list, report_path: str) -> None:
    Path(report_path).parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "dataset", "mode", "pass", "sas_rows", "aws_rows", "row_match",
            "schema_match", "mismatches", "missing_cols", "extra_cols", "notes",
        ])
        for r in results:
            writer.writerow([
                r.dataset_name, r.mode, r.overall_pass, r.sas_row_count,
                r.spark_row_count, r.row_count_match, r.schema_match,
                r.sample_mismatches,
                "; ".join(r.missing_in_spark),
                "; ".join(r.extra_in_spark),
                " | ".join(r.notes),
            ])
    console.print(f"\n[green]Validation report: {report_path}[/green]")


def _print_summary(results: list) -> None:
    passed = sum(1 for r in results if r.overall_pass)
    failed = sum(1 for r in results if not r.overall_pass)
    total = len(results)

    table = Table(title="Validation Summary")
    table.add_column("Metric", style="bold")
    table.add_column("Count", justify="right")

    table.add_row("Total datasets", str(total))
    table.add_row("[green]Passed[/green]", str(passed))
    table.add_row("[red]Failed[/red]", str(failed))
    table.add_row(
        "Pass rate",
        f"{passed / total * 100:.1f}%" if total else "N/A",
    )

    console.print()
    console.print(table)


if __name__ == "__main__":
    main()
