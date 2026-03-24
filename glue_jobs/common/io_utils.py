"""S3 and Glue I/O utilities for PySpark Glue jobs.

Provides helpers for reading/writing data in various formats,
managing S3 paths, and interacting with the Glue Data Catalog.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def read_s3_parquet(
    spark: SparkSession,
    s3_path: str,
    schema: str | None = None,
) -> DataFrame:
    """Read Parquet data from S3."""
    reader = spark.read.format("parquet")
    if schema:
        reader = reader.schema(schema)
    return reader.load(s3_path)


def read_s3_csv(
    spark: SparkSession,
    s3_path: str,
    header: bool = True,
    delimiter: str = ",",
    infer_schema: bool = True,
    schema: str | None = None,
) -> DataFrame:
    """Read CSV data from S3 with common SAS-like defaults."""
    reader = spark.read.format("csv")
    reader = reader.option("header", header)
    reader = reader.option("delimiter", delimiter)
    if schema:
        reader = reader.schema(schema)
    elif infer_schema:
        reader = reader.option("inferSchema", True)
    return reader.load(s3_path)


def read_glue_table(
    spark: SparkSession,
    database: str,
    table: str,
) -> DataFrame:
    """Read a table from the Glue Data Catalog."""
    return spark.table(f"glue_catalog.{database}.{table}")


def write_s3_parquet(
    df: DataFrame,
    s3_path: str,
    mode: str = "overwrite",
    partition_cols: list[str] | None = None,
    coalesce: int | None = None,
) -> None:
    """Write DataFrame to S3 as Parquet."""
    if coalesce:
        df = df.coalesce(coalesce)

    writer = df.write.mode(mode).format("parquet")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(s3_path)


def write_s3_csv(
    df: DataFrame,
    s3_path: str,
    mode: str = "overwrite",
    header: bool = True,
    delimiter: str = ",",
    coalesce: int | None = 1,
) -> None:
    """Write DataFrame to S3 as CSV."""
    if coalesce:
        df = df.coalesce(coalesce)

    df.write.mode(mode).option("header", header).option(
        "delimiter", delimiter
    ).csv(s3_path)


def write_glue_table(
    df: DataFrame,
    database: str,
    table: str,
    mode: str = "overwrite",
    format: str = "parquet",
    partition_cols: list[str] | None = None,
) -> None:
    """Write DataFrame to a Glue Data Catalog table."""
    writer = df.write.mode(mode).format(format)
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(f"glue_catalog.{database}.{table}")


def s3_path(bucket: str, *parts: str) -> str:
    """Build an S3 path from components."""
    joined = "/".join(p.strip("/") for p in parts if p)
    return f"s3://{bucket}/{joined}"


def resolve_sas_libname(
    lib_name: str,
    lib_mapping: dict[str, str] | None = None,
) -> str:
    """Resolve a SAS LIBNAME reference to a Glue catalog database or S3 path.

    Default mapping can be overridden with lib_mapping parameter.
    """
    default_mapping = {
        "work": "default",
        "sashelp": "reference",
    }

    mapping = {**default_mapping, **(lib_mapping or {})}
    return mapping.get(lib_name.lower(), lib_name.lower())
