"""I/O utilities for PySpark Glue jobs.

Provides helpers for reading/writing data from S3, Glue Data Catalog,
JDBC databases (DB2, MSSQL Server), and Excel files.
"""

from __future__ import annotations

import json
import logging

import boto3
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

_JDBC_DRIVERS = {
    "mssql": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "sqlserver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "db2": "com.ibm.db2.jcc.DB2Driver",
}


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


def get_db_credentials(secret_name: str, region: str | None = None) -> dict:
    """Fetch database credentials from AWS Secrets Manager.

    Expected JSON structure in the secret::

        {"host", "port", "database", "username", "password", "engine"}

    Returns the parsed dict.
    """
    client = boto3.client("secretsmanager", region_name=region)
    resp = client.get_secret_value(SecretId=secret_name)
    return json.loads(resp["SecretString"])


def read_jdbc(
    spark: SparkSession,
    table: str,
    secret_name: str,
    *,
    driver: str | None = None,
    fetch_size: int = 10_000,
    custom_url: str | None = None,
) -> DataFrame:
    """Read a table from DB2 or MSSQL Server via JDBC.

    Credentials are pulled from Secrets Manager. The engine field in the
    secret (``mssql``, ``db2``) selects the JDBC driver automatically.
    """
    creds = get_db_credentials(secret_name)
    engine = creds.get("engine", "mssql").lower()

    if custom_url:
        url = custom_url
    elif engine in ("mssql", "sqlserver"):
        url = (
            f"jdbc:sqlserver://{creds['host']}:{creds.get('port', 1433)};"
            f"databaseName={creds['database']}"
        )
    elif engine == "db2":
        url = (
            f"jdbc:db2://{creds['host']}:{creds.get('port', 50000)}"
            f"/{creds['database']}"
        )
    else:
        raise ValueError(f"Unsupported JDBC engine: {engine}")

    resolved_driver = driver or _JDBC_DRIVERS.get(engine)
    if not resolved_driver:
        raise ValueError(f"No JDBC driver known for engine: {engine}")

    logger.info("Reading table %s from %s via JDBC", table, engine)
    return (
        spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", table)
        .option("user", creds["username"])
        .option("password", creds["password"])
        .option("driver", resolved_driver)
        .option("fetchsize", str(fetch_size))
        .load()
    )


def read_excel(
    spark: SparkSession,
    s3_path_str: str,
    sheet_name: str | int = 0,
    header: int = 0,
) -> DataFrame:
    """Read an Excel file from S3 into a Spark DataFrame.

    Spark has no native Excel reader, so this uses pandas as an
    intermediate step (the file must fit in driver memory).
    """
    import pandas as pd

    logger.info("Reading Excel file %s (sheet=%s)", s3_path_str, sheet_name)
    pdf = pd.read_excel(s3_path_str, sheet_name=sheet_name, header=header)
    return spark.createDataFrame(pdf)


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
