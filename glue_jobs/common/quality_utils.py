"""Data quality utilities for PySpark Glue jobs.

Provides null handling, type coercion, and data validation helpers
commonly needed when migrating SAS data processing pipelines.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)


def null_summary(df: DataFrame) -> DataFrame:
    """Generate a null-count summary for all columns."""
    null_counts = []
    for col_name in df.columns:
        null_counts.append(
            F.sum(F.when(F.col(col_name).isNull(), 1).otherwise(0)).alias(col_name)
        )
    return df.agg(*null_counts)


def fill_missing_sas(
    df: DataFrame,
    numeric_fill: float = 0.0,
    string_fill: str = "",
) -> DataFrame:
    """Fill missing values SAS-style (. for numeric, blank for character)."""
    for field in df.schema.fields:
        if isinstance(field.dataType, (DoubleType, IntegerType, LongType)):
            df = df.fillna({field.name: numeric_fill})
        elif isinstance(field.dataType, StringType):
            df = df.fillna({field.name: string_fill})
    return df


def coerce_types(df: DataFrame, type_map: dict[str, str]) -> DataFrame:
    """Cast columns to specified types.

    type_map example: {"age": "integer", "amount": "double", "date": "date"}
    """
    type_mapping = {
        "integer": IntegerType(),
        "int": IntegerType(),
        "long": LongType(),
        "double": DoubleType(),
        "float": DoubleType(),
        "string": StringType(),
        "date": DateType(),
        "timestamp": TimestampType(),
    }

    for col_name, type_str in type_map.items():
        spark_type = type_mapping.get(type_str.lower())
        if spark_type and col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(spark_type))
    return df


def deduplicate(
    df: DataFrame,
    key_cols: list[str],
    order_col: str | None = None,
    keep: str = "first",
) -> DataFrame:
    """Remove duplicate rows, optionally keeping first/last by order column."""
    if order_col is None:
        return df.dropDuplicates(key_cols)

    from pyspark.sql import Window

    order_expr = F.col(order_col).asc() if keep == "first" else F.col(order_col).desc()
    w = Window.partitionBy(*[F.col(c) for c in key_cols]).orderBy(order_expr)
    return (
        df.withColumn("_dedup_rank", F.row_number().over(w))
        .filter(F.col("_dedup_rank") == 1)
        .drop("_dedup_rank")
    )


def validate_not_null(df: DataFrame, columns: list[str]) -> tuple[DataFrame, DataFrame]:
    """Split DataFrame into valid rows (no nulls) and invalid rows."""
    condition = F.lit(True)
    for col_name in columns:
        condition = condition & F.col(col_name).isNotNull()

    valid = df.filter(condition)
    invalid = df.filter(~condition)
    return valid, invalid


def add_audit_columns(df: DataFrame, job_name: str = "unknown") -> DataFrame:
    """Add standard audit columns for lineage tracking."""
    return (
        df.withColumn("_etl_load_timestamp", F.current_timestamp())
        .withColumn("_etl_job_name", F.lit(job_name))
        .withColumn("_etl_source_row_hash", F.sha2(F.concat_ws("|", *df.columns), 256))
    )
