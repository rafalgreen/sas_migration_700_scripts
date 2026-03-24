"""PySpark utilities for SAS-equivalent transformations.

Provides helper functions that replicate common SAS behaviors in PySpark,
including date handling, RETAIN emulation, FIRST./LAST. detection,
and SAS function equivalents.
"""

from __future__ import annotations

import datetime
from typing import Any

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, LongType, StringType

SAS_EPOCH = datetime.date(1960, 1, 1)
SAS_EPOCH_OFFSET_DAYS = (datetime.date(1970, 1, 1) - SAS_EPOCH).days  # 3653


def sas_date_to_spark(col: str | Column) -> Column:
    """Convert a SAS date (days since 1960-01-01) to a Spark DateType.

    SAS stores dates as integer days since January 1, 1960.
    Spark's to_date and date functions use Unix epoch (1970-01-01).
    """
    if isinstance(col, str):
        col = F.col(col)
    return F.date_add(F.lit(SAS_EPOCH), col.cast(IntegerType()))


def spark_date_to_sas(col: str | Column) -> Column:
    """Convert a Spark date to SAS date integer (days since 1960-01-01)."""
    if isinstance(col, str):
        col = F.col(col)
    return F.datediff(col, F.lit(SAS_EPOCH))


def sas_datetime_to_spark(col: str | Column) -> Column:
    """Convert SAS datetime (seconds since 1960-01-01) to Spark TimestampType."""
    if isinstance(col, str):
        col = F.col(col)
    sas_epoch_ts = F.lit(SAS_EPOCH).cast("timestamp")
    return (sas_epoch_ts + F.expr(f"INTERVAL {col} SECONDS")).cast("timestamp")


def first_last_flags(
    df: DataFrame,
    partition_cols: list[str],
    order_cols: list[str] | None = None,
) -> DataFrame:
    """Add FIRST.<var> and LAST.<var> boolean columns (SAS BY-group equivalent).

    For each partition variable, adds `_first_<var>` and `_last_<var>` columns
    that are True when the row is the first/last occurrence in the BY group.
    """
    if order_cols is None:
        order_cols = partition_cols

    partition = [F.col(c) for c in partition_cols]
    order = [F.col(c) for c in order_cols]

    w = Window.partitionBy(*partition).orderBy(*order)
    w_desc = Window.partitionBy(*partition).orderBy(*[c.desc() for c in order])

    df = df.withColumn("_row_asc", F.row_number().over(w))
    df = df.withColumn("_row_desc", F.row_number().over(w_desc))

    for col_name in partition_cols:
        df = df.withColumn(f"_first_{col_name}", F.col("_row_asc") == 1)
        df = df.withColumn(f"_last_{col_name}", F.col("_row_desc") == 1)

    df = df.drop("_row_asc", "_row_desc")
    return df


def retain_accumulate(
    df: DataFrame,
    partition_cols: list[str],
    order_cols: list[str],
    accumulate_col: str,
    output_col: str,
    init_value: Any = 0,
) -> DataFrame:
    """Emulate SAS RETAIN with running accumulation.

    Computes a cumulative sum within each BY-group partition,
    equivalent to SAS pattern:
        RETAIN <output_col> <init_value>;
        IF FIRST.<key> THEN <output_col> = <init_value>;
        <output_col> + <accumulate_col>;
    """
    w = Window.partitionBy(*[F.col(c) for c in partition_cols]).orderBy(
        *[F.col(c) for c in order_cols]
    )

    return df.withColumn(
        output_col,
        F.sum(F.col(accumulate_col)).over(w)
    )


def sas_substr(col: str | Column, start: int, length: int | None = None) -> Column:
    """SAS SUBSTR equivalent. SAS uses 1-based indexing (same as Spark)."""
    if isinstance(col, str):
        col = F.col(col)
    if length is not None:
        return F.substring(col, start, length)
    return F.substring(col, start, 32767)


def sas_compress(col: str | Column, chars: str = " ") -> Column:
    """SAS COMPRESS equivalent -- remove specified characters."""
    if isinstance(col, str):
        col = F.col(col)
    pattern = "[" + chars.replace("\\", "\\\\").replace("]", "\\]") + "]"
    return F.regexp_replace(col, pattern, "")


def sas_catx(separator: str, *cols: str | Column) -> Column:
    """SAS CATX equivalent -- concatenate with separator, stripping blanks."""
    spark_cols = []
    for c in cols:
        if isinstance(c, str):
            spark_cols.append(F.trim(F.col(c)))
        else:
            spark_cols.append(F.trim(c))
    return F.concat_ws(separator, *spark_cols)


def sas_input_numeric(col: str | Column) -> Column:
    """SAS INPUT(x, 8.) equivalent -- convert string to number."""
    if isinstance(col, str):
        col = F.col(col)
    return col.cast("double")


def sas_put_char(col: str | Column) -> Column:
    """SAS PUT(x, $20.) equivalent -- convert to string."""
    if isinstance(col, str):
        col = F.col(col)
    return col.cast(StringType())


def sas_intck(interval: str, start_col: str | Column, end_col: str | Column) -> Column:
    """SAS INTCK equivalent -- count intervals between two dates."""
    if isinstance(start_col, str):
        start_col = F.col(start_col)
    if isinstance(end_col, str):
        end_col = F.col(end_col)

    interval_upper = interval.upper()
    if interval_upper == "DAY":
        return F.datediff(end_col, start_col)
    elif interval_upper == "MONTH":
        return F.months_between(end_col, start_col).cast(IntegerType())
    elif interval_upper == "YEAR":
        return (F.months_between(end_col, start_col) / 12).cast(IntegerType())
    else:
        return F.datediff(end_col, start_col)


def sas_intnx(
    interval: str,
    date_col: str | Column,
    increment: int,
    alignment: str = "B",
) -> Column:
    """SAS INTNX equivalent -- increment a date by interval units."""
    if isinstance(date_col, str):
        date_col = F.col(date_col)

    interval_upper = interval.upper()
    if interval_upper == "DAY":
        return F.date_add(date_col, increment)
    elif interval_upper == "MONTH":
        result = F.add_months(date_col, increment)
        if alignment.upper() == "B":
            return F.trunc(result, "MM")
        elif alignment.upper() == "E":
            return F.last_day(result)
        return result
    elif interval_upper == "YEAR":
        result = F.add_months(date_col, increment * 12)
        if alignment.upper() == "B":
            return F.trunc(result, "yyyy")
        return result
    else:
        return F.date_add(date_col, increment)


def sas_missing(col: str | Column) -> Column:
    """SAS MISSING() equivalent -- check if value is null/missing."""
    if isinstance(col, str):
        col = F.col(col)
    return F.isnull(col) | F.isnan(col)


def sas_coalesce(*cols: str | Column) -> Column:
    """SAS COALESCE equivalent."""
    spark_cols = [F.col(c) if isinstance(c, str) else c for c in cols]
    return F.coalesce(*spark_cols)
