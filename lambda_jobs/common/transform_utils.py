"""Pandas utilities for SAS-equivalent transformations.

Provides helper functions that replicate common SAS behaviors in pandas,
including date handling, RETAIN emulation, FIRST./LAST. detection,
and SAS function equivalents.

This is the pandas counterpart of ``glue_jobs/common/transform_utils.py``
(PySpark) — same function names so the Bedrock prompt can reference a
single API regardless of the target compute engine.
"""

from __future__ import annotations

import datetime
from typing import Any

import numpy as np
import pandas as pd

SAS_EPOCH = datetime.date(1960, 1, 1)
_PANDAS_SAS_EPOCH = pd.Timestamp("1960-01-01")


def sas_date_to_pandas(series: pd.Series) -> pd.Series:
    """Convert SAS date integers (days since 1960-01-01) to pandas datetime."""
    return pd.to_timedelta(series, unit="D") + _PANDAS_SAS_EPOCH


def pandas_date_to_sas(series: pd.Series) -> pd.Series:
    """Convert pandas datetime to SAS date integers (days since 1960-01-01)."""
    return (pd.to_datetime(series) - _PANDAS_SAS_EPOCH).dt.days


def sas_datetime_to_pandas(series: pd.Series) -> pd.Series:
    """Convert SAS datetime (seconds since 1960-01-01) to pandas datetime."""
    return pd.to_timedelta(series, unit="s") + _PANDAS_SAS_EPOCH


def first_last_flags(
    df: pd.DataFrame,
    partition_cols: list[str],
    order_cols: list[str] | None = None,
) -> pd.DataFrame:
    """Add FIRST.<var> and LAST.<var> boolean columns (SAS BY-group equivalent).

    For each partition variable, adds ``_first_<var>`` and ``_last_<var>``
    columns that are True when the row is the first/last in the group.
    """
    if order_cols is None:
        order_cols = partition_cols

    df = df.sort_values(order_cols).copy()
    grouped = df.groupby(partition_cols, sort=False)

    for col in partition_cols:
        df[f"_first_{col}"] = ~df[col].eq(df[col].shift(1))
        df[f"_last_{col}"] = ~df[col].eq(df[col].shift(-1))

    # Fix boundaries: first/last rows in each group must be True
    first_idx = grouped.head(1).index
    last_idx = grouped.tail(1).index
    for col in partition_cols:
        df.loc[first_idx, f"_first_{col}"] = True
        df.loc[last_idx, f"_last_{col}"] = True

    return df


def retain_accumulate(
    df: pd.DataFrame,
    partition_cols: list[str],
    order_cols: list[str],
    accumulate_col: str,
    output_col: str,
    init_value: Any = 0,
) -> pd.DataFrame:
    """Emulate SAS RETAIN with running accumulation via cumulative sum."""
    df = df.sort_values(partition_cols + order_cols).copy()
    df[output_col] = df.groupby(partition_cols)[accumulate_col].cumsum()
    return df


def sas_substr(series: pd.Series, start: int, length: int | None = None) -> pd.Series:
    """SAS SUBSTR equivalent. SAS uses 1-based indexing."""
    idx = start - 1  # convert to 0-based
    if length is not None:
        return series.astype(str).str[idx : idx + length]
    return series.astype(str).str[idx:]


def sas_compress(series: pd.Series, chars: str = " ") -> pd.Series:
    """SAS COMPRESS equivalent — remove specified characters."""
    pattern = "[" + chars.replace("\\", "\\\\").replace("]", "\\]") + "]"
    return series.astype(str).str.replace(pattern, "", regex=True)


def sas_catx(separator: str, *cols_or_series: pd.Series) -> pd.Series:
    """SAS CATX equivalent — concatenate with separator, stripping blanks."""
    stripped = [s.astype(str).str.strip() for s in cols_or_series]
    result = stripped[0]
    for s in stripped[1:]:
        result = result + separator + s
    return result


def sas_input_numeric(series: pd.Series) -> pd.Series:
    """SAS INPUT(x, 8.) equivalent — convert string to numeric."""
    return pd.to_numeric(series, errors="coerce")


def sas_put_char(series: pd.Series) -> pd.Series:
    """SAS PUT(x, $20.) equivalent — convert to string."""
    return series.astype(str)


def sas_intck(interval: str, start: pd.Series, end: pd.Series) -> pd.Series:
    """SAS INTCK equivalent — count intervals between two dates."""
    start = pd.to_datetime(start)
    end = pd.to_datetime(end)
    interval_upper = interval.upper()

    if interval_upper == "DAY":
        return (end - start).dt.days
    elif interval_upper == "MONTH":
        return (end.dt.year - start.dt.year) * 12 + (end.dt.month - start.dt.month)
    elif interval_upper == "YEAR":
        return end.dt.year - start.dt.year
    else:
        return (end - start).dt.days


def sas_intnx(
    interval: str,
    date_series: pd.Series,
    increment: int,
    alignment: str = "B",
) -> pd.Series:
    """SAS INTNX equivalent — increment a date by interval units."""
    date_series = pd.to_datetime(date_series)
    interval_upper = interval.upper()

    if interval_upper == "DAY":
        return date_series + pd.Timedelta(days=increment)
    elif interval_upper == "MONTH":
        result = date_series + pd.DateOffset(months=increment)
        if alignment.upper() == "B":
            return result - pd.to_timedelta(result.dt.day - 1, unit="D")
        elif alignment.upper() == "E":
            return result + pd.offsets.MonthEnd(0)
        return result
    elif interval_upper == "YEAR":
        result = date_series + pd.DateOffset(years=increment)
        if alignment.upper() == "B":
            return result.apply(lambda d: d.replace(month=1, day=1))
        return result
    else:
        return date_series + pd.Timedelta(days=increment)


def sas_missing(series: pd.Series) -> pd.Series:
    """SAS MISSING() equivalent — check if value is null/NaN."""
    return series.isna()


def sas_coalesce(*series: pd.Series) -> pd.Series:
    """SAS COALESCE equivalent — return first non-null value across columns."""
    df_temp = pd.concat(series, axis=1)
    return df_temp.bfill(axis=1).iloc[:, 0]
