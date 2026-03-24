"""Data comparison engine for SAS migration validation.

Powered by DataComPy (Capital One's SAS PROC COMPARE replacement).
Supports both PySpark (Glue) and pandas (Lambda) comparison modes
so validation works regardless of which compute target was used.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
from datacompy.core import Compare as PandasCompare


@dataclass
class ColumnDiff:
    column_name: str
    sas_dtype: str
    aws_dtype: str
    sas_null_count: int
    aws_null_count: int
    sas_distinct_count: int
    aws_distinct_count: int
    match: bool = True
    notes: str = ""

    @property
    def spark_dtype(self) -> str:
        return self.aws_dtype

    @property
    def spark_null_count(self) -> int:
        return self.aws_null_count

    @property
    def spark_distinct_count(self) -> int:
        return self.aws_distinct_count


@dataclass
class ComparisonResult:
    dataset_name: str
    sas_row_count: int
    spark_row_count: int
    row_count_match: bool
    schema_match: bool
    mode: str = "pandas"
    column_diffs: list[ColumnDiff] = field(default_factory=list)
    missing_in_spark: list[str] = field(default_factory=list)
    extra_in_spark: list[str] = field(default_factory=list)
    sample_mismatches: int = 0
    overall_pass: bool = False
    notes: list[str] = field(default_factory=list)
    full_report: str = ""


class DataComPyComparator:
    """Compare SAS output vs AWS output datasets using DataComPy.

    Supports two modes:
    - ``"pandas"``: uses ``datacompy.core.Compare`` (no Spark needed)
    - ``"spark"``: uses ``datacompy.spark.sql.SparkSQLCompare``
    """

    def __init__(self, mode: str = "pandas", spark: object | None = None):
        self.mode = mode
        self.spark = spark
        if mode == "spark" and spark is None:
            raise ValueError("SparkSession required for mode='spark'")

    def compare(
        self,
        sas_df: object,
        aws_df: object,
        dataset_name: str,
        key_columns: list[str] | None = None,
        tolerance: float = 0.001,
    ) -> ComparisonResult:
        """Run a full DataComPy comparison and return a structured result."""
        if self.mode == "spark":
            return self._compare_spark(sas_df, aws_df, dataset_name, key_columns, tolerance)
        return self._compare_pandas(sas_df, aws_df, dataset_name, key_columns, tolerance)

    def _compare_pandas(
        self,
        sas_df: pd.DataFrame,
        aws_df: pd.DataFrame,
        dataset_name: str,
        key_columns: list[str] | None,
        tolerance: float,
    ) -> ComparisonResult:
        join_cols = key_columns or list(sas_df.columns[:1])

        comparison = PandasCompare(
            sas_df,
            aws_df,
            join_columns=join_cols,
            abs_tol=tolerance,
            rel_tol=tolerance,
            df1_name="SAS",
            df2_name="AWS",
        )

        sas_count = len(sas_df)
        aws_count = len(aws_df)

        sas_cols = set(c.lower() for c in sas_df.columns)
        aws_cols = set(c.lower() for c in aws_df.columns)
        missing = sorted(sas_cols - aws_cols)
        extra = sorted(aws_cols - sas_cols)
        schema_match = len(missing) == 0 and len(extra) == 0

        column_diffs = self._build_column_diffs_pandas(comparison, sas_df, aws_df)

        rows_only_sas = len(comparison.df1_unq_rows)
        rows_only_aws = len(comparison.df2_unq_rows)
        mismatched = len(comparison.all_mismatch()) if not comparison.matches() else 0
        total_mismatches = rows_only_sas + rows_only_aws + mismatched

        report_text = comparison.report()

        notes: list[str] = []
        if sas_count != aws_count:
            notes.append(f"Row count mismatch: SAS={sas_count}, AWS={aws_count}")
        if rows_only_sas > 0:
            notes.append(f"{rows_only_sas} rows only in SAS")
        if rows_only_aws > 0:
            notes.append(f"{rows_only_aws} rows only in AWS")
        if mismatched > 0:
            notes.append(f"{mismatched} rows with value differences")
        if missing:
            notes.append(f"Columns missing in AWS: {missing}")
        if extra:
            notes.append(f"Extra columns in AWS: {extra}")

        overall = bool(comparison.matches()) and schema_match

        return ComparisonResult(
            dataset_name=dataset_name,
            sas_row_count=sas_count,
            spark_row_count=aws_count,
            row_count_match=(sas_count == aws_count),
            schema_match=schema_match,
            mode="pandas",
            column_diffs=column_diffs,
            missing_in_spark=missing,
            extra_in_spark=extra,
            sample_mismatches=total_mismatches,
            overall_pass=overall,
            notes=notes,
            full_report=report_text,
        )

    def _compare_spark(
        self,
        sas_df: object,
        aws_df: object,
        dataset_name: str,
        key_columns: list[str] | None,
        tolerance: float,
    ) -> ComparisonResult:
        from datacompy.spark.sql import SparkSQLCompare

        join_cols = key_columns or [sas_df.columns[0]]

        comparison = SparkSQLCompare(
            self.spark,
            sas_df,
            aws_df,
            join_columns=join_cols,
            abs_tol=tolerance,
            rel_tol=tolerance,
            df1_name="SAS",
            df2_name="AWS",
        )

        sas_count = sas_df.count()
        aws_count = aws_df.count()

        sas_cols = {f.name.lower() for f in sas_df.schema.fields}
        aws_cols = {f.name.lower() for f in aws_df.schema.fields}
        missing = sorted(sas_cols - aws_cols)
        extra = sorted(aws_cols - sas_cols)
        schema_match = len(missing) == 0 and len(extra) == 0

        column_diffs = self._build_column_diffs_spark(sas_df, aws_df)

        rows_only_sas = comparison.rows_only_base.count()
        rows_only_aws = comparison.rows_only_compare.count()
        total_mismatches = rows_only_sas + rows_only_aws

        report_text = comparison.report()

        notes: list[str] = []
        if sas_count != aws_count:
            notes.append(f"Row count mismatch: SAS={sas_count}, AWS={aws_count}")
        if rows_only_sas > 0:
            notes.append(f"{rows_only_sas} rows only in SAS")
        if rows_only_aws > 0:
            notes.append(f"{rows_only_aws} rows only in AWS")
        if missing:
            notes.append(f"Columns missing in AWS: {missing}")
        if extra:
            notes.append(f"Extra columns in AWS: {extra}")

        overall = bool(comparison.matches()) and schema_match

        return ComparisonResult(
            dataset_name=dataset_name,
            sas_row_count=sas_count,
            spark_row_count=aws_count,
            row_count_match=(sas_count == aws_count),
            schema_match=schema_match,
            mode="spark",
            column_diffs=column_diffs,
            missing_in_spark=missing,
            extra_in_spark=extra,
            sample_mismatches=total_mismatches,
            overall_pass=overall,
            notes=notes,
            full_report=report_text,
        )

    @staticmethod
    def _build_column_diffs_pandas(
        comparison: PandasCompare,
        sas_df: pd.DataFrame,
        aws_df: pd.DataFrame,
    ) -> list[ColumnDiff]:
        common_cols = comparison.intersect_columns()
        diffs: list[ColumnDiff] = []

        intersect = comparison.intersect_rows
        mismatch_cols: set[str] = set()
        for col in common_cols:
            match_col = f"{col}_match"
            if match_col in intersect.columns and not intersect[match_col].all():
                mismatch_cols.add(col)

        for col in common_cols:
            sas_nulls = int(sas_df[col].isna().sum()) if col in sas_df.columns else 0
            aws_nulls = int(aws_df[col].isna().sum()) if col in aws_df.columns else 0
            sas_distinct = int(sas_df[col].nunique()) if col in sas_df.columns else 0
            aws_distinct = int(aws_df[col].nunique()) if col in aws_df.columns else 0

            match = col not in mismatch_cols
            notes = "" if match else "Value differences detected by DataComPy"

            diffs.append(
                ColumnDiff(
                    column_name=col,
                    sas_dtype=str(sas_df[col].dtype) if col in sas_df.columns else "unknown",
                    aws_dtype=str(aws_df[col].dtype) if col in aws_df.columns else "unknown",
                    sas_null_count=sas_nulls,
                    aws_null_count=aws_nulls,
                    sas_distinct_count=sas_distinct,
                    aws_distinct_count=aws_distinct,
                    match=match,
                    notes=notes,
                )
            )
        return diffs

    @staticmethod
    def _build_column_diffs_spark(sas_df: object, aws_df: object) -> list[ColumnDiff]:
        from pyspark.sql import functions as F

        sas_fields = {f.name.lower(): f for f in sas_df.schema.fields}
        aws_fields = {f.name.lower(): f for f in aws_df.schema.fields}
        common = sorted(set(sas_fields) & set(aws_fields))

        diffs: list[ColumnDiff] = []
        for col in common:
            sas_f = sas_fields[col]
            aws_f = aws_fields[col]
            sas_actual = col if col in sas_df.columns else sas_f.name
            aws_actual = col if col in aws_df.columns else aws_f.name

            sas_stats = sas_df.select(
                F.count(F.when(F.col(sas_actual).isNull(), 1)).alias("nulls"),
                F.countDistinct(F.col(sas_actual)).alias("distinct"),
            ).collect()[0]

            aws_stats = aws_df.select(
                F.count(F.when(F.col(aws_actual).isNull(), 1)).alias("nulls"),
                F.countDistinct(F.col(aws_actual)).alias("distinct"),
            ).collect()[0]

            diffs.append(
                ColumnDiff(
                    column_name=col,
                    sas_dtype=str(sas_f.dataType),
                    aws_dtype=str(aws_f.dataType),
                    sas_null_count=sas_stats["nulls"],
                    aws_null_count=aws_stats["nulls"],
                    sas_distinct_count=sas_stats["distinct"],
                    aws_distinct_count=aws_stats["distinct"],
                )
            )
        return diffs


# Backward-compatible alias
DatasetComparator = DataComPyComparator


def generate_golden_snapshot(
    df: object,
    dataset_name: str,
    output_dir: str | Path,
) -> dict:
    """Capture a 'golden' snapshot of dataset metrics for later comparison.

    Works with both PySpark and pandas DataFrames.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if isinstance(df, pd.DataFrame):
        return _golden_snapshot_pandas(df, dataset_name, output_dir)
    return _golden_snapshot_spark(df, dataset_name, output_dir)


def _golden_snapshot_pandas(
    df: pd.DataFrame, dataset_name: str, output_dir: Path
) -> dict:
    row_count = len(df)
    col_count = len(df.columns)

    column_stats = {}
    for col in df.columns:
        column_stats[col] = {
            "dtype": str(df[col].dtype),
            "null_count": int(df[col].isna().sum()),
            "distinct_count": int(df[col].nunique()),
        }

    snapshot = {
        "dataset_name": dataset_name,
        "row_count": row_count,
        "column_count": col_count,
        "schema": [
            {"name": col, "type": str(df[col].dtype), "nullable": df[col].isna().any()}
            for col in df.columns
        ],
        "column_stats": column_stats,
    }

    snapshot_path = output_dir / f"{dataset_name}_golden.json"
    snapshot_path.write_text(json.dumps(snapshot, indent=2, default=str))

    sample_path = output_dir / f"{dataset_name}_sample.parquet"
    df.head(1000).to_parquet(str(sample_path), index=False)

    return snapshot


def _golden_snapshot_spark(
    df: object, dataset_name: str, output_dir: Path
) -> dict:
    from pyspark.sql import functions as F

    row_count = df.count()
    col_count = len(df.columns)

    column_stats = {}
    for fld in df.schema.fields:
        stats = df.select(
            F.count(F.when(F.col(fld.name).isNull(), 1)).alias("nulls"),
            F.countDistinct(F.col(fld.name)).alias("distinct"),
        ).collect()[0]
        column_stats[fld.name] = {
            "dtype": str(fld.dataType),
            "null_count": stats["nulls"],
            "distinct_count": stats["distinct"],
        }

    snapshot = {
        "dataset_name": dataset_name,
        "row_count": row_count,
        "column_count": col_count,
        "schema": [
            {"name": f.name, "type": str(f.dataType), "nullable": f.nullable}
            for f in df.schema.fields
        ],
        "column_stats": column_stats,
    }

    snapshot_path = output_dir / f"{dataset_name}_golden.json"
    snapshot_path.write_text(json.dumps(snapshot, indent=2))

    sample_path = output_dir / f"{dataset_name}_sample.parquet"
    df.limit(1000).write.mode("overwrite").parquet(str(sample_path))

    return snapshot
