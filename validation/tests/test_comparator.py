"""Tests for the DataComPy-powered comparator.

All tests use pandas mode (no SparkSession needed), so they run in any CI
environment without Java/Spark dependencies.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from validation.comparator import (
    ColumnDiff,
    ComparisonResult,
    DataComPyComparator,
    DatasetComparator,
    generate_golden_snapshot,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def exact_dfs():
    """Two identical DataFrames."""
    df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"], "value": [10.0, 20.0, 30.0]})
    return df.copy(), df.copy()


@pytest.fixture()
def mismatched_rows_dfs():
    """Same schema but different rows."""
    sas = pd.DataFrame({"id": [1, 2, 3], "val": [10.0, 20.0, 30.0]})
    aws = pd.DataFrame({"id": [1, 2, 4], "val": [10.0, 20.0, 40.0]})
    return sas, aws


@pytest.fixture()
def extra_col_dfs():
    """AWS has an extra column; SAS has a column AWS lacks."""
    sas = pd.DataFrame({"id": [1, 2], "sas_only": [1, 2], "shared": [10, 20]})
    aws = pd.DataFrame({"id": [1, 2], "aws_only": [3, 4], "shared": [10, 20]})
    return sas, aws


@pytest.fixture()
def value_mismatch_dfs():
    """Matching keys but different values."""
    sas = pd.DataFrame({"id": [1, 2, 3], "metric": [100.0, 200.0, 300.0]})
    aws = pd.DataFrame({"id": [1, 2, 3], "metric": [100.0, 205.0, 300.0]})
    return sas, aws


@pytest.fixture()
def tolerance_dfs():
    """Values within and outside tolerance."""
    sas = pd.DataFrame({"id": [1, 2], "val": [1.0000, 2.0000]})
    aws = pd.DataFrame({"id": [1, 2], "val": [1.0005, 2.0020]})
    return sas, aws


# ---------------------------------------------------------------------------
# Initialization tests
# ---------------------------------------------------------------------------

class TestInit:
    def test_pandas_mode_no_spark_ok(self):
        comp = DataComPyComparator(mode="pandas")
        assert comp.mode == "pandas"
        assert comp.spark is None

    def test_spark_mode_requires_session(self):
        with pytest.raises(ValueError, match="SparkSession required"):
            DataComPyComparator(mode="spark")

    def test_backward_compat_alias(self):
        assert DatasetComparator is DataComPyComparator


# ---------------------------------------------------------------------------
# Exact match
# ---------------------------------------------------------------------------

class TestExactMatch:
    def test_identical_dataframes_pass(self, exact_dfs):
        sas, aws = exact_dfs
        comp = DataComPyComparator(mode="pandas")
        result = comp.compare(sas, aws, "exact_test", key_columns=["id"])

        assert result.overall_pass is True
        assert result.row_count_match is True
        assert result.schema_match is True
        assert result.sample_mismatches == 0
        assert result.sas_row_count == 3
        assert result.spark_row_count == 3
        assert result.mode == "pandas"
        assert len(result.notes) == 0

    def test_exact_match_full_report_not_empty(self, exact_dfs):
        sas, aws = exact_dfs
        comp = DataComPyComparator()
        result = comp.compare(sas, aws, "report_test", key_columns=["id"])
        assert len(result.full_report) > 0
        assert "DataComPy" in result.full_report


# ---------------------------------------------------------------------------
# Row count mismatch
# ---------------------------------------------------------------------------

class TestRowCountMismatch:
    def test_different_row_counts(self):
        sas = pd.DataFrame({"id": [1, 2, 3], "v": [1, 2, 3]})
        aws = pd.DataFrame({"id": [1, 2], "v": [1, 2]})
        comp = DataComPyComparator()
        result = comp.compare(sas, aws, "row_mismatch")

        assert result.row_count_match is False
        assert result.sas_row_count == 3
        assert result.spark_row_count == 2
        assert result.overall_pass is False
        assert any("Row count mismatch" in n for n in result.notes)


# ---------------------------------------------------------------------------
# Column / schema mismatch
# ---------------------------------------------------------------------------

class TestColumnMismatch:
    def test_missing_and_extra_columns_detected(self, extra_col_dfs):
        sas, aws = extra_col_dfs
        comp = DataComPyComparator()
        result = comp.compare(sas, aws, "col_mismatch", key_columns=["id"])

        assert result.schema_match is False
        assert "sas_only" in result.missing_in_spark
        assert "aws_only" in result.extra_in_spark
        assert result.overall_pass is False
        assert any("missing" in n.lower() for n in result.notes)
        assert any("extra" in n.lower() for n in result.notes)


# ---------------------------------------------------------------------------
# Value mismatch
# ---------------------------------------------------------------------------

class TestValueMismatch:
    def test_value_diffs_detected(self, value_mismatch_dfs):
        sas, aws = value_mismatch_dfs
        comp = DataComPyComparator()
        result = comp.compare(sas, aws, "val_mismatch", key_columns=["id"])

        assert result.overall_pass is False
        assert result.sample_mismatches > 0
        assert any("value differences" in n.lower() for n in result.notes)

    def test_column_diffs_flag_mismatch(self, value_mismatch_dfs):
        sas, aws = value_mismatch_dfs
        comp = DataComPyComparator()
        result = comp.compare(sas, aws, "val_mismatch", key_columns=["id"])

        metric_diffs = [d for d in result.column_diffs if d.column_name == "metric"]
        assert len(metric_diffs) == 1
        assert metric_diffs[0].match is False


# ---------------------------------------------------------------------------
# Unmatched rows
# ---------------------------------------------------------------------------

class TestUnmatchedRows:
    def test_rows_only_in_sas_or_aws(self, mismatched_rows_dfs):
        sas, aws = mismatched_rows_dfs
        comp = DataComPyComparator()
        result = comp.compare(sas, aws, "unmatched", key_columns=["id"])

        assert result.overall_pass is False
        assert result.sample_mismatches >= 2
        assert any("only in SAS" in n for n in result.notes)
        assert any("only in AWS" in n for n in result.notes)


# ---------------------------------------------------------------------------
# Tolerance
# ---------------------------------------------------------------------------

class TestTolerance:
    def test_within_tolerance_passes(self, tolerance_dfs):
        sas, aws = tolerance_dfs
        comp = DataComPyComparator()
        result = comp.compare(sas, aws, "tol_pass", key_columns=["id"], tolerance=0.01)
        assert result.overall_pass is True

    def test_outside_tolerance_fails(self, tolerance_dfs):
        sas, aws = tolerance_dfs
        comp = DataComPyComparator()
        result = comp.compare(sas, aws, "tol_fail", key_columns=["id"], tolerance=0.0001)
        assert result.overall_pass is False
        assert result.sample_mismatches > 0


# ---------------------------------------------------------------------------
# ColumnDiff backward compat
# ---------------------------------------------------------------------------

class TestColumnDiffCompat:
    def test_spark_property_aliases(self):
        cd = ColumnDiff(
            column_name="col1",
            sas_dtype="float64",
            aws_dtype="double",
            sas_null_count=0,
            aws_null_count=1,
            sas_distinct_count=5,
            aws_distinct_count=5,
        )
        assert cd.spark_dtype == "double"
        assert cd.spark_null_count == 1
        assert cd.spark_distinct_count == 5


# ---------------------------------------------------------------------------
# ComparisonResult dataclass
# ---------------------------------------------------------------------------

class TestComparisonResult:
    def test_defaults(self):
        r = ComparisonResult(
            dataset_name="ds",
            sas_row_count=10,
            spark_row_count=10,
            row_count_match=True,
            schema_match=True,
        )
        assert r.mode == "pandas"
        assert r.column_diffs == []
        assert r.full_report == ""
        assert r.overall_pass is False


# ---------------------------------------------------------------------------
# Auto key column fallback
# ---------------------------------------------------------------------------

class TestAutoKeyFallback:
    def test_no_key_columns_uses_first_column(self, exact_dfs):
        sas, aws = exact_dfs
        comp = DataComPyComparator()
        result = comp.compare(sas, aws, "auto_key")
        assert result.overall_pass is True


# ---------------------------------------------------------------------------
# Golden snapshot (pandas)
# ---------------------------------------------------------------------------

class TestGoldenSnapshotPandas:
    def test_generates_json_and_parquet(self):
        df = pd.DataFrame({"id": [1, 2, 3], "val": [10.0, None, 30.0]})
        with tempfile.TemporaryDirectory() as tmpdir:
            snapshot = generate_golden_snapshot(df, "test_ds", tmpdir)

            assert snapshot["row_count"] == 3
            assert snapshot["column_count"] == 2
            assert "id" in snapshot["column_stats"]
            assert snapshot["column_stats"]["val"]["null_count"] == 1

            json_path = Path(tmpdir) / "test_ds_golden.json"
            assert json_path.exists()
            loaded = json.loads(json_path.read_text())
            assert loaded["dataset_name"] == "test_ds"

            parquet_path = Path(tmpdir) / "test_ds_sample.parquet"
            assert parquet_path.exists()
            reloaded = pd.read_parquet(str(parquet_path))
            assert len(reloaded) == 3
