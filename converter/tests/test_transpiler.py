"""Tests for the SAS-to-PySpark transpiler (Bedrock-backed).

All Bedrock API calls are mocked via a shared fixture so tests run
offline and deterministically.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from analyzer.classifier import Tier
from converter.transpiler import Transpiler

FIXTURES = Path(__file__).resolve().parents[2] / "sas_source"

MOCK_SIMPLE_PYSPARK = """\
# DATA work.customers_filtered; SET mylib.customers;
df_mylib_customers = spark.table("glue_catalog.mylib.customers")
df_customers_filtered = df_mylib_customers
df_customers_filtered = df_customers_filtered.filter("age >= 18 AND status = 'ACTIVE'")
df_customers_filtered = df_customers_filtered.select("customer_id", "name", "age", "status", "region")
df_customers_filtered.write.mode("overwrite").saveAsTable("glue_catalog.work.customers_filtered")

# PROC SORT DATA=work.customers_filtered
df_customers_filtered = spark.table("glue_catalog.work.customers_filtered")
df_customers_filtered = df_customers_filtered.orderBy(F.col("region").asc(), F.col("customer_id").asc())
df_customers_filtered.write.mode("overwrite").saveAsTable("glue_catalog.work.customers_filtered")

# PROC SQL: CREATE TABLE work.region_summary
df_region_summary = spark.sql(\"\"\"
    SELECT region, COUNT(*) AS customer_count, AVG(age) AS avg_age
    FROM glue_catalog.work.customers_filtered
    GROUP BY region
\"\"\")
df_region_summary.write.mode("overwrite").saveAsTable("glue_catalog.work.region_summary")
"""

MOCK_MEDIUM_PYSPARK = """\
# MERGE sales.orders ref.products; BY product_id;
df_sales_orders = spark.table("glue_catalog.sales.orders")
df_ref_products = spark.table("glue_catalog.ref.products")
df_orders_enriched = df_sales_orders.join(df_ref_products, on=["product_id"], how="inner")
df_orders_enriched.write.mode("overwrite").saveAsTable("glue_catalog.work.orders_enriched")

# PROC SORT DATA=work.orders_enriched
df_orders_enriched = spark.table("glue_catalog.work.orders_enriched")
df_orders_enriched = df_orders_enriched.orderBy(F.col("customer_id").asc(), F.col("order_date").asc())
df_orders_enriched.write.mode("overwrite").saveAsTable("glue_catalog.work.orders_enriched")

# DATA work.customer_running_total -- RETAIN + FIRST/LAST
# TODO: Manual review -- verify RETAIN running_total accumulation logic
from transform_utils import retain_accumulate, first_last_flags
df_orders_enriched = spark.table("glue_catalog.work.orders_enriched")
df_customer_running_total = retain_accumulate(
    df_orders_enriched, ["customer_id"], ["order_date"], "total_amount", "running_total"
)
df_customer_running_total = first_last_flags(df_customer_running_total, ["customer_id"], ["order_date"])
df_customer_running_total = df_customer_running_total.filter(F.col("_last_customer_id"))
df_customer_running_total.write.mode("overwrite").saveAsTable("glue_catalog.work.customer_running_total")

# PROC MEANS
df_orders_enriched = spark.table("glue_catalog.work.orders_enriched")
df_tier_stats = df_orders_enriched.groupBy("price_tier").agg(
    F.mean("total_amount").alias("avg_amount"),
    F.sum("quantity").alias("total_qty"),
    F.count("total_amount").alias("order_count")
)
df_tier_stats.write.mode("overwrite").saveAsTable("glue_catalog.work.tier_stats")
"""

MOCK_COMPLEX_PYSPARK = """\
# %MACRO process_monthly -- converted to Python function
# TODO: Manual review -- macro parameters and loop expansion
def process_monthly(spark, year, month):
    df_transactions = spark.table("glue_catalog.raw.transactions")
    df_monthly = df_transactions.filter(
        (F.month("txn_date") == month) & (F.year("txn_date") == year)
    )
    df_monthly = df_monthly.select("txn_id", "customer_id", "txn_date", "amount", "category", "store_id")
    df_monthly.write.mode("overwrite").saveAsTable("glue_catalog.work.monthly_extract")

    df_customer_txns = spark.sql(\"\"\"
        SELECT a.*, b.segment, b.region, b.acquisition_date
        FROM glue_catalog.work.monthly_extract a
        LEFT JOIN glue_catalog.raw.customer_dim b ON a.customer_id = b.customer_id
    \"\"\")
    df_customer_txns.write.mode("overwrite").saveAsTable("glue_catalog.work.customer_txns")
    return df_customer_txns

for m in range(1, 13):
    process_monthly(spark, 2025, m)

# PROC SQL: CREATE TABLE mart.customer_summary
df_summary = spark.sql(\"\"\"
    SELECT customer_id, segment, region, tenure_group,
           SUM(amount) AS total_spend, COUNT(*) AS txn_count,
           MAX(txn_date) AS last_txn_date
    FROM glue_catalog.stage.yearly_txns
    GROUP BY customer_id, segment, region, tenure_group
\"\"\")
df_summary.write.mode("overwrite").saveAsTable("glue_catalog.mart.customer_summary")
"""

_CONTENT_MARKERS = [
    ("MACRO process_monthly", MOCK_COMPLEX_PYSPARK),
    ("MERGE sales.orders", MOCK_MEDIUM_PYSPARK),
    ("orders_enriched", MOCK_MEDIUM_PYSPARK),
    ("%MACRO", MOCK_COMPLEX_PYSPARK),
]


def _bedrock_response(text: str):
    return {
        "output": {"message": {"content": [{"text": f"```python\n{text}\n```"}]}},
        "usage": {"inputTokens": 150, "outputTokens": 300},
    }


def _smart_converse(**kwargs):
    """Return the right mock response based on the SAS content in the prompt."""
    messages = kwargs.get("messages", [])
    user_text = ""
    for m in reversed(messages):
        if m.get("role") == "user":
            user_text = m["content"][0]["text"]
            break

    for marker, code in _CONTENT_MARKERS:
        if marker in user_text:
            return _bedrock_response(code)

    return _bedrock_response(MOCK_SIMPLE_PYSPARK)


@pytest.fixture
def mock_bedrock_client():
    client = MagicMock()
    client.converse.side_effect = _smart_converse
    return client


@pytest.fixture
def transpiler(tmp_path, mock_bedrock_client):
    return Transpiler(
        s3_scripts_bucket="test-bucket",
        bedrock_client=mock_bedrock_client,
    )


class TestTranspilerSimple:
    def test_converts_simple_file(self, transpiler, tmp_path):
        result = transpiler.transpile_file(
            FIXTURES / "sample_simple.sas", tmp_path
        )
        assert result.tier == Tier.GREEN
        assert result.blocks_converted > 0
        assert result.output_file is not None
        assert Path(result.output_file).exists()

    def test_simple_contains_spark_read(self, transpiler, tmp_path):
        result = transpiler.transpile_file(
            FIXTURES / "sample_simple.sas", tmp_path
        )
        assert "spark.table" in result.pyspark_code or "spark.sql" in result.pyspark_code

    def test_simple_contains_glue_boilerplate(self, transpiler, tmp_path):
        result = transpiler.transpile_file(
            FIXTURES / "sample_simple.sas", tmp_path
        )
        assert "GlueContext" in result.pyspark_code
        assert "job.init" in result.pyspark_code
        assert "job.commit()" in result.pyspark_code

    def test_simple_contains_filter(self, transpiler, tmp_path):
        result = transpiler.transpile_file(
            FIXTURES / "sample_simple.sas", tmp_path
        )
        assert "filter" in result.pyspark_code

    def test_simple_contains_orderby(self, transpiler, tmp_path):
        result = transpiler.transpile_file(
            FIXTURES / "sample_simple.sas", tmp_path
        )
        assert "orderBy" in result.pyspark_code


class TestTranspilerMedium:
    def test_converts_medium_file(self, transpiler, tmp_path):
        result = transpiler.transpile_file(
            FIXTURES / "sample_medium.sas", tmp_path
        )
        assert result.tier == Tier.YELLOW
        assert result.blocks_converted > 0

    def test_medium_contains_join(self, transpiler, tmp_path):
        result = transpiler.transpile_file(
            FIXTURES / "sample_medium.sas", tmp_path
        )
        assert "join" in result.pyspark_code.lower()

    def test_medium_has_warnings(self, transpiler, tmp_path):
        result = transpiler.transpile_file(
            FIXTURES / "sample_medium.sas", tmp_path
        )
        assert len(result.warnings) > 0


class TestTranspilerComplex:
    def test_converts_complex_file(self, transpiler, tmp_path):
        result = transpiler.transpile_file(
            FIXTURES / "sample_complex.sas", tmp_path
        )
        assert result.tier == Tier.RED
        assert result.manual_review_needed

    def test_complex_has_macro_warnings(self, transpiler, tmp_path):
        result = transpiler.transpile_file(
            FIXTURES / "sample_complex.sas", tmp_path
        )
        todo_in_code = "# TODO" in result.pyspark_code
        has_warnings = len(result.warnings) > 0
        assert todo_in_code or has_warnings


class TestBatchConversion:
    def test_batch_converts_directory(self, transpiler, tmp_path):
        results = transpiler.transpile_directory(FIXTURES, tmp_path)
        assert len(results) >= 3

        output_files = list(tmp_path.rglob("*.py"))
        assert len(output_files) >= 3

    def test_batch_produces_valid_python(self, transpiler, tmp_path):
        results = transpiler.transpile_directory(FIXTURES, tmp_path)
        for result in results:
            if result.output_file:
                code = Path(result.output_file).read_text()
                try:
                    compile(code, result.output_file, "exec")
                except SyntaxError as e:
                    pytest.fail(
                        f"Generated code for {result.source_file} has "
                        f"syntax error: {e}"
                    )
