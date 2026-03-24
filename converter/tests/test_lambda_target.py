"""Tests for the Lambda (pandas) compute target.

Verifies that the Lambda prompt, template, and transpiler routing
work correctly when target="lambda".
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from analyzer.classifier import Tier
from converter.bedrock_converter import BedrockConverter, BedrockConversionResult
from converter.prompts import (
    SYSTEM_PROMPT,
    SYSTEM_PROMPT_LAMBDA,
    FEW_SHOT_EXAMPLES_LAMBDA,
    build_conversion_prompt,
)
from converter.transpiler import Transpiler

FIXTURES = Path(__file__).resolve().parents[2] / "sas_source"

MOCK_PANDAS_CODE = """\
# DATA work.customers_filtered; SET mylib.customers;
df_customers = pd.read_parquet("s3://data-bucket/mylib/customers.parquet")
df_customers_filtered = df_customers.query("age >= 18 and status == 'ACTIVE'")
df_customers_filtered = df_customers_filtered[["customer_id", "name", "age", "status", "region"]]

# PROC SORT DATA=work.customers_filtered
df_customers_filtered = df_customers_filtered.sort_values(["region", "customer_id"])

# PROC SQL: CREATE TABLE work.region_summary
df_region_summary = (
    df_customers_filtered
    .groupby("region", as_index=False)
    .agg(customer_count=("customer_id", "count"), avg_age=("age", "mean"))
)
"""


def _bedrock_response(text: str, input_tokens: int = 100, output_tokens: int = 200):
    return {
        "output": {"message": {"content": [{"text": text}]}},
        "usage": {"inputTokens": input_tokens, "outputTokens": output_tokens},
    }


# -----------------------------------------------------------------------
# Prompt tests
# -----------------------------------------------------------------------


class TestLambdaPrompts:
    def test_system_prompt_lambda_mentions_pandas(self):
        assert "pandas" in SYSTEM_PROMPT_LAMBDA
        assert "Lambda" in SYSTEM_PROMPT_LAMBDA

    def test_system_prompt_lambda_no_glue(self):
        assert "GlueContext" not in SYSTEM_PROMPT_LAMBDA
        assert "SparkSession" not in SYSTEM_PROMPT_LAMBDA

    def test_few_shot_examples_lambda_has_pandas_key(self):
        assert len(FEW_SHOT_EXAMPLES_LAMBDA) >= 1
        for ex in FEW_SHOT_EXAMPLES_LAMBDA:
            assert "pandas" in ex, "Lambda few-shot example must have 'pandas' key"
            assert "pd.read_parquet" in ex["pandas"] or "pd.read_csv" in ex["pandas"]

    def test_build_conversion_prompt_lambda_label(self):
        prompt = build_conversion_prompt(
            "DATA x; RUN;", {"tier": "GREEN"}, target="lambda"
        )
        assert "pandas" in prompt

    def test_build_conversion_prompt_glue_label(self):
        prompt = build_conversion_prompt(
            "DATA x; RUN;", {"tier": "GREEN"}, target="glue"
        )
        assert "PySpark" in prompt


# -----------------------------------------------------------------------
# BedrockConverter target routing
# -----------------------------------------------------------------------


class TestBedrockConverterLambdaTarget:
    def test_lambda_uses_lambda_system_prompt(self):
        mock_client = MagicMock()
        mock_client.converse.return_value = _bedrock_response(
            f"```python\n{MOCK_PANDAS_CODE}\n```"
        )

        converter = BedrockConverter(client=mock_client)
        converter.convert("DATA x; RUN;", {"tier": "GREEN"}, target="lambda")

        call_kwargs = mock_client.converse.call_args[1]
        system_text = call_kwargs["system"][0]["text"]
        assert "pandas" in system_text
        assert "Lambda" in system_text

    def test_glue_uses_glue_system_prompt(self):
        mock_client = MagicMock()
        mock_client.converse.return_value = _bedrock_response(
            f"```python\ndf = spark.table('x')\n```"
        )

        converter = BedrockConverter(client=mock_client)
        converter.convert("DATA x; RUN;", {"tier": "GREEN"}, target="glue")

        call_kwargs = mock_client.converse.call_args[1]
        system_text = call_kwargs["system"][0]["text"]
        assert "AWS Glue 4.0" in system_text

    def test_lambda_few_shot_uses_pandas_examples(self):
        mock_client = MagicMock()
        mock_client.converse.return_value = _bedrock_response(
            f"```python\n{MOCK_PANDAS_CODE}\n```"
        )

        converter = BedrockConverter(client=mock_client)
        converter.convert("DATA x; RUN;", {"tier": "GREEN"}, target="lambda")

        call_kwargs = mock_client.converse.call_args[1]
        messages = call_kwargs["messages"]
        assistant_text = messages[1]["content"][0]["text"]
        assert "pd.read_parquet" in assistant_text or "pd.read_csv" in assistant_text

    def test_result_target_field(self):
        mock_client = MagicMock()
        mock_client.converse.return_value = _bedrock_response(
            f"```python\n{MOCK_PANDAS_CODE}\n```"
        )

        converter = BedrockConverter(client=mock_client)
        result = converter.convert("DATA x; RUN;", {"tier": "GREEN"}, target="lambda")

        assert result.target == "lambda"
        assert len(result.code_blocks) >= 1

    def test_code_blocks_backward_compat(self):
        mock_client = MagicMock()
        mock_client.converse.return_value = _bedrock_response(
            f"```python\n{MOCK_PANDAS_CODE}\n```"
        )

        converter = BedrockConverter(client=mock_client)
        result = converter.convert("DATA x; RUN;", {"tier": "GREEN"}, target="lambda")

        assert result.pyspark_blocks == result.code_blocks


# -----------------------------------------------------------------------
# Transpiler Lambda target
# -----------------------------------------------------------------------


class TestTranspilerLambdaTarget:
    @pytest.fixture
    def mock_bedrock_client(self):
        client = MagicMock()
        client.converse.return_value = _bedrock_response(
            f"```python\n{MOCK_PANDAS_CODE}\n```"
        )
        return client

    def test_lambda_template_used(self, mock_bedrock_client, tmp_path):
        transpiler = Transpiler(
            s3_scripts_bucket="test-bucket",
            default_target="lambda",
            bedrock_client=mock_bedrock_client,
        )
        result = transpiler.transpile_file(
            FIXTURES / "sample_simple.sas", tmp_path, target="lambda"
        )

        assert result.target == "lambda"
        assert result.output_file is not None
        assert Path(result.output_file).exists()

        code = result.generated_code
        assert "import pandas as pd" in code
        assert "def handler(event, context)" in code
        assert "GlueContext" not in code
        assert "job.commit()" not in code

    def test_glue_template_still_works(self, mock_bedrock_client, tmp_path):
        transpiler = Transpiler(
            s3_scripts_bucket="test-bucket",
            bedrock_client=mock_bedrock_client,
        )
        result = transpiler.transpile_file(
            FIXTURES / "sample_simple.sas", tmp_path, target="glue"
        )

        assert result.target == "glue"
        assert "GlueContext" in result.generated_code
        assert "job.commit()" in result.generated_code

    def test_generated_code_backward_compat(self, mock_bedrock_client, tmp_path):
        transpiler = Transpiler(
            s3_scripts_bucket="test-bucket",
            bedrock_client=mock_bedrock_client,
        )
        result = transpiler.transpile_file(
            FIXTURES / "sample_simple.sas", tmp_path, target="glue"
        )

        assert result.pyspark_code == result.generated_code

    def test_lambda_output_compiles(self, mock_bedrock_client, tmp_path):
        transpiler = Transpiler(
            s3_scripts_bucket="test-bucket",
            bedrock_client=mock_bedrock_client,
        )
        result = transpiler.transpile_file(
            FIXTURES / "sample_simple.sas", tmp_path, target="lambda"
        )
        if result.output_file:
            code = Path(result.output_file).read_text()
            try:
                compile(code, result.output_file, "exec")
            except SyntaxError as e:
                pytest.fail(f"Lambda generated code has syntax error: {e}")

    def test_batch_with_lambda_list(self, mock_bedrock_client, tmp_path):
        transpiler = Transpiler(
            s3_scripts_bucket="test-bucket",
            bedrock_client=mock_bedrock_client,
        )
        lambda_list = {"sample_simple.sas"}
        results = transpiler.transpile_directory(
            FIXTURES, tmp_path, target="glue", lambda_list=lambda_list
        )

        targets = {r.source_file: r.target for r in results}
        for src, tgt in targets.items():
            if Path(src).name == "sample_simple.sas":
                assert tgt == "lambda", f"Expected lambda for sample_simple.sas, got {tgt}"
            else:
                assert tgt == "glue", f"Expected glue for {src}, got {tgt}"

    def test_default_target_lambda(self, mock_bedrock_client, tmp_path):
        transpiler = Transpiler(
            s3_scripts_bucket="test-bucket",
            default_target="lambda",
            bedrock_client=mock_bedrock_client,
        )
        result = transpiler.transpile_file(FIXTURES / "sample_simple.sas", tmp_path)
        assert result.target == "lambda"
