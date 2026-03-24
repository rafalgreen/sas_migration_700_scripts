"""Tests for the AWS Bedrock SAS converter."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from converter.bedrock_converter import (
    SUPPORTED_MODELS,
    BedrockConversionResult,
    BedrockConverter,
)
from converter.prompts import SYSTEM_PROMPT, FEW_SHOT_EXAMPLES, build_conversion_prompt

FIXTURES = Path(__file__).resolve().parents[2] / "sas_source"

VALID_PYSPARK = """\
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
"""

VALID_PYSPARK_WITH_TODO = """\
df = spark.table("glue_catalog.raw.transactions")
# TODO: Manual review -- macro parameter expansion
df = df.filter("amount > 0")
"""


def _bedrock_response(text: str, input_tokens: int = 100, output_tokens: int = 200):
    """Build a mock Converse API response dict."""
    return {
        "output": {"message": {"content": [{"text": text}]}},
        "usage": {"inputTokens": input_tokens, "outputTokens": output_tokens},
    }


def _make_throttle_error():
    return ClientError(
        {"Error": {"Code": "ThrottlingException", "Message": "Rate exceeded"}},
        "Converse",
    )


# -----------------------------------------------------------------------
# Unit tests — no Bedrock calls
# -----------------------------------------------------------------------


class TestSupportedModels:
    def test_registry_not_empty(self):
        assert len(SUPPORTED_MODELS) >= 4

    def test_known_models(self):
        assert "claude-sonnet" in SUPPORTED_MODELS
        assert "claude-haiku" in SUPPORTED_MODELS
        assert "nova-pro" in SUPPORTED_MODELS
        assert "nova-lite" in SUPPORTED_MODELS

    def test_model_ids_are_strings(self):
        for name, mid in SUPPORTED_MODELS.items():
            assert isinstance(mid, str), f"{name} → {mid}"


class TestPromptConstruction:
    def test_build_messages_includes_sas_code(self):
        mock_client = MagicMock()
        converter = BedrockConverter(client=mock_client)
        sas_code = "DATA work.test; SET lib.input; RUN;"
        metadata = {"tier": "GREEN", "score": 5, "line_count": 3}

        messages = converter._build_messages(sas_code, metadata)

        user_turns = [m for m in messages if m["role"] == "user"]
        last_user_text = user_turns[-1]["content"][0]["text"]
        assert sas_code in last_user_text

    def test_build_messages_includes_metadata(self):
        mock_client = MagicMock()
        converter = BedrockConverter(client=mock_client)
        sas_code = "DATA work.test; SET lib.input; RUN;"
        metadata = {
            "tier": "YELLOW",
            "score": 25,
            "line_count": 50,
            "inputs": ["lib.input"],
            "outputs": ["work.test"],
            "construct_flags": {"has_merge": True, "has_retain": True},
        }

        messages = converter._build_messages(sas_code, metadata)

        last_user_text = messages[-1]["content"][0]["text"]
        assert "YELLOW" in last_user_text
        assert "25" in last_user_text
        assert "lib.input" in last_user_text
        assert "has_merge" in last_user_text

    def test_build_messages_has_few_shot_examples(self):
        mock_client = MagicMock()
        converter = BedrockConverter(client=mock_client)
        messages = converter._build_messages("DATA x; RUN;", {"tier": "GREEN"})

        assert len(messages) >= 3  # at least 1 example pair + 1 user turn
        assert messages[0]["role"] == "user"
        assert messages[1]["role"] == "assistant"

    def test_build_conversion_prompt_minimal(self):
        prompt = build_conversion_prompt("DATA x; RUN;", {"tier": "GREEN"})
        assert "DATA x; RUN;" in prompt
        assert "GREEN" in prompt

    def test_build_conversion_prompt_with_flags(self):
        prompt = build_conversion_prompt(
            "DATA x; RUN;",
            {"tier": "RED", "score": 55, "construct_flags": {"has_array": True}},
        )
        assert "has_array" in prompt


class TestCodeExtraction:
    def test_extract_from_markdown_fence(self):
        response = "Here is the code:\n```python\ndf = spark.table('x')\n```\nDone."
        blocks = BedrockConverter._extract_code(response)
        assert len(blocks) == 1
        assert "spark.table" in blocks[0]

    def test_extract_from_pyspark_fence(self):
        response = "```pyspark\ndf = spark.table('x')\ndf.show()\n```"
        blocks = BedrockConverter._extract_code(response)
        assert len(blocks) == 1
        assert "df.show()" in blocks[0]

    def test_extract_multiple_fences(self):
        response = "```python\nblock1()\n```\nand\n```python\nblock2()\n```"
        blocks = BedrockConverter._extract_code(response)
        assert len(blocks) == 2

    def test_extract_plain_text_fallback(self):
        response = "df = spark.table('x')\ndf.show()"
        blocks = BedrockConverter._extract_code(response)
        assert len(blocks) == 1
        assert "spark.table" in blocks[0]

    def test_extract_empty_fence_skipped(self):
        response = "```python\n\n```\n```python\ndf = spark.table('x')\n```"
        blocks = BedrockConverter._extract_code(response)
        assert len(blocks) == 1


class TestSyntaxValidation:
    def test_valid_python(self):
        assert BedrockConverter._validate_syntax("x = 1\ny = x + 2") is None

    def test_invalid_python(self):
        err = BedrockConverter._validate_syntax("def foo(:\n  pass")
        assert err is not None
        assert "Line" in err

    def test_multiline_valid(self):
        code = (
            "df = spark.table('x')\n"
            "df = df.filter('a > 1')\n"
            "df.write.mode('overwrite').saveAsTable('y')\n"
        )
        assert BedrockConverter._validate_syntax(code) is None


# -----------------------------------------------------------------------
# Integration tests — mocked Bedrock API
# -----------------------------------------------------------------------


class TestBedrockConverterIntegration:
    def test_convert_simple_sas(self):
        mock_client = MagicMock()
        mock_client.converse.return_value = _bedrock_response(
            f"```python\n{VALID_PYSPARK}\n```"
        )

        converter = BedrockConverter(client=mock_client)
        result = converter.convert(
            "DATA work.x; SET lib.y; RUN;",
            {"tier": "GREEN", "score": 5},
        )

        assert isinstance(result, BedrockConversionResult)
        assert len(result.code_blocks) >= 1
        assert "spark.table" in result.code_blocks[0]
        assert result.retries_used == 0
        assert result.input_tokens == 100
        assert result.output_tokens == 200
        mock_client.converse.assert_called_once()

    def test_convert_passes_system_prompt(self):
        mock_client = MagicMock()
        mock_client.converse.return_value = _bedrock_response(
            f"```python\n{VALID_PYSPARK}\n```"
        )

        converter = BedrockConverter(client=mock_client)
        converter.convert("DATA x; RUN;", {"tier": "GREEN"})

        call_kwargs = mock_client.converse.call_args[1]
        system_text = call_kwargs["system"][0]["text"]
        assert "AWS Glue 4.0" in system_text
        assert "transform_utils" in system_text

    def test_convert_handles_throttle_retry(self):
        mock_client = MagicMock()
        mock_client.converse.side_effect = [
            _make_throttle_error(),
            _bedrock_response(f"```python\n{VALID_PYSPARK}\n```"),
        ]

        with patch("converter.bedrock_converter.time.sleep"):
            converter = BedrockConverter(client=mock_client)
            result = converter.convert("DATA x; RUN;", {"tier": "GREEN"})

        assert len(result.code_blocks) >= 1
        assert mock_client.converse.call_count == 2

    def test_convert_retries_on_syntax_error(self):
        bad_code = "def foo(:\n  pass"
        good_code = VALID_PYSPARK

        mock_client = MagicMock()
        mock_client.converse.side_effect = [
            _bedrock_response(f"```python\n{bad_code}\n```"),
            _bedrock_response(f"```python\n{good_code}\n```"),
        ]

        converter = BedrockConverter(client=mock_client, max_retries=2)
        result = converter.convert("DATA x; RUN;", {"tier": "GREEN"})

        assert result.retries_used == 1
        assert "spark.table" in result.code_blocks[0]
        assert mock_client.converse.call_count == 2

    def test_convert_exhausts_retries(self):
        bad_code = "def foo(:\n  pass"

        mock_client = MagicMock()
        mock_client.converse.return_value = _bedrock_response(
            f"```python\n{bad_code}\n```"
        )

        converter = BedrockConverter(client=mock_client, max_retries=1)
        result = converter.convert("DATA x; RUN;", {"tier": "GREEN"})

        assert result.retries_used == 1
        assert any("Syntax error" in w for w in result.warnings)

    def test_convert_complex_sas_with_todo(self):
        mock_client = MagicMock()
        mock_client.converse.return_value = _bedrock_response(
            f"```python\n{VALID_PYSPARK_WITH_TODO}\n```"
        )

        converter = BedrockConverter(client=mock_client)
        result = converter.convert(
            "%MACRO foo; DATA x; RUN; %MEND;",
            {"tier": "RED", "score": 55},
        )

        assert any("TODO" in w for w in result.warnings)

    def test_custom_model_id_forwarded(self):
        mock_client = MagicMock()
        mock_client.converse.return_value = _bedrock_response(
            f"```python\n{VALID_PYSPARK}\n```"
        )

        converter = BedrockConverter(model_id="nova-pro", client=mock_client)
        converter.convert("DATA x; RUN;", {"tier": "GREEN"})

        call_kwargs = mock_client.converse.call_args[1]
        assert call_kwargs["modelId"] == "amazon.nova-pro-v1:0"

    def test_raw_model_id_passthrough(self):
        mock_client = MagicMock()
        mock_client.converse.return_value = _bedrock_response(
            f"```python\n{VALID_PYSPARK}\n```"
        )

        converter = BedrockConverter(
            model_id="anthropic.claude-3-opus-20240229-v1:0",
            client=mock_client,
        )
        converter.convert("DATA x; RUN;", {"tier": "GREEN"})

        call_kwargs = mock_client.converse.call_args[1]
        assert call_kwargs["modelId"] == "anthropic.claude-3-opus-20240229-v1:0"

    def test_non_retryable_error_raises(self):
        mock_client = MagicMock()
        mock_client.converse.side_effect = ClientError(
            {"Error": {"Code": "ValidationException", "Message": "Bad request"}},
            "Converse",
        )

        converter = BedrockConverter(client=mock_client)
        with pytest.raises(ClientError):
            converter.convert("DATA x; RUN;", {"tier": "GREEN"})


# -----------------------------------------------------------------------
# End-to-end with Transpiler (mocked Bedrock)
# -----------------------------------------------------------------------


class TestEndToEnd:
    def test_transpile_file_uses_bedrock(self, tmp_path):
        mock_client = MagicMock()
        mock_client.converse.return_value = _bedrock_response(
            f"```python\n{VALID_PYSPARK}\n```"
        )

        from converter.transpiler import Transpiler

        transpiler = Transpiler(
            s3_scripts_bucket="test-bucket",
            bedrock_client=mock_client,
        )

        result = transpiler.transpile_file(
            FIXTURES / "sample_simple.sas", tmp_path
        )

        assert result.output_file is not None
        assert Path(result.output_file).exists()
        assert "GlueContext" in result.pyspark_code
        assert "job.commit()" in result.pyspark_code
        assert "spark.table" in result.pyspark_code
        mock_client.converse.assert_called_once()

    def test_transpile_file_reports_tier(self, tmp_path):
        mock_client = MagicMock()
        mock_client.converse.return_value = _bedrock_response(
            f"```python\n{VALID_PYSPARK}\n```"
        )

        from converter.transpiler import Transpiler

        transpiler = Transpiler(bedrock_client=mock_client)
        result = transpiler.transpile_file(
            FIXTURES / "sample_simple.sas", tmp_path
        )

        from analyzer.classifier import Tier

        assert result.tier == Tier.GREEN
        assert result.score > 0

    def test_transpile_directory_batch(self, tmp_path):
        mock_client = MagicMock()
        mock_client.converse.return_value = _bedrock_response(
            f"```python\n{VALID_PYSPARK}\n```"
        )

        from converter.transpiler import Transpiler

        transpiler = Transpiler(bedrock_client=mock_client)
        results = transpiler.transpile_directory(FIXTURES, tmp_path)

        assert len(results) >= 3
        output_files = list(tmp_path.rglob("*.py"))
        assert len(output_files) >= 3

    def test_transpile_produces_valid_python(self, tmp_path):
        mock_client = MagicMock()
        mock_client.converse.return_value = _bedrock_response(
            f"```python\n{VALID_PYSPARK}\n```"
        )

        from converter.transpiler import Transpiler

        transpiler = Transpiler(bedrock_client=mock_client)
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

    def test_transpile_bedrock_failure_graceful(self, tmp_path):
        mock_client = MagicMock()
        mock_client.converse.side_effect = ClientError(
            {"Error": {"Code": "ServiceUnavailableException", "Message": "Down"}},
            "Converse",
        )

        from converter.transpiler import Transpiler

        transpiler = Transpiler(bedrock_client=mock_client)
        result = transpiler.transpile_file(
            FIXTURES / "sample_simple.sas", tmp_path
        )

        assert not result.success
        assert result.manual_review_needed
        assert any("Bedrock conversion failed" in e for e in result.errors)
