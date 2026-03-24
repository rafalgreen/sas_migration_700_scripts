"""AWS Bedrock-powered SAS conversion engine.

Uses the Bedrock Converse API to send full SAS scripts to a configurable
foundation model (Claude, Nova, etc.) and extract transformation code
targeting either PySpark (Glue) or pandas (Lambda).
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field

import boto3
from botocore.exceptions import ClientError

from converter.prompts import (
    FEW_SHOT_EXAMPLES,
    FEW_SHOT_EXAMPLES_LAMBDA,
    SYSTEM_PROMPT,
    SYSTEM_PROMPT_LAMBDA,
    build_conversion_prompt,
)

logger = logging.getLogger(__name__)

SUPPORTED_MODELS: dict[str, str] = {
    "claude-sonnet": "anthropic.claude-sonnet-4-20250514",
    "claude-haiku": "anthropic.claude-haiku-4-20250514",
    "nova-pro": "amazon.nova-pro-v1:0",
    "nova-lite": "amazon.nova-lite-v1:0",
}

_CODE_FENCE_RE = re.compile(
    r"```(?:python|pyspark)?\s*\n(.*?)```",
    re.DOTALL,
)


@dataclass
class BedrockConversionResult:
    """Outcome of a single Bedrock conversion call."""

    code_blocks: list[str]
    raw_response: str
    model_id: str
    target: str = "glue"
    input_tokens: int = 0
    output_tokens: int = 0
    retries_used: int = 0
    warnings: list[str] = field(default_factory=list)

    @property
    def pyspark_blocks(self) -> list[str]:
        """Backward-compatible alias for ``code_blocks``."""
        return self.code_blocks


class BedrockConverter:
    """Converts SAS scripts to PySpark via AWS Bedrock foundation models."""

    def __init__(
        self,
        model_id: str = "claude-sonnet",
        region: str | None = None,
        max_retries: int = 2,
        temperature: float = 0.0,
        max_tokens: int = 4096,
        *,
        client: object | None = None,
    ):
        resolved_model = SUPPORTED_MODELS.get(model_id, model_id)
        self.model_id = resolved_model
        self.friendly_name = model_id
        self.max_retries = max_retries
        self.temperature = temperature
        self.max_tokens = max_tokens

        if client is not None:
            self._client = client
        else:
            kwargs: dict = {}
            if region:
                kwargs["region_name"] = region
            self._client = boto3.client("bedrock-runtime", **kwargs)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def convert(
        self,
        sas_code: str,
        metadata: dict,
        *,
        target: str = "glue",
    ) -> BedrockConversionResult:
        """Convert a full SAS script to transformation code blocks.

        Parameters
        ----------
        sas_code:
            Complete raw SAS source code.
        metadata:
            Analyzer/classifier output — tier, score, inputs, outputs, etc.
        target:
            ``"glue"`` (PySpark) or ``"lambda"`` (pandas).

        Returns
        -------
        BedrockConversionResult with extracted code blocks.
        """
        if target == "lambda":
            system_prompt = SYSTEM_PROMPT_LAMBDA
        else:
            system_prompt = SYSTEM_PROMPT
        messages = self._build_messages(sas_code, metadata, target=target)

        retries_used = 0
        last_error: str | None = None

        for attempt in range(1 + self.max_retries):
            if attempt > 0:
                retries_used = attempt
                messages = self._append_retry_feedback(messages, last_error or "")

            raw_response, input_tok, output_tok = self._call_bedrock(
                system_prompt, messages
            )

            code_blocks = self._extract_code(raw_response)
            combined = "\n\n".join(code_blocks)
            syntax_err = self._validate_syntax(combined)

            if syntax_err is None:
                warnings: list[str] = []
                if "# TODO" in combined:
                    warnings.append(
                        "Generated code contains TODO markers requiring manual review"
                    )
                return BedrockConversionResult(
                    code_blocks=code_blocks,
                    raw_response=raw_response,
                    model_id=self.model_id,
                    target=target,
                    input_tokens=input_tok,
                    output_tokens=output_tok,
                    retries_used=retries_used,
                    warnings=warnings,
                )

            last_error = syntax_err
            logger.warning(
                "Attempt %d/%d produced invalid syntax: %s",
                attempt + 1,
                1 + self.max_retries,
                syntax_err,
            )

        warnings = [f"Syntax error after {1 + self.max_retries} attempts: {last_error}"]
        return BedrockConversionResult(
            code_blocks=code_blocks,
            raw_response=raw_response,
            model_id=self.model_id,
            target=target,
            input_tokens=input_tok,
            output_tokens=output_tok,
            retries_used=retries_used,
            warnings=warnings,
        )

    # ------------------------------------------------------------------
    # Prompt construction
    # ------------------------------------------------------------------

    def _build_messages(
        self, sas_code: str, metadata: dict, *, target: str = "glue"
    ) -> list[dict]:
        """Build the Converse API message list with few-shot examples."""
        messages: list[dict] = []

        if target == "lambda":
            examples = FEW_SHOT_EXAMPLES_LAMBDA
            output_key = "pandas"
            label = "pandas"
        else:
            examples = FEW_SHOT_EXAMPLES
            output_key = "pyspark"
            label = "PySpark"

        for example in examples:
            messages.append(
                {
                    "role": "user",
                    "content": [
                        {
                            "text": (
                                f"Convert the following SAS script to {label} "
                                "transformation code.\n\n--- SAS SOURCE ---\n\n"
                                + example["sas"]
                            )
                        }
                    ],
                }
            )
            messages.append(
                {
                    "role": "assistant",
                    "content": [
                        {"text": f"```python\n{example[output_key]}\n```"}
                    ],
                }
            )

        user_prompt = build_conversion_prompt(sas_code, metadata, target=target)
        messages.append(
            {"role": "user", "content": [{"text": user_prompt}]}
        )
        return messages

    @staticmethod
    def _append_retry_feedback(
        messages: list[dict], error: str
    ) -> list[dict]:
        """Append an assistant-error-user correction turn for retry."""
        messages.append(
            {
                "role": "user",
                "content": [
                    {
                        "text": (
                            "The code you returned has a Python syntax error:\n"
                            f"{error}\n\n"
                            "Please fix the error and return the corrected code "
                            "in a ```python block."
                        )
                    }
                ],
            }
        )
        return messages

    # ------------------------------------------------------------------
    # Bedrock Converse API call with retry on throttle
    # ------------------------------------------------------------------

    def _call_bedrock(
        self,
        system_prompt: str,
        messages: list[dict],
    ) -> tuple[str, int, int]:
        """Invoke Bedrock Converse API, returning (text, input_tokens, output_tokens).

        Retries with exponential backoff on throttling.
        """
        max_api_retries = 5
        backoff = 1.0

        for attempt in range(max_api_retries):
            try:
                response = self._client.converse(
                    modelId=self.model_id,
                    system=[{"text": system_prompt}],
                    messages=messages,
                    inferenceConfig={
                        "temperature": self.temperature,
                        "maxTokens": self.max_tokens,
                    },
                )
                text = response["output"]["message"]["content"][0]["text"]
                usage = response.get("usage", {})
                return (
                    text,
                    usage.get("inputTokens", 0),
                    usage.get("outputTokens", 0),
                )
            except ClientError as exc:
                code = exc.response["Error"]["Code"]
                if code in ("ThrottlingException", "TooManyRequestsException"):
                    if attempt < max_api_retries - 1:
                        logger.warning(
                            "Bedrock throttled (attempt %d/%d), backing off %.1fs",
                            attempt + 1,
                            max_api_retries,
                            backoff,
                        )
                        time.sleep(backoff)
                        backoff *= 2
                        continue
                raise

        raise RuntimeError("Bedrock API retries exhausted")  # pragma: no cover

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_code(response_text: str) -> list[str]:
        """Extract Python code from markdown-fenced blocks in the LLM response.

        If no fenced blocks are found, treats the whole response as code.
        """
        matches = _CODE_FENCE_RE.findall(response_text)
        if matches:
            return [m.strip() for m in matches if m.strip()]
        return [response_text.strip()]

    @staticmethod
    def _validate_syntax(code: str) -> str | None:
        """Compile-check *code*; return the error message or ``None`` if valid."""
        try:
            compile(code, "<bedrock_output>", "exec")
            return None
        except SyntaxError as exc:
            return f"Line {exc.lineno}: {exc.msg}"
