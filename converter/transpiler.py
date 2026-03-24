"""Core SAS transpiler engine (Bedrock-powered).

Orchestrates the conversion pipeline: parse -> classify -> send to
AWS Bedrock foundation model -> assemble final output from Jinja2 template.
Supports two targets: Glue (PySpark) and Lambda (pandas).
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from analyzer.classifier import Tier, classify
from analyzer.sas_parser import BlockType, parse_sas_file
from converter.bedrock_converter import BedrockConverter


_TEMPLATE_MAP = {
    "glue": "glue_job.py.j2",
    "lambda": "lambda_handler.py.j2",
}

_OUTPUT_DIR_MAP = {
    "glue": "glue_jobs/jobs",
    "lambda": "lambda_jobs/handlers",
}


@dataclass
class TranspileResult:
    source_file: str
    output_file: str | None
    tier: Tier
    score: int
    success: bool
    generated_code: str
    target: str = "glue"
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    blocks_converted: int = 0
    blocks_skipped: int = 0
    blocks_failed: int = 0
    manual_review_needed: bool = False

    @property
    def pyspark_code(self) -> str:
        """Backward-compatible alias for ``generated_code``."""
        return self.generated_code


class Transpiler:
    """Main transpiler: converts SAS files via Bedrock to Glue or Lambda targets."""

    def __init__(
        self,
        template_dir: str | Path | None = None,
        s3_scripts_bucket: str = "glue-scripts-bucket",
        s3_data_bucket: str = "data-bucket",
        model_id: str = "claude-sonnet",
        region: str | None = None,
        max_retries: int = 2,
        default_target: str = "glue",
        *,
        bedrock_client: object | None = None,
    ):
        if template_dir is None:
            template_dir = Path(__file__).parent / "templates"
        self._template_dir = Path(template_dir)
        self._s3_scripts_bucket = s3_scripts_bucket
        self._s3_data_bucket = s3_data_bucket
        self._default_target = default_target

        self._bedrock = BedrockConverter(
            model_id=model_id,
            region=region,
            max_retries=max_retries,
            client=bedrock_client,
        )

        self._jinja_env = Environment(
            loader=FileSystemLoader(str(self._template_dir)),
            keep_trailing_newline=True,
        )

    def transpile_file(
        self,
        sas_path: str | Path,
        output_dir: str | Path | None = None,
        *,
        target: str | None = None,
    ) -> TranspileResult:
        """Convert a single SAS file to a Glue (PySpark) or Lambda (pandas) job."""
        target = target or self._default_target
        sas_path = Path(sas_path)
        sas_code = sas_path.read_text()

        parse_result = parse_sas_file(sas_path)
        classification = classify(parse_result)

        all_warnings: list[str] = list(parse_result.errors)
        all_errors: list[str] = []

        all_inputs: list[str] = []
        all_outputs: list[str] = []
        construct_flags: dict[str, bool] = {}
        for block in parse_result.blocks:
            all_inputs.extend(block.inputs)
            all_outputs.extend(block.outputs)
            for key, val in block.attributes.items():
                if isinstance(val, bool) and val:
                    construct_flags[key] = True

        metadata = {
            "tier": classification.tier.value,
            "score": classification.score,
            "line_count": parse_result.line_count,
            "inputs": sorted(set(all_inputs)),
            "outputs": sorted(set(all_outputs)),
            "construct_flags": construct_flags,
        }

        try:
            bedrock_result = self._bedrock.convert(
                sas_code, metadata, target=target
            )
        except Exception as exc:
            all_errors.append(f"Bedrock conversion failed: {exc}")
            return TranspileResult(
                source_file=str(sas_path),
                output_file=None,
                tier=classification.tier,
                score=classification.score,
                success=False,
                generated_code="",
                target=target,
                warnings=all_warnings,
                errors=all_errors,
                blocks_failed=len(parse_result.blocks),
                manual_review_needed=True,
            )

        all_warnings.extend(bedrock_result.warnings)
        code_blocks = bedrock_result.code_blocks
        manual_review = any("# TODO" in b for b in code_blocks)

        needs_types = (
            any("from pyspark.sql.types" in b for b in code_blocks)
            if target == "glue"
            else False
        )
        needs_io_utils = target == "lambda" and any(
            "transform_utils" in b for b in code_blocks
        )

        n_data_blocks = sum(
            1 for b in parse_result.blocks
            if b.block_type not in {
                BlockType.LIBNAME, BlockType.FILENAME, BlockType.INCLUDE,
                BlockType.OPTIONS, BlockType.COMMENT, BlockType.UNKNOWN,
            }
        )
        skipped = sum(
            1 for b in parse_result.blocks
            if b.block_type in {
                BlockType.LIBNAME, BlockType.FILENAME, BlockType.INCLUDE,
                BlockType.OPTIONS, BlockType.COMMENT, BlockType.UNKNOWN,
            }
        )

        template_name = _TEMPLATE_MAP.get(target, "glue_job.py.j2")
        try:
            template = self._jinja_env.get_template(template_name)
            generated_code = template.render(
                source_file=sas_path.name,
                timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                tier=classification.tier.value,
                s3_scripts_bucket=self._s3_scripts_bucket,
                s3_data_bucket=self._s3_data_bucket,
                needs_types=needs_types,
                needs_io_utils=needs_io_utils,
                extra_args="",
                blocks=code_blocks,
            )
        except Exception as exc:
            all_errors.append(f"Template rendering error: {exc}")
            generated_code = "\n\n".join(code_blocks)

        output_file = None
        if output_dir:
            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = str(output_dir / sas_path.stem) + ".py"
            Path(output_file).write_text(generated_code)

        success = len(all_errors) == 0 and not manual_review

        return TranspileResult(
            source_file=str(sas_path),
            output_file=output_file,
            tier=classification.tier,
            score=classification.score,
            success=success,
            generated_code=generated_code,
            target=target,
            warnings=all_warnings,
            errors=all_errors,
            blocks_converted=n_data_blocks,
            blocks_skipped=skipped,
            blocks_failed=0 if success else 1,
            manual_review_needed=manual_review,
        )

    def transpile_directory(
        self,
        input_dir: str | Path,
        output_dir: str | Path,
        *,
        target: str | None = None,
        lambda_list: set[str] | None = None,
    ) -> list[TranspileResult]:
        """Convert all SAS files in a directory.

        Parameters
        ----------
        lambda_list:
            Optional set of SAS filenames (e.g. ``{"script_a.sas"}``)
            that should be converted with ``target="lambda"`` regardless
            of the *target* parameter. All other files use *target*.
        """
        target = target or self._default_target
        input_dir = Path(input_dir)
        output_dir = Path(output_dir)
        results: list[TranspileResult] = []

        sas_files = sorted(input_dir.rglob("*.sas"))
        for sas_file in sas_files:
            file_target = target
            if lambda_list and sas_file.name in lambda_list:
                file_target = "lambda"

            rel = sas_file.relative_to(input_dir)
            file_output_dir = output_dir / rel.parent
            result = self.transpile_file(
                sas_file, file_output_dir, target=file_target
            )
            results.append(result)

        return results
