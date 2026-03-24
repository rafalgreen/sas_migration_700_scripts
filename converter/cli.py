"""CLI entry point for the SAS converter (Bedrock-powered).

Usage:
    sas-convert run --input sas_source/ --output glue_jobs/jobs/
    sas-convert run --input sas_source/ --target lambda --output lambda_jobs/handlers/
    sas-convert run --input sas_source/ --lambda-list lambda_scripts.txt
    sas-convert single --file my_script.sas --target lambda
    sas-convert models
"""

from __future__ import annotations

import csv
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from converter.bedrock_converter import SUPPORTED_MODELS
from converter.transpiler import Transpiler, TranspileResult

console = Console()


_TARGET_CHOICES = click.Choice(["glue", "lambda"], case_sensitive=False)


@click.group()
def main():
    """SAS Transpiler -- convert SAS scripts to AWS Glue or Lambda jobs via Bedrock."""


@main.command()
@click.option("--input", "-i", "input_dir", required=True, help="Directory containing .sas files")
@click.option("--output", "-o", "output_dir", default=None, help="Output directory (auto-detected from target if omitted)")
@click.option("--s3-bucket", default="glue-scripts-bucket", help="S3 bucket for Glue scripts")
@click.option("--report", "-r", "report_path", default="reports/conversion_report.csv", help="Conversion report path")
@click.option("--model", "-m", default="claude-sonnet", help=f"Bedrock model [{', '.join(SUPPORTED_MODELS)}]")
@click.option("--region", default=None, help="AWS region for Bedrock (default: boto3 default)")
@click.option("--target", "-t", type=_TARGET_CHOICES, default="glue", help="Compute target: glue (PySpark) or lambda (pandas)")
@click.option("--lambda-list", "lambda_list_path", default=None, type=click.Path(exists=True), help="Text file listing .sas filenames (one per line) to route to Lambda; rest go to --target")
def run(
    input_dir: str,
    output_dir: str | None,
    s3_bucket: str,
    report_path: str,
    model: str,
    region: str | None,
    target: str,
    lambda_list_path: str | None,
):
    """Batch-convert all SAS files in a directory."""
    if output_dir is None:
        output_dir = "glue_jobs/jobs" if target == "glue" else "lambda_jobs/handlers"

    transpiler = Transpiler(
        s3_scripts_bucket=s3_bucket,
        model_id=model,
        region=region,
        default_target=target,
    )

    input_path = Path(input_dir)
    if not input_path.exists():
        console.print(f"[red]Error: Input directory '{input_dir}' not found[/red]")
        raise SystemExit(1)

    lambda_list: set[str] | None = None
    if lambda_list_path:
        lambda_list = {
            line.strip()
            for line in Path(lambda_list_path).read_text().splitlines()
            if line.strip()
        }
        console.print(f"[blue]Lambda-list loaded: {len(lambda_list)} scripts routed to Lambda[/blue]")

    sas_files = sorted(input_path.rglob("*.sas"))
    console.print(f"[blue]Converting {len(sas_files)} SAS files using model '{model}' (target: {target})...[/blue]")

    results = transpiler.transpile_directory(
        input_dir, output_dir, target=target, lambda_list=lambda_list
    )

    _print_results(results)
    _write_report(results, report_path)

    console.print(f"\n[green]Output written to: {output_dir}/[/green]")
    console.print(f"[green]Report written to: {report_path}[/green]")


@main.command()
@click.option("--file", "-f", "sas_file", required=True, help="Single .sas file to convert")
@click.option("--output", "-o", "output_dir", default=None, help="Output directory (auto-detected from target if omitted)")
@click.option("--s3-bucket", default="glue-scripts-bucket", help="S3 bucket for Glue scripts")
@click.option("--model", "-m", default="claude-sonnet", help=f"Bedrock model [{', '.join(SUPPORTED_MODELS)}]")
@click.option("--region", default=None, help="AWS region for Bedrock (default: boto3 default)")
@click.option("--target", "-t", type=_TARGET_CHOICES, default="glue", help="Compute target: glue (PySpark) or lambda (pandas)")
def single(sas_file: str, output_dir: str | None, s3_bucket: str, model: str, region: str | None, target: str):
    """Convert a single SAS file."""
    if output_dir is None:
        output_dir = "glue_jobs/jobs" if target == "glue" else "lambda_jobs/handlers"

    transpiler = Transpiler(
        s3_scripts_bucket=s3_bucket,
        model_id=model,
        region=region,
        default_target=target,
    )
    result = transpiler.transpile_file(sas_file, output_dir, target=target)

    console.print(f"\n[bold]Conversion result for {Path(sas_file).name} (model: {model}, target: {target}):[/bold]")
    _print_single_result(result)

    if result.output_file:
        console.print(f"\n[green]Output: {result.output_file}[/green]")


@main.command()
def models():
    """List supported Bedrock foundation models."""
    table = Table(title="Supported Bedrock Models")
    table.add_column("Friendly Name", style="bold")
    table.add_column("Bedrock Model ID")

    for name, model_id in SUPPORTED_MODELS.items():
        table.add_row(name, model_id)

    console.print(table)


def _print_results(results: list[TranspileResult]) -> None:
    green = sum(1 for r in results if r.success and not r.manual_review_needed)
    yellow = sum(1 for r in results if r.success and r.manual_review_needed)
    red = sum(1 for r in results if not r.success)

    total = len(results)
    table = Table(title="Conversion Summary")
    table.add_column("Category", style="bold")
    table.add_column("Count", justify="right")
    table.add_column("Percentage", justify="right")

    table.add_row("Total scripts", str(total), "100%")
    table.add_row(
        "[green]Fully converted[/green]",
        str(green),
        f"{green / total * 100:.1f}%" if total else "0%",
    )
    table.add_row(
        "[yellow]Needs review[/yellow]",
        str(yellow),
        f"{yellow / total * 100:.1f}%" if total else "0%",
    )
    table.add_row(
        "[red]Failed / manual[/red]",
        str(red),
        f"{red / total * 100:.1f}%" if total else "0%",
    )

    console.print()
    console.print(table)

    total_warnings = sum(len(r.warnings) for r in results)
    total_errors = sum(len(r.errors) for r in results)
    console.print(f"\nTotal warnings: {total_warnings}")
    console.print(f"Total errors: {total_errors}")


def _print_single_result(result: TranspileResult) -> None:
    status = "[green]SUCCESS[/green]" if result.success else "[red]NEEDS REVIEW[/red]"
    console.print(f"  Status: {status}")
    console.print(f"  Target: {result.target}")
    console.print(f"  Tier: {result.tier.value}")
    console.print(f"  Score: {result.score}")
    console.print(f"  Blocks converted: {result.blocks_converted}")
    console.print(f"  Blocks skipped: {result.blocks_skipped}")
    console.print(f"  Blocks failed: {result.blocks_failed}")

    if result.warnings:
        console.print(f"\n  [yellow]Warnings ({len(result.warnings)}):[/yellow]")
        for w in result.warnings:
            console.print(f"    - {w}")

    if result.errors:
        console.print(f"\n  [red]Errors ({len(result.errors)}):[/red]")
        for e in result.errors:
            console.print(f"    - {e}")


def _write_report(results: list[TranspileResult], report_path: str) -> None:
    Path(report_path).parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "source_file", "output_file", "target", "tier", "score", "success",
            "blocks_converted", "blocks_skipped", "blocks_failed",
            "manual_review", "warnings", "errors",
        ])
        for r in sorted(results, key=lambda x: x.score, reverse=True):
            writer.writerow([
                r.source_file, r.output_file or "", r.target,
                r.tier.value, r.score,
                r.success, r.blocks_converted, r.blocks_skipped,
                r.blocks_failed, r.manual_review_needed,
                "; ".join(r.warnings), "; ".join(r.errors),
            ])


if __name__ == "__main__":
    main()
