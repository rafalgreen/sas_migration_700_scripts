"""CLI entry point for the SAS analyzer.

Usage:
    sas-analyze scan --input sas_source/ --output reports/
    sas-analyze scan --input sas_source/ --output reports/ --format csv
    sas-analyze scan --input sas_source/ --output reports/ --format html
"""

from __future__ import annotations

from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from analyzer.classifier import Tier, classify
from analyzer.dependency_graph import build_graph, export_dot, export_json
from analyzer.report_generator import generate_csv, generate_html, generate_summary
from analyzer.sas_parser import parse_sas_file

console = Console()


@click.group()
def main():
    """SAS Script Analyzer -- scan, classify, and report on SAS codebases."""


@main.command()
@click.option("--input", "-i", "input_dir", required=True, help="Directory containing .sas files")
@click.option("--output", "-o", "output_dir", default="reports", help="Output directory for reports")
@click.option(
    "--format",
    "-f",
    "fmt",
    type=click.Choice(["csv", "html", "all"]),
    default="all",
    help="Report format",
)
def scan(input_dir: str, output_dir: str, fmt: str):
    """Scan SAS files and generate inventory reports."""
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        console.print(f"[red]Error: Input directory '{input_dir}' not found[/red]")
        raise SystemExit(1)

    sas_files = sorted(input_path.rglob("*.sas"))
    if not sas_files:
        console.print(f"[yellow]No .sas files found in '{input_dir}'[/yellow]")
        raise SystemExit(0)

    console.print(f"[blue]Scanning {len(sas_files)} SAS files...[/blue]")

    classifications = []
    errors = []
    with console.status("[bold green]Parsing...") as status:
        for i, sas_file in enumerate(sas_files):
            status.update(f"[bold green]Parsing {i + 1}/{len(sas_files)}: {sas_file.name}")
            result = parse_sas_file(sas_file)
            if result.errors:
                errors.extend(result.errors)
            classification = classify(result)
            classifications.append(classification)

    graph = build_graph(classifications)
    summary = generate_summary(classifications)

    _print_summary(summary, classifications)

    if fmt in ("csv", "all"):
        csv_path = output_path / "inventory.csv"
        generate_csv(classifications, csv_path)
        console.print(f"[green]CSV report: {csv_path}[/green]")

    if fmt in ("html", "all"):
        html_path = output_path / "inventory.html"
        generate_html(classifications, graph, html_path)
        console.print(f"[green]HTML report: {html_path}[/green]")

    dot_path = output_path / "dependencies.dot"
    dot_path.write_text(export_dot(graph))
    console.print(f"[green]Dependency graph (DOT): {dot_path}[/green]")

    json_path = output_path / "dependencies.json"
    json_path.write_text(export_json(graph))
    console.print(f"[green]Dependency graph (JSON): {json_path}[/green]")

    if graph.execution_order:
        order_path = output_path / "execution_order.txt"
        order_path.write_text("\n".join(graph.execution_order))
        console.print(f"[green]Execution order: {order_path}[/green]")

    if errors:
        console.print(f"\n[yellow]Parsing warnings ({len(errors)}):[/yellow]")
        for err in errors[:20]:
            console.print(f"  [dim]{err}[/dim]")
        if len(errors) > 20:
            console.print(f"  [dim]... and {len(errors) - 20} more[/dim]")


def _print_summary(summary: dict, classifications: list) -> None:
    table = Table(title="SAS Migration Inventory Summary")
    table.add_column("Metric", style="bold")
    table.add_column("Value", justify="right")

    table.add_row("Total scripts", str(summary["total_scripts"]))
    table.add_row("Total lines", f"{summary['total_lines']:,}")
    table.add_row("Avg lines/script", str(summary["avg_lines"]))
    table.add_row("")
    table.add_row(
        "[green]GREEN (auto)[/green]",
        f"{summary['green_count']} ({summary['green_pct']}%)",
    )
    table.add_row(
        "[yellow]YELLOW (semi)[/yellow]",
        f"{summary['yellow_count']} ({summary['yellow_pct']}%)",
    )
    table.add_row(
        "[red]RED (manual)[/red]",
        f"{summary['red_count']} ({summary['red_pct']}%)",
    )

    console.print()
    console.print(table)
    console.print()

    if summary["construct_counts"]:
        ct = Table(title="Construct Distribution")
        ct.add_column("Construct")
        ct.add_column("Count", justify="right")
        for k, v in sorted(
            summary["construct_counts"].items(), key=lambda x: x[1], reverse=True
        ):
            ct.add_row(k, str(v))
        console.print(ct)
        console.print()


if __name__ == "__main__":
    main()
