"""Generate inventory reports from SAS analysis results.

Produces CSV and HTML reports summarizing classification results,
construct distributions, and dependency information.
"""

from __future__ import annotations

import csv
import html
import io
import re
from pathlib import Path

from analyzer.classifier import Classification, Tier
from analyzer.dependency_graph import DependencyGraph


def generate_csv(classifications: list[Classification], output_path: str | Path) -> None:
    """Write a CSV inventory report."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "file",
                "tier",
                "score",
                "lines",
                "data_steps",
                "proc_sql",
                "proc_sort",
                "proc_other",
                "macros",
                "includes",
                "has_merge",
                "has_retain",
                "has_first_last",
                "has_array",
                "has_do_loop",
                "inputs",
                "outputs",
                "notes",
            ]
        )
        for c in sorted(classifications, key=lambda x: x.score, reverse=True):
            writer.writerow(
                [
                    c.file_path,
                    c.tier.value,
                    c.score,
                    c.line_count,
                    c.block_counts.get("DATA_STEP", 0),
                    c.block_counts.get("PROC_SQL", 0),
                    c.block_counts.get("PROC_SORT", 0),
                    sum(
                        v
                        for k, v in c.block_counts.items()
                        if k.startswith("PROC_") and k not in ("PROC_SQL", "PROC_SORT")
                    ),
                    c.block_counts.get("MACRO_DEF", 0),
                    len(c.includes),
                    "has_merge" in c.construct_flags,
                    "has_retain" in c.construct_flags,
                    "has_first_last" in c.construct_flags,
                    "has_array" in c.construct_flags,
                    "has_do_loop" in c.construct_flags,
                    "; ".join(c.inputs),
                    "; ".join(c.outputs),
                    " | ".join(c.automation_notes),
                ]
            )


def generate_summary(classifications: list[Classification]) -> dict:
    """Compute summary statistics."""
    total = len(classifications)
    green = sum(1 for c in classifications if c.tier == Tier.GREEN)
    yellow = sum(1 for c in classifications if c.tier == Tier.YELLOW)
    red = sum(1 for c in classifications if c.tier == Tier.RED)
    total_lines = sum(c.line_count for c in classifications)

    all_constructs: dict[str, int] = {}
    for c in classifications:
        for k, v in c.block_counts.items():
            all_constructs[k] = all_constructs.get(k, 0) + v

    all_flags: dict[str, int] = {}
    for c in classifications:
        for f in c.construct_flags:
            all_flags[f] = all_flags.get(f, 0) + 1

    return {
        "total_scripts": total,
        "green_count": green,
        "yellow_count": yellow,
        "red_count": red,
        "green_pct": round(green / total * 100, 1) if total else 0,
        "yellow_pct": round(yellow / total * 100, 1) if total else 0,
        "red_pct": round(red / total * 100, 1) if total else 0,
        "total_lines": total_lines,
        "avg_lines": round(total_lines / total, 1) if total else 0,
        "construct_counts": all_constructs,
        "flag_counts": all_flags,
    }


def _mermaid_safe(text: str) -> str:
    """Escape characters that break Mermaid label syntax."""
    return re.sub(r'["\[\]{}()<>|#&;]', lambda m: f"#{ord(m.group()):d};", text)


def _build_mermaid_graph(
    graph: DependencyGraph,
    tier_map: dict[str, str],
) -> str:
    """Convert a DependencyGraph into a Mermaid flowchart definition."""
    lines = ["flowchart LR"]

    for node in graph.nodes:
        stem = Path(node).stem
        node_id = re.sub(r"\W", "_", stem)
        tier = tier_map.get(node, "")
        label = f"{stem} ({tier})" if tier else stem
        lines.append(f'  {node_id}["{_mermaid_safe(label)}"]')

    if graph.edges:
        for edge in graph.edges:
            src_id = re.sub(r"\W", "_", Path(edge.source_file).stem)
            tgt_id = re.sub(r"\W", "_", Path(edge.target_file).stem)
            if edge.shared_datasets:
                label = ", ".join(edge.shared_datasets[:3])
                if len(edge.shared_datasets) > 3:
                    label += f" +{len(edge.shared_datasets) - 3}"
                lines.append(f'  {src_id} -->|"{_mermaid_safe(label)}"| {tgt_id}')
            elif edge.edge_type == "include":
                lines.append(f"  {src_id} -.->|include| {tgt_id}")
            else:
                lines.append(f"  {src_id} --> {tgt_id}")
    else:
        lines.append("  no_deps[No inter-script dependencies detected]")

    return "\n".join(lines)


def generate_html(
    classifications: list[Classification],
    graph: DependencyGraph | None,
    output_path: str | Path,
) -> None:
    """Write an HTML inventory report with summary dashboard."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    summary = generate_summary(classifications)
    sorted_cls = sorted(classifications, key=lambda x: x.score, reverse=True)

    tier_colors = {"GREEN": "#22c55e", "YELLOW": "#eab308", "RED": "#ef4444"}

    buf = io.StringIO()
    buf.write("<!DOCTYPE html>\n<html><head>\n")
    buf.write("<meta charset='utf-8'>\n")
    buf.write("<title>SAS Migration Inventory Report</title>\n")
    buf.write("<style>\n")
    buf.write(
        """
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
       margin: 2rem; background: #f8fafc; color: #1e293b; }
h1 { color: #0f172a; }
.cards { display: flex; gap: 1rem; flex-wrap: wrap; margin: 1.5rem 0; }
.card { background: white; border-radius: 8px; padding: 1.5rem; min-width: 180px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
.card .number { font-size: 2rem; font-weight: 700; }
.card .label { color: #64748b; font-size: 0.875rem; }
table { border-collapse: collapse; width: 100%; background: white;
        border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
th { background: #1e293b; color: white; padding: 0.75rem 1rem;
     text-align: left; font-size: 0.8rem; text-transform: uppercase; }
td { padding: 0.6rem 1rem; border-bottom: 1px solid #e2e8f0; font-size: 0.875rem; }
tr:hover { background: #f1f5f9; }
.tier { padding: 0.25rem 0.75rem; border-radius: 4px; font-weight: 600;
        font-size: 0.75rem; color: white; display: inline-block; }
.constructs { display: flex; gap: 0.5rem; flex-wrap: wrap; margin: 1.5rem 0; }
.construct { background: white; border-radius: 6px; padding: 0.5rem 1rem;
             box-shadow: 0 1px 2px rgba(0,0,0,0.08); font-size: 0.85rem; }
.construct .cnt { font-weight: 700; color: #3b82f6; }
.graph-container { background: white; border-radius: 8px; padding: 1.5rem;
                   box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin: 1.5rem 0;
                   overflow-x: auto; }
.exec-order { list-style: decimal; padding-left: 1.5rem; }
.exec-order li { padding: 0.25rem 0; font-size: 0.875rem; }
.exec-order code { background: #f1f5f9; padding: 0.15rem 0.4rem; border-radius: 3px; }
"""
    )
    buf.write("</style>\n")
    buf.write(
        "<script src='https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js'></script>\n"
        "<script>mermaid.initialize({startOnLoad:true, theme:'neutral', "
        "securityLevel:'loose'});</script>\n"
    )
    buf.write("</head><body>\n")
    buf.write("<h1>SAS Migration Inventory Report</h1>\n")

    buf.write("<div class='cards'>\n")
    for label, value, color in [
        ("Total Scripts", summary["total_scripts"], "#3b82f6"),
        ("GREEN (auto)", f"{summary['green_count']} ({summary['green_pct']}%)", "#22c55e"),
        ("YELLOW (semi)", f"{summary['yellow_count']} ({summary['yellow_pct']}%)", "#eab308"),
        ("RED (manual)", f"{summary['red_count']} ({summary['red_pct']}%)", "#ef4444"),
        ("Total Lines", f"{summary['total_lines']:,}", "#8b5cf6"),
        ("Avg Lines/Script", summary["avg_lines"], "#6366f1"),
    ]:
        buf.write(f"<div class='card'><div class='number' style='color:{color}'>{value}</div>")
        buf.write(f"<div class='label'>{label}</div></div>\n")
    buf.write("</div>\n")

    buf.write("<h2>Construct Distribution</h2>\n<div class='constructs'>\n")
    for construct, count in sorted(
        summary["construct_counts"].items(), key=lambda x: x[1], reverse=True
    ):
        buf.write(f"<div class='construct'>{construct}: <span class='cnt'>{count}</span></div>\n")
    buf.write("</div>\n")

    if summary["flag_counts"]:
        buf.write("<h2>Complexity Flags</h2>\n<div class='constructs'>\n")
        for flag, count in sorted(
            summary["flag_counts"].items(), key=lambda x: x[1], reverse=True
        ):
            buf.write(f"<div class='construct'>{flag}: <span class='cnt'>{count}</span></div>\n")
        buf.write("</div>\n")

    if graph:
        tier_map = {c.file_path: c.tier.value for c in classifications}
        mermaid_def = _build_mermaid_graph(graph, tier_map)
        buf.write("<h2>Dependency Graph</h2>\n")
        buf.write("<div class='graph-container'>\n")
        buf.write(f"<pre class='mermaid'>\n{html.escape(mermaid_def)}\n</pre>\n")
        buf.write("</div>\n")

        if graph.execution_order:
            buf.write("<h2>Execution Order</h2>\n")
            buf.write("<ol class='exec-order'>\n")
            for step in graph.execution_order:
                buf.write(f"<li><code>{html.escape(Path(step).stem)}</code></li>\n")
            buf.write("</ol>\n")

    buf.write("<h2>Script Inventory</h2>\n")
    buf.write("<table><thead><tr>")
    for h in ["File", "Tier", "Score", "Lines", "DATA Steps", "PROC SQL", "Macros", "Notes"]:
        buf.write(f"<th>{h}</th>")
    buf.write("</tr></thead><tbody>\n")

    for c in sorted_cls:
        color = tier_colors.get(c.tier.value, "#94a3b8")
        notes_str = "; ".join(c.automation_notes[:2])
        buf.write("<tr>")
        buf.write(f"<td>{Path(c.file_path).name}</td>")
        buf.write(f"<td><span class='tier' style='background:{color}'>{c.tier.value}</span></td>")
        buf.write(f"<td>{c.score}</td>")
        buf.write(f"<td>{c.line_count}</td>")
        buf.write(f"<td>{c.block_counts.get('DATA_STEP', 0)}</td>")
        buf.write(f"<td>{c.block_counts.get('PROC_SQL', 0)}</td>")
        buf.write(f"<td>{c.block_counts.get('MACRO_DEF', 0)}</td>")
        buf.write(f"<td>{notes_str}</td>")
        buf.write("</tr>\n")

    buf.write("</tbody></table>\n")

    if graph and graph.orphan_datasets:
        buf.write("<h2>Orphan Datasets (consumed but never produced)</h2>\n<ul>\n")
        for ds in graph.orphan_datasets[:50]:
            buf.write(f"<li><code>{ds}</code></li>\n")
        buf.write("</ul>\n")

    buf.write("</body></html>\n")

    output_path.write_text(buf.getvalue())
