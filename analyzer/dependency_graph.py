"""Build a dependency graph across all analyzed SAS scripts.

Uses dataset lineage (inputs/outputs) and %INCLUDE references to construct
a directed acyclic graph of script dependencies.  Outputs DOT format and
adjacency lists for downstream tooling.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

try:
    import networkx as nx

    HAS_NX = True
except ImportError:
    HAS_NX = False

from analyzer.classifier import Classification


@dataclass
class DependencyEdge:
    source_file: str
    target_file: str
    shared_datasets: list[str] = field(default_factory=list)
    edge_type: str = "data"  # "data" or "include"


@dataclass
class DependencyGraph:
    nodes: list[str]
    edges: list[DependencyEdge]
    execution_order: list[str] = field(default_factory=list)
    orphan_datasets: list[str] = field(default_factory=list)


def build_graph(classifications: list[Classification]) -> DependencyGraph:
    """Build a dependency graph from classified SAS scripts."""
    producer_map: dict[str, str] = {}
    consumer_map: dict[str, list[str]] = {}
    include_map: dict[str, list[str]] = {}

    for c in classifications:
        fpath = c.file_path
        for ds in c.outputs:
            producer_map[ds] = fpath
        for ds in c.inputs:
            consumer_map.setdefault(ds, []).append(fpath)
        for inc in c.includes:
            include_map.setdefault(fpath, []).append(inc)

    edges: list[DependencyEdge] = []
    nodes = sorted({c.file_path for c in classifications})

    for dataset, consumers in consumer_map.items():
        producer = producer_map.get(dataset)
        if producer:
            for consumer in consumers:
                if consumer != producer:
                    existing = next(
                        (
                            e
                            for e in edges
                            if e.source_file == producer
                            and e.target_file == consumer
                        ),
                        None,
                    )
                    if existing:
                        existing.shared_datasets.append(dataset)
                    else:
                        edges.append(
                            DependencyEdge(
                                source_file=producer,
                                target_file=consumer,
                                shared_datasets=[dataset],
                                edge_type="data",
                            )
                        )

    for fpath, includes in include_map.items():
        for inc in includes:
            matched = _resolve_include(inc, nodes)
            if matched:
                edges.append(
                    DependencyEdge(
                        source_file=matched,
                        target_file=fpath,
                        edge_type="include",
                    )
                )

    orphans = [
        ds
        for ds in consumer_map
        if ds not in producer_map
    ]

    execution_order = _topological_sort(nodes, edges)

    return DependencyGraph(
        nodes=nodes,
        edges=edges,
        execution_order=execution_order,
        orphan_datasets=sorted(set(orphans)),
    )


def _resolve_include(include_path: str, known_files: list[str]) -> str | None:
    inc_name = Path(include_path).stem.lower()
    for f in known_files:
        if Path(f).stem.lower() == inc_name:
            return f
    return None


def _topological_sort(
    nodes: list[str], edges: list[DependencyEdge]
) -> list[str]:
    if HAS_NX:
        g = nx.DiGraph()
        g.add_nodes_from(nodes)
        for e in edges:
            g.add_edge(e.source_file, e.target_file)
        try:
            return list(nx.topological_sort(g))
        except nx.NetworkXUnfeasible:
            return nodes
    else:
        return _simple_topo_sort(nodes, edges)


def _simple_topo_sort(
    nodes: list[str], edges: list[DependencyEdge]
) -> list[str]:
    adj: dict[str, list[str]] = {n: [] for n in nodes}
    in_deg: dict[str, int] = {n: 0 for n in nodes}
    for e in edges:
        if e.source_file in adj:
            adj[e.source_file].append(e.target_file)
            in_deg[e.target_file] = in_deg.get(e.target_file, 0) + 1

    queue = [n for n in nodes if in_deg[n] == 0]
    result: list[str] = []
    while queue:
        queue.sort()
        node = queue.pop(0)
        result.append(node)
        for neighbor in adj.get(node, []):
            in_deg[neighbor] -= 1
            if in_deg[neighbor] == 0:
                queue.append(neighbor)
    result.extend(n for n in nodes if n not in result)
    return result


def export_dot(graph: DependencyGraph) -> str:
    """Export the dependency graph in Graphviz DOT format."""
    lines = ["digraph sas_dependencies {", '  rankdir="LR";']
    for node in graph.nodes:
        label = Path(node).stem
        lines.append(f'  "{label}";')
    for edge in graph.edges:
        src = Path(edge.source_file).stem
        tgt = Path(edge.target_file).stem
        label = ", ".join(edge.shared_datasets[:3]) if edge.shared_datasets else edge.edge_type
        lines.append(f'  "{src}" -> "{tgt}" [label="{label}"];')
    lines.append("}")
    return "\n".join(lines)


def export_json(graph: DependencyGraph) -> str:
    """Export the dependency graph as JSON."""
    data = {
        "nodes": graph.nodes,
        "edges": [
            {
                "source": e.source_file,
                "target": e.target_file,
                "shared_datasets": e.shared_datasets,
                "type": e.edge_type,
            }
            for e in graph.edges
        ],
        "execution_order": graph.execution_order,
        "orphan_datasets": graph.orphan_datasets,
    }
    return json.dumps(data, indent=2)
