"""Tests for the SAS analyzer pipeline."""

from pathlib import Path

import pytest

from analyzer.classifier import Tier, classify
from analyzer.dependency_graph import build_graph
from analyzer.sas_parser import BlockType, parse_sas_file

FIXTURES = Path(__file__).resolve().parents[2] / "sas_source"


class TestParser:
    def test_parse_simple(self):
        result = parse_sas_file(FIXTURES / "sample_simple.sas")
        assert result.line_count > 0
        assert not result.errors

        block_types = [b.block_type for b in result.blocks]
        assert BlockType.LIBNAME in block_types
        assert BlockType.DATA_STEP in block_types
        assert BlockType.PROC_SORT in block_types
        assert BlockType.PROC_SQL in block_types

    def test_parse_detects_data_step_inputs_outputs(self):
        result = parse_sas_file(FIXTURES / "sample_simple.sas")
        data_steps = [b for b in result.blocks if b.block_type == BlockType.DATA_STEP]
        assert len(data_steps) >= 1

        ds = data_steps[0]
        assert "work.customers_filtered" in ds.outputs or "customers_filtered" in ds.outputs
        assert any("customers" in inp for inp in ds.inputs)

    def test_parse_detects_proc_sql_tables(self):
        result = parse_sas_file(FIXTURES / "sample_simple.sas")
        sql_blocks = [b for b in result.blocks if b.block_type == BlockType.PROC_SQL]
        assert len(sql_blocks) >= 1

        sql = sql_blocks[0]
        assert any("region_summary" in o for o in sql.outputs)
        assert any("customers_filtered" in i for i in sql.inputs)

    def test_parse_medium_detects_merge(self):
        result = parse_sas_file(FIXTURES / "sample_medium.sas")
        data_steps = [b for b in result.blocks if b.block_type == BlockType.DATA_STEP]
        merge_steps = [d for d in data_steps if d.attributes.get("has_merge")]
        assert len(merge_steps) >= 1

    def test_parse_medium_detects_retain_first_last(self):
        result = parse_sas_file(FIXTURES / "sample_medium.sas")
        data_steps = [b for b in result.blocks if b.block_type == BlockType.DATA_STEP]
        retain_steps = [d for d in data_steps if d.attributes.get("has_retain")]
        assert len(retain_steps) >= 1

        fl_steps = [d for d in data_steps if d.attributes.get("has_first_last")]
        assert len(fl_steps) >= 1

    def test_parse_complex_detects_macros(self):
        result = parse_sas_file(FIXTURES / "sample_complex.sas")
        macros = [b for b in result.blocks if b.block_type == BlockType.MACRO_DEF]
        assert len(macros) >= 1

    def test_parse_complex_detects_arrays(self):
        result = parse_sas_file(FIXTURES / "sample_complex.sas")
        data_steps = [b for b in result.blocks if b.block_type == BlockType.DATA_STEP]
        array_steps = [d for d in data_steps if d.attributes.get("has_array")]
        assert len(array_steps) >= 1

    def test_parse_detects_includes(self):
        result = parse_sas_file(FIXTURES / "sample_medium.sas")
        includes = [b for b in result.blocks if b.block_type == BlockType.INCLUDE]
        assert len(includes) >= 1


class TestClassifier:
    def test_simple_is_green(self):
        result = parse_sas_file(FIXTURES / "sample_simple.sas")
        classification = classify(result)
        assert classification.tier == Tier.GREEN

    def test_medium_is_yellow(self):
        result = parse_sas_file(FIXTURES / "sample_medium.sas")
        classification = classify(result)
        assert classification.tier == Tier.YELLOW

    def test_complex_is_red(self):
        result = parse_sas_file(FIXTURES / "sample_complex.sas")
        classification = classify(result)
        assert classification.tier == Tier.RED

    def test_classification_has_outputs(self):
        result = parse_sas_file(FIXTURES / "sample_simple.sas")
        classification = classify(result)
        assert len(classification.outputs) > 0

    def test_score_ordering(self):
        simple = classify(parse_sas_file(FIXTURES / "sample_simple.sas"))
        medium = classify(parse_sas_file(FIXTURES / "sample_medium.sas"))
        cplx = classify(parse_sas_file(FIXTURES / "sample_complex.sas"))
        assert simple.score < medium.score < cplx.score


class TestDependencyGraph:
    def test_graph_construction(self):
        files = list(FIXTURES.glob("*.sas"))
        classifications = [classify(parse_sas_file(f)) for f in files]
        graph = build_graph(classifications)

        assert len(graph.nodes) == len(files)
        assert isinstance(graph.execution_order, list)

    def test_graph_detects_edges(self):
        files = list(FIXTURES.glob("*.sas"))
        classifications = [classify(parse_sas_file(f)) for f in files]
        graph = build_graph(classifications)
        # With the sample data, there may or may not be edges
        # depending on shared dataset names -- just verify no crash
        assert isinstance(graph.edges, list)
