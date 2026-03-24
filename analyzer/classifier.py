"""Classify SAS scripts by complexity and automation feasibility.

Assigns a complexity tier (GREEN / YELLOW / RED) to each parsed SAS file
based on the constructs detected and heuristic scoring rules.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from analyzer.sas_parser import BlockType, ParseResult


class Tier(Enum):
    GREEN = "GREEN"
    YELLOW = "YELLOW"
    RED = "RED"


TIER_THRESHOLDS = {
    "green_max": 15,
    "yellow_max": 40,
}

_BLOCK_WEIGHTS: dict[BlockType, int] = {
    BlockType.DATA_STEP: 2,
    BlockType.PROC_SQL: 3,
    BlockType.PROC_SORT: 1,
    BlockType.PROC_MEANS: 3,
    BlockType.PROC_SUMMARY: 3,
    BlockType.PROC_FREQ: 2,
    BlockType.PROC_TRANSPOSE: 4,
    BlockType.PROC_FORMAT: 2,
    BlockType.PROC_IMPORT: 2,
    BlockType.PROC_EXPORT: 2,
    BlockType.PROC_PRINT: 0,
    BlockType.PROC_CONTENTS: 0,
    BlockType.PROC_DATASETS: 2,
    BlockType.PROC_APPEND: 2,
    BlockType.PROC_COMPARE: 2,
    BlockType.PROC_REPORT: 3,
    BlockType.PROC_TABULATE: 4,
    BlockType.PROC_OTHER: 5,
    BlockType.MACRO_DEF: 6,
    BlockType.MACRO_CALL: 3,
    BlockType.INCLUDE: 2,
    BlockType.LIBNAME: 1,
    BlockType.FILENAME: 1,
    BlockType.OPTIONS: 0,
    BlockType.COMMENT: 0,
    BlockType.UNKNOWN: 1,
}

_ATTR_PENALTIES: dict[str, int] = {
    "has_merge": 3,
    "has_retain": 5,
    "has_first_last": 5,
    "has_array": 6,
    "has_do_loop": 4,
    "has_output_stmt": 2,
    "has_if_then": 1,
    "has_format": 1,
    "has_informat": 1,
    "has_rename": 1,
    "has_keep": 0,
    "has_drop": 0,
    "has_where": 0,
}


@dataclass
class Classification:
    file_path: str
    tier: Tier
    score: int
    line_count: int
    block_counts: dict[str, int] = field(default_factory=dict)
    construct_flags: list[str] = field(default_factory=list)
    automation_notes: list[str] = field(default_factory=list)
    inputs: list[str] = field(default_factory=list)
    outputs: list[str] = field(default_factory=list)
    includes: list[str] = field(default_factory=list)
    libnames: list[str] = field(default_factory=list)
    macro_defs: list[str] = field(default_factory=list)


def classify(parse_result: ParseResult) -> Classification:
    """Score and classify a parsed SAS file."""
    score = 0
    block_counts: dict[str, int] = {}
    flags: list[str] = []
    notes: list[str] = []
    all_inputs: list[str] = []
    all_outputs: list[str] = []
    includes: list[str] = []
    libnames: list[str] = []
    macro_defs: list[str] = []

    for block in parse_result.blocks:
        bt_name = block.block_type.value
        block_counts[bt_name] = block_counts.get(bt_name, 0) + 1
        score += _BLOCK_WEIGHTS.get(block.block_type, 1)

        all_inputs.extend(block.inputs)
        all_outputs.extend(block.outputs)

        if block.block_type == BlockType.INCLUDE:
            includes.append(block.attributes.get("include_path", ""))
        elif block.block_type == BlockType.LIBNAME:
            libnames.append(block.attributes.get("lib_name", ""))
        elif block.block_type == BlockType.MACRO_DEF:
            macro_defs.append(block.attributes.get("macro_name", ""))
            body_lines = block.attributes.get("body_lines", 0)
            if body_lines > 50:
                score += 10
                notes.append(
                    f"Large macro '{block.attributes.get('macro_name')}' "
                    f"({body_lines} lines) -- manual review required"
                )

        for attr, penalty in _ATTR_PENALTIES.items():
            if block.attributes.get(attr):
                if attr not in flags:
                    flags.append(attr)
                    score += penalty

    if parse_result.line_count > 500:
        score += 5
        notes.append("Large file (>500 lines)")
    if parse_result.line_count > 1000:
        score += 10
        notes.append("Very large file (>1000 lines)")

    n_blocks = len(parse_result.blocks)
    if n_blocks > 20:
        score += 5
        notes.append(f"Many blocks ({n_blocks}) -- complex pipeline")

    if "MACRO_DEF" in block_counts and block_counts["MACRO_DEF"] > 2:
        score += 5
        notes.append("Multiple macro definitions -- macro library")

    if score <= TIER_THRESHOLDS["green_max"]:
        tier = Tier.GREEN
        notes.insert(0, "Fully automatable -- straightforward ETL patterns")
    elif score <= TIER_THRESHOLDS["yellow_max"]:
        tier = Tier.YELLOW
        notes.insert(0, "Semi-automatable -- review generated code carefully")
    else:
        tier = Tier.RED
        notes.insert(0, "Manual conversion required -- complex logic detected")

    return Classification(
        file_path=parse_result.file_path,
        tier=tier,
        score=score,
        line_count=parse_result.line_count,
        block_counts=block_counts,
        construct_flags=flags,
        automation_notes=notes,
        inputs=sorted(set(all_inputs)),
        outputs=sorted(set(all_outputs)),
        includes=includes,
        libnames=libnames,
        macro_defs=macro_defs,
    )
