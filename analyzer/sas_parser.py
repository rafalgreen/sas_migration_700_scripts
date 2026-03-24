"""SAS script tokenizer and parser.

Extracts structured information from SAS source files using regex-based
pattern matching. Produces a list of SASBlock objects representing the
logical constructs found in each script.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path


class BlockType(Enum):
    DATA_STEP = "DATA_STEP"
    PROC_SQL = "PROC_SQL"
    PROC_SORT = "PROC_SORT"
    PROC_MEANS = "PROC_MEANS"
    PROC_SUMMARY = "PROC_SUMMARY"
    PROC_FREQ = "PROC_FREQ"
    PROC_TRANSPOSE = "PROC_TRANSPOSE"
    PROC_FORMAT = "PROC_FORMAT"
    PROC_IMPORT = "PROC_IMPORT"
    PROC_EXPORT = "PROC_EXPORT"
    PROC_PRINT = "PROC_PRINT"
    PROC_CONTENTS = "PROC_CONTENTS"
    PROC_DATASETS = "PROC_DATASETS"
    PROC_APPEND = "PROC_APPEND"
    PROC_COMPARE = "PROC_COMPARE"
    PROC_REPORT = "PROC_REPORT"
    PROC_TABULATE = "PROC_TABULATE"
    PROC_OTHER = "PROC_OTHER"
    MACRO_DEF = "MACRO_DEF"
    MACRO_CALL = "MACRO_CALL"
    INCLUDE = "INCLUDE"
    LIBNAME = "LIBNAME"
    FILENAME = "FILENAME"
    OPTIONS = "OPTIONS"
    COMMENT = "COMMENT"
    UNKNOWN = "UNKNOWN"


@dataclass
class SASBlock:
    block_type: BlockType
    raw_text: str
    start_line: int
    end_line: int
    outputs: list[str] = field(default_factory=list)
    inputs: list[str] = field(default_factory=list)
    attributes: dict = field(default_factory=dict)


@dataclass
class ParseResult:
    file_path: str
    line_count: int
    blocks: list[SASBlock] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


_STRIP_COMMENTS = re.compile(
    r"/\*.*?\*/",
    re.DOTALL,
)
_LINE_COMMENT = re.compile(r"^\s*\*[^;]*;", re.MULTILINE)

_PROC_PATTERN = re.compile(
    r"\bPROC\s+(\w+)\b(.*?)(?:RUN\s*;|QUIT\s*;)",
    re.IGNORECASE | re.DOTALL,
)
_DATA_STEP_PATTERN = re.compile(
    r"\bDATA\s+([\w.\s/()]+?)\s*;(.*?)(?:\bRUN\s*;)",
    re.IGNORECASE | re.DOTALL,
)
_MACRO_DEF_PATTERN = re.compile(
    r"%MACRO\s+(\w+)\s*(\([^)]*\))?\s*;(.*?)%MEND\b",
    re.IGNORECASE | re.DOTALL,
)
_MACRO_CALL_PATTERN = re.compile(
    r"%(\w+)\s*\(([^)]*)\)\s*;?",
    re.IGNORECASE,
)
_INCLUDE_PATTERN = re.compile(
    r"%INCLUDE\s+['\"]?([^'\";\s]+)['\"]?\s*;",
    re.IGNORECASE,
)
_LIBNAME_PATTERN = re.compile(
    r"\bLIBNAME\s+(\w+)\s+(.*?)\s*;",
    re.IGNORECASE | re.DOTALL,
)
_FILENAME_PATTERN = re.compile(
    r"\bFILENAME\s+(\w+)\s+(.*?)\s*;",
    re.IGNORECASE | re.DOTALL,
)
_SET_PATTERN = re.compile(r"\bSET\s+([\w.\s]+)\s*;", re.IGNORECASE)
_MERGE_PATTERN = re.compile(r"\bMERGE\s+([\w.\s()=]+)\s*;", re.IGNORECASE)
_BY_PATTERN = re.compile(r"\bBY\s+([\w\s]+)\s*;", re.IGNORECASE)
_WHERE_PATTERN = re.compile(r"\bWHERE\s+(.+?)\s*;", re.IGNORECASE)
_KEEP_PATTERN = re.compile(r"\bKEEP\s+([\w\s]+)\s*;", re.IGNORECASE)
_DROP_PATTERN = re.compile(r"\bDROP\s+([\w\s]+)\s*;", re.IGNORECASE)
_RENAME_PATTERN = re.compile(r"\bRENAME\s+(.+?)\s*;", re.IGNORECASE)
_RETAIN_PATTERN = re.compile(r"\bRETAIN\s+(.+?)\s*;", re.IGNORECASE)
_IF_PATTERN = re.compile(r"\bIF\s+(.+?)\s+THEN\b", re.IGNORECASE)
_OUTPUT_PATTERN = re.compile(r"\bOUTPUT\s*([\w.]*)\s*;", re.IGNORECASE)
_FORMAT_PATTERN = re.compile(r"\bFORMAT\s+(.+?)\s*;", re.IGNORECASE)
_INFORMAT_PATTERN = re.compile(r"\bINFORMAT\s+(.+?)\s*;", re.IGNORECASE)

_PROC_TYPE_MAP = {
    "SQL": BlockType.PROC_SQL,
    "SORT": BlockType.PROC_SORT,
    "MEANS": BlockType.PROC_MEANS,
    "SUMMARY": BlockType.PROC_SUMMARY,
    "FREQ": BlockType.PROC_FREQ,
    "TRANSPOSE": BlockType.PROC_TRANSPOSE,
    "FORMAT": BlockType.PROC_FORMAT,
    "IMPORT": BlockType.PROC_IMPORT,
    "EXPORT": BlockType.PROC_EXPORT,
    "PRINT": BlockType.PROC_PRINT,
    "CONTENTS": BlockType.PROC_CONTENTS,
    "DATASETS": BlockType.PROC_DATASETS,
    "APPEND": BlockType.PROC_APPEND,
    "COMPARE": BlockType.PROC_COMPARE,
    "REPORT": BlockType.PROC_REPORT,
    "TABULATE": BlockType.PROC_TABULATE,
}

_SAS_BUILTINS = {"_null_", "_data_", "_last_", "_all_"}


def _line_number_at(text: str, pos: int) -> int:
    return text[:pos].count("\n") + 1


def _split_dataset_names(raw: str) -> list[str]:
    names = re.split(r"\s+", raw.strip())
    cleaned = []
    for n in names:
        n = re.sub(r"\(.*?\)", "", n).strip()
        if n and n.lower() not in _SAS_BUILTINS:
            cleaned.append(n.lower())
    return cleaned


def _extract_sql_tables(sql_body: str) -> tuple[list[str], list[str]]:
    outputs: list[str] = []
    inputs: list[str] = []
    create_m = re.findall(
        r"CREATE\s+TABLE\s+([\w.]+)", sql_body, re.IGNORECASE
    )
    outputs.extend(t.lower() for t in create_m)
    from_m = re.findall(r"\bFROM\s+([\w.]+)", sql_body, re.IGNORECASE)
    join_m = re.findall(r"\bJOIN\s+([\w.]+)", sql_body, re.IGNORECASE)
    inputs.extend(t.lower() for t in from_m)
    inputs.extend(t.lower() for t in join_m)
    inputs = [t for t in inputs if t not in outputs]
    return outputs, inputs


def _parse_data_step(match: re.Match, full_text: str) -> SASBlock:
    output_raw = match.group(1)
    body = match.group(2)
    start_line = _line_number_at(full_text, match.start())
    end_line = _line_number_at(full_text, match.end())

    outputs = _split_dataset_names(output_raw)
    inputs: list[str] = []
    attrs: dict = {}

    for set_m in _SET_PATTERN.finditer(body):
        inputs.extend(_split_dataset_names(set_m.group(1)))
    for merge_m in _MERGE_PATTERN.finditer(body):
        inputs.extend(_split_dataset_names(merge_m.group(1)))
        attrs["has_merge"] = True

    if _BY_PATTERN.search(body):
        by_m = _BY_PATTERN.search(body)
        attrs["by_vars"] = by_m.group(1).strip().split()
    if _WHERE_PATTERN.search(body):
        attrs["has_where"] = True
    if _KEEP_PATTERN.search(body):
        attrs["has_keep"] = True
    if _DROP_PATTERN.search(body):
        attrs["has_drop"] = True
    if _RENAME_PATTERN.search(body):
        attrs["has_rename"] = True
    if _RETAIN_PATTERN.search(body):
        attrs["has_retain"] = True
    if _IF_PATTERN.search(body):
        attrs["has_if_then"] = True
    if _OUTPUT_PATTERN.search(body):
        attrs["has_output_stmt"] = True
    if _FORMAT_PATTERN.search(body):
        attrs["has_format"] = True
    if _INFORMAT_PATTERN.search(body):
        attrs["has_informat"] = True
    if re.search(r"\bFIRST\.\w+", body, re.IGNORECASE):
        attrs["has_first_last"] = True
    if re.search(r"\bLAST\.\w+", body, re.IGNORECASE):
        attrs["has_first_last"] = True
    if re.search(r"\bARRAY\s+", body, re.IGNORECASE):
        attrs["has_array"] = True
    if re.search(r"\bDO\s+", body, re.IGNORECASE):
        attrs["has_do_loop"] = True

    return SASBlock(
        block_type=BlockType.DATA_STEP,
        raw_text=match.group(0),
        start_line=start_line,
        end_line=end_line,
        outputs=outputs,
        inputs=inputs,
        attributes=attrs,
    )


def _parse_proc(match: re.Match, full_text: str) -> SASBlock:
    proc_name = match.group(1).upper()
    body = match.group(2)
    start_line = _line_number_at(full_text, match.start())
    end_line = _line_number_at(full_text, match.end())

    block_type = _PROC_TYPE_MAP.get(proc_name, BlockType.PROC_OTHER)
    attrs: dict = {"proc_name": proc_name}
    outputs: list[str] = []
    inputs: list[str] = []

    data_m = re.search(r"\bDATA\s*=\s*([\w.]+)", body, re.IGNORECASE)
    if data_m:
        if block_type in (BlockType.PROC_SORT,):
            inputs.append(data_m.group(1).lower())
            outputs.append(data_m.group(1).lower())
        else:
            inputs.append(data_m.group(1).lower())

    out_m = re.search(r"\bOUT\s*=\s*([\w.]+)", body, re.IGNORECASE)
    if out_m:
        outputs.append(out_m.group(1).lower())

    if block_type == BlockType.PROC_SQL:
        sql_out, sql_in = _extract_sql_tables(body)
        outputs.extend(sql_out)
        inputs.extend(sql_in)

    return SASBlock(
        block_type=block_type,
        raw_text=match.group(0),
        start_line=start_line,
        end_line=end_line,
        outputs=outputs,
        inputs=inputs,
        attributes=attrs,
    )


def parse_sas_file(file_path: str | Path) -> ParseResult:
    """Parse a SAS file and return structured block information."""
    file_path = Path(file_path)
    try:
        content = file_path.read_text(encoding="utf-8", errors="replace")
    except OSError as e:
        return ParseResult(
            file_path=str(file_path), line_count=0, errors=[str(e)]
        )

    line_count = content.count("\n") + 1
    result = ParseResult(file_path=str(file_path), line_count=line_count)

    cleaned = _STRIP_COMMENTS.sub("", content)
    cleaned = _LINE_COMMENT.sub("", cleaned)

    for m in _LIBNAME_PATTERN.finditer(cleaned):
        result.blocks.append(
            SASBlock(
                block_type=BlockType.LIBNAME,
                raw_text=m.group(0),
                start_line=_line_number_at(cleaned, m.start()),
                end_line=_line_number_at(cleaned, m.end()),
                attributes={
                    "lib_name": m.group(1).lower(),
                    "lib_path": m.group(2).strip(),
                },
            )
        )

    for m in _FILENAME_PATTERN.finditer(cleaned):
        result.blocks.append(
            SASBlock(
                block_type=BlockType.FILENAME,
                raw_text=m.group(0),
                start_line=_line_number_at(cleaned, m.start()),
                end_line=_line_number_at(cleaned, m.end()),
                attributes={
                    "fileref": m.group(1).lower(),
                    "file_path": m.group(2).strip(),
                },
            )
        )

    for m in _INCLUDE_PATTERN.finditer(cleaned):
        result.blocks.append(
            SASBlock(
                block_type=BlockType.INCLUDE,
                raw_text=m.group(0),
                start_line=_line_number_at(cleaned, m.start()),
                end_line=_line_number_at(cleaned, m.end()),
                attributes={"include_path": m.group(1)},
            )
        )

    for m in _MACRO_DEF_PATTERN.finditer(cleaned):
        params = m.group(2) or ""
        result.blocks.append(
            SASBlock(
                block_type=BlockType.MACRO_DEF,
                raw_text=m.group(0),
                start_line=_line_number_at(cleaned, m.start()),
                end_line=_line_number_at(cleaned, m.end()),
                attributes={
                    "macro_name": m.group(1).lower(),
                    "params": params.strip("()").strip(),
                    "body_lines": m.group(3).count("\n") + 1,
                },
            )
        )

    for m in _DATA_STEP_PATTERN.finditer(cleaned):
        try:
            result.blocks.append(_parse_data_step(m, cleaned))
        except Exception as e:
            result.errors.append(
                f"Error parsing DATA step at line "
                f"{_line_number_at(cleaned, m.start())}: {e}"
            )

    for m in _PROC_PATTERN.finditer(cleaned):
        try:
            result.blocks.append(_parse_proc(m, cleaned))
        except Exception as e:
            result.errors.append(
                f"Error parsing PROC at line "
                f"{_line_number_at(cleaned, m.start())}: {e}"
            )

    result.blocks.sort(key=lambda b: b.start_line)
    return result
