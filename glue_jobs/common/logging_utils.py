"""Structured logging utilities for AWS Glue PySpark jobs.

Provides a consistent logging setup that works with CloudWatch Logs
and includes job metadata for observability.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from contextlib import contextmanager
from typing import Any


def setup_logger(
    job_name: str,
    log_level: str = "INFO",
) -> logging.Logger:
    """Configure a structured logger for a Glue job."""
    logger = logging.getLogger(job_name)
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_StructuredFormatter(job_name))
        logger.addHandler(handler)

    return logger


class _StructuredFormatter(logging.Formatter):
    def __init__(self, job_name: str):
        super().__init__()
        self.job_name = job_name

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "job": self.job_name,
            "message": record.getMessage(),
        }

        if hasattr(record, "extra_data"):
            log_entry["data"] = record.extra_data

        if record.exc_info and record.exc_info[1]:
            log_entry["error"] = str(record.exc_info[1])
            log_entry["error_type"] = type(record.exc_info[1]).__name__

        return json.dumps(log_entry)


def log_dataframe_info(
    logger: logging.Logger,
    df: Any,
    name: str,
    show_schema: bool = False,
) -> None:
    """Log DataFrame metadata (row count, columns, schema)."""
    count = df.count()
    cols = len(df.columns)
    logger.info(
        f"DataFrame '{name}': {count:,} rows, {cols} columns",
        extra={"extra_data": {"dataframe": name, "rows": count, "columns": cols}},
    )
    if show_schema:
        schema_str = df._jdf.schema().treeString() if hasattr(df, "_jdf") else str(df.schema)
        logger.info(f"Schema for '{name}': {schema_str}")


@contextmanager
def log_step(logger: logging.Logger, step_name: str):
    """Context manager to log step start/end with timing."""
    start = time.time()
    logger.info(f"Starting: {step_name}")
    try:
        yield
        elapsed = time.time() - start
        logger.info(
            f"Completed: {step_name} ({elapsed:.2f}s)",
            extra={"extra_data": {"step": step_name, "elapsed_seconds": round(elapsed, 2)}},
        )
    except Exception as e:
        elapsed = time.time() - start
        logger.error(
            f"Failed: {step_name} ({elapsed:.2f}s) - {e}",
            extra={"extra_data": {"step": step_name, "elapsed_seconds": round(elapsed, 2)}},
            exc_info=True,
        )
        raise


def log_job_metrics(
    logger: logging.Logger,
    metrics: dict[str, Any],
) -> None:
    """Log job-level metrics for CloudWatch monitoring."""
    logger.info(
        f"Job metrics: {json.dumps(metrics)}",
        extra={"extra_data": {"metrics": metrics}},
    )
