"""I/O utilities for pandas Lambda handlers.

Provides helpers for reading data from JDBC databases (DB2, MSSQL Server),
Excel files on S3, and AWS Secrets Manager credential retrieval.
"""

from __future__ import annotations

import json
import logging
import os

import boto3
import pandas as pd

logger = logging.getLogger(__name__)


def get_db_credentials(secret_name: str, region: str | None = None) -> dict:
    """Fetch database credentials from AWS Secrets Manager.

    Expected JSON structure::

        {"host", "port", "database", "username", "password", "engine"}
    """
    client = boto3.client(
        "secretsmanager",
        region_name=region or os.environ.get("AWS_REGION", "eu-central-1"),
    )
    resp = client.get_secret_value(SecretId=secret_name)
    return json.loads(resp["SecretString"])


def build_connection_url(creds: dict) -> str:
    """Build a SQLAlchemy connection URL from a Secrets Manager credentials dict."""
    engine = creds.get("engine", "mssql").lower()
    host = creds["host"]
    port = creds.get("port")
    database = creds["database"]
    user = creds["username"]
    password = creds["password"]

    if engine in ("mssql", "sqlserver"):
        port = port or 1433
        return f"mssql+pymssql://{user}:{password}@{host}:{port}/{database}"
    elif engine == "db2":
        port = port or 50000
        return f"db2+ibm_db://{user}:{password}@{host}:{port}/{database}"
    else:
        raise ValueError(f"Unsupported engine: {engine}")


def read_db(
    query_or_table: str,
    secret_name: str,
    *,
    connection_url: str | None = None,
) -> pd.DataFrame:
    """Read from DB2 or MSSQL Server into a pandas DataFrame.

    If *connection_url* is not provided, credentials are fetched from
    Secrets Manager and a SQLAlchemy URL is built automatically.

    *query_or_table* can be a table name (``"schema.table"``) or a
    full SQL query (``"SELECT * FROM ..."``).
    """
    from sqlalchemy import create_engine, text

    if connection_url is None:
        creds = get_db_credentials(secret_name)
        connection_url = build_connection_url(creds)

    engine = create_engine(connection_url)

    is_query = query_or_table.strip().upper().startswith("SELECT")
    logger.info("Reading %s from database", "query" if is_query else query_or_table)

    with engine.connect() as conn:
        if is_query:
            return pd.read_sql(text(query_or_table), conn)
        return pd.read_sql_table(query_or_table, conn)


def read_excel(
    s3_path: str,
    sheet_name: str | int = 0,
    header: int = 0,
) -> pd.DataFrame:
    """Read an Excel file from S3 into a pandas DataFrame.

    Uses ``openpyxl`` engine for ``.xlsx`` and ``xlrd`` for ``.xls``.
    """
    logger.info("Reading Excel file %s (sheet=%s)", s3_path, sheet_name)
    engine = "openpyxl" if s3_path.lower().endswith(".xlsx") else "xlrd"
    return pd.read_excel(s3_path, sheet_name=sheet_name, header=header, engine=engine)
