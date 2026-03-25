"""Tests for database LIBNAME / PROC IMPORT DBMS=XLSX detection in the SAS parser,
and for database-related content in the Bedrock prompts.
"""

from pathlib import Path

import pytest

from analyzer.sas_parser import BlockType, parse_sas_file

FIXTURES = Path(__file__).resolve().parents[2] / "sas_source"


class TestDatabaseLibnameDetection:
    """Verify that the parser detects DB2 / ODBC / OLEDB LIBNAME statements."""

    @pytest.fixture(autouse=True)
    def parse(self):
        self.result = parse_sas_file(FIXTURES / "sample_db_sources.sas")

    def test_no_errors(self):
        assert not self.result.errors

    def test_detects_db2_libname(self):
        libnames = [
            b for b in self.result.blocks
            if b.block_type == BlockType.LIBNAME
        ]
        db2_libs = [b for b in libnames if b.attributes.get("db_engine") == "DB2"]
        assert len(db2_libs) >= 1
        assert db2_libs[0].attributes["is_database"] is True
        assert db2_libs[0].attributes["lib_name"] == "dwh"

    def test_detects_odbc_libname(self):
        libnames = [
            b for b in self.result.blocks
            if b.block_type == BlockType.LIBNAME
        ]
        odbc_libs = [b for b in libnames if b.attributes.get("db_engine") == "ODBC"]
        assert len(odbc_libs) >= 1
        assert odbc_libs[0].attributes["is_database"] is True
        assert odbc_libs[0].attributes["lib_name"] == "crm"

    def test_detects_connect_to_in_proc_sql(self):
        proc_sqls = [
            b for b in self.result.blocks
            if b.block_type == BlockType.PROC_SQL
        ]
        connected = [b for b in proc_sqls if b.attributes.get("has_connect_to")]
        assert len(connected) >= 1
        assert connected[0].attributes["connect_engine"] == "DB2"

    def test_detects_proc_import_xlsx(self):
        imports = [
            b for b in self.result.blocks
            if b.block_type == BlockType.PROC_IMPORT
        ]
        assert len(imports) >= 1
        assert imports[0].attributes.get("dbms") == "XLSX"
        assert imports[0].attributes.get("is_excel") is True

    def test_file_libname_has_no_db_engine(self):
        """Regular file-based LIBNAMEs should NOT have db_engine."""
        simple = parse_sas_file(FIXTURES / "sample_simple.sas")
        libnames = [
            b for b in simple.blocks
            if b.block_type == BlockType.LIBNAME
        ]
        assert len(libnames) >= 1
        for lb in libnames:
            assert lb.attributes.get("is_database") is None


class TestPromptsIncludeDatabaseHelpers:
    """Verify that the Bedrock prompts document database I/O helpers."""

    def test_glue_prompt_has_read_jdbc(self):
        from converter.prompts import SYSTEM_PROMPT
        assert "read_jdbc" in SYSTEM_PROMPT
        assert "get_db_credentials" in SYSTEM_PROMPT

    def test_glue_prompt_has_read_excel(self):
        from converter.prompts import SYSTEM_PROMPT
        assert "read_excel" in SYSTEM_PROMPT

    def test_glue_prompt_has_db_libname_rules(self):
        from converter.prompts import SYSTEM_PROMPT
        assert "LIBNAME" in SYSTEM_PROMPT
        assert "DB2" in SYSTEM_PROMPT
        assert "ODBC" in SYSTEM_PROMPT

    def test_glue_prompt_has_xlsx_rule(self):
        from converter.prompts import SYSTEM_PROMPT
        assert "DBMS=XLSX" in SYSTEM_PROMPT

    def test_lambda_prompt_has_read_db(self):
        from converter.prompts import SYSTEM_PROMPT_LAMBDA
        assert "read_db" in SYSTEM_PROMPT_LAMBDA
        assert "get_db_credentials" in SYSTEM_PROMPT_LAMBDA

    def test_lambda_prompt_has_read_excel(self):
        from converter.prompts import SYSTEM_PROMPT_LAMBDA
        assert "read_excel" in SYSTEM_PROMPT_LAMBDA

    def test_lambda_prompt_has_db_libname_rules(self):
        from converter.prompts import SYSTEM_PROMPT_LAMBDA
        assert "DB2" in SYSTEM_PROMPT_LAMBDA
        assert "ODBC" in SYSTEM_PROMPT_LAMBDA

    def test_lambda_prompt_has_xlsx_rule(self):
        from converter.prompts import SYSTEM_PROMPT_LAMBDA
        assert "DBMS=XLSX" in SYSTEM_PROMPT_LAMBDA


class TestGlueIoUtils:
    """Test the new functions in glue_jobs/common/io_utils.py (import-level checks)."""

    def test_get_db_credentials_importable(self):
        from glue_jobs.common.io_utils import get_db_credentials
        assert callable(get_db_credentials)

    def test_read_jdbc_importable(self):
        from glue_jobs.common.io_utils import read_jdbc
        assert callable(read_jdbc)

    def test_read_excel_importable(self):
        from glue_jobs.common.io_utils import read_excel
        assert callable(read_excel)

    def test_jdbc_drivers_registry(self):
        from glue_jobs.common.io_utils import _JDBC_DRIVERS
        assert "mssql" in _JDBC_DRIVERS
        assert "db2" in _JDBC_DRIVERS

    def test_existing_functions_still_importable(self):
        from glue_jobs.common.io_utils import (
            read_s3_csv,
            read_s3_parquet,
            read_glue_table,
            write_s3_csv,
            write_s3_parquet,
            write_glue_table,
            s3_path,
            resolve_sas_libname,
        )
        assert callable(read_s3_csv)


class TestLambdaIoUtils:
    """Test the new lambda_jobs/common/io_utils.py."""

    def test_get_db_credentials_importable(self):
        from lambda_jobs.common.io_utils import get_db_credentials
        assert callable(get_db_credentials)

    def test_read_db_importable(self):
        from lambda_jobs.common.io_utils import read_db
        assert callable(read_db)

    def test_read_excel_importable(self):
        from lambda_jobs.common.io_utils import read_excel
        assert callable(read_excel)

    def test_build_connection_url_mssql(self):
        from lambda_jobs.common.io_utils import build_connection_url
        url = build_connection_url({
            "engine": "mssql",
            "host": "db.example.com",
            "port": 1433,
            "database": "mydb",
            "username": "user",
            "password": "pass",
        })
        assert url.startswith("mssql+pymssql://")
        assert "db.example.com" in url
        assert "mydb" in url

    def test_build_connection_url_db2(self):
        from lambda_jobs.common.io_utils import build_connection_url
        url = build_connection_url({
            "engine": "db2",
            "host": "db2.example.com",
            "port": 50000,
            "database": "warehouse",
            "username": "user",
            "password": "pass",
        })
        assert url.startswith("db2+ibm_db://")
        assert "db2.example.com" in url

    def test_build_connection_url_unsupported_engine(self):
        from lambda_jobs.common.io_utils import build_connection_url
        with pytest.raises(ValueError, match="Unsupported engine"):
            build_connection_url({
                "engine": "oracle",
                "host": "x", "database": "x",
                "username": "x", "password": "x",
            })
