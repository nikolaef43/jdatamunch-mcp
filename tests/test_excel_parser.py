"""Tests for Excel parser (.xlsx and .xls)."""

import pytest

from jdatamunch_mcp.parser import parse_file
from jdatamunch_mcp.tools.index_local import index_local
from jdatamunch_mcp.storage.data_store import DataStore


# ---------------------------------------------------------------------------
# .xlsx tests (openpyxl)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    not pytest.importorskip("openpyxl", reason="openpyxl not installed") or False,
    reason="openpyxl not installed",
)
class TestXlsxParser:
    def test_parse_returns_correct_columns(self, sample_xlsx):
        ds = parse_file(sample_xlsx)
        names = [c.name for c in ds.columns]
        assert names == ["id", "name", "age", "city", "score"]

    def test_parse_yields_correct_row_count(self, sample_xlsx):
        ds = parse_file(sample_xlsx)
        rows = list(ds.row_iterator)
        assert len(rows) == 10

    def test_parse_cell_values_are_strings(self, sample_xlsx):
        ds = parse_file(sample_xlsx)
        rows = list(ds.row_iterator)
        for row in rows:
            for cell in row:
                assert isinstance(cell, str)

    def test_numeric_integers_have_no_decimal(self, sample_xlsx):
        ds = parse_file(sample_xlsx)
        rows = list(ds.row_iterator)
        # id column — all integers
        ids = [r[0] for r in rows]
        assert "1" in ids
        assert all("." not in v for v in ids if v)

    def test_null_cells_are_empty_string(self, sample_xlsx):
        ds = parse_file(sample_xlsx)
        rows = list(ds.row_iterator)
        # row 6 (Frank): age and score are None
        frank = next(r for r in rows if r[1] == "Frank")
        assert frank[2] == ""   # age
        assert frank[4] == ""   # score

    def test_metadata_has_file_size(self, sample_xlsx):
        ds = parse_file(sample_xlsx)
        assert ds.metadata["file_size"] > 0

    def test_sheet_name_in_metadata(self, sample_xlsx):
        ds = parse_file(sample_xlsx)
        assert ds.metadata["sheet"] == "data"

    def test_index_local_xlsx(self, sample_xlsx, storage_dir):
        result = index_local(path=sample_xlsx, name="xlsx-sample", storage_path=storage_dir)
        assert "error" not in result
        r = result["result"]
        assert r["rows"] == 10
        assert r["columns"] == 5

    def test_column_types_detected(self, sample_xlsx, storage_dir):
        index_local(path=sample_xlsx, name="xlsx-sample", storage_path=storage_dir)
        store = DataStore(base_path=storage_dir)
        idx = store.load("xlsx-sample")
        col_types = {c["name"]: c["type"] for c in idx.columns}
        assert col_types["id"] == "integer"
        assert col_types["name"] == "string"
        assert col_types["score"] == "float"

    def test_incremental_skip_xlsx(self, sample_xlsx, storage_dir):
        index_local(path=sample_xlsx, name="xlsx-sample", storage_path=storage_dir)
        result2 = index_local(
            path=sample_xlsx, name="xlsx-sample", incremental=True, storage_path=storage_dir
        )
        assert result2["result"].get("skipped") is True


# ---------------------------------------------------------------------------
# .xls tests (xlrd)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    not pytest.importorskip("xlrd", reason="xlrd not installed") or False,
    reason="xlrd not installed",
)
class TestXlsParser:
    def test_parse_returns_correct_columns(self, sample_xls):
        ds = parse_file(sample_xls)
        names = [c.name for c in ds.columns]
        assert names == ["id", "name", "age", "city", "score"]

    def test_parse_yields_correct_row_count(self, sample_xls):
        ds = parse_file(sample_xls)
        rows = list(ds.row_iterator)
        assert len(rows) == 10

    def test_parse_cell_values_are_strings(self, sample_xls):
        ds = parse_file(sample_xls)
        rows = list(ds.row_iterator)
        for row in rows:
            for cell in row:
                assert isinstance(cell, str)

    def test_numeric_integers_have_no_decimal(self, sample_xls):
        ds = parse_file(sample_xls)
        rows = list(ds.row_iterator)
        ids = [r[0] for r in rows]
        assert "1" in ids
        assert all("." not in v for v in ids if v)

    def test_null_cells_are_empty_string(self, sample_xls):
        ds = parse_file(sample_xls)
        rows = list(ds.row_iterator)
        frank = next(r for r in rows if r[1] == "Frank")
        assert frank[2] == ""
        assert frank[4] == ""

    def test_metadata_has_file_size(self, sample_xls):
        ds = parse_file(sample_xls)
        assert ds.metadata["file_size"] > 0

    def test_estimated_rows_in_metadata(self, sample_xls):
        ds = parse_file(sample_xls)
        assert ds.metadata["estimated_rows"] == 10

    def test_index_local_xls(self, sample_xls, storage_dir):
        result = index_local(path=sample_xls, name="xls-sample", storage_path=storage_dir)
        assert "error" not in result
        r = result["result"]
        assert r["rows"] == 10
        assert r["columns"] == 5

    def test_column_types_detected(self, sample_xls, storage_dir):
        index_local(path=sample_xls, name="xls-sample", storage_path=storage_dir)
        store = DataStore(base_path=storage_dir)
        idx = store.load("xls-sample")
        col_types = {c["name"]: c["type"] for c in idx.columns}
        assert col_types["id"] == "integer"
        assert col_types["name"] == "string"
        assert col_types["score"] == "float"

    def test_incremental_skip_xls(self, sample_xls, storage_dir):
        index_local(path=sample_xls, name="xls-sample", storage_path=storage_dir)
        result2 = index_local(
            path=sample_xls, name="xls-sample", incremental=True, storage_path=storage_dir
        )
        assert result2["result"].get("skipped") is True


# ---------------------------------------------------------------------------
# Format-routing tests
# ---------------------------------------------------------------------------

def test_unsupported_extension_raises(tmp_path, storage_dir):
    p = tmp_path / "data.parquet"
    p.write_bytes(b"fake")
    result = index_local(path=str(p), storage_path=storage_dir)
    assert "error" in result
