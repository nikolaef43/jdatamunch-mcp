"""Tests for the summarizer module."""

import pytest

from jdatamunch_mcp.summarizer import summarize_column, summarize_dataset


# ---------------------------------------------------------------------------
# Column summary tests
# ---------------------------------------------------------------------------

class TestSummarizeColumn:

    def test_integer_pk(self):
        col = {
            "name": "id", "type": "integer", "count": 100, "null_count": 0,
            "null_pct": 0, "cardinality": 100, "cardinality_is_exact": True,
            "is_unique": True, "is_primary_key_candidate": True,
            "min": 1, "max": 100, "mean": 50.5, "median": 50.0,
            "sample_values": [], "top_values": [],
        }
        s = summarize_column(col)
        assert "Integer" in s
        assert "unique identifier" in s
        assert "1 to 100" in s

    def test_float_with_nulls(self):
        col = {
            "name": "score", "type": "float", "count": 90, "null_count": 10,
            "null_pct": 10.0, "cardinality": 50, "cardinality_is_exact": True,
            "is_unique": False, "is_primary_key_candidate": False,
            "min": 0.0, "max": 100.0, "mean": 72.5, "median": 75.0,
            "sample_values": [], "top_values": [],
        }
        s = summarize_column(col)
        assert "Float" in s
        assert "10% null" in s
        assert "0 to 100" in s
        assert "mean 72.50" in s

    def test_string_categorical(self):
        col = {
            "name": "city", "type": "string", "count": 100, "null_count": 0,
            "null_pct": 0, "cardinality": 5, "cardinality_is_exact": True,
            "is_unique": False, "is_primary_key_candidate": False,
            "sample_values": [], "top_values": [
                {"value": "NYC", "count": 40},
                {"value": "LA", "count": 30},
                {"value": "Chicago", "count": 15},
            ],
        }
        s = summarize_column(col)
        assert "categorical (5 distinct" in s
        assert "NYC" in s

    def test_datetime_column(self):
        col = {
            "name": "created_at", "type": "datetime", "count": 100, "null_count": 0,
            "null_pct": 0, "cardinality": 100, "cardinality_is_exact": True,
            "is_unique": True, "is_primary_key_candidate": False,
            "datetime_min": "2020-01-01", "datetime_max": "2024-12-31",
            "datetime_format": "%Y-%m-%d",
            "sample_values": [], "top_values": [],
        }
        s = summarize_column(col)
        assert "Datetime" in s
        assert "2020-01-01" in s
        assert "2024-12-31" in s

    def test_binary_column(self):
        col = {
            "name": "is_active", "type": "string", "count": 100, "null_count": 0,
            "null_pct": 0, "cardinality": 2, "cardinality_is_exact": True,
            "is_unique": False, "is_primary_key_candidate": False,
            "sample_values": [], "top_values": [
                {"value": "true", "count": 60},
                {"value": "false", "count": 40},
            ],
        }
        s = summarize_column(col)
        assert "binary" in s

    def test_high_null_sparse(self):
        col = {
            "name": "notes", "type": "string", "count": 20, "null_count": 80,
            "null_pct": 80.0, "cardinality": 15, "cardinality_is_exact": True,
            "is_unique": False, "is_primary_key_candidate": False,
            "sample_values": [], "top_values": [],
        }
        s = summarize_column(col)
        assert "sparse" in s
        assert "80% null" in s

    def test_constant_column(self):
        col = {
            "name": "version", "type": "string", "count": 100, "null_count": 0,
            "null_pct": 0, "cardinality": 1, "cardinality_is_exact": True,
            "is_unique": False, "is_primary_key_candidate": False,
            "sample_values": ["v1"], "top_values": [{"value": "v1", "count": 100}],
        }
        s = summarize_column(col)
        assert "single constant" in s


# ---------------------------------------------------------------------------
# Dataset summary tests
# ---------------------------------------------------------------------------

class TestSummarizeDataset:

    def _make_cols(self):
        return [
            {
                "name": "id", "type": "integer", "count": 1000, "null_count": 0,
                "null_pct": 0, "cardinality": 1000, "is_unique": True,
                "is_primary_key_candidate": True, "min": 1, "max": 1000,
                "mean": 500, "median": 500,
            },
            {
                "name": "name", "type": "string", "count": 1000, "null_count": 0,
                "null_pct": 0, "cardinality": 800, "is_unique": False,
                "is_primary_key_candidate": False,
            },
            {
                "name": "created", "type": "datetime", "count": 1000, "null_count": 0,
                "null_pct": 0, "cardinality": 500, "is_unique": False,
                "is_primary_key_candidate": False,
                "datetime_min": "2023-01-01", "datetime_max": "2024-06-30",
            },
            {
                "name": "score", "type": "float", "count": 900, "null_count": 100,
                "null_pct": 10.0, "cardinality": 200, "is_unique": False,
                "is_primary_key_candidate": False,
                "min": 0.0, "max": 100.0, "mean": 65.0, "median": 70.0,
            },
        ]

    def test_basic_summary(self):
        cols = self._make_cols()
        s = summarize_dataset("test", cols, 1000, "csv", 50000)
        assert "CSV" in s
        assert "1,000 rows" in s
        assert "4 columns" in s
        assert "48.8 KB" in s  # 50000 bytes = 48.8 KiB

    def test_pk_detected(self):
        cols = self._make_cols()
        s = summarize_dataset("test", cols, 1000, "csv", 50000)
        assert "Primary key candidate: id" in s

    def test_temporal_range(self):
        cols = self._make_cols()
        s = summarize_dataset("test", cols, 1000, "csv", 50000)
        assert "2023-01-01" in s
        assert "2024-06-30" in s

    def test_type_breakdown(self):
        cols = self._make_cols()
        s = summarize_dataset("test", cols, 1000, "csv", 50000)
        assert "1 integer" in s
        assert "1 float" in s
        assert "1 datetime" in s
        assert "1 string" in s

    def test_high_null_warning(self):
        cols = [
            {
                "name": "sparse_col", "type": "string", "count": 100,
                "null_count": 50, "null_pct": 50.0, "cardinality": 10,
                "is_unique": False, "is_primary_key_candidate": False,
            },
        ]
        s = summarize_dataset("test", cols, 100, "csv", 1000)
        assert "High null rate" in s

    def test_empty_columns(self):
        s = summarize_dataset("test", [], 0, "csv", 0)
        assert "0 rows" in s
        assert "0 columns" in s

    def test_parquet_format(self):
        cols = self._make_cols()[:1]
        s = summarize_dataset("test", cols, 100, "parquet", 10000)
        assert "Parquet" in s

    def test_xlsx_format(self):
        cols = self._make_cols()[:1]
        s = summarize_dataset("test", cols, 100, "xlsx", 10000)
        assert "Xlsx" in s


# ---------------------------------------------------------------------------
# Integration: summarize_dataset tool
# ---------------------------------------------------------------------------

class TestSummarizeDatasetTool:

    def test_summarize_indexed_dataset(self, indexed_sample, storage_dir):
        from jdatamunch_mcp.tools.summarize_dataset import summarize_dataset as tool
        result = tool(dataset="sample", storage_path=storage_dir)
        assert "error" not in result
        r = result["result"]
        assert r["dataset"] == "sample"
        assert r["dataset_summary"]
        assert r["columns_summarized"] == 5
        for cs in r["column_summaries"]:
            assert cs["summary"]  # every column gets a summary

    def test_summarize_not_indexed(self, storage_dir):
        from jdatamunch_mcp.tools.summarize_dataset import summarize_dataset as tool
        result = tool(dataset="nonexistent", storage_path=storage_dir)
        assert "error" in result

    def test_index_local_generates_summaries(self, sample_csv, storage_dir):
        """index_local should auto-generate summaries."""
        from jdatamunch_mcp.tools.index_local import index_local
        from jdatamunch_mcp.storage.data_store import DataStore

        index_local(path=sample_csv, name="with-summaries", storage_path=storage_dir)
        store = DataStore(base_path=storage_dir)
        idx = store.load("with-summaries")

        assert idx.dataset_summary is not None
        assert len(idx.dataset_summary) > 20
        for col in idx.columns:
            assert col.get("ai_summary"), f"Column {col['name']} missing ai_summary"
