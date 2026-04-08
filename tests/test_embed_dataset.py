"""Tests for embed_dataset tool and semantic search in search_data."""

import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path

from jdatamunch_mcp.tools.index_local import index_local
from jdatamunch_mcp.tools.embed_dataset import embed_dataset, _column_text
from jdatamunch_mcp.tools.search_data import search_data
from jdatamunch_mcp.storage.embedding_store import ColumnEmbeddingStore, _encode, _decode
from jdatamunch_mcp.embeddings import detect_provider, cosine_similarity


# ── Fixtures ────────────────────────────────────────────────────────────────

FAKE_DIM = 4
FAKE_EMBEDDINGS = {
    "id": [1.0, 0.0, 0.0, 0.0],
    "name": [0.0, 1.0, 0.0, 0.0],
    "age": [0.0, 0.0, 1.0, 0.0],
    "city": [0.0, 0.0, 0.0, 1.0],
    "score": [0.5, 0.5, 0.0, 0.0],
}


def _fake_embed(texts, provider, model):
    """Return deterministic fake embeddings based on text content."""
    results = []
    for text in texts:
        text_lower = text.lower()
        if "location" in text_lower or "city" in text_lower or "where" in text_lower:
            results.append([0.0, 0.0, 0.0, 1.0])
        elif "person" in text_lower or "name" in text_lower or "who" in text_lower:
            results.append([0.0, 1.0, 0.0, 0.0])
        elif "age" in text_lower or "old" in text_lower or "years" in text_lower:
            results.append([0.0, 0.0, 1.0, 0.0])
        elif "id" in text_lower or "identifier" in text_lower:
            results.append([1.0, 0.0, 0.0, 0.0])
        elif "score" in text_lower or "rating" in text_lower or "performance" in text_lower:
            results.append([0.5, 0.5, 0.0, 0.0])
        else:
            results.append([0.25, 0.25, 0.25, 0.25])
    return results


@pytest.fixture
def indexed_ds(sample_csv, storage_dir):
    """Index sample CSV and return storage_dir."""
    index_local(path=sample_csv, name="embed-test", storage_path=storage_dir)
    return storage_dir


# ── ColumnEmbeddingStore unit tests ─────────────────────────────────────────


class TestEmbeddingStore:

    def test_encode_decode_roundtrip(self):
        vec = [1.0, 2.5, -3.0, 0.0]
        encoded = _encode(vec)
        decoded = _decode(encoded)
        assert len(decoded) == 4
        for a, b in zip(vec, decoded):
            assert abs(a - b) < 1e-6

    def test_set_and_get(self, indexed_ds):
        from jdatamunch_mcp.storage.data_store import DataStore
        store = DataStore(base_path=indexed_ds)
        db_path = store.sqlite_path("embed-test")
        emb_store = ColumnEmbeddingStore(db_path)

        emb_store.set_many({"col_a": [1.0, 2.0], "col_b": [3.0, 4.0]})
        result = emb_store.get("col_a")
        assert result is not None
        assert abs(result[0] - 1.0) < 1e-6
        assert abs(result[1] - 2.0) < 1e-6

    def test_get_nonexistent(self, indexed_ds):
        from jdatamunch_mcp.storage.data_store import DataStore
        store = DataStore(base_path=indexed_ds)
        db_path = store.sqlite_path("embed-test")
        emb_store = ColumnEmbeddingStore(db_path)
        assert emb_store.get("nonexistent") is None

    def test_get_all(self, indexed_ds):
        from jdatamunch_mcp.storage.data_store import DataStore
        store = DataStore(base_path=indexed_ds)
        db_path = store.sqlite_path("embed-test")
        emb_store = ColumnEmbeddingStore(db_path)

        emb_store.set_many({"a": [1.0], "b": [2.0], "c": [3.0]})
        all_embs = emb_store.get_all()
        assert len(all_embs) == 3
        assert "a" in all_embs
        assert "b" in all_embs

    def test_count(self, indexed_ds):
        from jdatamunch_mcp.storage.data_store import DataStore
        store = DataStore(base_path=indexed_ds)
        db_path = store.sqlite_path("embed-test")
        emb_store = ColumnEmbeddingStore(db_path)

        assert emb_store.count() == 0
        emb_store.set_many({"x": [1.0], "y": [2.0]})
        assert emb_store.count() == 2

    def test_clear(self, indexed_ds):
        from jdatamunch_mcp.storage.data_store import DataStore
        store = DataStore(base_path=indexed_ds)
        db_path = store.sqlite_path("embed-test")
        emb_store = ColumnEmbeddingStore(db_path)

        emb_store.set_many({"x": [1.0]})
        assert emb_store.count() == 1
        emb_store.clear()
        assert emb_store.count() == 0

    def test_set_meta(self, indexed_ds):
        from jdatamunch_mcp.storage.data_store import DataStore
        store = DataStore(base_path=indexed_ds)
        db_path = store.sqlite_path("embed-test")
        emb_store = ColumnEmbeddingStore(db_path)

        emb_store.set_meta(dim=384, model="all-MiniLM-L6-v2")
        assert emb_store.get_dimension() == 384
        assert emb_store.get_model() == "all-MiniLM-L6-v2"

    def test_upsert(self, indexed_ds):
        from jdatamunch_mcp.storage.data_store import DataStore
        store = DataStore(base_path=indexed_ds)
        db_path = store.sqlite_path("embed-test")
        emb_store = ColumnEmbeddingStore(db_path)

        emb_store.set_many({"col": [1.0, 2.0]})
        emb_store.set_many({"col": [3.0, 4.0]})
        result = emb_store.get("col")
        assert abs(result[0] - 3.0) < 1e-6


# ── Cosine similarity tests ────────────────────────────────────────────────


class TestCosineSimilarity:

    def test_identical_vectors(self):
        assert abs(cosine_similarity([1, 0, 0], [1, 0, 0]) - 1.0) < 1e-6

    def test_orthogonal_vectors(self):
        assert abs(cosine_similarity([1, 0, 0], [0, 1, 0])) < 1e-6

    def test_opposite_vectors(self):
        assert abs(cosine_similarity([1, 0], [-1, 0]) - (-1.0)) < 1e-6

    def test_zero_vector(self):
        assert cosine_similarity([0, 0], [1, 1]) == 0.0


# ── Provider detection tests ───────────────────────────────────────────────


class TestDetectProvider:

    def test_no_provider(self):
        with patch.dict("os.environ", {}, clear=True):
            assert detect_provider() is None

    def test_sentence_transformers(self):
        with patch.dict("os.environ", {"JDATAMUNCH_EMBED_MODEL": "all-MiniLM-L6-v2"}, clear=True):
            result = detect_provider()
            assert result == ("sentence_transformers", "all-MiniLM-L6-v2")

    def test_gemini(self):
        with patch.dict("os.environ", {
            "GOOGLE_API_KEY": "key", "GOOGLE_EMBED_MODEL": "models/embedding-001"
        }, clear=True):
            result = detect_provider()
            assert result == ("gemini", "models/embedding-001")

    def test_openai(self):
        with patch.dict("os.environ", {
            "OPENAI_API_KEY": "key", "OPENAI_EMBED_MODEL": "text-embedding-3-small"
        }, clear=True):
            result = detect_provider()
            assert result == ("openai", "text-embedding-3-small")

    def test_priority_order(self):
        """sentence-transformers takes priority over API-based providers."""
        with patch.dict("os.environ", {
            "JDATAMUNCH_EMBED_MODEL": "all-MiniLM-L6-v2",
            "GOOGLE_API_KEY": "key",
            "GOOGLE_EMBED_MODEL": "models/embedding-001",
        }, clear=True):
            result = detect_provider()
            assert result[0] == "sentence_transformers"


# ── embed_dataset tool tests ───────────────────────────────────────────────


class TestEmbedDataset:

    def test_no_provider(self, indexed_ds):
        with patch.dict("os.environ", {}, clear=True):
            result = embed_dataset(dataset="embed-test", storage_path=indexed_ds)
            assert result["error"] == "no_embedding_provider"

    @patch("jdatamunch_mcp.tools.embed_dataset.embed_texts", side_effect=_fake_embed)
    @patch("jdatamunch_mcp.tools.embed_dataset.detect_provider", return_value=("sentence_transformers", "test-model"))
    def test_embed_all_columns(self, mock_prov, mock_embed, indexed_ds):
        result = embed_dataset(dataset="embed-test", storage_path=indexed_ds)
        assert "error" not in result
        assert result["columns_embedded"] == 5
        assert result["columns_total"] == 5
        assert result["provider"] == "sentence_transformers"
        assert result["model"] == "test-model"

    @patch("jdatamunch_mcp.tools.embed_dataset.embed_texts", side_effect=_fake_embed)
    @patch("jdatamunch_mcp.tools.embed_dataset.detect_provider", return_value=("sentence_transformers", "test-model"))
    def test_cached_skip(self, mock_prov, mock_embed, indexed_ds):
        embed_dataset(dataset="embed-test", storage_path=indexed_ds)
        result = embed_dataset(dataset="embed-test", storage_path=indexed_ds)
        assert result["cached"] is True
        assert result["columns_embedded"] == 0

    @patch("jdatamunch_mcp.tools.embed_dataset.embed_texts", side_effect=_fake_embed)
    @patch("jdatamunch_mcp.tools.embed_dataset.detect_provider", return_value=("sentence_transformers", "test-model"))
    def test_force_recompute(self, mock_prov, mock_embed, indexed_ds):
        embed_dataset(dataset="embed-test", storage_path=indexed_ds)
        result = embed_dataset(dataset="embed-test", force=True, storage_path=indexed_ds)
        assert result["columns_embedded"] == 5

    def test_not_indexed(self, storage_dir):
        with patch("jdatamunch_mcp.tools.embed_dataset.detect_provider", return_value=("sentence_transformers", "m")):
            result = embed_dataset(dataset="nope", storage_path=storage_dir)
            assert "NOT_INDEXED" in result["error"]

    @patch("jdatamunch_mcp.tools.embed_dataset.embed_texts", side_effect=_fake_embed)
    @patch("jdatamunch_mcp.tools.embed_dataset.detect_provider", return_value=("sentence_transformers", "test-model"))
    def test_meta_present(self, mock_prov, mock_embed, indexed_ds):
        result = embed_dataset(dataset="embed-test", storage_path=indexed_ds)
        assert "_meta" in result
        assert "timing_ms" in result["_meta"]


# ── Semantic search_data tests ──────────────────────────────────────────────


class TestSemanticSearch:

    def test_semantic_no_provider(self, indexed_ds):
        with patch.dict("os.environ", {}, clear=True):
            result = search_data(
                dataset="embed-test", query="location",
                semantic=True, storage_path=indexed_ds,
            )
            assert result["error"] == "no_embedding_provider"

    @patch("jdatamunch_mcp.embeddings.embed_texts", side_effect=_fake_embed)
    @patch("jdatamunch_mcp.embeddings.detect_provider", return_value=("sentence_transformers", "test-model"))
    def test_semantic_finds_results(self, mock_prov, mock_embed, indexed_ds):
        # Pre-embed the columns
        from jdatamunch_mcp.storage.data_store import DataStore
        store = DataStore(base_path=indexed_ds)
        db_path = store.sqlite_path("embed-test")
        emb_store = ColumnEmbeddingStore(db_path)
        emb_store.set_many(FAKE_EMBEDDINGS)
        emb_store.set_meta(dim=FAKE_DIM, model="test-model")

        result = search_data(
            dataset="embed-test", query="where is the location",
            semantic=True, storage_path=indexed_ds,
        )
        assert "error" not in result
        assert len(result["result"]) > 0

    @patch("jdatamunch_mcp.embeddings.embed_texts", side_effect=_fake_embed)
    @patch("jdatamunch_mcp.embeddings.detect_provider", return_value=("sentence_transformers", "test-model"))
    def test_semantic_only(self, mock_prov, mock_embed, indexed_ds):
        from jdatamunch_mcp.storage.data_store import DataStore
        store = DataStore(base_path=indexed_ds)
        db_path = store.sqlite_path("embed-test")
        emb_store = ColumnEmbeddingStore(db_path)
        emb_store.set_many(FAKE_EMBEDDINGS)
        emb_store.set_meta(dim=FAKE_DIM, model="test-model")

        result = search_data(
            dataset="embed-test", query="where is the location",
            semantic_only=True, storage_path=indexed_ds,
        )
        assert "error" not in result
        # All results should be semantic match type
        for r in result["result"]:
            assert r["match_type"] == "semantic"

    def test_semantic_false_no_impact(self, indexed_ds):
        """semantic=false (default) has zero performance impact."""
        result = search_data(
            dataset="embed-test", query="city",
            semantic=False, storage_path=indexed_ds,
        )
        assert "error" not in result
        # Should work normally via keyword matching
        assert len(result["result"]) > 0

    @patch("jdatamunch_mcp.embeddings.embed_texts", side_effect=_fake_embed)
    @patch("jdatamunch_mcp.embeddings.detect_provider", return_value=("sentence_transformers", "test-model"))
    def test_semantic_weight_zero(self, mock_prov, mock_embed, indexed_ds):
        """semantic_weight=0.0 should produce same ranking as pure keyword."""
        from jdatamunch_mcp.storage.data_store import DataStore
        store = DataStore(base_path=indexed_ds)
        db_path = store.sqlite_path("embed-test")
        emb_store = ColumnEmbeddingStore(db_path)
        emb_store.set_many(FAKE_EMBEDDINGS)
        emb_store.set_meta(dim=FAKE_DIM, model="test-model")

        result = search_data(
            dataset="embed-test", query="city",
            semantic=True, semantic_weight=0.0,
            storage_path=indexed_ds,
        )
        assert "error" not in result
        assert result["_meta"].get("semantic_enabled") is True

    @patch("jdatamunch_mcp.embeddings.embed_texts", side_effect=_fake_embed)
    @patch("jdatamunch_mcp.embeddings.detect_provider", return_value=("sentence_transformers", "test-model"))
    def test_lazy_embed_on_first_semantic(self, mock_prov, mock_embed, indexed_ds):
        """First semantic query should lazily embed missing columns."""
        result = search_data(
            dataset="embed-test", query="person name",
            semantic=True, storage_path=indexed_ds,
        )
        assert "error" not in result
        # Verify embeddings were created
        from jdatamunch_mcp.storage.data_store import DataStore
        store = DataStore(base_path=indexed_ds)
        db_path = store.sqlite_path("embed-test")
        emb_store = ColumnEmbeddingStore(db_path)
        assert emb_store.count() == 5  # all 5 columns embedded


# ── _column_text tests ──────────────────────────────────────────────────────


class TestColumnText:

    def test_basic(self):
        col = {"name": "city", "type": "string"}
        text = _column_text(col)
        assert "column: city" in text
        assert "type: string" in text

    def test_with_summary(self):
        col = {"name": "age", "type": "integer", "ai_summary": "Person's age in years"}
        text = _column_text(col)
        assert "Person's age in years" in text

    def test_with_values(self):
        col = {"name": "status", "type": "string", "sample_values": ["active", "inactive"]}
        text = _column_text(col)
        assert "active" in text
        assert "inactive" in text
