"""embed_dataset tool — precompute and cache column embeddings for semantic search.

Optional warm-up. ``search_data`` with ``semantic=true`` lazily embeds
missing columns on first use; ``embed_dataset`` just warms the cache so
the first semantic query returns immediately.
"""

import logging
import time
from typing import Optional

from ..config import get_index_path
from ..embeddings import detect_provider, embed_texts
from ..storage.data_store import DataStore
from ..storage.embedding_store import ColumnEmbeddingStore

logger = logging.getLogger(__name__)


def _column_text(col: dict) -> str:
    """Build the text string used to represent a column for embedding."""
    parts = [
        f"column: {col['name']}",
        f"type: {col.get('type', 'unknown')}",
    ]
    if col.get("ai_summary"):
        parts.append(col["ai_summary"])
    # Add sample values for context
    samples = col.get("sample_values") or []
    if samples:
        parts.append(f"values: {', '.join(str(v) for v in samples[:10])}")
    # Add top values for low-cardinality columns
    if col.get("value_index"):
        top = list(col["value_index"].keys())[:10]
        parts.append(f"categories: {', '.join(str(v) for v in top)}")
    elif col.get("top_values"):
        top = [tv["value"] for tv in col["top_values"][:10]]
        parts.append(f"categories: {', '.join(str(v) for v in top)}")
    return ". ".join(parts)


def embed_dataset(
    dataset: str,
    force: bool = False,
    storage_path: Optional[str] = None,
) -> dict:
    """Precompute and store column embeddings for a dataset.

    Args:
        dataset: Dataset identifier.
        force: Recompute all embeddings even if cached (default False).
        storage_path: Custom storage path.
    """
    t0 = time.time()

    provider_info = detect_provider()
    if provider_info is None:
        return {
            "error": "no_embedding_provider",
            "message": (
                "No embedding provider configured. Set one of: "
                "JDATAMUNCH_EMBED_MODEL (sentence-transformers, free/local), "
                "GOOGLE_API_KEY + GOOGLE_EMBED_MODEL (Gemini), or "
                "OPENAI_API_KEY + OPENAI_EMBED_MODEL (OpenAI)."
            ),
        }
    provider, model = provider_info

    store = DataStore(base_path=storage_path or str(get_index_path()))
    idx = store.load(dataset)
    if idx is None:
        return {"error": f"NOT_INDEXED: dataset {dataset!r} is not indexed."}

    db_path = store.sqlite_path(dataset)
    emb_store = ColumnEmbeddingStore(db_path)

    # Detect model mismatch
    stored_model = emb_store.get_model()
    if not force and stored_model and stored_model != model:
        logger.info("embed_dataset: model changed (%r → %r); forcing re-embed", stored_model, model)
        force = True

    if force:
        emb_store.clear()
        columns_to_embed = list(idx.columns)
    else:
        existing = set(emb_store.get_all().keys())
        columns_to_embed = [c for c in idx.columns if c["name"] not in existing]

    if not columns_to_embed:
        return {
            "dataset": dataset,
            "provider": provider,
            "model": model,
            "columns_total": len(idx.columns),
            "columns_embedded": 0,
            "cached": True,
            "_meta": {"timing_ms": round((time.time() - t0) * 1000, 1)},
        }

    texts = [_column_text(c) for c in columns_to_embed]
    try:
        vecs = embed_texts(texts, provider, model)
    except Exception as exc:
        return {"error": f"EMBEDDING_FAILED: {exc}"}

    emb_store.set_many({columns_to_embed[i]["name"]: vecs[i] for i in range(len(vecs))})

    if vecs:
        emb_store.set_meta(dim=len(vecs[0]), model=model)

    return {
        "dataset": dataset,
        "provider": provider,
        "model": model,
        "columns_total": len(idx.columns),
        "columns_embedded": len(columns_to_embed),
        "embedding_dimension": len(vecs[0]) if vecs else None,
        "_meta": {"timing_ms": round((time.time() - t0) * 1000, 1)},
    }
