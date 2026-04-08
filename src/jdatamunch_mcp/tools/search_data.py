"""search_data tool: Search across column names, values, and metadata."""

import logging
import time
from typing import Optional

from ..config import get_index_path, HARD_CAP_SEARCH_MAX_RESULTS
from ..storage.data_store import DataStore
from ..storage.token_tracker import get_total_saved

logger = logging.getLogger(__name__)

# Scoring weights (per PRD)
_W_NAME_EXACT = 20
_W_NAME_SUBSTR = 10
_W_NAME_WORD = 5
_W_AI_SUMMARY_WORD = 3
_W_VALUE_EXACT = 8
_W_VALUE_SUBSTR = 4
_W_TYPE_BOOST = 2

_DATE_KEYWORDS = frozenset(["date", "time", "year", "month", "day", "datetime", "timestamp"])
_NUM_KEYWORDS = frozenset(["count", "amount", "number", "num", "total", "age", "id", "code"])


def _score_column(col: dict, query_lower: str, query_words: set) -> tuple:
    """Score a column against a query. Returns (score, match_details)."""
    score = 0
    matched_values: list = []
    match_type = "schema"

    name_lower = col["name"].lower()

    # Column name scoring
    if query_lower == name_lower:
        score += _W_NAME_EXACT
    elif query_lower in name_lower:
        score += _W_NAME_SUBSTR
    else:
        name_words = set(name_lower.replace("_", " ").replace("-", " ").split())
        word_hits = len(query_words & name_words)
        if word_hits:
            score += word_hits * _W_NAME_WORD

    # AI summary scoring (when available)
    if col.get("ai_summary"):
        summary_lower = col["ai_summary"].lower()
        for word in query_words:
            if word in summary_lower:
                score += _W_AI_SUMMARY_WORD

    # Value index: exact match
    value_source: list = []
    if col.get("value_index"):
        value_source = list(col["value_index"].keys())
    elif col.get("top_values"):
        value_source = [tv["value"] for tv in col["top_values"]]

    for v in value_source:
        v_lower = str(v).lower()
        hit = False
        for word in query_words:
            if word == v_lower:
                score += _W_VALUE_EXACT
                if str(v) not in matched_values:
                    matched_values.append(str(v))
                match_type = "value"
                hit = True
                break
        if not hit:
            for word in query_words:
                if len(word) >= 3 and word in v_lower:
                    score += _W_VALUE_SUBSTR
                    if str(v) not in matched_values:
                        matched_values.append(str(v))
                    match_type = "value"
                    break

    # Type-aware boost
    if col["type"] == "datetime" and query_words & _DATE_KEYWORDS:
        score += _W_TYPE_BOOST
    elif col["type"] in ("integer", "float") and query_words & _NUM_KEYWORDS:
        score += _W_TYPE_BOOST

    return score, matched_values, match_type


def _column_text(col: dict) -> str:
    """Build text representation of a column for embedding."""
    parts = [
        f"column: {col['name']}",
        f"type: {col.get('type', 'unknown')}",
    ]
    if col.get("ai_summary"):
        parts.append(col["ai_summary"])
    samples = col.get("sample_values") or []
    if samples:
        parts.append(f"values: {', '.join(str(v) for v in samples[:10])}")
    if col.get("value_index"):
        top = list(col["value_index"].keys())[:10]
        parts.append(f"categories: {', '.join(str(v) for v in top)}")
    elif col.get("top_values"):
        top = [tv["value"] for tv in col["top_values"][:10]]
        parts.append(f"categories: {', '.join(str(v) for v in top)}")
    return ". ".join(parts)


def _semantic_scores(
    query: str,
    columns: list[dict],
    store: "DataStore",
    dataset: str,
) -> dict[str, float]:
    """Compute semantic similarity scores for all columns.

    Lazily embeds missing columns on first call. Returns
    {column_name: cosine_similarity}.
    """
    from ..embeddings import detect_provider, embed_texts, cosine_similarity
    from ..storage.embedding_store import ColumnEmbeddingStore

    provider_info = detect_provider()
    if provider_info is None:
        raise ValueError(
            "No embedding provider configured. Set one of: "
            "JDATAMUNCH_EMBED_MODEL (sentence-transformers, free/local), "
            "GOOGLE_API_KEY + GOOGLE_EMBED_MODEL (Gemini), or "
            "OPENAI_API_KEY + OPENAI_EMBED_MODEL (OpenAI)."
        )
    provider, model = provider_info

    db_path = store.sqlite_path(dataset)
    emb_store = ColumnEmbeddingStore(db_path)

    # Lazy embed: compute missing column embeddings
    all_embeddings = emb_store.get_all()
    missing = [c for c in columns if c["name"] not in all_embeddings]
    if missing:
        texts = [_column_text(c) for c in missing]
        vecs = embed_texts(texts, provider, model)
        new_embeddings = {missing[i]["name"]: vecs[i] for i in range(len(vecs))}
        emb_store.set_many(new_embeddings)
        if vecs:
            emb_store.set_meta(dim=len(vecs[0]), model=model)
        all_embeddings.update(new_embeddings)

    # Embed the query
    query_vec = embed_texts([query], provider, model)[0]

    # Compute cosine similarity for each column
    scores: dict[str, float] = {}
    for col in columns:
        col_vec = all_embeddings.get(col["name"])
        if col_vec:
            scores[col["name"]] = cosine_similarity(query_vec, col_vec)
    return scores


def search_data(
    dataset: str,
    query: str,
    search_scope: str = "all",
    max_results: int = 10,
    semantic: bool = False,
    semantic_weight: float = 0.5,
    semantic_only: bool = False,
    storage_path: Optional[str] = None,
) -> dict:
    """Search across column names, values, and metadata.

    Returns column-level results with IDs — tells the agent where to look,
    not the data itself.

    When semantic=true, uses embedding-based similarity alongside keyword
    scoring. semantic_weight controls the blend (0.0 = pure keyword,
    1.0 = pure semantic). semantic_only=true skips keyword scoring entirely.
    """
    t0 = time.time()
    max_results = min(max(1, max_results), HARD_CAP_SEARCH_MAX_RESULTS)
    semantic_weight = max(0.0, min(1.0, semantic_weight))
    store = DataStore(base_path=storage_path or str(get_index_path()))

    idx = store.load(dataset)
    if idx is None:
        return {"error": f"NOT_INDEXED: dataset {dataset!r} is not indexed."}

    # Semantic scoring (if requested)
    sem_scores: dict[str, float] = {}
    if semantic or semantic_only:
        try:
            sem_scores = _semantic_scores(query, idx.columns, store, dataset)
        except ValueError as exc:
            return {"error": "no_embedding_provider", "message": str(exc)}
        except Exception as exc:
            logger.warning("Semantic search failed: %s", exc)
            if semantic_only:
                return {"error": f"SEMANTIC_FAILED: {exc}"}
            # Fall back to keyword-only

    query_lower = query.lower().strip()
    query_words = set(query_lower.split())

    scored: list = []
    for col in idx.columns:
        bm25_score = 0.0
        mv: list = []
        mt = "schema"

        if not semantic_only:
            if search_scope == "schema":
                name_lower = col["name"].lower()
                if query_lower == name_lower:
                    bm25_score = _W_NAME_EXACT
                elif query_lower in name_lower:
                    bm25_score = _W_NAME_SUBSTR
                else:
                    name_words = set(name_lower.replace("_", " ").split())
                    bm25_score = len(query_words & name_words) * _W_NAME_WORD
                if bm25_score > 0:
                    mt = "schema"
            elif search_scope == "values":
                value_source = []
                if col.get("value_index"):
                    value_source = list(col["value_index"].keys())
                elif col.get("top_values"):
                    value_source = [tv["value"] for tv in col["top_values"]]
                for v in value_source:
                    v_lower = str(v).lower()
                    for word in query_words:
                        if word == v_lower:
                            bm25_score += _W_VALUE_EXACT
                            mv.append(str(v))
                            break
                        elif len(word) >= 3 and word in v_lower:
                            bm25_score += _W_VALUE_SUBSTR
                            mv.append(str(v))
                            break
                if bm25_score > 0:
                    mt = "value"
            else:
                bm25_score, mv, mt = _score_column(col, query_lower, query_words)

        # Combine scores
        sem = sem_scores.get(col["name"], 0.0) if sem_scores else 0.0

        if semantic_only:
            combined = sem
            if sem > 0:
                mt = "semantic"
        elif sem_scores:
            # Normalize BM25 score to [0, 1] range for blending
            combined = (1 - semantic_weight) * bm25_score + semantic_weight * (sem * 20)
            if bm25_score == 0 and sem > 0.3:
                mt = "semantic"
        else:
            combined = bm25_score

        if combined > 0:
            scored.append((combined, col, mv, mt))

    scored.sort(key=lambda x: x[0], reverse=True)

    results = []
    max_score = scored[0][0] if scored else 1
    for sc, col, mv, mt in scored[:max_results]:
        r: dict = {
            "id": f"{dataset}::{col['name']}#column",
            "name": col["name"],
            "type": col["type"],
            "cardinality": col["cardinality"],
            "null_pct": col["null_pct"],
            "match_type": mt,
            "score": round(sc / max_score, 2),
        }
        if mv:
            r["matched_values"] = mv[:10]
        if col.get("ai_summary"):
            r["ai_summary"] = col["ai_summary"]
        results.append(r)

    total_saved = get_total_saved(str(store.base_path))

    meta: dict = {
        "timing_ms": round((time.time() - t0) * 1000, 1),
        "tokens_saved": 0,
        "total_tokens_saved": total_saved,
    }
    if sem_scores:
        meta["semantic_enabled"] = True

    return {
        "result": results,
        "_meta": meta,
    }
