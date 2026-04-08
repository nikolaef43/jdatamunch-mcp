"""summarize_dataset tool: Generate or regenerate summaries for an indexed dataset."""

import json
import time
from typing import Optional

from ..config import get_index_path
from ..storage.data_store import DataStore, _index_to_dict
from ..summarizer import summarize_dataset as _summarize_ds, summarize_column


def summarize_dataset(
    dataset: str,
    storage_path: Optional[str] = None,
) -> dict:
    """Generate natural-language summaries for a dataset and all its columns.

    Works on already-indexed datasets — reads profiles from index.json,
    generates summaries, and writes them back.  No re-parsing of source files.
    """
    t0 = time.time()
    store = DataStore(base_path=storage_path or str(get_index_path()))

    idx = store.load(dataset)
    if idx is None:
        return {"error": f"NOT_INDEXED: dataset {dataset!r} is not indexed. Call index_local first."}

    # Generate column summaries
    for col in idx.columns:
        col["ai_summary"] = summarize_column(col)

    # Generate dataset summary
    idx.dataset_summary = _summarize_ds(
        dataset_id=idx.dataset,
        columns=idx.columns,
        row_count=idx.row_count,
        source_format=idx.source_format,
        source_size_bytes=idx.source_size_bytes,
        source_path=idx.source_path,
    )

    # Persist updated index
    index_path = store.index_path(dataset)
    tmp = index_path.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(_index_to_dict(idx), f, indent=2)
    tmp.replace(index_path)

    # Collect column summaries for response
    col_summaries = [
        {"name": c["name"], "summary": c.get("ai_summary", "")}
        for c in idx.columns
    ]

    return {
        "result": {
            "dataset": dataset,
            "dataset_summary": idx.dataset_summary,
            "column_summaries": col_summaries,
            "columns_summarized": len(col_summaries),
        },
        "_meta": {
            "timing_ms": round((time.time() - t0) * 1000, 1),
        },
    }
