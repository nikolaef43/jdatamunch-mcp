"""get_schema_drift tool: Compare schema metadata between two indexed datasets."""

import time
from typing import Optional

from ..config import get_index_path
from ..storage.data_store import DataStore


def get_schema_drift(
    dataset_a: str,
    dataset_b: str,
    storage_path: Optional[str] = None,
) -> dict:
    """Compare schema (columns, types, nullability) between two indexed datasets.

    Pure in-memory comparison of already-indexed metadata — no re-reading source files.
    Useful for detecting schema changes between versions of the same dataset.
    """
    t0 = time.perf_counter()
    store = DataStore(base_path=storage_path or str(get_index_path()))

    idx_a = store.load(dataset_a)
    if idx_a is None:
        return {"error": f"NOT_INDEXED: dataset {dataset_a!r} is not indexed. Call index_local first."}

    idx_b = store.load(dataset_b)
    if idx_b is None:
        return {"error": f"NOT_INDEXED: dataset {dataset_b!r} is not indexed. Call index_local first."}

    cols_a = {c["name"]: c for c in idx_a.columns}
    cols_b = {c["name"]: c for c in idx_b.columns}

    names_a = set(cols_a)
    names_b = set(cols_b)

    added_columns = sorted(names_b - names_a)
    removed_columns = sorted(names_a - names_b)

    type_changes = []
    nullability_changes = []

    for name in sorted(names_a & names_b):
        ca = cols_a[name]
        cb = cols_b[name]

        if ca["type"] != cb["type"]:
            type_changes.append({
                "column": name,
                "type_in_a": ca["type"],
                "type_in_b": cb["type"],
            })

        null_a = ca.get("null_pct", 0.0) or 0.0
        null_b = cb.get("null_pct", 0.0) or 0.0
        # Flag columns where null rate crossed a meaningful threshold (1%)
        if abs(null_a - null_b) >= 1.0:
            nullability_changes.append({
                "column": name,
                "null_pct_in_a": round(null_a, 2),
                "null_pct_in_b": round(null_b, 2),
                "delta": round(null_b - null_a, 2),
            })

    total_changes = (
        len(added_columns) + len(removed_columns)
        + len(type_changes) + len(nullability_changes)
    )

    if total_changes == 0:
        assessment = "identical"
    elif len(removed_columns) > 0 or len(type_changes) > 0:
        assessment = "breaking"
    else:
        assessment = "additive"

    return {
        "result": {
            "dataset_a": dataset_a,
            "dataset_b": dataset_b,
            "columns_in_a": len(cols_a),
            "columns_in_b": len(cols_b),
            "added_columns": added_columns,
            "removed_columns": removed_columns,
            "type_changes": type_changes,
            "nullability_changes": nullability_changes,
            "total_changes": total_changes,
            "assessment": assessment,
        },
        "_meta": {
            "timing_ms": round((time.perf_counter() - t0) * 1000, 1),
        },
    }
