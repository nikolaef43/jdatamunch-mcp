"""Tests for get_schema_drift tool."""

import csv
import pytest
from jdatamunch_mcp.tools.index_local import index_local
from jdatamunch_mcp.tools.get_schema_drift import get_schema_drift


@pytest.fixture
def csv_a(tmp_path):
    path = tmp_path / "a.csv"
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "name", "age", "score"])
        w.writerows([[1, "Alice", 30, 9.5], [2, "Bob", 25, 7.0], [3, "Carol", None, 8.0]])
    return str(path)


@pytest.fixture
def csv_b_identical(tmp_path):
    """Same schema as csv_a, similar null profile."""
    path = tmp_path / "b_identical.csv"
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "name", "age", "score"])
        # Keep similar null rate in 'age' as csv_a (1 null out of 3)
        w.writerows([[4, "Dave", 40, 6.0], [5, "Eve", None, 8.5], [6, "Frank", 28, 7.0]])
    return str(path)


@pytest.fixture
def csv_b_additive(tmp_path):
    """csv_a schema + new 'city' column."""
    path = tmp_path / "b_additive.csv"
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "name", "age", "score", "city"])
        w.writerows([[1, "Alice", 30, 9.5, "LA"], [2, "Bob", 25, 7.0, "NY"]])
    return str(path)


@pytest.fixture
def csv_b_breaking(tmp_path):
    """csv_a schema minus 'score', 'age' type changed to string."""
    path = tmp_path / "b_breaking.csv"
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "name", "age"])
        w.writerows([[1, "Alice", "thirty"], [2, "Bob", "twenty-five"]])
    return str(path)


@pytest.fixture
def csv_b_nullability(tmp_path):
    """Same columns as csv_a but age is now heavily null."""
    path = tmp_path / "b_null.csv"
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "name", "age", "score"])
        for i in range(10):
            w.writerow([i, f"Person{i}", None if i < 8 else i + 20, 5.0])
    return str(path)


def _index_two(tmp_path, path_a, path_b, name_a="ds_a", name_b="ds_b"):
    storage = str(tmp_path / "store")
    index_local(path=path_a, name=name_a, storage_path=storage)
    index_local(path=path_b, name=name_b, storage_path=storage)
    return storage


# --- Error cases ---

def test_missing_dataset_a(tmp_path, csv_b_identical):
    storage = str(tmp_path / "store")
    index_local(path=csv_b_identical, name="ds_b", storage_path=storage)
    r = get_schema_drift("nonexistent", "ds_b", storage_path=storage)
    assert "error" in r
    assert "NOT_INDEXED" in r["error"]


def test_missing_dataset_b(tmp_path, csv_a):
    storage = str(tmp_path / "store")
    index_local(path=csv_a, name="ds_a", storage_path=storage)
    r = get_schema_drift("ds_a", "nonexistent", storage_path=storage)
    assert "error" in r
    assert "NOT_INDEXED" in r["error"]


# --- Identical schema ---

def test_identical_schema(tmp_path, csv_a, csv_b_identical):
    storage = _index_two(tmp_path, csv_a, csv_b_identical)
    r = get_schema_drift("ds_a", "ds_b", storage_path=storage)
    assert "result" in r
    res = r["result"]
    assert res["assessment"] == "identical"
    assert res["added_columns"] == []
    assert res["removed_columns"] == []
    assert res["type_changes"] == []
    assert res["total_changes"] == 0


# --- Additive schema ---

def test_additive_schema(tmp_path, csv_a, csv_b_additive):
    storage = _index_two(tmp_path, csv_a, csv_b_additive)
    r = get_schema_drift("ds_a", "ds_b", storage_path=storage)
    res = r["result"]
    assert res["assessment"] == "additive"
    assert "city" in res["added_columns"]
    assert res["removed_columns"] == []
    assert res["type_changes"] == []
    assert res["total_changes"] >= 1


# --- Breaking schema ---

def test_breaking_schema_removed_column(tmp_path, csv_a, csv_b_breaking):
    storage = _index_two(tmp_path, csv_a, csv_b_breaking)
    r = get_schema_drift("ds_a", "ds_b", storage_path=storage)
    res = r["result"]
    assert res["assessment"] == "breaking"
    assert "score" in res["removed_columns"]


def test_breaking_schema_type_change(tmp_path, csv_a, csv_b_breaking):
    storage = _index_two(tmp_path, csv_a, csv_b_breaking)
    r = get_schema_drift("ds_a", "ds_b", storage_path=storage)
    res = r["result"]
    # 'age' changed from integer to string
    type_cols = [t["column"] for t in res["type_changes"]]
    assert "age" in type_cols


# --- Nullability changes ---

def test_nullability_change_detected(tmp_path, csv_a, csv_b_nullability):
    storage = _index_two(tmp_path, csv_a, csv_b_nullability)
    r = get_schema_drift("ds_a", "ds_b", storage_path=storage)
    res = r["result"]
    null_cols = [n["column"] for n in res["nullability_changes"]]
    assert "age" in null_cols


def test_nullability_delta_direction(tmp_path, csv_a, csv_b_nullability):
    storage = _index_two(tmp_path, csv_a, csv_b_nullability)
    r = get_schema_drift("ds_a", "ds_b", storage_path=storage)
    age_null = next(n for n in r["result"]["nullability_changes"] if n["column"] == "age")
    # b has more nulls than a
    assert age_null["delta"] > 0


# --- Meta ---

def test_meta_timing(tmp_path, csv_a, csv_b_identical):
    storage = _index_two(tmp_path, csv_a, csv_b_identical)
    r = get_schema_drift("ds_a", "ds_b", storage_path=storage)
    assert "_meta" in r
    assert r["_meta"]["timing_ms"] >= 0


def test_column_counts(tmp_path, csv_a, csv_b_additive):
    storage = _index_two(tmp_path, csv_a, csv_b_additive)
    r = get_schema_drift("ds_a", "ds_b", storage_path=storage)
    res = r["result"]
    assert res["columns_in_a"] == 4
    assert res["columns_in_b"] == 5


def test_same_dataset_both_sides(tmp_path, csv_a):
    storage = str(tmp_path / "store")
    index_local(path=csv_a, name="ds_a", storage_path=storage)
    r = get_schema_drift("ds_a", "ds_a", storage_path=storage)
    res = r["result"]
    assert res["assessment"] == "identical"
    assert res["total_changes"] == 0
