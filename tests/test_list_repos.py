"""Tests for list_repos tool."""

import pytest
from pathlib import Path

from jdatamunch_mcp.tools.list_repos import list_repos
from jdatamunch_mcp.tools.index_local import index_local


@pytest.fixture
def repo_storage(sample_csv, storage_dir):
    """Simulate an index_repo result: marker file + datasets with repo prefix."""
    # Create marker file
    marker = Path(storage_dir) / ".repo-sha-acme--widgets"
    marker.write_text("abc123def456789")

    # Index two datasets with repo-style naming
    index_local(path=sample_csv, name="acme--widgets--sales", storage_path=storage_dir)
    index_local(path=sample_csv, name="acme--widgets--inventory", storage_path=storage_dir)
    return storage_dir


@pytest.fixture
def multi_repo_storage(sample_csv, storage_dir):
    """Two repos indexed."""
    Path(storage_dir, ".repo-sha-acme--widgets").write_text("sha1111")
    Path(storage_dir, ".repo-sha-bob--data").write_text("sha2222")

    index_local(path=sample_csv, name="acme--widgets--sales", storage_path=storage_dir)
    index_local(path=sample_csv, name="bob--data--records", storage_path=storage_dir)
    return storage_dir


class TestListRepos:

    def test_empty_storage(self, storage_dir):
        result = list_repos(storage_path=storage_dir)
        assert result["result"] == []

    def test_no_markers(self, sample_csv, storage_dir):
        """Datasets exist but no repo markers → empty list."""
        index_local(path=sample_csv, name="local-dataset", storage_path=storage_dir)
        result = list_repos(storage_path=storage_dir)
        assert result["result"] == []

    def test_single_repo(self, repo_storage):
        result = list_repos(storage_path=repo_storage)
        assert len(result["result"]) == 1
        repo = result["result"][0]
        assert repo["repo"] == "acme/widgets"
        assert repo["datasets"] == 2
        assert repo["total_rows"] == 20  # 10 rows x 2 datasets
        assert "acme--widgets--sales" in repo["dataset_names"]
        assert "acme--widgets--inventory" in repo["dataset_names"]

    def test_head_sha_truncated(self, repo_storage):
        result = list_repos(storage_path=repo_storage)
        repo = result["result"][0]
        assert repo["head_sha"] == "abc123def456"
        assert len(repo["head_sha"]) == 12

    def test_multiple_repos(self, multi_repo_storage):
        result = list_repos(storage_path=multi_repo_storage)
        assert len(result["result"]) == 2
        names = {r["repo"] for r in result["result"]}
        assert names == {"acme/widgets", "bob/data"}

    def test_repo_with_deleted_datasets(self, repo_storage):
        """Marker exists but datasets were deleted → 0 datasets."""
        from jdatamunch_mcp.tools.delete_dataset import delete_dataset
        delete_dataset(dataset="acme--widgets--sales", storage_path=repo_storage)
        delete_dataset(dataset="acme--widgets--inventory", storage_path=repo_storage)

        result = list_repos(storage_path=repo_storage)
        assert len(result["result"]) == 1
        repo = result["result"][0]
        assert repo["datasets"] == 0
        assert repo["total_rows"] == 0

    def test_meta_present(self, repo_storage):
        result = list_repos(storage_path=repo_storage)
        assert "_meta" in result
        assert "timing_ms" in result["_meta"]

    def test_total_size_bytes(self, repo_storage):
        result = list_repos(storage_path=repo_storage)
        repo = result["result"][0]
        assert repo["total_size_bytes"] > 0
