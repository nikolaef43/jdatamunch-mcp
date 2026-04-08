"""SQLite-backed storage for column embeddings.

The column_embeddings table lives in the dataset's data.sqlite file.
Embeddings are stored as float32 BLOBs via the stdlib ``array`` module —
no numpy required.
"""

import array
import logging
import sqlite3
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_SCHEMA = """\
CREATE TABLE IF NOT EXISTS column_embeddings (
    column_name TEXT PRIMARY KEY,
    embedding   BLOB NOT NULL
);
"""

_META_SCHEMA = """\
CREATE TABLE IF NOT EXISTS embed_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

_DIM_KEY = "embed_dimension"
_MODEL_KEY = "embed_model"


def _encode(vec: list[float]) -> bytes:
    return array.array("f", vec).tobytes()


def _decode(data: bytes) -> list[float]:
    a: array.array = array.array("f")
    a.frombytes(data)
    return list(a)


class ColumnEmbeddingStore:
    """CRUD wrapper for column embeddings in a dataset's SQLite file."""

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self._db_path))
        conn.execute("PRAGMA journal_mode = WAL")
        conn.executescript(_SCHEMA)
        conn.executescript(_META_SCHEMA)
        return conn

    # ── Meta ──────────────────────────────────────────────────────────────

    def get_dimension(self) -> Optional[int]:
        try:
            conn = self._connect()
            try:
                row = conn.execute(
                    "SELECT value FROM embed_meta WHERE key = ?", (_DIM_KEY,)
                ).fetchone()
                return int(row[0]) if row else None
            finally:
                conn.close()
        except Exception:
            logger.debug("ColumnEmbeddingStore.get_dimension failed", exc_info=True)
            return None

    def get_model(self) -> Optional[str]:
        try:
            conn = self._connect()
            try:
                row = conn.execute(
                    "SELECT value FROM embed_meta WHERE key = ?", (_MODEL_KEY,)
                ).fetchone()
                return row[0] if row else None
            finally:
                conn.close()
        except Exception:
            return None

    def set_meta(self, dim: int, model: str) -> None:
        conn = self._connect()
        try:
            conn.execute(
                "INSERT OR REPLACE INTO embed_meta (key, value) VALUES (?, ?)",
                (_DIM_KEY, str(dim)),
            )
            conn.execute(
                "INSERT OR REPLACE INTO embed_meta (key, value) VALUES (?, ?)",
                (_MODEL_KEY, model),
            )
            conn.commit()
        finally:
            conn.close()

    # ── Read ──────────────────────────────────────────────────────────────

    def get(self, column_name: str) -> Optional[list[float]]:
        try:
            conn = self._connect()
            try:
                row = conn.execute(
                    "SELECT embedding FROM column_embeddings WHERE column_name = ?",
                    (column_name,),
                ).fetchone()
                return _decode(row[0]) if row else None
            finally:
                conn.close()
        except Exception:
            logger.debug("ColumnEmbeddingStore.get failed for %s", column_name, exc_info=True)
            return None

    def get_all(self) -> dict[str, list[float]]:
        try:
            conn = self._connect()
            try:
                rows = conn.execute(
                    "SELECT column_name, embedding FROM column_embeddings"
                ).fetchall()
                return {row[0]: _decode(row[1]) for row in rows}
            finally:
                conn.close()
        except Exception:
            logger.debug("ColumnEmbeddingStore.get_all failed", exc_info=True)
            return {}

    def count(self) -> int:
        try:
            conn = self._connect()
            try:
                row = conn.execute(
                    "SELECT COUNT(*) FROM column_embeddings"
                ).fetchone()
                return int(row[0]) if row else 0
            finally:
                conn.close()
        except Exception:
            return 0

    # ── Write ─────────────────────────────────────────────────────────────

    def set_many(self, embeddings: dict[str, list[float]]) -> None:
        if not embeddings:
            return
        conn = self._connect()
        try:
            conn.executemany(
                "INSERT OR REPLACE INTO column_embeddings (column_name, embedding) VALUES (?, ?)",
                [(name, _encode(vec)) for name, vec in embeddings.items()],
            )
            conn.commit()
        finally:
            conn.close()

    def clear(self) -> None:
        conn = self._connect()
        try:
            conn.execute("DELETE FROM column_embeddings")
            conn.commit()
        finally:
            conn.close()
