"""Прямое тестирование crm_admin без HTTP."""

from __future__ import annotations

import sqlite3

import pytest

from src.backend.crm_admin import clear_crm_sqlite, seed_crm_sqlite


def test_clear_missing_file_raises(tmp_path) -> None:
    missing = tmp_path / "nope.sqlite3"
    with pytest.raises(FileNotFoundError):
        clear_crm_sqlite(missing)


def test_seed_and_clear_roundtrip(tmp_path) -> None:
    db = tmp_path / "r.sqlite3"
    summary = seed_crm_sqlite(db)
    assert summary["clients"] == 10

    conn = sqlite3.connect(str(db))
    try:
        n = conn.execute("SELECT COUNT(*) FROM managers").fetchone()[0]
        assert n == 5
    finally:
        conn.close()

    clear_crm_sqlite(db)
    conn = sqlite3.connect(str(db))
    try:
        assert conn.execute("SELECT COUNT(*) FROM managers").fetchone()[0] == 0
    finally:
        conn.close()
