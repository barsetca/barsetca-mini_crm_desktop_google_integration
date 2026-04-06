"""Общие фикстуры: API-клиент с временной SQLite."""

from __future__ import annotations

import importlib
from collections.abc import Generator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Generator[TestClient, None, None]:
    """
    Перезагружает crm_api с CRM_DB_PATH на временный файл — изоляция от data/ разработчика.
    """
    monkeypatch.setenv("CRM_DB_PATH", str(tmp_path / "test_crm.sqlite3"))
    monkeypatch.setenv("CRM_ALLOW_ADMIN_ENDPOINTS", "false")
    monkeypatch.setenv("LOG_LEVEL", "WARNING")

    import src.backend.crm_api as crm_api

    importlib.reload(crm_api)

    with TestClient(crm_api.app) as tc:
        yield tc


@pytest.fixture
def client_with_admin(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Generator[TestClient, None, None]:
    monkeypatch.setenv("CRM_DB_PATH", str(tmp_path / "test_crm_admin.sqlite3"))
    monkeypatch.setenv("CRM_ALLOW_ADMIN_ENDPOINTS", "true")
    monkeypatch.setenv("LOG_LEVEL", "WARNING")

    import src.backend.crm_api as crm_api

    importlib.reload(crm_api)

    with TestClient(crm_api.app) as tc:
        yield tc
