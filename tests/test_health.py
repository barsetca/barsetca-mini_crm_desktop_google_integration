from __future__ import annotations


def test_health(client) -> None:
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert "database" in body
    assert str(body["database"]).endswith("test_crm.sqlite3")
