from __future__ import annotations


def test_admin_endpoints_hidden_when_disabled(client) -> None:
    assert client.post("/admin/clear-database").status_code == 404
    assert client.post("/admin/seed-test-data").status_code == 404


def test_admin_clear_and_seed(client_with_admin) -> None:
    c = client_with_admin
    r = c.post("/admin/seed-test-data")
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    assert data["managers"] == 5

    r = c.get("/managers")
    assert len(r.json()) == 5

    r = c.post("/admin/clear-database")
    assert r.status_code == 200

    r = c.get("/managers")
    assert r.json() == []
