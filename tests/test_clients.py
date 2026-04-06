from __future__ import annotations


def test_client_create_list_search(client) -> None:
    client.post("/managers", json={"full_name": "М1"})
    r = client.post(
        "/clients",
        json={"full_name": "Клиент А", "status": "ACTIVE", "manager_id": 1},
    )
    assert r.status_code == 200
    cid = r.json()["id"]

    r = client.get("/clients")
    assert r.status_code == 200
    assert len(r.json()) == 1

    r = client.get("/clients/search/by-text", params={"q": "Клиент"})
    assert r.status_code == 200
    assert len(r.json()) == 1
    assert r.json()[0]["id"] == cid


def test_client_invalid_manager_id_returns_400(client) -> None:
    r = client.post(
        "/clients",
        json={"full_name": "X", "manager_id": 9999},
    )
    assert r.status_code == 400
    assert "целостности" in r.json()["detail"]
