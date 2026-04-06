from __future__ import annotations


def test_managers_crud(client) -> None:
    r = client.post("/managers", json={"full_name": "Иван Тестов", "email": "ivan@test.dev"})
    assert r.status_code == 200
    m = r.json()
    assert m["id"] == 1
    assert m["full_name"] == "Иван Тестов"

    r = client.get("/managers")
    assert r.status_code == 200
    assert len(r.json()) == 1

    r = client.get("/managers/1")
    assert r.status_code == 200
    assert r.json()["email"] == "ivan@test.dev"

    r = client.patch("/managers/1", json={"phone": "+79990001122"})
    assert r.status_code == 200
    assert r.json()["phone"] == "+79990001122"

    r = client.delete("/managers/1")
    assert r.status_code == 200

    r = client.get("/managers/1")
    assert r.status_code == 404


def test_manager_not_found(client) -> None:
    assert client.get("/managers/999").status_code == 404
