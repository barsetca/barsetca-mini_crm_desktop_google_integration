"""
Операции администрирования БД CRM: полная очистка и заполнение тестовыми данными.

Используются HTTP-эндпоинтами (при CRM_ALLOW_ADMIN_ENDPOINTS) и скриптами в scripts/.
"""

from __future__ import annotations

import random
import sqlite3
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict

TABLES_ORDER = ("tasks", "orders", "deals", "clients", "managers")

CLIENT_STATUSES = (
    "ACTIVE",
    "ACTIVE",
    "ACTIVE",
    "ACTIVE",
    "ACTIVE",
    "ACTIVE",
    "ACTIVE",
    "PENDING",
    "ACTIVE",
    "ARCHIVED",
)
DEAL_STATUSES = ("NEW", "NEW", "QUALIFICATION", "PROPOSAL", "NEGOTIATION", "WON", "LOST", "ON_HOLD")
ORDER_STATUSES = ("NEW", "CONFIRMED", "PAID", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED")


def _money() -> float:
    return round(random.uniform(100.0, 99_999.99), 2)


def _money_small() -> float:
    return round(random.uniform(50.0, 9_999.99), 2)


def clear_crm_sqlite(db_path: Path) -> None:
    """Удаляет все строки из таблиц CRM и сбрасывает sqlite_sequence."""
    if not db_path.is_file():
        raise FileNotFoundError(f"Файл БД не найден: {db_path}")
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys = OFF;")
        for name in TABLES_ORDER:
            conn.execute(f"DELETE FROM {name};")
        conn.execute(
            """
            DELETE FROM sqlite_sequence
            WHERE name IN ('tasks', 'orders', 'deals', 'clients', 'managers');
            """
        )
        conn.commit()
        conn.execute("PRAGMA foreign_keys = ON;")
    finally:
        conn.close()


def seed_crm_sqlite(db_path: Path, *, seed: int = 42) -> Dict[str, Any]:
    """
    Создаёт схему при необходимости и заполняет: 5 менеджеров, 10 клиентов,
    20 сделок, 50 заказов, 100 задач. Возвращает сводку для API.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    random.seed(seed)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS managers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                full_name TEXT NOT NULL,
                email TEXT,
                phone TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS clients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                full_name TEXT NOT NULL,
                email TEXT,
                phone TEXT,
                company_name TEXT,
                notes TEXT,
                status TEXT NOT NULL DEFAULT 'ACTIVE',
                manager_id INTEGER,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(manager_id) REFERENCES managers(id) ON DELETE SET NULL
            );
            CREATE TABLE IF NOT EXISTS deals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                description TEXT,
                amount REAL,
                status TEXT NOT NULL DEFAULT 'NEW',
                client_id INTEGER,
                manager_id INTEGER,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(client_id) REFERENCES clients(id) ON DELETE SET NULL,
                FOREIGN KEY(manager_id) REFERENCES managers(id) ON DELETE SET NULL
            );
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                deal_id INTEGER,
                client_id INTEGER,
                manager_id INTEGER,
                order_number TEXT,
                total_amount REAL,
                status TEXT NOT NULL DEFAULT 'NEW',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(deal_id) REFERENCES deals(id) ON DELETE SET NULL,
                FOREIGN KEY(client_id) REFERENCES clients(id) ON DELETE SET NULL,
                FOREIGN KEY(manager_id) REFERENCES managers(id) ON DELETE SET NULL
            );
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                description TEXT,
                due_date TEXT,
                is_done INTEGER NOT NULL DEFAULT 0,
                client_id INTEGER,
                deal_id INTEGER,
                manager_id INTEGER,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(client_id) REFERENCES clients(id) ON DELETE SET NULL,
                FOREIGN KEY(deal_id) REFERENCES deals(id) ON DELETE SET NULL,
                FOREIGN KEY(manager_id) REFERENCES managers(id) ON DELETE SET NULL
            );
            """
        )
        conn.execute("PRAGMA foreign_keys = ON;")

        for i in range(1, 6):
            conn.execute(
                """
                INSERT INTO managers (full_name, email, phone)
                VALUES (?, ?, ?)
                """,
                (f"Менеджер Тест {i}", f"manager{i}@example.test", f"+7900{i:07d}"),
            )

        for i in range(1, 11):
            mid = random.randint(1, 5)
            conn.execute(
                """
                INSERT INTO clients (
                    full_name, email, phone, company_name, notes, status, manager_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"Клиент Тестовый {i}",
                    f"client{i}@mail.test",
                    f"+7911{i:07d}",
                    f"ООО «Тест-{i}»",
                    f"Заметка по клиенту #{i}",
                    CLIENT_STATUSES[i - 1],
                    mid,
                ),
            )

        for i in range(1, 21):
            cid = None if random.random() < 0.15 else random.randint(1, 10)
            conn.execute(
                """
                INSERT INTO deals (title, description, amount, status, client_id, manager_id)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    f"Сделка демо #{i:02d}",
                    f"Описание сделки {i}; сумма договорная.",
                    _money(),
                    random.choice(DEAL_STATUSES),
                    cid,
                    random.randint(1, 5),
                ),
            )

        for i in range(1, 51):
            did = None if random.random() < 0.2 else random.randint(1, 20)
            cid = random.randint(1, 10)
            conn.execute(
                """
                INSERT INTO orders (
                    deal_id, client_id, manager_id, order_number, total_amount, status
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    did,
                    cid,
                    random.randint(1, 5),
                    f"ORD-2026-{i:04d}",
                    _money_small(),
                    random.choice(ORDER_STATUSES),
                ),
            )

        base = date(2026, 1, 1)
        for i in range(1, 101):
            d = base + timedelta(days=random.randint(0, 120))
            link_roll = random.random()
            client_id = random.randint(1, 10) if link_roll < 0.85 else None
            deal_id = random.randint(1, 20) if 0.3 < link_roll < 0.75 else None
            conn.execute(
                """
                INSERT INTO tasks (
                    title, description, due_date, is_done,
                    client_id, deal_id, manager_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"Напоминание / уведомление #{i}",
                    f"Текст задачи #{i}: перезвонить, выслать КП, уточнить оплату.",
                    d.isoformat(),
                    1 if random.random() < 0.35 else 0,
                    client_id,
                    deal_id,
                    random.randint(1, 5),
                ),
            )

        conn.commit()
        return {
            "db_path": str(db_path),
            "managers": 5,
            "clients": 10,
            "deals": 20,
            "orders": 50,
            "tasks": 100,
        }
    finally:
        conn.close()
