"""
SQLite ядро мини-CRM.

Содержит:
- создание схемы БД при инициализации;
- CRUD для клиентов, менеджеров, сделок, заказов, задач;
- архивирование клиента;
- поиск по клиентам и сделкам через case-insensitive LIKE (аналог ILIKE).
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class CRMDatabase:
    def __init__(self, db_path: str = "crm.sqlite3") -> None:
        self.db_path = str(Path(db_path))
        logger.info("Подключение к SQLite: %s", self.db_path)
        try:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        except sqlite3.Error as e:
            logger.error("Не удалось открыть БД %s: %s", self.db_path, e)
            raise
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON;")
        self._create_tables()
        logger.debug("Схема CRM проверена/создана")

    def close(self) -> None:
        self.conn.close()

    def _create_tables(self) -> None:
        self.conn.executescript(
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
        self.conn.commit()

    def _fetchone(self, query: str, params: tuple[Any, ...] = ()) -> Optional[Dict[str, Any]]:
        cur = self.conn.execute(query, params)
        row = cur.fetchone()
        return dict(row) if row else None

    def _fetchall(self, query: str, params: tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
        cur = self.conn.execute(query, params)
        return [dict(r) for r in cur.fetchall()]

    # ---------------- Managers ----------------
    def create_manager(self, full_name: str, email: str | None, phone: str | None) -> Dict[str, Any]:
        cur = self.conn.execute(
            """
            INSERT INTO managers(full_name, email, phone)
            VALUES (?, ?, ?)
            """,
            (full_name, email, phone),
        )
        self.conn.commit()
        return self.get_manager(cur.lastrowid)

    def get_manager(self, manager_id: int) -> Optional[Dict[str, Any]]:
        return self._fetchone("SELECT * FROM managers WHERE id = ?", (manager_id,))

    def list_managers(self) -> List[Dict[str, Any]]:
        return self._fetchall("SELECT * FROM managers ORDER BY id DESC")

    def update_manager(
        self,
        manager_id: int,
        full_name: str | None = None,
        email: str | None = None,
        phone: str | None = None,
    ) -> Optional[Dict[str, Any]]:
        current = self.get_manager(manager_id)
        if not current:
            return None
        self.conn.execute(
            """
            UPDATE managers
            SET full_name = ?, email = ?, phone = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                full_name if full_name is not None else current["full_name"],
                email if email is not None else current["email"],
                phone if phone is not None else current["phone"],
                manager_id,
            ),
        )
        self.conn.commit()
        return self.get_manager(manager_id)

    def delete_manager(self, manager_id: int) -> bool:
        cur = self.conn.execute("DELETE FROM managers WHERE id = ?", (manager_id,))
        self.conn.commit()
        return cur.rowcount > 0

    # ---------------- Clients ----------------
    def create_client(
        self,
        full_name: str,
        email: str | None = None,
        phone: str | None = None,
        company_name: str | None = None,
        notes: str | None = None,
        status: str = "ACTIVE",
        manager_id: int | None = None,
    ) -> Dict[str, Any]:
        cur = self.conn.execute(
            """
            INSERT INTO clients(full_name, email, phone, company_name, notes, status, manager_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (full_name, email, phone, company_name, notes, status, manager_id),
        )
        self.conn.commit()
        return self.get_client(cur.lastrowid)

    def get_client(self, client_id: int) -> Optional[Dict[str, Any]]:
        return self._fetchone("SELECT * FROM clients WHERE id = ?", (client_id,))

    def list_clients(self, include_archived: bool = False) -> List[Dict[str, Any]]:
        if include_archived:
            return self._fetchall("SELECT * FROM clients ORDER BY id DESC")
        return self._fetchall(
            "SELECT * FROM clients WHERE status <> 'ARCHIVED' ORDER BY id DESC"
        )

    def update_client(
        self,
        client_id: int,
        full_name: str | None = None,
        email: str | None = None,
        phone: str | None = None,
        company_name: str | None = None,
        notes: str | None = None,
        status: str | None = None,
        manager_id: int | None = None,
    ) -> Optional[Dict[str, Any]]:
        current = self.get_client(client_id)
        if not current:
            return None
        self.conn.execute(
            """
            UPDATE clients
            SET full_name = ?, email = ?, phone = ?, company_name = ?, notes = ?,
                status = ?, manager_id = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                full_name if full_name is not None else current["full_name"],
                email if email is not None else current["email"],
                phone if phone is not None else current["phone"],
                company_name if company_name is not None else current["company_name"],
                notes if notes is not None else current["notes"],
                status if status is not None else current["status"],
                manager_id if manager_id is not None else current["manager_id"],
                client_id,
            ),
        )
        self.conn.commit()
        return self.get_client(client_id)

    def archive_client(self, client_id: int) -> Optional[Dict[str, Any]]:
        self.conn.execute(
            "UPDATE clients SET status = 'ARCHIVED', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (client_id,),
        )
        self.conn.commit()
        return self.get_client(client_id)

    def delete_client(self, client_id: int) -> bool:
        cur = self.conn.execute("DELETE FROM clients WHERE id = ?", (client_id,))
        self.conn.commit()
        return cur.rowcount > 0

    def search_clients(self, q: str, include_archived: bool = False) -> List[Dict[str, Any]]:
        pattern = f"%{q.strip()}%"
        sql = """
            SELECT * FROM clients
            WHERE (
                lower(full_name) LIKE lower(?)
                OR lower(COALESCE(email, '')) LIKE lower(?)
                OR lower(COALESCE(phone, '')) LIKE lower(?)
                OR lower(COALESCE(company_name, '')) LIKE lower(?)
            )
        """
        params: tuple[Any, ...] = (pattern, pattern, pattern, pattern)
        if not include_archived:
            sql += " AND status <> 'ARCHIVED'"
        sql += " ORDER BY id DESC"
        return self._fetchall(sql, params)

    # ---------------- Deals ----------------
    def create_deal(
        self,
        title: str,
        description: str | None = None,
        amount: float | None = None,
        status: str = "NEW",
        client_id: int | None = None,
        manager_id: int | None = None,
    ) -> Dict[str, Any]:
        cur = self.conn.execute(
            """
            INSERT INTO deals(title, description, amount, status, client_id, manager_id)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (title, description, amount, status, client_id, manager_id),
        )
        self.conn.commit()
        return self.get_deal(cur.lastrowid)

    def get_deal(self, deal_id: int) -> Optional[Dict[str, Any]]:
        return self._fetchone("SELECT * FROM deals WHERE id = ?", (deal_id,))

    def list_deals(self) -> List[Dict[str, Any]]:
        return self._fetchall("SELECT * FROM deals ORDER BY id DESC")

    def update_deal(
        self,
        deal_id: int,
        title: str | None = None,
        description: str | None = None,
        amount: float | None = None,
        status: str | None = None,
        client_id: int | None = None,
        manager_id: int | None = None,
    ) -> Optional[Dict[str, Any]]:
        current = self.get_deal(deal_id)
        if not current:
            return None
        self.conn.execute(
            """
            UPDATE deals
            SET title = ?, description = ?, amount = ?, status = ?, client_id = ?, manager_id = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                title if title is not None else current["title"],
                description if description is not None else current["description"],
                amount if amount is not None else current["amount"],
                status if status is not None else current["status"],
                client_id if client_id is not None else current["client_id"],
                manager_id if manager_id is not None else current["manager_id"],
                deal_id,
            ),
        )
        self.conn.commit()
        return self.get_deal(deal_id)

    def delete_deal(self, deal_id: int) -> bool:
        cur = self.conn.execute("DELETE FROM deals WHERE id = ?", (deal_id,))
        self.conn.commit()
        return cur.rowcount > 0

    def search_deals(self, q: str) -> List[Dict[str, Any]]:
        pattern = f"%{q.strip()}%"
        return self._fetchall(
            """
            SELECT * FROM deals
            WHERE lower(title) LIKE lower(?)
               OR lower(COALESCE(description, '')) LIKE lower(?)
               OR lower(COALESCE(status, '')) LIKE lower(?)
            ORDER BY id DESC
            """,
            (pattern, pattern, pattern),
        )

    # ---------------- Orders ----------------
    def create_order(
        self,
        deal_id: int | None = None,
        client_id: int | None = None,
        manager_id: int | None = None,
        order_number: str | None = None,
        total_amount: float | None = None,
        status: str = "NEW",
    ) -> Dict[str, Any]:
        cur = self.conn.execute(
            """
            INSERT INTO orders(deal_id, client_id, manager_id, order_number, total_amount, status)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (deal_id, client_id, manager_id, order_number, total_amount, status),
        )
        self.conn.commit()
        return self.get_order(cur.lastrowid)

    def get_order(self, order_id: int) -> Optional[Dict[str, Any]]:
        return self._fetchone("SELECT * FROM orders WHERE id = ?", (order_id,))

    def list_orders(self) -> List[Dict[str, Any]]:
        return self._fetchall("SELECT * FROM orders ORDER BY id DESC")

    def update_order(
        self,
        order_id: int,
        deal_id: int | None = None,
        client_id: int | None = None,
        manager_id: int | None = None,
        order_number: str | None = None,
        total_amount: float | None = None,
        status: str | None = None,
    ) -> Optional[Dict[str, Any]]:
        current = self.get_order(order_id)
        if not current:
            return None
        self.conn.execute(
            """
            UPDATE orders
            SET deal_id = ?, client_id = ?, manager_id = ?, order_number = ?, total_amount = ?, status = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                deal_id if deal_id is not None else current["deal_id"],
                client_id if client_id is not None else current["client_id"],
                manager_id if manager_id is not None else current["manager_id"],
                order_number if order_number is not None else current["order_number"],
                total_amount if total_amount is not None else current["total_amount"],
                status if status is not None else current["status"],
                order_id,
            ),
        )
        self.conn.commit()
        return self.get_order(order_id)

    def delete_order(self, order_id: int) -> bool:
        cur = self.conn.execute("DELETE FROM orders WHERE id = ?", (order_id,))
        self.conn.commit()
        return cur.rowcount > 0

    # ---------------- Tasks ----------------
    def create_task(
        self,
        title: str,
        description: str | None = None,
        due_date: str | None = None,
        is_done: bool = False,
        client_id: int | None = None,
        deal_id: int | None = None,
        manager_id: int | None = None,
    ) -> Dict[str, Any]:
        cur = self.conn.execute(
            """
            INSERT INTO tasks(title, description, due_date, is_done, client_id, deal_id, manager_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (title, description, due_date, int(is_done), client_id, deal_id, manager_id),
        )
        self.conn.commit()
        return self.get_task(cur.lastrowid)

    def get_task(self, task_id: int) -> Optional[Dict[str, Any]]:
        return self._fetchone("SELECT * FROM tasks WHERE id = ?", (task_id,))

    def list_tasks(self) -> List[Dict[str, Any]]:
        return self._fetchall("SELECT * FROM tasks ORDER BY id DESC")

    def update_task(
        self,
        task_id: int,
        title: str | None = None,
        description: str | None = None,
        due_date: str | None = None,
        is_done: bool | None = None,
        client_id: int | None = None,
        deal_id: int | None = None,
        manager_id: int | None = None,
    ) -> Optional[Dict[str, Any]]:
        current = self.get_task(task_id)
        if not current:
            return None
        self.conn.execute(
            """
            UPDATE tasks
            SET title = ?, description = ?, due_date = ?, is_done = ?, client_id = ?, deal_id = ?, manager_id = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                title if title is not None else current["title"],
                description if description is not None else current["description"],
                due_date if due_date is not None else current["due_date"],
                int(is_done) if is_done is not None else current["is_done"],
                client_id if client_id is not None else current["client_id"],
                deal_id if deal_id is not None else current["deal_id"],
                manager_id if manager_id is not None else current["manager_id"],
                task_id,
            ),
        )
        self.conn.commit()
        return self.get_task(task_id)

    def set_task_done(self, task_id: int, is_done: bool) -> Optional[Dict[str, Any]]:
        self.conn.execute(
            "UPDATE tasks SET is_done = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (int(is_done), task_id),
        )
        self.conn.commit()
        return self.get_task(task_id)

    def delete_task(self, task_id: int) -> bool:
        cur = self.conn.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
        self.conn.commit()
        return cur.rowcount > 0
