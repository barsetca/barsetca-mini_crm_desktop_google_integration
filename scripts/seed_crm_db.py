#!/usr/bin/env python3
"""
Заполнение базы CRM тестовыми данными.

Использование:
  python scripts/seed_crm_db.py
  python scripts/seed_crm_db.py /path/to/crm.sqlite3

При работающем API: POST /admin/seed-test-data (если включён CRM_ALLOW_ADMIN_ENDPOINTS).
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from backend.crm_admin import seed_crm_sqlite  # noqa: E402
from backend.crm_logging import setup_logging  # noqa: E402

DEFAULT_DB = ROOT / "data" / "crm.sqlite3"


def main() -> None:
    setup_logging("scripts.seed_crm_db")
    db_path = Path(sys.argv[1]).expanduser() if len(sys.argv) > 1 else DEFAULT_DB
    if not db_path.is_absolute():
        db_path = (ROOT / db_path).resolve()
    summary = seed_crm_sqlite(db_path)
    print(f"Заполнено: {summary['db_path']}")
    print(
        "Менеджеры: %(managers)s, клиенты: %(clients)s, сделки: %(deals)s, "
        "заказы: %(orders)s, задачи: %(tasks)s" % summary
    )


if __name__ == "__main__":
    main()
