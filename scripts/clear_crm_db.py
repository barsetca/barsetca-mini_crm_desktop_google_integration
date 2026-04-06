#!/usr/bin/env python3
"""
Полная очистка базы CRM (SQLite): удаление всех строк и сброс AUTOINCREMENT.

Использование:
  python scripts/clear_crm_db.py
  python scripts/clear_crm_db.py /path/to/crm.sqlite3

При работающем API лучше вызывать POST /admin/clear-database (если включён CRM_ALLOW_ADMIN_ENDPOINTS).
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from backend.crm_admin import clear_crm_sqlite  # noqa: E402
from backend.crm_logging import setup_logging  # noqa: E402

DEFAULT_DB = ROOT / "data" / "crm.sqlite3"


def main() -> None:
    setup_logging("scripts.clear_crm_db")
    path = Path(sys.argv[1]).expanduser() if len(sys.argv) > 1 else DEFAULT_DB
    if not path.is_absolute():
        path = (ROOT / path).resolve()
    try:
        clear_crm_sqlite(path)
    except FileNotFoundError as e:
        print(f"Ошибка: {e}", file=sys.stderr)
        sys.exit(1)
    print(f"Очищено: {path}")


if __name__ == "__main__":
    main()
