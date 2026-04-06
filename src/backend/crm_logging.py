"""Настройка логирования: уровень из переменной окружения LOG_LEVEL."""

from __future__ import annotations

import logging
import os
from pathlib import Path

from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_VALID_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})


def setup_logging(component: str | None = None) -> int:
    """
    Загружает .env из корня проекта и настраивает root-логгер и логгеры uvicorn/fastapi.

    Возвращает числовой уровень logging (например logging.INFO).
    """
    load_dotenv(_PROJECT_ROOT / ".env")
    raw = (os.getenv("LOG_LEVEL") or "INFO").strip().upper()
    if raw not in _VALID_LEVELS:
        # базовая конфигурация ещё не применена — пишем в stderr
        import sys

        print(
            f"Предупреждение: некорректный LOG_LEVEL={raw!r}, используется INFO",
            file=sys.stderr,
        )
        raw = "INFO"
    level = getattr(logging, raw)
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(level=level, format=fmt, force=True)
    for name in ("uvicorn", "uvicorn.access", "uvicorn.error", "fastapi"):
        logging.getLogger(name).setLevel(level)
    log = logging.getLogger(component or "crm")
    log.debug("Логирование: уровень %s", raw)
    return level
