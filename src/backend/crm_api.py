from __future__ import annotations

import logging
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.exceptions import RequestValidationError
from fastapi.requests import Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict
from starlette.exceptions import HTTPException as StarletteHTTPException

from .crm_admin import clear_crm_sqlite, seed_crm_sqlite
from .crm_db import CRMDatabase
from .crm_logging import setup_logging

setup_logging("crm_api")
logger = logging.getLogger("crm_api")

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _resolve_db_path() -> Path:
    raw = (os.getenv("CRM_DB_PATH") or "").strip()
    if raw:
        p = Path(raw).expanduser()
        return p if p.is_absolute() else (PROJECT_ROOT / p).resolve()
    return (PROJECT_ROOT / "data" / "crm.sqlite3").resolve()


DB_PATH = _resolve_db_path()
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
db = CRMDatabase(db_path=str(DB_PATH))
logger.info("CRM БД: %s", DB_PATH)

app = FastAPI(title="Mini CRM API", version="0.1.0")


def _admin_endpoints_enabled() -> bool:
    return os.getenv("CRM_ALLOW_ADMIN_ENDPOINTS", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.perf_counter()
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("--> %s %s", request.method, request.url.path)
    try:
        response = await call_next(request)
    except Exception:
        logger.exception(
            "Необработанное исключение при %s %s",
            request.method,
            request.url.path,
        )
        raise
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            "<-- %s %s %s %.1f ms",
            request.method,
            request.url.path,
            response.status_code,
            (time.perf_counter() - start) * 1000,
        )
    return response


@app.exception_handler(sqlite3.IntegrityError)
async def sqlite_integrity_handler(_request: Request, exc: sqlite3.IntegrityError):
    # FK/UNIQUE and similar database constraints should return 400, not 500.
    logger.warning("IntegrityError: %s", exc)
    return JSONResponse(
        status_code=400,
        content={"detail": f"Ошибка целостности данных: {exc}"},
    )


@app.exception_handler(sqlite3.OperationalError)
async def sqlite_operational_handler(_request: Request, exc: sqlite3.OperationalError):
    logger.error("SQLite OperationalError: %s", exc, exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": f"Ошибка доступа к базе данных: {exc}"},
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    if isinstance(exc, StarletteHTTPException):
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail},
        )
    if isinstance(exc, RequestValidationError):
        return JSONResponse(status_code=422, content={"detail": exc.errors()})
    logger.exception("Необработанная ошибка %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Внутренняя ошибка сервера. Подробности в журнале (LOG_LEVEL=DEBUG).",
            "error_type": type(exc).__name__,
        },
    )


class APIModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ManagerCreate(APIModel):
    full_name: str
    email: Optional[str] = None
    phone: Optional[str] = None


class ManagerUpdate(APIModel):
    full_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None


class ClientCreate(APIModel):
    full_name: str
    email: Optional[str] = None
    phone: Optional[str] = None
    company_name: Optional[str] = None
    notes: Optional[str] = None
    status: str = "ACTIVE"
    manager_id: Optional[int] = None


class ClientUpdate(APIModel):
    full_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    company_name: Optional[str] = None
    notes: Optional[str] = None
    status: Optional[str] = None
    manager_id: Optional[int] = None


class DealCreate(APIModel):
    title: str
    description: Optional[str] = None
    amount: Optional[float] = None
    status: str = "NEW"
    client_id: Optional[int] = None
    manager_id: Optional[int] = None


class DealUpdate(APIModel):
    title: Optional[str] = None
    description: Optional[str] = None
    amount: Optional[float] = None
    status: Optional[str] = None
    client_id: Optional[int] = None
    manager_id: Optional[int] = None


class OrderCreate(APIModel):
    deal_id: Optional[int] = None
    client_id: Optional[int] = None
    manager_id: Optional[int] = None
    order_number: Optional[str] = None
    total_amount: Optional[float] = None
    status: str = "NEW"


class OrderUpdate(APIModel):
    deal_id: Optional[int] = None
    client_id: Optional[int] = None
    manager_id: Optional[int] = None
    order_number: Optional[str] = None
    total_amount: Optional[float] = None
    status: Optional[str] = None


class TaskCreate(APIModel):
    title: str
    description: Optional[str] = None
    due_date: Optional[str] = None
    is_done: bool = False
    client_id: Optional[int] = None
    deal_id: Optional[int] = None
    manager_id: Optional[int] = None


class TaskUpdate(APIModel):
    title: Optional[str] = None
    description: Optional[str] = None
    due_date: Optional[str] = None
    is_done: Optional[bool] = None
    client_id: Optional[int] = None
    deal_id: Optional[int] = None
    manager_id: Optional[int] = None


class TaskDoneUpdate(APIModel):
    is_done: bool


def _or_404(data: Any, entity: str):
    if not data:
        raise HTTPException(status_code=404, detail=f"{entity} not found")
    return data


@app.get("/health")
def health():
    return {"status": "ok", "database": str(DB_PATH)}


@app.post("/admin/clear-database")
def admin_clear_database():
    """
    Удаляет все строки в таблицах CRM (опасная операция).
    Включите CRM_ALLOW_ADMIN_ENDPOINTS=1 в .env только для разработки/Docker.
    """
    if not _admin_endpoints_enabled():
        raise HTTPException(status_code=404, detail="Not found")
    try:
        clear_crm_sqlite(DB_PATH)
    except FileNotFoundError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except OSError as e:
        logger.error("admin clear-database: %s", e, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Не удалось очистить базу: {e}",
        ) from e
    logger.warning("Админ: база данных очищена (%s)", DB_PATH)
    return {"ok": True, "message": f"База очищена: {DB_PATH}"}


@app.post("/admin/seed-test-data")
def admin_seed_test_data():
    """
    Заполняет БД фиксированным набором тестовых данных (см. crm_admin.seed_crm_sqlite).
    Обычно перед этим вызывают /admin/clear-database, иначе данные дописываются к существующим.
    """
    if not _admin_endpoints_enabled():
        raise HTTPException(status_code=404, detail="Not found")
    try:
        summary = seed_crm_sqlite(DB_PATH)
    except sqlite3.Error as e:
        logger.exception("admin seed-test-data")
        raise HTTPException(
            status_code=500,
            detail=f"Ошибка SQLite при заполнении базы: {e}",
        ) from e
    except OSError as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    logger.warning("Админ: загружены тестовые данные (%s)", DB_PATH)
    return {"ok": True, **summary}


# Managers
@app.post("/managers")
def create_manager(payload: ManagerCreate):
    return db.create_manager(payload.full_name, payload.email, payload.phone)


@app.get("/managers")
def list_managers():
    return db.list_managers()


@app.get("/managers/{manager_id}")
def get_manager(manager_id: int):
    return _or_404(db.get_manager(manager_id), "manager")


@app.patch("/managers/{manager_id}")
def update_manager(manager_id: int, payload: ManagerUpdate):
    return _or_404(
        db.update_manager(
            manager_id,
            payload.full_name,
            payload.email,
            payload.phone,
        ),
        "manager",
    )


@app.delete("/managers/{manager_id}")
def delete_manager(manager_id: int):
    _or_404(db.delete_manager(manager_id), "manager")
    return {"deleted": True}


# Clients
@app.post("/clients")
def create_client(payload: ClientCreate):
    return db.create_client(
        payload.full_name,
        payload.email,
        payload.phone,
        payload.company_name,
        payload.notes,
        payload.status,
        payload.manager_id,
    )


@app.get("/clients")
def list_clients(include_archived: bool = Query(default=False)):
    return db.list_clients(include_archived=include_archived)


@app.get("/clients/{client_id}")
def get_client(client_id: int):
    return _or_404(db.get_client(client_id), "client")


@app.patch("/clients/{client_id}")
def update_client(client_id: int, payload: ClientUpdate):
    return _or_404(
        db.update_client(
            client_id,
            payload.full_name,
            payload.email,
            payload.phone,
            payload.company_name,
            payload.notes,
            payload.status,
            payload.manager_id,
        ),
        "client",
    )


@app.post("/clients/{client_id}/archive")
def archive_client(client_id: int):
    return _or_404(db.archive_client(client_id), "client")


@app.delete("/clients/{client_id}")
def delete_client(client_id: int):
    _or_404(db.delete_client(client_id), "client")
    return {"deleted": True}


@app.get("/clients/search/by-text")
def search_clients(q: str, include_archived: bool = Query(default=False)):
    return db.search_clients(q, include_archived=include_archived)


# Deals
@app.post("/deals")
def create_deal(payload: DealCreate):
    return db.create_deal(
        payload.title,
        payload.description,
        payload.amount,
        payload.status,
        payload.client_id,
        payload.manager_id,
    )


@app.get("/deals")
def list_deals():
    return db.list_deals()


@app.get("/deals/{deal_id}")
def get_deal(deal_id: int):
    return _or_404(db.get_deal(deal_id), "deal")


@app.patch("/deals/{deal_id}")
def update_deal(deal_id: int, payload: DealUpdate):
    return _or_404(
        db.update_deal(
            deal_id,
            payload.title,
            payload.description,
            payload.amount,
            payload.status,
            payload.client_id,
            payload.manager_id,
        ),
        "deal",
    )


@app.delete("/deals/{deal_id}")
def delete_deal(deal_id: int):
    _or_404(db.delete_deal(deal_id), "deal")
    return {"deleted": True}


@app.get("/deals/search/by-text")
def search_deals(q: str):
    return db.search_deals(q)


# Orders
@app.post("/orders")
def create_order(payload: OrderCreate):
    return db.create_order(
        payload.deal_id,
        payload.client_id,
        payload.manager_id,
        payload.order_number,
        payload.total_amount,
        payload.status,
    )


@app.get("/orders")
def list_orders():
    return db.list_orders()


@app.get("/orders/{order_id}")
def get_order(order_id: int):
    return _or_404(db.get_order(order_id), "order")


@app.patch("/orders/{order_id}")
def update_order(order_id: int, payload: OrderUpdate):
    return _or_404(
        db.update_order(
            order_id,
            payload.deal_id,
            payload.client_id,
            payload.manager_id,
            payload.order_number,
            payload.total_amount,
            payload.status,
        ),
        "order",
    )


@app.delete("/orders/{order_id}")
def delete_order(order_id: int):
    _or_404(db.delete_order(order_id), "order")
    return {"deleted": True}


# Tasks
@app.post("/tasks")
def create_task(payload: TaskCreate):
    return db.create_task(
        payload.title,
        payload.description,
        payload.due_date,
        payload.is_done,
        payload.client_id,
        payload.deal_id,
        payload.manager_id,
    )


@app.get("/tasks")
def list_tasks():
    return db.list_tasks()


@app.get("/tasks/{task_id}")
def get_task(task_id: int):
    return _or_404(db.get_task(task_id), "task")


@app.patch("/tasks/{task_id}")
def update_task(task_id: int, payload: TaskUpdate):
    return _or_404(
        db.update_task(
            task_id,
            payload.title,
            payload.description,
            payload.due_date,
            payload.is_done,
            payload.client_id,
            payload.deal_id,
            payload.manager_id,
        ),
        "task",
    )


@app.post("/tasks/{task_id}/done")
def set_task_done(task_id: int, payload: TaskDoneUpdate):
    return _or_404(db.set_task_done(task_id, payload.is_done), "task")


@app.delete("/tasks/{task_id}")
def delete_task(task_id: int):
    _or_404(db.delete_task(task_id), "task")
    return {"deleted": True}
