from __future__ import annotations

import json
import logging
import os
import re
import sys
import threading
import tkinter as tk
import webbrowser
from pathlib import Path
from tkinter import messagebox, scrolledtext, ttk
from typing import Any, Dict, Iterable, Optional

import requests
from dotenv import dotenv_values, load_dotenv

logger = logging.getLogger(__name__)

API_BASE = "http://127.0.0.1:8000"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_PATH = PROJECT_ROOT / ".env"


def merge_env_file(path: Path, updates: Dict[str, str]) -> None:
    """Обновляет или добавляет ключи в .env, остальные строки сохраняет."""
    lines: list[str] = []
    if path.exists():
        lines = path.read_text(encoding="utf-8").splitlines()
    seen: set[str] = set()
    out: list[str] = []
    key_re = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)=(.*)$")
    for line in lines:
        m = key_re.match(line.strip())
        if m:
            k = m.group(1)
            if k in updates:
                out.append(f"{k}={updates[k]}")
                seen.add(k)
                continue
        out.append(line)
    for k, v in updates.items():
        if k not in seen:
            out.append(f"{k}={v}")
    path.write_text("\n".join(out).rstrip() + "\n", encoding="utf-8")


class APIClient:
    def __init__(self, base_url: str = API_BASE) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.timeout = 10

    @staticmethod
    def _format_error_body(resp: requests.Response) -> str:
        raw = (resp.text or "").strip()
        try:
            data = resp.json()
        except (json.JSONDecodeError, ValueError):
            return (raw[:800] if raw else "") or (resp.reason or f"HTTP {resp.status_code}")

        if isinstance(data, dict) and "detail" in data:
            d = data["detail"]
            if isinstance(d, list):
                parts = []
                for item in d:
                    if isinstance(item, dict):
                        loc = item.get("loc", [])
                        msg = item.get("msg", item)
                        parts.append(f"{'/'.join(str(x) for x in loc)}: {msg}")
                    else:
                        parts.append(str(item))
                return "; ".join(parts) if parts else str(data)
            return str(d)
        return raw[:800] if raw else str(data)

    def _request(self, method: str, path: str, **kwargs) -> Any:
        url = f"{self.base_url}{path}"
        try:
            resp = self.session.request(method, url, timeout=self.timeout, **kwargs)
            if resp.status_code >= 400:
                detail = self._format_error_body(resp)
                msg = f"{method} {path}: HTTP {resp.status_code}. {detail}"
                logger.warning(msg)
                raise RuntimeError(msg)
            if resp.content:
                return resp.json()
            return None
        except requests.Timeout as e:
            msg = (
                f"Превышено время ожидания ({self.timeout} с) для {url}. "
                "Проверьте, что API запущен и адрес в поле «Backend URL» верный."
            )
            logger.error("%s (%s)", msg, e)
            raise RuntimeError(msg) from e
        except requests.ConnectionError as e:
            msg = (
                f"Не удалось подключиться к {self.base_url}. "
                "Запустите сервер (например: uvicorn src.backend.crm_api:app) или укажите URL контейнера Docker."
            )
            logger.error("%s: %s", msg, e)
            raise RuntimeError(msg) from e
        except requests.RequestException as e:
            msg = f"Ошибка сети при {method} {path}: {e}"
            logger.error(msg)
            raise RuntimeError(msg) from e

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request("GET", path, params=params)

    def post(self, path: str, payload: Dict[str, Any]) -> Any:
        return self._request("POST", path, json=payload)

    def patch(self, path: str, payload: Dict[str, Any]) -> Any:
        return self._request("PATCH", path, json=payload)

    def delete(self, path: str) -> Any:
        return self._request("DELETE", path)


class BaseTab(ttk.Frame):
    def __init__(
        self,
        parent,
        api: APIClient,
        status_var: tk.StringVar,
        crm_app: Optional["CRMApp"] = None,
    ) -> None:
        super().__init__(parent, padding=8)
        self.api = api
        self.status_var = status_var
        self.crm_app = crm_app

    def set_status(self, text: str) -> None:
        self.status_var.set(text)

    def safe_call(self, fn, success_message: Optional[str] = None):
        try:
            result = fn()
            if success_message:
                self.set_status(success_message)
            return result
        except Exception as e:
            self.set_status("Ошибка выполнения запроса")
            logger.error("Сбой операции: %s", e, exc_info=logger.isEnabledFor(logging.DEBUG))
            messagebox.showerror("Ошибка", str(e))
            return None

    @staticmethod
    def selected_id(tree: ttk.Treeview) -> Optional[int]:
        sel = tree.selection()
        if not sel:
            return None
        values = tree.item(sel[0], "values")
        if not values:
            return None
        return int(values[0])

    @staticmethod
    def fill_tree(tree: ttk.Treeview, rows: Iterable[Dict[str, Any]], columns: list[str]) -> None:
        tree.delete(*tree.get_children())
        for row in rows:
            tree.insert("", tk.END, values=[row.get(c, "") for c in columns])

    def _pack_tree_with_export(self, tree: ttk.Treeview, tab_label: str) -> None:
        bar = ttk.Frame(self)
        bar.pack(fill=tk.X, pady=(0, 4))
        ttk.Button(
            bar,
            text="Выгрузить отчёт в Google Sheets",
            command=lambda: self._export_to_google(tab_label),
        ).pack(side=tk.RIGHT)
        tree.pack(fill=tk.BOTH, expand=True, pady=6)

    def _export_to_google(self, tab_label: str) -> None:
        if self.crm_app is None:
            return
        self.crm_app.export_table_to_google(self.tree, tab_label, self.cols)


class GoogleSettingsDialog(tk.Toplevel):
    """Поля Google: SA JSON, OAuth client JSON, ID папки; сохранение в .env и справка."""

    def __init__(self, parent: tk.Misc, crm_app: "CRMApp") -> None:
        super().__init__(parent)
        self.title("Настройки Google")
        self.crm_app = crm_app
        self.geometry("720x420")
        self.minsize(640, 360)

        frm = ttk.Frame(self, padding=12)
        frm.pack(fill=tk.BOTH, expand=True)

        row = 0
        ttk.Label(frm, text="Путь к JSON сервисного аккаунта (Sheets API):").grid(
            row=row, column=0, sticky=tk.W, pady=4
        )
        ttk.Entry(frm, textvariable=crm_app.var_sa_path, width=58).grid(
            row=row, column=1, sticky=tk.EW, padx=8, pady=4
        )
        row += 1

        ttk.Label(frm, text="Путь к OAuth client secret (Drive API):").grid(
            row=row, column=0, sticky=tk.W, pady=4
        )
        ttk.Entry(frm, textvariable=crm_app.var_oauth_path, width=58).grid(
            row=row, column=1, sticky=tk.EW, padx=8, pady=4
        )
        row += 1

        ttk.Label(frm, text="ID папки Google Drive для отчётов:").grid(
            row=row, column=0, sticky=tk.W, pady=4
        )
        ttk.Entry(frm, textvariable=crm_app.var_folder_id, width=58).grid(
            row=row, column=1, sticky=tk.EW, padx=8, pady=4
        )
        row += 1

        frm.columnconfigure(1, weight=1)

        hint = (
            "Вставка из буфера: Ctrl+V в поле работает как обычно. "
            "После «Сохранить» значения записываются в .env в корне проекта."
        )
        ttk.Label(frm, text=hint, wraplength=660, foreground="#444").grid(
            row=row, column=0, columnspan=2, sticky=tk.W, pady=8
        )
        row += 1

        btns = ttk.Frame(frm)
        btns.grid(row=row, column=0, columnspan=2, sticky=tk.EW, pady=8)
        ttk.Button(btns, text="Сохранить в .env", command=self._save).pack(side=tk.LEFT, padx=4)
        ttk.Button(btns, text="Справка", command=self._help).pack(side=tk.LEFT, padx=4)
        ttk.Button(btns, text="Закрыть", command=self.destroy).pack(side=tk.RIGHT, padx=4)

    def _save(self) -> None:
        updates = {
            "CREDENTIALS_PATH": self.crm_app.var_sa_path.get().strip(),
            "CREDENTIALS_CLIENT_PATH": self.crm_app.var_oauth_path.get().strip(),
            "REPORT_FOLDER_ID": self.crm_app.var_folder_id.get().strip(),
        }
        try:
            merge_env_file(ENV_PATH, updates)
            load_dotenv(ENV_PATH, override=True)
            for k, v in updates.items():
                os.environ[k] = v
            messagebox.showinfo("Настройки", "Параметры сохранены в .env", parent=self)
        except OSError as e:
            messagebox.showerror("Ошибка", str(e), parent=self)

    def _help(self) -> None:
        win = tk.Toplevel(self)
        win.title("Справка: Google Cloud Console")
        win.geometry("720x480")
        txt = scrolledtext.ScrolledText(win, wrap=tk.WORD, height=22)
        txt.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)
        help_text = """Кратко: что нужно для этих полей

1) Сервисный аккаунт (JSON) — для Google Sheets API
   • Откройте Google Cloud Console → ваш проект.
   • APIs & Services → Library → включите «Google Sheets API».
   • IAM & Admin → Service Accounts → Create service account.
   • Keys → Add key → JSON — скачанный файл укажите в первом поле.
   • Поле client_email в этом JSON — добавьте как редактора на папку отчётов в Google Drive
     (ПКМ по папке → Доступ → добавить email), иначе запись в созданную таблицу может быть запрещена.

2) OAuth client secret (JSON) — для Google Drive API (ваш личный аккаунт)
   • APIs & Services → Library → включите «Google Drive API».
   • APIs & Services → Credentials → Create credentials → OAuth client ID.
   • Application type: Desktop (или Web, если вы настроили redirect для установленного приложения).
   • Скачанный JSON клиента укажите во втором поле.
   • При первой выгрузке откроется браузер для входа в Google — это нормально.

3) ID папки для отчётов (REPORT_FOLDER_ID)
   • В Google Drive откройте нужную папку — из URL вида
     https://drive.google.com/drive/folders/<FOLDER_ID>
     скопируйте <FOLDER_ID> в третье поле.

Подсказка: Backend CRM и Google настраиваются отдельно; выгрузка из приложения идёт с вашего ПК."""
        txt.insert(tk.END, help_text)
        txt.configure(state=tk.DISABLED)
        ttk.Button(win, text="Закрыть", command=win.destroy).pack(pady=6)


class ManagersTab(BaseTab):
    cols = ["id", "full_name", "email", "phone", "created_at"]

    def __init__(self, parent, api: APIClient, status_var: tk.StringVar, crm_app: Optional["CRMApp"] = None) -> None:
        super().__init__(parent, api, status_var, crm_app)
        self.vars = {k: tk.StringVar() for k in ("full_name", "email", "phone")}
        self._build()
        self.refresh()

    def _build(self) -> None:
        form = ttk.LabelFrame(self, text="Менеджер")
        form.pack(fill=tk.X, pady=4)
        for i, key in enumerate(("full_name", "email", "phone")):
            ttk.Label(form, text=key).grid(row=i, column=0, padx=6, pady=4, sticky=tk.W)
            ttk.Entry(form, textvariable=self.vars[key], width=40).grid(
                row=i, column=1, padx=6, pady=4, sticky=tk.W
            )
        actions = ttk.Frame(form)
        actions.grid(row=0, column=2, rowspan=3, padx=8)
        ttk.Button(actions, text="Создать", command=self.create).pack(fill=tk.X, pady=2)
        ttk.Button(actions, text="Обновить", command=self.update).pack(fill=tk.X, pady=2)
        ttk.Button(actions, text="Удалить", command=self.delete).pack(fill=tk.X, pady=2)
        ttk.Button(actions, text="Обновить список", command=self.refresh).pack(fill=tk.X, pady=2)

        self.tree = ttk.Treeview(self, columns=self.cols, show="headings", height=12)
        for c in self.cols:
            self.tree.heading(c, text=c)
            self.tree.column(c, width=120 if c != "id" else 60)
        self.tree.bind("<<TreeviewSelect>>", self.on_select)
        self._pack_tree_with_export(self.tree, "Менеджеры")

    def on_select(self, _evt=None) -> None:
        sel = self.tree.selection()
        if not sel:
            return
        vals = self.tree.item(sel[0], "values")
        self.vars["full_name"].set(vals[1])
        self.vars["email"].set(vals[2])
        self.vars["phone"].set(vals[3])

    def payload(self) -> Dict[str, Any]:
        return {k: v.get().strip() or None for k, v in self.vars.items()}

    def create(self) -> None:
        payload = self.payload()
        if not payload.get("full_name"):
            messagebox.showwarning("Поля", "full_name обязателен")
            return
        self.safe_call(lambda: self.api.post("/managers", payload), "Менеджер создан")
        self.refresh()

    def update(self) -> None:
        manager_id = self.selected_id(self.tree)
        if not manager_id:
            messagebox.showwarning("Выбор", "Выберите менеджера в таблице")
            return
        self.safe_call(
            lambda: self.api.patch(f"/managers/{manager_id}", self.payload()),
            "Менеджер обновлён",
        )
        self.refresh()

    def delete(self) -> None:
        manager_id = self.selected_id(self.tree)
        if not manager_id:
            messagebox.showwarning("Выбор", "Выберите менеджера в таблице")
            return
        if not messagebox.askyesno("Подтверждение", "Удалить менеджера?"):
            return
        self.safe_call(lambda: self.api.delete(f"/managers/{manager_id}"), "Менеджер удалён")
        self.refresh()

    def refresh(self) -> None:
        rows = self.safe_call(lambda: self.api.get("/managers"), "Список менеджеров обновлён")
        if rows is not None:
            self.fill_tree(self.tree, rows, self.cols)


class ClientsTab(BaseTab):
    cols = ["id", "full_name", "status", "company_name", "email", "phone", "manager_id"]

    def __init__(self, parent, api: APIClient, status_var: tk.StringVar, crm_app: Optional["CRMApp"] = None) -> None:
        super().__init__(parent, api, status_var, crm_app)
        self.vars = {
            "full_name": tk.StringVar(),
            "status": tk.StringVar(value="ACTIVE"),
            "company_name": tk.StringVar(),
            "email": tk.StringVar(),
            "phone": tk.StringVar(),
            "manager_id": tk.StringVar(),
            "notes": tk.StringVar(),
            "search": tk.StringVar(),
        }
        self.include_archived = tk.BooleanVar(value=False)
        self._build()
        self.refresh()

    def _build(self) -> None:
        form = ttk.LabelFrame(self, text="Клиент")
        form.pack(fill=tk.X, pady=4)
        fields = [
            ("full_name", 0, 0),
            ("status", 0, 2),
            ("company_name", 1, 0),
            ("email", 1, 2),
            ("phone", 2, 0),
            ("manager_id", 2, 2),
            ("notes", 3, 0),
        ]
        for key, r, c in fields:
            ttk.Label(form, text=key).grid(row=r, column=c, padx=6, pady=4, sticky=tk.W)
            width = 22 if key != "notes" else 50
            span = 1 if key != "notes" else 3
            ttk.Entry(form, textvariable=self.vars[key], width=width).grid(
                row=r, column=c + 1, columnspan=span, padx=6, pady=4, sticky=tk.W
            )

        btns = ttk.Frame(form)
        btns.grid(row=0, column=4, rowspan=4, padx=8, sticky=tk.N)
        ttk.Button(btns, text="Создать", command=self.create).pack(fill=tk.X, pady=2)
        ttk.Button(btns, text="Обновить", command=self.update).pack(fill=tk.X, pady=2)
        ttk.Button(btns, text="Архивировать", command=self.archive).pack(fill=tk.X, pady=2)
        ttk.Button(btns, text="Удалить", command=self.delete).pack(fill=tk.X, pady=2)
        ttk.Button(btns, text="Обновить список", command=self.refresh).pack(fill=tk.X, pady=2)

        search_bar = ttk.Frame(self)
        search_bar.pack(fill=tk.X, pady=4)
        ttk.Label(search_bar, text="Поиск").pack(side=tk.LEFT, padx=4)
        ttk.Entry(search_bar, textvariable=self.vars["search"], width=40).pack(side=tk.LEFT, padx=4)
        ttk.Checkbutton(
            search_bar, text="включая архив", variable=self.include_archived
        ).pack(side=tk.LEFT, padx=6)
        ttk.Button(search_bar, text="Найти", command=self.search).pack(side=tk.LEFT, padx=4)
        ttk.Button(search_bar, text="Сброс", command=self.refresh).pack(side=tk.LEFT, padx=4)

        self.tree = ttk.Treeview(self, columns=self.cols, show="headings", height=12)
        for c in self.cols:
            self.tree.heading(c, text=c)
            self.tree.column(c, width=130 if c != "id" else 60)
        self.tree.bind("<<TreeviewSelect>>", self.on_select)
        self._pack_tree_with_export(self.tree, "Клиенты")

    def on_select(self, _evt=None) -> None:
        sel = self.tree.selection()
        if not sel:
            return
        vals = self.tree.item(sel[0], "values")
        self.vars["full_name"].set(vals[1])
        self.vars["status"].set(vals[2])
        self.vars["company_name"].set(vals[3])
        self.vars["email"].set(vals[4])
        self.vars["phone"].set(vals[5])
        self.vars["manager_id"].set(vals[6] if vals[6] is not None else "")

    def payload(self) -> Dict[str, Any]:
        manager_val = self.vars["manager_id"].get().strip()
        return {
            "full_name": self.vars["full_name"].get().strip() or None,
            "status": self.vars["status"].get().strip() or None,
            "company_name": self.vars["company_name"].get().strip() or None,
            "email": self.vars["email"].get().strip() or None,
            "phone": self.vars["phone"].get().strip() or None,
            "notes": self.vars["notes"].get().strip() or None,
            "manager_id": int(manager_val) if manager_val else None,
        }

    def create(self) -> None:
        payload = self.payload()
        if not payload.get("full_name"):
            messagebox.showwarning("Поля", "full_name обязателен")
            return
        self.safe_call(lambda: self.api.post("/clients", payload), "Клиент создан")
        self.refresh()

    def update(self) -> None:
        client_id = self.selected_id(self.tree)
        if not client_id:
            messagebox.showwarning("Выбор", "Выберите клиента в таблице")
            return
        self.safe_call(
            lambda: self.api.patch(f"/clients/{client_id}", self.payload()),
            "Клиент обновлён",
        )
        self.refresh()

    def archive(self) -> None:
        client_id = self.selected_id(self.tree)
        if not client_id:
            messagebox.showwarning("Выбор", "Выберите клиента в таблице")
            return
        self.safe_call(
            lambda: self.api.post(f"/clients/{client_id}/archive", {}),
            "Клиент архивирован",
        )
        self.refresh()

    def delete(self) -> None:
        client_id = self.selected_id(self.tree)
        if not client_id:
            messagebox.showwarning("Выбор", "Выберите клиента в таблице")
            return
        if not messagebox.askyesno("Подтверждение", "Удалить клиента?"):
            return
        self.safe_call(lambda: self.api.delete(f"/clients/{client_id}"), "Клиент удалён")
        self.refresh()

    def refresh(self) -> None:
        rows = self.safe_call(
            lambda: self.api.get(
                "/clients", params={"include_archived": self.include_archived.get()}
            ),
            "Список клиентов обновлён",
        )
        if rows is not None:
            self.fill_tree(self.tree, rows, self.cols)

    def search(self) -> None:
        q = self.vars["search"].get().strip()
        if not q:
            self.refresh()
            return
        rows = self.safe_call(
            lambda: self.api.get(
                "/clients/search/by-text",
                params={"q": q, "include_archived": self.include_archived.get()},
            ),
            "Поиск клиентов выполнен",
        )
        if rows is not None:
            self.fill_tree(self.tree, rows, self.cols)


class DealsTab(BaseTab):
    cols = ["id", "title", "status", "amount", "client_id", "manager_id", "updated_at"]

    def __init__(self, parent, api: APIClient, status_var: tk.StringVar, crm_app: Optional["CRMApp"] = None) -> None:
        super().__init__(parent, api, status_var, crm_app)
        self.vars = {
            "title": tk.StringVar(),
            "status": tk.StringVar(value="NEW"),
            "amount": tk.StringVar(),
            "description": tk.StringVar(),
            "client_id": tk.StringVar(),
            "manager_id": tk.StringVar(),
            "search": tk.StringVar(),
        }
        self._build()
        self.refresh()

    def _build(self) -> None:
        form = ttk.LabelFrame(self, text="Сделка")
        form.pack(fill=tk.X, pady=4)
        fields = [
            ("title", 0, 0),
            ("status", 0, 2),
            ("amount", 1, 0),
            ("client_id", 1, 2),
            ("manager_id", 2, 0),
            ("description", 2, 2),
        ]
        for key, r, c in fields:
            ttk.Label(form, text=key).grid(row=r, column=c, padx=6, pady=4, sticky=tk.W)
            ttk.Entry(form, textvariable=self.vars[key], width=24).grid(
                row=r, column=c + 1, padx=6, pady=4, sticky=tk.W
            )

        btns = ttk.Frame(form)
        btns.grid(row=0, column=4, rowspan=3, padx=8, sticky=tk.N)
        ttk.Button(btns, text="Создать", command=self.create).pack(fill=tk.X, pady=2)
        ttk.Button(btns, text="Обновить", command=self.update).pack(fill=tk.X, pady=2)
        ttk.Button(btns, text="Удалить", command=self.delete).pack(fill=tk.X, pady=2)
        ttk.Button(btns, text="Обновить список", command=self.refresh).pack(fill=tk.X, pady=2)

        search_bar = ttk.Frame(self)
        search_bar.pack(fill=tk.X, pady=4)
        ttk.Label(search_bar, text="Поиск").pack(side=tk.LEFT, padx=4)
        ttk.Entry(search_bar, textvariable=self.vars["search"], width=40).pack(side=tk.LEFT, padx=4)
        ttk.Button(search_bar, text="Найти", command=self.search).pack(side=tk.LEFT, padx=4)
        ttk.Button(search_bar, text="Сброс", command=self.refresh).pack(side=tk.LEFT, padx=4)

        self.tree = ttk.Treeview(self, columns=self.cols, show="headings", height=12)
        for c in self.cols:
            self.tree.heading(c, text=c)
            self.tree.column(c, width=130 if c != "id" else 60)
        self.tree.bind("<<TreeviewSelect>>", self.on_select)
        self._pack_tree_with_export(self.tree, "Сделки")

    def on_select(self, _evt=None) -> None:
        sel = self.tree.selection()
        if not sel:
            return
        vals = self.tree.item(sel[0], "values")
        self.vars["title"].set(vals[1])
        self.vars["status"].set(vals[2])
        self.vars["amount"].set(vals[3] if vals[3] is not None else "")
        self.vars["client_id"].set(vals[4] if vals[4] is not None else "")
        self.vars["manager_id"].set(vals[5] if vals[5] is not None else "")

    def payload(self) -> Dict[str, Any]:
        amount = self.vars["amount"].get().strip()
        client = self.vars["client_id"].get().strip()
        manager = self.vars["manager_id"].get().strip()
        return {
            "title": self.vars["title"].get().strip() or None,
            "description": self.vars["description"].get().strip() or None,
            "status": self.vars["status"].get().strip() or None,
            "amount": float(amount) if amount else None,
            "client_id": int(client) if client else None,
            "manager_id": int(manager) if manager else None,
        }

    def create(self) -> None:
        payload = self.payload()
        if not payload.get("title"):
            messagebox.showwarning("Поля", "title обязателен")
            return
        self.safe_call(lambda: self.api.post("/deals", payload), "Сделка создана")
        self.refresh()

    def update(self) -> None:
        deal_id = self.selected_id(self.tree)
        if not deal_id:
            messagebox.showwarning("Выбор", "Выберите сделку в таблице")
            return
        self.safe_call(lambda: self.api.patch(f"/deals/{deal_id}", self.payload()), "Сделка обновлена")
        self.refresh()

    def delete(self) -> None:
        deal_id = self.selected_id(self.tree)
        if not deal_id:
            messagebox.showwarning("Выбор", "Выберите сделку в таблице")
            return
        if not messagebox.askyesno("Подтверждение", "Удалить сделку?"):
            return
        self.safe_call(lambda: self.api.delete(f"/deals/{deal_id}"), "Сделка удалена")
        self.refresh()

    def refresh(self) -> None:
        rows = self.safe_call(lambda: self.api.get("/deals"), "Список сделок обновлён")
        if rows is not None:
            self.fill_tree(self.tree, rows, self.cols)

    def search(self) -> None:
        q = self.vars["search"].get().strip()
        if not q:
            self.refresh()
            return
        rows = self.safe_call(
            lambda: self.api.get("/deals/search/by-text", params={"q": q}),
            "Поиск сделок выполнен",
        )
        if rows is not None:
            self.fill_tree(self.tree, rows, self.cols)


class OrdersTab(BaseTab):
    cols = ["id", "order_number", "status", "total_amount", "deal_id", "client_id", "manager_id", "updated_at"]

    def __init__(self, parent, api: APIClient, status_var: tk.StringVar, crm_app: Optional["CRMApp"] = None) -> None:
        super().__init__(parent, api, status_var, crm_app)
        self.vars = {
            "order_number": tk.StringVar(),
            "status": tk.StringVar(value="NEW"),
            "total_amount": tk.StringVar(),
            "deal_id": tk.StringVar(),
            "client_id": tk.StringVar(),
            "manager_id": tk.StringVar(),
        }
        self._build()
        self.refresh()

    def _build(self) -> None:
        form = ttk.LabelFrame(self, text="Заказ")
        form.pack(fill=tk.X, pady=4)
        fields = [
            ("order_number", 0, 0),
            ("status", 0, 2),
            ("total_amount", 1, 0),
            ("deal_id", 1, 2),
            ("client_id", 2, 0),
            ("manager_id", 2, 2),
        ]
        for key, r, c in fields:
            ttk.Label(form, text=key).grid(row=r, column=c, padx=6, pady=4, sticky=tk.W)
            ttk.Entry(form, textvariable=self.vars[key], width=24).grid(
                row=r, column=c + 1, padx=6, pady=4, sticky=tk.W
            )

        btns = ttk.Frame(form)
        btns.grid(row=0, column=4, rowspan=3, padx=8, sticky=tk.N)
        ttk.Button(btns, text="Создать", command=self.create).pack(fill=tk.X, pady=2)
        ttk.Button(btns, text="Обновить", command=self.update).pack(fill=tk.X, pady=2)
        ttk.Button(btns, text="Удалить", command=self.delete).pack(fill=tk.X, pady=2)
        ttk.Button(btns, text="Обновить список", command=self.refresh).pack(fill=tk.X, pady=2)

        self.tree = ttk.Treeview(self, columns=self.cols, show="headings", height=12)
        for c in self.cols:
            self.tree.heading(c, text=c)
            self.tree.column(c, width=130 if c != "id" else 60)
        self.tree.bind("<<TreeviewSelect>>", self.on_select)
        self._pack_tree_with_export(self.tree, "Заказы")

    def on_select(self, _evt=None) -> None:
        sel = self.tree.selection()
        if not sel:
            return
        vals = self.tree.item(sel[0], "values")
        self.vars["order_number"].set(vals[1] if vals[1] is not None else "")
        self.vars["status"].set(vals[2] if vals[2] is not None else "")
        self.vars["total_amount"].set(vals[3] if vals[3] is not None else "")
        self.vars["deal_id"].set(vals[4] if vals[4] is not None else "")
        self.vars["client_id"].set(vals[5] if vals[5] is not None else "")
        self.vars["manager_id"].set(vals[6] if vals[6] is not None else "")

    def payload(self) -> Dict[str, Any]:
        amount = self.vars["total_amount"].get().strip()
        deal = self.vars["deal_id"].get().strip()
        client = self.vars["client_id"].get().strip()
        manager = self.vars["manager_id"].get().strip()
        return {
            "order_number": self.vars["order_number"].get().strip() or None,
            "status": self.vars["status"].get().strip() or None,
            "total_amount": float(amount) if amount else None,
            "deal_id": int(deal) if deal else None,
            "client_id": int(client) if client else None,
            "manager_id": int(manager) if manager else None,
        }

    def create(self) -> None:
        self.safe_call(lambda: self.api.post("/orders", self.payload()), "Заказ создан")
        self.refresh()

    def update(self) -> None:
        order_id = self.selected_id(self.tree)
        if not order_id:
            messagebox.showwarning("Выбор", "Выберите заказ в таблице")
            return
        self.safe_call(lambda: self.api.patch(f"/orders/{order_id}", self.payload()), "Заказ обновлён")
        self.refresh()

    def delete(self) -> None:
        order_id = self.selected_id(self.tree)
        if not order_id:
            messagebox.showwarning("Выбор", "Выберите заказ в таблице")
            return
        if not messagebox.askyesno("Подтверждение", "Удалить заказ?"):
            return
        self.safe_call(lambda: self.api.delete(f"/orders/{order_id}"), "Заказ удалён")
        self.refresh()

    def refresh(self) -> None:
        rows = self.safe_call(lambda: self.api.get("/orders"), "Список заказов обновлён")
        if rows is not None:
            self.fill_tree(self.tree, rows, self.cols)


class TasksTab(BaseTab):
    cols = ["id", "title", "is_done", "due_date", "client_id", "deal_id", "manager_id"]

    def __init__(self, parent, api: APIClient, status_var: tk.StringVar, crm_app: Optional["CRMApp"] = None) -> None:
        super().__init__(parent, api, status_var, crm_app)
        self.vars = {
            "title": tk.StringVar(),
            "description": tk.StringVar(),
            "due_date": tk.StringVar(),
            "client_id": tk.StringVar(),
            "deal_id": tk.StringVar(),
            "manager_id": tk.StringVar(),
        }
        self._build()
        self.refresh()

    def _build(self) -> None:
        form = ttk.LabelFrame(self, text="Задача / напоминание")
        form.pack(fill=tk.X, pady=4)
        fields = [("title", 0, 0), ("due_date", 0, 2), ("client_id", 1, 0), ("deal_id", 1, 2), ("manager_id", 2, 0), ("description", 2, 2)]
        for key, r, c in fields:
            ttk.Label(form, text=key).grid(row=r, column=c, padx=6, pady=4, sticky=tk.W)
            ttk.Entry(form, textvariable=self.vars[key], width=24).grid(
                row=r, column=c + 1, padx=6, pady=4, sticky=tk.W
            )

        btns = ttk.Frame(form)
        btns.grid(row=0, column=4, rowspan=3, padx=8, sticky=tk.N)
        ttk.Button(btns, text="Создать", command=self.create).pack(fill=tk.X, pady=2)
        ttk.Button(btns, text="Обновить", command=self.update).pack(fill=tk.X, pady=2)
        ttk.Button(btns, text="Отметить выполненной", command=self.mark_done).pack(fill=tk.X, pady=2)
        ttk.Button(btns, text="Снять выполнение", command=self.mark_undone).pack(fill=tk.X, pady=2)
        ttk.Button(btns, text="Удалить", command=self.delete).pack(fill=tk.X, pady=2)
        ttk.Button(btns, text="Обновить список", command=self.refresh).pack(fill=tk.X, pady=2)

        self.tree = ttk.Treeview(self, columns=self.cols, show="headings", height=12)
        for c in self.cols:
            self.tree.heading(c, text=c)
            self.tree.column(c, width=120 if c != "id" else 60)
        self.tree.bind("<<TreeviewSelect>>", self.on_select)
        self._pack_tree_with_export(self.tree, "Задачи")

    def on_select(self, _evt=None) -> None:
        sel = self.tree.selection()
        if not sel:
            return
        vals = self.tree.item(sel[0], "values")
        self.vars["title"].set(vals[1])
        self.vars["due_date"].set(vals[3] if vals[3] is not None else "")
        self.vars["client_id"].set(vals[4] if vals[4] is not None else "")
        self.vars["deal_id"].set(vals[5] if vals[5] is not None else "")
        self.vars["manager_id"].set(vals[6] if vals[6] is not None else "")

    def payload(self) -> Dict[str, Any]:
        client = self.vars["client_id"].get().strip()
        deal = self.vars["deal_id"].get().strip()
        manager = self.vars["manager_id"].get().strip()
        return {
            "title": self.vars["title"].get().strip() or None,
            "description": self.vars["description"].get().strip() or None,
            "due_date": self.vars["due_date"].get().strip() or None,
            "client_id": int(client) if client else None,
            "deal_id": int(deal) if deal else None,
            "manager_id": int(manager) if manager else None,
        }

    def create(self) -> None:
        payload = self.payload()
        if not payload.get("title"):
            messagebox.showwarning("Поля", "title обязателен")
            return
        payload["is_done"] = False
        self.safe_call(lambda: self.api.post("/tasks", payload), "Задача создана")
        self.refresh()

    def update(self) -> None:
        task_id = self.selected_id(self.tree)
        if not task_id:
            messagebox.showwarning("Выбор", "Выберите задачу в таблице")
            return
        self.safe_call(lambda: self.api.patch(f"/tasks/{task_id}", self.payload()), "Задача обновлена")
        self.refresh()

    def mark_done(self) -> None:
        self._set_done(True)

    def mark_undone(self) -> None:
        self._set_done(False)

    def _set_done(self, is_done: bool) -> None:
        task_id = self.selected_id(self.tree)
        if not task_id:
            messagebox.showwarning("Выбор", "Выберите задачу в таблице")
            return
        self.safe_call(
            lambda: self.api.post(f"/tasks/{task_id}/done", {"is_done": is_done}),
            "Статус задачи обновлён",
        )
        self.refresh()

    def delete(self) -> None:
        task_id = self.selected_id(self.tree)
        if not task_id:
            messagebox.showwarning("Выбор", "Выберите задачу в таблице")
            return
        if not messagebox.askyesno("Подтверждение", "Удалить задачу?"):
            return
        self.safe_call(lambda: self.api.delete(f"/tasks/{task_id}"), "Задача удалена")
        self.refresh()

    def refresh(self) -> None:
        rows = self.safe_call(lambda: self.api.get("/tasks"), "Список задач обновлён")
        if rows is not None:
            self.fill_tree(self.tree, rows, self.cols)


class CRMApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Mini CRM client (Tkinter + FastAPI)")
        self.geometry("1300x820")
        self.minsize(1080, 700)

        load_dotenv(ENV_PATH)
        env = dotenv_values(ENV_PATH) if ENV_PATH.exists() else {}
        self.var_sa_path = tk.StringVar(
            value=(env.get("CREDENTIALS_PATH") or "my-zerocoder-project-da0be47bed0c.json").strip()
        )
        self.var_oauth_path = tk.StringVar(
            value=(env.get("CREDENTIALS_CLIENT_PATH") or "client_secret.json").strip()
        )
        self.var_folder_id = tk.StringVar(value=(env.get("REPORT_FOLDER_ID") or "").strip())

        self.api = APIClient()
        self.status_var = tk.StringVar(value="Готово к работе")

        top = ttk.Frame(self, padding=8)
        top.pack(fill=tk.X)
        ttk.Label(top, text="Backend URL:").pack(side=tk.LEFT)
        self.base_var = tk.StringVar(value=API_BASE)
        ttk.Entry(top, textvariable=self.base_var, width=40).pack(side=tk.LEFT, padx=6)
        ttk.Button(top, text="Подключить", command=self.reconnect).pack(side=tk.LEFT, padx=6)
        ttk.Button(top, text="Проверить health", command=self.check_health).pack(side=tk.LEFT, padx=6)
        ttk.Button(top, text="Настройки Google", command=self.open_google_settings).pack(
            side=tk.LEFT, padx=10
        )

        self.tabs = ttk.Notebook(self)
        self.tabs.pack(fill=tk.BOTH, expand=True)

        self.tab_managers = ManagersTab(self.tabs, self.api, self.status_var, self)
        self.tab_clients = ClientsTab(self.tabs, self.api, self.status_var, self)
        self.tab_deals = DealsTab(self.tabs, self.api, self.status_var, self)
        self.tab_orders = OrdersTab(self.tabs, self.api, self.status_var, self)
        self.tab_tasks = TasksTab(self.tabs, self.api, self.status_var, self)

        self.tabs.add(self.tab_managers, text="Менеджеры")
        self.tabs.add(self.tab_clients, text="Клиенты")
        self.tabs.add(self.tab_deals, text="Сделки")
        self.tabs.add(self.tab_orders, text="Заказы")
        self.tabs.add(self.tab_tasks, text="Задачи")

        bottom = ttk.Frame(self, padding=8)
        bottom.pack(fill=tk.X)
        ttk.Label(bottom, textvariable=self.status_var).pack(side=tk.LEFT)

    def open_google_settings(self) -> None:
        GoogleSettingsDialog(self, self)

    def _show_report_link_dialog(self, url: str) -> None:
        win = tk.Toplevel(self)
        win.title("Отчёт создан")
        win.geometry("560x160")
        ttk.Label(win, text="Таблица создана. Откройте по ссылке:", padding=8).pack(anchor=tk.W)
        link_lbl = ttk.Label(win, text=url, foreground="#1a73e8", cursor="hand2", padding=8)
        link_lbl.pack(anchor=tk.W)
        link_lbl.bind("<Button-1>", lambda _e: webbrowser.open(url))
        ttk.Label(win, text="(клик по ссылке откроет браузер)", foreground="#666").pack(anchor=tk.W)
        ttk.Button(win, text="Закрыть", command=win.destroy).pack(pady=12)

    def export_table_to_google(self, tree: ttk.Treeview, tab_label: str, columns: list[str]) -> None:
        from crm_google_export import (
            export_table_to_google_sheet,
            resolve_project_path,
            tree_to_matrix,
        )

        sa = resolve_project_path(PROJECT_ROOT, self.var_sa_path.get())
        oauth = resolve_project_path(PROJECT_ROOT, self.var_oauth_path.get())
        folder = self.var_folder_id.get().strip()
        token_path = PROJECT_ROOT / "token_drive.json"

        if not folder:
            messagebox.showerror("Google", "Укажите ID папки отчётов (Настройки Google).")
            return
        if not sa.is_file():
            messagebox.showerror("Google", f"Не найден файл сервисного аккаунта:\n{sa}")
            return
        if not oauth.is_file():
            messagebox.showerror("Google", f"Не найден OAuth client secret:\n{oauth}")
            return

        header, rows = tree_to_matrix(columns, tree)
        self.status_var.set(f"Выгрузка «{tab_label}» в Google…")

        def work() -> None:
            err: Optional[str] = None
            url_out: Optional[str] = None
            try:
                url_out = export_table_to_google_sheet(
                    project_root=PROJECT_ROOT,
                    service_account_json=sa,
                    oauth_client_secret=oauth,
                    token_path=token_path,
                    report_folder_id=folder,
                    table_label=tab_label,
                    header=header,
                    data_rows=rows,
                )
            except Exception as e:
                err = str(e)
            if err:
                self.after(0, lambda m=err: self._export_done_err(m))
            else:
                self.after(0, lambda u=url_out: self._export_done_ok(u))

        threading.Thread(target=work, daemon=True).start()

    def _export_done_ok(self, url: Optional[str]) -> None:
        self.status_var.set("Выгрузка в Google завершена.")
        if url:
            self._show_report_link_dialog(url)

    def _export_done_err(self, msg: str) -> None:
        self.status_var.set("Ошибка выгрузки в Google.")
        messagebox.showerror("Выгрузка в Google Sheets", msg)

    def reconnect(self) -> None:
        self.api = APIClient(self.base_var.get().strip())
        for tab in (
            self.tab_managers,
            self.tab_clients,
            self.tab_deals,
            self.tab_orders,
            self.tab_tasks,
        ):
            tab.api = self.api
        self.status_var.set(f"Подключено к {self.api.base_url}")

    def check_health(self) -> None:
        try:
            data = self.api.get("/health")
            self.status_var.set(f"Health OK: {data}")
        except Exception as e:
            self.status_var.set("Health check failed")
            messagebox.showerror("Backend", str(e))


def _setup_logging() -> None:
    """Уровень из LOG_LEVEL в .env (см. backend.crm_logging)."""
    root = Path(__file__).resolve().parents[2]
    src = root / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
    from backend.crm_logging import setup_logging

    setup_logging("crm_tkinter")


def main() -> None:
    load_dotenv(Path(__file__).resolve().parents[2] / ".env")
    _setup_logging()
    app = CRMApp()
    app.mainloop()


if __name__ == "__main__":
    main()
