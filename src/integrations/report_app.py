#!/usr/bin/env python3
"""
Простое окно Tkinter: период, подразделение, тип отчёта — симуляция и запись в Google Sheets.
"""

from __future__ import annotations

import logging
import sys
import threading
import tkinter as tk
from datetime import date, datetime
from tkinter import messagebox, ttk

from google.auth.exceptions import GoogleAuthError
from googleapiclient.errors import HttpError

from google_sheets import GoogleSheetsClient, _configure_logging
from report_generator import export_report_to_sheets

logger = logging.getLogger(__name__)

DEPARTMENTS = ("Продажи", "Маркетинг", "IT", "Бухгалтерия", "Логистика")
REPORT_KINDS = ("Ежедневный", "Недельный", "Сводный", "План-факт")


def _parse_date(s: str) -> date:
    s = s.strip()
    return datetime.strptime(s, "%Y-%m-%d").date()


class ReportApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Симулятор отчётов → Google Таблица")
        self.geometry("520x320")
        self.minsize(480, 280)

        self._client: GoogleSheetsClient | None = None
        self._busy = False

        pad = {"padx": 10, "pady": 6}
        frm = ttk.Frame(self, padding=16)
        frm.pack(fill=tk.BOTH, expand=True)

        ttk.Label(frm, text="Дата начала периода (ГГГГ-ММ-ДД)").grid(
            row=0, column=0, sticky=tk.W, **pad
        )
        self.var_from = tk.StringVar(value=date.today().replace(day=1).isoformat())
        ttk.Entry(frm, textvariable=self.var_from, width=28).grid(
            row=0, column=1, sticky=tk.W, **pad
        )

        ttk.Label(frm, text="Дата окончания периода").grid(
            row=1, column=0, sticky=tk.W, **pad
        )
        self.var_to = tk.StringVar(value=date.today().isoformat())
        ttk.Entry(frm, textvariable=self.var_to, width=28).grid(
            row=1, column=1, sticky=tk.W, **pad
        )

        ttk.Label(frm, text="Подразделение").grid(
            row=2, column=0, sticky=tk.W, **pad
        )
        self.combo_dept = ttk.Combobox(
            frm, values=DEPARTMENTS, state="readonly", width=26
        )
        self.combo_dept.grid(row=2, column=1, sticky=tk.W, **pad)
        self.combo_dept.current(0)

        ttk.Label(frm, text="Тип отчёта").grid(
            row=3, column=0, sticky=tk.W, **pad
        )
        self.combo_kind = ttk.Combobox(
            frm, values=REPORT_KINDS, state="readonly", width=26
        )
        self.combo_kind.grid(row=3, column=1, sticky=tk.W, **pad)
        self.combo_kind.current(2)

        self.btn = ttk.Button(
            frm,
            text="Сформировать отчёт и записать в таблицу (новый лист)",
            command=self._on_export,
        )
        self.btn.grid(row=4, column=0, columnspan=2, pady=16)

        self.status = tk.StringVar(value="Укажите период и нажмите кнопку.")
        ttk.Label(frm, textvariable=self.status, wraplength=460).grid(
            row=5, column=0, columnspan=2, sticky=tk.W, **pad
        )

        ttk.Separator(frm, orient=tk.HORIZONTAL).grid(
            row=6, column=0, columnspan=2, sticky=tk.EW, pady=8
        )
        hint = (
            "Каждый запуск создаёт новый лист в книге из .env (SPREADSHEET_ID). "
            "Данные случайные, оформление — как документ в ячейках."
        )
        ttk.Label(frm, text=hint, wraplength=460, foreground="#444").grid(
            row=7, column=0, columnspan=2, sticky=tk.W, padx=10
        )

    def _ensure_client(self) -> GoogleSheetsClient:
        if self._client is None:
            self._client = GoogleSheetsClient()
        return self._client

    def _set_busy(self, busy: bool) -> None:
        self._busy = busy
        self.btn.configure(state=tk.DISABLED if busy else tk.NORMAL)

    def _on_export(self) -> None:
        if self._busy:
            return
        try:
            d0 = _parse_date(self.var_from.get())
            d1 = _parse_date(self.var_to.get())
        except ValueError:
            messagebox.showerror(
                "Дата",
                "Введите даты в формате ГГГГ-ММ-ДД, например 2026-04-01.",
            )
            return

        if d1 < d0:
            messagebox.showerror("Период", "Дата «по» не может быть раньше даты «с».")
            return

        dept = self.combo_dept.get()
        kind = self.combo_kind.get()
        if not dept or not kind:
            messagebox.showerror("Поля", "Выберите подразделение и тип отчёта.")
            return

        self._set_busy(True)
        self.status.set("Отправка в Google Sheets…")

        def work() -> None:
            try:
                client = self._ensure_client()
                title = export_report_to_sheets(client, d0, d1, dept, kind)
                self.after(0, lambda t=title: self._done_ok(t))
            except FileNotFoundError as e:
                msg = str(e)
                self.after(0, lambda m=msg: self._done_err(m))
            except (ValueError, HttpError, GoogleAuthError) as e:
                msg = str(e)
                self.after(0, lambda m=msg: self._done_err(m))
            except Exception as e:
                logger.exception("export")
                err = f"{type(e).__name__}: {e}"
                self.after(0, lambda m=err: self._done_err(m))

        threading.Thread(target=work, daemon=True).start()

    def _done_ok(self, sheet_title: str) -> None:
        self._set_busy(False)
        self.status.set(f"Готово. Создан лист: «{sheet_title}»")
        messagebox.showinfo(
            "Готово",
            f"Отчёт записан на новый лист:\n«{sheet_title}»",
        )

    def _done_err(self, msg: str) -> None:
        self._set_busy(False)
        self.status.set("Ошибка — см. сообщение.")
        messagebox.showerror("Ошибка", msg)


def main() -> None:
    _configure_logging()
    logging.getLogger("googleapiclient").setLevel(logging.WARNING)
    app = ReportApp()
    app.mainloop()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
