"""
Генерация случайных «отчётных» данных и выгрузка в Google Таблицу
с оформлением (новый лист на каждый отчёт).
"""

from __future__ import annotations

import logging
import random
import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Sequence, Tuple

from google_sheets import GoogleSheetsClient

logger = logging.getLogger(__name__)

NUM_COLS = 8

# Фиксированная вёрстка: строки 0–6 — шапка и заголовки таблицы, далее данные, последняя — итог
_TITLE_ROW = 0
_HEADER_ROW = 6
_DATA_START_ROW = 7

_INDICATORS = (
    "Выручка",
    "Количество заказов",
    "Средний чек",
    "Конверсия",
    "Возвраты",
    "Новые клиенты",
    "Повторные обращения",
    "Себестоимость",
)
_UNITS = ("руб.", "шт.", "%", "ед.", "чел.")
_STATUSES = ("OK", "В работе", "Требует внимания", "Закрыто")
_NAMES = (
    "Иванова А.",
    "Петров П.",
    "Сидорова Е.",
    "Козлов Д.",
    "Никитина М.",
    "Орлов В.",
)
_NOTES = (
    "В пределах плана",
    "Пересмотреть на след. период",
    "Согласовано",
    "Черновик",
    "",
)


def _safe_sheet_title(base: str) -> str:
    """Имя листа Google Sheets: макс. 100 символов, без \\ / ? * [ ]."""
    s = re.sub(r'[/\\?*\[\]]', "_", base.strip()) or "Отчёт"
    return s[:100]


def generate_sheet_title() -> str:
    """Уникальное имя листа по времени."""
    return _safe_sheet_title(f"Отчёт {datetime.now():%Y-%m-%d %H-%M-%S}")


def _random_day(d0: date, d1: date) -> date:
    if d1 < d0:
        d0, d1 = d1, d0
    span = (d1 - d0).days + 1
    return d0 + timedelta(days=random.randint(0, span - 1))


def _pad_row(row: Sequence[Any], n: int = NUM_COLS) -> List[Any]:
    r = list(row)
    while len(r) < n:
        r.append("")
    return r[:n]


def simulate_report_values(
    date_from: date,
    date_to: date,
    department: str,
    report_kind: str,
    n_data_rows: int | None = None,
) -> Tuple[List[List[Any]], int]:
    """
    Строит двумерную сетку значений отчёта и число строк данных.

    Пример:
        grid, n = simulate_report_values(
            date(2026, 1, 1), date(2026, 1, 31), "Продажи", "Сводный"
        )
    """
    if n_data_rows is None:
        n_data_rows = random.randint(6, 14)
    n_data_rows = max(1, min(n_data_rows, 200))

    title = (
        f"ОПЕРАЦИОННЫЙ ОТЧЁТ — {department.upper()} "
        f"({report_kind})"
    )

    rows: List[List[Any]] = []
    rows.append(_pad_row([title]))
    rows.append(_pad_row([]))
    rows.append(
        _pad_row(
            [
                "Период с",
                date_from.isoformat(),
                "по",
                date_to.isoformat(),
                "",
                "",
                "",
                "",
            ]
        )
    )
    rows.append(_pad_row(["Подразделение", department, "", "", "", "", "", ""]))
    rows.append(_pad_row(["Тип отчёта", report_kind, "", "", "", "", "", ""]))
    rows.append(_pad_row([]))
    rows.append(
        _pad_row(
            [
                "№",
                "Дата",
                "Показатель",
                "Значение",
                "Ед.",
                "Статус",
                "Ответственный",
                "Примечание",
            ]
        )
    )

    value_sum = 0.0
    for i in range(1, n_data_rows + 1):
        d = _random_day(date_from, date_to)
        ind = random.choice(_INDICATORS)
        val = round(random.uniform(120, 99_000) + random.random(), 2)
        value_sum += val
        rows.append(
            _pad_row(
                [
                    i,
                    d.isoformat(),
                    ind,
                    val,
                    random.choice(_UNITS),
                    random.choice(_STATUSES),
                    random.choice(_NAMES),
                    random.choice(_NOTES),
                ]
            )
        )

    rows.append(
        _pad_row(
            [
                "ИТОГО",
                "",
                f"Строк данных: {n_data_rows}",
                round(value_sum, 2),
                "",
                "",
                "",
                "Сумма по столбцу «Значение»",
            ]
        )
    )

    return rows, n_data_rows


def _color(r: float, g: float, b: float) -> Dict[str, float]:
    return {"red": r, "green": g, "blue": b}


def build_format_requests(
    sheet_id: int,
    n_data_rows: int,
    num_cols: int = NUM_COLS,
) -> List[Dict[str, Any]]:
    """
    Запросы batchUpdate: объединения, шрифты, заливка, границы, ширина колонок.

    Пример:
        reqs = build_format_requests(sheet_id=12345, n_data_rows=10)
        client.batch_update(reqs)
    """
    header_row = _HEADER_ROW
    data_start = _DATA_START_ROW
    total_row = data_start + n_data_rows
    last_row_excl = total_row + 1
    col_excl = num_cols

    reqs: List[Dict[str, Any]] = []

    reqs.append(
        {
            "mergeCells": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": _TITLE_ROW,
                    "endRowIndex": _TITLE_ROW + 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": col_excl,
                },
                "mergeType": "MERGE_ALL",
            }
        }
    )

    border_color = _color(0.65, 0.65, 0.68)
    solid = {"style": "SOLID", "width": 1, "color": border_color}

    reqs.append(
        {
            "updateBorders": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": header_row,
                    "endRowIndex": last_row_excl,
                    "startColumnIndex": 0,
                    "endColumnIndex": col_excl,
                },
                "top": solid,
                "bottom": solid,
                "left": solid,
                "right": solid,
                "innerHorizontal": solid,
                "innerVertical": solid,
            }
        }
    )

    reqs.append(
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": _TITLE_ROW,
                    "endRowIndex": _TITLE_ROW + 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": col_excl,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": _color(0.92, 0.95, 1.0),
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "wrapStrategy": "WRAP",
                        "textFormat": {
                            "bold": True,
                            "fontSize": 13,
                            "foregroundColor": _color(0.1, 0.2, 0.35),
                        },
                    }
                },
                "fields": (
                    "userEnteredFormat(backgroundColor,textFormat,"
                    "horizontalAlignment,verticalAlignment,wrapStrategy)"
                ),
            }
        }
    )

    reqs.append(
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": header_row,
                    "endRowIndex": header_row + 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": col_excl,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": _color(0.88, 0.88, 0.9),
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {"bold": True, "fontSize": 10},
                    }
                },
                "fields": (
                    "userEnteredFormat(backgroundColor,textFormat,"
                    "horizontalAlignment,verticalAlignment)"
                ),
            }
        }
    )

    reqs.append(
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": data_start,
                    "endRowIndex": last_row_excl,
                    "startColumnIndex": 0,
                    "endColumnIndex": col_excl,
                },
                "cell": {
                    "userEnteredFormat": {
                        "verticalAlignment": "MIDDLE",
                        "wrapStrategy": "WRAP",
                        "textFormat": {"fontSize": 10},
                    }
                },
                "fields": "userEnteredFormat(verticalAlignment,wrapStrategy,textFormat)",
            }
        }
    )

    reqs.append(
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": total_row,
                    "endRowIndex": last_row_excl,
                    "startColumnIndex": 0,
                    "endColumnIndex": col_excl,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": _color(0.96, 0.98, 0.94),
                        "textFormat": {"bold": True, "fontSize": 10},
                        "verticalAlignment": "MIDDLE",
                    }
                },
                "fields": (
                    "userEnteredFormat(backgroundColor,textFormat,verticalAlignment)"
                ),
            }
        }
    )

    widths = [52, 110, 200, 100, 56, 130, 140, 220]
    for i, px in enumerate(widths[:num_cols]):
        reqs.append(
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": i,
                        "endIndex": i + 1,
                    },
                    "properties": {"pixelSize": px},
                    "fields": "pixelSize",
                }
            }
        )

    reqs.append(
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": _TITLE_ROW,
                    "endIndex": _TITLE_ROW + 1,
                },
                "properties": {"pixelSize": 44},
                "fields": "pixelSize",
            }
        }
    )

    return reqs


def export_report_to_sheets(
    client: GoogleSheetsClient,
    date_from: date,
    date_to: date,
    department: str,
    report_kind: str,
) -> str:
    """
    Создаёт новый лист, записывает отчёт и применяет оформление.

    Пример:
        from google_sheets import GoogleSheetsClient
        client = GoogleSheetsClient()
        title = export_report_to_sheets(
            client, date(2026, 1, 1), date(2026, 1, 31), "Продажи", "Сводный"
        )
    """
    sheet_title = generate_sheet_title()
    actual_title, sheet_id = client.add_sheet(sheet_title)
    values, n_data = simulate_report_values(
        date_from, date_to, department, report_kind
    )

    end_col = chr(ord("A") + NUM_COLS - 1)
    last_row = len(values)
    range_a1 = f"A1:{end_col}{last_row}"

    client.write_range(range_a1, values, sheet_name=actual_title)
    fmt = build_format_requests(sheet_id, n_data, NUM_COLS)
    client.batch_update(fmt)

    logger.info(
        "Отчёт выгружен на лист «%s», строк данных: %s", actual_title, n_data
    )
    return actual_title
