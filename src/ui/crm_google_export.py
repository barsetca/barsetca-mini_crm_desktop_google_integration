"""
Выгрузка таблицы CRM в новую Google Таблицу: создание файла через Drive (OAuth),
запись и форматирование через Sheets (сервисный аккаунт).

Используются только уже реализованные классы google_drive.GoogleDriveClient и
google_sheets.GoogleSheetsClient (без добавления новых методов в эти модули).
"""

from __future__ import annotations

import logging
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from google.auth.exceptions import GoogleAuthError
from googleapiclient.errors import HttpError

# Подключаем пакет интеграций (как при запуске из src/ui)
_ROOT = Path(__file__).resolve().parents[2]
_INTEGRATIONS = _ROOT / "src" / "integrations"
if str(_INTEGRATIONS) not in sys.path:
    sys.path.insert(0, str(_INTEGRATIONS))

from google_drive import GoogleDriveClient  # noqa: E402
from google_sheets import GoogleSheetsClient  # noqa: E402

logger = logging.getLogger(__name__)


def _user_message_for_http_error(context: str, err: HttpError) -> str:
    status = err.resp.status if getattr(err, "resp", None) else "?"
    snippet = ""
    try:
        if getattr(err, "content", None):
            snippet = err.content.decode("utf-8", errors="replace")[:400]
    except Exception:
        snippet = str(err)
    hint = ""
    if status in (403, 404):
        hint = (
            " Проверьте, что сервисному аккаунту (client_email из JSON) выдан доступ "
            "редактора к файлу/папке на Google Drive."
        )
    if status == 400 and "not supported" in snippet.lower():
        hint = " Убедитесь, что объект — нативная Google Таблица, а не загруженный Excel."
    return f"{context}: HTTP {status}. {snippet}{hint}"


def col_letter_1based(n: int) -> str:
    """A=1, Z=26, AA=27."""
    if n < 1:
        return "A"
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def _color(r: float, g: float, b: float) -> Dict[str, float]:
    return {"red": r, "green": g, "blue": b}


def tree_to_matrix(tree_columns: Sequence[str], tree) -> Tuple[List[str], List[List[Any]]]:
    """Извлекает заголовки Treeview и строки значений (как в UI)."""
    header = list(tree_columns)
    rows: List[List[Any]] = []
    for iid in tree.get_children():
        rows.append(list(tree.item(iid, "values")))
    return header, rows


def _try_float(x: Any) -> Tuple[bool, float]:
    if x is None or x == "":
        return False, 0.0
    if isinstance(x, (int, float)):
        return True, float(x)
    try:
        return True, float(str(x).replace(",", "."))
    except ValueError:
        return False, 0.0


def _is_date_column(name: str) -> bool:
    h = name.lower()
    return "date" in h or h.endswith("_at")


def _month_key(cell: Any) -> Optional[Tuple[int, int]]:
    """Год и месяц (YYYY, MM) из значения ячейки или None."""
    if cell is None or cell == "":
        return None
    s = str(cell).strip()
    if not s:
        return None
    try:
        part = s.replace("Z", "").split("+")[0].split("T")[0].strip()
        d = datetime.strptime(part[:10], "%Y-%m-%d")
        return (d.year, d.month)
    except ValueError:
        pass
    try:
        d = datetime.fromisoformat(s.replace("Z", "+00:00").split("+")[0])
        return (d.year, d.month)
    except ValueError:
        return None


def build_analytics(
    header: Sequence[str],
    data_rows: Sequence[Sequence[Any]],
) -> List[Tuple[str, Any, str]]:
    """
    Верхний блок отчёта (только по требованиям CRM):
    1) общее число строк;
    2) для колонок с «amount» в имени — сумма и среднее;
    3) для остальных колонок — число уникальных значений;
    4) для колонок с датой — по каждому месяцу сумма и среднее по каждой amount-колонке.

    Третий элемент кортежа: «decimal» — сумма/среднее (число float в ячейке + формат с плавающей точкой),
    «plain» — прочие строки (целые счётчики и т.д.).
    """
    out: List[Tuple[str, Any, str]] = []
    n = len(data_rows)
    out.append(("Общее количество строк", n, "plain"))
    if n == 0:
        return out

    names = [str(h) for h in header]
    amount_indices = [j for j, h in enumerate(names) if "amount" in h.lower()]
    date_indices = [j for j, h in enumerate(names) if _is_date_column(h)]
    other_indices = [j for j in range(len(names)) if j not in amount_indices]

    for j in amount_indices:
        vals: List[float] = []
        for row in data_rows:
            if j >= len(row):
                continue
            ok, v = _try_float(row[j])
            if ok and row[j] not in (None, ""):
                vals.append(v)
        col = names[j]
        if vals:
            out.append((f"{col} — сумма", round(float(sum(vals)), 4), "decimal"))
            out.append(
                (
                    f"{col} — среднее",
                    round(float(sum(vals) / len(vals)), 4),
                    "decimal",
                )
            )

    for j in other_indices:
        uniq: set[str] = set()
        for row in data_rows:
            if j < len(row) and row[j] not in (None, ""):
                uniq.add(str(row[j]).strip())
        out.append((f"{names[j]} — уникальных значений", len(uniq), "plain"))

    if amount_indices and date_indices:
        for d_idx in date_indices:
            buckets: Dict[Tuple[int, int], Dict[int, List[float]]] = defaultdict(
                lambda: defaultdict(list)
            )
            for row in data_rows:
                if d_idx >= len(row):
                    continue
                mk = _month_key(row[d_idx])
                if mk is None:
                    continue
                for a_idx in amount_indices:
                    if a_idx >= len(row):
                        continue
                    ok, v = _try_float(row[a_idx])
                    if ok and row[a_idx] not in (None, ""):
                        buckets[mk][a_idx].append(v)
            dname = names[d_idx]
            for mk in sorted(buckets.keys()):
                y, m = mk
                prefix = f"По месяцам ({dname}): {y:04d}-{m:02d}"
                for a_idx in amount_indices:
                    vs = buckets[mk].get(a_idx, [])
                    aname = names[a_idx]
                    if vs:
                        out.append(
                            (f"{prefix} | {aname} — сумма", round(float(sum(vs)), 4), "decimal")
                        )
                        out.append(
                            (
                                f"{prefix} | {aname} — среднее",
                                round(float(sum(vs) / len(vs)), 4),
                                "decimal",
                            )
                        )

    return out


def build_sheet_matrix(
    report_title: str,
    header: Sequence[str],
    data_rows: Sequence[Sequence[Any]],
) -> Tuple[List[List[Any]], int, int, int, int, int, List[Tuple[str, Any, str]]]:
    """
    Возвращает (values, n_cols, title_row, header_row, data_start, n_data_rows, analytics_rows).
    Строки 0-based индексы в grid.
    """
    n_cols = max(len(header), 1)
    analytics = build_analytics(header, data_rows)

    grid: List[List[Any]] = []

    def pad_row(r: List[Any]) -> List[Any]:
        row = list(r)
        while len(row) < n_cols:
            row.append("")
        return row[:n_cols]

    title_row = 0
    grid.append(pad_row([report_title]))

    grid.append(pad_row([]))

    grid.append(pad_row(["Показатель", "Значение"]))

    for _label, _val, _tag in analytics:
        grid.append(pad_row([_label, _val]))

    grid.append(pad_row([]))

    header_row = len(grid)
    grid.append(pad_row(list(header)))

    data_start = len(grid)
    for row in data_rows:
        grid.append(pad_row(list(row)))
    n_data_rows = len(data_rows)

    return grid, n_cols, title_row, header_row, data_start, n_data_rows, analytics


def build_format_requests(
    sheet_id: int,
    n_cols: int,
    title_row: int,
    header_row: int,
    data_start: int,
    n_data_rows: int,
    analytics_rows: Sequence[Tuple[str, Any, str]],
) -> List[Dict[str, Any]]:
    """Сборка batchUpdate для оформления отчёта (публичный API: batch_update)."""
    col_excl = n_cols
    data_end_excl = data_start + max(n_data_rows, 0)
    last_row_excl = max(header_row + 1, data_end_excl)
    border_color = _color(0.65, 0.65, 0.68)
    solid = {"style": "SOLID", "width": 1, "color": border_color}

    reqs: List[Dict[str, Any]] = []

    reqs.append(
        {
            "mergeCells": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": title_row,
                    "endRowIndex": title_row + 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": col_excl,
                },
                "mergeType": "MERGE_ALL",
            }
        }
    )

    reqs.append(
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": title_row,
                    "endRowIndex": title_row + 1,
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
                            "fontSize": 12,
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

    # Блок аналитики: 0 заголовок отчёта, 1 пустая, 2 шапка «Показатель/Значение», 3.. данные, пустая, шапка основной таблицы
    analytics_header_row = title_row + 2
    analytics_body_start = title_row + 3
    analytics_block_end_excl = header_row - 1  # строка перед пустой разделительной (не включаем пустую)
    acol_excl = min(2, col_excl)

    if acol_excl >= 2 and analytics_block_end_excl > analytics_header_row:
        reqs.append(
            {
                "updateBorders": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": analytics_header_row,
                        "endRowIndex": analytics_block_end_excl,
                        "startColumnIndex": 0,
                        "endColumnIndex": acol_excl,
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

    if acol_excl >= 2 and analytics_block_end_excl > analytics_header_row:
        reqs.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": analytics_header_row,
                        "endRowIndex": analytics_header_row + 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": acol_excl,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": _color(0.78, 0.84, 0.93),
                            "horizontalAlignment": "CENTER",
                            "verticalAlignment": "MIDDLE",
                            "wrapStrategy": "WRAP",
                            "textFormat": {
                                "bold": True,
                                "fontSize": 11,
                                "foregroundColor": _color(0.12, 0.18, 0.35),
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

    if analytics_block_end_excl > analytics_body_start and acol_excl >= 1:
        reqs.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": analytics_body_start,
                        "endRowIndex": analytics_block_end_excl,
                        "startColumnIndex": 0,
                        "endColumnIndex": 1,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "horizontalAlignment": "LEFT",
                            "verticalAlignment": "MIDDLE",
                            "wrapStrategy": "WRAP",
                            "textFormat": {"fontSize": 10},
                        }
                    },
                    "fields": (
                        "userEnteredFormat(horizontalAlignment,verticalAlignment,"
                        "wrapStrategy,textFormat)"
                    ),
                }
            }
        )

    if analytics_block_end_excl > analytics_body_start and acol_excl >= 2:
        reqs.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": analytics_body_start,
                        "endRowIndex": analytics_block_end_excl,
                        "startColumnIndex": 1,
                        "endColumnIndex": 2,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "horizontalAlignment": "CENTER",
                            "verticalAlignment": "MIDDLE",
                            "wrapStrategy": "WRAP",
                            "textFormat": {"fontSize": 10},
                        }
                    },
                    "fields": (
                        "userEnteredFormat(horizontalAlignment,verticalAlignment,"
                        "wrapStrategy,textFormat)"
                    ),
                }
            }
        )

    # Суммы и средние: числовой формат (столбец B по-прежнему центрируется)
    if col_excl >= 2:
        for i, (_lab, _val, tag) in enumerate(analytics_rows):
            if tag != "decimal":
                continue
            row_idx = analytics_body_start + i
            reqs.append(
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": row_idx,
                            "endRowIndex": row_idx + 1,
                            "startColumnIndex": 1,
                            "endColumnIndex": 2,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "horizontalAlignment": "CENTER",
                                "verticalAlignment": "MIDDLE",
                                "numberFormat": {
                                    "type": "NUMBER",
                                    "pattern": "#,##0.####",
                                },
                            }
                        },
                        "fields": (
                            "userEnteredFormat(numberFormat,horizontalAlignment,verticalAlignment)"
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

    if n_data_rows > 0:
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
                        "startRowIndex": data_start,
                        "endRowIndex": data_end_excl,
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

    widths = []
    for c in range(n_cols):
        if c == 0:
            widths.append(420)
        elif c == 1:
            widths.append(180)
        else:
            widths.append(min(280, max(96, 14 * 6)))
    for i, px in enumerate(widths[:n_cols]):
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
                    "startIndex": title_row,
                    "endIndex": title_row + 1,
                },
                "properties": {"pixelSize": 40},
                "fields": "pixelSize",
            }
        }
    )

    if analytics_block_end_excl > analytics_header_row:
        reqs.append(
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": analytics_header_row,
                        "endIndex": analytics_header_row + 1,
                    },
                    "properties": {"pixelSize": 36},
                    "fields": "pixelSize",
                }
            }
        )

    return reqs


def resolve_project_path(root: Path, p: str) -> Path:
    raw = (p or "").strip()
    if not raw:
        return Path()
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = (root / path).resolve()
    return path


def export_table_to_google_sheet(
    *,
    project_root: Path,
    service_account_json: Path,
    oauth_client_secret: Path,
    token_path: Path,
    report_folder_id: str,
    table_label: str,
    header: Sequence[str],
    data_rows: Sequence[Sequence[Any]],
) -> str:
    """
    Создаёт таблицу в Drive (OAuth), заполняет и форматирует через Sheets (SA).
    Возвращает URL для открытия в браузере.
    """
    safe_label = "".join(ch if ch.isalnum() or ch in " _-" else "_" for ch in table_label)[:40]
    title = f"CRM_{safe_label}_{datetime.now():%Y-%m-%d_%H-%M-%S}"[:99]

    report_heading = f"Отчёт CRM — {table_label}"
    logger.info("Выгрузка в Google: лист «%s», строк данных: %s", table_label, len(data_rows))
    try:
        values, n_cols, title_row, header_row, data_start, n_data, analytics_rows = (
            build_sheet_matrix(report_heading, header, data_rows)
        )

        logger.debug("Создание файла на Drive через OAuth…")
        drive = GoogleDriveClient(
            credentials_client_path=oauth_client_secret,
            report_folder_id=report_folder_id.strip(),
            token_path=token_path,
        )
        file_id, created_name = drive.create_google_sheet(title)
        logger.info("Создан файл на Drive: %s (%s)", created_name, file_id)

        logger.debug("Запись в Sheets через сервисный аккаунт…")
        sheets = GoogleSheetsClient(
            spreadsheet_id=file_id,
            credentials_path=service_account_json,
        )
        end_letter = col_letter_1based(n_cols)
        last_row = len(values)
        rng = f"A1:{end_letter}{last_row}"
        sheets.write_range(rng, values)

        internal_sheet_id = sheets._sheet_id(None)
        reqs = build_format_requests(
            internal_sheet_id,
            n_cols,
            title_row,
            header_row,
            data_start,
            n_data,
            analytics_rows,
        )
        sheets.batch_update(reqs)
        logger.info("Выгрузка завершена: %s", file_id)
        return f"https://docs.google.com/spreadsheets/d/{file_id}/edit"
    except FileNotFoundError as e:
        logger.error("Файл учётных данных не найден: %s", e)
        raise RuntimeError(
            f"Не найден файл ключей Google: {e}. Проверьте пути в «Настройки Google» и .env."
        ) from e
    except ValueError as e:
        logger.error("Некорректные параметры Google: %s", e)
        raise RuntimeError(
            f"Проверьте настройки Google (папка, пути к JSON): {e}"
        ) from e
    except GoogleAuthError as e:
        logger.error("Ошибка авторизации Google: %s", e)
        raise RuntimeError(
            f"Ошибка входа Google (OAuth или сервисный аккаунт): {e}"
        ) from e
    except HttpError as e:
        msg = _user_message_for_http_error("Запрос к Google API", e)
        logger.error("%s", msg)
        raise RuntimeError(msg) from e
    except OSError as e:
        logger.error("Ошибка ОС при выгрузке: %s", e)
        raise RuntimeError(f"Не удалось прочитать/записать файлы (токен, ключи): {e}") from e
