"""
Google Sheets CRUD через сервисный аккаунт (Google Sheets API v4).
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from dotenv import load_dotenv
from google.auth.exceptions import GoogleAuthError
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

load_dotenv()

DEFAULT_CREDENTIALS = "my-zerocoder-project-da0be47bed0c.json"
DEFAULT_SPREADSHEET_ID = "1aVBJJ8EGxfXVVaE-BT2I3ZfZkH79OUOD"
SCOPES = ("https://www.googleapis.com/auth/spreadsheets",)

logger = logging.getLogger(__name__)


def _normalize_cell_value(value: Any) -> Any:
    """Убирает неразрывный пробел из строк (формат валюты в таблицах).

    Пример:
        _normalize_cell_value("1\u00a0000")  # -> "1000"
        _normalize_cell_value(42)  # без изменений
    """
    if isinstance(value, str):
        return value.replace("\xa0", "")
    return value


def _normalize_grid(values: Optional[List[List[Any]]]) -> List[List[Any]]:
    """Нормализует все строковые ячейки сетки (убирает \\xa0).

    Пример:
        _normalize_grid([["a\u00a0b", 1], ["x"]])  # -> [["ab", 1], ["x"]]
    """
    if not values:
        return []
    return [[_normalize_cell_value(c) for c in row] for row in values]


class GoogleSheetsClient:
    """
    Клиент для чтения и изменения одной таблицы.
    spreadsheet_id и путь к ключу берутся из .env или аргументов конструктора.

    Пример:
        client = GoogleSheetsClient()
        client = GoogleSheetsClient(
            spreadsheet_id="1AbC...xyz",
            credentials_path="/path/to/service-account.json",
        )
    """

    def __init__(
        self,
        spreadsheet_id: Optional[str] = None,
        credentials_path: Optional[Union[str, Path]] = None,
    ) -> None:
        """Создаёт клиент и подключается к API.

        Пример:
            GoogleSheetsClient()
            GoogleSheetsClient(spreadsheet_id=os.environ["SPREADSHEET_ID"])
        """
        root = Path(__file__).resolve().parent
        cred_name = os.getenv("CREDENTIALS_PATH", DEFAULT_CREDENTIALS)
        self._credentials_path = Path(credentials_path or (root / cred_name))
        self.spreadsheet_id = spreadsheet_id or os.getenv(
            "SPREADSHEET_ID", DEFAULT_SPREADSHEET_ID
        )
        self._service = self._build_service()

    def _build_service(self):
        """Собирает объект сервиса Sheets API (вызывается из __init__).

        Пример:
            # напрямую не вызывается; клиент: GoogleSheetsClient()
        """
        if not self._credentials_path.is_file():
            msg = (
                f"Файл ключа сервисного аккаунта не найден: {self._credentials_path}"
            )
            logger.error(msg)
            raise FileNotFoundError(msg)
        try:
            creds = service_account.Credentials.from_service_account_file(
                str(self._credentials_path), scopes=SCOPES
            )
            return build("sheets", "v4", credentials=creds, cache_discovery=False)
        except GoogleAuthError as e:
            self._log_auth_error(e)
            raise
        except Exception as e:
            self._log_unexpected("Ошибка при создании клиента API", e)
            raise

    @staticmethod
    def _log_http_error(context: str, e: HttpError) -> None:
        """Пишет в лог и stderr описание ошибки HTTP API.

        Пример:
            GoogleSheetsClient._log_http_error("Контекст", http_error)
        """
        status = e.resp.status if getattr(e, "resp", None) else "?"
        try:
            detail = (
                e.content.decode("utf-8", errors="replace")
                if getattr(e, "content", None)
                else str(e)
            )
        except Exception:
            detail = str(e)
        text = f"{context}: HTTP {status} — {detail}"
        logger.error(text)
        print(text, file=sys.stderr)

    @staticmethod
    def _log_unexpected(context: str, e: Exception) -> None:
        """Логирует непредвиденное исключение.

        Пример:
            GoogleSheetsClient._log_unexpected("Операция X", exc)
        """
        text = f"{context}: {type(e).__name__}: {e}"
        logger.error(text)
        print(text, file=sys.stderr)

    @staticmethod
    def _log_auth_error(e: GoogleAuthError) -> None:
        """Логирует ошибку аутентификации Google.

        Пример:
            GoogleSheetsClient._log_auth_error(auth_exc)
        """
        text = f"Ошибка аутентификации Google: {e}"
        logger.error(text)
        print(text, file=sys.stderr)

    def _spreadsheet_metadata(self) -> dict:
        """Загружает метаданные таблицы (листы, id и т.д.).

        Пример:
            meta = client._spreadsheet_metadata()
        """
        try:
            return (
                self._service.spreadsheets()
                .get(spreadsheetId=self.spreadsheet_id)
                .execute()
            )
        except HttpError as e:
            self._log_http_error(
                f"Не удалось открыть таблицу (spreadsheet_id={self.spreadsheet_id})",
                e,
            )
            raise
        except Exception as e:
            self._log_unexpected("Ошибка при получении метаданных таблицы", e)
            raise

    def _resolve_sheet_title(self, sheet_name: Optional[str]) -> str:
        """Возвращает название листа: первый при None или проверка имени.

        Пример:
            title = client._resolve_sheet_title(None)
            title = client._resolve_sheet_title("Лист1")
        """
        meta = self._spreadsheet_metadata()
        sheets = meta.get("sheets", [])
        if not sheets:
            raise ValueError("В таблице нет листов.")
        if sheet_name is None:
            title = sheets[0].get("properties", {}).get("title")
            if not title:
                raise ValueError("Не удалось определить первый лист.")
            return title
        titles = {
            s.get("properties", {}).get("title")
            for s in sheets
            if s.get("properties", {}).get("title")
        }
        if sheet_name not in titles:
            raise ValueError(
                f"Лист «{sheet_name}» не найден. Доступные: {sorted(titles)}"
            )
        return sheet_name

    def _sheet_id(self, sheet_name: Optional[str]) -> int:
        """Числовой sheetId листа для batchUpdate.

        Пример:
            sid = client._sheet_id("Лист1")
            sid = client._sheet_id(None)
        """
        title = self._resolve_sheet_title(sheet_name)
        meta = self._spreadsheet_metadata()
        for s in meta.get("sheets", []):
            props = s.get("properties", {})
            if props.get("title") == title:
                sid = props.get("sheetId")
                if sid is not None:
                    return int(sid)
        raise ValueError(f"Не найден sheetId для листа «{title}».")

    def _a1(self, sheet_name: Optional[str], cell_or_range: str) -> str:
        """Собирает полный A1-диапазон с экранированием имени листа.

        Пример:
            rng = client._a1(None, "A1")
            rng = client._a1("Данные", "B2:D10")
        """
        title = self._resolve_sheet_title(sheet_name)
        if "'" in title:
            safe = "'" + title.replace("'", "''") + "'"
        else:
            safe = title
        return f"{safe}!{cell_or_range}"

    def read_all_values(self, sheet_name: Optional[str] = None) -> List[List[Any]]:
        """Читает все заполненные ячейки листа (диапазон = имя листа).

        Пример:
            rows = client.read_all_values()
            rows = client.read_all_values("Отчёт")
        """
        title = self._resolve_sheet_title(sheet_name)
        try:
            result = (
                self._service.spreadsheets()
                .values()
                .get(spreadsheetId=self.spreadsheet_id, range=title)
                .execute()
            )
            raw = result.get("values", [])
            out = _normalize_grid(raw)
            logger.info("Прочитан лист «%s», строк: %s", title, len(out))
            return out
        except HttpError as e:
            self._log_http_error(f"Не удалось прочитать лист «{title}»", e)
            raise
        except Exception as e:
            self._log_unexpected("Ошибка чтения таблицы", e)
            raise

    def update_cell(
        self,
        cell_a1: str,
        value: Any,
        sheet_name: Optional[str] = None,
    ) -> None:
        """Записывает значение в одну ячейку (например, A1).

        Пример:
            client.update_cell("A1", "Заголовок")
            client.update_cell("B2", 100, sheet_name="Лист1")
        """
        rng = self._a1(sheet_name, cell_a1)
        body = {"values": [[value]]}
        try:
            self._service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=rng,
                valueInputOption="USER_ENTERED",
                body=body,
            ).execute()
            logger.info("Обновлена ячейка %s", rng)
        except HttpError as e:
            self._log_http_error(f"Не удалось записать ячейку {rng}", e)
            raise
        except Exception as e:
            self._log_unexpected("Ошибка записи ячейки", e)
            raise

    def write_range(
        self,
        range_a1: str,
        values: Sequence[Sequence[Any]],
        sheet_name: Optional[str] = None,
    ) -> None:
        """
        Записывает данные в диапазон (например, A1:C3).
        values — список строк; каждая строка — список значений ячеек.

        Пример:
            client.write_range(
                "A1:C2",
                [["Имя", "Год", "Город"], ["Иван", 2024, "Москва"]],
            )
            client.write_range("D1:E2", [[1, 2], [3, 4]], sheet_name="Данные")
        """
        rng = self._a1(sheet_name, range_a1)
        rows = [list(row) for row in values]
        body = {"values": rows}
        try:
            self._service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=rng,
                valueInputOption="USER_ENTERED",
                body=body,
            ).execute()
            logger.info("Записан диапазон %s (%s строк)", rng, len(rows))
        except HttpError as e:
            self._log_http_error(f"Не удалось записать диапазон {rng}", e)
            raise
        except Exception as e:
            self._log_unexpected("Ошибка записи диапазона", e)
            raise

    def clear_range(self, range_a1: str, sheet_name: Optional[str] = None) -> None:
        """Очищает значения в указанном диапазоне.

        Пример:
            client.clear_range("A1:Z100")
            client.clear_range("B2:D5", sheet_name="Черновик")
        """
        rng = self._a1(sheet_name, range_a1)
        try:
            self._service.spreadsheets().values().clear(
                spreadsheetId=self.spreadsheet_id,
                range=rng,
                body={},
            ).execute()
            logger.info("Очищен диапазон %s", rng)
        except HttpError as e:
            self._log_http_error(f"Не удалось очистить диапазон {rng}", e)
            raise
        except Exception as e:
            self._log_unexpected("Ошибка очистки диапазона", e)
            raise

    def append_row(
        self,
        values: Sequence[Any],
        sheet_name: Optional[str] = None,
    ) -> None:
        """Добавляет одну строку в конец таблицы на листе (удобно для пакетной записи).

        Пример:
            client.append_row(["2024-01-01", 1500, "готово"])
            client.append_row([1, 2, 3], sheet_name="Лог")
        """
        title = self._resolve_sheet_title(sheet_name)
        rng = self._a1(sheet_name, "A1")
        body = {"values": [list(values)]}
        try:
            self._service.spreadsheets().values().append(
                spreadsheetId=self.spreadsheet_id,
                range=rng,
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body=body,
            ).execute()
            logger.info("Добавлена строка на лист «%s»", title)
        except HttpError as e:
            self._log_http_error(f"Не удалось добавить строку на «{title}»", e)
            raise
        except Exception as e:
            self._log_unexpected("Ошибка добавления строки", e)
            raise

    def delete_row(
        self,
        row_index_1based: int,
        sheet_name: Optional[str] = None,
    ) -> None:
        """Удаляет строку по номеру (1 — первая строка на листе).

        Пример:
            client.delete_row(5)
            client.delete_row(2, sheet_name="Импорт")
        """
        if row_index_1based < 1:
            raise ValueError("Номер строки должен быть не меньше 1.")
        sheet_id = self._sheet_id(sheet_name)
        start = row_index_1based - 1
        body = {
            "requests": [
                {
                    "deleteDimension": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "ROWS",
                            "startIndex": start,
                            "endIndex": start + 1,
                        }
                    }
                }
            ]
        }
        try:
            self._service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id, body=body
            ).execute()
            logger.info("Удалена строка %s (лист sheetId=%s)", row_index_1based, sheet_id)
        except HttpError as e:
            self._log_http_error("Не удалось удалить строку", e)
            raise
        except Exception as e:
            self._log_unexpected("Ошибка удаления строки", e)
            raise

    def delete_sheet(self, sheet_name: str) -> None:
        """Удаляет лист по имени.

        Пример:
            client.delete_sheet("Временный")
        """
        sheet_id = self._sheet_id(sheet_name)
        body = {"requests": [{"deleteSheet": {"sheetId": sheet_id}}]}
        try:
            self._service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id, body=body
            ).execute()
            logger.info("Удалён лист «%s» (sheetId=%s)", sheet_name, sheet_id)
        except HttpError as e:
            self._log_http_error(f"Не удалось удалить лист «{sheet_name}»", e)
            raise
        except Exception as e:
            self._log_unexpected("Ошибка удаления листа", e)
            raise

    def add_sheet(self, title: str) -> Tuple[str, int]:
        """Добавляет новый лист. Возвращает (точное имя листа, sheetId).

        Пример:
            name, sid = client.add_sheet("Отчёт_2026-04-03")
        """
        body = {"requests": [{"addSheet": {"properties": {"title": title}}}]}
        try:
            resp = (
                self._service.spreadsheets()
                .batchUpdate(spreadsheetId=self.spreadsheet_id, body=body)
                .execute()
            )
            reply = resp["replies"][0]["addSheet"]
            props = reply["properties"]
            stitle = props["title"]
            sid = int(props["sheetId"])
            logger.info("Добавлен лист «%s» (sheetId=%s)", stitle, sid)
            return stitle, sid
        except HttpError as e:
            self._log_http_error(f"Не удалось добавить лист «{title}»", e)
            raise
        except Exception as e:
            self._log_unexpected("Ошибка добавления листа", e)
            raise

    def batch_update(self, requests: Sequence[Dict[str, Any]]) -> None:
        """Произвольный batchUpdate (форматирование, merge, размеры столбцов и т.д.).

        Пример:
            client.batch_update([
                {"mergeCells": {"range": {...}, "mergeType": "MERGE_ALL"}},
            ])
        """
        if not requests:
            return
        body = {"requests": list(requests)}
        try:
            self._service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id, body=body
            ).execute()
            logger.info("Выполнен batchUpdate (%s запросов)", len(requests))
        except HttpError as e:
            self._log_http_error("Ошибка batchUpdate", e)
            raise
        except Exception as e:
            self._log_unexpected("Ошибка batchUpdate", e)
            raise


def _configure_logging() -> None:
    """Настраивает базовый вывод логов (используется в точке входа модуля).

    Пример:
        _configure_logging()
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


if __name__ == "__main__":
    _configure_logging()
    try:
        client = GoogleSheetsClient()
        data = client.read_all_values()
        print("Содержимое первого листа (после нормализации \\xa0):")
        for i, row in enumerate(data, start=1):
            print(f"  {i}: {row}")
    except FileNotFoundError as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
    except ValueError as e:
        print(f"Ошибка параметров или данных: {e}", file=sys.stderr)
        sys.exit(1)
    except HttpError:
        sys.exit(1)
    except GoogleAuthError:
        sys.exit(1)
    except Exception as e:
        print(f"Неожиданная ошибка: {e}", file=sys.stderr)
        sys.exit(1)
