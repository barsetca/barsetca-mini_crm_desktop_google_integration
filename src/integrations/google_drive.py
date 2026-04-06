"""
Google Drive CRUD через OAuth2 (личный аккаунт пользователя).

Поддерживаются операции:
- создание Google Docs / Google Sheets в указанной папке;
- удаление файла;
- переименование файла;
- получение списка документов и таблиц в папке.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
from google.auth.exceptions import GoogleAuthError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

load_dotenv()

DEFAULT_CLIENT_CREDENTIALS = "client_secret.json"
DEFAULT_TOKEN_PATH = "token_drive.json"
DEFAULT_REPORT_FOLDER_ID = ""
SCOPES = ("https://www.googleapis.com/auth/drive",)

MIME_GOOGLE_DOC = "application/vnd.google-apps.document"
MIME_GOOGLE_SHEET = "application/vnd.google-apps.spreadsheet"

logger = logging.getLogger(__name__)


class GoogleDriveClient:
    """
    Клиент Google Drive API (личный аккаунт через OAuth2).

    Для работы нужны:
    - `CREDENTIALS_CLIENT_PATH` (JSON client secret OAuth2),
    - `REPORT_FOLDER_ID` (ID папки на Drive).
    """

    def __init__(
        self,
        credentials_client_path: Optional[str | Path] = None,
        report_folder_id: Optional[str] = None,
        token_path: Optional[str | Path] = None,
    ) -> None:
        root = Path(__file__).resolve().parent
        client_name = os.getenv("CREDENTIALS_CLIENT_PATH", DEFAULT_CLIENT_CREDENTIALS)
        token_name = os.getenv("TOKEN_PATH", DEFAULT_TOKEN_PATH)

        self._client_secret_path = Path(credentials_client_path or (root / client_name))
        self._token_path = Path(token_path or (root / token_name))
        self.report_folder_id = (report_folder_id or os.getenv("REPORT_FOLDER_ID", "")).strip()

        if not self.report_folder_id:
            raise ValueError(
                "Не задан REPORT_FOLDER_ID. Укажите ID папки в .env или аргументе report_folder_id."
            )

        self._service = self._build_service()

    def _build_service(self):
        """Создает сервис Drive API с OAuth2 авторизацией пользователя."""
        if not self._client_secret_path.is_file():
            msg = f"Файл OAuth2 client secret не найден: {self._client_secret_path}"
            logger.error(msg)
            raise FileNotFoundError(msg)

        creds: Optional[Credentials] = None
        try:
            if self._token_path.exists():
                creds = Credentials.from_authorized_user_file(str(self._token_path), SCOPES)

            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                else:
                    flow = InstalledAppFlow.from_client_secrets_file(
                        str(self._client_secret_path),
                        SCOPES,
                    )
                    creds = flow.run_local_server(port=0)
                self._token_path.write_text(creds.to_json(), encoding="utf-8")

            return build("drive", "v3", credentials=creds, cache_discovery=False)
        except GoogleAuthError as e:
            self._log_auth_error(e)
            raise
        except Exception as e:
            self._log_unexpected("Ошибка инициализации Drive API", e)
            raise

    @staticmethod
    def _log_http_error(context: str, e: HttpError) -> None:
        status = e.resp.status if getattr(e, "resp", None) else "?"
        detail = ""
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
    def _log_auth_error(e: GoogleAuthError) -> None:
        text = f"Ошибка OAuth2 аутентификации: {e}"
        logger.error(text)
        print(text, file=sys.stderr)

    @staticmethod
    def _log_unexpected(context: str, e: Exception) -> None:
        text = f"{context}: {type(e).__name__}: {e}"
        logger.error(text)
        print(text, file=sys.stderr)

    def list_google_files(self, folder_id: Optional[str] = None) -> List[Dict[str, str]]:
        """
        Возвращает список Google Docs и Google Sheets в указанной папке.

        Формат элемента:
        {"id": "...", "name": "...", "mimeType": "..."}
        """
        target_folder = (folder_id or self.report_folder_id).strip()
        query = (
            f"'{target_folder}' in parents and trashed = false and ("
            f"mimeType = '{MIME_GOOGLE_DOC}' or mimeType = '{MIME_GOOGLE_SHEET}')"
        )
        try:
            items: List[Dict[str, str]] = []
            page_token: Optional[str] = None
            while True:
                resp = (
                    self._service.files()
                    .list(
                        q=query,
                        fields="nextPageToken, files(id, name, mimeType)",
                        pageToken=page_token,
                        pageSize=1000,
                        supportsAllDrives=True,
                        includeItemsFromAllDrives=True,
                    )
                    .execute()
                )
                files = resp.get("files", [])
                for item in files:
                    items.append(
                        {
                            "id": item.get("id", ""),
                            "name": item.get("name", ""),
                            "mimeType": item.get("mimeType", ""),
                        }
                    )
                page_token = resp.get("nextPageToken")
                if not page_token:
                    break

            items.sort(key=lambda x: (x["mimeType"], x["name"].lower()))
            logger.info("Найдено файлов в папке %s: %s", target_folder, len(items))
            return items
        except HttpError as e:
            self._log_http_error("Не удалось получить список файлов", e)
            raise
        except Exception as e:
            self._log_unexpected("Ошибка получения списка файлов", e)
            raise

    def list_google_doc_names(self, folder_id: Optional[str] = None) -> List[str]:
        """Список имен только Google документов из папки."""
        files = self.list_google_files(folder_id)
        return [f["name"] for f in files if f["mimeType"] == MIME_GOOGLE_DOC]

    def list_google_sheet_names(self, folder_id: Optional[str] = None) -> List[str]:
        """Список имен только Google таблиц из папки."""
        files = self.list_google_files(folder_id)
        return [f["name"] for f in files if f["mimeType"] == MIME_GOOGLE_SHEET]

    def create_google_doc(self, name: str, folder_id: Optional[str] = None) -> Tuple[str, str]:
        """Создает Google Документ в папке. Возвращает (file_id, file_name)."""
        return self._create_google_file(name, MIME_GOOGLE_DOC, folder_id)

    def create_google_sheet(
        self,
        name: str,
        folder_id: Optional[str] = None,
    ) -> Tuple[str, str]:
        """Создает Google Таблицу в папке. Возвращает (file_id, file_name)."""
        return self._create_google_file(name, MIME_GOOGLE_SHEET, folder_id)

    def _create_google_file(
        self,
        name: str,
        mime_type: str,
        folder_id: Optional[str] = None,
    ) -> Tuple[str, str]:
        target_folder = (folder_id or self.report_folder_id).strip()
        body = {
            "name": name.strip() or "Новый файл",
            "mimeType": mime_type,
            "parents": [target_folder],
        }
        try:
            created = (
                self._service.files()
                .create(
                    body=body,
                    fields="id,name,mimeType",
                    supportsAllDrives=True,
                )
                .execute()
            )
            file_id = created.get("id", "")
            file_name = created.get("name", "")
            logger.info("Создан файл %s (%s)", file_name, file_id)
            return file_id, file_name
        except HttpError as e:
            self._log_http_error("Не удалось создать файл", e)
            raise
        except Exception as e:
            self._log_unexpected("Ошибка создания файла", e)
            raise

    def rename_file(self, file_id: str, new_name: str) -> Dict[str, str]:
        """Переименовывает файл по ID. Возвращает обновленные данные файла."""
        clean_id = file_id.strip()
        clean_name = new_name.strip()
        if not clean_id:
            raise ValueError("file_id не должен быть пустым.")
        if not clean_name:
            raise ValueError("new_name не должен быть пустым.")
        try:
            updated = (
                self._service.files()
                .update(
                    fileId=clean_id,
                    body={"name": clean_name},
                    fields="id,name,mimeType",
                    supportsAllDrives=True,
                )
                .execute()
            )
            out = {
                "id": updated.get("id", ""),
                "name": updated.get("name", ""),
                "mimeType": updated.get("mimeType", ""),
            }
            logger.info("Переименован файл %s -> %s", clean_id, out["name"])
            return out
        except HttpError as e:
            self._log_http_error(f"Не удалось переименовать файл {clean_id}", e)
            raise
        except Exception as e:
            self._log_unexpected("Ошибка переименования файла", e)
            raise

    def delete_file(self, file_id: str) -> None:
        """Удаляет файл по ID (перемещает в корзину/удаляет в рамках API-политик)."""
        clean_id = file_id.strip()
        if not clean_id:
            raise ValueError("file_id не должен быть пустым.")
        try:
            self._service.files().delete(
                fileId=clean_id,
                supportsAllDrives=True,
            ).execute()
            logger.info("Удален файл %s", clean_id)
        except HttpError as e:
            self._log_http_error(f"Не удалось удалить файл {clean_id}", e)
            raise
        except Exception as e:
            self._log_unexpected("Ошибка удаления файла", e)
            raise


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


if __name__ == "__main__":
    _configure_logging()
    try:
        client = GoogleDriveClient()
        files = client.list_google_files()
        if not files:
            print("В папке не найдено Google Документов/Таблиц.")
        else:
            print("Файлы в рабочей папке (Google Docs + Google Sheets):")
            for idx, f in enumerate(files, start=1):
                kind = "DOC" if f["mimeType"] == MIME_GOOGLE_DOC else "SHEET"
                print(f"  {idx}. [{kind}] {f['name']} (id={f['id']})")
    except FileNotFoundError as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
    except ValueError as e:
        print(f"Ошибка параметров: {e}", file=sys.stderr)
        sys.exit(1)
    except (GoogleAuthError, HttpError):
        sys.exit(1)
    except Exception as e:
        print(f"Неожиданная ошибка: {e}", file=sys.stderr)
        sys.exit(1)
