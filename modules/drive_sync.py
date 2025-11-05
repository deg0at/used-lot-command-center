"""Utilities for synchronizing Google Drive inventory and Carfax assets."""

from __future__ import annotations

import io
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, Mapping, Optional, Tuple

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

SYNC_SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
SUPPORTED_INVENTORY_EXTENSIONS = {".csv", ".xls", ".xlsx"}
SUPPORTED_CARFAX_EXTENSIONS = {".pdf"}


@dataclass
class SyncOutcome:
    downloaded: int = 0
    skipped: int = 0
    errors: Tuple[str, ...] = ()

    def as_status_message(self) -> str:
        base = f"Downloaded {self.downloaded} file{'s' if self.downloaded != 1 else ''}"
        if self.skipped:
            base += f", skipped {self.skipped} up-to-date"
        if self.errors:
            base += f" (encountered {len(self.errors)} error{'s' if len(self.errors) != 1 else ''})"
        return base


def _load_service_account_info(secrets: Mapping[str, object]) -> Optional[Dict[str, object]]:
    """Return the service-account info dict from Streamlit secrets/env variables."""

    if "google_service_account" in secrets:
        raw_secret = secrets["google_service_account"]
        # Streamlit secrets behave like a config object and expose mapping-like
        # access. However, some deployments supply the value as a JSON string.
        # Guard against non-mapping types before coercing to ``dict`` to avoid
        # ``TypeError: dictionary update sequence element ...`` when ``dict``
        # receives a plain string or other iterables.
        if isinstance(raw_secret, Mapping):
            info = dict(raw_secret)
            if info:
                return info
        elif isinstance(raw_secret, str):
            raw_secret = raw_secret.strip()
            if raw_secret:
                try:
                    info = json.loads(raw_secret)
                    if isinstance(info, dict) and info:
                        return info
                except json.JSONDecodeError:
                    logging.warning(
                        "google_service_account secret is not valid JSON; ignoring."
                    )
        else:
            logging.warning(
                "google_service_account secret is of unsupported type %s; ignoring.",
                type(raw_secret).__name__,
            )

    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not raw:
        return None

    if os.path.isfile(raw):
        with open(raw, "r", encoding="utf-8") as f:
            return json.load(f)

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        logging.warning("GOOGLE_SERVICE_ACCOUNT_JSON env var is neither JSON nor file path; ignoring.")
        return None


def _safe_filename(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", name.strip())
    return cleaned or "file"


def _parse_modified_time(value: str) -> datetime:
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value)
    except Exception:
        return datetime.utcnow()


def _load_index(path: str) -> Dict[str, Dict[str, str]]:
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_index(path: str, data: Dict[str, Dict[str, str]]):
    if not path:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)


def _iter_drive_files(service, folder_id: str) -> Iterable[Dict[str, str]]:
    page_token = None
    fields = "nextPageToken, files(id, name, mimeType, modifiedTime)"
    query = f"'{folder_id}' in parents and trashed = false"
    while True:
        response = service.files().list(
            q=query,
            spaces="drive",
            fields=fields,
            pageSize=1000,
            pageToken=page_token,
        ).execute()
        for file in response.get("files", []):
            yield file
        page_token = response.get("nextPageToken")
        if not page_token:
            break


def _resolve_target_dir(filename: str, listings_dir: str, carfax_dir: str) -> Optional[str]:
    ext = os.path.splitext(filename)[1].lower()
    if ext in SUPPORTED_INVENTORY_EXTENSIONS:
        return listings_dir
    if ext in SUPPORTED_CARFAX_EXTENSIONS:
        return carfax_dir
    return None


def sync_google_drive_folder(
    *,
    folder_id: str,
    listings_dir: str,
    carfax_dir: str,
    index_path: str,
    secrets: Mapping[str, object],
    logger: Optional[logging.Logger] = None,
) -> Optional[SyncOutcome]:
    """Synchronise Google Drive files into the local inventory/Carfax folders.

    Returns ``None`` when credentials or configuration are missing; otherwise a
    ``SyncOutcome`` describing the work carried out.
    """

    log = logger or logging.getLogger(__name__)

    if not folder_id:
        log.debug("Google Drive sync skipped: no folder ID configured.")
        return None

    info = _load_service_account_info(secrets)
    if not info:
        log.debug("Google Drive sync skipped: no service-account credentials provided.")
        return None

    creds = service_account.Credentials.from_service_account_info(info, scopes=SYNC_SCOPES)
    try:
        service = build("drive", "v3", credentials=creds, cache_discovery=False)
    except Exception as exc:
        log.error("Failed to initialise Google Drive client: %s", exc)
        return SyncOutcome(downloaded=0, skipped=0, errors=(str(exc),))

    os.makedirs(listings_dir, exist_ok=True)
    os.makedirs(carfax_dir, exist_ok=True)

    index = _load_index(index_path)
    updated_index: Dict[str, Dict[str, str]] = dict(index)
    outcome = SyncOutcome()

    try:
        for file in _iter_drive_files(service, folder_id):
            file_id = file.get("id")
            name = file.get("name") or ""
            modified = file.get("modifiedTime") or ""
            target_dir = _resolve_target_dir(name, listings_dir, carfax_dir)
            if not target_dir:
                log.debug("Skipping unsupported file type: %s", name)
                continue

            index_entry = index.get(file_id)
            if index_entry and index_entry.get("modifiedTime") == modified and os.path.exists(index_entry.get("localPath", "")):
                outcome.skipped += 1
                updated_index[file_id] = index_entry
                continue

            dt = _parse_modified_time(modified)
            stamp = dt.strftime("%Y%m%d_%H%M%S")
            safe_name = _safe_filename(name)
            local_name = f"{stamp}__{file_id}__{safe_name}"
            local_path = os.path.join(target_dir, local_name)

            request = service.files().get_media(fileId=file_id)
            with io.FileIO(local_path, "wb") as fh:
                downloader = MediaIoBaseDownload(fh, request, chunksize=1024 * 1024)
                done = False
                while not done:
                    _, done = downloader.next_chunk()

            updated_index[file_id] = {
                "modifiedTime": modified,
                "localPath": local_path,
            }
            outcome.downloaded += 1
    except HttpError as exc:
        outcome.errors += (f"Drive API error: {exc}",)
        log.error("Drive API error while syncing: %s", exc)
    except Exception as exc:
        outcome.errors += (str(exc),)
        log.error("Unexpected error during Drive sync: %s", exc)

    _save_index(index_path, updated_index)
    return outcome


__all__ = ["SyncOutcome", "sync_google_drive_folder"]
