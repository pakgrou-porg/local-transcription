"""Google Drive operations — folder resolution, file listing, download, archive."""

import io
import logging
import os
from typing import Any

from googleapiclient.http import MediaIoBaseDownload

logger = logging.getLogger(__name__)


def resolve_folder_id(drive_service, folder_name: str, parent_id: str | None = None) -> str:
    """Resolve a Google Drive folder ID by name.

    Parameters
    ----------
    drive_service : googleapiclient.discovery.Resource
        Authenticated Drive v3 service.
    folder_name : str
        Human-readable folder name to find.
    parent_id : str | None
        If provided, restrict search to children of this parent folder.

    Returns
    -------
    str
        The folder's Drive file ID.

    Raises
    ------
    FileNotFoundError
        If the folder cannot be found.
    """
    query_parts = [
        f"name = '{folder_name}'",
        "mimeType = 'application/vnd.google-apps.folder'",
        "trashed = false",
    ]
    if parent_id:
        query_parts.append(f"'{parent_id}' in parents")

    query = " and ".join(query_parts)
    logger.info("Resolving folder '%s' with query: %s", folder_name, query)

    response = drive_service.files().list(
        q=query,
        spaces="drive",
        fields="files(id, name)",
        pageSize=10,
    ).execute()

    files = response.get("files", [])
    if not files:
        raise FileNotFoundError(f"Drive folder not found: '{folder_name}'")

    folder_id = files[0]["id"]
    logger.info("Resolved folder '%s' -> %s", folder_name, folder_id)
    return folder_id


def list_audio_files(drive_service, folder_id: str) -> list[dict[str, Any]]:
    """List audio files (.mp3, .wav) in a Drive folder.

    Parameters
    ----------
    drive_service : googleapiclient.discovery.Resource
        Authenticated Drive v3 service.
    folder_id : str
        Drive folder ID to scan.

    Returns
    -------
    list[dict]
        List of file metadata dicts with keys: id, name, mimeType, size.
    """
    query = (
        f"'{folder_id}' in parents and trashed = false and ("
        "mimeType = 'audio/mpeg' or "
        "mimeType = 'audio/wav' or "
        "mimeType = 'audio/x-wav' or "
        "name contains '.mp3' or "
        "name contains '.wav'"
        ")"
    )
    logger.info("Listing audio files in folder %s", folder_id)

    all_files = []
    page_token = None

    while True:
        response = drive_service.files().list(
            q=query,
            spaces="drive",
            fields="nextPageToken, files(id, name, mimeType, size)",
            pageSize=100,
            pageToken=page_token,
        ).execute()

        files = response.get("files", [])
        all_files.extend(files)
        page_token = response.get("nextPageToken")
        if not page_token:
            break

    logger.info("Found %d audio file(s) in folder %s", len(all_files), folder_id)
    return all_files


def download_file(drive_service, file_id: str, dest_path: str) -> str:
    """Download a file from Google Drive to a local path.

    Parameters
    ----------
    drive_service : googleapiclient.discovery.Resource
        Authenticated Drive v3 service.
    file_id : str
        Drive file ID to download.
    dest_path : str
        Full local path where the file will be written.

    Returns
    -------
    str
        The dest_path on success.
    """
    logger.info("Downloading Drive file %s -> %s", file_id, dest_path)

    request = drive_service.files().get_media(fileId=file_id)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()
        if status:
            pct = int(status.progress() * 100)
            logger.info("Download progress: %d%%", pct)

    dest_dir = os.path.dirname(dest_path)
    if dest_dir:
        os.makedirs(dest_dir, exist_ok=True)

    with open(dest_path, "wb") as f:
        f.write(buffer.getvalue())

    file_size = os.path.getsize(dest_path)
    logger.info("Downloaded %s (%d bytes)", dest_path, file_size)
    return dest_path


def archive_file(
    drive_service,
    file_id: str,
    source_folder_id: str,
    archive_folder_id: str,
) -> None:
    """Move a file from the source folder to the archive folder on Drive.

    Parameters
    ----------
    drive_service : googleapiclient.discovery.Resource
        Authenticated Drive v3 service.
    file_id : str
        Drive file ID to move.
    source_folder_id : str
        Current parent folder ID.
    archive_folder_id : str
        Destination archive folder ID.
    """
    logger.info(
        "Archiving file %s: %s -> %s",
        file_id, source_folder_id, archive_folder_id,
    )
    drive_service.files().update(
        fileId=file_id,
        addParents=archive_folder_id,
        removeParents=source_folder_id,
        fields="id, parents",
    ).execute()
    logger.info("File %s archived successfully", file_id)


def file_exists(drive_service, file_id: str) -> bool:
    """Check whether a file ID still exists (not trashed) on Drive.

    Parameters
    ----------
    drive_service : googleapiclient.discovery.Resource
        Authenticated Drive v3 service.
    file_id : str
        Drive file ID to check.

    Returns
    -------
    bool
        True if the file exists, False otherwise.
    """
    try:
        meta = drive_service.files().get(
            fileId=file_id, fields="id, trashed"
        ).execute()
        return not meta.get("trashed", False)
    except Exception:
        return False
