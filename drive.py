import os
import logging
import tempfile
from pathlib import Path
from typing import Optional, List


logger = logging.getLogger(__name__)


def resolve_folder_by_name(drive_service, folder_name: str, parent_id: str = "root") -> Optional[str]:
    """
    Resolve a folder ID by name within a parent folder.
    
    Searches for a folder with exact name match (case-sensitive).
    
    Args:
        drive_service: Authenticated Google Drive service object
        folder_name (str): Name of folder to find
        parent_id (str): Parent folder ID to search within (default: "root")
        
    Returns:
        str: Folder ID if found, None otherwise
    """
    try:
        query = (
            f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' "
            f"and '{parent_id}' in parents and trashed=false"
        )
        
        results = drive_service.files().list(
            q=query,
            spaces="drive",
            pageSize=10,
            fields="files(id, name)"
        ).execute()
        
        files = results.get("files", [])
        
        if not files:
            logger.error(f"Folder '{folder_name}' not found in parent {parent_id}")
            return None
        
        if len(files) > 1:
            logger.warning(f"Multiple folders named '{folder_name}' found, using first")
        
        folder_id = files[0]["id"]
        logger.info(f"Resolved folder '{folder_name}' to ID {folder_id}")
        return folder_id
    
    except Exception as e:
        logger.error(f"Failed to resolve folder '{folder_name}': {e}")
        return None


def resolve_source_folder_id(drive_service, source_folder_name: str) -> Optional[str]:
    """
    Resolve the source folder ID by name in Google Drive root.
    
    Args:
        drive_service: Authenticated Google Drive service object
        source_folder_name (str): Name of source folder (from GOOGLE_DRIVE_SOURCE_FOLDER)
        
    Returns:
        str: Folder ID, or None if not found
    """
    return resolve_folder_by_name(drive_service, source_folder_name, parent_id="root")


def resolve_archive_folder_id(
    drive_service,
    source_folder_id: str,
    archive_folder_name: str
) -> Optional[str]:
    """
    Resolve the archive folder ID by name within the source folder.
    
    Args:
        drive_service: Authenticated Google Drive service object
        source_folder_id (str): ID of source folder
        archive_folder_name (str): Name of archive subfolder
        
    Returns:
        str: Folder ID, or None if not found
    """
    return resolve_folder_by_name(drive_service, archive_folder_name, parent_id=source_folder_id)


def list_audio_files(drive_service, folder_id: str) -> List[dict]:
    """
    List all .mp3 and .wav files in a folder.
    
    Returns untrashed files only, in descending creation order.
    
    Args:
        drive_service: Authenticated Google Drive service object
        folder_id (str): Folder ID to search
        
    Returns:
        List[dict]: List of files with id, name, size, createdTime
    """
    try:
        query = (
            f"(name contains '.mp3' or name contains '.wav') "
            f"and '{folder_id}' in parents and trashed=false"
        )
        
        results = drive_service.files().list(
            q=query,
            spaces="drive",
            pageSize=100,
            fields="files(id, name, size, createdTime)",
            orderBy="createdTime desc"
        ).execute()
        
        files = results.get("files", [])
        logger.info(f"Found {len(files)} audio files in folder {folder_id}")
        return files
    
    except Exception as e:
        logger.error(f"Failed to list audio files in folder {folder_id}: {e}")
        return []


def download_file(drive_service, file_id: str, dest_path: str) -> bool:
    """
    Download a file from Google Drive to local file.
    
    Args:
        drive_service: Authenticated Google Drive service object
        file_id (str): Google Drive file ID
        dest_path (str): Local destination path
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        from googleapiclient.http import MediaIoBaseDownload
        import io
        
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.FileIO(dest_path, "wb")
        downloader = MediaIoBaseDownload(fh, request)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
        
        fh.close()
        
        logger.info(f"Downloaded file {file_id} to {dest_path}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to download file {file_id}: {e}")
        return False


def move_file(
    drive_service,
    file_id: str,
    source_folder_id: str,
    archive_folder_id: str,
) -> bool:
    """
    Move a file from source folder to archive folder.
    
    Uses addParents and removeParents to move file (not copy).
    
    Args:
        drive_service: Authenticated Google Drive service object
        file_id (str): File ID to move
        source_folder_id (str): Current parent folder ID
        archive_folder_id (str): Destination folder ID
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        file = drive_service.files().update(
            fileId=file_id,
            addParents=archive_folder_id,
            removeParents=source_folder_id,
            fields="id, parents"
        ).execute()
        
        logger.info(f"Moved file {file_id} from {source_folder_id} to {archive_folder_id}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to move file {file_id}: {e}")
        return False


def get_file_parents(drive_service, file_id: str) -> Optional[List[str]]:
    """
    Return the current parent folder IDs for a Drive file.

    Args:
        drive_service: Authenticated Google Drive service object
        file_id (str): File ID to inspect

    Returns:
        list[str] | None: Parent folder IDs, or None on failure
    """
    try:
        metadata = drive_service.files().get(
            fileId=file_id,
            fields="id, name, parents"
        ).execute()
        return metadata.get("parents", [])
    except Exception as e:
        logger.error(f"Failed to fetch metadata for file {file_id}: {e}")
        return None


def archive_file_if_needed(
    drive_service,
    file_id: str,
    source_folder_id: str,
    archive_folder_id: str,
) -> bool:
    """
    Ensure a file is archived exactly once.

    If the file is already in the archive folder, this is a no-op.
    If the file is still in the source folder, it is moved.
    If the file is in some other parent set, all current parents are replaced
    with the archive folder to restore a deterministic archive state.
    """
    parents = get_file_parents(drive_service, file_id)
    if parents is None:
        return False

    if archive_folder_id in parents:
        logger.info(f"File {file_id} is already in archive folder {archive_folder_id}")
        return True

    remove_parents = ",".join(parents) if parents else source_folder_id

    try:
        drive_service.files().update(
            fileId=file_id,
            addParents=archive_folder_id,
            removeParents=remove_parents,
            fields="id, parents"
        ).execute()
        logger.info(f"Archived file {file_id} into folder {archive_folder_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to archive file {file_id}: {e}")
        return False


def download_file_from_archive(
    drive_service,
    file_id: str,
    file_name: str,
) -> Optional[str]:
    """
    Download a file from archive to temporary location.
    
    Creates a temp file with _archive suffix for tracking.
    
    Args:
        drive_service: Authenticated Google Drive service object
        file_id (str): Google Drive file ID
        file_name (str): Original file name (for logging)
        
    Returns:
        str: Path to downloaded file, or None on failure
    """
    try:
        # Create temp file
        temp_dir = tempfile.mkdtemp(prefix="archive_")
        dest_path = Path(temp_dir) / f"{Path(file_name).stem}_archive{Path(file_name).suffix}"
        
        success = download_file(drive_service, file_id, str(dest_path))
        
        if success:
            logger.info(f"Downloaded archive file {file_name} to {dest_path}")
            return str(dest_path)
        else:
            # Clean up temp dir on failure
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)
            return None
    
    except Exception as e:
        logger.error(f"Failed to download archive file: {e}")
        return None
