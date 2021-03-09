import io
import os
import shutil
from typing import Any, Dict, List, Optional

from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

from ..common import execute
from .client import DriveService


def _share_file(
    client: DriveService, file_id: str, body: Dict[str, Any], sendNotificationEmail: bool
) -> None:
    service = client.getService()
    with DriveService._lock:
        service.permissions().create(
            fileId=file_id,
            body=body,
            sendNotificationEmail=sendNotificationEmail,
        ).execute()


def _transfer_ownership(client: DriveService, file_id: str, body: Dict[str, Any]) -> None:
    service = client.getService()
    with DriveService._lock:
        service.permissions().create(
            fileId=file_id,
            body=body,
            transferOwnership=True,
        ).execute()


def _delete_file(client: DriveService, file_id: str) -> None:
    service = client.getService()
    with DriveService._lock:
        service.files().delete(fileId=file_id).execute()


def _copy_file(client: DriveService, file_id: str, body: Dict[str, Any]) -> Dict[str, str]:
    service = client.getService()
    with DriveService._lock:
        return service.files().copy(fileId=file_id, body=body).execute()


def _find_file(client: DriveService, name: str, parent_id: str) -> List[Dict[str, str]]:
    service = client.getService()
    with DriveService._lock:
        return (
            service.files().list(q=f"name = '{name}' and '{parent_id}' in parents").execute().get("files", [])
        )


def _list_files(client: DriveService, parent_id: str, page_token: Optional[str]) -> Dict[str, Any]:
    service = client.getService()
    with DriveService._lock:
        return service.files().list(q=f"'{parent_id}' in parents", pageToken=page_token).execute()


def _download_file(client: DriveService, file_id: str, destination: str) -> str:
    request = client.getService().files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        _status, done = downloader.next_chunk()
    fh.seek(0)
    with open(destination, "wb") as fp:
        shutil.copyfileobj(fh, fp)  # type: ignore # Workaround for python/mypy#8962
        return destination


def download_file(client: DriveService, file_id: str, destination: str) -> Optional[str]:
    """Downloads a file

    Args:
    - client (DriveService): Client to use - must have read-access to the file
    - file_id (str): File ID to download
    - destination (str): Destination path of the file (where it should be downloaded)

    Returns:
    - Optional[str]: Destination path of the file, or None if failed
    """

    try:
        return execute(lambda: _download_file(client, file_id, destination))
    except Exception as e:
        client._logger.warning("Could not download file {}: {}".format(file_id, e), exc_info=e)
        return None


def _upload_file(client: DriveService, file_metadata: Dict[str, Any], media_body: MediaFileUpload) -> str:
    f = client.getService().files().create(body=file_metadata, media_body=media_body, fields="id").execute()
    return f.get("id")


def upload_file(
    client: DriveService, file: str, destination: str, mimetype: str = "image/jpeg"
) -> Optional[str]:
    """Uploads a file.

    Note: even if a file with the same name already exists in this folder, a new file will be created.

    Args:
    - client (DriveService): Client to use - must have write-access to the folder
    - file (str): Path of the file to upload
    - destination (str): Folder ID
    - mimetype (str, optional): Mime-type of the file. Defaults to "image/jpeg".

    Returns:
    - Optional[str]: ID of the created file, or None if failed
    """
    file_metadata = {
        "name": os.path.basename(file),
        "parents": [destination],
    }
    media = MediaFileUpload(file, mimetype=mimetype)
    try:
        return execute(lambda: _upload_file(client, file_metadata, media))
    except Exception as e:
        client._logger.warning("Could not upload file {}: {}".format(file, e), exc_info=e)
        return None


def _update_file(client: DriveService, fileId: str, media_body: MediaFileUpload) -> str:
    f = client.getService().files().update(fileId=fileId, media_body=media_body, fields="id").execute()
    return f.get("id")


def update_file(client: DriveService, file: str, fileId: str, mimetype: str = "image/jpeg") -> Optional[str]:
    """Updates a file.

    Args:
    - client (DriveService): Client to use - must have write-access to the file
    - file (str): Path of the file to upload
    - destination (str): File ID to update
    - mimetype (str, optional): Mime-type of the file. Defaults to "image/jpeg".

    Returns:
    - Optional[str]: ID of the updated file, or None if failed
    """
    media = MediaFileUpload(file, mimetype=mimetype)
    try:
        return execute(lambda: _update_file(client, fileId, media))
    except Exception as e:
        client._logger.warning("Could not update file {} ({}): {}".format(file, fileId, e), exc_info=e)
        return None


def _insert_file(client: DriveService, file_metadata: Dict[str, Any]) -> str:
    f = client.getService().files().create(body=file_metadata, fields="id").execute()
    return f.get("id")


def insert_file(
    client: DriveService,
    name: str,
    destination: str,
    mimetype: str = "application/vnd.google-apps.spreadsheet",
) -> Optional[str]:
    """Inserts a new empty file.

    This is useful if inserting a new Spreadsheet that can be manipulated dynamically through the sheets module
    of this library - see https://developers.google.com/drive/api/v3/mime-types for Google Workspace MIME types.

    Note: even if a file with the same name already exists in this folder, a new file will be created.

    Args:
    - client (DriveService): Client to use - must have write-access to the folder
    - name (str): Name of the file
    - destination (str): Folder ID
    - mimetype (str, optional): Mime-type of the file. Defaults to "application/vnd.google-apps.spreadsheet".

    Returns:
    - Optional[str]: ID of the created file, or None if failed
    """
    file_metadata = {"name": name, "mimeType": mimetype, "parents": [destination]}
    try:
        return execute(lambda: _insert_file(client, file_metadata))
    except Exception as e:
        client._logger.warning("Could not create file {}: {}".format(name, e), exc_info=e)
        return None
