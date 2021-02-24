import io
import os
import shutil
import time
from typing import Optional

from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

from .client import DriveService


def download_file(client: DriveService, file_id: str, destination: str) -> Optional[str]:
    """Downloads a file

    Args:
    - client (DriveService): Client to use - must have read-access to the file
    - file_id (str): File ID to download
    - destination (str): Destination path of the file (where it should be downloaded)

    Returns:
    - Optional[str]: Destination path of the file, or None if failed
    """
    failures = 0
    delay = DriveService._retry_delay
    while True:
        try:
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
        except Exception as e:
            failures += 1
            if failures > 2:
                client._logger.warning("Too many failures, abandonning")
                client._logger.warning("Could not download file {}: {}".format(file_id, e))
                return None

            # Retry
            client._logger.warning(
                "Failed {} times ({}), retrying in {} seconds...".format(failures, e, delay)
            )
            time.sleep(delay)
            client.reset()
            delay *= 1.5


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
    failures = 0
    delay = DriveService._retry_delay
    file_metadata = {
        "name": os.path.basename(file),
        "parents": [destination],
    }
    while True:
        try:
            media = MediaFileUpload(file, mimetype=mimetype)
            f = (
                client.getService()
                .files()
                .create(body=file_metadata, media_body=media, fields="id")
                .execute()
            )
            return f.get("id")
        except Exception as e:
            failures += 1
            if failures > 2:
                client._logger.warning("Too many failures, abandonning")
                client._logger.warning("Could not upload file {}: {}".format(file, e))
                return None

            # Retry
            client._logger.warning(
                "Failed {} times ({}), retrying in {} seconds...".format(failures, e, delay)
            )
            time.sleep(delay)
            client.reset()
            delay *= 1.5


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
    failures = 0
    delay = DriveService._retry_delay
    while True:
        try:
            media = MediaFileUpload(file, mimetype=mimetype)
            f = client.getService().files().update(fileId=fileId, media_body=media, fields="id").execute()
            return f.get("id")
        except Exception as e:
            failures += 1
            if failures > 2:
                client._logger.warning("Too many failures, abandonning")
                client._logger.warning("Could not update file {} ({}): {}".format(file, fileId, e))
                return None

            # Retry
            client._logger.warning(
                "Failed {} times ({}), retrying in {} seconds...".format(failures, e, delay)
            )
            time.sleep(delay)
            client.reset()
            delay *= 1.5


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
    failures = 0
    delay = DriveService._retry_delay
    file_metadata = {"name": name, "mimeType": mimetype, "parents": [destination]}
    while True:
        try:
            f = client.getService().files().create(body=file_metadata, fields="id").execute()
            return f.get("id")
        except Exception as e:
            failures += 1
            if failures > 2:
                client._logger.warning("Too many failures, abandonning")
                client._logger.warning("Could not create file {}: {}".format(name, e))
                return None

            # Retry
            client._logger.warning(
                "Failed {} times ({}), retrying in {} seconds...".format(failures, e, delay)
            )
            time.sleep(delay)
            client.reset()
            delay *= 1.5
