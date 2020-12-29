import io
import os
import shutil
import time
from typing import Optional

from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

from .client import DriveService


def download_file(client: DriveService, file_id: str, destination: str) -> Optional[str]:
    failures = 0
    delay: float = 5
    while True:
        try:
            request = client.getService().files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
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
    failures = 0
    delay: float = 5
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
    failures = 0
    delay: float = 5
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
