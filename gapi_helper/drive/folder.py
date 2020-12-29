import os
import time
from typing import Any, List, Optional

from .client import DriveService
from .operations import download_file, update_file, upload_file


class Folder:
    def __init__(self, folder_name: str, folder_id: str, client: Optional[DriveService] = None) -> None:
        self.folder_name = folder_name
        self.folder_id = folder_id
        if client is None:
            self.client = DriveService()
        else:
            self.client = client

    def downloadFile(self, name: str, destination: str) -> Optional[str]:
        file = self.findFile(name)
        if file:
            self.client._logger.info(
                "Downloading file {} from {} ({})...".format(name, self.folder_name, self.folder_id)
            )
            return download_file(self.client, file.get("id"), destination)
        return None

    def uploadFile(self, file: str, mimetype: str = "image/jpeg", update: bool = False) -> Optional[str]:
        if update:
            filename = os.path.basename(file)
            filedrive = self.findFile(filename)
            if filedrive:
                self.client._logger.info(
                    "Uploading file {} as new revision into {} ({})...".format(
                        file, self.folder_name, self.folder_id
                    )
                )
                return update_file(self.client, file, filedrive.get("id"), mimetype)
        self.client._logger.info(
            "Uploading file {} as new file into {} ({})...".format(file, self.folder_name, self.folder_id)
        )
        return upload_file(self.client, file, self.folder_id, mimetype)

    def findFile(self, name: str) -> Any:
        failures = 0
        delay: float = 5
        while True:
            try:
                self.client._logger.info(
                    "Searching for file {} in {} ({})...".format(name, self.folder_name, self.folder_id)
                )
                response = (
                    self.client.getService()
                    .files()
                    .list(q="name = '{}' and '{}' in parents".format(name, self.folder_id))
                    .execute()
                )
                for file in response.get("files", []):
                    return file
                return None

            except Exception as e:
                failures += 1
                if failures > 2:
                    self.client._logger.warning("Too many failures, abandonning")
                    self.client._logger.warning("Could not find file {}: {}".format(name, e))
                    return None

                # Retry
                self.client._logger.warning(
                    "Failed {} times ({}), retrying in {} seconds...".format(failures, e, delay)
                )
                time.sleep(delay)
                self.client.reset()
                delay *= 1.5

    def hasFile(self, filename: str, destination: str) -> Optional[str]:
        if os.path.isfile(destination):
            return destination
        else:
            f = self.downloadFile(filename, destination)
            if f:
                return destination
            else:
                return None

    def list(self) -> Optional[List[Any]]:
        failures = 0
        delay: float = 5
        files = []
        page_token = None
        self.client._logger.info("Retrieving files in {} ({})...".format(self.folder_name, self.folder_id))
        while True:
            try:
                response = (
                    self.client.getService()
                    .files()
                    .list(q="'{}' in parents".format(self.folder_id), pageToken=page_token)
                    .execute()
                )
                for file in response.get("files", []):
                    files.append(file)

                page_token = response.get("nextPageToken", None)
                if page_token is None:
                    return files

            except Exception as e:
                failures += 1
                if failures > 2:
                    self.client._logger.warning("Too many failures, abandonning")
                    self.client._logger.warning("Could not find folder {}: {}".format(self.folder_id, e))
                    return None

                # Retry
                self.client._logger.warning(
                    "Failed {} times ({}), retrying in {} seconds...".format(failures, e, delay)
                )
                time.sleep(delay)
                self.client.reset()
                delay *= 1.5
