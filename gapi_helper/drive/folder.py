import os
import time
from typing import Any, List, Optional

from .client import DriveService
from .file import File
from .operations import download_file, insert_file, update_file, upload_file


class Folder(File):
    """Drive Folder"""

    def __init__(self, folder_name: str, folder_id: str, client: Optional[DriveService] = None) -> None:
        """Constructor

        Args:
        - folder_name (str): Folder name - can be different from the actual name on the drive
        - folder_id (str): Folder ID (as given for instance via its url e.g. https://drive.google.com/drive/folders/<ID>)
        - client (DriveService, optional): Drive service to be used - useful if delegation is required. Defaults to None (default service).
        """
        super().__init__(folder_name, folder_id, client)

    def downloadFile(self, name: str, destination: str) -> Optional[str]:
        """Downloads a file from this folder

        Args:
        - name (str): Name of the file
        - destination (str): Destination path of the file (where it should be downloaded)

        Returns:
        - Optional[str]: Destination path of the file, or None if the file could not be found
        """
        file = self.findFile(name)
        if file:
            self.client._logger.info(
                "Downloading file {} from {} ({})...".format(name, self.file_name, self.file_id)
            )
            return download_file(self.client, file.get("id"), destination)
        return None

    def uploadFile(self, file: str, mimetype: str = "image/jpeg", update: bool = False) -> Optional[str]:
        """Uploads a file to this folder

        Args:
        - file (str): Path of the file to upload
        - mimetype (str, optional): Mime-type of the file. Defaults to "image/jpeg".
        - update (bool, optional): Updates the file if already exists. Defaults to False, creates a new file anyway.

        Returns:
        - Optional[str]: ID of the file created or updated, or None if failed
        """
        if update:
            filename = os.path.basename(file)
            filedrive = self.findFile(filename)
            if filedrive:
                self.client._logger.info(
                    "Uploading file {} as new revision into {} ({})...".format(
                        file, self.file_name, self.file_id
                    )
                )
                return update_file(self.client, file, filedrive.get("id"), mimetype)
        self.client._logger.info(
            "Uploading file {} as new file into {} ({})...".format(file, self.file_name, self.file_id)
        )
        return upload_file(self.client, file, self.file_id, mimetype)

    def insertFile(
        self, name: str, mimetype: str = "application/vnd.google-apps.spreadsheet"
    ) -> Optional[File]:
        """Inserts a new empty file in this folder.

        This is useful if inserting a new Spreadsheet that can be manipulated dynamically through the sheets module
        of this library - see https://developers.google.com/drive/api/v3/mime-types for Google Workspace MIME types.

        Note: even if a file with the same name already exists in this folder, a new file will be created.

        Args:
        - name (str): Name of the file
        - mimetype (str, optional): Mime-type of the file. Defaults to "application/vnd.google-apps.spreadsheet".

        Returns:
        - Optional[File]: Created file, or None if failed
        """
        file_id = insert_file(self.client, name, self.file_id, mimetype)
        if file_id is not None:
            return File(name, file_id, self.client)
        else:
            return None

    def createFolder(self, name: str) -> Optional["Folder"]:
        """Inserts a new empty folder in this folder.

        Note: even if a folder with the same name already exists in this folder, a new folder will be created.

        Args:
        - name (str): Name of the folder

        Returns:
        - Optional[Folder]: Created folder, or None if failed
        """
        file_id = insert_file(self.client, name, self.file_id, "application/vnd.google-apps.folder")
        if file_id is not None:
            return Folder(name, file_id, self.client)
        else:
            return None

    def findFile(self, name: str) -> Any:
        """Find file(s) in this folder based on its name.

        If multiple files have the same name, only one is returned (may not be determistic).

        Args:
        - name (str): Name of the file to search for

        Returns:
        - Any: File that matches the name, or None if not found
        """
        failures = 0
        delay = DriveService._retry_delay
        while True:
            try:
                self.client._logger.info(
                    "Searching for file {} in {} ({})...".format(name, self.file_name, self.file_id)
                )
                response = (
                    self.client.getService()
                    .files()
                    .list(q="name = '{}' and '{}' in parents".format(name, self.file_id))
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
        """Returns whether a file has been downloaded or not.

        If the file does not already exist at `destination`, tries to download it.

        Args:
        - filename (str): Name of the file
        - destination (str): Destination path of the file (where it should be downloaded)

        Returns:
        - Optional[str]: Destination path of the file, or None if the file does not exist and could not be downloaded
        """
        if os.path.isfile(destination):
            return destination
        else:
            f = self.downloadFile(filename, destination)
            if f:
                return destination
            else:
                return None

    def list(self) -> Optional[List[Any]]:
        """Lists all files in this folder

        Returns:
        - Optional[List[Any]]: List of files, or None if failed
        """
        failures = 0
        delay = DriveService._retry_delay
        files = []
        page_token = None
        self.client._logger.info("Retrieving files in {} ({})...".format(self.file_name, self.file_id))
        while True:
            try:
                response = (
                    self.client.getService()
                    .files()
                    .list(q="'{}' in parents".format(self.file_id), pageToken=page_token)
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
                    self.client._logger.warning("Could not find folder {}: {}".format(self.file_id, e))
                    return None

                # Retry
                self.client._logger.warning(
                    "Failed {} times ({}), retrying in {} seconds...".format(failures, e, delay)
                )
                time.sleep(delay)
                self.client.reset()
                delay *= 1.5
