from typing import TYPE_CHECKING, Optional

from .client import DriveService

if TYPE_CHECKING:
    from .folder import Folder


class File:
    """Drive File"""

    def __init__(self, file_name: str, file_id: str, client: Optional[DriveService] = None) -> None:
        """Constructor

        Args:
        - file_name (str): Folder name - can be different from the actual name on the drive
        - file_id (str): Folder ID (as given for instance via its url e.g. https://drive.google.com/drive/folders/<ID>)
        - client (DriveService, optional): Drive service to be used - useful if delegation is required. Defaults to None (default service).
        """
        self.file_name = file_name
        self.file_id = file_id
        if client is None:
            self.client = DriveService()
        else:
            self.client = client

    def share(
        self, user: str, role: Optional[str] = "reader", sendNotificationEmail: Optional[bool] = False
    ) -> "File":
        """Shares this file with a specific user

        Args:
        - user (str): User email
        - role (str, optional): Role (reader, commenter or writer) - Defaults to reader
        - sendNotificationEmail (bool, optional): Sends a notification by email to the user - Defaults to False

        Returns:
        - File: self
        """
        body = {
            "role": role,
            "type": "user",
            "emailAddress": user,
        }

        self.client.getService().permissions().create(
            fileId=self.file_id,
            body=body,
            sendNotificationEmail=sendNotificationEmail,
        ).execute()
        return self

    def transfer_ownership(self, user: str) -> "File":
        """Transfers ownership of this file to a user.

        Args:
        - user (str): User email

        Returns:
        - File: self
        """
        body = {
            "role": "owner",
            "type": "user",
            "emailAddress": user,
        }

        self.client.getService().permissions().create(
            fileId=self.file_id,
            body=body,
            transferOwnership=True,
        ).execute()
        return self

    def delete(self) -> None:
        """Deletes this file."""
        self.client.getService().files().delete(
            fileId=self.file_id,
        ).execute()

    def copyTo(self, folder: "Folder", new_name: Optional[str]) -> "File":
        """Copies a file into another folder.

        This creates a new copy of the file (e.g. duplicates) - it does *not* add the same file into another folder.

        **Note:** this does *not* work with folders.

        Args:
        - folder (Folder): Folder to copy the file into
        - new_name (str, optional): New name of the file - Defaults to the same name as the original file

        Returns:
        - File: Newly created file
        """
        body = {"name": new_name or self.file_name, "parents": [folder.file_id]}

        f = self.client.getService().files().copy(fileId=self.file_id, body=body).execute()
        return File(f.get("name"), f.get("id"), client=self.client)
