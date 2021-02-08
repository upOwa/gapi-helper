import logging
import threading
from typing import Any, Optional

import googleapiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials


class DriveService:
    """Service to handle Google Drive."""

    _sa_keyfile: Optional[str] = None
    _logger = logging.getLogger("gapi_helper")
    _lock = threading.Lock()
    _retry_delay: float = 5.0

    @staticmethod
    def configure(sa_keyfile: str, logger_namespace: str = None, retry_delay: float = None) -> None:
        """Configures the service. Must be called before using this service.

        Args:
        - sa_keyfile (str): Path to the service account key file
        - logger_namespace (str, optional): Namespace for the logger. Defaults to None, using "gapi_helper".
        - retry_delay (float, optional): Delay for retrying operations (in seconds). Defaults to 5.
        """
        DriveService._sa_keyfile = sa_keyfile
        if logger_namespace:
            DriveService._logger = logging.getLogger(logger_namespace)
        if retry_delay is not None:
            DriveService._retry_delay = retry_delay

    def __init__(self, user: Optional[str] = None, logger_namespace: str = None) -> None:
        """Constructor

        Args:
        - user (Optional[str], optional): User to use, using delegated credentials. Defaults to None, using service account credentials
        - logger_namespace (str, optional): Namespace for the logger. Defaults to None, using the one provided to `DriveService.configure()`
        """
        self._user = user
        self.credentials: Any = None
        self.service: googleapiclient.discovery.Resource = None
        if logger_namespace:
            self._logger = logging.getLogger(logger_namespace)
        else:
            self._logger = DriveService._logger

    def getService(self) -> googleapiclient.discovery.Resource:
        """Returns the Google Drive API service.

        Raises:
        - RuntimeError: Service is not configured

        Returns:
        - googleapiclient.discovery.Resource: Resource for interacting with the Google Drive API.
        """
        with DriveService._lock:
            if self.service is None:
                if DriveService._sa_keyfile is None:
                    raise RuntimeError("Service is not configured")

                credentials = ServiceAccountCredentials.from_json_keyfile_name(
                    DriveService._sa_keyfile,
                    (
                        "https://mail.google.com/",
                        "https://www.googleapis.com/auth/drive",
                        "https://www.googleapis.com/auth/drive.metadata",
                        "https://www.googleapis.com/auth/drive.metadata.readonly",
                        "https://www.googleapis.com/auth/drive.readonly",
                        "https://www.googleapis.com/auth/drive.appdata",
                        "https://www.googleapis.com/auth/drive.file",
                    ),
                )
                if self._user:
                    self.credentials = credentials.create_delegated(self._user)
                else:
                    self.credentials = credentials
                self.service = googleapiclient.discovery.build("drive", "v3", credentials=self.credentials)

            return self.service

    def reset(self) -> None:
        """Resets the service"""
        with DriveService._lock:
            self.service = None
            self.credentials = None
