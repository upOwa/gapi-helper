import logging
import threading
from typing import Any, Optional

from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials


class DriveService:
    _sa_keyfile: Optional[str] = None
    _logger = logging.getLogger("gapi_helper")
    _lock = threading.Lock()

    @staticmethod
    def configure(sa_keyfile: str, logger_namespace: str = None) -> None:
        DriveService._sa_keyfile = sa_keyfile
        if logger_namespace:
            DriveService._logger = logging.getLogger(logger_namespace)

    def __init__(self, user: Optional[str] = None, logger_namespace: str = None) -> None:
        self._user = user
        self.credentials: Any = None
        self.service: Any = None
        if logger_namespace:
            self._logger = logging.getLogger(logger_namespace)
        else:
            self._logger = DriveService._logger

    def getService(self) -> Any:
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
                self.service = build("drive", "v3", credentials=self.credentials)

            return self.service

    def reset(self) -> None:
        with DriveService._lock:
            self.service = None
            self.credentials = None
