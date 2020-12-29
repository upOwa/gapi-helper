import logging
import os
import threading
import time
from typing import TYPE_CHECKING, Any, Optional

from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

if TYPE_CHECKING:
    from gapi_helper.sheets.sheet import Sheet


class SheetsService:
    credentials: Any = None
    service: Any = None
    headers: Any = None
    _sa_keyfile: Optional[str] = None
    _spreadsheet_test: Optional["Sheet"] = None
    _backupLocation: Optional[str] = None
    _cacheLocation: Optional[str] = None
    _lock = threading.Lock()
    _logger = logging.getLogger("gapi_helper")
    _force_test_spreadsheet = False

    @staticmethod
    def configure(
        sa_keyfile: str,
        spreadsheet_test: "Sheet",
        backupLocation: str,
        cacheLocation: str,
        logger_namespace: str = None,
        force_test_spreadsheet: bool = False,
    ) -> None:
        SheetsService._sa_keyfile = sa_keyfile
        SheetsService._spreadsheet_test = spreadsheet_test
        SheetsService._backupLocation = backupLocation
        SheetsService._cacheLocation = cacheLocation
        if logger_namespace:
            SheetsService._logger = logging.getLogger(logger_namespace)
        SheetsService._force_test_spreadsheet = force_test_spreadsheet

    @staticmethod
    def getBackupLocation() -> str:
        if SheetsService._backupLocation is None or not os.path.isdir(SheetsService._backupLocation):
            raise RuntimeError("Backup location is not configured")
        return SheetsService._backupLocation

    @staticmethod
    def getCacheLocation() -> str:
        if SheetsService._cacheLocation is None or not os.path.isdir(SheetsService._cacheLocation):
            raise RuntimeError("Cache location is not configured")
        return SheetsService._cacheLocation

    @staticmethod
    def getTestSpreadsheet() -> "Sheet":
        if SheetsService._spreadsheet_test is None:
            raise RuntimeError("Test spreadsheet is not configured")
        SheetsService._spreadsheet_test.loadInfos()
        return SheetsService._spreadsheet_test

    @staticmethod
    def getService() -> Any:
        with SheetsService._lock:
            if SheetsService.service is None:
                if SheetsService._sa_keyfile is None or SheetsService._spreadsheet_test is None:
                    raise RuntimeError("Service is not configured")

                credentials = ServiceAccountCredentials.from_json_keyfile_name(
                    SheetsService._sa_keyfile,
                    (
                        "https://www.googleapis.com/auth/drive",
                        "https://spreadsheets.google.com/feeds",
                        "https://docs.google.com/feeds",
                    ),
                )

                service = build("sheets", "v4", credentials=credentials)

                failures: int = 0
                delay: float = 30
                while True:
                    try:
                        service.spreadsheets().get(
                            spreadsheetId=SheetsService._spreadsheet_test.parent.spreadsheet_id
                        ).execute()

                        SheetsService.credentials = credentials
                        SheetsService.headers = {
                            "Authorization": "Bearer " + credentials.access_token,
                        }
                        SheetsService.service = service
                        break
                    except Exception as e:
                        failures += 1
                        if failures > 5:
                            SheetsService._logger.warning("Too many failures getting service, abandonning")
                            raise e

                        # Retry
                        SheetsService._logger.warning(
                            "Failed getting service {} times ({}), retrying in {} seconds...".format(
                                failures, e, delay
                            )
                        )
                        time.sleep(delay)
                        delay *= 1.5

            return SheetsService.service

    @staticmethod
    def getHeaders() -> Any:
        if SheetsService.headers is None:
            SheetsService.getService()
        return SheetsService.headers

    @staticmethod
    def reset() -> None:
        with SheetsService._lock:
            SheetsService.service = None
            SheetsService.headers = None
