import logging
import os
import threading
import time
from typing import TYPE_CHECKING, Any, Dict, Optional

import googleapiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials

if TYPE_CHECKING:
    from gapi_helper.sheets.sheet import Sheet  # pragma: no cover


class SheetsService:
    """Service to handle Google Sheets."""

    credentials: Any = None
    service: googleapiclient.discovery.Resource = None
    headers: Optional[Dict[str, str]] = None
    _sa_keyfile: Optional[str] = None
    _spreadsheet_test: Optional["Sheet"] = None
    _backupLocation: Optional[str] = None
    _cacheLocation: Optional[str] = None
    _lock = threading.Lock()
    _logger = logging.getLogger("gapi_helper")
    _force_test_spreadsheet = False
    _retry_delay: float = 5.0

    @staticmethod
    def configure(
        sa_keyfile: str,
        spreadsheet_test: "Sheet",
        backupLocation: Optional[str],
        cacheLocation: Optional[str],
        logger_namespace: str = None,
        force_test_spreadsheet: bool = False,
        retry_delay: float = None,
    ) -> None:
        """Configures the service. Must be called before using this service.

        Args:
        - sa_keyfile (str): Path to the service account key file
        - spreadsheet_test (Sheet): Sheet to be used as test
        - backupLocation (str): Path to where downloaded Sheets are stored
        - cacheLocation (str): Path to where cache info on Spreadsheets are stored
        - logger_namespace (str, optional): Namespace for the logger. Defaults to None, using "gapi_helper".
        - force_test_spreadsheet (bool, optional): Forces using the test Sheet. Defaults to False.
        - retry_delay (float, optional): Delay for retrying operations (in seconds). Defaults to 5.
        """
        SheetsService._sa_keyfile = sa_keyfile
        SheetsService._spreadsheet_test = spreadsheet_test
        SheetsService._backupLocation = backupLocation
        SheetsService._cacheLocation = cacheLocation
        if logger_namespace:
            SheetsService._logger = logging.getLogger(logger_namespace)
        SheetsService._force_test_spreadsheet = force_test_spreadsheet
        if retry_delay is not None:
            SheetsService._retry_delay = retry_delay

    @staticmethod
    def getBackupLocation() -> str:
        """Returns backup location

        Raises:
        - RuntimeError: Service is not configured or directory does not exist

        Returns:
        - str: Path to backup location
        """
        if SheetsService._backupLocation is None or not os.path.isdir(SheetsService._backupLocation):
            raise RuntimeError("Backup location is not configured")
        return SheetsService._backupLocation

    @staticmethod
    def getCacheLocation() -> str:
        """Returns cache location

        Raises:
        - RuntimeError: Service is not configured or directory does not exist

        Returns:
        - str: Path to cache location
        """
        if SheetsService._cacheLocation is None or not os.path.isdir(SheetsService._cacheLocation):
            raise RuntimeError("Cache location is not configured")
        return SheetsService._cacheLocation

    @staticmethod
    def getTestSpreadsheet() -> "Sheet":
        """Returns test sheet

        Raises:
        - RuntimeError: Service is not configured

        Returns:
        - Sheet: Test sheet
        """
        if SheetsService._spreadsheet_test is None:
            raise RuntimeError("Test spreadsheet is not configured")
        SheetsService._spreadsheet_test.loadInfos()
        return SheetsService._spreadsheet_test

    @staticmethod
    def getService() -> googleapiclient.discovery.Resource:
        """Returns the Google Sheets API service

        Raises:
        - RuntimeError: Service is not configured

        Returns:
        - googleapiclient.discovery.Resource: Resource for interacting with the Google Mail API.
        """
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

                service = googleapiclient.discovery.build("sheets", "v4", credentials=credentials)

                failures: int = 0
                delay = SheetsService._retry_delay
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
    def getHeaders() -> Dict[str, str]:
        """Get authorization headers.

        Returns:
        - Dict[str, str]: authorization headers
        """
        if SheetsService.headers is None:
            SheetsService.getService()
        assert SheetsService.headers is not None
        return SheetsService.headers

    @staticmethod
    def reset() -> None:
        """Resets the service"""
        with SheetsService._lock:
            SheetsService.service = None
            SheetsService.headers = None
