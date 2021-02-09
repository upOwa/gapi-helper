import logging
import os
import tempfile

import googleapiclient
import pytest
from googleapiclient.http import HttpMockSequence

from gapi_helper.sheets import Sheet, SheetsService, Spreadsheet

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


def datafile(filename):
    return os.path.join(DATA_DIR, filename)


def read_datafile(filename, mode="r"):
    with open(datafile(filename), mode=mode) as f:
        return f.read()


@pytest.fixture(scope="module")
def request_mock():
    credentials_old = SheetsService.credentials
    service_old = SheetsService.service
    headers_old = SheetsService.headers
    _sa_keyfile_old = SheetsService._sa_keyfile
    _spreadsheet_test_old = SheetsService._spreadsheet_test
    _backupLocation_old = SheetsService._backupLocation
    _cacheLocation_old = SheetsService._cacheLocation
    _logger_old = SheetsService._logger
    _force_test_spreadsheet_old = SheetsService._force_test_spreadsheet

    SheetsService.credentials = None
    SheetsService.service = None
    SheetsService.headers = None
    SheetsService._sa_keyfile = None
    SheetsService._spreadsheet_test = None
    SheetsService._backupLocation = None
    SheetsService._cacheLocation = None
    SheetsService._logger = logging.getLogger("gapi_helper")
    SheetsService._force_test_spreadsheet = False

    _testfolder = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    _testspreadsheet = Spreadsheet("myFakeGoogleSpreadsheetId")
    _testsheet = Sheet(_testspreadsheet, "MY TAB")
    _serviceAccountCredentialsPath = "myproject-123456-abcdef012345.json"

    _cacheDir = tempfile.TemporaryDirectory()
    _backupDir = tempfile.TemporaryDirectory()
    SheetsService.configure(
        os.path.join(_testfolder, _serviceAccountCredentialsPath),
        _testsheet,
        _cacheDir.name,
        _backupDir.name,
        force_test_spreadsheet=False,  # OK as we are using mock
    )

    mock = HttpMockSequence(
        [
            ({"status": "200"}, read_datafile("discovery.json", "rb")),
        ]
    )
    SheetsService.service = googleapiclient.discovery.build("sheets", "v4", http=mock)

    yield mock

    _cacheDir.cleanup()
    _backupDir.cleanup()

    SheetsService.credentials = credentials_old
    SheetsService.service = service_old
    SheetsService.headers = headers_old
    SheetsService._sa_keyfile = _sa_keyfile_old
    SheetsService._spreadsheet_test = _spreadsheet_test_old
    SheetsService._backupLocation = _backupLocation_old
    SheetsService._cacheLocation = _cacheLocation_old
    SheetsService._logger = _logger_old
    SheetsService._force_test_spreadsheet = _force_test_spreadsheet_old
