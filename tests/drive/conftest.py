import logging
import os

import googleapiclient
import pytest
from googleapiclient.http import HttpMockSequence

from gapi_helper.drive import DriveService

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


def datafile(filename):
    return os.path.join(DATA_DIR, filename)


def read_datafile(filename, mode="r"):
    with open(datafile(filename), mode=mode) as f:
        return f.read()


@pytest.fixture(scope="module")
def request_mock():
    _sa_keyfile_old = DriveService._sa_keyfile
    _logger_old = DriveService._logger
    _retry_delay_old = DriveService._retry_delay
    _default_service_old = DriveService._defaultService

    DriveService._sa_keyfile = None
    DriveService._logger = logging.getLogger("gapi_helper")
    DriveService._retry_delay = 0
    DriveService._defaultService = None

    _testfolder = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    _serviceAccountCredentialsPath = "myproject-123456-abcdef012345.json"

    DriveService.configure(
        os.path.join(_testfolder, _serviceAccountCredentialsPath),
    )

    mock = HttpMockSequence(
        [
            ({"status": "200"}, read_datafile("discovery.json", "rb")),
        ]
    )
    service = DriveService.getDefaultService()
    service.service = googleapiclient.discovery.build("drive", "v3", http=mock)

    yield mock

    DriveService._sa_keyfile = _sa_keyfile_old
    DriveService._logger = _logger_old
    DriveService._retry_delay = _retry_delay_old
    DriveService._defaultService = _default_service_old
